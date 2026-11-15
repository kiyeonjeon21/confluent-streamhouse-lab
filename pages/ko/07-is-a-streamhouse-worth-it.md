---
title: Streamhouse는 도입할 가치가 있는가?
description: Streamhouse의 약속과 실험으로 확인한 결과, 적합한 영역과 부적합한 영역을 비교한다.
date: 2026-11-15
part: 7
lang: ko
alternate_url: /07-is-a-streamhouse-worth-it.html
translation_key: chapter-07
reading_time: 8
sample: 6개 기능의 실험 증거
takeaways:
  - label: 관측 결과
    text: 승객 상태를 정의한 하나의 Flink 로직이 Lightning 읽기와 에이전트에 모두 사용됐고 Tableflow는 별도 export pipeline 없이 히스토리를 보존했다.
  - label: 해석
    text: 기본 exactly-once delivery로 시험한 이 구성은 수십 초를 허용하는 운영 의사결정에 적합하다. Sub-second read-your-write나 serving layer의 분석에는 맞지 않는다.
  - label: 한계
    text: 프로덕션 도입 전에는 현실적인 table 크기에서 scan cost를 측정하고 에이전트에 최소 권한을 줄 수 있는 클러스터를 사용해야 한다.
---

앞의 여섯 챕터에서는 각 구성 요소의 동작을 살펴봤다.
이제 팀이 실제로 물을 질문, “다음 시스템을 이 방식으로 구축해야 하는가?”를 답할 차례다.
판단은 이 실험에서 측정한 내용으로만 제한한다.

## 판단 기준

대부분의 회사는 비즈니스의 복사본을 두 개 유지한다.
운영 시스템은 지금 무엇이 사실인지 알고, warehouse나 lakehouse는 몇 분 또는 몇 시간 전에 무슨 일이 있었는지 안다.
애플리케이션, dashboard, AI agent에는 둘 다 조금씩 필요하므로 그 사이에 stream processor, serving database, cache, 야간 export 같은 복사본이 더 생긴다.
각 복사본은 같은 비즈니스 규칙을 자체 코드로 구현하고 시간이 지나면서 서로 달라진다.

Streamhouse는 event stream에서 현재 상태를 한 번, 지속적으로 계산한다. 별도 pipeline 없이 history까지 포함한 같은 결과를 필요한 모든 consumer에 제공한다.

## 일반적인 대안

| 접근 방식 | 현재 상태 유지 방법 | Freshness | 히스토리 | 운영 대상 |
|---|---|---|---|---|
| Warehouse 또는 lakehouse batch | 적재된 데이터에 schedule job 실행 | 수분–수시간 | 매우 좋음 | Loader, scheduler, warehouse |
| 직접 구축한 streaming | Stream processor가 serving database 또는 cache에 기록 | 구축한 만큼 빠름 | Lake로 보내는 별도 pipeline | 모든 component와 정합성 유지 코드 |
| Streaming database | 한 시스템이 view를 유지하고 query에 응답 | 시스템마다 다름 | 시스템에 따라 다름 | 보통 event platform 밖의 시스템 하나 |
| Confluent Cloud Streamhouse | Kafka topic 위의 Flink Materialized Table, Lightning Tables serving, Tableflow의 Iceberg 기록 | 이 실험에서 18–68초 | 같은 topic에서 만든 Iceberg | 대부분 configuration이며 component는 관리형 |

마지막 방식에서는 모든 계층이 같은 Kafka topic을 사용한다.
Flink가 topic에 쓰고, Lightning이 제공하며, Tableflow가 보관하고, agent가 MCP로 읽으므로 비즈니스 로직은 한 곳에 남는다.

## 네 가지 약속과 실험 증거

Confluent quick start는 Streamhouse를 네 가지 solution으로 설명한다. 각 약속을 실험 결과와 비교했다.

### 1. 일관된 상태를 한 번만 유지한다

이 약속은 성립했다.
승객 상태를 정의한 하나의 Flink 로직을 Lightning과 에이전트가 함께 읽었고, bounded join도 같은 연결 시간을 계산했다. 지연 이벤트를 넣자 정의를 바꾸지 않았는데도 승객 두 명이 `AT_RISK`로 바뀌었다([Chapter 3](03-materialized-tables-in-practice.md)). Tableflow는 별도 export pipeline 없이 히스토리를 보존했다.

단, 모든 reader가 같은 상태에 합의하는 시점은 약 1분 늦었다.
Flink의 기본 exactly-once output은 [Confluent 문서](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html)에 따르면 약 1분마다 커밋되고 Lightning은 커밋 뒤 3–7초가 더 걸렸다([Chapter 6](06-how-fresh-is-real-time.md)).

### 2. 최근 5초와 지난 5년을 함께 판단한다

“초”를 수십 초로 해석하면 동작한다.
Source write에서 derived state를 Lightning으로 읽기까지 18–68초, 평균 약 42초가 걸렸다. 운영 팀에는 실시간일 수 있지만 checkout page에는 느리다.

History 경로는 잘 동작했다.
Tableflow는 약 4분 만에 Iceberg에 데이터를 기록했고 pyiceberg로 직접 읽을 수 있었다. Kafka copy를 삭제한 뒤에도 Flink snapshot query 하나가 Iceberg의 과거 행과 Kafka에만 있는 새 행을 함께 반환했다([Chapter 4](04-proving-tableflow-wrote-iceberg.md)).
외부 analytics engine에서 대규모 Iceberg query는 시험하지 않았다.

### 3. AI agent를 실시간 상태에 연결하고 변화에 적응시킨다

이 약속도 성립했고 가장 인상적이었다.
관리형 Claude model의 Flink agent가 Real-Time Context Engine으로 실시간 항공편과 호텔을 조회해 첫 실행에서 정확한 재예약 제안을 반환했다([Chapter 5](05-a-streaming-agent-grounded-in-live-state.md)).
Model endpoint, SQL의 API key, retrieval pipeline을 따로 만들 필요가 없었다.

Model은 한 줄 응답 형식을 무시했으므로 output을 방어적으로 parsing해야 한다.
Basic cluster에서는 읽기 전용 role을 쓸 수 없어 agent credential이 cluster 관리 권한까지 가졌다([Chapter 2](02-basic-clusters-and-read-only-agents.md)).

### 4. 비즈니스를 모니터링한다

작은 규모에서는 성립했다.
위험 flag가 자동으로 최신 상태를 유지하는 비즈니스 monitor 역할을 했다.

다만 Flink는 quick start query에 CRITICAL 경고 두 개를 표시한다. Primary key와 유도된 upsert key가 다르고 join state가 영구히 유지된다.
승객 세 명에서는 영향이 보이지 않았지만 실제 규모에서는 설계 검토 단계에서 둘 다 해결해야 한다.

## 적합한 영역

실험 결과를 기준으로 다음 상황에 적합하다.

- 비즈니스가 이미 Kafka에서 실행되고 여러 consumer 사이의 derived state를 일관되게 유지하는 일이 어렵다.
- 현재 상태가 필요하지만 수십 초 지연을 허용할 수 있다. 운영 dashboard, 재예약 같은 예외 처리, alerting, 비즈니스 상태에 따라 행동하는 agent가 예다.
- AI agent에 live, governed context가 필요하고 retrieval stack을 직접 만들고 싶지 않다.
- Serving database, cache, export pipeline을 직접 운영하기보다 managed service를 구성하는 편을 선호한다.

## 아직 적합하지 않은 영역

- 시험한 기본 exactly-once delivery에서 sub-second freshness 또는 read-your-write가 필요하다. Flink는 약 1분마다 commit하고 Lightning이 몇 초를 더하므로 방금 변경한 derived state를 바로 볼 수 없다. Confluent는 이 latency를 줄이는 작업을 하고 있다고 설명한다.
- Serving layer에서 analytical query가 필요하다. Lightning은 MCP query당 최대 200행을 반환하고 aggregate, `GROUP BY`, join을 거부하므로 Flink 또는 Iceberg를 읽는 engine이 맡아야 한다.
- 측정 전의 대규모 table 고빈도 lookup. 10,000행에서 query당 평균 약 1.4 MB를 scan해 table 대부분을 읽는 것처럼 보였다. Lightning compute는 scan byte도 과금 기준이므로 table size와 함께 증가한다면 비싸질 수 있다.
- 가장 저렴한 cluster에서 최소 권한이 필요하다. Agent가 read-only credential을 가져야 한다면 Standard cluster 비용을 반영해야 한다.
- AWS 이외의 cloud. Lightning Tables와 Real-Time Context Engine은 AWS에서만 제공된다.

## 비용 구조

실행되는 항목에 비용이 발생한다.

- Materialized Table은 데이터 도착 여부와 관계없이 존재하는 동안 최소 Flink CFU 하나를 사용한다.
- Tableflow topic은 활성화된 시간만큼 과금된다.
- Lightning Tables는 항상 storage 비용이 들고 데이터가 도착하거나 query가 scan할 때 compute 비용이 든다.
- Agent는 model inference 비용이 든다.

유휴 Basic cluster, compute pool, Schema Registry에는 비용이 들지 않는다. 그래서 전체 구성을 코드로 만들고 작업 세션 사이에 철거할 수 있었다.

## 더 확인하고 싶은 것

- Statement의 commit interval을 줄일 수 있는가? 가능하다면 CFU 사용량에는 어떤 영향이 있는가? 문서에는 설정이 없고 support 문의를 안내한다.
- 리전 내부에서 측정할 때 100,000행 이상에서 scanned bytes와 latency는 어떻게 변하는가?
- Materialized Table을 실행 중에 변경하거나 부하 중지하고 schema를 깨뜨렸을 때 evolution과 failure는 어떤 모습인가?
- 이 모든 항목은 실제 invoice에서 얼마인가?

## 내가 사용하겠는가?

Streamhouse라는 발상은 실험을 통과했다고 생각한다.
현재 상태를 한 번 정의하면 Flink가 최신으로 유지하고 애플리케이션과 agent가 읽으며 Iceberg가 history를 보관한다. 계층 사이의 유일한 interface는 Kafka topic이다.
Confluent Cloud에서는 놀랄 만큼 적은 코드로 구성할 수 있었고 streaming agent는 SQL에서 시작해 첫 실행에 정확한 답을 냈다.

하지만 이 구성에서 “실시간”은 수십 초였고 그 값을 결정한 것은 serving layer가 아니라 Flink의 exactly-once commit interval이었다.
Lightning은 빠른 lookup에는 적합하지만 aggregate나 join은 할 수 없다.

1분이면 충분히 최신인 운영 의사결정과 agent context에는 사용하겠다.
고빈도 lookup에 의존하기 전에는 내 데이터로 scan 비용을 측정하고 analytical work는 Iceberg에 두겠다.
