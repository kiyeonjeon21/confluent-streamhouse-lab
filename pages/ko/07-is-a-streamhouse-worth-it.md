---
title: Streamhouse는 도입할 가치가 있는가?
description: 이 실험이 입증한 가치, 적합한 영역과 부적합한 영역, 프로덕션 전에 남은 질문.
date: 2026-11-15
part: 7
lang: ko
alternate_url: /07-is-a-streamhouse-worth-it.html
reading_time: 6
sample: 여섯 기능의 실험 증거
takeaways:
  - label: 관측 결과
    text: 하나의 Flink 정의가 애플리케이션, Lightning 읽기, 에이전트에 상태를 제공했고 Tableflow는 별도 export pipeline 없이 히스토리를 보존했다.
  - label: 해석
    text: 수십 초를 허용하는 운영 의사결정에는 맞지만 sub-second read-your-write나 serving layer의 분석 쿼리에는 맞지 않는다.
  - label: 한계
    text: 프로덕션 판단에는 현실적인 규모의 scan cost 측정과 에이전트에 최소 권한을 줄 수 있는 cluster tier가 필요하다.
---

## 판단 기준

Streamhouse의 가치는 “모든 것을 하나의 시스템으로 바꾼다”가 아니다. Kafka에 들어온 이벤트에서 현재 상태를 한 번 계산하고, 같은 토픽을 현재 상태의 serving과 Iceberg history에 함께 사용해 중복 pipeline과 비즈니스 로직의 drift를 줄이는 데 있다.

## 이 실험이 입증한 것

- Materialized Table 하나가 위험 승객 상태를 지속적으로 유지했다.
- Lightning Tables와 MCP agent가 같은 파생 상태를 읽었다.
- Tableflow는 같은 토픽을 Iceberg history로 기록했다.
- Flink snapshot query는 Iceberg의 과거와 Kafka의 최신 데이터를 결합했다.

## 맞는 영역

수십 초의 freshness를 허용하는 운영 의사결정, 현재 상태와 history가 모두 필요한 workflow, 관리형 Kafka와 Flink를 이미 사용하는 팀에 적합하다. 애플리케이션과 AI agent가 같은 정의의 상태를 읽는 경우 특히 의미가 있다.

## 맞지 않는 영역

sub-second read-your-write, 복잡한 join과 aggregation을 serving layer에서 즉석 실행해야 하는 분석, Basic cluster에서 강한 최소 권한이 필수인 agent workload에는 맞지 않는다. Lightning SQL의 제한 때문에 join과 aggregate는 upstream Flink에서 미리 계산해야 한다.

## 프로덕션 전에 남은 검증

100,000행 이상에서 primary-key lookup의 scanned bytes가 어떻게 증가하는지, in-region client에서 latency와 saturation이 어떻게 달라지는지, 장기 실행 join state와 비용이 어떻게 증가하는지를 추가로 측정해야 한다. 이 실험의 결론은 “쓸 수 있다”이지만 “어디에나 맞는다”는 아니다.
