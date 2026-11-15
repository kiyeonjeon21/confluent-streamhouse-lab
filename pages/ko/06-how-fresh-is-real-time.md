---
title: “실시간”은 얼마나 최신인가?
description: Kafka 쓰기부터 Lightning 읽기까지의 freshness, 쿼리 지연, 동시성을 단계별로 측정했다.
date: 2026-11-15
part: 6
lang: ko
alternate_url: /06-how-fresh-is-real-time.html
reading_time: 9
sample: freshness 샘플 30개
takeaways:
  - label: 관측 결과
    text: 파생 상태의 가시성은 p50 44.3초, 최대 68.2초였다. 동시 클라이언트 32개에서는 오류 없이 40.6 req/s에 도달했다.
  - label: 해석
    text: Flink는 p50 2.6초에 출력을 썼지만 38.5초까지 commit되지 않았다. 종단 간 freshness는 commit 대기가 지배했다.
  - label: 한계
    text: 승객 10,000명과 서울 클라이언트로 실행했으므로 in-region latency, 서비스 포화점, scan scaling을 분리하지 못했다.
---

## 측정 환경

| 항목 | 값 |
|---|---|
| 리전 | AWS `us-east-1`, Basic cluster, 최대 5 CFU Flink pool |
| 데이터 | 항공편 1,021개, 승객 10,000명 |
| 읽기 경로 | RTCE over MCP, 클라이언트마다 장기 세션 하나 |
| 클라이언트 | 서울, MCP host까지 TCP connect 190–210 ms |

모든 latency에는 태평양 횡단 round trip이 포함된다. percentile은 nearest-rank 방식이다.

## 종단 간 freshness

브로커가 source write를 확인한 시점부터 Lightning에서 새 값이 보일 때까지 30개 샘플을 측정했다.

| min | p50 | p95 | max | mean |
|---|---|---|---|---|
| 17.8초 | 46.1초 | 66.9초 | 67.6초 | 42.4초 |

첫 실행은 각 샘플을 이전 결과가 보인 지 5초 뒤에 기록해 Flink commit 주기와 동기화되는 오류가 있었다. seeded random jitter를 추가해 write 시점을 61초 구간에 분산했다.

## 단계별 breakdown

| 단계 | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|
| Flink가 output 기록 | 0.2 | 2.6 | 4.8 | 5.0 | 2.7 |
| output commit | 11.3 | 38.5 | 61.1 | 61.8 | 36.7 |
| Lightning에서 가시화 | 17.5 | 44.3 | 66.9 | 68.2 | 42.8 |

**관측 결과.** p50에서 write는 2.6초에 끝났지만 commit은 38.5초, Lightning 가시성은 44.3초였다.

**해석.** serving query보다 Flink transaction commit 대기가 전체 freshness를 지배했다.

**한계.** 이 실험은 문서의 100 ms 목표가 시작하는 “데이터 도착” 내부 지점을 볼 수 없다. 외부에서 관측 가능한 broker acknowledgement부터 성공한 query까지를 측정한다.

## 쿼리 latency

| 형태 | 행 | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| point lookup | 1 | 689 ms | 788 ms | 874 ms | 1,360 ms |
| equality filter | 200 | 697 ms | 817 ms | 1,205 ms | 1,261 ms |
| range filter | 200 | 694 ms | 814 ms | 1,039 ms | 1,222 ms |

세 형태의 median은 689–697 ms로 거의 같았다. 10,000행에서는 query shape보다 호출 자체의 비용이 더 컸다. 다만 이 실행은 약 190 ms의 네트워크를 포함하며 나머지를 MCP와 Lightning engine으로 분리하지 못한다.

## 동시성에 따른 throughput

| 동시성 | 요청 수 | 처리량 | 오류 | p50 | p95 |
|---|---|---|---|---|---|
| 1 | 40 | 1.3 req/s | 0 | 694 ms | 1,182 ms |
| 8 | 354 | 11.8 req/s | 0 | 644 ms | 720 ms |
| 16 | 698 | 23.3 req/s | 0 | 646 ms | 743 ms |
| 32 | 1,219 | 40.6 req/s | 0 | 774 ms | 837 ms |

16 clients까지는 처리량이 선형으로 늘고 latency가 거의 유지됐다. 32 clients에서는 40.6 req/s로 선형 기대치 약 46 req/s보다 낮았고 median이 약 130 ms 증가했다. 모든 worker가 서울의 한 Python process를 공유했으므로 이 둔화를 서비스 포화로 단정할 수 없다.
