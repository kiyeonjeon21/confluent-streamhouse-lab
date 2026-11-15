---
title: Terraform과 SQL로 Streamhouse 구축하기
description: Confluent Streamhouse의 구성 요소와 공식 quick start를 재현 가능한 실험실로 다시 만든 과정.
date: 2026-11-15
part: 1
lang: ko
alternate_url: /01-the-lab.html
reading_time: 6
sample: 실시간 기능 6개
takeaways:
  - label: 관측 결과
    text: Kafka 소스, Flink 변환, materialized state, Lightning 읽기, Iceberg 히스토리, 재예약 에이전트까지 여섯 기능이 모두 통과했다.
  - label: 해석
    text: 각 관리형 계층은 Kafka 토픽으로 데이터를 교환하고, 파생 비즈니스 상태의 정의는 Flink 한 곳에 남는다.
  - label: 한계
    text: 이 일회성 실험실은 통합 경로를 검증하지만 프로덕션 규모의 신뢰성, 비용, 운영 방식까지 증명하지는 않는다.
---

## Streamhouse의 구성

Kafka 토픽은 이벤트와 스키마를 보관한다. Flink SQL의 Materialized Table은 이벤트를 지속적으로 최신 상태로 변환한다. Lightning Tables는 그 상태를 애플리케이션과 Real-Time Context Engine의 MCP 도구에 제공하고, Tableflow는 같은 토픽의 히스토리를 Iceberg 테이블로 기록한다.

핵심은 파생 상태를 서빙 데이터베이스나 export pipeline에서 다시 구현하지 않는다는 점이다. 비즈니스 로직은 Flink에 한 번 정의되고 Kafka 토픽이 계층 사이의 인터페이스가 된다.

## River Air 시나리오

지연된 RA417편 때문에 연결편을 놓칠 승객을 찾고, 각 승객에게 대체 항공편 두 개와 필요한 경우 호텔을 제안한다. 동시에 과거 지연 기록도 질의해야 한다.

## 재현 가능한 구축

콘솔 클릭 대신 Terraform으로 클러스터, 토픽, 스키마, 서비스 계정을 선언했다. Flink SQL은 한 파일에 한 statement를 두고 순서대로 실행했으며, 테스트가 토픽 데이터·Materialized Table·Lightning·Iceberg·에이전트 결과를 검증했다.

## 검증 결과

| 기능 | 결과 |
|---|---|
| Kafka 소스와 스키마 | 통과 |
| Flink 변환 | 통과 |
| Materialized Table | 통과 |
| Lightning Tables 읽기 | 통과 |
| Tableflow와 Iceberg | 통과 |
| 재예약 에이전트 | 통과 |

이 결과는 제품 전반의 성능을 입증하는 벤치마크가 아니라, 동일한 실험을 폐기하고 다시 구축할 수 있는 출발점이다.
