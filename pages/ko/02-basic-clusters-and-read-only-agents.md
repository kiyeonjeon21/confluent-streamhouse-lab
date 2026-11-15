---
title: Basic 클러스터에서는 읽기 전용 에이전트를 만들 수 없다
description: Basic 클러스터에서 MCP 에이전트에 필요한 읽기 전용 역할과 ACL을 시험한 결과.
date: 2026-11-15
part: 2
lang: ko
alternate_url: /02-basic-clusters-and-read-only-agents.html
reading_time: 4
sample: 권한 부여 시도 3회
takeaways:
  - label: 관측 결과
    text: DeveloperRead는 토픽과 클러스터 범위 모두에서 실패했고, Kafka READ ACL도 RTCE 도구 세 개를 하나도 허용하지 못했다.
  - label: 해석
    text: CloudClusterAdmin은 동작하지만 읽기 전용 MCP 클라이언트에 과도한 권한을 부여한다.
  - label: 한계
    text: 이 실험은 Basic 클러스터의 경계를 확인했다. 최소 권한은 resource role을 지원하는 상위 tier에서 다시 검증해야 한다.
---

## 목표

비즈니스 상태를 읽기만 하는 AI 에이전트는 그 상태를 바꿀 수 없어야 한다. 전용 서비스 계정에 필요한 토픽과 Schema Registry의 읽기 권한만 주려 했지만 Basic 클러스터에서는 이 구성이 불가능했다.

## 세 번의 시도

| 시도 | 결과 |
|---|---|
| 토픽 범위 `DeveloperRead` | 403: Basic cluster는 resource role을 사용할 수 없음 |
| 클러스터 범위 `DeveloperRead` | 같은 이유로 거부 |
| Kafka `READ` ACL | 생성됐지만 RTCE의 metadata·query 도구를 허용하지 않음 |

`CloudClusterAdmin`을 부여하자 MCP 호출은 성공했다. 그러나 자격 증명이 유출되면 읽기 전용 에이전트의 범위를 넘어 클러스터 전체를 관리할 수 있다.

## 결론

Basic 클러스터와 최소 권한 MCP reader는 양립하지 않았다. 실험 편의상 관리자 역할을 사용할 수는 있어도, 프로덕션에서는 resource role을 지원하는 tier에서 `DeveloperRead`를 적용하고 실제 도구 호출까지 검증해야 한다.
