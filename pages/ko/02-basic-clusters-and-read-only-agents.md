---
title: Basic 클러스터에서는 읽기 전용 에이전트를 만들 수 없다
description: Basic 클러스터에서 MCP 에이전트에 필요한 읽기 전용 역할은 거부되고 ACL은 무시됐다. 시도한 방법과 동작한 방법을 정리한다.
date: 2026-11-15
part: 2
lang: ko
alternate_url: /02-basic-clusters-and-read-only-agents.html
translation_key: chapter-02
reading_time: 5
sample: 권한 부여 시도 3회
takeaways:
  - label: 관측 결과
    text: DeveloperRead는 토픽과 클러스터 범위 모두에서 실패했고, Kafka READ ACL도 RTCE 도구 세 개에 접근 권한을 부여하지 못했다.
  - label: 해석
    text: CloudClusterAdmin은 동작했지만 읽기 전용 MCP 클라이언트가 클러스터를 관리할 수 있는 키를 갖게 됐다.
  - label: 한계
    text: Basic만 시험했다. 최소 권한 구성은 resource role을 지원하는 Standard 같은 클러스터에서 검증해야 한다.
---

## 목표

비즈니스 상태를 읽는 AI 에이전트가 그 상태를 변경할 수 있어서는 안 된다.
그래서 전용 서비스 계정인 *context reader*에 Global API key와 읽기 권한만 부여하도록 설계했다.
Basic 클러스터에서는 마지막 조건을 충족할 수 없었다.

[Lightning Tables access-control 문서](https://docs.confluent.io/cloud/current/lightning-tables/access-control.html)는 역할별 허용 범위를 다음과 같이 설명한다.

| 역할 | 범위 | 토픽 목록 | 데이터 쿼리 | 메타데이터 조회 |
|---|---|---|---|---|
| DeveloperRead | 클러스터 | 가능 | 가능 | 가능 |
| DeveloperRead | 토픽 | 불가 | 가능 | 불가 |
| DeveloperRead | Schema Registry | 가능 | 불가 | 가능 |

계획은 필요한 토픽과 Schema Registry subject에 `DeveloperRead`를 부여하는 것이었다.

## 시도 1: 토픽에 DeveloperRead 부여

Terraform이 role binding을 거부했다.

```
error creating role binding "DeveloperRead": 403 Forbidden:
Forbidden - Basic Clusters can not use resource roles
```

클러스터 유형 문서에는 Basic이 클러스터 내부 리소스에 RBAC role을 지원하지 않는다고 적혀 있다.
처음에는 이를 세분화된 토픽 role만 제한한다는 뜻으로 이해했고, 문서 표의 클러스터 범위 binding은 가능할 것으로 예상했다.

## 시도 2: 클러스터에 DeveloperRead 부여

같은 오류로 실패했다.
Basic에서는 어떤 범위에서도 서비스 계정에 Kafka resource role을 부여할 수 없다.
Schema Registry binding은 성공했다.

## 시도 3: Kafka ACL 사용

Basic은 ACL로 데이터 접근을 허가한다. 문서에도 “Kafka에서 이미 읽을 권한이 있는” 토픽만 쿼리할 수 있다고 적혀 있다.
그래서 context reader에 모든 토픽의 `READ` ACL을 부여했고 Kafka는 이를 받아들였다.

Real-Time Context Engine은 여전히 요청을 거부했다.

```json
{
  "status": "error",
  "error": {
    "code": "FE_AUTHORIZATION_FAILED",
    "message": "Not authorized to access resource: Access denied by
                authorization service"
  }
}
```

MCP 도구 세 개(`listTopics`, `getMetadata`, `queryData`)가 모두 같은 방식으로 실패했다.
10분이 지나도 결과가 같았으므로 권한 전파 지연도 아니었다.
context reader가 모든 subject에 `DeveloperRead`를 갖고 있었는데도 Schema Registry 접근만 필요하다고 표시된 `getMetadata`까지 거부됐다.
따라서 RTCE는 RBAC authorization service를 확인하고 Kafka ACL은 사용하지 않는 것으로 판단했다.

## 동작한 방법

실험 클러스터의 `CloudClusterAdmin`은 resource role이 아니라 cluster-level role이므로 Basic에서도 허용된다.
이 역할을 부여하자 첫 호출에서 `listTopics`가 성공했다.
결과적으로 이 Basic 클러스터에서 읽기 전용 MCP 클라이언트에 사용할 수 있었던 가장 좁은 자격 증명도 클러스터 관리 권한을 포함했다.

| 선택지 | 자격 증명의 권한 | 클러스터 요구사항 또는 대가 |
|---|---|---|
| Basic + reader에 `CloudClusterAdmin` | MCP 도구는 읽기 전용이지만 키가 유출되면 클러스터를 관리할 수 있음 | 첫 eCKU는 무료이고, 그 이상의 부하는 eCKU-hour당 $0.135로 청구됨([Chapter 7](07-is-a-streamhouse-worth-it.md#what-the-lab-cost)) |
| Standard + `DeveloperRead` | 문서에 명시된 읽기 전용 권한 | eCKU-hour당 $0.75부터, 하루 약 $18 |
| Quick start처럼 사용자 본인의 Global key 사용 | 해당 사용자가 할 수 있는 모든 작업 | 클러스터 가격 변화는 없지만 사용자당 두 개인 Global key 중 하나 사용 |

이 실험에서는 폐기 가능한 단일 클러스터에 첫 번째 방식을 사용했다.
Terraform은 Standard 클러스터를 선택하면 문서의 읽기 전용 binding을 자동으로 적용하도록 작성했다.
실제 시스템이라면 Standard 비용을 지불하겠다.

## 서비스 계정 키도 에이전트에서 동작한다

서비스 계정의 Global API key는 quick start의 사용자 소유 키와 같은 방식으로 Flink agent의 MCP connection에서 동작한다.
따라서 Terraform이 생성하고 삭제하는 principal로 에이전트를 실행하면서 사용자 본인의 Global key 슬롯 두 개를 모두 비워 둘 수 있다.

## 클러스터를 선택할 때

Lightning Tables 앞에 에이전트를 둘 계획이라면 throughput과 가격뿐 아니라 권한 모델도 클러스터 유형을 결정하는 기준에 포함해야 한다.
Basic에서는 context reader가 클러스터의 관리자 역할을 가져야 한다.
