---
title: 실시간 상태에 근거하는 스트리밍 에이전트
description: 관리형 Claude 모델이 RTCE의 MCP 도구로 항공편과 호텔을 조회해 승객을 재예약한다.
date: 2026-11-15
part: 5
lang: ko
alternate_url: /05-a-streaming-agent-grounded-in-live-state.html
reading_time: 5
sample: 에이전트 실행 2회
takeaways:
  - label: 관측 결과
    text: 관리형 Claude 모델은 MCP로 실시간 항공편과 호텔을 조회해 두 승객 모두에게 예상한 항공편 두 개와 호텔을 반환했다.
  - label: 해석
    text: AI_RUN_AGENT는 append-only 입력을 요구하므로 upsert Materialized Table의 위험 승객을 별도 스트림으로 복사해야 했다.
  - label: 한계
    text: 두 실행 모두 한 줄 출력 형식을 지키지 않았고 Basic 클러스터의 읽기 도구 자격 증명에는 여전히 CloudClusterAdmin이 필요했다.
---

## 에이전트 구성

스택은 관리형 Claude model, RTCE MCP connection을 사용하는 tool, prompt와 model·tool을 묶는 agent, 그리고 agent를 실행하는 append-only 입력으로 구성된다.

## 왜 중간 스트림이 필요한가

`passenger_state`는 같은 승객 행을 갱신하는 upsert table이다. 반면 `AI_RUN_AGENT`는 append-only 입력만 받는다. 따라서 `AT_RISK`로 바뀐 승객을 별도 append-only table로 내보낸 다음 agent를 실행했다.

## 결과와 경계

두 승객 모두에서 도구 호출과 최종 선택은 예상과 일치했다. 에이전트는 취소 항공편을 제외하고 연결 가능 시간이 충분한 항공편 두 개와 조건에 맞는 호텔을 선택했다.

하지만 정답과 형식 준수는 별개였다. 모델은 두 번 모두 요청한 단일 행 형식을 따르지 않았다. 또한 Chapter 2의 권한 문제 때문에 “읽기 전용” 도구 뒤의 서비스 계정은 관리자 역할을 가지고 있었다. 정확성, 출력 계약, 최소 권한을 각각 검증해야 한다.
