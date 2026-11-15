---
title: Materialized Table 실전 운용
description: statement 목록에 보이지 않는 continuous query, 공식 예제의 CRITICAL 경고 두 개, freshness를 결정하는 설정.
date: 2026-11-15
part: 3
lang: ko
alternate_url: /03-materialized-tables-in-practice.html
translation_key: chapter-03
reading_time: 6
sample: CRITICAL 경고 2개
takeaways:
  - label: 관측 결과
    text: 공식 River Air 쿼리에는 key mismatch와 unbounded join state라는 CRITICAL 경고 두 개가 표시됐다.
  - label: 해석
    text: CREATE statement가 COMPLETED로 표시돼도 과금되는 continuous query는 계속 실행된다. 5초 sink flush도 5초 이내의 freshness를 뜻하지 않았다.
  - label: 한계
    text: 작은 실험 데이터에서는 경고의 영향이 드러나지 않았고 장기간 실행했을 때 상태가 얼마나 증가하는지는 측정하지 않았다.
---

Confluent Cloud의 Flink Materialized Table은 테이블 정의, compaction이 설정된 backing topic, 스키마, 결과를 채우는 continuous query를 함께 소유하는 단일 객체다.
River Air의 `passenger_state`는 각 itinerary를 도착편과 연결편에 join하고, 승객마다 위험 상태를 포함한 행 하나를 유지한다.

```sql
CREATE MATERIALIZED TABLE passenger_state (
  PRIMARY KEY (passenger_id) NOT ENFORCED
)
DISTRIBUTED BY (passenger_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert')
AS SELECT
  p.passenger_id,
  ...
  CASE
    WHEN p.connecting_flight_id IS NULL THEN 'NO_CONNECTION'
    WHEN TIMESTAMPDIFF(MINUTE, inbound.estimated_time,
                       onward.estimated_time) < 45 THEN 'AT_RISK'
    ELSE 'OK'
  END AS risk
FROM passenger_itineraries p
JOIN flight_status inbound ON p.inbound_flight_id = inbound.flight_id
LEFT JOIN flight_status onward ON p.connecting_flight_id = onward.flight_id;
```

문서에 설명된 대로 동작했다.
RA417편이 지연되자 정의를 변경하지 않았는데도 `P-001`, `P-002`가 `AT_RISK`로 바뀌었다.
다음 세 가지를 이해하는 데에는 시간이 더 걸렸다.

## 1. 실행 중인 쿼리는 statement 목록에 없다

제출 직후 `CREATE MATERIALIZED TABLE` statement는 `RUNNING`으로 표시됐다.
몇 분 뒤 bounded statement처럼 `COMPLETED`로 바뀌었다.
statement 목록에는 실행 중인 항목이 없었지만 테이블은 계속 갱신됐다.

콘솔도 두 항목을 분리한다.
Statements 탭에서는 quick start용과 부하 테스트용 Materialized Table이 완료된 `CREATE MATERIALIZED TABLE` statement로만 보인다.

[![두 CREATE MATERIALIZED TABLE statement가 Completed로 표시된 Flink Statements 탭](../assets/flink-statements-mt-completed.png)](../assets/flink-statements-mt-completed.png)

옆의 Materialized tables 탭에서는 두 테이블이 각각 1 CFU를 사용하며 실행 중이다.

[![Materialized Table 두 개가 각각 1 CFU로 Running 상태인 화면](../assets/flink-mt-running.png)](../assets/flink-mt-running.png)

*Confluent Cloud 콘솔, 2026-11-15. 클러스터 ID 열은 가렸다. 이미지를 누르면 원본 크기로 볼 수 있다.*

API에서는 continuous query를 `GET /sql/v1/.../materialized-tables`에서 조회한다. 이 API는 자체 phase, version, `stopped` flag를 반환한다.

```
materialized tables (continuous queries; billed while RUNNING):
  RUNNING    passenger_state v1
```

Stream Lineage에는 source topic과 output 사이의 Flink node로 표시된다. [Chapter 1](01-the-lab.md)에 화면이 있다.

statement 목록만 사용해 Flink를 모니터링하거나 정리하면 24시간 과금되는 부분을 놓치게 된다.
실험 환경의 status command는 이제 두 목록을 모두 읽는다.

## 2. 공식 예제에 CRITICAL 경고가 두 개 있다

Materialized Table API는 경고도 반환한다.
문서에서 그대로 복사한 quick start 쿼리에는 둘 다 `CRITICAL`인 경고 두 개가 있었다.

- `UPSERT_AND_PRIMARY_KEYS_DIFFERENT`: 선언한 primary key인 `passenger_id`와 Flink가 쿼리에서 유도한 upsert key가 다르다. Flink는 상태를 많이 사용하는 보정 단계를 추가한다.
- `HIGH_STATE_OPERATOR_WITHOUT_TTL`: 일반 join에 time-to-live가 없어 상태를 영구 보관한다. statement 성능이 저하되고 CFU 사용량이 증가할 수 있다.

콘솔은 테이블 페이지 상단에 두 경고를 모두 표시한다.

[![activity 탭 위에 CRITICAL 경고 두 개가 표시된 Materialized Table 상세 화면](../assets/flink-mt-warnings.png)](../assets/flink-mt-warnings.png)

승객 세 명으로 실행한 실험에서는 경고로 인한 문제가 보이지 않았지만 Materialized Table은 여전히 유료 CFU 하나를 사용했다.
프로덕션 규모라면 각각 해결해야 한다. 유도된 upsert key와 선언한 primary key가 일치하도록 쿼리나 key를 다시 설계하고, join 상태의 상한을 두도록 state TTL을 추가해야 한다.
Materialized Table을 변경하면 상태가 삭제되고 다시 구축될 수 있다. upsert mode에서는 새 쿼리가 더 이상 내보내지 않는 오래된 key가 남을 수도 있다.

## 3. Freshness를 결정하는 설정

확정된 table option에는 다음 값이 포함된다.

```
'confluent.sink.buffer-flush.enabled'  = 'true'
'confluent.sink.buffer-flush.interval' = '5 s'
'kafka.cleanup-policy'                 = 'compact'
'key.format'                           = 'avro-registry'
'value.format'                         = 'avro-registry'
```

flush가 5초이므로 downstream reader도 몇 초 안에 변경을 볼 것으로 예상했다.
실제 측정에서는 derived state가 약 1분 간격으로 Lightning Tables에 나타났다.
Confluent의 [delivery guarantees 문서](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html)에 따르면 기본 exactly-once mode에서 Flink는 output transaction을 약 1분에 한 번 커밋한다. Lightning을 포함해 커밋된 데이터만 읽는 reader는 이 커밋을 기다린다.
[Chapter 6](06-how-fresh-is-real-time.md)에서 각 단계를 측정했다.

## 그 밖의 참고 사항

- 같은 join을 bounded snapshot query로 실행한 복사본은 약 10초 만에 끝났고 상태 관련 경고도 없었다. 탐색 단계에서는 snapshot query를 사용하고 결과를 계속 최신으로 유지해야 할 때만 Materialized Table로 만들겠다.
- Materialized Table은 존재하는 동안 최소 CFU 하나의 비용이 발생한다. 삭제하지 않고도 API 또는 Terraform의 `stopped = true`로 중지할 수 있다.
