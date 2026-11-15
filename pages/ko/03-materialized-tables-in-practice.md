---
title: Materialized Table 실전 운용
description: statement 목록에 보이지 않는 continuous query, CRITICAL 경고 두 개, freshness를 결정하는 설정.
date: 2026-11-15
part: 3
lang: ko
alternate_url: /03-materialized-tables-in-practice.html
reading_time: 5
sample: CRITICAL 경고 2개
takeaways:
  - label: 관측 결과
    text: 공식 River Air 쿼리는 key mismatch와 unbounded join state라는 CRITICAL 경고 두 개를 발생시켰다.
  - label: 해석
    text: CREATE statement가 끝나도 과금되는 continuous query는 계속 실행되며 5초 sink flush가 5초 가시성을 뜻하지 않는다.
  - label: 한계
    text: 작은 quick-start 데이터에서 경고를 관측했지만 장기간의 state 증가량은 측정하지 않았다.
---

## 하나의 객체, 계속 실행되는 쿼리

Flink Materialized Table은 테이블 정의, compacted backing topic, 스키마, 결과를 채우는 continuous query를 함께 소유한다. `CREATE MATERIALIZED TABLE` statement 자체는 완료되지만 continuous query는 별도 materialized-tables API에서 계속 실행되고 CFU를 사용한다.

## 공식 쿼리의 경고

첫 번째 경고는 선언한 primary key와 출력 changelog key가 일치하지 않을 수 있다는 내용이었다. 두 번째는 event-time 제한이 없는 join 때문에 state가 무한히 커질 수 있다는 경고였다. 빠른 데모에서는 결과가 맞더라도, 장기 실행 환경에서는 state와 비용을 따로 관찰해야 한다.

## Flush와 commit은 다르다

5초 sink flush 이후 output topic에는 결과가 빨리 기록됐다. 하지만 `read_committed` consumer와 Lightning Tables는 Flink transaction이 commit될 때까지 볼 수 없었다. 이 실험에서는 commit된 출력이 약 1분 단위로 나타났다.

따라서 freshness를 조정할 때 sink buffer만 보면 안 된다. write, transaction commit, serving visibility를 각각 측정해야 한다.
