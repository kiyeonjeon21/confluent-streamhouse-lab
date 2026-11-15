---
title: Tableflow가 Iceberg에 기록했음을 증명하기
description: pyiceberg 직접 조회와 Kafka 데이터 삭제 실험으로 하이브리드 읽기 경로를 검증했다.
date: 2026-11-15
part: 4
lang: ko
alternate_url: /04-proving-tableflow-wrote-iceberg.html
reading_time: 6
sample: offset으로 대조한 행 6개
takeaways:
  - label: 관측 결과
    text: 관리형 Iceberg catalog를 pyiceberg로 직접 읽어 offset 0부터 5까지 소스 행 여섯 개를 모두 확인했다.
  - label: 해석
    text: Kafka 레코드를 삭제한 뒤에도 Iceberg의 과거 행과 Kafka의 새 행을 한 Flink snapshot query로 읽어 하이브리드 경로를 입증했다.
  - label: 한계
    text: 여섯 행의 정합성은 이 경로의 정확성을 보여 주지만 분석 규모의 compaction, retention, query behavior까지 검증하지는 않는다.
---

## 성공한 쿼리만으로는 부족하다

Tableflow-enabled topic에 대한 Flink snapshot query는 오래된 데이터를 Iceberg에서, 최신 데이터를 Kafka에서 읽는다. 레코드가 아직 Kafka에 남아 있다면 같은 결과가 나와도 실제로 Iceberg를 사용했는지 알 수 없다.

## 증거 1: catalog 직접 조회

Iceberg REST catalog에 `pyiceberg`로 연결해 snapshot과 data file을 직접 읽었다. source topic의 offset 0–5가 모두 존재했고 여섯 행의 값이 Kafka 원본과 일치했다.

## 증거 2: 하이브리드 경로 분리

과거 Kafka 레코드를 삭제한 뒤 새 레코드를 추가했다. 이후 한 snapshot query가 Kafka에는 더 이상 없는 과거 집계와 Iceberg에는 아직 반영되지 않은 최신 행을 함께 반환했다.

이 실험은 단순히 “쿼리가 성공했다”보다 강한 증거다. 두 저장 계층에만 존재하는 데이터를 의도적으로 만든 뒤 하나의 읽기 경로가 둘을 결합하는지 확인했기 때문이다.
