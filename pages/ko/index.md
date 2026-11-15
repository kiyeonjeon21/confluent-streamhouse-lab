---
title: Confluent Cloud에서 Streamhouse 구축하기
description: Kafka, Flink Materialized Table, Lightning Tables, Tableflow, AI 에이전트를 직접 구축하고 측정한 7부작 실험 기록.
layout: landing
lang: ko
alternate_url: /
permalink: /ko/
---

<section class="home-hero">
  <div class="home-hero__copy">
    <p class="eyebrow">7개 챕터로 구성한 재현 가능한 실험</p>
    <h1>Streamhouse의 “실시간”은 얼마나 최신인가?</h1>
    <p class="home-hero__dek">Confluent의 River Air Streamhouse를 Terraform, Git으로 관리하는 Flink SQL, 자동화 테스트로 다시 구축했다. 이어서 Kafka 쓰기부터 derived state, Iceberg history, AI agent까지 이어지는 경로의 latency를 측정했다.</p>
    <p class="home-hero__summary">아키텍처는 동작했다. 예상 밖이었던 것은 latency였다. 이 구성에서 derived state가 Lightning Tables에서 조회 가능해지기까지 <strong>p50 기준 44.3초</strong>가 걸렸고, 대부분은 약 1분 간격의 Flink exactly-once commit을 기다리는 시간이었다.</p>
    <div class="home-hero__actions">
      <a class="button button--primary" href="{{ '/ko/01-the-lab.html' | relative_url }}">실험실부터 읽기</a>
      <a class="button" href="{{ '/ko/06-how-fresh-is-real-time.html' | relative_url }}">벤치마크 보기</a>
    </div>
  </div>
  <aside class="home-hero__scope" aria-label="실험 범위">
    <p class="section-label">실험 범위</p>
    <dl>
      <div><dt>환경</dt><dd>AWS <code>us-east-1</code></dd></div>
      <div><dt>클러스터</dt><dd>Confluent Basic</dd></div>
      <div><dt>벤치마크 데이터</dt><dd>승객 10,000명</dd></div>
      <div><dt>클라이언트</dt><dd>서울 · 약 190 ms</dd></div>
      <div><dt>실행일</dt><dd>2026년 11월 15일</dd></div>
    </dl>
  </aside>
</section>

<section class="home-section home-section--metrics" aria-labelledby="ko-measured-title">
  <div class="section-heading"><div><p class="section-label">측정 결과</p><h2 id="ko-measured-title">핵심 수치</h2></div><p>아래 수치 중 앞의 세 개는 Chapter 6, Iceberg 수치는 Chapter 4의 결과다. 모두 서울에서 <code>us-east-1</code>의 Basic 클러스터를 측정한 값이다.</p></div>
  <div class="metric-grid">
    <article class="metric-card metric-card--primary"><span class="metric-card__value">44.3<span>초</span></span><strong>End-to-end freshness, p50</strong><p>Broker acknowledgement부터 derived row가 Lightning Tables에서 조회 가능해질 때까지 측정한 30개 샘플.</p></article>
    <article class="metric-card"><span class="metric-card__value">3–7<span>초</span></span><strong>Commit-to-Lightning visibility</strong><p>Broker commit 이후 첫 쿼리가 변경을 반환하기까지 관측한 범위.</p></article>
    <article class="metric-card"><span class="metric-card__value">40.6<span>req/s</span></span><strong>동시 클라이언트 32개</strong><p>30초간 MCP point lookup을 실행했으며 오류나 rate limit은 없었다.</p></article>
    <article class="metric-card"><span class="metric-card__value">4.5<span>분</span></span><strong>첫 Iceberg 데이터</strong><p>처리량이 낮은 토픽의 세 번째 스냅샷에 소스 행 6개가 모두 포함됐다.</p></article>
  </div>
</section>

<section class="home-section" aria-labelledby="ko-chapters-title">
  <div class="section-heading"><div><p class="section-label">실험 기록</p><h2 id="ko-chapters-title">구축부터 결론까지, 7개 챕터</h2></div><p>전체 조사 과정은 순서대로 읽고, 필요한 구현 세부 정보나 벤치마크가 있다면 해당 챕터로 바로 이동할 수 있다.</p></div>
  {% include chapter-grid.html %}
</section>

<section class="method-note" aria-labelledby="ko-method-title"><div><p class="section-label">방법</p><h2 id="ko-method-title">재현하고 검증할 수 있는 실험</h2></div><p>Terraform, Git으로 관리하는 SQL, 오프라인 unit test 150개와 live test 6개, 별도 benchmark table을 사용했다. 측정 챕터에는 샘플 수와 실험이 증명하지 못한 범위를 함께 기록했다.</p><a href="{{ site.repository_url }}">저장소 보기 <span aria-hidden="true">→</span></a></section>
