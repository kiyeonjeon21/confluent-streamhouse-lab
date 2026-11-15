---
title: Building a Streamhouse on Confluent Cloud
description: A hands-on series on Kafka, Flink materialized tables, Lightning Tables, Tableflow, and a streaming agent on Confluent Cloud, with measured results.
layout: landing
---

<section class="home-hero">
  <div class="home-hero__copy">
    <p class="eyebrow">A seven-part lab series</p>
    <h1>How real-time is a Streamhouse?</h1>
    <p class="home-hero__dek">I rebuilt Confluent's River Air Streamhouse as Terraform, version-controlled Flink SQL, and automated tests, then measured the path from a Kafka write to derived state, Iceberg history, and an AI agent.</p>
    <p class="home-hero__summary">The architecture worked. The surprise was time: in this setup, derived state reached Lightning Tables in <strong>44.3 seconds at p50</strong>, and most of the wait was Flink's exactly-once commit, which Confluent documents at about once a minute.</p>
    <div class="home-hero__actions">
      <a class="button button--primary" href="{{ '/01-the-lab.html' | relative_url }}">Start with the lab</a>
      <a class="button" href="{{ '/06-how-fresh-is-real-time.html' | relative_url }}">See the benchmarks</a>
    </div>
  </div>
  <aside class="home-hero__scope" aria-label="Experiment scope">
    <p class="section-label">Experiment scope</p>
    <dl>
      <div><dt>Environment</dt><dd>AWS <code>us-east-1</code></dd></div>
      <div><dt>Cluster</dt><dd>Confluent Basic</dd></div>
      <div><dt>Benchmark data</dt><dd>10,000 passengers</dd></div>
      <div><dt>Client</dt><dd>Seoul · ~190 ms away</dd></div>
      <div><dt>Run date</dt><dd>November 15, 2026</dd></div>
    </dl>
  </aside>
</section>


<section class="home-section home-section--metrics" aria-labelledby="measured-title">
  <div class="section-heading">
    <div>
      <p class="section-label">Measurements</p>
      <h2 id="measured-title">What I measured</h2>
    </div>
    <p>The first three figures come from the benchmarks in Chapter 6 and the Iceberg timing from Chapter 4. All of them describe one configuration: a Basic cluster in <code>us-east-1</code>, measured from Seoul. The chapters include the method, sample size, raw distributions, and limitations.</p>
  </div>
  <div class="metric-grid">
    <article class="metric-card metric-card--primary">
      <span class="metric-card__value">44.3<span>s</span></span>
      <strong>End-to-end freshness, p50</strong>
      <p>Broker acknowledgement to a derived row becoming visible in Lightning Tables, 30 samples.</p>
    </article>
    <article class="metric-card">
      <span class="metric-card__value">3–7<span>s</span></span>
      <strong>Commit to Lightning visibility</strong>
      <p>The observed range between broker commit visibility and the first successful Lightning query.</p>
    </article>
    <article class="metric-card">
      <span class="metric-card__value">40.6<span>req/s</span></span>
      <strong>At 32 concurrent clients</strong>
      <p>Point lookups over MCP for 30 seconds, with no request or rate-limit errors.</p>
    </article>
    <article class="metric-card">
      <span class="metric-card__value">4.5<span>min</span></span>
      <strong>First Iceberg data</strong>
      <p>From enabling Tableflow to all six source rows in Iceberg, on a low-throughput topic.</p>
    </article>
  </div>
</section>

<section class="home-section" aria-labelledby="architecture-section-title">
  <div class="section-heading">
    <div>
      <p class="section-label">Architecture</p>
      <h2 id="architecture-section-title">Kafka topics connect every layer</h2>
    </div>
    <p>Flink defines the business state once. Lightning serves that state to applications and agents, while Tableflow carries topic history into Iceberg.</p>
  </div>
  {% include architecture.html %}
</section>

<section class="home-section home-section--findings" aria-labelledby="findings-title">
  <div class="section-heading">
    <div>
      <p class="section-label">Findings</p>
      <h2 id="findings-title">What worked, and what to watch</h2>
    </div>
  </div>
  <div class="finding-columns">
    <article>
      <span class="finding-columns__mark finding-columns__mark--yes" aria-hidden="true">✓</span>
      <h3>Worked</h3>
      <ul>
        <li>All six capabilities matched the quick start's expected result.</li>
        <li>A Flink snapshot query combined Iceberg-only history with Kafka-only new rows.</li>
        <li>The managed agent returned the correct flights and hotel on its first run.</li>
      </ul>
    </article>
    <article>
      <span class="finding-columns__mark finding-columns__mark--caution" aria-hidden="true">!</span>
      <h3>Watch out for</h3>
      <ul>
        <li>Derived state arrived in tens of seconds, because exactly-once output commits about once a minute.</li>
        <li>A Basic cluster could not give the MCP reader least privilege.</li>
        <li>The official materialized-table query raised two CRITICAL warnings.</li>
      </ul>
    </article>
  </div>
</section>

<section class="home-section" aria-labelledby="chapters-title">
  <div class="section-heading">
    <div>
      <p class="section-label">Chapters</p>
      <h2 id="chapters-title">The series</h2>
    </div>
    <p>Read them in order, or start with the chapter you need.</p>
  </div>
  {% include chapter-grid.html %}
</section>

<section class="method-note" aria-labelledby="method-title">
  <div>
    <p class="section-label">Method</p>
    <h2 id="method-title">How the lab is built</h2>
  </div>
  <p>The lab uses Terraform, version-controlled SQL, separate benchmark tables, 137 offline unit tests, and six live tests that check each capability against a real deployment. Every measured chapter states its sample size and what the run could not prove. The environment was torn down between sessions and rebuilt from code.</p>
  {%- if site.repository_public %}
  <a href="{{ site.repository_url }}">Inspect the repository <span aria-hidden="true">→</span></a>
  {%- else %}
  <p class="method-note__aside">The lab's repository is private for now.</p>
  {%- endif %}
</section>
