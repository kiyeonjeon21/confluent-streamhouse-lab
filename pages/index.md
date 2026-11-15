---
title: Building a Streamhouse on Confluent Cloud
description: A hands-on series on Kafka, Flink materialized tables, Lightning Tables, Tableflow, and a streaming agent on Confluent Cloud, with measured results.
layout: landing
---

<section class="home-hero">
  <div class="home-hero__copy">
    <p class="eyebrow">Seven chapters · One reproducible lab · Measured from Seoul</p>
    <h1>How real-time is a Streamhouse?</h1>
    <p class="home-hero__dek">I rebuilt Confluent's River Air Streamhouse as Terraform, version-controlled Flink SQL, and automated tests—then measured the path from a Kafka write to derived state, Iceberg history, and an AI agent.</p>
    <p class="home-hero__summary">The architecture worked. The surprise was time: derived state reached Lightning Tables in <strong>44.3 seconds at p50</strong>, with most of the wait inside Flink's transaction commit.</p>
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
      <div><dt>Dataset</dt><dd>10,000 passengers</dd></div>
      <div><dt>Client</dt><dd>Seoul · ~190 ms away</dd></div>
      <div><dt>Run date</dt><dd>November 15, 2026</dd></div>
    </dl>
  </aside>
</section>

<nav class="reader-paths" aria-labelledby="reader-paths-title">
  <div class="reader-paths__heading">
    <p class="section-label">Choose a reading path</p>
    <h2 id="reader-paths-title">Start with the question you need answered</h2>
  </div>
  <div class="reader-paths__links">
    <a href="{{ '/01-the-lab.html' | relative_url }}"><span>Understand the system</span><strong>Architecture and reproducible lab</strong></a>
    <a href="{{ '/02-basic-clusters-and-read-only-agents.html' | relative_url }}"><span>Plan safe operations</span><strong>Security and runtime constraints</strong></a>
    <a href="{{ '/06-how-fresh-is-real-time.html' | relative_url }}"><span>Inspect the evidence</span><strong>Freshness, latency, and throughput</strong></a>
    <a href="{{ '/07-is-a-streamhouse-worth-it.html' | relative_url }}"><span>Make a decision</span><strong>Fit, trade-offs, and open questions</strong></a>
  </div>
</nav>

<section class="home-section home-section--metrics" aria-labelledby="measured-title">
  <div class="section-heading">
    <div>
      <p class="section-label">Measured, not estimated</p>
      <h2 id="measured-title">The numbers that changed the conclusion</h2>
    </div>
    <p>Each figure comes from the experiments in Chapter 6. The article includes the method, sample size, raw distributions, and limitations.</p>
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
      <p>A low-throughput topic reached its third snapshot with all six source rows.</p>
    </article>
  </div>
</section>

<section class="home-section" aria-labelledby="architecture-section-title">
  <div class="section-heading">
    <div>
      <p class="section-label">Architecture</p>
      <h2 id="architecture-section-title">Current state and history share Kafka as the interface</h2>
    </div>
    <p>Flink defines the business state once. Lightning serves that state to applications and agents, while Tableflow carries topic history into Iceberg.</p>
  </div>
  {% include architecture.html %}
</section>

<section class="home-section home-section--findings" aria-labelledby="findings-title">
  <div class="section-heading">
    <div>
      <p class="section-label">What held up</p>
      <h2 id="findings-title">A useful architecture with sharp operational edges</h2>
    </div>
  </div>
  <div class="finding-columns">
    <article>
      <span class="finding-columns__mark finding-columns__mark--yes" aria-hidden="true">✓</span>
      <h3>The core path worked</h3>
      <ul>
        <li>All six capabilities matched the quick start's expected result.</li>
        <li>A Flink snapshot query combined Iceberg-only history with Kafka-only new rows.</li>
        <li>The managed agent returned the correct flights and hotel on its first run.</li>
      </ul>
    </article>
    <article>
      <span class="finding-columns__mark finding-columns__mark--caution" aria-hidden="true">!</span>
      <h3>The constraints matter</h3>
      <ul>
        <li>Derived state arrived in tens of seconds, not sub-second time.</li>
        <li>A Basic cluster could not give the MCP reader least privilege.</li>
        <li>The official materialized-table query raised two CRITICAL warnings.</li>
      </ul>
    </article>
  </div>
</section>

<section class="home-section" aria-labelledby="chapters-title">
  <div class="section-heading">
    <div>
      <p class="section-label">The field notes</p>
      <h2 id="chapters-title">Seven chapters, from build to verdict</h2>
    </div>
    <p>Read in order for the full investigation, or jump directly to the implementation detail or benchmark you need.</p>
  </div>
  {% include chapter-grid.html %}
</section>

<section class="method-note" aria-labelledby="method-title">
  <div>
    <p class="section-label">Method</p>
    <h2 id="method-title">Built to be challenged and repeated</h2>
  </div>
  <p>The lab uses Terraform, version-controlled SQL, about 140 automated tests, and separate benchmark tables. Every measured chapter states its sample size and what the run could not prove. The environment was torn down between sessions and can be rebuilt from the repository.</p>
  <a href="{{ site.repository_url }}">Inspect the repository <span aria-hidden="true">→</span></a>
</section>
