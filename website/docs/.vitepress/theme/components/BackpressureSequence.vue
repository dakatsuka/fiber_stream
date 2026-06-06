<template>
  <figure class="backpressure-sequence" aria-labelledby="backpressure-sequence-title">
    <div class="backpressure-sequence__scroller">
      <svg
        viewBox="0 0 1160 990"
        role="img"
        aria-labelledby="backpressure-sequence-title backpressure-sequence-desc"
      >
        <title id="backpressure-sequence-title">
          Pull-based backpressure sequence
        </title>
        <desc id="backpressure-sequence-desc">
          A sink pull travels upstream through flow stages to the source. The
          source produces one element only after demand reaches it. When take(2)
          receives its second element, it closes upstream before returning that
          element downstream.
        </desc>
        <defs>
          <marker
            id="backpressure-sequence-arrow"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" class="arrow-head" />
          </marker>
          <marker
            id="backpressure-sequence-arrow-muted"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" class="arrow-head-muted" />
          </marker>
          <marker
            id="backpressure-sequence-arrow-close"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" class="arrow-head-close" />
          </marker>
        </defs>

        <rect class="panel" x="1" y="1" width="1158" height="988" rx="10" />

        <g transform="translate(36 30)">
          <rect class="section" width="300" height="34" rx="6" />
          <text class="section-text" x="16" y="23">Demand travels upstream</text>
        </g>

        <g class="participants">
          <g transform="translate(28 84)">
            <rect class="participant" width="152" height="52" rx="6" />
            <text class="participant-text" x="76" y="32" text-anchor="middle">
              Sink
            </text>
          </g>
          <g transform="translate(252 84)">
            <rect class="participant" width="180" height="52" rx="6" />
            <text class="participant-text" x="90" y="32" text-anchor="middle">
              Flow.take(2)
            </text>
          </g>
          <g transform="translate(536 84)">
            <rect class="participant" width="172" height="52" rx="6" />
            <text class="participant-text" x="86" y="32" text-anchor="middle">
              Flow.map
            </text>
          </g>
          <g transform="translate(906 84)">
            <rect class="participant" width="190" height="52" rx="6" />
            <text class="participant-text" x="95" y="32" text-anchor="middle">
              Source.each
            </text>
          </g>
        </g>

        <line class="lifeline" x1="104" y1="136" x2="104" y2="960" />
        <line class="lifeline" x1="342" y1="136" x2="342" y2="960" />
        <line class="lifeline" x1="622" y1="136" x2="622" y2="960" />
        <line class="lifeline" x1="1001" y1="136" x2="1001" y2="960" />

        <g>
          <text class="step" x="50" y="176">1</text>
          <line class="message" x1="104" y1="170" x2="340" y2="170" />
          <text class="label" x="222" y="158" text-anchor="middle">next</text>

          <text class="step" x="276" y="231">2</text>
          <line class="message" x1="342" y1="225" x2="620" y2="225" />
          <text class="label" x="481" y="213" text-anchor="middle">next</text>

          <text class="step" x="558" y="286">3</text>
          <line class="message" x1="622" y1="280" x2="999" y2="280" />
          <text class="label" x="811" y="268" text-anchor="middle">next</text>

          <text class="step" x="906" y="341">4</text>
          <line class="message-muted" x1="1001" y1="335" x2="624" y2="335" />
          <text class="label" x="812" y="323" text-anchor="middle">element 1</text>

          <text class="step" x="560" y="396">5</text>
          <line class="message-muted" x1="622" y1="390" x2="344" y2="390" />
          <text class="label" x="483" y="378" text-anchor="middle">
            mapped element
          </text>

          <text class="step" x="276" y="451">6</text>
          <line class="message-muted" x1="342" y1="445" x2="106" y2="445" />
          <text class="label" x="224" y="433" text-anchor="middle">
            emit element
          </text>
        </g>

        <g transform="translate(36 515)">
          <rect class="section" width="500" height="34" rx="6" />
          <text class="section-text" x="16" y="23">
            Second element reaches take(2); upstream closes before emit
          </text>
        </g>

        <g>
          <text class="step" x="50" y="586">7</text>
          <line class="message" x1="104" y1="580" x2="340" y2="580" />
          <text class="label" x="222" y="568" text-anchor="middle">next</text>

          <text class="step" x="276" y="641">8</text>
          <line class="message" x1="342" y1="635" x2="620" y2="635" />
          <text class="label" x="481" y="623" text-anchor="middle">next</text>

          <text class="step" x="558" y="696">9</text>
          <line class="message" x1="622" y1="690" x2="999" y2="690" />
          <text class="label" x="811" y="678" text-anchor="middle">next</text>

          <text class="step" x="900" y="751">10</text>
          <line class="message-muted" x1="1001" y1="745" x2="624" y2="745" />
          <text class="label" x="812" y="733" text-anchor="middle">element 2</text>

          <text class="step" x="554" y="806">11</text>
          <line class="message-muted" x1="622" y1="800" x2="344" y2="800" />
          <text class="label" x="483" y="788" text-anchor="middle">
            mapped element
          </text>

          <text class="step" x="270" y="861">12</text>
          <line class="close-message" x1="342" y1="855" x2="620" y2="855" />
          <text class="label" x="481" y="843" text-anchor="middle">
            close upstream
          </text>

          <text class="step" x="554" y="916">13</text>
          <line class="close-message" x1="622" y1="910" x2="999" y2="910" />
          <text class="label" x="811" y="898" text-anchor="middle">
            close source before element 3
          </text>

          <text class="step" x="270" y="961">14</text>
          <line class="message-muted" x1="342" y1="955" x2="106" y2="955" />
          <text class="label" x="224" y="943" text-anchor="middle">
            emit final element
          </text>
        </g>
      </svg>
    </div>
  </figure>
</template>

<style scoped>
.backpressure-sequence {
  --diagram-bg: var(--vp-c-bg-soft);
  --diagram-box-bg: var(--vp-c-bg);
  --diagram-section-bg: var(--vp-c-brand-soft);
  --diagram-border: var(--vp-c-divider);
  --diagram-border-strong: var(--vp-c-brand-2);
  --diagram-text: var(--vp-c-text-1);
  --diagram-muted: var(--vp-c-text-2);
  --diagram-line: var(--vp-c-text-2);
  --diagram-line-muted: var(--vp-c-text-3);
  --diagram-close: var(--vp-c-danger-1);

  margin: 24px 0 28px;
}

.backpressure-sequence__scroller {
  overflow-x: auto;
  padding-bottom: 4px;
}

.backpressure-sequence svg {
  display: block;
  height: auto;
  min-width: 860px;
  width: 100%;
}

.panel {
  fill: var(--diagram-bg);
  stroke: var(--diagram-border);
  stroke-width: 1.5;
}

.participant {
  fill: var(--diagram-box-bg);
  stroke: var(--diagram-border);
  stroke-width: 1.5;
}

.section {
  fill: var(--diagram-section-bg);
  stroke: var(--diagram-border-strong);
  stroke-width: 1;
}

.participant-text {
  fill: var(--diagram-text);
  font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

.section-text {
  fill: var(--diagram-text);
  font: 700 14px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

.lifeline {
  stroke: var(--diagram-border);
  stroke-dasharray: 7 8;
  stroke-width: 1.5;
}

.message {
  marker-end: url("#backpressure-sequence-arrow");
  stroke: var(--diagram-line);
  stroke-width: 2;
}

.message-muted {
  marker-end: url("#backpressure-sequence-arrow-muted");
  stroke: var(--diagram-line-muted);
  stroke-dasharray: 7 7;
  stroke-width: 2;
}

.close-message {
  marker-end: url("#backpressure-sequence-arrow-close");
  stroke: var(--diagram-close);
  stroke-width: 2;
}

.arrow-head {
  fill: var(--diagram-line);
}

.arrow-head-muted {
  fill: var(--diagram-line-muted);
}

.arrow-head-close {
  fill: var(--diagram-close);
}

.label {
  fill: var(--diagram-text);
  font: 14px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

.step {
  fill: var(--diagram-muted);
  font: 700 13px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
</style>
