<template>
  <figure class="ractor-sequence" aria-labelledby="ractor-sequence-title">
    <div class="ractor-sequence__scroller">
      <svg
        viewBox="0 0 1160 900"
        role="img"
        aria-labelledby="ractor-sequence-title ractor-sequence-desc"
      >
        <title id="ractor-sequence-title">
          Source.ractor_port acknowledgement sequence
        </title>
        <desc id="ractor-sequence-desc">
          Downstream demand causes Source.ractor_port to acknowledge the
          producer Ractor. The producer then sends one element or a terminal
          message through the data port.
        </desc>
        <defs>
          <marker
            id="ractor-sequence-arrow"
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
            id="ractor-sequence-arrow-muted"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" class="arrow-head-muted" />
          </marker>
        </defs>

        <rect class="panel" x="1" y="1" width="1158" height="898" rx="10" />

        <g transform="translate(36 30)">
          <rect class="section" width="260" height="34" rx="6" />
          <text class="section-text" x="16" y="23">One downstream pull</text>
        </g>

        <g class="participants">
          <g transform="translate(24 84)">
            <rect class="participant" width="160" height="52" rx="6" />
            <text class="participant-text" x="80" y="32" text-anchor="middle">
              Downstream
            </text>
          </g>
          <g transform="translate(238 84)">
            <rect class="participant" width="196" height="52" rx="6" />
            <text class="participant-text" x="98" y="32" text-anchor="middle">
              Source.ractor_port
            </text>
          </g>
          <g transform="translate(502 84)">
            <rect class="participant" width="156" height="52" rx="6" />
            <text class="participant-text" x="78" y="32" text-anchor="middle">
              Ack port
            </text>
          </g>
          <g transform="translate(726 84)">
            <rect class="participant" width="188" height="52" rx="6" />
            <text class="participant-text" x="94" y="32" text-anchor="middle">
              Producer Ractor
            </text>
          </g>
          <g transform="translate(976 84)">
            <rect class="participant" width="150" height="52" rx="6" />
            <text class="participant-text" x="75" y="32" text-anchor="middle">
              Data port
            </text>
          </g>
        </g>

        <line class="lifeline" x1="104" y1="136" x2="104" y2="860" />
        <line class="lifeline" x1="336" y1="136" x2="336" y2="860" />
        <line class="lifeline" x1="580" y1="136" x2="580" y2="860" />
        <line class="lifeline" x1="820" y1="136" x2="820" y2="860" />
        <line class="lifeline" x1="1051" y1="136" x2="1051" y2="860" />

        <g>
          <text class="step" x="48" y="176">1</text>
          <line class="message" x1="104" y1="170" x2="334" y2="170" />
          <text class="label" x="219" y="158" text-anchor="middle">next</text>

          <text class="step" x="268" y="236">2</text>
          <line class="message" x1="336" y1="230" x2="578" y2="230" />
          <text class="label" x="457" y="218" text-anchor="middle">
            RactorPort::Ack
          </text>

          <text class="step" x="622" y="296">3</text>
          <line class="message-muted" x1="580" y1="290" x2="818" y2="290" />
          <text class="label" x="699" y="278" text-anchor="middle">
            deliver ack
          </text>

          <text class="step" x="852" y="356">4</text>
          <line class="message" x1="820" y1="350" x2="1049" y2="350" />
          <text class="label" x="934" y="338" text-anchor="middle">
            RactorPort::Element(value)
          </text>

          <text class="step" x="950" y="416">5</text>
          <line class="message-muted" x1="1051" y1="410" x2="338" y2="410" />
          <text class="label" x="695" y="398" text-anchor="middle">
            deliver element
          </text>

          <text class="step" x="274" y="476">6</text>
          <line class="message-muted" x1="336" y1="470" x2="106" y2="470" />
          <text class="label" x="221" y="458" text-anchor="middle">
            emit value
          </text>
        </g>

        <g transform="translate(36 520)">
          <rect class="section" width="330" height="34" rx="6" />
          <text class="section-text" x="16" y="23">
            Next pull returns terminal state
          </text>
        </g>

        <g>
          <text class="step" x="48" y="596">7</text>
          <line class="message" x1="104" y1="590" x2="334" y2="590" />
          <text class="label" x="219" y="578" text-anchor="middle">next</text>

          <text class="step" x="268" y="656">8</text>
          <line class="message" x1="336" y1="650" x2="578" y2="650" />
          <text class="label" x="457" y="638" text-anchor="middle">
            RactorPort::Ack
          </text>

          <text class="step" x="622" y="716">9</text>
          <line class="message-muted" x1="580" y1="710" x2="818" y2="710" />
          <text class="label" x="699" y="698" text-anchor="middle">
            deliver ack
          </text>

          <text class="step" x="846" y="776">10</text>
          <line class="message" x1="820" y1="770" x2="1049" y2="770" />
          <text class="label" x="934" y="758" text-anchor="middle">
            Complete or Failure
          </text>

          <text class="step" x="944" y="836">11</text>
          <line class="message-muted" x1="1051" y1="830" x2="338" y2="830" />
          <text class="label" x="695" y="818" text-anchor="middle">
            deliver terminal message
          </text>

          <text class="step" x="268" y="876">12</text>
          <line class="message-muted" x1="336" y1="870" x2="106" y2="870" />
          <text class="label" x="221" y="858" text-anchor="middle">
            complete or raise
          </text>
        </g>
      </svg>
    </div>
  </figure>
</template>

<style scoped>
.ractor-sequence {
  --diagram-bg: var(--vp-c-bg-soft);
  --diagram-box-bg: var(--vp-c-bg);
  --diagram-section-bg: var(--vp-c-brand-soft);
  --diagram-border: var(--vp-c-divider);
  --diagram-border-strong: var(--vp-c-brand-2);
  --diagram-text: var(--vp-c-text-1);
  --diagram-muted: var(--vp-c-text-2);
  --diagram-line: var(--vp-c-text-2);
  --diagram-line-muted: var(--vp-c-text-3);

  margin: 24px 0 28px;
}

.ractor-sequence__scroller {
  overflow-x: auto;
  padding-bottom: 4px;
}

.ractor-sequence svg {
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
  marker-end: url("#ractor-sequence-arrow");
  stroke: var(--diagram-line);
  stroke-width: 2;
}

.message-muted {
  marker-end: url("#ractor-sequence-arrow-muted");
  stroke: var(--diagram-line-muted);
  stroke-dasharray: 7 7;
  stroke-width: 2;
}

.arrow-head {
  fill: var(--diagram-line);
}

.arrow-head-muted {
  fill: var(--diagram-line-muted);
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
