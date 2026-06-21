import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitepress";

const configDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(configDir, "../../..");

function normalizeBase(value: string | undefined): string {
  const base = value?.trim() || "/";
  const prefixed = base.startsWith("/") ? base : `/${base}`;

  return prefixed.endsWith("/") ? prefixed : `${prefixed}/`;
}

function readLibraryVersion(): string {
  const versionFile = resolve(repoRoot, "lib/fiber_stream/version.rb");
  const versionSource = readFileSync(versionFile, "utf8");
  const match = versionSource.match(/VERSION\s*=\s*"([^"]+)"/);

  if (!match) {
    throw new Error(`Unable to read FiberStream version from ${versionFile}`);
  }

  return match[1];
}

const libraryVersion = readLibraryVersion();
const versionLabel = `v${libraryVersion}`;

export default defineConfig({
  lang: "en-US",
  title: "FiberStream",
  description: "Ruby stream processing with pull-based backpressure.",
  base: normalizeBase(process.env.VITEPRESS_BASE),
  cleanUrls: true,
  lastUpdated: true,
  themeConfig: {
    siteTitle: "FiberStream",
    search: {
      provider: "local"
    },
    outline: {
      level: [2, 3]
    },
    nav: [
      { text: "Guide", link: "/guide/getting-started" },
      { text: "Tutorials", link: "/tutorials/basic-pipeline" },
      { text: "Reference", link: "/reference/source" },
      {
        text: versionLabel,
        link: `https://github.com/dakatsuka/fiber_stream/releases/tag/${versionLabel}`
      },
      { text: "Changelog", link: "https://github.com/dakatsuka/fiber_stream/blob/main/CHANGELOG.md" }
    ],
    sidebar: {
      "/guide/": [
        {
          text: "Guide",
          items: [
            { text: "Getting Started", link: "/guide/getting-started" },
            { text: "Core Concepts", link: "/guide/core-concepts" },
            { text: "Backpressure", link: "/guide/backpressure" }
          ]
        }
      ],
      "/tutorials/": [
        {
          text: "Tutorials",
          items: [
            { text: "Basic Pipeline", link: "/tutorials/basic-pipeline" },
            { text: "File Copy", link: "/tutorials/file-copy" },
            { text: "Async HTTP", link: "/tutorials/async-http" },
            { text: "Rate Limiting", link: "/tutorials/rate-limiting" },
            { text: "Ractor Source", link: "/tutorials/ractor-source" },
            { text: "Ractor Map", link: "/tutorials/ractor-map" }
          ]
        }
      ],
      "/reference/": [
        {
          text: "Reference",
          items: [
            { text: "Source", link: "/reference/source" },
            { text: "Flow", link: "/reference/flow" },
            { text: "RateLimiter", link: "/reference/rate-limiter" },
            { text: "Sink", link: "/reference/sink" },
            { text: "Pipeline", link: "/reference/pipeline" },
            { text: "Errors", link: "/reference/errors" }
          ]
        }
      ]
    },
    socialLinks: [
      { icon: "github", link: "https://github.com/dakatsuka/fiber_stream" }
    ],
    editLink: {
      pattern: "https://github.com/dakatsuka/fiber_stream/edit/main/website/docs/:path",
      text: "Edit this page on GitHub"
    },
    footer: {
      message: `${versionLabel}. Released under the MIT License.`,
      copyright: "Copyright (c) Dai Akatsuka"
    }
  }
});
