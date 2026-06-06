import { defineConfig } from "vitepress";

function normalizeBase(value: string | undefined): string {
  const base = value?.trim() || "/";
  const prefixed = base.startsWith("/") ? base : `/${base}`;

  return prefixed.endsWith("/") ? prefixed : `${prefixed}/`;
}

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
      message: "Released under the MIT License.",
      copyright: "Copyright (c) Dai Akatsuka"
    }
  }
});
