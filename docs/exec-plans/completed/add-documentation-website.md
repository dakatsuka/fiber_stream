# Add Documentation Website

## Status

Completed

## Objective

Add an official user-facing FiberStream documentation website with concise
English guide, tutorial, and reference content.

## Context

- Repository agent guide: `AGENTS.md`
- README public API surface: `README.md`
- Product specs: `docs/product-specs/`
- Design docs: `docs/design-docs/`
- VitePress documentation for static docs sites, default theme navigation,
  i18n-ready structure, and GitHub Pages deployment.

## Clarifications

- Initial site content is English only.
- The site should be suitable for official documentation and reference use.
- Copy should be direct, technical, and free of slang, filler, or verbose
  generated-sounding prose.
- Japanese pages may be added later, so paths and configuration should not
  block future VitePress locale support.

## Contract First

No Ruby public API changes are planned.

Website contracts:

- The site source lives under `website/` so repository-local design and product
  documents remain internal source-of-truth artifacts.
- The content root is `website/docs/`.
- Initial information architecture includes:
  - `guide/`
  - `tutorials/`
  - `reference/`
- The VitePress `base` value is configurable through `VITEPRESS_BASE`, with
  `/` as the local and custom-domain default.
- GitHub Pages deployment uses a dedicated docs workflow and does not alter
  the Ruby CI or RubyGems release workflows.
- The docs workflow builds on pull requests and pushes that touch website
  files. Deployment is gated to pushes to `main`.
- The docs workflow uses `npm ci`, a committed `package-lock.json`, a pinned
  Node version, and npm cache keyed by `website/package-lock.json`.
- The docs workflow reads `VITEPRESS_BASE` from a repository variable and falls
  back to `/fiber_stream/` for GitHub project Pages.
- VitePress config normalizes `VITEPRESS_BASE` so values start and end with
  `/`.
- English remains the root locale. A later Japanese locale should use `/ja/`
  without moving English URLs.
- Reference pages follow public RBS signatures and source comments. Tutorial
  pages use existing examples as source material or include snippets that are
  covered by smoke checks where practical.
- Diagram content should not add a large runtime renderer to every page unless
  the docs need interactive rendering. Inline SVG is preferred for a single
  tutorial diagram so it can use VitePress theme colors.

## Steps

- [x] Explore: inspect repository docs, workflows, release state, and current
      public API.
- [x] Design review: request context-free review and incorporate justified
      feedback.
- [x] Red: define build and content verification commands before scaffold work.
- [x] Green: scaffold the VitePress site, navigation, workflow, and initial
      English pages.
- [x] Refactor: tighten structure and copy while keeping the site build green.
- [x] Static checks: run the docs build and the repository default checks.
- [x] Code review: request context-free review after implementation.
- [x] Re-review: fix review findings and repeat review until it passes.

## Decisions

- Use VitePress rather than Docusaurus for the first site because FiberStream
  needs a compact Markdown-first documentation site, not multi-version docs
  infrastructure yet.
- Keep public docs separate from `docs/product-specs/` and
  `docs/design-docs/`. Those documents remain implementation and planning
  source material; website pages are written for library users.
- Start with one English root locale. Add Japanese later through VitePress
  `locales` and a `/ja/` subtree.
- Use GitHub Actions for Pages deployment because the site needs a Node build
  step and GitHub recommends Actions for custom build processes.
- Use an inline SVG Vue component for the `Source.ractor_port` sequence
  diagram. VitePress can render Mermaid through plugins or Vue components, but
  a single runtime Mermaid diagram adds a large client chunk to pages that do
  not need it.
- Accept design-review feedback to add PR docs builds, official Pages actions,
  dependency locking, `base` normalization, and source-of-truth rules for
  public snippets.
- Exclude Node dependency and VitePress output directories from RuboCop because
  installed npm packages can contain unrelated Ruby files.

## Third-Party Review

Context-free design review requested before implementation. Accepted feedback:

- Add pull-request docs builds and gate deployment to push events.
- Use the official GitHub Pages action sequence with Pages permissions,
  environment, concurrency, and artifact upload from the VitePress output path.
- Avoid a hard-coded Pages base by reading a repository variable and applying a
  `/fiber_stream/` default in the workflow.
- Normalize `VITEPRESS_BASE` in VitePress config.
- Commit `package-lock.json`, use `npm ci`, pin Node, and configure npm cache.
- Record that English root URLs remain stable and later Japanese docs should
  live under `/ja/`.
- Treat RBS, source comments, README, specs, and existing examples as source
  material to reduce public docs drift.

## Verification

Commands run:

```sh
cd website && npm ci
cd website && npm run docs:build
cd website && VITEPRESS_BASE=/fiber_stream/ npm run docs:build
cd website && VITEPRESS_BASE=fiber_stream npm run docs:build
cd website && npm audit --omit=dev
cd website && npm audit --audit-level=moderate
bundle exec rake
bundle exec ruby examples/basic_pipeline.rb
bundle exec ruby examples/file_copy.rb
bundle exec ruby examples/ractor_map_hashing.rb
```

Additional checks:

- Markdown internal link check across `website/docs/**/*.md`.
- Generated output check for the Ractor Source inline diagram and absence of
  Mermaid runtime assets.
- VitePress build commands were run sequentially because concurrent builds
  share `website/docs/.vitepress/.temp`.
- `npm audit --audit-level=moderate` reports three moderate dev dependency
  advisories through the current latest VitePress 1.6.4 dependency chain. npm
  reports no fix available. The affected path is the Vite/esbuild development
  server dependency chain, not the static GitHub Pages output.

## Completion Notes

- Added a VitePress documentation site under `website/`.
- Added English root-locale pages for guide, tutorial, and reference content.
- Added a GitHub Pages docs workflow that builds on pull requests and deploys
  only on pushes to `main`.
- Kept repository-internal design and product documents separate from
  user-facing website content.
- Fixed code-review findings around public cancellation errors and
  non-blocking fiber requirements in reference pages.
- Added a `Source.ractor_port` sequence diagram to the Ractor Source tutorial
  as an inline SVG component that follows VitePress light and dark themes.
- Added a pull-based backpressure sequence diagram to the Backpressure guide
  using the same inline SVG approach.
- Remaining dependency risk: the current latest VitePress release reports a
  moderate dev-server advisory through its Vite/esbuild chain with no fix
  available. Static Pages output and production dependencies are unaffected.

## Commit

Not committed in this turn. Suggested message: `docs: add VitePress documentation website`.
