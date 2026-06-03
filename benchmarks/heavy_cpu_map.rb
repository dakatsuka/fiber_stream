# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "digest"
require "fiber_stream"
require "fileutils"
require "optparse"

options = {
  items: 1_600,
  work: 1_000,
  concurrency: 4,
  workers: [2, 4],
  svg: "benchmarks/results/heavy_cpu_map.svg",
  csv: "benchmarks/results/heavy_cpu_map.csv"
}

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby benchmarks/heavy_cpu_map.rb [options]"
  parser.on("--items COUNT", Integer, "Input item count") { |value| options[:items] = value }
  parser.on("--work COUNT", Integer, "SHA-256 rounds per item") { |value| options[:work] = value }
  parser.on("--concurrency COUNT", Integer, "Async and parallel_map concurrency") do |value|
    options[:concurrency] = value
  end
  parser.on("--workers LIST", String, "Comma-separated ractor_map worker counts") do |value|
    options[:workers] = value.split(",").map { |item| Integer(item, 10) }
  end
  parser.on("--svg PATH", String, "SVG chart output path") { |value| options[:svg] = value }
  parser.on("--csv PATH", String, "CSV output path") { |value| options[:csv] = value }
end.parse!

HEAVY_DIGEST =
  Ractor.shareable_proc do |input|
    value, work = input
    text = "#{value}:#{value * 65_537}"
    work.times.reduce(text) { |digest, _| Digest::SHA256.hexdigest(digest) }
  end

def checksum(digests)
  digests.sum { |digest| digest.getbyte(0) }
end

def run_case(label)
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  result = yield
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  [label, elapsed, result]
end

def xml_escape(value)
  value.to_s
    .gsub("&", "&amp;")
    .gsub("<", "&lt;")
    .gsub(">", "&gt;")
    .gsub('"', "&quot;")
end

def write_csv(path, results)
  FileUtils.mkdir_p(File.dirname(path))
  rows = results.map { |label, elapsed, checksum| "#{label},#{elapsed},#{checksum}" }
  File.write(path, ["case,seconds,checksum", *rows].join("\n") << "\n")
end

def svg_text(x:, y:, size:, fill:, content:, weight: nil)
  weight_attr = weight ? %( font-weight="#{weight}") : ""

  %(<text x="#{x}" y="#{y}" font-family="Arial, sans-serif" ) +
    %(font-size="#{size}"#{weight_attr} fill="#{fill}">#{xml_escape(content)}</text>)
end

def svg_rect(x:, y:, width:, height:, fill:, rx: nil)
  rx_attr = rx ? %( rx="#{rx}") : ""

  %(<rect x="#{x}" y="#{y}" width="#{width}" height="#{height}"#{rx_attr} fill="#{fill}"/>)
end

def write_svg(path, results, title:, subtitle:)
  FileUtils.mkdir_p(File.dirname(path))

  width = 920
  row_height = 44
  top = 78
  left = 250
  right = 160
  chart_width = width - left - right
  height = top + (results.length * row_height) + 52
  max_elapsed = results.map { |(_, elapsed, _)| elapsed }.max
  palette = ["#2667ff", "#2a9d8f", "#e76f51", "#8f5bd5", "#f4a261", "#5f6c7b", "#0f766e"]

  lines = []
  lines << %(<?xml version="1.0" encoding="UTF-8"?>)
  lines << %(<svg xmlns="http://www.w3.org/2000/svg" width="#{width}" ) +
           %(height="#{height}" viewBox="0 0 #{width} #{height}">)
  lines << %(<rect width="100%" height="100%" fill="#ffffff"/>)
  lines << svg_text(x: 32, y: 34, size: 22, fill: "#111827", content: title, weight: 700)
  lines << svg_text(x: 32, y: 58, size: 13, fill: "#4b5563", content: subtitle)
  lines << %(<line x1="#{left}" y1="#{top - 18}" x2="#{left + chart_width}" y2="#{top - 18}" stroke="#d1d5db"/>)

  results.each_with_index do |(label, elapsed, _), index|
    y = top + (index * row_height)
    bar_width = [(elapsed / max_elapsed) * chart_width, 1].max
    color = palette.fetch(index % palette.length)

    lines << svg_text(x: 32, y: y + 24, size: 14, fill: "#111827", content: label)
    lines << svg_rect(x: left, y: y + 8, width: bar_width.round(1), height: 24, fill: color, rx: 4)
    lines << svg_text(
      x: left + bar_width + 10,
      y: y + 25,
      size: 13,
      fill: "#111827",
      content: format("%.3fs", elapsed)
    )
  end

  lines << svg_text(
    x: 32,
    y: height - 22,
    size: 12,
    fill: "#6b7280",
    content: "Lower is faster. Ractor workers can run CPU-bound Ruby mapping work in parallel."
  )
  lines << %(</svg>)

  File.write(path, lines.join("\n") << "\n")
end

values = (1..options.fetch(:items)).map { |value| [value, options.fetch(:work)] }
results = []

results << run_case("Enumerable serial") do
  checksum(values.map { |value| HEAVY_DIGEST.call(value) })
end

results << run_case("Enumerable::Lazy forced") do
  checksum(values.lazy.map { |value| HEAVY_DIGEST.call(value) }.force)
end

results << run_case("Direct Async tasks") do
  Sync do
    digests =
      values.each_slice(options.fetch(:concurrency)).flat_map do |slice|
        slice.map { |value| Async { HEAVY_DIGEST.call(value) } }.map(&:wait)
      end
    checksum(digests)
  end
end

results << run_case("FiberStream linear") do
  FiberStream::Source.each(values)
    .map { |value| HEAVY_DIGEST.call(value) }
    .run_with(FiberStream::Sink.fold(0) { |sum, digest| sum + digest.getbyte(0) })
end

results << run_case("FiberStream parallel_map") do
  Sync do
    FiberStream::Source.each(values)
      .parallel_map(concurrency: options.fetch(:concurrency)) { |value| HEAVY_DIGEST.call(value) }
      .run_with(FiberStream::Sink.fold(0) { |sum, digest| sum + digest.getbyte(0) })
  end
end

options.fetch(:workers).each do |workers|
  results << run_case("FiberStream ractor_map #{workers}") do
    FiberStream::Source.each(values)
      .ractor_map(workers: workers, &HEAVY_DIGEST)
      .run_with(FiberStream::Sink.fold(0) { |sum, digest| sum + digest.getbyte(0) })
  end
end

expected = results.fetch(0).fetch(2)
results.each do |label, _, result|
  raise "#{label} produced a different checksum" unless result == expected
end

fastest = results.map { |(_, elapsed, _)| elapsed }.min

puts "Ruby #{RUBY_VERSION}"
puts "items=#{options.fetch(:items)} work=#{options.fetch(:work)} " \
     "concurrency=#{options.fetch(:concurrency)} workers=#{options.fetch(:workers).join(',')}"
puts
puts format("%-30s %10s %10s %10s", "case", "seconds", "slower", "checksum")
puts "-" * 68
results.each do |label, elapsed, result|
  puts format("%-30s %10.4f %9.2fx %10d", label, elapsed, elapsed / fastest, result)
end

write_csv(options.fetch(:csv), results)
write_svg(
  options.fetch(:svg),
  results,
  title: "Heavy CPU map benchmark",
  subtitle: "Ruby #{RUBY_VERSION}; items=#{options.fetch(:items)}; SHA-256 rounds/item=#{options.fetch(:work)}"
)

puts
puts "Wrote #{options.fetch(:csv)}"
puts "Wrote #{options.fetch(:svg)}"
