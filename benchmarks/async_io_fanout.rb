# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"
require "fileutils"
require "optparse"
require "socket"

options = {
  requests: 24,
  delay: 0.03,
  concurrency: 8,
  payload_size: 256,
  svg: "benchmarks/results/async_io_fanout.svg",
  csv: "benchmarks/results/async_io_fanout.csv"
}

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby benchmarks/async_io_fanout.rb [options]"
  parser.on("--requests COUNT", Integer, "Request count") { |value| options[:requests] = value }
  parser.on("--delay SECONDS", Float, "Per-request server delay") { |value| options[:delay] = value }
  parser.on("--concurrency COUNT", Integer, "Async and FiberStream concurrency") do |value|
    options[:concurrency] = value
  end
  parser.on("--payload-size BYTES", Integer, "Response body size") { |value| options[:payload_size] = value }
  parser.on("--svg PATH", String, "SVG chart output path") { |value| options[:svg] = value }
  parser.on("--csv PATH", String, "CSV output path") { |value| options[:csv] = value }
end.parse!

Request = Data.define(:id, :path)
Response = Data.define(:id, :status, :body_bytes)

def monotonic_time
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def run_case(label)
  started_at = monotonic_time
  result = yield
  elapsed = monotonic_time - started_at
  [label, elapsed, result]
end

def handle_connection(socket, delay, payload_size)
  request = socket.readpartial(1024)
  id = request[/GET\s+\/items\/(\d+)/, 1] || "unknown"
  body = "#{id}:".ljust(payload_size, "x")

  sleep delay

  socket.write(
    "HTTP/1.1 200 OK\r\n" \
    "Content-Type: text/plain\r\n" \
    "Content-Length: #{body.bytesize}\r\n" \
    "Connection: close\r\n\r\n" \
    "#{body}"
  )
ensure
  socket&.close
end

def run_server(server, delay, payload_size)
  loop do
    socket = server.accept
    Async { handle_connection(socket, delay, payload_size) }
  rescue IOError, Errno::EBADF
    break
  end
end

def http_get(host, port, request)
  raw_response = +""
  socket = TCPSocket.new(host, port)

  socket.write(
    "GET #{request.path} HTTP/1.1\r\n" \
    "Host: #{host}:#{port}\r\n" \
    "Connection: close\r\n\r\n"
  )

  loop do
    raw_response << socket.readpartial(1024)
  rescue EOFError
    break
  end

  headers, body = raw_response.split("\r\n\r\n", 2)
  status = Integer(headers.lines.first.split.fetch(1), 10)
  Response.new(id: request.id, status: status, body_bytes: body.bytesize)
ensure
  socket&.close
end

def checksum(responses)
  responses.sum { |response| (response.id * 31) + response.status + response.body_bytes }
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
  left = 270
  right = 160
  chart_width = width - left - right
  height = top + (results.length * row_height) + 52
  max_elapsed = results.map { |(_, elapsed, _)| elapsed }.max
  palette = ["#2667ff", "#2a9d8f", "#e76f51", "#8f5bd5", "#f4a261", "#5f6c7b"]

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
    content: "Lower is faster. FiberScheduler can overlap non-blocking IO waits without Ractors."
  )
  lines << %(</svg>)

  File.write(path, lines.join("\n") << "\n")
end

requests = (1..options.fetch(:requests)).map { |id| Request.new(id: id, path: "/items/#{id}") }
delay = options.fetch(:delay)
payload_size = options.fetch(:payload_size)
results = []

Sync do
  server = TCPServer.new("127.0.0.1", 0)
  host = "127.0.0.1"
  port = server.addr.fetch(1)
  server_task = Async { run_server(server, delay, payload_size) }

  begin
    results << run_case("Enumerable serial") do
      checksum(requests.map { |request| http_get(host, port, request) })
    end

    results << run_case("Enumerable::Lazy forced") do
      checksum(requests.lazy.map { |request| http_get(host, port, request) }.force)
    end

    results << run_case("Direct Async tasks") do
      responses =
        requests.each_slice(options.fetch(:concurrency)).flat_map do |slice|
          slice.map { |request| Async { http_get(host, port, request) } }.map(&:wait)
        end
      checksum(responses)
    end

    results << run_case("FiberStream linear") do
      FiberStream::Source.each(requests)
        .map { |request| http_get(host, port, request) }
        .run_with(FiberStream::Sink.fold(0) { |sum, response| sum + checksum([response]) })
    end

    results << run_case("FiberStream parallel_map") do
      FiberStream::Source.each(requests)
        .parallel_map(concurrency: options.fetch(:concurrency)) { |request| http_get(host, port, request) }
        .run_with(FiberStream::Sink.fold(0) { |sum, response| sum + checksum([response]) })
    end
  ensure
    server.close
    server_task.stop
  end
end

expected = results.fetch(0).fetch(2)
results.each do |label, _, result|
  raise "#{label} produced a different checksum" unless result == expected
end

fastest = results.map { |(_, elapsed, _)| elapsed }.min

puts "Ruby #{RUBY_VERSION}"
puts "requests=#{options.fetch(:requests)} delay=#{delay}s concurrency=#{options.fetch(:concurrency)} " \
     "payload_size=#{payload_size}"
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
  title: "Async IO fan-out benchmark",
  subtitle: "Ruby #{RUBY_VERSION}; requests=#{options.fetch(:requests)}; delay/request=#{delay}s"
)

puts
puts "Wrote #{options.fetch(:csv)}"
puts "Wrote #{options.fetch(:svg)}"
