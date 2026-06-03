# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"
require "socket"

ROUTES = {
  "/profile" => [0.18, "profile loaded"],
  "/orders" => [0.12, "orders loaded"],
  "/recommendations" => [0.16, "recommendations loaded"]
}.freeze

Endpoint = Struct.new(:path, :label, keyword_init: true)

def monotonic_time
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def run_server(server)
  loop do
    socket = server.accept
    Async { handle_connection(socket) }
  rescue IOError, Errno::EBADF
    break
  end
end

def handle_connection(socket)
  request = socket.readpartial(1024)
  path = request[/GET\s+(\S+)/, 1] || "/"
  delay, body = ROUTES.fetch(path, [0.02, "not found"])
  status = ROUTES.key?(path) ? "200 OK" : "404 Not Found"

  sleep delay

  socket.write(
    "HTTP/1.1 #{status}\r\n" \
    "Content-Type: text/plain\r\n" \
    "Content-Length: #{body.bytesize}\r\n" \
    "Connection: close\r\n\r\n" \
    "#{body}"
  )
ensure
  socket&.close
end

def http_get(host, port, endpoint)
  response = +""
  started_at = monotonic_time
  socket = TCPSocket.new(host, port)

  socket.write(
    "GET #{endpoint.path} HTTP/1.1\r\n" \
    "Host: #{host}:#{port}\r\n" \
    "Connection: close\r\n\r\n"
  )

  loop do
    response << socket.readpartial(1024)
  rescue EOFError
    break
  end

  headers, body = response.split("\r\n\r\n", 2)
  status = headers.lines.first.split.fetch(1)
  elapsed = monotonic_time - started_at

  {
    label: endpoint.label,
    status: status,
    body: body,
    elapsed: elapsed
  }
ensure
  socket&.close
end

def measure(label)
  started_at = monotonic_time
  result = yield
  elapsed = monotonic_time - started_at
  [label, elapsed, result]
end

def print_run(label, elapsed, responses)
  puts "#{label}: #{format('%.3f', elapsed)}s"
  responses.each do |response|
    puts format(
      "- %-15<label>s status=%<status>s request=%<elapsed>.3fs body=%<body>s",
      label: response.fetch(:label),
      status: response.fetch(:status),
      elapsed: response.fetch(:elapsed),
      body: response.fetch(:body)
    )
  end
  puts
end

endpoints = [
  Endpoint.new(path: "/profile", label: "profile"),
  Endpoint.new(path: "/orders", label: "orders"),
  Endpoint.new(path: "/recommendations", label: "recommendations")
]

Sync do
  server = TCPServer.new("127.0.0.1", 0)
  host = "127.0.0.1"
  port = server.addr.fetch(1)
  server_task = Async { run_server(server) }

  begin
    serial = measure("Serial HTTP requests") do
      endpoints.map { |endpoint| http_get(host, port, endpoint) }
    end

    parallel = measure("FiberStream parallel HTTP requests") do
      FiberStream::Source.each(endpoints)
        .parallel_map(concurrency: endpoints.length) { |endpoint| http_get(host, port, endpoint) }
        .run_with(FiberStream::Sink.to_a)
    end

    print_run(*serial)
    print_run(*parallel)

    puts "Serial waits add up. FiberStream starts all requests together and keeps the responses ordered."
  ensure
    server.close
    server_task.stop
  end
end
