# Async HTTP

Streaming response bodies that implement `#each` can be used with
`Source.each`. Keep ownership clear: the HTTP client owns the response body,
so use the client's block form or close the response explicitly.

```ruby
require "async"
require "async/http/internet/instance"
require "fiber_stream"

status_counts = Hash.new(0)
url = "https://example.com/access.log"

processed =
  Sync do
    Async::HTTP::Internet.get(url) do |response|
      raise "unexpected status #{response.status}" unless response.status == 200

      FiberStream::Source.each(response.body)
        .lines(max_length: 16 * 1024)
        .map { |line| line.split.fetch(8, nil) }
        .select { |status| status&.match?(/\A\d{3}\z/) }
        .run_with(
          FiberStream::Sink.foreach do |status|
            status_counts[status] += 1
          end
        )
    end
  end
```

Set `max_length` for network-facing line framing. With `max_length: nil`, one
unterminated line can buffer without bound until a newline or upstream
completion.

The full example is `examples/async_http_streaming_body.rb`.
