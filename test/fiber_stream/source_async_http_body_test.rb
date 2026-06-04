# frozen_string_literal: true

require_relative "../test_helper"

require "async/http/internet/instance"
require "protocol/http/body/readable"
require "protocol/http/response"

module FiberStream
  class SourceAsyncHttpBodyTest < Minitest::Test
    def test_each_streams_protocol_http_response_body_through_flows
      body = TrackingReadableBody.new(["alpha\nbr", "avo\ncharlie\n"])
      response = Protocol::HTTP::Response[200, {}, body]

      result =
        Source.each(response.body)
          .lines
          .map(&:upcase)
          .run_with(Sink.to_a)

      assert_equal ["ALPHA", "BRAVO", "CHARLIE"], result
      assert_equal 3, body.read_count
      assert body.closed?
    end

    def test_each_does_not_read_past_early_downstream_completion
      body = TrackingReadableBody.new(["one\n", "two\n", "three\n"])
      response = Protocol::HTTP::Response[200, {}, body]

      result =
        Source.each(response.body)
          .lines
          .take(2)
          .run_with(Sink.to_a)

      assert_equal ["one", "two"], result
      assert_equal 2, body.read_count
      refute body.closed?
    ensure
      response&.close
    end

    class TrackingReadableBody < Protocol::HTTP::Body::Readable
      attr_reader :read_count

      def initialize(chunks)
        super()

        @chunks = chunks.dup
        @read_count = 0
        @closed = false
      end

      def read
        @read_count += 1
        @chunks.shift
      end

      def close(_error = nil)
        @closed = true
      end

      def closed?
        @closed
      end
    end
  end
end
