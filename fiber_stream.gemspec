# frozen_string_literal: true

require_relative "lib/fiber_stream/version"

Gem::Specification.new do |spec|
  spec.name = "fiber_stream"
  spec.version = FiberStream::VERSION
  spec.authors = ["Dai Akatsuka"]

  spec.summary = "Asynchronous, non-blocking stream processing with backpressure."
  spec.description = "A Ruby stream processing library built around Fiber and Fiber.scheduler."
  spec.homepage = "https://github.com/dakatsuka/fiber_stream"
  spec.license = "MIT"

  spec.required_ruby_version = ">= 4.0"

  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}/tree/v#{spec.version}"
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").select do |file|
      file.start_with?("lib/", "sig/", "examples/") ||
        ["CHANGELOG.md", "LICENSE", "README.md"].include?(file)
    end
  end
  spec.require_paths = ["lib"]

  spec.add_development_dependency "async", ">= 2.0"
  spec.add_development_dependency "async-http", ">= 0.95"
  spec.add_development_dependency "minitest", ">= 5.0"
  spec.add_development_dependency "rake", ">= 13.0"
  spec.add_development_dependency "rbs", ">= 3.0"
  spec.add_development_dependency "rubocop", ">= 1.0", "< 2.0"
end
