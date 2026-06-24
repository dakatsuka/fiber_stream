# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |task|
  task.libs << "test"
  task.pattern = "test/**/*_test.rb"
  task.warning = true
end

desc "Validate RBS signatures"
task :rbs do
  sh "bundle exec rbs validate"
end

desc "Run RuboCop"
task :rubocop do
  sh "bundle exec rubocop"
end

namespace :docs do
  desc "Check documentation index coverage"
  task :index do
    require_relative "scripts/dev/check_doc_indexes"

    result = Dev::DocIndexCheck.new(root: __dir__).call
    abort result.errors.join("\n") unless result.errors.empty?
  end

  desc "Build the documentation website"
  task :build do
    Dir.chdir("website") do
      sh "npm run docs:build"
    end
  end
end

desc "Run the default local verification gate"
task verify: [:test, :rbs, :rubocop, "docs:index"]

namespace :verify do
  desc "Run the full local verification gate, including the website build"
  task full: [:verify, "docs:build"]
end

task default: :verify
