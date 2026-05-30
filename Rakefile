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

task default: [:test, :rbs, :rubocop]
