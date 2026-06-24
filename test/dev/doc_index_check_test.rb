# frozen_string_literal: true

require "tmpdir"
require "minitest/autorun"

require_relative "../../scripts/dev/check_doc_indexes"

class DocIndexCheckTest < Minitest::Test
  def test_repository_doc_indexes_are_current
    result = Dev::DocIndexCheck.new(root: File.expand_path("../..", __dir__)).call

    assert_empty result.errors
  end

  def test_reports_markdown_files_missing_from_index
    Dir.mktmpdir do |root|
      docs_dir = File.join(root, "docs", "product-specs")
      FileUtils.mkdir_p(docs_dir)
      File.write(File.join(docs_dir, "index.md"), "# Product Specs\n")
      File.write(File.join(docs_dir, "missing.md"), "# Missing\n")

      result = Dev::DocIndexCheck.new(root: root).call

      assert_includes result.errors,
                      "docs/product-specs/missing.md is not linked from docs/product-specs/index.md"
    end
  end
end
