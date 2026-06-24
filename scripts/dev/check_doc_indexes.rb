# frozen_string_literal: true

module Dev
  class DocIndexCheck
    Result = Data.define(:errors)

    DEFAULT_INDEXES = {
      "docs/product-specs" => "docs/product-specs/index.md",
      "docs/design-docs" => "docs/design-docs/index.md",
      "docs/references" => "docs/references/index.md",
      "docs/exec-plans" => "docs/exec-plans/index.md"
    }.freeze

    LINK_PATTERN = /\[[^\]]+\]\(([^)\s]+)(?:\s+"[^"]*")?\)/.freeze

    def initialize(root:, indexes: DEFAULT_INDEXES)
      @root = Pathname(root)
      @indexes = indexes
    end

    def call
      errors = @indexes.flat_map do |directory, index|
        check_index(directory: directory, index: index)
      end

      Result.new(errors: errors)
    end

    private

    attr_reader :root, :indexes

    def check_index(directory:, index:)
      directory_path = root.join(directory)
      index_path = root.join(index)
      return [] unless directory_path.directory?

      unless index_path.file?
        return ["#{relative(index_path)} is missing for #{relative(directory_path)}"]
      end

      linked_files = linked_markdown_files(index_path)

      markdown_files(directory_path).filter_map do |file|
        next if file == index_path
        next if linked_files.include?(file)

        "#{relative(file)} is not linked from #{relative(index_path)}"
      end
    end

    def linked_markdown_files(index_path)
      index_path.read.scan(LINK_PATTERN).filter_map do |match|
        target = match.first
        next if external_link?(target)

        target_without_fragment = target.split("#", 2).first
        next unless target_without_fragment.end_with?(".md")

        index_path.dirname.join(target_without_fragment).cleanpath
      end.to_set
    end

    def markdown_files(directory_path)
      directory_path.glob("**/*.md").map(&:cleanpath).sort_by { |path| relative(path) }
    end

    def external_link?(target)
      target.start_with?("http://", "https://", "mailto:")
    end

    def relative(path)
      path.relative_path_from(root).to_s
    end
  end
end

if $PROGRAM_NAME == __FILE__
  result = Dev::DocIndexCheck.new(root: Pathname(__dir__).join("../..").expand_path).call

  if result.errors.empty?
    warn "Documentation indexes are current."
  else
    warn result.errors.join("\n")
    exit 1
  end
end
