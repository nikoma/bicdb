require "test_helper"

class WorkflowTest < ActiveSupport::TestCase
  setup { build_team }

  test "joins and eager loaded many to many associations preserve identity" do
    issue = create_issue
    label = @workspace.labels.create!(name: "Backend")
    issue.labels << label
    issue.comments.create!(author: @user, body: "Reviewed")
    loaded = Issue.includes(:labels, comments: :author).find(issue.id)
    assert_equal [label.id], loaded.labels.map(&:id)
    assert_equal @user.id, loaded.comments.first.author.id
    assert_equal 1, loaded.reload.comments_count
    assert_equal [@workspace.id], @user.workspaces.ids
  end

  test "JSONB and arrays survive serialization and querying" do
    issue = create_issue
    assert_equal ["launch"], issue.reload.tags
    assert_equal({ "points" => 3 }, issue.metadata)
    assert_equal [issue.id], Issue.where("metadata->>'points' = ?", "3").pluck(:id)
    assert_equal [issue.id], Issue.where("? = ANY(tags)", "launch").pluck(:id)
  end

  test "full text search reflects insert update and delete" do
    issue = create_issue(title: "Resumable archive verification", body: "durable recovery")
    assert_includes Issue.search("archive").ids, issue.id
    issue.update!(title: "Worker throughput", body: "parallel execution")
    refute_includes Issue.search("archive").ids, issue.id
    assert_includes Issue.search("throughput").ids, issue.id
    issue.destroy!
    refute_includes Issue.search("throughput").ids, issue.id
  end

  test "rollback restores rows across associations and indexes" do
    original = create_issue
    Issue.transaction(requires_new: true) do
      original.update!(title: "This must disappear", status: :done)
      create_issue(title: "Rolled back insertion")
      original.comments.create!(author: @user, body: "Rolled back comment")
      raise ActiveRecord::Rollback
    end
    assert_equal "Searchable migration evidence", original.reload.title
    assert_equal "backlog", original.status
    assert_equal 0, original.comments_count
    refute Issue.exists?(title: "Rolled back insertion")
    refute_includes Issue.search("disappear").ids, original.id
  end

  test "savepoint rollback does not discard outer writes" do
    outer = create_issue(title: "Outer write")
    Issue.transaction(requires_new: true) do
      create_issue(title: "Inner write")
      raise ActiveRecord::Rollback
    end
    assert Issue.exists?(outer.id)
    refute Issue.exists?(title: "Inner write")
  end

  test "optimistic locking refuses a stale writer" do
    first = create_issue
    stale = Issue.find(first.id)
    first.update!(status: :active)
    assert_raises(ActiveRecord::StaleObjectError) { stale.update!(status: :done) }
    assert_equal "active", first.reload.status
  end

  test "unique constraints reject bypassed model validation" do
    assert_raises(ActiveRecord::RecordNotUnique) do
      User.transaction(requires_new: true) do
        User.insert_all!([{ name: "Duplicate", email: @user.email, password_digest: @user.password_digest }])
      end
    end
    assert_equal 1, User.where(email: @user.email).count
  end

  test "foreign keys reject orphaned records" do
    assert_raises(ActiveRecord::InvalidForeignKey) do
      Comment.transaction(requires_new: true) do
        Comment.insert_all!([{ issue_id: -123, author_id: @user.id, body: "Orphan" }])
      end
    end
  end

  test "upsert respects the unique index and returns durable values" do
    label = @workspace.labels.create!(name: "Launch", color: "blue")
    Label.upsert_all([{ workspace_id: @workspace.id, name: "Launch", color: "green" }],
      unique_by: [:workspace_id, :name])
    assert_equal "green", label.reload.color
    assert_equal 1, @workspace.labels.where(name: "Launch").count
  end

  test "grouping and joins return correct aggregate results" do
    create_issue(status: :active).comments.create!(author: @user, body: "one")
    create_issue(status: :active)
    create_issue(status: :done)
    assert_equal({ "active" => 2, "done" => 1 }, @project.status_counts)
    assert_equal 1, Comment.joins(:issue).where(issues: { project_id: @project.id }).count
  end

  test "workspace boundaries exclude foreign assignees and labels" do
    outsider = User.create!(name: "Other user", email: "other-#{SecureRandom.hex(4)}@example.test", password: "test-password")
    issue = @project.issues.build(title: "Restricted", author: @user, assignee: outsider)
    refute issue.valid?
    assert_includes issue.errors[:assignee], "must belong to this workspace"
    other = Workspace.create!(name: "Other", slug: "other-#{SecureRandom.hex(4)}")
    foreign_label = other.labels.create!(name: "Private")
    refute IssueLabel.new(issue: create_issue, label: foreign_label).valid?
  end

  test "Active Storage persists attachment metadata and retrieves original bytes" do
    issue = create_issue
    bytes = "Attachment on BicDB\n"
    issue.attachments.attach(io: StringIO.new(bytes), filename: "evidence.txt", content_type: "text/plain")
    assert_equal bytes, issue.reload.attachments.first.download
    assert_equal "evidence.txt", issue.attachments.first.filename.to_s
  end

  test "database schema exposes foreign keys and indexes to the standard adapter" do
    connection = ActiveRecord::Base.connection
    assert_includes connection.foreign_keys(:comments).map(&:to_table), "issues"
    assert connection.indexes(:issues).any? { |index| index.name == "issues_full_text" && index.using == :gin }
  end
end
