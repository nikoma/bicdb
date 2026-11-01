require "test_helper"

class BoardTest < ActionDispatch::IntegrationTest
  setup { build_team }

  def sign_in
    post session_path, params: { email: @user.email, password: "test-password" }
    assert_redirected_to root_path
  end

  test "visitors must authenticate" do
    get root_path
    assert_redirected_to new_session_path
    get new_session_path
    assert_response :success
    assert_select "form[action=?]", session_path
  end

  test "authenticated user browses projects and searches the board" do
    issue = create_issue(title: "Offline synchronization")
    sign_in
    get root_path
    assert_response :success
    assert_select "h2", text: /Test project/
    get project_path(@project), params: { q: "synchronization" }
    assert_response :success
    assert_select "h3", text: issue.title
  end

  test "issue creation labels audit event comments and status update" do
    label = @workspace.labels.create!(name: "Backend")
    sign_in
    post project_issues_path(@project), params: { issue: { title: "New workflow", body: "Details", status: "active", priority: 2, label_ids: [label.id] } }
    issue = @project.issues.find_by!(title: "New workflow")
    assert_redirected_to project_issue_path(@project, issue)
    assert_equal [label.id], issue.labels.ids
    assert Activity.exists?(subject: issue, action: "issue.created")
    post project_issue_comments_path(@project, issue), params: { comment: { body: "Looks good" } }
    assert_equal 1, issue.reload.comments_count
    patch project_issue_path(@project, issue), params: { issue: { status: "done", lock_version: issue.lock_version } }
    assert_equal "done", issue.reload.status
    get project_issue_path(@project, issue)
    assert_response :success
    assert_select ".comment", text: /Looks good/
  end

  test "invalid form returns validation errors without mutation" do
    sign_in
    assert_no_difference "Issue.count" do
      post project_issues_path(@project), params: { issue: { title: "", priority: 1 } }
    end
    assert_response :unprocessable_entity
    assert_select ".alert", text: /Title can't be blank/
  end

  test "queued report status reconciles completion and respects project access" do
    report = @project.reports.create!
    get project_report_path(@project, report)
    assert_redirected_to new_session_path
    sign_in
    get project_report_path(@project, report)
    assert_response :success
    assert_select "#latest_report[data-controller='report-progress']"
    report.update!(status: :ready, payload: { "statuses" => { "active" => 3 }, "comments" => 2 })
    get project_report_path(@project, report)
    assert_select "h2", "The latest snapshot"
    assert_select "#latest_report[data-controller='report-progress']", count: 0
    private_workspace = Workspace.create!(name: "Private", slug: "private-#{SecureRandom.hex(4)}")
    private_project = private_workspace.projects.create!(name: "Private", slug: "private", owner: @user)
    private_report = private_project.reports.create!
    get project_report_path(private_project, private_report)
    assert_response :not_found
  end

  test "other workspace records are inaccessible even with a known URL" do
    other = Workspace.create!(name: "Private", slug: "private-#{SecureRandom.hex(4)}")
    hidden = other.projects.create!(name: "Private", slug: "private", owner: @user)
    sign_in
    get project_path(hidden)
    assert_response :not_found
    post project_reports_path(hidden)
    assert_response :not_found
  end

  test "wrong credentials cannot create a session" do
    post session_path, params: { email: @user.email, password: "wrong" }
    assert_response :unprocessable_entity
    get root_path
    assert_redirected_to new_session_path
  end
end
