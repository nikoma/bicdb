class IssuesController < ApplicationController
  before_action :load_project

  def show
    @issue = @project.issues.includes(:labels, comments: :author).find(params[:id])
  end

  def new
    @issue = @project.issues.build
  end

  def create
    @issue = @project.issues.build(issue_params.except(:label_ids))
    @issue.author = current_user
    Issue.transaction do
      @issue.labels = @project.workspace.labels.where(id: issue_params[:label_ids])
      @issue.save!
      Activity.create!(workspace: @project.workspace, actor: current_user,
        subject: @issue, action: "issue.created", payload: { title: @issue.title })
    end
    redirect_to project_issue_path(@project, @issue)
  rescue ActiveRecord::RecordInvalid
    render :new, status: :unprocessable_entity
  end

  def update
    @issue = @project.issues.find(params[:id])
    @issue.update!(issue_params.except(:label_ids))
    redirect_to project_issue_path(@project, @issue), status: :see_other
  rescue ActiveRecord::StaleObjectError
    redirect_to project_issue_path(@project, @issue), alert: "Someone changed this issue. Reload and try again."
  rescue ActiveRecord::RecordInvalid
    render :show, status: :unprocessable_entity
  end

  private

  def issue_params
    params.expect(issue: [:title, :body, :status, :priority, :assignee_id, :due_on, :lock_version,
      { label_ids: [], attachments: [] }])
  end
end
