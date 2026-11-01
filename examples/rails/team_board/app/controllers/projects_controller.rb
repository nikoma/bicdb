class ProjectsController < ApplicationController
  before_action :load_project, only: :show

  def index
    @projects = accessible_projects.includes(:workspace, :owner).order(:name)
  end

  def show
    scope = @project.issues.includes(:assignee, :labels).order(priority: :desc, created_at: :desc)
    scope = scope.search(params[:q]) if params[:q].present?
    scope = scope.where(assignee_id: params[:assignee_id]) if params[:assignee_id].present?
    @issues = scope.to_a.group_by(&:status)
    @counts = @project.status_counts
    @report = @project.reports.order(created_at: :desc).first
  end
end
