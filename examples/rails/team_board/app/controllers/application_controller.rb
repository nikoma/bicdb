class ApplicationController < ActionController::Base
  before_action :authenticate
  helper_method :current_user

  private

  def current_user
    Current.user ||= User.find_by(id: session[:user_id])
  end

  def authenticate
    redirect_to new_session_path unless current_user
  end

  def accessible_projects
    Project.where(workspace_id: current_user.workspaces.select(:id))
  end

  def load_project
    @project = accessible_projects.find(params[:project_id] || params[:id])
  end
end
