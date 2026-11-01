class CommentsController < ApplicationController
  before_action :load_project

  def create
    issue = @project.issues.find(params[:issue_id])
    issue.comments.create!(author: current_user, body: params.expect(comment: [:body])[:body])
    redirect_to project_issue_path(@project, issue)
  end
end
