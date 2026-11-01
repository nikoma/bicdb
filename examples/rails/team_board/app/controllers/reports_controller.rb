class ReportsController < ApplicationController
  before_action :load_project

  def create
    Report.transaction do
      report = @project.reports.create!
      ProjectReportJob.perform_later(report)
    end
    redirect_to project_path(@project), notice: "Report queued. A worker will update this page when it is ready."
  end
end
