class ProjectReportJob < ApplicationJob
  queue_as :reports

  def perform(report)
    report.update!(started_at: Time.current, worker_pid: Process.pid)
    report.increment!(:execution_count)
    project = report.project
    payload = {
      "statuses" => project.status_counts,
      "comments" => Comment.joins(:issue).where(issues: { project_id: project.id }).count,
      "labels" => Label.joins(:issues).where(issues: { project_id: project.id }).group(:name).count
    }
    report.update!(status: :ready, payload: payload)
    Rails.cache.write("project-report/#{project.id}", payload, expires_in: 30.minutes)
    Turbo::StreamsChannel.broadcast_replace_to(project, target: "latest_report",
      partial: "projects/report", locals: { report: report })
    report.update!(finished_at: Time.current)
  end
end
