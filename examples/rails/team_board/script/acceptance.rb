# Runs inside a real Rails environment, without adapter patches or mock workers.
require "json"
require "digest"
require "timeout"

root = ENV.fetch("TEAM_BOARD_EVIDENCE_DIR")
receipt_path = File.join(root, "job-receipt.json")
canary = { "bytes" => "\x00\xffBicDB\n".b, "list" => [1, true, nil, "é"] }
check = ->(condition, message) { raise message unless condition }

# Rails runners enable request-scoped query caching. Poll live database state,
# rather than repeatedly reading the first cached count while workers commit.
ActiveRecord::Base.uncached do
  SolidQueue::Record.uncached do
    SolidCable::Record.uncached do
      case ARGV.fetch(0)
      when "prepare"
        check.call(Rails.version == "8.1.4", "unexpected Rails version")
        check.call(ActiveRecord::Base.connection.adapter_name == "PostgreSQL", "standard PostgreSQL adapter required")
        before = SolidQueue::Job.count
        Report.transaction do
          rollback_report = Report.create!(project: Project.first!)
          ProjectReportJob.perform_later(rollback_report)
          raise ActiveRecord::Rollback
        end
        check.call(SolidQueue::Job.count == before, "rolled-back transaction published a job")
        Rails.cache.write("qualification/canary", canary, expires_in: 1.hour)
        reports = []
        24.times do |n|
          report = Report.create!(project: Project.order(:id).to_a[n % Project.count])
          ProjectReportJob.perform_later(report)
          reports << report.id
        end
        check.call(SolidQueue::ReadyExecution.count >= reports.length, "jobs were not durably queued")
        receipt = { "rails" => Rails.version, "adapter" => ActiveRecord::Base.connection.adapter_name,
          "reports" => reports, "rolled_back_job_absent" => true,
          "canary_sha256" => Digest::SHA256.hexdigest(Marshal.dump(canary)) }
        File.write(receipt_path, JSON.pretty_generate(receipt))
        puts JSON.generate(receipt)
      when "after_restart"
        receipt = JSON.parse(File.read(receipt_path))
        check.call(Rails.cache.read("qualification/canary") == canary, "cache bytes changed across restart")
        check.call(Report.where(id: receipt.fetch("reports"), status: :queued).count == 24, "pending reports lost")
        check.call(SolidQueue::ReadyExecution.count >= 24, "pending jobs lost")
        puts "Pending jobs, reports and binary cache value survived BicDB restart."
      when "wait"
        receipt = JSON.parse(File.read(receipt_path))
        ids = receipt.fetch("reports")
        Timeout.timeout(180) do
          loop do
            raise "Solid Queue failed jobs: #{SolidQueue::FailedExecution.pluck(:error)}" if SolidQueue::FailedExecution.exists?
            break if Report.where(id: ids).where.not(finished_at: nil).count == ids.length && SolidQueue::ReadyExecution.count.zero? && SolidQueue::ClaimedExecution.count.zero?
            sleep 0.2
          end
        end
        reports = Report.where(id: ids).to_a
        check.call(reports.all? { |report| report.execution_count == 1 && report.ready? }, "missing or duplicate job execution")
        pids = reports.map(&:worker_pid).uniq
        check.call(pids.length >= 2, "two worker processes must make progress")
        overlap = reports.any? do |a|
          reports.any? { |b| a.worker_pid != b.worker_pid && a.started_at < b.finished_at && b.started_at < a.finished_at }
        end
        check.call(overlap, "workers did not overlap")
        reports.each do |report|
          project = report.project
          check.call(report.payload.fetch("statuses") == project.status_counts, "report aggregation mismatch")
          check.call(report.payload.fetch("comments") == Comment.joins(:issue).where(issues: { project_id: project.id }).count, "report comment count mismatch")
          check.call(Rails.cache.read("project-report/#{project.id}") == report.payload, "report cache mismatch")
        end
        check.call(SolidCable::Message.count >= ids.length, "live-update broadcasts were not persisted")
        receipt.merge!("passed" => true, "worker_pids" => pids, "overlapping_workers" => overlap,
          "completed_reports" => reports.length, "cable_messages" => SolidCable::Message.count,
          "failed_jobs" => SolidQueue::FailedExecution.count)
        File.write(receipt_path, JSON.pretty_generate(receipt))
        puts JSON.pretty_generate(receipt)
      else
        raise "unknown acceptance phase"
      end
    end
  end
end
