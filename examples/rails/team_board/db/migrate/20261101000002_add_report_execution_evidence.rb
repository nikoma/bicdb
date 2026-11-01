class AddReportExecutionEvidence < ActiveRecord::Migration[8.1]
  def change
    add_column :reports, :execution_count, :integer, null: false, default: 0
    add_column :reports, :worker_pid, :integer
    add_column :reports, :started_at, :datetime
    add_column :reports, :finished_at, :datetime
  end
end
