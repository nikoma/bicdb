class IssueLabel < ApplicationRecord
  belongs_to :issue
  belongs_to :label
  validates :label_id, uniqueness: { scope: :issue_id }
  validate :same_workspace

  private

  def same_workspace
    if issue && label && issue.project.workspace_id != label.workspace_id
      errors.add(:label, "must belong to this workspace")
    end
  end
end
