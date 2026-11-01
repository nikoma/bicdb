class Issue < ApplicationRecord
  belongs_to :project
  belongs_to :author, class_name: "User"
  belongs_to :assignee, class_name: "User", optional: true
  has_many :comments, dependent: :destroy
  has_many :issue_labels, dependent: :destroy
  has_many :labels, through: :issue_labels
  has_many_attached :attachments
  enum :status, { backlog: "backlog", active: "active", review: "review", done: "done" }, validate: true
  validates :title, presence: true, length: { maximum: 200 }
  validates :priority, inclusion: { in: 0..3 }
  validate :assignee_belongs_to_workspace
  scope :search, ->(query) {
    where("to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '')) @@ plainto_tsquery('english', ?)", query)
  }

  private

  def assignee_belongs_to_workspace
    if assignee && !project.workspace.memberships.exists?(user_id: assignee.id)
      errors.add(:assignee, "must belong to this workspace")
    end
  end
end
