class Project < ApplicationRecord
  belongs_to :workspace
  belongs_to :owner, class_name: "User"
  has_many :issues, dependent: :destroy
  has_many :reports, dependent: :destroy
  validates :name, :slug, presence: true
  validates :slug, uniqueness: { scope: :workspace_id }

  def status_counts
    issues.group(:status).count
  end
end
