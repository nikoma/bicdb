class Label < ApplicationRecord
  belongs_to :workspace
  has_many :issue_labels, dependent: :destroy
  has_many :issues, through: :issue_labels
  validates :name, presence: true, uniqueness: { scope: :workspace_id }
end
