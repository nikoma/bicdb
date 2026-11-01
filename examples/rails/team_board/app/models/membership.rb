class Membership < ApplicationRecord
  belongs_to :workspace
  belongs_to :user
  enum :role, { owner: "owner", member: "member" }, validate: true
  validates :user_id, uniqueness: { scope: :workspace_id }
end
