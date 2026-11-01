class User < ApplicationRecord
  has_secure_password
  normalizes :email, with: ->(email) { email.strip.downcase }
  has_many :memberships, dependent: :destroy
  has_many :workspaces, through: :memberships
  validates :name, :email, presence: true
  validates :email, uniqueness: true
end
