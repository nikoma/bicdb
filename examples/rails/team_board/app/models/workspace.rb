class Workspace < ApplicationRecord
  has_many :memberships, dependent: :destroy
  has_many :users, through: :memberships
  has_many :projects, dependent: :destroy
  has_many :labels, dependent: :destroy
  has_many :activities, dependent: :destroy
  validates :name, :slug, presence: true
  validates :slug, uniqueness: true
end
