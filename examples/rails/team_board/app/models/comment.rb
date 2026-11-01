class Comment < ApplicationRecord
  belongs_to :issue, counter_cache: true, touch: true
  belongs_to :author, class_name: "User"
  validates :body, presence: true
end
