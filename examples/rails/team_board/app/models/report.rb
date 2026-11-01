class Report < ApplicationRecord
  belongs_to :project
  enum :status, { queued: "queued", ready: "ready" }, validate: true
end
