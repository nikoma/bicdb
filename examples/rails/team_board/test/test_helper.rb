ENV["RAILS_ENV"] ||= "test"
require_relative "../config/environment"
require "rails/test_help"

class ActiveSupport::TestCase
  parallelize(workers: ENV.fetch("PARALLEL_WORKERS", "2").to_i)
  fixtures :all

  def build_team
    suffix = SecureRandom.hex(6)
    @user = User.create!(name: "Test Owner", email: "owner-#{suffix}@example.test", password: "test-password")
    @workspace = Workspace.create!(name: "Test team", slug: "team-#{suffix}")
    Membership.create!(workspace: @workspace, user: @user, role: :owner)
    @project = @workspace.projects.create!(name: "Test project", slug: "demo", owner: @user)
  end

  def create_issue(**attributes)
    @project.issues.create!({ title: "Searchable migration evidence", author: @user,
      tags: ["launch"], metadata: { "points" => 3 } }.merge(attributes))
  end
end
