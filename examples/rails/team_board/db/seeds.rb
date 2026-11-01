owner = User.find_or_create_by!(email: "demo@example.test") do |user|
  user.name = "Alex Morgan"
  user.password = "demo-password-please-change"
end
member = User.find_or_create_by!(email: "maya@example.test") do |user|
  user.name = "Maya Chen"
  user.password = "demo-password-please-change"
end
workspace = Workspace.find_or_create_by!(slug: "northstar") { |w| w.name = "Northstar Studio" }
Membership.find_or_create_by!(workspace: workspace, user: owner) { |m| m.role = :owner }
Membership.find_or_create_by!(workspace: workspace, user: member)
labels = %w[Design Backend Customer].map { |name| workspace.labels.find_or_create_by!(name: name) }
["Product launch", "Customer experience", "Platform reliability"].each_with_index do |name, index|
  project = workspace.projects.find_or_create_by!(slug: name.parameterize) do |p|
    p.name = name
    p.owner = owner
    p.settings = { "visibility" => "workspace", "iteration_days" => 14 }
  end
  ["Make search feel instant", "Ship the onboarding checklist", "Add CSV export", "Review accessibility", "Document the release", "Restore yesterday's backup"].each_with_index do |title, n|
    issue = project.issues.find_or_create_by!(title: title) do |i|
      i.author = owner
      i.assignee = n.even? ? member : owner
      i.body = "#{title}. Keep the workflow simple, test it with real users, and record the result."
      i.status = Issue.statuses.keys[n % 4]
      i.priority = n % 4
      i.tags = ["launch", "iteration-#{index + 1}"]
      i.metadata = { "estimate" => n + 1, "source" => "demo" }
      i.due_on = Date.today + n
    end
    issue.labels = [labels[n % labels.length]] if issue.labels.empty?
    issue.comments.create!(author: member, body: "I can take the next step on this.") if issue.comments.empty?
  end
end
puts "Team Board ready: demo@example.test / demo-password-please-change"
