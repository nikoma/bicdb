class CreateTeamBoard < ActiveRecord::Migration[8.1]
  def change
    create_table :users do |t|
      t.string :name, null: false
      t.string :email, null: false
      t.string :password_digest, null: false
      t.timestamps
    end
    add_index :users, :email, unique: true

    create_table :workspaces do |t|
      t.string :name, null: false
      t.string :slug, null: false
      t.timestamps
    end
    add_index :workspaces, :slug, unique: true

    create_table :memberships do |t|
      t.references :workspace, null: false, foreign_key: true
      t.references :user, null: false, foreign_key: true
      t.string :role, null: false, default: "member"
      t.timestamps
    end
    add_index :memberships, [:workspace_id, :user_id], unique: true

    create_table :projects do |t|
      t.references :workspace, null: false, foreign_key: true
      t.references :owner, null: false, foreign_key: { to_table: :users }
      t.string :name, null: false
      t.string :slug, null: false
      t.jsonb :settings, null: false, default: {}
      t.timestamps
    end
    add_index :projects, [:workspace_id, :slug], unique: true

    create_table :issues do |t|
      t.references :project, null: false, foreign_key: true
      t.references :author, null: false, foreign_key: { to_table: :users }
      t.references :assignee, foreign_key: { to_table: :users }
      t.string :title, null: false
      t.text :body
      t.string :status, null: false, default: "backlog"
      t.integer :priority, null: false, default: 1
      t.text :tags, array: true, null: false, default: []
      t.jsonb :metadata, null: false, default: {}
      t.date :due_on
      t.integer :comments_count, null: false, default: 0
      t.integer :lock_version, null: false, default: 0
      t.timestamps
    end
    add_index :issues, [:project_id, :status, :created_at]
    add_index :issues, "to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, ''))",
      using: :gin, name: "issues_full_text"
    add_check_constraint :issues, "priority >= 0 AND priority <= 3", name: "issues_priority_range"

    create_table :labels do |t|
      t.references :workspace, null: false, foreign_key: true
      t.string :name, null: false
      t.string :color, null: false, default: "indigo"
      t.timestamps
    end
    add_index :labels, [:workspace_id, :name], unique: true

    create_table :issue_labels do |t|
      t.references :issue, null: false, foreign_key: true
      t.references :label, null: false, foreign_key: true
      t.timestamps
    end
    add_index :issue_labels, [:issue_id, :label_id], unique: true

    create_table :comments do |t|
      t.references :issue, null: false, foreign_key: true
      t.references :author, null: false, foreign_key: { to_table: :users }
      t.text :body, null: false
      t.timestamps
    end

    create_table :activities do |t|
      t.references :workspace, null: false, foreign_key: true
      t.references :actor, null: false, foreign_key: { to_table: :users }
      t.references :subject, polymorphic: true, null: false
      t.string :action, null: false
      t.jsonb :payload, null: false, default: {}
      t.timestamps
    end

    create_table :reports do |t|
      t.references :project, null: false, foreign_key: true
      t.string :status, null: false, default: "queued"
      t.jsonb :payload, null: false, default: {}
      t.timestamps
    end
  end
end
