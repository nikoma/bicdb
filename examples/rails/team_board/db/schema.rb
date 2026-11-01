# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.1].define(version: 2026_11_01_064314) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "pg_catalog.plpgsql"

  create_table "active_storage_attachments", force: :cascade do |t|
    t.string "name", null: false
    t.string "record_type", null: false
    t.bigint "record_id", null: false
    t.bigint "blob_id", null: false
    t.datetime "created_at", null: false
    t.index ["blob_id"], name: "index_active_storage_attachments_on_blob_id"
    t.index ["record_type", "record_id", "name", "blob_id"], name: "index_active_storage_attachments_uniqueness", unique: true
  end

  create_table "active_storage_blobs", force: :cascade do |t|
    t.string "key", null: false
    t.string "filename", null: false
    t.string "content_type"
    t.text "metadata"
    t.string "service_name", null: false
    t.bigint "byte_size", null: false
    t.string "checksum"
    t.datetime "created_at", null: false
    t.index ["key"], name: "index_active_storage_blobs_on_key", unique: true
  end

  create_table "active_storage_variant_records", force: :cascade do |t|
    t.bigint "blob_id", null: false
    t.string "variation_digest", null: false
    t.index ["blob_id", "variation_digest"], name: "index_active_storage_variant_records_uniqueness", unique: true
  end

  create_table "activities", force: :cascade do |t|
    t.bigint "workspace_id", null: false
    t.bigint "actor_id", null: false
    t.string "subject_type", null: false
    t.bigint "subject_id", null: false
    t.string "action", null: false
    t.jsonb "payload", default: {}, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["actor_id"], name: "index_activities_on_actor_id"
    t.index ["subject_type", "subject_id"], name: "index_activities_on_subject"
    t.index ["workspace_id"], name: "index_activities_on_workspace_id"
  end

  create_table "comments", force: :cascade do |t|
    t.bigint "issue_id", null: false
    t.bigint "author_id", null: false
    t.text "body", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["author_id"], name: "index_comments_on_author_id"
    t.index ["issue_id"], name: "index_comments_on_issue_id"
  end

  create_table "issue_labels", force: :cascade do |t|
    t.bigint "issue_id", null: false
    t.bigint "label_id", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["issue_id", "label_id"], name: "index_issue_labels_on_issue_id_and_label_id", unique: true
    t.index ["issue_id"], name: "index_issue_labels_on_issue_id"
    t.index ["label_id"], name: "index_issue_labels_on_label_id"
  end

  create_table "issues", force: :cascade do |t|
    t.bigint "project_id", null: false
    t.bigint "author_id", null: false
    t.bigint "assignee_id"
    t.string "title", null: false
    t.text "body"
    t.string "status", default: "backlog", null: false
    t.integer "priority", default: 1, null: false
    t.text "tags", default: [], null: false, array: true
    t.jsonb "metadata", default: {}, null: false
    t.date "due_on"
    t.integer "comments_count", default: 0, null: false
    t.integer "lock_version", default: 0, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index "to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, ''))", name: "issues_full_text", using: :gin
    t.index ["assignee_id"], name: "index_issues_on_assignee_id"
    t.index ["author_id"], name: "index_issues_on_author_id"
    t.index ["project_id", "status", "created_at"], name: "index_issues_on_project_id_and_status_and_created_at"
    t.index ["project_id"], name: "index_issues_on_project_id"
    t.check_constraint "priority >= 0 AND priority <= 3", name: "issues_priority_range"
  end

  create_table "labels", force: :cascade do |t|
    t.bigint "workspace_id", null: false
    t.string "name", null: false
    t.string "color", default: "indigo", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["workspace_id", "name"], name: "index_labels_on_workspace_id_and_name", unique: true
    t.index ["workspace_id"], name: "index_labels_on_workspace_id"
  end

  create_table "memberships", force: :cascade do |t|
    t.bigint "workspace_id", null: false
    t.bigint "user_id", null: false
    t.string "role", default: "member", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["user_id"], name: "index_memberships_on_user_id"
    t.index ["workspace_id", "user_id"], name: "index_memberships_on_workspace_id_and_user_id", unique: true
    t.index ["workspace_id"], name: "index_memberships_on_workspace_id"
  end

  create_table "projects", force: :cascade do |t|
    t.bigint "workspace_id", null: false
    t.bigint "owner_id", null: false
    t.string "name", null: false
    t.string "slug", null: false
    t.jsonb "settings", default: {}, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["owner_id"], name: "index_projects_on_owner_id"
    t.index ["workspace_id", "slug"], name: "index_projects_on_workspace_id_and_slug", unique: true
    t.index ["workspace_id"], name: "index_projects_on_workspace_id"
  end

  create_table "reports", force: :cascade do |t|
    t.bigint "project_id", null: false
    t.string "status", default: "queued", null: false
    t.jsonb "payload", default: {}, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.integer "execution_count", default: 0, null: false
    t.integer "worker_pid"
    t.datetime "started_at"
    t.datetime "finished_at"
    t.index ["project_id"], name: "index_reports_on_project_id"
  end

  create_table "users", force: :cascade do |t|
    t.string "name", null: false
    t.string "email", null: false
    t.string "password_digest", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["email"], name: "index_users_on_email", unique: true
  end

  create_table "workspaces", force: :cascade do |t|
    t.string "name", null: false
    t.string "slug", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["slug"], name: "index_workspaces_on_slug", unique: true
  end

  add_foreign_key "active_storage_attachments", "active_storage_blobs", column: "blob_id"
  add_foreign_key "active_storage_variant_records", "active_storage_blobs", column: "blob_id"
  add_foreign_key "activities", "users", column: "actor_id"
  add_foreign_key "activities", "workspaces"
  add_foreign_key "comments", "issues"
  add_foreign_key "comments", "users", column: "author_id"
  add_foreign_key "issue_labels", "issues"
  add_foreign_key "issue_labels", "labels"
  add_foreign_key "issues", "projects"
  add_foreign_key "issues", "users", column: "assignee_id"
  add_foreign_key "issues", "users", column: "author_id"
  add_foreign_key "labels", "workspaces"
  add_foreign_key "memberships", "users"
  add_foreign_key "memberships", "workspaces"
  add_foreign_key "projects", "users", column: "owner_id"
  add_foreign_key "projects", "workspaces"
  add_foreign_key "reports", "projects"
end
