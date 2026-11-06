-- Synthetic schema and public EHR migration 0188 status function. No real patient records.
CREATE TABLE patients(id uuid PRIMARY KEY,timezone text);
CREATE TABLE patient_education_definitions(id uuid PRIMARY KEY,content_kind text,content_json jsonb,estimated_duration_minutes int);
CREATE TABLE patient_education_assignments(id uuid PRIMARY KEY,org_id text,title text,instructions text,status text,education_definition_id uuid);
CREATE TABLE content_prescriptions(id uuid PRIMARY KEY,org_id text,patient_profile_id uuid,education_assignment_id uuid,frequency text,times_per_week int,starts_at timestamptz,ends_at timestamptz,duration_days int,status text,prescribed_by_email text,deleted_at timestamptz);
CREATE TABLE content_engagement_events(id uuid,patient_profile_id uuid,prescription_id uuid,event_kind text,occurred_at timestamptz,rating int,symptom_change text,note text);
INSERT INTO patients VALUES('11111111-1111-4111-8111-111111111111','UTC');
INSERT INTO patient_education_definitions VALUES('22222222-2222-4222-8222-222222222222','audio','{"duration_seconds":600}',10);
INSERT INTO patient_education_assignments VALUES('33333333-3333-4333-8333-333333333333','north','Body scan','Before bed','assigned','22222222-2222-4222-8222-222222222222');
INSERT INTO content_prescriptions VALUES('44444444-4444-4444-8444-444444444444','north','11111111-1111-4111-8111-111111111111','33333333-3333-4333-8333-333333333333','daily',NULL,'2026-11-03 12:00:00+00','2026-11-17 12:00:00+00',14,'active','nurse@example.invalid',NULL);
CREATE OR REPLACE FUNCTION content_prescription_status(p_patient uuid, p_now timestamptz) RETURNS jsonb
LANGUAGE sql STABLE AS $$
  WITH tz AS (
    SELECT COALESCE(NULLIF((SELECT p.timezone FROM patients p WHERE p.id = p_patient), ''), 'UTC') AS name
  ),
  rx AS (
    SELECT r.*, a.title, a.instructions, a.status AS assignment_status, d.content_kind, d.content_json, d.estimated_duration_minutes, d.id AS definition_id,
      LEAST(p_now, r.ends_at) AS effective_end
    FROM content_prescriptions r
    JOIN patient_education_assignments a ON a.id = r.education_assignment_id AND a.org_id = r.org_id
    JOIN patient_education_definitions d ON d.id = a.education_definition_id
    WHERE r.patient_profile_id = p_patient AND r.deleted_at IS NULL
  ),
  completions AS (
    SELECT e.prescription_id, (e.occurred_at AT TIME ZONE (SELECT name FROM tz))::date AS day,
      date_trunc('week', e.occurred_at AT TIME ZONE (SELECT name FROM tz))::date AS week, e.occurred_at
    FROM content_engagement_events e
    WHERE e.patient_profile_id = p_patient AND e.event_kind = 'completed'
  ),
  stats AS (
    SELECT rx.id,
      GREATEST(0, (rx.effective_end AT TIME ZONE (SELECT name FROM tz))::date - (rx.starts_at AT TIME ZONE (SELECT name FROM tz))::date + 1) AS days_elapsed,
      GREATEST(1, ceil(GREATEST(1, extract(epoch FROM rx.effective_end - rx.starts_at) / 86400.0) / 7.0))::int AS weeks_elapsed,
      (SELECT count(DISTINCT c.day) FROM completions c WHERE c.prescription_id = rx.id AND c.occurred_at BETWEEN rx.starts_at AND rx.ends_at) AS distinct_days,
      (SELECT count(DISTINCT c.week) FROM completions c WHERE c.prescription_id = rx.id AND c.occurred_at BETWEEN rx.starts_at AND rx.ends_at) AS distinct_weeks,
      (SELECT COALESCE(sum(LEAST(n, COALESCE(rx.times_per_week, 1))), 0) FROM (SELECT count(*) AS n FROM completions c WHERE c.prescription_id = rx.id
          AND c.occurred_at BETWEEN rx.starts_at AND rx.ends_at GROUP BY c.week) w) AS capped_per_week,
      (SELECT count(*) FROM completions c WHERE c.prescription_id = rx.id AND c.week = date_trunc('week', p_now AT TIME ZONE (SELECT name FROM tz))::date) AS this_week,
      EXISTS (SELECT 1 FROM completions c WHERE c.prescription_id = rx.id AND c.day = (p_now AT TIME ZONE (SELECT name FROM tz))::date) AS done_today,
      (SELECT max(c.occurred_at) FROM completions c WHERE c.prescription_id = rx.id) AS last_completed_at
    FROM rx
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', rx.id, 'education_assignment_id', rx.education_assignment_id, 'definition_id', rx.definition_id,
      'title', rx.title, 'instructions', rx.instructions, 'content_kind', rx.content_kind, 'content_json', rx.content_json,
      'estimated_duration_minutes', rx.estimated_duration_minutes, 'frequency', rx.frequency, 'times_per_week', rx.times_per_week,
      'starts_at', rx.starts_at, 'ends_at', rx.ends_at, 'status', rx.status, 'prescribed_by_email', rx.prescribed_by_email,
      'expected', CASE rx.frequency WHEN 'daily' THEN LEAST(s.days_elapsed, rx.duration_days) WHEN 'weekly' THEN s.weeks_elapsed
                    ELSE s.weeks_elapsed * COALESCE(rx.times_per_week, 1) END,
      'completed', CASE rx.frequency WHEN 'daily' THEN s.distinct_days WHEN 'weekly' THEN s.distinct_weeks ELSE s.capped_per_week END,
      'adherence_percent', CASE
          WHEN (CASE rx.frequency WHEN 'daily' THEN LEAST(s.days_elapsed, rx.duration_days) WHEN 'weekly' THEN s.weeks_elapsed ELSE s.weeks_elapsed * COALESCE(rx.times_per_week, 1) END) = 0 THEN NULL
          ELSE LEAST(100, round(100.0 * (CASE rx.frequency WHEN 'daily' THEN s.distinct_days WHEN 'weekly' THEN s.distinct_weeks ELSE s.capped_per_week END)
               / (CASE rx.frequency WHEN 'daily' THEN LEAST(s.days_elapsed, rx.duration_days) WHEN 'weekly' THEN s.weeks_elapsed ELSE s.weeks_elapsed * COALESCE(rx.times_per_week, 1) END))) END,
      'due_now', rx.status = 'active' AND p_now BETWEEN rx.starts_at AND rx.ends_at AND CASE rx.frequency
          WHEN 'daily' THEN NOT s.done_today WHEN 'weekly' THEN s.this_week = 0 ELSE s.this_week < COALESCE(rx.times_per_week, 1) END,
      'done_today', s.done_today, 'this_week', s.this_week, 'last_completed_at', s.last_completed_at,
      'plays', (SELECT count(*) FROM content_engagement_events e WHERE e.prescription_id = rx.id AND e.event_kind = 'started'),
      'average_rating', (SELECT round(avg(e.rating), 1) FROM content_engagement_events e WHERE e.prescription_id = rx.id AND e.rating IS NOT NULL),
      'feedback', (SELECT COALESCE(jsonb_agg(jsonb_build_object('at', e.occurred_at, 'rating', e.rating, 'symptom_change', e.symptom_change, 'has_note', e.note IS NOT NULL) ORDER BY e.occurred_at), '[]'::jsonb)
                   FROM content_engagement_events e WHERE e.prescription_id = rx.id AND e.event_kind = 'feedback')
    ) ORDER BY rx.status = 'active' DESC, rx.starts_at DESC), '[]'::jsonb)
  FROM rx JOIN stats s ON s.id = rx.id
$$;
