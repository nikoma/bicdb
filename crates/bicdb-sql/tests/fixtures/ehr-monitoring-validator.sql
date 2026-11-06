-- Public application SQL only; no patient records or credentials.

CREATE OR REPLACE FUNCTION monitoring_cfg_ids(arr jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE AS $$
  SELECT COALESCE(array_agg(e->>'id'), ARRAY[]::text[])
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(arr) = 'array' THEN arr ELSE '[]'::jsonb END) e
$$;

CREATE OR REPLACE FUNCTION monitoring_validate_window(w jsonb, path text) RETURNS text[]
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE errs text[] := ARRAY[]::text[]; k text; n int := 0;
BEGIN
  IF w IS NULL THEN RETURN errs; END IF;
  IF jsonb_typeof(w) <> 'object' THEN RETURN ARRAY[path || ': window must be an object']; END IF;
  FOR k IN SELECT jsonb_object_keys(w) LOOP
    n := n + 1;
    IF k NOT IN ('days', 'hours', 'last_n', 'since_enrollment') THEN
      errs := errs || (path || ': unsupported window key "' || k || '"');
    END IF;
  END LOOP;
  IF n <> 1 THEN errs := errs || (path || ': a window has exactly one of days, hours, last_n, since_enrollment'); END IF;
  IF w ? 'days' AND NOT (jsonb_typeof(w->'days') = 'number' AND (w->>'days')::numeric BETWEEN 1 AND 400) THEN
    errs := errs || (path || ': days must be a number from 1 to 400');
  END IF;
  IF w ? 'hours' AND NOT (jsonb_typeof(w->'hours') = 'number' AND (w->>'hours')::numeric BETWEEN 1 AND 720) THEN
    errs := errs || (path || ': hours must be a number from 1 to 720');
  END IF;
  IF w ? 'last_n' AND NOT (jsonb_typeof(w->'last_n') = 'number' AND (w->>'last_n')::numeric BETWEEN 1 AND 200) THEN
    errs := errs || (path || ': last_n must be a number from 1 to 200');
  END IF;
  IF w ? 'since_enrollment' AND w->'since_enrollment' <> 'true'::jsonb THEN
    errs := errs || (path || ': since_enrollment must be true');
  END IF;
  RETURN errs;
END $$;

CREATE OR REPLACE FUNCTION monitoring_validate_expr(cfg jsonb, e jsonb, path text, depth int) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  errs text[] := ARRAY[]::text[];
  uses_count boolean := false;
  keys text[];
  k text;
  m jsonb;
  sub jsonb;
  r jsonb;
  i int;
BEGIN
  IF depth > 8 THEN RETURN jsonb_build_object('errors', to_jsonb(ARRAY[path || ': expression nested too deeply']), 'counts', false); END IF;
  IF e IS NULL OR jsonb_typeof(e) <> 'object' THEN
    RETURN jsonb_build_object('errors', to_jsonb(ARRAY[path || ': expression must be an object']), 'counts', false);
  END IF;
  SELECT array_agg(x) INTO keys FROM jsonb_object_keys(e) x;
  IF 'const' = ANY(keys) THEN
    IF jsonb_typeof(e->'const') <> 'number' THEN errs := errs || (path || ': const must be a number'); END IF;
  ELSIF 'target' = ANY(keys) THEN
    IF NOT (e->>'target') = ANY(monitoring_cfg_ids(cfg->'targets')) THEN
      errs := errs || (path || ': unknown target "' || COALESCE(e->>'target', '') || '"');
    END IF;
  ELSIF 'baseline' = ANY(keys) THEN
    IF NOT (e->>'baseline') = ANY(monitoring_cfg_ids(cfg->'measurements')) THEN
      errs := errs || (path || ': baseline names an unknown measurement "' || COALESCE(e->>'baseline', '') || '"');
    END IF;
  ELSIF 'param' = ANY(keys) THEN
    IF NOT (e->>'param') = ANY(monitoring_cfg_ids(cfg->'parameters')) THEN
      errs := errs || (path || ': unknown parameter "' || COALESCE(e->>'param', '') || '"');
    END IF;
  ELSIF 'agg' = ANY(keys) THEN
    IF NOT (e->>'agg') = ANY(ARRAY['count','sum','min','max','avg','latest','first','distinct_days','distinct_nights','slope_per_day']) THEN
      errs := errs || (path || ': unsupported aggregate "' || COALESCE(e->>'agg', '') || '"');
    END IF;
    SELECT x INTO m FROM jsonb_array_elements(COALESCE(cfg->'measurements', '[]'::jsonb)) x WHERE x->>'id' = e->>'measurement';
    IF m IS NULL THEN
      errs := errs || (path || ': unknown measurement "' || COALESCE(e->>'measurement', '') || '"');
    ELSIF COALESCE(m->>'kind', 'event') = 'period_summary'
          AND (e->>'agg') IN ('count','sum','distinct_days','distinct_nights','avg','slope_per_day') THEN
      -- A period summary already covers a period ("nights woken in the past 7 days"). Summing or counting
      -- overlapping summaries would double-count the same nights.
      errs := errs || (path || ': measurement "' || (m->>'id') || '" is a period summary; use latest, first, min or max');
    END IF;
    IF (e->>'agg') IN ('count','sum','distinct_days','distinct_nights') THEN uses_count := true; END IF;
    IF NOT e ? 'window' THEN errs := errs || (path || ': an aggregate needs a window'); END IF;
    errs := errs || monitoring_validate_window(e->'window', path || '.window');
    IF e ? 'where' THEN
      IF jsonb_typeof(e->'where') <> 'object'
         OR NOT (e->'where'->>'op') = ANY(ARRAY['gt','gte','lt','lte','eq','neq'])
         OR jsonb_typeof(e->'where'->'value') <> 'number' THEN
        errs := errs || (path || '.where: needs op (gt/gte/lt/lte/eq/neq) and a numeric value');
      END IF;
    END IF;
    FOREACH k IN ARRAY keys LOOP
      IF k NOT IN ('agg','measurement','window','where') THEN errs := errs || (path || ': unsupported key "' || k || '"'); END IF;
    END LOOP;
  ELSIF keys && ARRAY['add','sub','mul','div','pct_change'] THEN
    k := keys[1];
    IF array_length(keys, 1) <> 1 OR jsonb_typeof(e->k) <> 'array' OR jsonb_array_length(e->k) <> 2 THEN
      errs := errs || (path || ': ' || k || ' takes exactly two operands');
    ELSE
      FOR i IN 0..1 LOOP
        r := monitoring_validate_expr(cfg, e->k->i, path || '.' || k || '[' || i || ']', depth + 1);
        errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
        uses_count := uses_count OR (r->>'counts')::boolean;
      END LOOP;
    END IF;
  ELSIF 'abs' = ANY(keys) THEN
    r := monitoring_validate_expr(cfg, e->'abs', path || '.abs', depth + 1);
    errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
    uses_count := uses_count OR (r->>'counts')::boolean;
  ELSE
    errs := errs || (path || ': unsupported expression (allowed: const, target, baseline, param, agg, add, sub, mul, div, pct_change, abs)');
  END IF;
  RETURN jsonb_build_object('errors', to_jsonb(errs), 'counts', uses_count);
END $$;

CREATE OR REPLACE FUNCTION monitoring_validate_cond(cfg jsonb, c jsonb, path text, depth int) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE errs text[] := ARRAY[]::text[]; uses_count boolean := false; r jsonb; i int; k text;
BEGIN
  IF depth > 8 THEN RETURN jsonb_build_object('errors', to_jsonb(ARRAY[path || ': condition nested too deeply']), 'counts', false); END IF;
  IF c IS NULL OR jsonb_typeof(c) <> 'object' THEN
    RETURN jsonb_build_object('errors', to_jsonb(ARRAY[path || ': condition must be an object']), 'counts', false);
  END IF;
  IF c ? 'all' OR c ? 'any' THEN
    k := CASE WHEN c ? 'all' THEN 'all' ELSE 'any' END;
    IF jsonb_typeof(c->k) <> 'array' OR jsonb_array_length(c->k) NOT BETWEEN 1 AND 20 THEN
      errs := errs || (path || ': ' || k || ' takes 1 to 20 conditions');
    ELSE
      FOR i IN 0..jsonb_array_length(c->k) - 1 LOOP
        r := monitoring_validate_cond(cfg, c->k->i, path || '.' || k || '[' || i || ']', depth + 1);
        errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
        uses_count := uses_count OR (r->>'counts')::boolean;
      END LOOP;
    END IF;
  ELSIF c ? 'not' THEN
    r := monitoring_validate_cond(cfg, c->'not', path || '.not', depth + 1);
    errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
    uses_count := uses_count OR (r->>'counts')::boolean;
  ELSIF c ? 'op' THEN
    IF NOT (c->>'op') = ANY(ARRAY['gt','gte','lt','lte','eq','neq']) THEN
      errs := errs || (path || ': unsupported comparison "' || COALESCE(c->>'op', '') || '"');
    END IF;
    FOREACH k IN ARRAY ARRAY['left','right'] LOOP
      r := monitoring_validate_expr(cfg, c->k, path || '.' || k, depth + 1);
      errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
      uses_count := uses_count OR (r->>'counts')::boolean;
    END LOOP;
  ELSE
    errs := errs || (path || ': a condition is one of all, any, not, or a comparison with op/left/right');
  END IF;
  RETURN jsonb_build_object('errors', to_jsonb(errs), 'counts', uses_count);
END $$;

CREATE OR REPLACE FUNCTION monitoring_validate_finding(f jsonb, path text) RETURNS text[]
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE errs text[] := ARRAY[]::text[];
BEGIN
  IF f IS NULL OR jsonb_typeof(f) <> 'object' THEN RETURN ARRAY[path || ': finding must be an object']; END IF;
  IF COALESCE(f->>'code', '') !~ '^[a-z0-9][a-z0-9._-]{1,118}$' THEN errs := errs || (path || ': code must be a lowercase identifier'); END IF;
  IF length(COALESCE(f->>'title', '')) NOT BETWEEN 3 AND 200 THEN errs := errs || (path || ': title is required (3-200 characters)'); END IF;
  IF COALESCE(f->>'severity', 'unspecified') NOT IN ('unspecified','low','moderate','high','critical') THEN errs := errs || (path || ': unsupported severity'); END IF;
  IF COALESCE(f->>'urgency', 'routine') NOT IN ('low','routine','urgent','stat') THEN errs := errs || (path || ': unsupported urgency'); END IF;
  RETURN errs;
END $$;

CREATE OR REPLACE FUNCTION monitoring_validate_route(r jsonb, path text) RETURNS text[]
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE errs text[] := ARRAY[]::text[];
BEGIN
  IF r IS NULL THEN RETURN ARRAY[path || ': routing is required (who owns the resulting task)']; END IF;
  IF COALESCE(r->>'target', '') NOT IN ('enrollment_owner', 'role') THEN errs := errs || (path || ': target must be enrollment_owner or role'); END IF;
  IF r->>'target' = 'role' AND COALESCE(r->>'role', '') NOT IN ('practitioner','nurse','cihc','clinical_triage') THEN
    errs := errs || (path || ': role must be practitioner, nurse, cihc or clinical_triage');
  END IF;
  IF NOT (jsonb_typeof(r->'due_hours') = 'number' AND (r->>'due_hours')::numeric BETWEEN 0.25 AND 720) THEN
    errs := errs || (path || ': due_hours must be a number from 0.25 to 720');
  END IF;
  IF COALESCE(r->>'priority', 'routine') NOT IN ('low','routine','urgent','stat') THEN errs := errs || (path || ': priority must be low, routine, urgent or stat'); END IF;
  IF COALESCE(r->>'task_category', 'assessment_review') NOT IN ('assessment_review','patient_outreach','care_plan_review','activity_follow_up') THEN
    errs := errs || (path || ': task_category must be assessment_review, patient_outreach, care_plan_review or activity_follow_up');
  END IF;
  IF r ? 'escalation' THEN
    IF NOT (jsonb_typeof(r->'escalation'->'after_hours') = 'number' AND (r->'escalation'->>'after_hours')::numeric BETWEEN 0.25 AND 720) THEN
      errs := errs || (path || '.escalation: after_hours must be a number from 0.25 to 720');
    END IF;
    IF COALESCE(r->'escalation'->>'to_role', 'clinical_triage') NOT IN ('practitioner','nurse','cihc','clinical_triage') THEN
      errs := errs || (path || '.escalation: to_role must be practitioner, nurse, cihc or clinical_triage');
    END IF;
    IF r->'escalation' ? 'max_levels' AND NOT (jsonb_typeof(r->'escalation'->'max_levels') = 'number' AND (r->'escalation'->>'max_levels')::numeric BETWEEN 1 AND 5) THEN
      errs := errs || (path || '.escalation: max_levels must be 1 to 5');
    END IF;
  END IF;
  RETURN errs;
END $$;

-- Limit serialized configuration bytes independently of storage layout.
-- The original immutable migration remains unchanged.
CREATE OR REPLACE FUNCTION monitoring_validate_config(cfg jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  errs text[] := ARRAY[]::text[];
  item jsonb;
  inner_item jsonb;
  r jsonb;
  i int;
  ids text[];
  path text;
  measurement_ids text[];
  form_ids text[];
BEGIN
  IF cfg IS NULL OR jsonb_typeof(cfg) <> 'object' THEN
    RETURN jsonb_build_object('valid', false, 'errors', jsonb_build_array('configuration must be a JSON object'));
  END IF;
  IF cfg->>'schema_version' IS DISTINCT FROM 'monitoring.v1' THEN errs := errs || 'schema_version must be "monitoring.v1"'::text; END IF;
  IF COALESCE(cfg->>'code', '') !~ '^[a-z0-9][a-z0-9._-]{1,118}$' THEN errs := errs || 'code must be a lowercase identifier'::text; END IF;
  IF length(COALESCE(cfg->>'title', '')) NOT BETWEEN 3 AND 200 THEN errs := errs || 'title is required'::text; END IF;
  IF octet_length(cfg::text) > 262144 THEN errs := errs || 'configuration is larger than 256 KB'::text; END IF;

  -- governance
  IF jsonb_typeof(cfg->'governance') <> 'object' THEN
    errs := errs || 'governance is required (source_references, clinical_approval_required, synthetic)'::text;
  ELSE
    IF jsonb_typeof(cfg->'governance'->'source_references') <> 'array' THEN errs := errs || 'governance.source_references must be a list (may be empty for drafts)'::text; END IF;
    IF jsonb_typeof(cfg->'governance'->'clinical_approval_required') <> 'boolean' THEN errs := errs || 'governance.clinical_approval_required must be true or false'::text; END IF;
    IF jsonb_typeof(cfg->'governance'->'synthetic') <> 'boolean' THEN errs := errs || 'governance.synthetic must be true or false'::text; END IF;
  END IF;

  -- unique ids per section
  FOR path IN SELECT unnest(ARRAY['measurements','targets','parameters','forms','schedules','rules']) LOOP
    IF cfg ? path AND jsonb_typeof(cfg->path) <> 'array' THEN errs := errs || (path || ' must be a list'); CONTINUE; END IF;
    ids := monitoring_cfg_ids(cfg->path);
    IF EXISTS (SELECT 1 FROM unnest(ids) x WHERE x IS NULL OR x !~ '^[a-z0-9][a-z0-9._-]{0,118}$') THEN
      errs := errs || (path || ': every entry needs a lowercase id');
    END IF;
    IF (SELECT count(*) FROM unnest(ids)) <> (SELECT count(DISTINCT x) FROM unnest(ids) x) THEN
      errs := errs || (path || ': ids must be unique');
    END IF;
  END LOOP;
  IF jsonb_array_length(COALESCE(cfg->'measurements', '[]'::jsonb)) = 0 THEN errs := errs || 'at least one measurement is required'::text; END IF;
  IF jsonb_array_length(COALESCE(cfg->'schedules', '[]'::jsonb)) = 0 THEN errs := errs || 'at least one schedule is required'::text; END IF;
  measurement_ids := monitoring_cfg_ids(cfg->'measurements');
  form_ids := monitoring_cfg_ids(cfg->'forms');

  -- measurements
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'measurements', '[]'::jsonb)) LOOP
    path := 'measurements.' || COALESCE(item->>'id', '?');
    IF COALESCE(item->>'observation_code', '') !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{1,118}$' THEN errs := errs || (path || ': observation_code is required'); END IF;
    IF COALESCE(item->>'type', '') NOT IN ('quantity','count','score','boolean','scale') THEN errs := errs || (path || ': type must be quantity, count, score, boolean or scale'); END IF;
    IF COALESCE(item->>'kind', 'event') NOT IN ('event','period_summary') THEN errs := errs || (path || ': kind must be event or period_summary'); END IF;
    IF item->>'kind' = 'period_summary' AND NOT (jsonb_typeof(item->'period_days') = 'number' AND (item->>'period_days')::numeric BETWEEN 1 AND 90) THEN
      errs := errs || (path || ': a period_summary needs period_days (1-90)');
    END IF;
    IF length(COALESCE(item->>'label', '')) NOT BETWEEN 2 AND 160 THEN errs := errs || (path || ': label is required'); END IF;
    IF item ? 'conversions' THEN
      FOR inner_item IN SELECT * FROM jsonb_array_elements(item->'conversions') LOOP
        IF COALESCE(inner_item->>'from_unit', '') = '' OR jsonb_typeof(inner_item->'factor') <> 'number' THEN
          errs := errs || (path || '.conversions: each needs from_unit and a numeric factor (optional numeric offset)');
        END IF;
      END LOOP;
    END IF;
  END LOOP;

  -- targets
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'targets', '[]'::jsonb)) LOOP
    path := 'targets.' || COALESCE(item->>'id', '?');
    IF item ? 'default' AND jsonb_typeof(item->'default') NOT IN ('number', 'null') THEN errs := errs || (path || ': default must be a number or null'); END IF;
    IF jsonb_typeof(item->'individualizable') <> 'boolean' THEN errs := errs || (path || ': individualizable must be true or false'); END IF;
    IF item ? 'min' AND item ? 'max' AND (item->>'min')::numeric > (item->>'max')::numeric THEN errs := errs || (path || ': min is greater than max'); END IF;
  END LOOP;

  -- parameters
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'parameters', '[]'::jsonb)) LOOP
    path := 'parameters.' || COALESCE(item->>'id', '?');
    IF COALESCE(item->>'type', '') NOT IN ('choice','number','boolean') THEN errs := errs || (path || ': type must be choice, number or boolean'); END IF;
    IF item->>'type' = 'choice' AND (jsonb_typeof(item->'options') <> 'array' OR jsonb_array_length(item->'options') = 0) THEN
      errs := errs || (path || ': a choice parameter needs options');
    END IF;
  END LOOP;

  -- forms
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'forms', '[]'::jsonb)) LOOP
    path := 'forms.' || COALESCE(item->>'id', '?');
    IF COALESCE(item->>'form_code', '') !~ '^[a-z0-9][a-z0-9._-]{1,118}$' THEN errs := errs || (path || ': form_code is required'); END IF;
    IF jsonb_typeof(item->'questionnaire') <> 'object' OR jsonb_typeof(item->'questionnaire'->'item') <> 'array' THEN
      errs := errs || (path || ': questionnaire with an item list is required');
    END IF;
    FOR inner_item IN SELECT * FROM jsonb_array_elements(COALESCE(item->'mappings', '[]'::jsonb)) LOOP
      IF NOT (inner_item->>'measurement') = ANY(measurement_ids) THEN
        errs := errs || (path || '.mappings: unknown measurement "' || COALESCE(inner_item->>'measurement', '') || '"');
      END IF;
      IF COALESCE(inner_item->>'link_id', '') = '' AND jsonb_typeof(inner_item->'link_ids') <> 'array' THEN
        errs := errs || (path || '.mappings: each mapping needs link_id or link_ids');
      END IF;
      IF COALESCE(inner_item->>'aggregate', 'none') NOT IN ('none','sum','count','average') THEN
        errs := errs || (path || '.mappings: aggregate must be none, sum, count or average');
      END IF;
    END LOOP;
  END LOOP;

  -- schedules
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'schedules', '[]'::jsonb)) LOOP
    path := 'schedules.' || COALESCE(item->>'id', '?');
    IF NOT (item->>'form') = ANY(form_ids) THEN errs := errs || (path || ': form must name a form in this configuration'); END IF;
    IF NOT (jsonb_typeof(item->'recurrence'->'every') = 'number' AND (item->'recurrence'->>'every')::numeric BETWEEN 1 AND 365) THEN
      errs := errs || (path || ': recurrence.every must be 1 to 365');
    END IF;
    IF COALESCE(item->'recurrence'->>'unit', '') NOT IN ('day','week','month') THEN errs := errs || (path || ': recurrence.unit must be day, week or month'); END IF;
    IF COALESCE(item->>'due_time', '') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' THEN errs := errs || (path || ': due_time must be HH:MM'); END IF;
    IF NOT (jsonb_typeof(item->'open_hours_before_due') = 'number' AND (item->>'open_hours_before_due')::numeric BETWEEN 0 AND 336) THEN
      errs := errs || (path || ': open_hours_before_due must be 0 to 336');
    END IF;
    IF NOT (jsonb_typeof(item->'grace_hours') = 'number' AND (item->>'grace_hours')::numeric BETWEEN 0 AND 336) THEN
      errs := errs || (path || ': grace_hours must be 0 to 336');
    END IF;
    IF item ? 'late_acceptance_hours' AND NOT (jsonb_typeof(item->'late_acceptance_hours') = 'number' AND (item->>'late_acceptance_hours')::numeric BETWEEN 0 AND 720) THEN
      errs := errs || (path || ': late_acceptance_hours must be 0 to 720');
    END IF;
    IF item ? 'reminders' THEN
      IF COALESCE(item->'reminders'->>'channel', 'in_app') NOT IN ('in_app','email','whatsapp') THEN
        errs := errs || (path || '.reminders: channel must be in_app, email or whatsapp');
      END IF;
      IF EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(item->'reminders'->'hours_relative_to_due', '[]'::jsonb)) x
                 WHERE jsonb_typeof(x) <> 'number' OR x::numeric NOT BETWEEN -168 AND 168) THEN
        errs := errs || (path || '.reminders: hours_relative_to_due must be numbers from -168 to 168');
      END IF;
    END IF;
    IF item ? 'missed' THEN
      IF NOT (jsonb_typeof(item->'missed'->'after_consecutive') = 'number' AND (item->'missed'->>'after_consecutive')::numeric BETWEEN 1 AND 20) THEN
        errs := errs || (path || '.missed: after_consecutive must be 1 to 20');
      END IF;
      errs := errs || monitoring_validate_finding(item->'missed'->'finding', path || '.missed.finding');
      errs := errs || monitoring_validate_route(item->'missed'->'route', path || '.missed.route');
    END IF;
  END LOOP;

  -- rules
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'rules', '[]'::jsonb)) LOOP
    path := 'rules.' || COALESCE(item->>'id', '?');
    IF NOT (jsonb_typeof(item->'version') = 'number' AND (item->>'version')::numeric >= 1) THEN errs := errs || (path || ': version must be a positive number'); END IF;
    IF jsonb_typeof(item->'evaluate_on') <> 'array'
       OR EXISTS (SELECT 1 FROM jsonb_array_elements_text(item->'evaluate_on') x WHERE x NOT IN ('submission','schedule'))
       OR jsonb_array_length(item->'evaluate_on') = 0 THEN
      errs := errs || (path || ': evaluate_on must list submission and/or schedule');
    END IF;
    IF item ? 'applies_when' THEN
      IF NOT (item->'applies_when'->>'param') = ANY(monitoring_cfg_ids(cfg->'parameters')) THEN
        errs := errs || (path || '.applies_when: unknown parameter');
      END IF;
      IF jsonb_typeof(item->'applies_when'->'in') <> 'array' THEN errs := errs || (path || '.applies_when: needs an "in" list'); END IF;
    END IF;
    r := monitoring_validate_cond(cfg, item->'when', path || '.when', 0);
    errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
    IF (r->>'counts')::boolean AND NOT item ? 'sufficiency' THEN
      -- A count over a window is 0 both when nothing happened and when nothing was reported. Without
      -- a sufficiency clause the engine could read silence as "well controlled".
      errs := errs || (path || ': uses a count-style aggregate, so it must declare sufficiency (minimum reports in the window)');
    END IF;
    IF item ? 'sufficiency' THEN
      errs := errs || monitoring_validate_window(item->'sufficiency'->'window', path || '.sufficiency.window');
      IF NOT (jsonb_typeof(item->'sufficiency'->'min_reports') = 'number' AND (item->'sufficiency'->>'min_reports')::numeric BETWEEN 1 AND 400) THEN
        errs := errs || (path || '.sufficiency: min_reports must be 1 to 400');
      END IF;
      IF jsonb_typeof(item->'sufficiency'->'measurements') <> 'array'
         OR EXISTS (SELECT 1 FROM jsonb_array_elements_text(item->'sufficiency'->'measurements') x WHERE NOT x = ANY(measurement_ids)) THEN
        errs := errs || (path || '.sufficiency: measurements must list known measurements');
      END IF;
      IF item->'sufficiency' ? 'on_insufficient' THEN
        errs := errs || monitoring_validate_finding(item->'sufficiency'->'on_insufficient'->'finding', path || '.sufficiency.on_insufficient.finding');
        errs := errs || monitoring_validate_route(item->'sufficiency'->'on_insufficient'->'route', path || '.sufficiency.on_insufficient.route');
      END IF;
    END IF;
    errs := errs || monitoring_validate_finding(item->'finding', path || '.finding');
    errs := errs || monitoring_validate_route(item->'route', path || '.route');
    IF item ? 'patient_guidance' THEN
      IF COALESCE(item->'patient_guidance'->>'level', '') NOT IN ('emergency','advice') THEN errs := errs || (path || '.patient_guidance: level must be emergency or advice'); END IF;
      IF length(COALESCE(item->'patient_guidance'->>'text', '')) NOT BETWEEN 10 AND 1000 THEN errs := errs || (path || '.patient_guidance: text is required'); END IF;
      IF COALESCE(item->'patient_guidance'->>'code', '') !~ '^[a-z0-9][a-z0-9._-]{1,118}$' THEN errs := errs || (path || '.patient_guidance: code is required'); END IF;
    END IF;
  END LOOP;

  -- evaluation cadence and the local night window used by distinct_nights
  IF cfg ? 'evaluation' AND NOT (jsonb_typeof(cfg->'evaluation'->'schedule_every_hours') = 'number'
       AND (cfg->'evaluation'->>'schedule_every_hours')::numeric BETWEEN 1 AND 168) THEN
    errs := errs || 'evaluation.schedule_every_hours must be 1 to 168'::text;
  END IF;
  IF cfg ? 'night' AND NOT (jsonb_typeof(cfg->'night'->'start_hour') = 'number' AND (cfg->'night'->>'start_hour')::int BETWEEN 12 AND 23
       AND jsonb_typeof(cfg->'night'->'end_hour') = 'number' AND (cfg->'night'->>'end_hour')::int BETWEEN 1 AND 12) THEN
    errs := errs || 'night needs start_hour (12-23) and end_hour (1-12), local time'::text;
  END IF;

  -- display: cards are value expressions (the same bounded language as rules); charts name measurements
  FOR item IN SELECT * FROM jsonb_array_elements(COALESCE(cfg->'display'->'sections', '[]'::jsonb)) LOOP
    path := 'display.' || COALESCE(item->>'id', '?');
    IF length(COALESCE(item->>'title', '')) NOT BETWEEN 2 AND 160 THEN errs := errs || (path || ': title is required'); END IF;
    FOR inner_item IN SELECT * FROM jsonb_array_elements(COALESCE(item->'cards', '[]'::jsonb)) LOOP
      IF length(COALESCE(inner_item->>'label', '')) NOT BETWEEN 2 AND 160 THEN errs := errs || (path || '.cards: each card needs a label'); END IF;
      r := monitoring_validate_expr(cfg, inner_item->'value', path || '.cards.' || COALESCE(inner_item->>'id', '?'), 0);
      errs := errs || ARRAY(SELECT jsonb_array_elements_text(r->'errors'));
      IF inner_item ? 'target' AND NOT (inner_item->>'target') = ANY(monitoring_cfg_ids(cfg->'targets')) THEN
        errs := errs || (path || '.cards: unknown target');
      END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(item->'charts', '[]'::jsonb)) c, jsonb_array_elements_text(COALESCE(c->'measurements', '[]'::jsonb)) mm
               WHERE NOT mm = ANY(measurement_ids)) THEN
      errs := errs || (path || '.charts: unknown measurement');
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(item->'charts', '[]'::jsonb)) c
               WHERE NOT (jsonb_typeof(c->'days') = 'number' AND (c->>'days')::numeric BETWEEN 1 AND 365)) THEN
      errs := errs || (path || '.charts: days must be 1 to 365');
    END IF;
  END LOOP;

  RETURN jsonb_build_object('valid', cardinality(errs) = 0, 'errors', to_jsonb(errs));
END $$;

