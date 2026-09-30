-- Workflow body. Parameterized statement identifiers come from the manifest.
local review_verified = false
local operation_key = 'eligibility:' .. event.id
if db.scalar('operation_completed', {operation_key}) then return {status='already_completed'} end
local appointment = db.one('load_appointment', {event.appointment_id})
if appointment.status ~= 'booked' then return {status='skipped'} end
local response = http.post('https://insurance.example/v1/eligibility', {
  headers = {Authorization='Bearer ' .. secrets.get('insurance_api'), ['Idempotency-Key']=operation_key},
  json = {member_id=appointment.member_id, payer_code=appointment.payer_code, service_date=appointment.starts_at},
  timeout_ms = 8000
})
if response.status == 429 or response.status >= 500 then
  jobs.retry({delay_seconds=60})
  return {status='retry_scheduled'}
end
assert(response.status == 200, 'Eligibility API rejected the request')
local result = json.decode(response.body)
assert(type(result.eligible) == 'boolean' and type(result.reference) == 'string', 'Invalid eligibility result')
local coverage = result.eligible and 'verified' or 'needs_review'
return db.transaction(function(tx)
  local current = tx.one('lock_appointment', {event.appointment_id})
  if tx.scalar('operation_completed', {operation_key}) then return {status='already_completed'} end
  if current.status ~= 'booked' or current.revision ~= appointment.revision then return {status='stale_result'} end
  tx.execute('insert_check', {operation_key, appointment.id, result.eligible, result.reference})
  tx.execute('update_coverage', {coverage, appointment.id})
  local staff_review = not result.eligible or review_verified
  if staff_review then tx.execute('insert_task', {operation_key, appointment.id, 'Review insurance eligibility before the appointment'}) end
  tx.execute('insert_notification', {operation_key, appointment.patient_id, result.eligible and 'coverage_verified' or 'coverage_review', json.encode({appointment_id=appointment.id})})
  tx.execute('complete_operation', {operation_key, appointment.id, coverage})
  return {status='completed', coverage=coverage, staff_review_created=staff_review}
end)
