// Statement identifiers refer to parameterized SQL in the signed manifest.
// Change this rule and publish/activate a new immutable version.
const REVIEW_VERIFIED = false;
interface Booking { id: string; appointment_id: string }
interface Eligibility { eligible: boolean; reference: string }
async function workflow(event: Booking) {
  const operationKey = "eligibility:" + event.id;
  if (db.scalar("operation_completed", [operationKey])) return {status: "already_completed"};
  const appointment = db.one("load_appointment", [event.appointment_id]);
  if (appointment.status !== "booked") return {status: "skipped"};
  const response = http.post("https://insurance.example/v1/eligibility", {
    headers: {
      Authorization: "Bearer " + secrets.get("insurance_api"),
      "Idempotency-Key": operationKey,
    },
    json: {member_id: appointment.member_id, payer_code: appointment.payer_code, service_date: appointment.starts_at},
    timeout_ms: 8000,
  });
  if (response.status === 429 || response.status >= 500) {
    jobs.retry({delay_seconds: 60});
    return {status: "retry_scheduled"};
  }
  if (response.status !== 200) throw new Error("Eligibility API rejected the request");
  const result: Eligibility = json.decode(response.body);
  if (typeof result.eligible !== "boolean" || typeof result.reference !== "string") throw new Error("Invalid eligibility result");
  const coverage = result.eligible ? "verified" : "needs_review";
  return db.transaction(tx => {
    const current = tx.one("lock_appointment", [event.appointment_id]);
    if (tx.scalar("operation_completed", [operationKey])) return {status: "already_completed"};
    if (current.status !== "booked" || current.revision !== appointment.revision) return {status: "stale_result"};
    tx.execute("insert_check", [operationKey, appointment.id, result.eligible, result.reference]);
    tx.execute("update_coverage", [coverage, appointment.id]);
    const staffReview = !result.eligible || REVIEW_VERIFIED;
    if (staffReview) tx.execute("insert_task", [operationKey, appointment.id, "Review insurance eligibility before the appointment"]);
    tx.execute("insert_notification", [operationKey, appointment.patient_id, result.eligible ? "coverage_verified" : "coverage_review", json.encode({appointment_id: appointment.id})]);
    tx.execute("complete_operation", [operationKey, appointment.id, coverage]);
    return {status: "completed", coverage, staff_review_created: staffReview};
  });
}
