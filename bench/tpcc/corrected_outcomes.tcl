# Applied after loadscript, identically for both engines. Requires workload v2.
# Count replies per terminal; database deltas independently verify real commits.
set neword_start [string first {proc neword } $_ED(package)]
set payment_start [string first {proc payment } $_ED(package)]
if {$neword_start < 0 || $payment_start <= $neword_start} {
    error "Cannot locate NewOrder/Payment procedures for outcome accounting"
}
set neword_body [string range $_ED(package) $neword_start [expr {$payment_start - 1}]]
set success_site "} else {\n                pg_result \$result -clear"
if {[llength [split $neword_body \n]] < 10 || [string first $success_site $neword_body] < 0
    || [string first $success_site $neword_body] != [string last $success_site $neword_body]} {
    error "Expected exactly one NewOrder success-result cleanup"
}
set outcome_hook {
                set outcome_row [pg_result $result -getTuple 0]
                if {[llength $outcome_row] != 6} {error "Unexpected NewOrder OUT columns"}
                set outcome_id [lindex $outcome_row 5]
                if {![string is integer -strict $outcome_id]} {error "Invalid NewOrder OUT ID"}
                if {$outcome_id == -1} {
                    incr ::bicdb_outcomes(invalid)
                } elseif {$outcome_id > 0} {
                    incr ::bicdb_outcomes(positive)
                } else {
                    incr ::bicdb_outcomes(other)
                }
                pg_result $result -clear
}
set replacement "} else {\n$outcome_hook"
set neword_body [string map [list $success_site $replacement] $neword_body]
set _ED(package) "[string range $_ED(package) 0 [expr {$neword_start - 1}]]$neword_body[string range $_ED(package) $payment_start end]"
# Install wrappers once in each active VU after all procedure definitions.
set wrapper_hook {
        array set ::bicdb_outcomes {neword 0 positive 0 invalid 0 other 0 payment 0}
        rename neword bicdb_original_neword
        proc neword {args} {
            incr ::bicdb_outcomes(neword)
            return [uplevel 1 [linsert $args 0 bicdb_original_neword]]
        }
        rename payment bicdb_original_payment
        proc payment {args} {
            incr ::bicdb_outcomes(payment)
            return [uplevel 1 [linsert $args 0 bicdb_original_payment]]
        }
}
set run_site {#RUN TPC-C}
if {[string first $run_site $_ED(package)] < 0
    || [string first $run_site $_ED(package)] != [string last $run_site $_ED(package)]} {
    error "Expected exactly one active-VU run initialization"
}
set _ED(package) [string map [list $run_site "$wrapper_hook\n$run_site"] $_ED(package)]
set disconnect {pg_disconnect $lda}
set end_pos [string last $disconnect $_ED(package)]
if {$end_pos < 0} {error "Missing active-VU disconnect"}
set end_pos [expr {$end_pos + [string length $disconnect] - 1}]
set report_hook {
        puts "BICDB_OUTCOMES $myposition $::bicdb_outcomes(neword) $::bicdb_outcomes(positive) $::bicdb_outcomes(invalid) $::bicdb_outcomes(other) $::bicdb_outcomes(payment)"
}
set _ED(package) "[string range $_ED(package) 0 $end_pos]\n$report_hook[string range $_ED(package) [expr {$end_pos + 1}] end]"
