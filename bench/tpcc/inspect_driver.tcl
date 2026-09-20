# Read-only inspection: generate the standard client driver without running it.
dbset db pg
dbset bm TPC-C
diset tpcc pg_driver timed
diset tpcc pg_allwarehouse false
loadscript
print script
if {[info exists env(BICDB_INSPECT_DRIVER_INTERNALS)]} {
    foreach command {print loadscript vucreate customscript} {
        puts "INSPECT_COMMAND $command"
        if {![catch {info body $command} body]} { puts $body }
    }
}
if {[info exists env(BICDB_INSPECT_RANDOM)]} {
    package require tpcccommon
    foreach command [info procs ::tpcccommon::*] {
        set body [info body $command]
        if {[string match *RandomNumber $command] || [string match *srand* $body]} {
            puts "INSPECT_RANDOM $command"
            puts $body
        }
    }
}
exit 0
