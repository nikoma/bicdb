# Existing-cluster learner birth observation

This source-only checkpoint adds unpublished wire version 25. No local Cargo
build or execution has occurred; the associated control is unexecuted. It does
not establish rolling compatibility with prior wire versions.

`NativeUnassignedLearnerHost` accepts the actual fresh factory plus an owned,
live Host MQ observation whose requester is that registered unassigned learner.
It opens one actual data service/range store over the same DB. The listener
captures a provisional provider option permanently: only Ping and the dedicated
learner-birth exchange are permitted. Losing the weak provider cannot expose
metadata votes, arbitrary schema import, ordinary reads or writes.

The receiver retains its own original Host MQ business, cancellation and TTL.
Before published-topology/service/DB latches, it validates that original proof
and derives the actual caller identity from its committed member incarnation and
the presented TLS certificate. Request labels do not construct a participant.
It checks the captured actual open, immutable birth/incarnation, verified schema,
bootstrap/restore domain and requested scope. The existing local factory
producer then uses a nonwaiting DB borrow to revalidate actual ownership,
uninitialized metadata, no data assignment and the actual schema/birth. Its
configuration proposal is not transmitted or treated as authority.

The client consumes a distinct owned Host MQ observation for the real voter
requester. A private authenticated envelope seals the actual mTLS peer, original
business/challenge/context/request, destination and cancellation/deadline. The
opaque observation verifies all request scope against that caller's actual
metadata, including expected birth, learner role, no replicas, schema and
bootstrap/restore. It retains the actual presented peer for later current-pin
checks. Original caller cancellation cannot be replaced by a new token.

The receiver reports a remaining source-proof budget; the client conservatively
caps it from the beginning of its RPC, as well as its own original MQ/network
deadlines. Returned source context/term/read-index labels are audit facts, never
metadata ACKs or fresh authority. The resulting object is not Clone or
Deserialize and has no DTO/raw-parts constructor. Refreshing the receiver consumes
an independently fresh real Host MQ observation and does not revive older
observations.

The added control follows the genuine four-voter/fifth-learner setup: actual
factories, owned metadata stores, votes/Appends and mTLS Host MQ. It checks a real
birth response, cancellation under held service/DB latches, independent refresh,
unchanged local namespace identity, absent range floor and lack of writer hook,
then retains the old listener after dropping its provider and verifies refusal.
No RPC control result is claimed before execution.

Next enrollment must rebase these physical facts onto a separately fresh actual
metadata-leader configuration, checking the current exact learner incarnation,
actual presented pin, birth, schema and restore domain and refusing an existing
different birth. Neither the serialized response nor the local configuration
proposal is that authority. No enrollment/promotion/data grant is provided here.
Metadata initialization/catch-up requires a real stopped-and-drained same-service
transition; the host retains the stopping owner on drain errors. Joint data
placement, native recovery, Ready/Serving and the first ACK remain separate
operator-integration gates. Initial all-member-C bootstrap is not reused.
