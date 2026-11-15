# Metadata rebind component: retain full response validation

Public **1.0.520-beta / `d3af39c107951802a612d31b42e70fd4bc90e71c`**
all-target job `132945355208` failed with one actual `E0624`: the real mTLS
rebind component called `MetadataReadAcquireResponse::validate` from a sibling
module, while that method was private to `distribution_consensus`. The full raw
log SHA256 is
`2ee177da3d88f9a99bda3abc8148b3dc02610f3b8c4eab8802f1933f580c25f9`.
Zero test cases executed; the no-Core-defaults compilation step was skipped.
This compile exposed neither of e2's previous two library errors, but does not
establish a passing compiler or runtime result for d3.

The separate correction changes only the full DATA validator's visibility to
`pub(super)`, its actual crate ancestor. The existing rebind test still calls
that exact method with the original request and retains every assertion. The
validator still performs its complete quorum/correlation/requester/lifecycle
checks. No check is deleted or duplicated into a weaker test surrogate.

`MetadataReadProvider::acquire` already obtains its actual authority from
the live consensus/provider round, then consumes it through
`MetadataReadAuthority::into_acquisition_response`; that producer already calls
the same full validator. `MetadataReadObservation::from_transport` also retains
its independent actual peer/source/deadline validation. Their bodies are
unchanged. Validating decoded response DATA returns only `Result<()>`; it does
not construct an authority, observation, Role, OWN, Ready or recipient scope.
The method is not externally public, and no owner fields or constructors are
exposed. The test does not manufacture a transport observation merely to bypass
privacy.

This child follows the independently frozen allocation test-cut correction;
the two units remain separate source findings. Only standalone formatting,
diff, full-file inverse and unchanged-caller/validator-body checks were run.
No Cargo/compiler, Rust test, CI action, source push, version change, physical
qualification or deadline/assertion change was performed. The original live d3
run and its retained evidence remain unchanged.
