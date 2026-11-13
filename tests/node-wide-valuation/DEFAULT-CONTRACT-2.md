These are independent specifications and existing-runtime captures, not executed
qualification of the new Rust leaves or ERP producers. No Rust tests, helpers or
builds were run. The original approved APIs, vectors and limits remain unchanged.

The newly authored Core exports are `exact_prorate_native_units_value(&[Value])`,
`nearest_native_full_display_ratio_value(&[Value])` and
`exact_units_from_limbs_value(&[Value])`, each returning `Result<Value>`.

Default proration accepts `(String Cunits,String Qatoms,Decimal q,Int p,Int s)`;
p/s are0..28, canonical unsigned C/Q strings have at most77digits and magnitude
below2^254, Q>0, q>0 and integral at p, qAtoms<=Q. Partial cost is exact
Cunits*qAtoms/(Qatoms*10^s), rounded once midpoint-even to the greatest native
scale whose coefficient fits96bits. Full depletion must admit C EXACTLY and
cannot round away residual value. A partial rounded result exceeding the exact
available C is explicitly refused; it is never clamped or rerounded. The exact
coefficient/common-power comparison permits s0,C1,Q3,q1 to return native1/3,
which is finer than its input unit scale. The real DEFAULT producer uses fixed
scale28 for its cost basis/residual, so every admitted native issue re-encodes
exactly. A narrower source-UOM allowed precision is a separate authentic unit
policy, not the aggregate encoding scale.

The helper-level conservation refusal is an explicit amendment to frozen private
DEFAULT-CONTRACT-1, approved by the integration owner. Its adversary is
Cunits=MAX96*10^28−1, Qatoms=MAX96*10^28+1, q=MAX96,p=s=28: nearest-native alone
would round to MAX96, exceeding available cost by one scale28 unit. Native
source conservation/authority remains mandatory independently of this pure leaf.

Full display accepts `(String Cunits,String Qatoms,Int p,Int s)`, p/s0..28,
and nearest-rounds Cunits*10^p/(Qatoms*10^s). This is explicitly
nonauthoritative and is separate from the unchanged approved display API. Rate
overflow requires truthful V2 rational lineage plus a nullable/unrepresentable
display contract or an explicit business refusal, never a fake old Decimal0.

The codec has exactly SIX actual Int64 arguments, highest limb first, each
0<=limb<10^14; output is canonical unsigned decimal with magnitude<2^254.
It has no source/tenant/order/posting authority. Native invariant execution must
check real Int types and bind every input limb to real owned numeric evidence.
Raw JSON2.0/2e0 kind checks are separate vectors because JavaScript parsing erases
that lexical distinction. A claimed field type or decimal mirror alone is not
proof. The Core additions do not wire any invariant, ABI, compiler or SQL path.

Per native input term at scale28, atoms<2^190. Abstract u64-count history totals
are<2^254. Proration's C*qAtoms<2^444 and output-scale numerator<2^538; use bounded
BigInt, not U256. These are theoretical arithmetic bounds. No list/fuel/body/time
limit changes or actual batch-capacity claims follow. Typed DB all-history
SumUnits over genuine owner rows plus pending terms remains required.

`default-ordinary-b998.json` captures95 real old multiply-first issue results,
from380 authenticated pure program observations on b998/Carrier344. The73
ordinary agreements,10 accepted rounded-product corrections,6 old intermediate
overflows and6 invalid bases are tested separately. In particular old
C3e−28,q.05,Q.1 returns0; the new exact rational rounds to2e−28. The fixture has
no ERP resource, ledger, source-proof, new primitive or display-schema authority.
Old lossless-product cases are equivalent; the rounded/overflow cases are
explicit corrective/new domains. Historical facts are not relabeled or adopted.
