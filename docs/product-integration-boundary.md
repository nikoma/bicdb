# Product and Integration Boundary

BicDB is an industry-neutral database and application substrate. Product and
vertical implementations consume its public APIs from sibling repositories;
they do not define the engine's public model.

## Repository direction

```text
bicdb-integrations  -> bicdb public APIs
product repositories -> bicdb public APIs
bicdb -X-> product or vertical repositories
```

The local `../bicdb-integrations` staging repository contains code extracted
while final product repository ownership is assigned. It is a real, buildable
workspace rather than a second copy of BicDB.

## Extracted components

| Component | New location | Public BicDB seam |
|---|---|---|
| Carrier Broker and its AMQP/MQTT/Kafka/HTTP/gRPC adapters | `../bicdb-integrations/crates/carrier-broker` | durable broker APIs, pgwire and runtime contracts |
| HL7 provider | `../bicdb-integrations/crates/bicdb-provider-hl7` | application/provider ABI |
| Healthcare terminology and FHIR built-ins | `../bicdb-integrations/crates/bicdb-provider-healthcare` | `ApplicationProgramHost` |
| Patient, appointment and clinical graph projections | `../bicdb-integrations/crates/bicdb-healthcare-projections` | `EventProjection` and `GraphProjection` |
| WalkNorth desktop/package integration | `../bicdb-integrations/products/bicdb-macos` | CLI, pgwire and Cell interfaces |
| ERP, EHR, WalkNorth fixtures, benchmarks and reports | `../bicdb-integrations/{fixtures,docs,reports,tests,archive}` | SQL, backup, benchmark and migration interfaces |

## Compatibility policy

Generic application-module contracts replace ERP-specific package concepts.
Readers continue accepting the legacy signed JSON keys `erp_modules`,
`erp_module_contract`, and `doctypes`, while new packages emit the neutral
`application_modules`, `application_module_contract`, and `record_types`
vocabulary.

Protected-data APIs replace PHI-specific Rust APIs and environment variables.
The old environment names and the `__bicdb_phi` on-disk marker remain read-only
compatibility aliases so extraction does not make existing data inaccessible.

Product-specific graph definitions are now supplied as serialized public
`GraphProjection` values. BicDB no longer selects a clinical graph internally.

## What remains public

The generic application runtime, broker mechanism, extension ABI, provider
interfaces, package verifier, graph/event projection APIs, SQL compatibility,
and secure Cell runtime remain in BicDB. These are database capabilities and
are usable without any product or commercial repository.
