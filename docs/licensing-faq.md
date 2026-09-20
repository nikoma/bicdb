# BicDB: Apache 2.0 + three exceptions

**Can I build my business on BicDB? Yes.** Closed-source applications, ERP SaaS,
private modifications, consulting, support and community forks are allowed.
Your SaaS can have 10 million customers and 100,000 BicDB instances. No revenue,
employee, customer, tenant, workspace or instance threshold applies.

The boundary is what the recipient is supplied: **an application that uses a
database, or a database product/service**.

The [BicDB License](../LICENSE) preserves the Apache 2.0 text verbatim and adds
three exceptions: managed database services, commercial database products/OEM,
and white-label removal of attribution from BicDB's included console. It is
source-available, not unmodified Apache-2.0 or OSI-approved open source.
The operative license governs if this explanation differs.

## Three things that require a commercial license

1. Hosting/managing BicDB databases for other people's chosen workloads, including
   free tiers, dedicated containers and customer-cloud managed database offerings.
2. Selling BicDB itself as a database engine, appliance or platform. Renaming it
   MegaDB, adding an MCP/REST adapter or putting a thin UI around it does not avoid this.
3. Removing BicDB origin attribution from its own included administration console
   when redistributing it for a white-label database offering.

AWS cannot use covered BicDB code to launch an Amazon-branded managed database
without authorization. An independent implementation that does not copy covered
code is outside this license. Building an ERP on BicDB is permitted.

## Modifications, forks and proprietary applications

**There is no requirement to license your modifications under the same license.**
Apache section 4 permits different terms for your own modifications and derivatives
as a whole, while the underlying BicDB code keeps its applicable conditions.
Private changes need not be published, and object-only distribution is permitted.
Mere linking, embedding and API use do not impose BicDB's license on independent
application code. Your application may remain proprietary and use its own branding.

When distributing covered BicDB code, include the complete LICENSE, relevant
scope and notices, and mark modified files as Apache section 4 requires. Passing
on the underlying license does not force your independent additions under it.
Your terms cannot grant customers reserved rights to BicDB code you do not control.
Ordinary community forks, mirrors and registries remain permitted, including
incidental bandwidth/media charges. Selling database-product rights remains reserved.

## Scenario review

“Exception” references the three conditions preceding the verbatim Apache text.
“Apache §4” refers to that text as incorporated in the complete BicDB License.
These outcomes concern covered rights, not independently available earlier grants.

| Scenario | Outcome | Applicable text |
| --- | --- | --- |
| School stores student records in BicDB | Permitted under community terms | Permissions paragraph; Apache §2 |
| EHR SaaS serves 5,000 clinics using tenant-isolated BicDB storage | Permitted under community terms | Permissions paragraph; exception 1 application boundary |
| Company runs 300 internal instances for subsidiaries and departments | Permitted under community terms | Permissions paragraph |
| Paid desktop app bundles BicDB for local application storage | Permitted under community terms | Exception 2 application embedding; Apache §4 |
| Ecommerce platform uses BicDB and offers ordinary product search/reporting APIs | Permitted under community terms | Exception 1 application boundary |
| Customer builds an independent dashboard using BicDB APIs with its own branding | Permitted under community terms | Exception 3 independent applications |
| Consultant is paid to migrate and administer a customer's deployment | Permitted under community terms | Exception 1 customer-directed administration |
| Vendor sells support for community builds without supplying reserved hosting/product rights | Permitted under community terms | Exception 2 support |
| Community publishes a clearly identified fork with required notices and terms | Permitted under community terms | Exceptions 2–3; Apache §4 |
| Generic infrastructure provider supplies VMs on which customers independently install BicDB | Permitted under community terms | Exception 1 does not cover generic compute alone |
| Hosting company offers BicDB databases to subscribers | Separate commercial agreement required | Exception 1 |
| Provider uses one isolated BicDB container per customer as its managed database offering | Separate commercial agreement required | Exception 1 |
| Vendor offers a managed database product deployed into each customer's cloud account | Separate commercial agreement required | Exception 1 |
| Vendor proxies BicDB through MCP/REST and sells general-purpose database access | Separate commercial agreement required | Exception 1 |
| Cloud provider offers a free BicDB database tier within its commercial platform | Separate commercial agreement required | Exception 1 |
| Competitor renames BicDB and sells it as a database engine | Separate commercial agreement required | Exception 2 |
| Appliance primarily supplies BicDB database functionality with a thin proprietary wrapper | Separate commercial agreement required | Exception 2 |
| Reseller removes BicDB identification from the included BicDB console for white-label distribution | Separate commercial agreement required | Exception 3 |

## Borderline cases

A backend-as-a-service supplying arbitrary schemas and general-purpose storage/query
operations is a managed database service, even alongside auth or functions. Custom
fields, application-specific imports, reports and business APIs alone do not turn a
substantive application into one. Internal users may choose any schemas they need.

Customer-cloud ownership alone does not exempt a vendor's managed database product;
customer-directed consulting is allowed even with administrative access, recurring
fees or automation. Generic VM hosting alone does not supply a BicDB database.

Ongoing free hosting of arbitrary third-party databases is reserved, including
community/nonprofit hosting. Constrained sample-data demos and temporary evaluations
are permitted. This preserves the existing hosting boundary without banning a
nonprofit's own use.

Independent products need no logo or powered-by badge. Fork names, accessibility,
translation and styling changes are allowed with clear origin attribution in the
redistributed BicDB console. No unused frontend must be recreated. Third-party
credits survive a commercial waiver of first-party branding.

## Scope, earlier copies and authorization

See [LICENSE-SCOPE.md](../LICENSE-SCOPE.md) for mixed-license boundaries and
[commercial licensing](commercial-licensing.md#request-commercial-authorization)
for the inquiry route and requirements for written authorization.

Existing Apache copies and copies under the earlier Community License retain their
respective grants. This new license identity does not automatically amend them.
No claim is made that earlier Apache rights can be revoked or that independent
implementations can be prevented. The [transition record](licensing-transition.md)
distinguishes those rights from the terms offered with the current source.
