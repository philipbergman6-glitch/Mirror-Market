# Physical-soy intelligence: competitive review

- Research date: 2026-11-05; linked sources accessed on that date.
- Question: What open-source and adjacent products already serve a daily physical-soy buyer, and where could Mirror-Market add value?
- Method: First-party repositories, documentation and product pages. Commercial capabilities below are vendor-described, not independently benchmarked. No competitor was installed or trialled. This is research, not a second capability-status record; project evidence remains in [ARCHITECTURE.md](../ARCHITECTURE.md#capability-status).

The defensible proposition is a focused daily soy decision workflow with explicit price meaning and missing-input reasons. This review does **not** establish that Mirror-Market is unique, better than commercial products, or operationally ready for a buyer's book. Generic open-source financial infrastructure already exists; commercial vendors describe substantially deeper physical-market and execution coverage.

## OpenBB has changed: verify the current repositories

OpenBB's official site announces its company's wind-down and the release of its product suite, with an October 1, 2026 update naming OpenBQ as a steward and FINOS for Workspace. Earlier pricing pages and documentation describing Workspace as a proprietary commercial product should not be treated as the current licensing authority. [OpenBB announcement](https://openbb.co/)

The former OpenBB repository redirects to `openbq-org/OpenBB`. Its README describes data integration across proprietary, licensed and public sources, exposing them through Python, REST, MCP and downstream applications. The current `develop` LICENSE explicitly specifies Apache-2.0. This is the observed branch license, not a claim about every historical release. [ODP repository](https://github.com/openbq-org/OpenBB), [current license](https://raw.githubusercontent.com/openbq-org/OpenBB/develop/LICENSE)

FINOS Workspace is now a public Apache-2.0 repository. Its documented features include configurable dashboards, data backends, AI agents, team sharing, user/role administration and plugins. Its quick start requires a running backend/frontend stack and user setup. The FINOS contribution issue supplies the historical donation context; the public repository provides stronger evidence of today's released code. [Workspace repository](https://github.com/finos-labs/finos-workspace), [contribution record](https://github.com/finos/community/issues/445)

**Implication, inferred:** OpenBB/FINOS can supply a general financial workspace or integration foundation. The reviewed documentation does not demonstrate an out-of-the-box soy cargo workflow with named loading months, synchronized crush legs, bounded historical FX conversion, or Mirror-Market's specific public/private boundary. That is a limited documentary finding, not proof those capabilities cannot be built or already exist in extensions. Replacing this project's static delivery with that stack would need a concrete user benefit and an operational-cost comparison.

## Comparison by buyer task

| Product or source | Verified scope | Relationship to Mirror-Market |
|---|---|---|
| OpenBB ODP / FINOS Workspace | Open-source integration and configurable analyst workspace, as documented above | Potential infrastructure or presentation alternative; soy-specific semantics still require evidence and implementation. |
| `rnassqs` | R client for USDA NASS Quick Stats; documentation also states its NASS non-endorsement notice. [Maintainer documentation](https://docs.ropensci.org/rnassqs/) | Reusable agricultural data access, rather than evidence of a complete daily buyer application. |
| USDA FAS PSD | Official commodity statistics, downloadable datasets and API access; API describes release-date queries and possible revisions across years. [PSD](https://fas.usda.gov/psdonline), [API](https://apps.fas.usda.gov/opendataweb/home) | Direct alternative for an analyst who needs fundamentals in a spreadsheet; also an upstream input. Data access is not the same as a buyer workflow. |
| AMIS Market Monitor | Periodic synthesis for wheat, maize, rice and soybeans; the 2026 schedule has ten publication dates. [Monitor and schedule](https://www.amis-outlook.org/market-monitor) | Strong public context. Its documented publication cadence differs from a daily cargo/basis workflow. Do not label a publicly readable report an open-source application. |
| Barchart cmdtyView | Vendor describes cash bids, basis tracking, producer offers, hedge execution, accounting integration and electronic contracting. [Grain workflow](https://www.barchart.com/solutions/software/cmdtyview/solutions/grain) | A closer commercial comparison for physical grain work than a generic stock terminal. It covers activities Mirror-Market explicitly excludes. |
| Kpler agriculture/dry analytics | Vendor describes grain/oilseed flows using AIS, ports, customs and human contributors, delivered through terminal/API and other integrations. [Product description](https://www.kpler.com/solutions/fundamental-intelligence/ags-metals-dry) | Commercial physical-flow intelligence beyond this project's public-data remit. Coverage, accuracy, rights and price need a buyer-specific evaluation. |
| Bloomberg | Official materials describe private commodity-data contribution with custom permissions, including agricultural products; access is offered via sales/demo contact. [Commodity-data brochure](https://assets.bbhub.io/professional/sites/10/Proprietary-Commodity-Data-on-Bloomberg-Brochure.pdf), [access page](https://professional.bloomberg.com/products/bloomberg-terminal/access/) | Broad commercial benchmark; private-data support alone is not a differentiator for Mirror-Market. |

Barchart also documents US/Canadian elevator bids, historical bid/basis charts and proprietary grain assessments with forward delivery periods. These are specific benchmarks for a buyer comparison, not capabilities that a public government-data aggregation should imply it replaces. [Barchart grain layers](https://barchartsolutions.atlassian.net/wiki/spaces/CMDTYV/pages/1953923113/Grain%2BLayers?atl_f=content-tree)

## What this project can credibly differentiate

The repository's mission and invariants specify daily physical-soy use, shared price semantics, USD/MT presentation, conversion at eligible historical FX dates, same-session spread/crush calculations and explicit withholding. They also specify private desk files outside the published site. These are concrete design choices worth testing with buyers, rather than claims of competitor inferiority. [CLAUDE.md](../CLAUDE.md)

The architecture evidence limits the claim: the public origin board has been observed withholding every landed total pending assumptions; private book and costed-route workflows have fixture tests but no recorded real desk use; the physical-buyer trial has not started. Thus “ready-made landed-cost intelligence” or “proven desk decision support” would overstate the available evidence. Refer to the canonical rows for subsequent changes. [Capability status](../ARCHITECTURE.md#capability-status)

**Research recommendation:** validate one real daily comparison—same commodity/specification, loading month and destination—against the buyer's existing spreadsheet and information subscriptions. Measure time saved, valid comparable routes, explanations for withheld results and decision changes. A convincing outcome would establish a useful narrow product even if the buyer retains cmdtyView, Kpler or Bloomberg. Adding generic financial-terminal breadth does not itself test that proposition.

## Rights, pricing and remaining uncertainty

An open-source software license covers that software; it does not establish redistribution rights for each connected data feed. ODP explicitly integrates licensed as well as public data. Likewise, an accessible agricultural report does not establish that every embedded third-party series may be republished. This project already requires source-specific publication rights under invariant 9. [ODP README](https://github.com/openbq-org/OpenBB), [project invariants](../CLAUDE.md)

No current official Bloomberg subscription price was verified. Its reviewed access page provides a demo/contact route, not a public price schedule. Do not present a remembered annual dollar figure, savings percentage, or all-in replacement-cost comparison as fact. Obtain a quote for the actual users, exchanges and entitlements before making a procurement comparison. [Bloomberg access](https://professional.bloomberg.com/products/bloomberg-terminal/access/)

The search is not exhaustive and establishes neither an absence of direct open-source competitors nor a market-size estimate. It did not audit competitor reliability, data contracts, deployment security or user outcomes. Current repository license observations and first-party feature descriptions should be revisited before a build-versus-buy decision.
