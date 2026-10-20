// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
// SPDX-License-Identifier: Apache-2.0

//! Service self-registration data for Trade8:Router.
//!
//! Output via `--print-registration`. Scopes per master design §9; pages
//! per router design §6.

use utoipa::OpenApi;

use sentin8::registration::{
    AdminScope, AdminUi, AdminUiPage, CrossSchemaGrant, ErrorCode, M2mGrant, PgRole, PolicySetDecl,
    Registration, RelationDecl, SqlMigration,
};

/// The Shield text of the declared scope-mirror set - public so the
/// policy wiring tests assert against the REAL seed.
pub const SCOPE_MIRROR_POLICIES: &str = "permit(principal, action == Action::\"trade8:router:executions:report\", resource) when { principal.scopes.contains(\"trade8:router:executions:report\") };\npermit(principal, action == Action::\"trade8:router:executions:report\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };\npermit(principal, action == Action::\"trade8:router:executions:report\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };\npermit(principal, action == Action::\"trade8:router:executions:report\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };\npermit(principal, action == Action::\"trade8:router:journal:read\", resource) when { principal.scopes.contains(\"trade8:router:journal:read\") };\npermit(principal, action == Action::\"trade8:router:journal:read\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };\npermit(principal, action == Action::\"trade8:router:journal:read\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };\npermit(principal, action == Action::\"trade8:router:journal:read\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };\npermit(principal, action == Action::\"trade8:router:session:manage\", resource) when { principal.scopes.contains(\"trade8:router:session:manage\") };\npermit(principal, action == Action::\"trade8:router:session:manage\", resource) when { principal.scopes.contains(\"trade8:router:session:read\") };\npermit(principal, action == Action::\"trade8:router:session:manage\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };\npermit(principal, action == Action::\"trade8:router:session:manage\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };\npermit(principal, action == Action::\"trade8:router:session:manage\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };\npermit(principal, action == Action::\"trade8:router:session:read\", resource) when { principal.scopes.contains(\"trade8:router:session:read\") };\npermit(principal, action == Action::\"trade8:router:session:read\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };\npermit(principal, action == Action::\"trade8:router:session:read\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };\npermit(principal, action == Action::\"trade8:router:session:read\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };";

/// Default declarative policy set (Sentin8:Policy design §3.2/§3.4,
/// seeded by `vertical enroll` as `origin='platform'`, `mode='enforced'`):
/// action-granular scope permits over the LIVE effective scope set - the
/// exact mirror of the gates this service's handlers evaluate in step 5
/// (policy design §4.3). Enforced: step 5 is the
/// only gate; shadow is the emergency one-UPDATE rollback lever only.
const POLICIES: &[PolicySetDecl] = &[PolicySetDecl {
    policy_set_name: "scope-mirror",
    description: "Action-granular scope permits for trade8:router over the LIVE effective scope set (design §3.1d). Enforced (enforced) - step 5 is the only gate.",
    // Shield text lives in SCOPE_MIRROR_POLICIES (single source).
    policies: SCOPE_MIRROR_POLICIES,
    schema: None,
    mode: "enforced",
    default_enabled: true,
}];

/// Default ReBAC relation tuples (design §3.1c) - none declared by this
/// service today; env admins author their own graph (office/trader,
/// entitlement edges) via the relations admin API.
const RELATIONS: &[RelationDecl] = &[];

const PG_ROLES: &[PgRole] = &[PgRole {
    scope: "trade8:router:session:manage,trade8:router:session:read,trade8:router:journal:read,trade8:cron:run",
    role: "trade8_router_service",
    bypass_rls: false,
    database: "trade8",
        // Declared SELECT-only reads (enroll applies them in a post-loop
        // pass - never blanket schema grants; data-model §1.1).
    cross_schema_grants: &[
        CrossSchemaGrant { schema: "desk", table: "destination", privilege: "SELECT" },
        CrossSchemaGrant { schema: "desk", table: "gateway", privilege: "SELECT" },
        CrossSchemaGrant { schema: "desk", table: "market_gateway_destination", privilege: "SELECT" },
        CrossSchemaGrant { schema: "desk", table: "office_broker", privilege: "SELECT" },
        CrossSchemaGrant { schema: "desk", table: "msg_template", privilege: "SELECT" },
    ],
}];

const ADMIN_SCOPES: &[AdminScope] = &[
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:router:session:manage",
    },
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:router:session:read",
    },
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:router:journal:read",
    },
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:destination:manage",
    },
    // Policy-admin surface (policy design §3.6; principles §6.0b per-service scope).
    AdminScope::Typed {
        role: "trade8:router:admin",
        scope: "trade8:router:policy:read",
    },
    AdminScope::Typed {
        role: "trade8:router:admin",
        scope: "trade8:router:policy:write",
    },
];

const ERROR_CODES: &[ErrorCode] = &[
    ErrorCode {
        enum_name: "Trade8GatewayReject",
        error_code: "trade8_gateway_reject",
        status_code: 422,
    },
    ErrorCode {
        enum_name: "Trade8SessionDown",
        error_code: "trade8_session_down",
        status_code: 409,
    },
];

const ADMIN_UI_PAGES: &[AdminUiPage] = &[
    AdminUiPage {
        route: "sessions",
        label: "Sessions",
        ui_endpoint: "/v1/admin/ui/sessions",
        data_endpoint: "/v1/admin/sessions",
        icon: "0xe8b3",
    },
    AdminUiPage {
        route: "destinations",
        label: "Destinations",
        ui_endpoint: "/v1/admin/ui/destinations",
        data_endpoint: "/v1/admin/destinations",
        icon: "0xe8d2",
    },
    AdminUiPage {
        route: "templates",
        label: "Templates",
        ui_endpoint: "/v1/admin/ui/templates",
        data_endpoint: "/v1/admin/templates",
        icon: "0xe873",
    },
    AdminUiPage {
        route: "fees",
        label: "Fees",
        ui_endpoint: "/v1/admin/ui/fees",
        data_endpoint: "/v1/admin/fees",
        icon: "0xe870",
    },
    AdminUiPage {
        route: "journal",
        label: "Journal",
        ui_endpoint: "/v1/admin/ui/journal",
        data_endpoint: "/v1/admin/messages",
        icon: "0xe8b6",
    },
    AdminUiPage {
        route: "policy-sets",
        label: "Policy Sets",
        ui_endpoint: "/v1/admin/ui/policy-sets",
        data_endpoint: "/v1/admin/policy-sets",
        icon: "0xe8b8",
    },
    AdminUiPage {
        route: "relations",
        label: "Relations",
        ui_endpoint: "/v1/admin/ui/relations",
        data_endpoint: "/v1/admin/relations",
        icon: "0xe572",
    },
    AdminUiPage {
        route: "attributes",
        label: "Attributes",
        ui_endpoint: "/v1/admin/ui/attributes",
        data_endpoint: "/v1/admin/attributes",
        icon: "0xe8d3",
    },
];

/// Workload scope grants (self-registration design §3.11). One
/// entry per workload this service presents a certificate for - the
/// fail-closed registry map admits only these. Scope values use the
/// full vocabulary form (bare `bus:consume` fails the map's superset
/// check).
const M2M_GRANTS: &[M2mGrant] = &[
    // The service's own workload identity: the shell 8b outbox relay +
    // the direct callers. Colon + short spellings both granted - the
    // registry map looks up the exact workload name each call site
    // presents a certificate for.
    M2mGrant {
        workload: "trade8:router",
        scopes: &[
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "router",
        scopes: &[
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    // Shell audit-journal pusher workload (audit crate §6.8; boot step 8c).
    M2mGrant {
        workload: "router-audit",
        scopes: &["sentin8:sys:audit:write"],
    },
    // Shield hot-reload consumer (policy crate `policy_bus_client`,
    // full colon identity) + stat emitter (stat_emit short form).
    M2mGrant {
        workload: "trade8:router-policy",
        scopes: &["sentin8:bus:consume", "sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "router-policy",
        scopes: &["sentin8:stat:measure"],
    },
    // TLS-RLS-4 (VDC §VDC.7): the database-free HostContextResolver's
    // resolve workload - cache miss calls Sys `GET /v1/resolve/hostname`.
    M2mGrant {
        workload: "trade8-router-tls",
        scopes: &["sentin8:sys:instances:read"],
    },
    // Sentin8:Cron journal-retention schedule registration at boot
    // (trade8-client cron.rs: the `{service_type}-cron` workload
    // certificate; POST /v1/schedules).
    M2mGrant {
        workload: "trade8:router-cron",
        scopes: &["sentin8:cron:write"],
    },
];

const SQL_1_INIT: &str = include_str!("../sql/init.sql");

fn empty_openapi() -> serde_json::Value {
    serde_json::to_value(crate::openapi::RouterOpenApi::openapi())
        .unwrap_or(serde_json::Value::Null)
}

pub fn print(section: Option<String>) -> anyhow::Result<()> {
    let reg = Registration {
        service_type: "trade8:router",
        subdomain: "trade8-router",
        health_path: "/v1/health",
        admin_scopes: ADMIN_SCOPES,
        error_codes: ERROR_CODES,
        consent_providers: &[],
        consent_flows: &[],
        admin_ui: Some(AdminUi {
            label: "Router",
            icon: "0xe8d2",
            service_flag: "trade8-router",
            pages: ADMIN_UI_PAGES,
        }),
        sql_migrations: vec![SqlMigration {
            version: 1,
            name: "init",
            sql: SQL_1_INIT,
        }],
        pg_roles: PG_ROLES,
        audit_findings: None,
        service_actions: None,
        m2m_grants: Some(M2M_GRANTS),
        openapi_url: "/openapi.json",
        openapi: empty_openapi,
        policies: POLICIES,
        relations: RELATIONS,
        event_triggers: &[],
        schedules: &[],
    };
    sentin8::registration::print_registration(section, &reg, empty_openapi)
}
