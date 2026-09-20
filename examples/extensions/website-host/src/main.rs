use std::{
    env,
    error::Error,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::post,
    Router,
};
use bicdb_core::BicDb;
use bicdb_extension::{
    host::{ExtensionPackageStore, ExtensionRuntime, WasmExtension, WasmHostConfig},
    http::{extension_router, ExtensionHttpConfig},
    ExtensionState,
};
use bicdb_sql::{
    load_extension, load_website, load_website_release, sync_extension_runtime, SqlSession,
};

const EXTENSION_NAME: &str = "website_renderer";
const WEBSITE_NAME: &str = "bicdb_live";
const RETRO_VERSION: &str = "0.1.0";
const MODERN_VERSION: &str = "1.1.0";
const RETRO_SITE: &str = include_str!("../../website-renderer/site-v1.json");
const MODERN_SITE: &str = include_str!("../../website-renderer/site-v2.json");

#[derive(Clone)]
struct DemoState {
    db: Arc<Mutex<BicDb>>,
    runtime: ExtensionRuntime,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address =
        env::var("BICDB_WEBSITE_ADDRESS").unwrap_or_else(|_| "127.0.0.1:4173".to_string());
    let data_path = env::var_os("BICDB_WEBSITE_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./bicdb-website-demo-data"));
    let wasm_path = env::var_os("BICDB_WEBSITE_WASM")
        .map(PathBuf::from)
        .unwrap_or_else(default_wasm_path);

    let mut db = BicDb::open(&data_path)?;
    let host_config = WasmHostConfig::default();
    let packages = ExtensionPackageStore::open(db.data_path().join("extensions/packages"))?;
    install_renderer(&mut db, &packages, &host_config, &wasm_path)?;
    install_websites(&mut db)?;

    let runtime = ExtensionRuntime::new(packages, host_config)?;
    sync_extension_runtime(&db, &runtime)?;
    let active_version = load_website(&db, WEBSITE_NAME)?
        .and_then(|website| website.active_version)
        .unwrap_or_else(|| "none".to_string());
    let state = DemoState {
        db: Arc::new(Mutex::new(db)),
        runtime: runtime.clone(),
    };
    let controls = Router::new()
        .route("/__bicdb_demo/rollback", post(activate_retro))
        .route("/__bicdb_demo/modern", post(activate_modern))
        .with_state(state);
    let app = controls.merge(extension_router(runtime, ExtensionHttpConfig::default()));
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!(
        "BicDB website {WEBSITE_NAME}@{active_version} is serving on http://{address} \
         (data: {})",
        data_path.display()
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn default_wasm_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../../target/wasm32-unknown-unknown/release/\
         bicdb_website_renderer_example.wasm",
    )
}

fn install_renderer(
    db: &mut BicDb,
    packages: &ExtensionPackageStore,
    host_config: &WasmHostConfig,
    wasm_path: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let module_bytes = fs::read(wasm_path)?;
    let module_sha256 = packages.install(&module_bytes, host_config.max_module_bytes)?;
    let module = WasmExtension::load(&module_bytes, host_config.clone())?;
    let manifest_json = sql_literal(&serde_json::to_string(module.manifest())?);

    match load_extension(db, EXTENSION_NAME)? {
        None => {
            let mut sql = SqlSession::new(db);
            sql.execute(&format!(
                "CREATE EXTENSION {EXTENSION_NAME} FROM MODULE '{module_sha256}' \
                 MANIFEST '{manifest_json}'"
            ))?;
            sql.execute(&format!(
                "ALTER EXTENSION {EXTENSION_NAME} ACTIVATE SINGLE NODE"
            ))?;
        }
        Some(installation)
            if installation.state == ExtensionState::Active
                && (installation.module_sha256 != module_sha256
                    || installation.manifest != *module.manifest()) =>
        {
            SqlSession::new(db).execute(&format!(
                "ALTER EXTENSION {EXTENSION_NAME} UPDATE FROM MODULE '{module_sha256}' \
                 MANIFEST '{manifest_json}'"
            ))?;
        }
        Some(installation)
            if installation.module_sha256 != module_sha256
                || installation.manifest != *module.manifest() =>
        {
            let mut sql = SqlSession::new(db);
            sql.execute(&format!("DROP EXTENSION {EXTENSION_NAME} CASCADE"))?;
            sql.execute(&format!(
                "CREATE EXTENSION {EXTENSION_NAME} FROM MODULE '{module_sha256}' \
                 MANIFEST '{manifest_json}'"
            ))?;
            sql.execute(&format!(
                "ALTER EXTENSION {EXTENSION_NAME} ACTIVATE SINGLE NODE"
            ))?;
        }
        Some(installation) if installation.state != ExtensionState::Active => {
            SqlSession::new(db).execute(&format!(
                "ALTER EXTENSION {EXTENSION_NAME} ACTIVATE SINGLE NODE"
            ))?;
        }
        Some(_) => {}
    }
    Ok(())
}

fn install_websites(db: &mut BicDb) -> Result<(), Box<dyn Error>> {
    if load_website(db, WEBSITE_NAME)?.is_none() {
        SqlSession::new(db).execute(&format!(
            "CREATE WEBSITE {WEBSITE_NAME} USING EXTENSION {EXTENSION_NAME} \
             WITH (host = '*', path = '/', export = 'render_website', auth = 'public')"
        ))?;
    }

    if load_website_release(db, WEBSITE_NAME, RETRO_VERSION)?.is_none() {
        publish_release(db, RETRO_VERSION, RETRO_SITE, false)?;
    }
    if load_website_release(db, WEBSITE_NAME, MODERN_VERSION)?.is_none() {
        publish_release(db, MODERN_VERSION, MODERN_SITE, true)?;
    }
    let active_version = load_website(db, WEBSITE_NAME)?.and_then(|website| website.active_version);
    if !matches!(
        active_version.as_deref(),
        Some(RETRO_VERSION | MODERN_VERSION)
    ) {
        activate_release(db, MODERN_VERSION)?;
    }
    Ok(())
}

fn publish_release(
    db: &mut BicDb,
    version: &str,
    content: &str,
    activate: bool,
) -> Result<(), Box<dyn Error>> {
    let activate = if activate { " ACTIVATE" } else { "" };
    SqlSession::new(db).execute(&format!(
        "PUBLISH WEBSITE {WEBSITE_NAME} VERSION '{version}' CONTENT '{}'{}",
        sql_literal(content),
        activate
    ))?;
    Ok(())
}

async fn activate_retro(State(state): State<DemoState>) -> Response {
    activate_from_request(&state, RETRO_VERSION)
}

async fn activate_modern(State(state): State<DemoState>) -> Response {
    activate_from_request(&state, MODERN_VERSION)
}

fn activate_from_request(state: &DemoState, version: &str) -> Response {
    let result = (|| {
        let mut db = state
            .db
            .lock()
            .map_err(|_| "BicDB demo database lock was poisoned".to_string())?;
        activate_release(&mut db, version).map_err(|error| error.to_string())?;
        sync_extension_runtime(&db, &state.runtime).map_err(|error| error.to_string())
    })();
    match result {
        Ok(()) => {
            println!("activated BicDB website {WEBSITE_NAME}@{version}");
            Redirect::to("/").into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

fn activate_release(db: &mut BicDb, version: &str) -> bicdb_sql::Result<()> {
    SqlSession::new(db)
        .execute(&format!(
            "ALTER WEBSITE {WEBSITE_NAME} ACTIVATE VERSION '{version}'"
        ))
        .map(|_| ())
}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
