use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use rusqlite::Connection;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use toml_edit::Document;

use crate::modules;

const DEFAULT_INSTANCE_ID: &str = "__default__";
const DEFAULT_INSTANCE_NAME: &str = "默认实例";
const DEFAULT_PROVIDER_ID: &str = "openai";
const STATE_DB_FILE: &str = "state_5.sqlite";
const LOCAL_THREAD_CATALOG_DIR: &str = "sqlite";
const LOCAL_THREAD_CATALOG_DB_FILE: &str = "codex-dev.db";
const LOCAL_THREAD_CATALOG_HOST_ID: &str = "local";
const CONFIG_FILE_NAME: &str = "config.toml";
const SESSION_INDEX_FILE: &str = "session_index.jsonl";
const GLOBAL_STATE_FILE: &str = ".codex-global-state.json";
const SELECTED_REMOTE_HOST_ID_KEY: &str = "selected-remote-host-id";
const SESSION_DIRS: [&str; 2] = ["sessions", "archived_sessions"];
const ACTIVE_SESSION_DIR: &str = "sessions";
const SESSION_VISIBILITY_REPAIR_BACKUP_PREFIX: &str = "backup-";
const SESSION_VISIBILITY_REPAIR_BACKUP_SUFFIX: &str = "-session-visibility-repair";
const MAX_SESSION_VISIBILITY_REPAIR_BACKUPS: usize = 1;
const SESSION_INDEX_ACTIVITY_DRIFT_MS: i128 = 3_600_000;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionVisibilityRepairItem {
    pub instance_id: String,
    pub instance_name: String,
    pub target_provider: String,
    pub changed_rollout_file_count: usize,
    pub updated_sqlite_row_count: usize,
    pub missing_sqlite_thread_count: usize,
    pub added_session_index_entry_count: usize,
    pub repaired_local_thread_catalog_count: usize,
    pub repaired_project_index_workspace_count: usize,
    pub reset_sidebar_host_selection: bool,
    pub metadata_rebuild_failed: bool,
    pub skipped_sqlite_file: bool,
    pub backup_dir: Option<String>,
    pub running: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSessionVisibilityRepairSummary {
    pub instance_count: usize,
    pub mutated_instance_count: usize,
    pub changed_rollout_file_count: usize,
    pub updated_sqlite_row_count: usize,
    pub missing_sqlite_thread_count: usize,
    pub added_session_index_entry_count: usize,
    pub repaired_local_thread_catalog_count: usize,
    pub repaired_project_index_workspace_count: usize,
    pub reset_sidebar_host_selection_count: usize,
    pub metadata_rebuild_failed_instance_count: usize,
    pub skipped_sqlite_file_count: usize,
    pub items: Vec<CodexSessionVisibilityRepairItem>,
    pub backup_dirs: Vec<String>,
    pub message: String,
}

#[derive(Debug, Clone)]
struct CodexSyncInstance {
    id: String,
    name: String,
    data_dir: PathBuf,
    last_pid: Option<u32>,
}

#[derive(Debug, Clone)]
struct RolloutProviderChange {
    relative_path: PathBuf,
    absolute_path: PathBuf,
    updated_first_line: Option<String>,
    target_modified_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy)]
struct SqliteProviderScan {
    rows_to_update: usize,
    skipped_unusable_database: bool,
}

#[derive(Debug, Clone, Copy)]
struct ThreadsTableColumns {
    id: bool,
    model_provider: bool,
    has_user_event: bool,
    first_user_message: bool,
    thread_source: bool,
    rollout_path: bool,
    created_at: bool,
    updated_at: bool,
    created_at_ms: bool,
    updated_at_ms: bool,
    source: bool,
    cwd: bool,
    title: bool,
    archived: bool,
    git_branch: bool,
    preview: bool,
}

#[derive(Debug, Clone)]
struct SqliteThreadIndexRow {
    id: String,
    title: String,
    updated_at: Option<i64>,
    rollout_path: Option<String>,
}

#[derive(Debug, Clone)]
struct LocalThreadCatalogSourceRow {
    id: String,
    title: String,
    source_created_at: f64,
    source_updated_at: f64,
    cwd: String,
    source_kind: String,
    source_detail: String,
    model_provider: String,
    git_branch: Option<String>,
}

#[derive(Debug, Clone)]
struct ThreadSourceMetadataRepair {
    id: String,
    current_source: String,
    next_source: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct LocalThreadCatalogRepairPlan {
    missing_row_count: usize,
    stale_path_count: usize,
}

impl LocalThreadCatalogRepairPlan {
    fn has_work(self) -> bool {
        self.missing_row_count > 0 || self.stale_path_count > 0
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct GlobalStateRepairResult {
    repaired_project_root_count: usize,
    reset_remote_host_selection: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct SingleInstanceRepairResult {
    sqlite_rows_updated: usize,
    session_index_entries_added: usize,
    local_thread_catalog_rows_repaired: usize,
    repaired_project_root_count: usize,
    reset_sidebar_host_selection: bool,
    metadata_rebuild_failed: bool,
}

pub fn repair_session_visibility_across_instances(
) -> Result<CodexSessionVisibilityRepairSummary, String> {
    let instances = collect_instances()?;
    let process_entries = modules::process::collect_codex_process_entries();
    let mut items = Vec::with_capacity(instances.len());
    let mut backup_dirs = Vec::new();
    let mut mutated_instance_count = 0usize;
    let mut changed_rollout_file_count = 0usize;
    let mut updated_sqlite_row_count = 0usize;
    let mut missing_sqlite_thread_count = 0usize;
    let mut added_session_index_entry_count = 0usize;
    let mut repaired_local_thread_catalog_count = 0usize;
    let mut repaired_project_index_workspace_count = 0usize;
    let mut reset_sidebar_host_selection_count = 0usize;
    let mut metadata_rebuild_failed_instance_count = 0usize;
    let mut skipped_sqlite_file_count = 0usize;
    let mut mutated_running_instance_count = 0usize;

    for instance in &instances {
        let running = is_instance_running(instance, &process_entries);
        let target_provider = read_target_provider(&instance.data_dir)?;
        let rollout_changes =
            collect_rollout_provider_changes(&instance.data_dir, &target_provider)?;
        let sqlite_scan = count_sqlite_rows_to_update(&instance.data_dir, &target_provider)?;
        let sqlite_rows_to_update = sqlite_scan.rows_to_update;
        let sqlite_source_rows_to_update =
            count_sqlite_thread_source_metadata_rows_to_update(&instance.data_dir)?;
        let sqlite_path_rows_to_update = count_sqlite_thread_paths_to_update(&instance.data_dir)?;
        let missing_sqlite_threads = count_missing_sqlite_thread_rows(&instance.data_dir)?;
        let missing_session_index_entries =
            count_missing_session_index_entries(&instance.data_dir)?;
        let local_thread_catalog_plan = preview_local_thread_catalog_repair(&instance.data_dir)?;
        let should_repair_local_thread_catalog = local_thread_catalog_plan.has_work()
            || sqlite_rows_to_update > 0
            || sqlite_source_rows_to_update > 0
            || sqlite_path_rows_to_update > 0
            || missing_sqlite_threads > 0;
        let workspace_roots = collect_active_rollout_workspace_roots(&instance.data_dir)?;
        let global_state_plan =
            preview_global_state_project_root_repair(&instance.data_dir, &workspace_roots)?;
        if sqlite_scan.skipped_unusable_database {
            skipped_sqlite_file_count += 1;
        }

        if rollout_changes.is_empty()
            && sqlite_rows_to_update == 0
            && sqlite_source_rows_to_update == 0
            && sqlite_path_rows_to_update == 0
            && missing_sqlite_threads == 0
            && missing_session_index_entries == 0
            && !local_thread_catalog_plan.has_work()
            && global_state_plan.repaired_project_root_count == 0
            && !global_state_plan.reset_remote_host_selection
        {
            items.push(CodexSessionVisibilityRepairItem {
                instance_id: instance.id.clone(),
                instance_name: instance.name.clone(),
                target_provider,
                changed_rollout_file_count: 0,
                updated_sqlite_row_count: 0,
                missing_sqlite_thread_count: 0,
                added_session_index_entry_count: 0,
                repaired_local_thread_catalog_count: 0,
                repaired_project_index_workspace_count: 0,
                reset_sidebar_host_selection: false,
                metadata_rebuild_failed: false,
                skipped_sqlite_file: sqlite_scan.skipped_unusable_database,
                backup_dir: None,
                running,
            });
            continue;
        }

        let backup_dir = backup_instance_files(
            &instance.data_dir,
            &rollout_changes,
            sqlite_rows_to_update > 0
                || sqlite_source_rows_to_update > 0
                || sqlite_path_rows_to_update > 0
                || missing_sqlite_threads > 0,
            missing_session_index_entries > 0,
            global_state_plan.repaired_project_root_count > 0
                || global_state_plan.reset_remote_host_selection,
            should_repair_local_thread_catalog,
            &instance.id,
            &target_provider,
        )?;
        let backup_dir_string = backup_dir.to_string_lossy().to_string();

        let repaired = repair_single_instance(
            &instance.data_dir,
            &target_provider,
            &rollout_changes,
            sqlite_rows_to_update > 0,
            sqlite_source_rows_to_update > 0,
            sqlite_path_rows_to_update > 0,
            missing_sqlite_threads > 0,
            missing_session_index_entries > 0,
            should_repair_local_thread_catalog,
            &workspace_roots,
        );
        let repaired = match repaired {
            Ok(value) => value,
            Err(error) => {
                let restore_result = restore_instance_files_from_backup(
                    &instance.data_dir,
                    &backup_dir,
                    sqlite_rows_to_update > 0
                        || sqlite_source_rows_to_update > 0
                        || sqlite_path_rows_to_update > 0
                        || missing_sqlite_threads > 0,
                );
                if let Err(restore_error) = restore_result {
                    return Err(format!(
                        "修复实例历史会话可见性失败 ({}): {}；自动回滚也失败: {}；备份目录: {}",
                        instance.name,
                        error,
                        restore_error,
                        backup_dir.display()
                    ));
                }
                return Err(format!(
                    "修复实例历史会话可见性失败 ({}): {}；已自动回滚，备份目录: {}",
                    instance.name,
                    error,
                    backup_dir.display()
                ));
            }
        };

        mutated_instance_count += 1;
        changed_rollout_file_count += rollout_changes.len();
        updated_sqlite_row_count += repaired.sqlite_rows_updated;
        missing_sqlite_thread_count += missing_sqlite_threads;
        added_session_index_entry_count += repaired.session_index_entries_added;
        repaired_local_thread_catalog_count += repaired.local_thread_catalog_rows_repaired;
        repaired_project_index_workspace_count += repaired.repaired_project_root_count;
        if repaired.reset_sidebar_host_selection {
            reset_sidebar_host_selection_count += 1;
        }
        if repaired.metadata_rebuild_failed {
            metadata_rebuild_failed_instance_count += 1;
        }
        if running {
            mutated_running_instance_count += 1;
        }
        backup_dirs.push(backup_dir_string.clone());
        items.push(CodexSessionVisibilityRepairItem {
            instance_id: instance.id.clone(),
            instance_name: instance.name.clone(),
            target_provider,
            changed_rollout_file_count: rollout_changes.len(),
            updated_sqlite_row_count: repaired.sqlite_rows_updated,
            missing_sqlite_thread_count: missing_sqlite_threads,
            added_session_index_entry_count: repaired.session_index_entries_added,
            repaired_local_thread_catalog_count: repaired.local_thread_catalog_rows_repaired,
            repaired_project_index_workspace_count: repaired.repaired_project_root_count,
            reset_sidebar_host_selection: repaired.reset_sidebar_host_selection,
            metadata_rebuild_failed: repaired.metadata_rebuild_failed,
            skipped_sqlite_file: sqlite_scan.skipped_unusable_database,
            backup_dir: Some(backup_dir_string),
            running,
        });
    }

    prune_session_visibility_repair_backups(&instances);

    let message = build_summary_message(
        mutated_instance_count,
        changed_rollout_file_count,
        updated_sqlite_row_count,
        missing_sqlite_thread_count,
        added_session_index_entry_count,
        repaired_local_thread_catalog_count,
        repaired_project_index_workspace_count,
        reset_sidebar_host_selection_count,
        metadata_rebuild_failed_instance_count,
        mutated_running_instance_count,
        skipped_sqlite_file_count,
    );

    Ok(CodexSessionVisibilityRepairSummary {
        instance_count: instances.len(),
        mutated_instance_count,
        changed_rollout_file_count,
        updated_sqlite_row_count,
        missing_sqlite_thread_count,
        added_session_index_entry_count,
        repaired_local_thread_catalog_count,
        repaired_project_index_workspace_count,
        reset_sidebar_host_selection_count,
        metadata_rebuild_failed_instance_count,
        skipped_sqlite_file_count,
        items,
        backup_dirs,
        message,
    })
}

pub fn read_history_visibility_provider_for_dir(data_dir: &Path) -> Result<String, String> {
    read_target_provider(data_dir)
}

fn repair_single_instance(
    data_dir: &Path,
    target_provider: &str,
    rollout_changes: &[RolloutProviderChange],
    update_sqlite: bool,
    repair_sqlite_thread_sources: bool,
    normalize_sqlite_paths: bool,
    rebuild_missing_sqlite_threads: bool,
    reconcile_session_index: bool,
    repair_local_thread_catalog: bool,
    workspace_roots: &[String],
) -> Result<SingleInstanceRepairResult, String> {
    let sqlite_rows_updated = if update_sqlite {
        update_sqlite_provider(data_dir, target_provider)?
    } else {
        0
    };
    for change in rollout_changes {
        rewrite_rollout_provider(change)?;
    }
    let sqlite_source_rows_updated = if repair_sqlite_thread_sources {
        repair_sqlite_thread_source_metadata(data_dir)?
    } else {
        0
    };
    let sqlite_path_rows_updated = if normalize_sqlite_paths {
        normalize_sqlite_thread_paths(data_dir)?
    } else {
        0
    };
    repair_sqlite_thread_timestamps(data_dir)?;
    let metadata_rebuild_failed = if rebuild_missing_sqlite_threads {
        match modules::codex_official_app_server::rebuild_thread_metadata(data_dir) {
            Ok(()) => false,
            Err(error) => {
                modules::logger::log_warn(&format!(
                    "[Codex Session Visibility] official metadata rebuild failed after missing SQLite thread detection: codex_home={}, error={}",
                    data_dir.display(),
                    error
                ));
                true
            }
        }
    } else {
        false
    };
    let session_index_entries_added = if reconcile_session_index {
        reconcile_session_index_from_sqlite(data_dir)?
    } else {
        0
    };
    let local_thread_catalog_rows_repaired = if repair_local_thread_catalog {
        repair_local_thread_catalog_from_sqlite(data_dir)?
    } else {
        0
    };
    let global_state_repair = repair_global_state_project_roots(data_dir, workspace_roots)?;
    Ok(SingleInstanceRepairResult {
        sqlite_rows_updated: sqlite_rows_updated
            + sqlite_source_rows_updated
            + sqlite_path_rows_updated,
        session_index_entries_added,
        local_thread_catalog_rows_repaired,
        repaired_project_root_count: global_state_repair.repaired_project_root_count,
        reset_sidebar_host_selection: global_state_repair.reset_remote_host_selection,
        metadata_rebuild_failed,
    })
}

fn build_summary_message(
    mutated_instance_count: usize,
    changed_rollout_file_count: usize,
    updated_sqlite_row_count: usize,
    _missing_sqlite_thread_count: usize,
    added_session_index_entry_count: usize,
    repaired_local_thread_catalog_count: usize,
    _repaired_project_index_workspace_count: usize,
    _reset_sidebar_host_selection_count: usize,
    _metadata_rebuild_failed_instance_count: usize,
    mutated_running_instance_count: usize,
    _skipped_sqlite_file_count: usize,
) -> String {
    if mutated_instance_count == 0 {
        return "所有 Codex 实例的历史会话 provider 元数据与 session_index 已与当前 provider 一致，无需修复"
            .to_string();
    }

    let index_suffix = if added_session_index_entry_count > 0 {
        format!(
            "，补写 {} 条 session_index 记录",
            added_session_index_entry_count
        )
    } else {
        String::new()
    };
    let catalog_suffix = if repaired_local_thread_catalog_count > 0 {
        format!(
            "，修复 {} 条本地线程目录索引",
            repaired_local_thread_catalog_count
        )
    } else {
        String::new()
    };

    if mutated_running_instance_count > 0 {
        return format!(
            "已为 {} 个实例修复历史会话可见性：改写 {} 个 rollout 文件，更新 {} 条 SQLite 记录{}{}。请彻底退出 Codex 进程后再启动，以确保左侧项目和会话列表重新加载",
            mutated_instance_count,
            changed_rollout_file_count,
            updated_sqlite_row_count,
            index_suffix,
            catalog_suffix
        );
    }

    format!(
        "已为 {} 个实例修复历史会话可见性：改写 {} 个 rollout 文件，更新 {} 条 SQLite 记录{}{}。请彻底退出 Codex 进程后再启动，以确保左侧项目和会话列表重新加载",
        mutated_instance_count,
        changed_rollout_file_count,
        updated_sqlite_row_count,
        index_suffix,
        catalog_suffix
    )
}

fn collect_instances() -> Result<Vec<CodexSyncInstance>, String> {
    let mut instances = Vec::new();
    let default_dir = modules::codex_instance::get_default_codex_home()?;
    let store = modules::codex_instance::load_instance_store()?;
    instances.push(CodexSyncInstance {
        id: DEFAULT_INSTANCE_ID.to_string(),
        name: DEFAULT_INSTANCE_NAME.to_string(),
        data_dir: default_dir,
        last_pid: store.default_settings.last_pid,
    });

    for instance in store.instances {
        let user_data_dir = instance.user_data_dir.trim();
        if user_data_dir.is_empty() {
            continue;
        }
        instances.push(CodexSyncInstance {
            id: instance.id,
            name: instance.name,
            data_dir: PathBuf::from(user_data_dir),
            last_pid: instance.last_pid,
        });
    }

    Ok(instances)
}

fn is_instance_running(
    instance: &CodexSyncInstance,
    process_entries: &[(u32, Option<String>)],
) -> bool {
    let codex_home = instance.data_dir.to_str();
    modules::process::resolve_codex_pid_from_entries(instance.last_pid, codex_home, process_entries)
        .is_some()
}

fn read_target_provider(data_dir: &Path) -> Result<String, String> {
    let config_path = data_dir.join(CONFIG_FILE_NAME);
    if !config_path.exists() {
        return Ok(DEFAULT_PROVIDER_ID.to_string());
    }

    let content = fs::read_to_string(&config_path).map_err(|error| {
        format!(
            "读取 config.toml 失败 ({}): {}",
            config_path.display(),
            error
        )
    })?;
    if content.trim().is_empty() {
        return Ok(DEFAULT_PROVIDER_ID.to_string());
    }

    let doc = content.parse::<Document>().map_err(|error| {
        format!(
            "解析 config.toml 失败 ({}): {}",
            config_path.display(),
            error
        )
    })?;
    let provider = doc
        .get("model_provider")
        .and_then(|item| item.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_PROVIDER_ID);
    Ok(provider.to_string())
}

fn collect_rollout_provider_changes(
    data_dir: &Path,
    target_provider: &str,
) -> Result<Vec<RolloutProviderChange>, String> {
    let session_index_map = match read_session_index_map(data_dir) {
        Ok(value) => value,
        Err(error) => {
            modules::logger::log_warn(&format!(
                "读取 Codex session_index.jsonl 失败，跳过该时间来源并继续修复会话可见性: {}",
                error
            ));
            HashMap::new()
        }
    };
    let source_by_id = load_thread_source_map(data_dir)?;
    let mut changes = Vec::new();

    for dir_name in SESSION_DIRS {
        let root_dir = data_dir.join(dir_name);
        if !root_dir.exists() {
            continue;
        }
        let rollout_paths = list_rollout_files(&root_dir)?;
        for rollout_path in rollout_paths {
            let Some((first_line, _separator)) = read_first_line(&rollout_path)? else {
                continue;
            };
            let Some(mut parsed) = parse_session_meta_record(&first_line) else {
                continue;
            };
            let session_id = session_meta_id(&parsed);
            let fallback_modified_ms =
                modules::codex_session_file_time::read_modified_time(&rollout_path)
                    .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                    .map(|value| value.as_millis() as i128);
            let target_modified_at = resolve_target_modified_at_ms(
                session_id.as_deref(),
                &session_index_map,
                &rollout_path,
                fallback_modified_ms,
            )
            .and_then(modules::codex_session_file_time::system_time_from_unix_millis);
            let current_modified_at =
                modules::codex_session_file_time::read_modified_time(&rollout_path);
            let current_provider = parsed["payload"]
                .get("model_provider")
                .and_then(JsonValue::as_str)
                .unwrap_or("");
            let provider_matches = current_provider == target_provider;
            let payload_source = parsed["payload"].get("source");
            let target_source = if payload_source.is_some_and(|value| !value.is_string()) {
                session_id
                    .as_deref()
                    .and_then(|id| resolve_effective_thread_source(id, &source_by_id))
            } else {
                None
            };
            let source_matches = target_source.is_none();
            let target_workspace_root = session_id
                .as_deref()
                .and_then(|id| session_index_map.get(id))
                .and_then(session_index_workspace_root);
            let current_workspace_root = session_meta_cwd(&parsed);
            let cwd_matches = target_workspace_root.is_none()
                || current_workspace_root.as_deref() == target_workspace_root.as_deref();
            let modified_time_matches = target_modified_at.is_none()
                || modules::codex_session_file_time::same_modified_time_millis(
                    current_modified_at,
                    target_modified_at,
                );
            if provider_matches && cwd_matches && modified_time_matches {
                if source_matches {
                    continue;
                }
            }

            if provider_matches && cwd_matches && source_matches && modified_time_matches {
                continue;
            }

            let updated_first_line = if provider_matches && cwd_matches && source_matches {
                None
            } else if let Some(payload) =
                parsed.get_mut("payload").and_then(JsonValue::as_object_mut)
            {
                payload.insert(
                    "model_provider".to_string(),
                    JsonValue::String(target_provider.to_string()),
                );
                if !cwd_matches {
                    if let Some(workspace_root) = target_workspace_root.as_ref() {
                        payload.insert(
                            "cwd".to_string(),
                            JsonValue::String(workspace_root.to_string()),
                        );
                    }
                }
                if !source_matches {
                    if let Some(source) = target_source.as_ref() {
                        payload.insert("source".to_string(), JsonValue::String(source.to_string()));
                    }
                }
                Some(
                    serde_json::to_string(&parsed)
                        .map_err(|error| format!("序列化 session_meta 失败: {}", error))?,
                )
            } else {
                None
            };

            let relative_path = rollout_path
                .strip_prefix(data_dir)
                .map_err(|_| format!("无法计算 rollout 相对路径: {}", rollout_path.display()))?;
            changes.push(RolloutProviderChange {
                relative_path: relative_path.to_path_buf(),
                absolute_path: rollout_path,
                updated_first_line,
                target_modified_at,
            });
        }
    }

    changes.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(changes)
}

fn list_rollout_files(root_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut result = Vec::new();
    let entries = fs::read_dir(root_dir)
        .map_err(|error| format!("读取目录失败 ({}): {}", root_dir.display(), error))?;

    for entry in entries {
        let entry =
            entry.map_err(|error| format!("读取目录项失败 ({}): {}", root_dir.display(), error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("读取文件类型失败 ({}): {}", path.display(), error))?;
        if file_type.is_dir() {
            result.extend(list_rollout_files(&path)?);
            continue;
        }
        if file_type.is_file() {
            let file_name = path
                .file_name()
                .and_then(|item| item.to_str())
                .unwrap_or_default();
            if file_name.starts_with("rollout-") && file_name.ends_with(".jsonl") {
                result.push(path);
            }
        }
    }

    result.sort();
    Ok(result)
}

fn read_first_line(path: &Path) -> Result<Option<(String, String)>, String> {
    let file = fs::File::open(path)
        .map_err(|error| format!("打开 rollout 文件失败 ({}): {}", path.display(), error))?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    let bytes_read = reader
        .read_until(b'\n', &mut buffer)
        .map_err(|error| format!("读取 rollout 首行失败 ({}): {}", path.display(), error))?;
    if bytes_read == 0 {
        return Ok(None);
    }

    let (line_bytes, separator) = if buffer.ends_with(b"\r\n") {
        (&buffer[..buffer.len() - 2], "\r\n")
    } else if buffer.ends_with(b"\n") {
        (&buffer[..buffer.len() - 1], "\n")
    } else {
        (&buffer[..], "")
    };

    let line = String::from_utf8(line_bytes.to_vec()).map_err(|error| {
        format!(
            "解析 rollout 首行 UTF-8 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    Ok(Some((line, separator.to_string())))
}

fn parse_session_meta_record(first_line: &str) -> Option<JsonValue> {
    if first_line.trim().is_empty() {
        return None;
    }

    let parsed = serde_json::from_str::<JsonValue>(first_line).ok()?;
    if parsed.get("type").and_then(JsonValue::as_str) != Some("session_meta") {
        return None;
    }
    if !parsed.get("payload").is_some_and(JsonValue::is_object) {
        return None;
    }
    Some(parsed)
}

fn session_meta_id(meta: &JsonValue) -> Option<String> {
    meta.get("payload")
        .and_then(|payload| payload.get("id").or_else(|| payload.get("session_id")))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .or_else(|| {
            meta.get("id")
                .or_else(|| meta.get("session_id"))
                .and_then(JsonValue::as_str)
                .map(str::to_string)
        })
}

fn session_meta_cwd(meta: &JsonValue) -> Option<String> {
    meta.get("payload")
        .and_then(|payload| payload.get("cwd"))
        .or_else(|| meta.get("cwd"))
        .and_then(JsonValue::as_str)
        .and_then(normalize_workspace_root)
}

fn session_index_workspace_root(entry: &JsonValue) -> Option<String> {
    [
        "cwd",
        "workspace_root",
        "workspaceRoot",
        "working_directory",
        "workingDirectory",
    ]
    .iter()
    .find_map(|key| entry.get(*key).and_then(JsonValue::as_str))
    .and_then(normalize_workspace_root)
}

fn normalize_workspace_root(value: &str) -> Option<String> {
    let mut value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(stripped) = value.strip_prefix(r"\\?\UNC\") {
        return normalize_workspace_root(&format!(r"\\{}", stripped));
    }
    if let Some(stripped) = value.strip_prefix(r"\\?\") {
        value = stripped;
    }

    let is_windows_path = value.starts_with(r"\\")
        || value
            .as_bytes()
            .get(1)
            .is_some_and(|separator| *separator == b':');
    let separator = if is_windows_path { '\\' } else { '/' };
    let mut normalized = if is_windows_path {
        value.replace('/', r"\")
    } else {
        value.replace('\\', "/")
    };
    while normalized.len() > 3 && normalized.ends_with(separator) {
        normalized.pop();
    }
    if normalized.trim().is_empty() || looks_like_projectless_workspace_root(&normalized) {
        None
    } else {
        Some(normalized)
    }
}

fn looks_like_projectless_workspace_root(value: &str) -> bool {
    let parts = value
        .split(['\\', '/'])
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() < 2 {
        return false;
    }
    let Some(date_part) = parts.get(parts.len().saturating_sub(2)) else {
        return false;
    };
    let Some(name_part) = parts.last() else {
        return false;
    };
    date_part.len() == 10
        && date_part.chars().enumerate().all(|(index, ch)| {
            matches!(index, 4 | 7) && ch == '-' || !matches!(index, 4 | 7) && ch.is_ascii_digit()
        })
        && name_part.len() <= 32
}

fn read_session_index_map(root_dir: &Path) -> Result<HashMap<String, JsonValue>, String> {
    let path = root_dir.join(SESSION_INDEX_FILE);
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let content = fs::read_to_string(&path).map_err(|error| {
        format!(
            "读取 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    let mut entries = HashMap::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        let Some(id) = entry.get("id").and_then(JsonValue::as_str) else {
            continue;
        };
        entries.insert(id.to_string(), entry);
    }
    Ok(entries)
}

fn collect_active_rollout_session_ids(data_dir: &Path) -> Result<HashSet<String>, String> {
    let root_dir = data_dir.join(ACTIVE_SESSION_DIR);
    if !root_dir.exists() {
        return Ok(HashSet::new());
    }
    let mut ids = HashSet::new();
    for rollout_path in list_rollout_files(&root_dir)? {
        let Some((first_line, _separator)) = read_first_line(&rollout_path)? else {
            continue;
        };
        let Some(parsed) = parse_session_meta_record(&first_line) else {
            continue;
        };
        if let Some(id) = session_meta_id(&parsed) {
            ids.insert(id);
        }
    }
    Ok(ids)
}

fn read_sqlite_thread_ids(data_dir: &Path) -> Result<HashSet<String>, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(HashSet::new());
    }
    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(HashSet::new());
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    let mut statement = match connection.prepare("SELECT id FROM threads") {
        Ok(statement) => statement,
        Err(error) if is_missing_threads_table_error(&error) => return Ok(HashSet::new()),
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "prepare SQLite thread id query failed",
                &error,
            ));
        }
    };
    let rows = statement
        .query_map([], |row| row.get::<usize, String>(0))
        .map_err(|error| {
            format_sqlite_read_error(&db_path, "query SQLite thread ids failed", &error)
        })?;
    let mut ids = HashSet::new();
    for row in rows {
        ids.insert(row.map_err(|error| {
            format_sqlite_read_error(&db_path, "read SQLite thread id failed", &error)
        })?);
    }
    Ok(ids)
}

fn count_missing_sqlite_thread_rows(data_dir: &Path) -> Result<usize, String> {
    let rollout_ids = collect_active_rollout_session_ids(data_dir)?;
    if rollout_ids.is_empty() {
        return Ok(0);
    }
    let sqlite_ids = read_sqlite_thread_ids(data_dir)?;
    Ok(rollout_ids
        .iter()
        .filter(|id| !sqlite_ids.contains(*id))
        .count())
}

fn collect_active_rollout_workspace_roots(data_dir: &Path) -> Result<Vec<String>, String> {
    let session_index_map = read_session_index_map(data_dir).unwrap_or_default();
    let root_dir = data_dir.join(ACTIVE_SESSION_DIR);
    if !root_dir.exists() {
        return Ok(Vec::new());
    }
    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    for rollout_path in list_rollout_files(&root_dir)? {
        let Some((first_line, _separator)) = read_first_line(&rollout_path)? else {
            continue;
        };
        let Some(parsed) = parse_session_meta_record(&first_line) else {
            continue;
        };
        let id = session_meta_id(&parsed);
        let root = id
            .as_deref()
            .and_then(|id| session_index_map.get(id))
            .and_then(session_index_workspace_root)
            .or_else(|| session_meta_cwd(&parsed));
        let Some(root) = root else {
            continue;
        };
        if seen.insert(root.clone()) {
            roots.push(root);
        }
    }
    Ok(roots)
}

fn read_global_state(data_dir: &Path) -> Result<JsonValue, String> {
    let path = data_dir.join(GLOBAL_STATE_FILE);
    if !path.exists() {
        return Ok(json!({}));
    }
    let raw = fs::read_to_string(&path)
        .map_err(|error| format!("读取 Codex 全局状态失败 ({}): {}", path.display(), error))?;
    Ok(serde_json::from_str::<JsonValue>(&raw).unwrap_or_else(|_| json!({})))
}

fn global_state_array_contains(
    object: &serde_json::Map<String, JsonValue>,
    key: &str,
    workspace: &str,
) -> bool {
    object
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|values| {
            values.iter().any(|value| {
                value.as_str().and_then(normalize_workspace_root).as_deref() == Some(workspace)
            })
        })
        .unwrap_or(false)
}

fn should_reset_sidebar_host_selection(object: &serde_json::Map<String, JsonValue>) -> bool {
    object
        .get(SELECTED_REMOTE_HOST_ID_KEY)
        .and_then(JsonValue::as_str)
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "local"
        })
        .unwrap_or(false)
}

fn preview_global_state_project_root_repair(
    data_dir: &Path,
    workspace_roots: &[String],
) -> Result<GlobalStateRepairResult, String> {
    if workspace_roots.is_empty() {
        return Ok(GlobalStateRepairResult::default());
    }
    let value = read_global_state(data_dir)?;
    let Some(object) = value.as_object() else {
        return Ok(GlobalStateRepairResult {
            repaired_project_root_count: workspace_roots.len(),
            reset_remote_host_selection: false,
        });
    };
    let repaired_project_root_count = workspace_roots
        .iter()
        .filter_map(|root| normalize_workspace_root(root))
        .filter(|root| {
            !global_state_array_contains(object, "project-order", root)
                || !global_state_array_contains(object, "electron-saved-workspace-roots", root)
        })
        .count();
    Ok(GlobalStateRepairResult {
        repaired_project_root_count,
        reset_remote_host_selection: should_reset_sidebar_host_selection(object),
    })
}

fn merge_string_array(
    object: &mut serde_json::Map<String, JsonValue>,
    key: &str,
    additions: &[String],
) -> usize {
    let mut values = object
        .get(key)
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| item.as_str().and_then(normalize_workspace_root))
        .collect::<Vec<_>>();
    let mut seen = values.iter().cloned().collect::<HashSet<_>>();
    let mut added = 0usize;

    for addition in additions {
        let Some(normalized) = normalize_workspace_root(addition) else {
            continue;
        };
        if seen.insert(normalized.clone()) {
            values.push(normalized);
            added += 1;
        }
    }

    if added > 0 {
        object.insert(
            key.to_string(),
            JsonValue::Array(values.into_iter().map(JsonValue::String).collect()),
        );
    }
    added
}

fn repair_global_state_project_roots(
    data_dir: &Path,
    workspace_roots: &[String],
) -> Result<GlobalStateRepairResult, String> {
    if workspace_roots.is_empty() {
        return Ok(GlobalStateRepairResult::default());
    }
    let path = data_dir.join(GLOBAL_STATE_FILE);
    let mut value = read_global_state(data_dir)?;
    if !value.is_object() {
        value = json!({});
    }
    let Some(object) = value.as_object_mut() else {
        return Err("Codex global state is not an object".to_string());
    };

    let added_project_order = merge_string_array(object, "project-order", workspace_roots);
    let added_saved_roots =
        merge_string_array(object, "electron-saved-workspace-roots", workspace_roots);
    let reset_remote_host_selection = should_reset_sidebar_host_selection(object);
    if reset_remote_host_selection {
        object.insert(
            SELECTED_REMOTE_HOST_ID_KEY.to_string(),
            JsonValue::String("local".to_string()),
        );
    }

    let repaired_project_root_count = added_project_order.max(added_saved_roots);
    if repaired_project_root_count > 0 || reset_remote_host_selection {
        let serialized = serde_json::to_string_pretty(&value)
            .map_err(|error| format!("serialize Codex global state failed: {}", error))?;
        modules::atomic_write::write_string_atomic(&path, &format!("{}\n", serialized)).map_err(
            |error| {
                format!(
                    "write Codex global state failed ({}): {}",
                    path.display(),
                    error
                )
            },
        )?;
    }

    Ok(GlobalStateRepairResult {
        repaired_project_root_count,
        reset_remote_host_selection,
    })
}

fn count_missing_session_index_entries(data_dir: &Path) -> Result<usize, String> {
    let session_index_map = read_session_index_map(data_dir)?;
    let rows = load_sqlite_thread_index_rows(data_dir)?;
    Ok(rows
        .iter()
        .filter(|row| !session_index_map.contains_key(&row.id))
        .count())
}

fn load_sqlite_thread_index_rows(data_dir: &Path) -> Result<Vec<SqliteThreadIndexRow>, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };

    let mut statement = match connection.prepare("PRAGMA table_info(threads)") {
        Ok(statement) => statement,
        Err(error) if is_missing_threads_table_error(&error) => return Ok(Vec::new()),
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let rows = statement
        .query_map([], |row| row.get::<usize, String>(1))
        .map_err(|error| {
            format_sqlite_read_error(&db_path, "读取 SQLite threads 表结构失败", &error)
        })?;
    let mut names = HashSet::new();
    for row in rows {
        names.insert(row.map_err(|error| {
            format_sqlite_read_error(&db_path, "读取 SQLite threads 表结构失败", &error)
        })?);
    }
    if !names.contains("id") {
        return Ok(Vec::new());
    }

    let title_expr = if names.contains("title") {
        "COALESCE(title, '')"
    } else {
        "''"
    };
    let updated_at_expr = if names.contains("updated_at") {
        "updated_at"
    } else {
        "NULL"
    };
    let rollout_path_expr = if names.contains("rollout_path") {
        "rollout_path"
    } else {
        "NULL"
    };
    let sql = format!(
        "SELECT id, {title_expr}, {updated_at_expr}, {rollout_path_expr} FROM threads ORDER BY updated_at DESC"
    );
    let mut statement = connection.prepare(sql.as_str()).map_err(|error| {
        format!(
            "准备 SQLite 会话索引查询失败 ({}): {}",
            db_path.display(),
            error
        )
    })?;
    let mapped = statement
        .query_map([], |row| {
            Ok(SqliteThreadIndexRow {
                id: row.get(0)?,
                title: row.get(1)?,
                updated_at: row.get(2)?,
                rollout_path: row.get(3)?,
            })
        })
        .map_err(|error| {
            format!(
                "查询 SQLite 会话索引行失败 ({}): {}",
                db_path.display(),
                error
            )
        })?;
    let mut result = Vec::new();
    for row in mapped {
        result.push(row.map_err(|error| {
            format!(
                "读取 SQLite 会话索引行失败 ({}): {}",
                db_path.display(),
                error
            )
        })?);
    }
    Ok(result)
}

fn format_thread_updated_at_iso(updated_at: Option<i64>) -> String {
    let seconds = updated_at.unwrap_or_else(|| Utc::now().timestamp());
    Utc.timestamp_opt(seconds, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

fn resolve_thread_updated_at_seconds(data_dir: &Path, row: &SqliteThreadIndexRow) -> Option<i64> {
    let rollout_activity_seconds = row
        .rollout_path
        .as_deref()
        .map(|path| resolve_rollout_path(data_dir, path))
        .filter(|path| path.exists())
        .and_then(|path| rollout_file_activity_ms(&path))
        .map(|value| (value / 1000) as i64);
    match (row.updated_at, rollout_activity_seconds) {
        (Some(sqlite_seconds), Some(activity_seconds))
            if i64::abs(sqlite_seconds - activity_seconds) > 3600 =>
        {
            Some(activity_seconds)
        }
        (Some(sqlite_seconds), _) => Some(sqlite_seconds),
        (None, Some(activity_seconds)) => Some(activity_seconds),
        (None, None) => None,
    }
}

fn build_session_index_entry_from_thread(data_dir: &Path, row: &SqliteThreadIndexRow) -> JsonValue {
    json!({
        "id": row.id,
        "thread_name": if row.title.trim().is_empty() {
            "Untitled"
        } else {
            row.title.as_str()
        },
        "updated_at": format_thread_updated_at_iso(resolve_thread_updated_at_seconds(data_dir, row)),
    })
}

fn reconcile_session_index_from_sqlite(data_dir: &Path) -> Result<usize, String> {
    let session_index_map = read_session_index_map(data_dir)?;
    let rows = load_sqlite_thread_index_rows(data_dir)?;
    let missing_rows: Vec<&SqliteThreadIndexRow> = rows
        .iter()
        .filter(|row| !session_index_map.contains_key(&row.id))
        .collect();
    if missing_rows.is_empty() {
        return Ok(0);
    }

    let path = data_dir.join(SESSION_INDEX_FILE);
    let mut lines = if path.exists() {
        fs::read_to_string(&path)
            .map_err(|error| {
                format!(
                    "读取 session_index.jsonl 失败 ({}): {}",
                    path.display(),
                    error
                )
            })?
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }

    for row in &missing_rows {
        let entry = build_session_index_entry_from_thread(data_dir, row);
        let line = serde_json::to_string(&entry)
            .map_err(|error| format!("序列化 session_index 条目失败: {}", error))?;
        lines.push(line);
    }

    let mut output = lines.join("\n");
    output.push('\n');
    modules::atomic_write::write_string_atomic(&path, &output).map_err(|error| {
        format!(
            "写入 session_index.jsonl 失败 ({}): {}",
            path.display(),
            error
        )
    })?;
    Ok(missing_rows.len())
}

fn local_thread_catalog_db_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(LOCAL_THREAD_CATALOG_DIR)
        .join(LOCAL_THREAD_CATALOG_DB_FILE)
}

fn local_thread_catalog_schema_available(
    connection: &Connection,
    catalog_path: &Path,
) -> Result<bool, String> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .map_err(|error| {
            format_sqlite_read_error(catalog_path, "read local catalog schema", &error)
        })?;
    let rows = statement
        .query_map([], |row| row.get::<usize, String>(0))
        .map_err(|error| {
            format_sqlite_read_error(catalog_path, "query local catalog schema", &error)
        })?;
    let mut names = HashSet::new();
    for row in rows {
        names.insert(row.map_err(|error| {
            format_sqlite_read_error(catalog_path, "read local catalog schema row", &error)
        })?);
    }
    Ok(names.contains("local_thread_catalog")
        && names.contains("local_thread_catalog_hosts")
        && names.contains("local_thread_catalog_metadata")
        && names.contains("local_thread_catalog_sync_state"))
}

fn load_local_thread_catalog_source_rows(
    data_dir: &Path,
) -> Result<Vec<LocalThreadCatalogSourceRow>, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    if columns.is_none() {
        return Ok(Vec::new());
    }
    let Some(columns) = columns else {
        return Ok(Vec::new());
    };
    if !columns.id {
        return Ok(Vec::new());
    }
    let title_expr = if columns.title {
        "NULLIF(title, '')"
    } else {
        "NULL"
    };
    let preview_expr = if columns.preview {
        "NULLIF(preview, '')"
    } else {
        "NULL"
    };
    let first_user_message_expr = if columns.first_user_message {
        "NULLIF(first_user_message, '')"
    } else {
        "NULL"
    };
    let created_expr = match (columns.created_at_ms, columns.created_at) {
        (true, true) => "COALESCE(created_at_ms, created_at * 1000)",
        (true, false) => "created_at_ms",
        (false, true) => "created_at * 1000",
        (false, false) => "NULL",
    };
    let updated_expr = match (columns.updated_at_ms, columns.updated_at) {
        (true, true) => "COALESCE(updated_at_ms, updated_at * 1000)",
        (true, false) => "updated_at_ms",
        (false, true) => "updated_at * 1000",
        (false, false) => "NULL",
    };
    let cwd_expr = if columns.cwd {
        "COALESCE(cwd, '')"
    } else {
        "''"
    };
    let source_expr = match (columns.source, columns.thread_source) {
        (true, true) => "COALESCE(NULLIF(source, ''), NULLIF(thread_source, ''), 'user')",
        (true, false) => "COALESCE(NULLIF(source, ''), 'user')",
        (false, true) => "COALESCE(NULLIF(thread_source, ''), 'user')",
        (false, false) => "'user'",
    };
    let rollout_path_expr = if columns.rollout_path {
        "COALESCE(rollout_path, '')"
    } else {
        "''"
    };
    let model_provider_expr = if columns.model_provider {
        "COALESCE(model_provider, '')"
    } else {
        "''"
    };
    let git_branch_expr = if columns.git_branch {
        "git_branch"
    } else {
        "NULL"
    };
    let archived_predicate = if columns.archived {
        "COALESCE(archived, 0) = 0"
    } else {
        "1 = 1"
    };
    let has_user_event_predicate = if columns.has_user_event {
        "COALESCE(has_user_event, 0) = 1"
    } else {
        "1 = 1"
    };
    let visibility_expr =
        format!("COALESCE({preview_expr}, {first_user_message_expr}, {title_expr}, '') <> ''");
    let sql = format!(
        "SELECT id,
                COALESCE({title_expr}, {preview_expr}, {first_user_message_expr}, id),
                {created_expr},
                {updated_expr},
                {cwd_expr},
                {source_expr},
                {rollout_path_expr},
                {model_provider_expr},
                {git_branch_expr}
         FROM threads
         WHERE {archived_predicate}
           AND {has_user_event_predicate}
           AND {visibility_expr}"
    );

    let mut statement = connection.prepare(sql.as_str()).map_err(|error| {
        format_sqlite_read_error(&db_path, "prepare local catalog source query", &error)
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })
        .map_err(|error| {
            format_sqlite_read_error(&db_path, "query local catalog source rows", &error)
        })?;

    let mut result = Vec::new();
    for row in rows {
        let (
            id,
            title,
            created_at_ms,
            updated_at_ms,
            cwd,
            source_kind,
            source_detail,
            model_provider,
            git_branch,
        ) = row.map_err(|error| {
            format_sqlite_read_error(&db_path, "read local catalog source row", &error)
        })?;
        let Some(cwd) = normalize_workspace_root(&cwd) else {
            continue;
        };
        result.push(LocalThreadCatalogSourceRow {
            id,
            title,
            source_created_at: created_at_ms.unwrap_or_default() as f64 / 1000.0,
            source_updated_at: updated_at_ms.unwrap_or_default() as f64 / 1000.0,
            cwd,
            source_kind,
            source_detail: normalize_optional_path_text(&source_detail),
            model_provider,
            git_branch,
        });
    }
    Ok(result)
}

fn normalize_optional_path_text(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(stripped) = trimmed.strip_prefix(r"\\?\UNC\") {
        return format!(r"\\{}", stripped);
    }
    if let Some(stripped) = trimmed.strip_prefix(r"\\?\") {
        return stripped.to_string();
    }
    trimmed.to_string()
}

fn preview_local_thread_catalog_repair(
    data_dir: &Path,
) -> Result<LocalThreadCatalogRepairPlan, String> {
    let catalog_path = local_thread_catalog_db_path(data_dir);
    if !catalog_path.exists() {
        return Ok(LocalThreadCatalogRepairPlan::default());
    }
    let source_rows = load_local_thread_catalog_source_rows(data_dir)?;
    if source_rows.is_empty() {
        return Ok(LocalThreadCatalogRepairPlan::default());
    }

    let connection = match Connection::open(&catalog_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            modules::logger::log_warn(&format!(
                "跳过无效或损坏的 Codex local_thread_catalog 数据库 ({}): {}",
                catalog_path.display(),
                error
            ));
            return Ok(LocalThreadCatalogRepairPlan::default());
        }
        Err(error) => {
            return Err(format!(
                "打开 Codex local_thread_catalog 数据库失败 ({}): {}",
                catalog_path.display(),
                error
            ));
        }
    };
    if !local_thread_catalog_schema_available(&connection, &catalog_path)? {
        return Ok(LocalThreadCatalogRepairPlan::default());
    }

    let mut missing_row_count = 0usize;
    let mut stale_path_count = 0usize;
    let mut statement = connection
        .prepare(
            "SELECT cwd, source_detail FROM local_thread_catalog
             WHERE host_id = ?1 AND thread_id = ?2",
        )
        .map_err(|error| {
            format_sqlite_read_error(&catalog_path, "prepare local catalog preview query", &error)
        })?;
    for source in &source_rows {
        let existing = statement
            .query_row([LOCAL_THREAD_CATALOG_HOST_ID, source.id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            });
        match existing {
            Ok((cwd, source_detail)) => {
                let cwd_stale = normalize_workspace_root(&cwd).as_deref()
                    != Some(source.cwd.as_str())
                    || cwd != source.cwd;
                let normalized_source_detail = source_detail
                    .as_deref()
                    .map(normalize_optional_path_text)
                    .unwrap_or_default();
                let detail_stale = normalized_source_detail != source.source_detail;
                if cwd_stale || detail_stale {
                    stale_path_count += 1;
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => missing_row_count += 1,
            Err(error) => {
                return Err(format_sqlite_read_error(
                    &catalog_path,
                    "read local catalog preview row",
                    &error,
                ));
            }
        }
    }
    Ok(LocalThreadCatalogRepairPlan {
        missing_row_count,
        stale_path_count,
    })
}

fn count_missing_local_thread_catalog_rows(data_dir: &Path) -> Result<usize, String> {
    Ok(preview_local_thread_catalog_repair(data_dir)?.missing_row_count)
}

fn repair_local_thread_catalog_from_sqlite(data_dir: &Path) -> Result<usize, String> {
    let catalog_path = local_thread_catalog_db_path(data_dir);
    if !catalog_path.exists() {
        return Ok(0);
    }
    let source_rows = load_local_thread_catalog_source_rows(data_dir)?;
    if source_rows.is_empty() {
        return Ok(0);
    }

    let mut connection = match Connection::open(&catalog_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            modules::logger::log_warn(&format!(
                "跳过无效或损坏的 Codex local_thread_catalog 数据库 ({}): {}",
                catalog_path.display(),
                error
            ));
            return Ok(0);
        }
        Err(error) => {
            return Err(format!(
                "打开 Codex local_thread_catalog 数据库失败 ({}): {}",
                catalog_path.display(),
                error
            ));
        }
    };
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| {
            format!(
                "设置 local_thread_catalog busy_timeout 失败 ({}): {}",
                catalog_path.display(),
                error
            )
        })?;
    if !local_thread_catalog_schema_available(&connection, &catalog_path)? {
        return Ok(0);
    }

    let transaction = connection
        .transaction()
        .map_err(|error| format_sqlite_write_error(&catalog_path, &error))?;
    transaction
        .execute(
            "INSERT OR IGNORE INTO local_thread_catalog_hosts (host_id, host_kind)
             VALUES (?1, 'local')",
            [LOCAL_THREAD_CATALOG_HOST_ID],
        )
        .map_err(|error| format_sqlite_write_error(&catalog_path, &error))?;

    let max_sequence = transaction
        .query_row(
            "SELECT COALESCE(MAX(observation_sequence), 0) FROM local_thread_catalog WHERE host_id = ?1",
            [LOCAL_THREAD_CATALOG_HOST_ID],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let mut repaired = 0usize;
    for (index, source) in source_rows.iter().enumerate() {
        let observation_sequence = max_sequence + index as i64 + 1;
        let changed = transaction
            .execute(
                "INSERT INTO local_thread_catalog
                 (host_id, thread_id, display_title, source_created_at, source_updated_at, cwd,
                  source_kind, source_detail, model_provider, git_branch, observation_sequence,
                  missing_candidate)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0)
                 ON CONFLICT(host_id, thread_id) DO UPDATE SET
                   display_title = excluded.display_title,
                   source_created_at = excluded.source_created_at,
                   source_updated_at = excluded.source_updated_at,
                   cwd = excluded.cwd,
                   source_kind = excluded.source_kind,
                   source_detail = excluded.source_detail,
                   model_provider = excluded.model_provider,
                   git_branch = excluded.git_branch,
                   missing_candidate = 0
                 WHERE local_thread_catalog.display_title <> excluded.display_title
                    OR local_thread_catalog.source_created_at <> excluded.source_created_at
                    OR local_thread_catalog.source_updated_at <> excluded.source_updated_at
                    OR local_thread_catalog.cwd <> excluded.cwd
                    OR local_thread_catalog.source_kind <> excluded.source_kind
                    OR COALESCE(local_thread_catalog.source_detail, '') <> COALESCE(excluded.source_detail, '')
                    OR local_thread_catalog.model_provider <> excluded.model_provider
                    OR COALESCE(local_thread_catalog.git_branch, '') <> COALESCE(excluded.git_branch, '')
                    OR local_thread_catalog.missing_candidate <> 0",
                (
                    LOCAL_THREAD_CATALOG_HOST_ID,
                    source.id.as_str(),
                    source.title.as_str(),
                    source.source_created_at,
                    source.source_updated_at,
                    source.cwd.as_str(),
                    source.source_kind.as_str(),
                    source.source_detail.as_str(),
                    source.model_provider.as_str(),
                    source.git_branch.as_deref(),
                    observation_sequence,
                ),
            )
            .map_err(|error| format_sqlite_write_error(&catalog_path, &error))?;
        repaired += changed;
    }

    if repaired > 0 {
        transaction
            .execute(
                "INSERT INTO local_thread_catalog_metadata (id, catalog_revision)
                 VALUES (1, 1)
                 ON CONFLICT(id) DO UPDATE SET
                   catalog_revision = local_thread_catalog_metadata.catalog_revision + 1",
                [],
            )
            .map_err(|error| format_sqlite_write_error(&catalog_path, &error))?;
        transaction
            .execute(
                "INSERT INTO local_thread_catalog_sync_state
                 (host_id, watermark_updated_at, initial_build_complete, observation_sequence)
                 VALUES (?1, NULL, 1, ?2)
                 ON CONFLICT(host_id) DO UPDATE SET
                   initial_build_complete = 1,
                   observation_sequence = MAX(local_thread_catalog_sync_state.observation_sequence, excluded.observation_sequence)",
                (LOCAL_THREAD_CATALOG_HOST_ID, max_sequence + source_rows.len() as i64),
            )
            .map_err(|error| format_sqlite_write_error(&catalog_path, &error))?;
    }
    transaction
        .commit()
        .map_err(|error| format_sqlite_write_error(&catalog_path, &error))?;

    Ok(repaired)
}

fn normalize_codex_timestamp_ms(timestamp: i64) -> i128 {
    let timestamp = timestamp as i128;
    if timestamp > 10_000_000_000_000 {
        timestamp / 1_000
    } else if timestamp > 10_000_000_000 {
        timestamp
    } else {
        timestamp * 1_000
    }
}

fn parse_timestamp_ms(value: &JsonValue) -> Option<i128> {
    match value {
        JsonValue::Number(number) => number.as_i64().map(normalize_codex_timestamp_ms),
        JsonValue::String(text) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|value| value.timestamp_millis() as i128)
            .or_else(|| text.parse::<i64>().ok().map(normalize_codex_timestamp_ms)),
        _ => None,
    }
}

fn parse_session_index_updated_at_ms(entry: &JsonValue) -> Option<i128> {
    [
        "updated_at",
        "updatedAt",
        "last_updated_at",
        "lastUpdatedAt",
    ]
    .iter()
    .filter_map(|key| entry.get(*key))
    .find_map(parse_timestamp_ms)
}

fn parse_rollout_line_timestamp_ms(value: &JsonValue) -> Option<i128> {
    value
        .get("timestamp")
        .or_else(|| value.get("time"))
        .or_else(|| value.get("created_at"))
        .or_else(|| value.get("createdAt"))
        .and_then(parse_timestamp_ms)
        .or_else(|| {
            value
                .get("payload")
                .and_then(|payload| {
                    payload
                        .get("timestamp")
                        .or_else(|| payload.get("time"))
                        .or_else(|| payload.get("created_at"))
                        .or_else(|| payload.get("createdAt"))
                })
                .and_then(parse_timestamp_ms)
        })
}

fn rollout_file_activity_ms(path: &Path) -> Option<i128> {
    let content = fs::read_to_string(path).ok()?;
    content
        .lines()
        .filter_map(|line| serde_json::from_str::<JsonValue>(line.trim()).ok())
        .filter_map(|value| parse_rollout_line_timestamp_ms(&value))
        .max()
}

fn resolve_target_modified_at_ms(
    session_id: Option<&str>,
    session_index_map: &HashMap<String, JsonValue>,
    rollout_path: &Path,
    fallback_ms: Option<i128>,
) -> Option<i128> {
    let indexed = session_id
        .and_then(|id| session_index_map.get(id))
        .and_then(parse_session_index_updated_at_ms);
    let activity = rollout_file_activity_ms(rollout_path);
    match (indexed, activity) {
        (Some(indexed), Some(activity))
            if (indexed - activity).abs() > SESSION_INDEX_ACTIVITY_DRIFT_MS =>
        {
            Some(activity)
        }
        (Some(indexed), _) => Some(indexed),
        (None, Some(activity)) => Some(activity),
        (None, None) => fallback_ms,
    }
}

fn resolve_rollout_path(data_dir: &Path, rollout_path: &str) -> PathBuf {
    let trimmed = rollout_path.trim();
    let stripped = trimmed.strip_prefix(r"\\?\").unwrap_or(trimmed);
    let path = Path::new(stripped);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        data_dir.join(path)
    }
}

fn normalize_sqlite_path_text(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(normalized) = normalize_workspace_root(trimmed) {
        return normalized;
    }
    normalize_optional_path_text(trimmed)
}

fn count_sqlite_thread_paths_to_update(data_dir: &Path) -> Result<usize, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(0);
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let Some(columns) = columns else {
        return Ok(0);
    };
    if !columns.id || (!columns.cwd && !columns.rollout_path) {
        return Ok(0);
    }

    let cwd_expr = if columns.cwd { "cwd" } else { "NULL" };
    let rollout_expr = if columns.rollout_path {
        "rollout_path"
    } else {
        "NULL"
    };
    let sql = format!("SELECT id, {cwd_expr}, {rollout_expr} FROM threads");
    let mut statement = connection
        .prepare(sql.as_str())
        .map_err(|error| format_sqlite_read_error(&db_path, "prepare SQLite path query", &error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|error| format_sqlite_read_error(&db_path, "query SQLite paths", &error))?;

    let mut count = 0usize;
    for row in rows {
        let (_id, cwd, rollout_path) = row
            .map_err(|error| format_sqlite_read_error(&db_path, "read SQLite path row", &error))?;
        let cwd_changed = cwd
            .as_deref()
            .is_some_and(|value| normalize_sqlite_path_text(value) != value);
        let rollout_changed = rollout_path
            .as_deref()
            .is_some_and(|value| normalize_optional_path_text(value) != value);
        if cwd_changed || rollout_changed {
            count += 1;
        }
    }
    Ok(count)
}

fn normalize_sqlite_thread_paths(data_dir: &Path) -> Result<usize, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(0);
    }

    let mut connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| {
            format!(
                "设置 SQLite busy_timeout 失败 ({}): {}",
                db_path.display(),
                error
            )
        })?;
    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let Some(columns) = columns else {
        return Ok(0);
    };
    if !columns.id || (!columns.cwd && !columns.rollout_path) {
        return Ok(0);
    }

    let cwd_expr = if columns.cwd { "cwd" } else { "NULL" };
    let rollout_expr = if columns.rollout_path {
        "rollout_path"
    } else {
        "NULL"
    };
    let sql = format!("SELECT id, {cwd_expr}, {rollout_expr} FROM threads");
    let mut statement = connection
        .prepare(sql.as_str())
        .map_err(|error| format_sqlite_read_error(&db_path, "prepare SQLite path query", &error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|error| format_sqlite_read_error(&db_path, "query SQLite paths", &error))?;

    let mut updates = Vec::new();
    for row in rows {
        let (id, cwd, rollout_path) = row
            .map_err(|error| format_sqlite_read_error(&db_path, "read SQLite path row", &error))?;
        let next_cwd = cwd.as_deref().map(normalize_sqlite_path_text);
        let next_rollout_path = rollout_path.as_deref().map(normalize_optional_path_text);
        if next_cwd.as_deref() != cwd.as_deref()
            || next_rollout_path.as_deref() != rollout_path.as_deref()
        {
            updates.push((id, next_cwd, next_rollout_path));
        }
    }
    drop(statement);

    if updates.is_empty() {
        return Ok(0);
    }
    let transaction = connection
        .transaction()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    for (id, next_cwd, next_rollout_path) in &updates {
        match (columns.cwd, columns.rollout_path) {
            (true, true) => {
                transaction
                    .execute(
                        "UPDATE threads SET cwd = ?1, rollout_path = ?2 WHERE id = ?3",
                        (
                            next_cwd.as_deref(),
                            next_rollout_path.as_deref(),
                            id.as_str(),
                        ),
                    )
                    .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
            }
            (true, false) => {
                transaction
                    .execute(
                        "UPDATE threads SET cwd = ?1 WHERE id = ?2",
                        (next_cwd.as_deref(), id.as_str()),
                    )
                    .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
            }
            (false, true) => {
                transaction
                    .execute(
                        "UPDATE threads SET rollout_path = ?1 WHERE id = ?2",
                        (next_rollout_path.as_deref(), id.as_str()),
                    )
                    .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
            }
            (false, false) => {}
        }
    }
    transaction
        .commit()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;

    Ok(updates.len())
}

fn repair_sqlite_thread_timestamps(data_dir: &Path) -> Result<usize, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(0);
    }

    let mut connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };

    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let Some(columns) = columns else {
        return Ok(0);
    };
    if !columns.id || !columns.rollout_path || !columns.updated_at {
        return Ok(0);
    }

    let mut statement = match connection.prepare(
        "SELECT id, rollout_path, updated_at FROM threads WHERE rollout_path IS NOT NULL AND rollout_path <> ''",
    ) {
        Ok(statement) => statement,
        Err(error) if is_missing_threads_table_error(&error) => return Ok(0),
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "准备 SQLite 会话时间修复查询失败",
                &error,
            ));
        }
    };

    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })
        .map_err(|error| format_sqlite_read_error(&db_path, "查询 SQLite 会话时间失败", &error))?;

    let mut updates = Vec::new();
    for row in rows {
        let (thread_id, rollout_path, updated_at) = row.map_err(|error| {
            format_sqlite_read_error(&db_path, "读取 SQLite 会话时间失败", &error)
        })?;
        let rollout = resolve_rollout_path(data_dir, &rollout_path);
        if !rollout.exists() {
            continue;
        }
        let Some(activity_ms) = rollout_file_activity_ms(&rollout) else {
            continue;
        };
        let activity_seconds = (activity_ms / 1000) as i64;
        let current = updated_at.unwrap_or(0);
        if i64::abs(current - activity_seconds) <= 1 {
            continue;
        }
        updates.push((activity_seconds, thread_id));
    }
    drop(statement);

    if updates.is_empty() {
        return Ok(0);
    }

    let transaction = connection
        .transaction()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    for (activity_seconds, thread_id) in &updates {
        if columns.updated_at_ms {
            transaction
                .execute(
                    "UPDATE threads SET updated_at = ?1, updated_at_ms = ?2 WHERE id = ?3",
                    (
                        *activity_seconds,
                        *activity_seconds * 1000,
                        thread_id.as_str(),
                    ),
                )
                .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
        } else {
            transaction
                .execute(
                    "UPDATE threads SET updated_at = ?1 WHERE id = ?2",
                    (*activity_seconds, thread_id.as_str()),
                )
                .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
        }
    }
    transaction
        .commit()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    Ok(updates.len())
}

fn is_missing_threads_table_error(error: &rusqlite::Error) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("no such table: threads")
}

fn log_skipped_sqlite_database(path: &Path, reason: &str) {
    modules::logger::log_warn(&format!(
        "跳过无效或损坏的 Codex state_5.sqlite ({}): {}",
        path.display(),
        reason
    ));
}

fn count_sqlite_rows_to_update(
    data_dir: &Path,
    target_provider: &str,
) -> Result<SqliteProviderScan, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(SqliteProviderScan {
            rows_to_update: 0,
            skipped_unusable_database: false,
        });
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(SqliteProviderScan {
                rows_to_update: 0,
                skipped_unusable_database: true,
            });
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(SqliteProviderScan {
                rows_to_update: 0,
                skipped_unusable_database: true,
            });
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let Some(columns) = columns else {
        return Ok(SqliteProviderScan {
            rows_to_update: 0,
            skipped_unusable_database: false,
        });
    };
    let Some(where_clause) = build_threads_repair_where_clause(columns) else {
        return Ok(SqliteProviderScan {
            rows_to_update: 0,
            skipped_unusable_database: false,
        });
    };
    let sql = format!("SELECT COUNT(*) FROM threads WHERE {where_clause}");
    let count_result = if columns.model_provider {
        connection.query_row(sql.as_str(), [target_provider], |row| {
            row.get::<usize, i64>(0)
        })
    } else {
        connection.query_row(sql.as_str(), [], |row| row.get::<usize, i64>(0))
    };
    let count = match count_result {
        Ok(count) => count,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(SqliteProviderScan {
                rows_to_update: 0,
                skipped_unusable_database: true,
            });
        }
        Err(error) if is_missing_threads_table_error(&error) => {
            return Ok(SqliteProviderScan {
                rows_to_update: 0,
                skipped_unusable_database: false,
            });
        }
        Err(error) => {
            return Err(format!(
                "统计 SQLite 会话可见性差异失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    Ok(SqliteProviderScan {
        rows_to_update: count.max(0) as usize,
        skipped_unusable_database: false,
    })
}

fn update_sqlite_provider(data_dir: &Path, target_provider: &str) -> Result<usize, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(0);
    }

    let mut connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| {
            format!(
                "设置 SQLite busy_timeout 失败 ({}): {}",
                db_path.display(),
                error
            )
        })?;
    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let Some(columns) = columns else {
        return Ok(0);
    };
    let Some(where_clause) = build_threads_repair_where_clause(columns) else {
        return Ok(0);
    };
    let set_clause = build_threads_repair_set_clause(columns);
    let transaction = connection
        .transaction()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    let sql = format!("UPDATE threads SET {set_clause} WHERE {where_clause}");
    let update_result = if columns.model_provider {
        transaction.execute(sql.as_str(), [target_provider])
    } else {
        transaction.execute(sql.as_str(), [])
    };
    let updated_rows = match update_result {
        Ok(updated_rows) => updated_rows,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) if is_missing_threads_table_error(&error) => {
            return Ok(0);
        }
        Err(error) => return Err(format_sqlite_write_error(&db_path, &error)),
    };
    if let Err(error) = transaction.commit() {
        if modules::db::is_unusable_sqlite_database_error(&error) {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        return Err(format_sqlite_write_error(&db_path, &error));
    }
    Ok(updated_rows)
}

fn load_thread_source_metadata_repairs(
    data_dir: &Path,
) -> Result<Vec<ThreadSourceMetadataRepair>, String> {
    let source_by_id = load_thread_source_map(data_dir)?;
    let mut repairs = Vec::new();
    for (id, current_source) in &source_by_id {
        if parse_subagent_parent_thread_id(current_source).is_none() {
            continue;
        }
        let Some(next_source) = resolve_effective_thread_source(id, &source_by_id) else {
            continue;
        };
        if current_source != &next_source {
            repairs.push(ThreadSourceMetadataRepair {
                id: id.to_string(),
                current_source: current_source.to_string(),
                next_source,
            });
        }
    }
    repairs.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(repairs)
}

fn load_thread_source_map(data_dir: &Path) -> Result<HashMap<String, String>, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(HashMap::new());
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(HashMap::new());
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    let columns = match read_threads_table_columns(&connection) {
        Ok(columns) => columns,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(HashMap::new());
        }
        Err(error) => {
            return Err(format_sqlite_read_error(
                &db_path,
                "读取 SQLite threads 表结构失败",
                &error,
            ));
        }
    };
    let Some(columns) = columns else {
        return Ok(HashMap::new());
    };
    if !columns.id || !columns.source {
        return Ok(HashMap::new());
    }
    let source_expr = if columns.thread_source {
        "COALESCE(NULLIF(source, ''), NULLIF(thread_source, ''), '')"
    } else {
        "COALESCE(source, '')"
    };

    let mut statement = connection
        .prepare(&format!("SELECT id, {source_expr} FROM threads"))
        .map_err(|error| {
            format_sqlite_read_error(&db_path, "prepare SQLite source metadata query", &error)
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| {
            format_sqlite_read_error(&db_path, "query SQLite source metadata rows", &error)
        })?;
    let mut source_by_id = HashMap::new();
    for row in rows {
        let (id, source) = row.map_err(|error| {
            format_sqlite_read_error(&db_path, "read SQLite source metadata row", &error)
        })?;
        source_by_id.insert(id, source);
    }
    Ok(source_by_id)
}

fn count_sqlite_thread_source_metadata_rows_to_update(data_dir: &Path) -> Result<usize, String> {
    Ok(load_thread_source_metadata_repairs(data_dir)?.len())
}

fn repair_sqlite_thread_source_metadata(data_dir: &Path) -> Result<usize, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(0);
    }
    let repairs = load_thread_source_metadata_repairs(data_dir)?;
    if repairs.is_empty() {
        return Ok(0);
    }

    let mut connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(error) if modules::db::is_unusable_sqlite_database_error(&error) => {
            log_skipped_sqlite_database(&db_path, &error.to_string());
            return Ok(0);
        }
        Err(error) => {
            return Err(format!(
                "打开实例数据库失败 ({}): {}",
                db_path.display(),
                error
            ));
        }
    };
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| {
            format!(
                "设置 SQLite busy_timeout 失败 ({}): {}",
                db_path.display(),
                error
            )
        })?;
    let transaction = connection
        .transaction()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    let mut updated_rows = 0usize;
    for repair in &repairs {
        updated_rows += transaction
            .execute(
                "UPDATE threads SET source = ?1 WHERE id = ?2 AND source = ?3",
                (
                    repair.next_source.as_str(),
                    repair.id.as_str(),
                    repair.current_source.as_str(),
                ),
            )
            .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    }
    transaction
        .commit()
        .map_err(|error| format_sqlite_write_error(&db_path, &error))?;
    Ok(updated_rows)
}

fn resolve_effective_thread_source(
    thread_id: &str,
    source_by_id: &HashMap<String, String>,
) -> Option<String> {
    let mut current_id = thread_id.to_string();
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(current_id.clone()) {
            return None;
        }
        let source = source_by_id.get(&current_id)?;
        if is_plain_thread_source(source) {
            return Some(source.trim().to_string());
        }
        current_id = parse_subagent_parent_thread_id(source)?;
    }
}

fn is_plain_thread_source(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && !value.starts_with('{')
}

fn parse_subagent_parent_thread_id(source: &str) -> Option<String> {
    let parsed = serde_json::from_str::<JsonValue>(source.trim()).ok()?;
    parsed
        .get("subagent")
        .and_then(|subagent| subagent.get("thread_spawn"))
        .and_then(|spawn| spawn.get("parent_thread_id"))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

fn read_threads_table_columns(
    connection: &Connection,
) -> Result<Option<ThreadsTableColumns>, rusqlite::Error> {
    let mut statement = connection.prepare("PRAGMA table_info(threads)")?;
    let rows = statement.query_map([], |row| row.get::<usize, String>(1))?;
    let mut names = HashSet::new();
    for row in rows {
        let name = row?;
        names.insert(name);
    }
    if names.is_empty() {
        return Ok(None);
    }
    Ok(Some(ThreadsTableColumns {
        id: names.contains("id"),
        model_provider: names.contains("model_provider"),
        has_user_event: names.contains("has_user_event"),
        first_user_message: names.contains("first_user_message"),
        thread_source: names.contains("thread_source"),
        rollout_path: names.contains("rollout_path"),
        created_at: names.contains("created_at"),
        updated_at: names.contains("updated_at"),
        created_at_ms: names.contains("created_at_ms"),
        updated_at_ms: names.contains("updated_at_ms"),
        source: names.contains("source"),
        cwd: names.contains("cwd"),
        title: names.contains("title"),
        archived: names.contains("archived"),
        git_branch: names.contains("git_branch"),
        preview: names.contains("preview"),
    }))
}

fn build_threads_repair_where_clause(columns: ThreadsTableColumns) -> Option<String> {
    let mut predicates = Vec::new();
    if columns.model_provider {
        predicates.push("COALESCE(model_provider, '') <> ?1".to_string());
    }
    let visibility_expr = build_threads_visibility_expr(columns);
    if columns.has_user_event {
        predicates.push(format!(
            "(({visibility_expr}) AND COALESCE(has_user_event, 0) <> 1)"
        ));
    }
    if columns.thread_source {
        predicates.push(format!(
            "(({visibility_expr}) AND COALESCE(thread_source, '') = '')"
        ));
    }
    if predicates.is_empty() {
        None
    } else {
        Some(predicates.join(" OR "))
    }
}

fn build_threads_repair_set_clause(columns: ThreadsTableColumns) -> String {
    let mut assignments = Vec::new();
    if columns.model_provider {
        assignments.push("model_provider = ?1".to_string());
    }
    let visibility_expr = build_threads_visibility_expr(columns);
    if columns.has_user_event {
        assignments.push(format!(
            "has_user_event = CASE WHEN ({visibility_expr}) THEN 1 ELSE has_user_event END"
        ));
    }
    if columns.thread_source {
        assignments.push(format!(
            "thread_source = CASE WHEN COALESCE(thread_source, '') = '' AND ({visibility_expr}) THEN 'user' ELSE thread_source END"
        ));
    }
    assignments.join(", ")
}

fn build_threads_visibility_expr(columns: ThreadsTableColumns) -> String {
    let mut parts = Vec::new();
    if columns.first_user_message {
        parts.push("NULLIF(first_user_message, '')");
    }
    if columns.preview {
        parts.push("NULLIF(preview, '')");
    }
    if columns.title {
        parts.push("NULLIF(title, '')");
    }
    if parts.is_empty() {
        "0".to_string()
    } else {
        format!("COALESCE({}, '') <> ''", parts.join(", "))
    }
}

fn format_sqlite_read_error(path: &Path, action: &str, error: &rusqlite::Error) -> String {
    format!("{} ({}): {}", action, path.display(), error)
}

fn format_sqlite_write_error(path: &Path, error: &rusqlite::Error) -> String {
    let message = error.to_string();
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("database is locked") || lowered.contains("database busy") {
        return format!(
            "state_5.sqlite 当前被占用，请关闭 Codex / Codex App 后重试 ({}): {}",
            path.display(),
            message
        );
    }
    format!(
        "更新 SQLite 会话可见性失败 ({}): {}",
        path.display(),
        message
    )
}

fn rewrite_rollout_provider(change: &RolloutProviderChange) -> Result<(), String> {
    let original_modified_at =
        modules::codex_session_file_time::read_modified_time(&change.absolute_path);
    if let Some(updated_first_line) = change.updated_first_line.as_deref() {
        let bytes = fs::read(&change.absolute_path).map_err(|error| {
            format!(
                "读取 rollout 文件失败 ({}): {}",
                change.absolute_path.display(),
                error
            )
        })?;
        let (offset, separator) = detect_first_line_boundary(&bytes);
        let mut next_bytes = Vec::with_capacity(updated_first_line.len() + bytes.len());
        next_bytes.extend_from_slice(updated_first_line.as_bytes());
        next_bytes.extend_from_slice(separator.as_bytes());
        next_bytes.extend_from_slice(&bytes[offset..]);
        write_bytes_atomic(&change.absolute_path, &next_bytes)?;
    }
    modules::codex_session_file_time::restore_modified_time(
        &change.absolute_path,
        change.target_modified_at.or(original_modified_at),
    )
}

fn detect_first_line_boundary(bytes: &[u8]) -> (usize, &'static str) {
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            if index > 0 && bytes[index - 1] == b'\r' {
                return (index + 1, "\r\n");
            }
            return (index + 1, "\n");
        }
    }
    (bytes.len(), "")
}

fn write_bytes_atomic(path: &Path, content: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("无法定位目标目录: {}", path.display()))?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("创建目录失败 ({}): {}", parent.display(), error))?;

    let temp_path = parent.join(format!(
        ".{}.provider-repair.{}.{}",
        path.file_name()
            .and_then(|item| item.to_str())
            .unwrap_or("file"),
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::write(&temp_path, content)
        .map_err(|error| format!("写入临时文件失败 ({}): {}", temp_path.display(), error))?;
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(format!("替换文件失败 ({}): {}", path.display(), error));
    }
    Ok(())
}

fn sqlite_sidecar_paths(db_path: &Path) -> Vec<PathBuf> {
    let raw = db_path.to_string_lossy();
    vec![
        PathBuf::from(format!("{}-wal", raw)),
        PathBuf::from(format!("{}-shm", raw)),
    ]
}

fn remove_sqlite_sidecar_files(db_path: &Path) -> Result<(), String> {
    for path in sqlite_sidecar_paths(db_path) {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "清理 SQLite sidecar 文件失败 ({}): {}",
                    path.display(),
                    error
                ));
            }
        }
    }
    Ok(())
}

fn backup_sqlite_database(data_dir: &Path, backup_dir: &Path) -> Result<bool, String> {
    let db_path = data_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        return Ok(false);
    }

    let backup_db_path = backup_dir.join(STATE_DB_FILE);
    let connection = Connection::open(&db_path).map_err(|error| {
        format!(
            "打开 state_5.sqlite 以创建一致备份失败 ({}): {}",
            db_path.display(),
            error
        )
    })?;
    connection
        .busy_timeout(Duration::from_secs(3))
        .map_err(|error| {
            format!(
                "设置 SQLite 备份 busy_timeout 失败 ({}): {}",
                db_path.display(),
                error
            )
        })?;

    if backup_db_path.exists() {
        fs::remove_file(&backup_db_path).map_err(|error| {
            format!(
                "删除旧 state_5.sqlite 备份失败 ({}): {}",
                backup_db_path.display(),
                error
            )
        })?;
    }
    let backup_target = backup_db_path.to_string_lossy().to_string();
    connection
        .execute("VACUUM main INTO ?1", [backup_target.as_str()])
        .map_err(|error| {
            format!(
                "备份 state_5.sqlite 失败 ({} -> {}): {}",
                db_path.display(),
                backup_db_path.display(),
                error
            )
        })?;
    Ok(true)
}

fn restore_sqlite_database_from_backup(data_dir: &Path, backup_dir: &Path) -> Result<bool, String> {
    let backup_db_path = backup_dir.join(STATE_DB_FILE);
    if !backup_db_path.exists() {
        return Ok(false);
    }

    let target_db_path = data_dir.join(STATE_DB_FILE);
    fs::create_dir_all(data_dir).map_err(|error| {
        format!(
            "创建 state_5.sqlite 恢复目录失败 ({}): {}",
            data_dir.display(),
            error
        )
    })?;
    remove_sqlite_sidecar_files(&target_db_path)?;
    fs::copy(&backup_db_path, &target_db_path).map_err(|error| {
        format!(
            "恢复 state_5.sqlite 失败 ({} -> {}): {}",
            backup_db_path.display(),
            target_db_path.display(),
            error
        )
    })?;
    remove_sqlite_sidecar_files(&target_db_path)?;
    Ok(true)
}

fn backup_instance_files(
    data_dir: &Path,
    rollout_changes: &[RolloutProviderChange],
    include_sqlite: bool,
    include_session_index: bool,
    include_global_state: bool,
    include_local_thread_catalog: bool,
    instance_id: &str,
    target_provider: &str,
) -> Result<PathBuf, String> {
    let backup_dir_name = format!(
        "{}{}{}",
        SESSION_VISIBILITY_REPAIR_BACKUP_PREFIX,
        Utc::now().format("%Y%m%d-%H%M%S"),
        SESSION_VISIBILITY_REPAIR_BACKUP_SUFFIX
    );
    let backup_dir = data_dir.join(backup_dir_name);
    fs::create_dir_all(&backup_dir)
        .map_err(|error| format!("创建备份目录失败 ({}): {}", backup_dir.display(), error))?;

    let mut backed_up_files = Vec::new();
    let mut sqlite_backup_created = false;
    for change in rollout_changes {
        let target = backup_dir.join("files").join(&change.relative_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "创建 rollout 备份目录失败 ({}): {}",
                    parent.display(),
                    error
                )
            })?;
        }
        fs::copy(&change.absolute_path, &target).map_err(|error| {
            format!(
                "备份 rollout 文件失败 ({} -> {}): {}",
                change.absolute_path.display(),
                target.display(),
                error
            )
        })?;
        modules::codex_session_file_time::restore_modified_time(
            &target,
            modules::codex_session_file_time::read_modified_time(&change.absolute_path),
        )?;
        backed_up_files.push(change.relative_path.to_string_lossy().to_string());
    }

    if include_sqlite {
        sqlite_backup_created = backup_sqlite_database(data_dir, &backup_dir)?;
    }

    let mut session_index_backup_created = false;
    if include_session_index {
        let source = data_dir.join(SESSION_INDEX_FILE);
        if source.exists() {
            let target = backup_dir.join("files").join(SESSION_INDEX_FILE);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    format!(
                        "创建 session_index 备份目录失败 ({}): {}",
                        parent.display(),
                        error
                    )
                })?;
            }
            fs::copy(&source, &target).map_err(|error| {
                format!(
                    "备份 session_index.jsonl 失败 ({} -> {}): {}",
                    source.display(),
                    target.display(),
                    error
                )
            })?;
            session_index_backup_created = true;
        }
    }

    let mut global_state_backup_created = false;
    if include_global_state {
        let source = data_dir.join(GLOBAL_STATE_FILE);
        if source.exists() {
            let target = backup_dir.join("files").join(GLOBAL_STATE_FILE);
            fs::copy(&source, &target).map_err(|error| {
                format!(
                    "澶囦唤 Codex 鍏ㄥ眬鐘舵€佸け璐?({} -> {}): {}",
                    source.display(),
                    target.display(),
                    error
                )
            })?;
            global_state_backup_created = true;
        }
    }

    let mut local_thread_catalog_backup_created = false;
    if include_local_thread_catalog {
        let source = local_thread_catalog_db_path(data_dir);
        if source.exists() {
            let target = backup_dir
                .join("files")
                .join(LOCAL_THREAD_CATALOG_DIR)
                .join(LOCAL_THREAD_CATALOG_DB_FILE);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    format!(
                        "创建 local_thread_catalog 备份目录失败 ({}): {}",
                        parent.display(),
                        error
                    )
                })?;
            }
            fs::copy(&source, &target).map_err(|error| {
                format!(
                    "备份 local_thread_catalog 失败 ({} -> {}): {}",
                    source.display(),
                    target.display(),
                    error
                )
            })?;
            local_thread_catalog_backup_created = true;
        }
    }

    let manifest = json!({
        "instanceId": instance_id,
        "instanceRoot": data_dir,
        "targetProvider": target_provider,
        "createdAt": Utc::now().to_rfc3339(),
        "hasSqliteBackup": sqlite_backup_created,
        "hasSessionIndexBackup": session_index_backup_created,
        "hasGlobalStateBackup": global_state_backup_created,
        "hasLocalThreadCatalogBackup": local_thread_catalog_backup_created,
        "rolloutFiles": backed_up_files,
    });
    fs::write(
        backup_dir.join("manifest.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&manifest)
                .map_err(|error| format!("序列化可见性修复备份清单失败: {}", error))?
        ),
    )
    .map_err(|error| {
        format!(
            "写入可见性修复备份清单失败 ({}): {}",
            backup_dir.display(),
            error
        )
    })?;

    Ok(backup_dir)
}

fn parse_session_visibility_repair_backup_timestamp(name: &str) -> Option<&str> {
    let timestamp = name
        .strip_prefix(SESSION_VISIBILITY_REPAIR_BACKUP_PREFIX)?
        .strip_suffix(SESSION_VISIBILITY_REPAIR_BACKUP_SUFFIX)?;
    if timestamp.len() != 15 {
        return None;
    }
    if !timestamp.chars().enumerate().all(|(index, value)| {
        if index == 8 {
            value == '-'
        } else {
            value.is_ascii_digit()
        }
    }) {
        return None;
    }
    Some(timestamp)
}

fn prune_session_visibility_repair_backups(instances: &[CodexSyncInstance]) {
    for instance in instances {
        if let Err(error) = prune_instance_session_visibility_repair_backups(&instance.data_dir) {
            modules::logger::log_warn(&format!(
                "清理 Codex 会话可见性修复旧备份失败 ({}): {}",
                instance.data_dir.display(),
                error
            ));
        }
    }
}

fn prune_instance_session_visibility_repair_backups(data_dir: &Path) -> Result<(), String> {
    let entries = match fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "读取实例目录失败 ({}): {}",
                data_dir.display(),
                error
            ));
        }
    };
    let mut backups: Vec<(String, PathBuf)> = Vec::new();

    for entry in entries {
        let entry = entry
            .map_err(|error| format!("读取实例目录项失败 ({}): {}", data_dir.display(), error))?;
        let file_type = entry.file_type().map_err(|error| {
            format!(
                "读取实例目录项类型失败 ({}): {}",
                entry.path().display(),
                error
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }

        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(timestamp) = parse_session_visibility_repair_backup_timestamp(file_name) else {
            continue;
        };
        backups.push((timestamp.to_string(), entry.path()));
    }

    if backups.len() <= MAX_SESSION_VISIBILITY_REPAIR_BACKUPS {
        return Ok(());
    }

    backups.sort_by(|left, right| right.0.cmp(&left.0));
    for (_, path) in backups
        .into_iter()
        .skip(MAX_SESSION_VISIBILITY_REPAIR_BACKUPS)
    {
        fs::remove_dir_all(&path)
            .map_err(|error| format!("删除旧备份失败 ({}): {}", path.display(), error))?;
    }

    Ok(())
}

fn restore_instance_files_from_backup(
    data_dir: &Path,
    backup_dir: &Path,
    include_sqlite: bool,
) -> Result<(), String> {
    let files_root = backup_dir.join("files");
    if files_root.exists() {
        restore_directory_contents(&files_root, data_dir)?;
    }

    if include_sqlite {
        let _ = restore_sqlite_database_from_backup(data_dir, backup_dir)?;
    }

    Ok(())
}

fn restore_directory_contents(source_root: &Path, target_root: &Path) -> Result<(), String> {
    let entries = fs::read_dir(source_root)
        .map_err(|error| format!("读取备份目录失败 ({}): {}", source_root.display(), error))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!("读取备份目录项失败 ({}): {}", source_root.display(), error)
        })?;
        let source_path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            format!(
                "读取备份文件类型失败 ({}): {}",
                source_path.display(),
                error
            )
        })?;
        let relative = source_path
            .strip_prefix(source_root)
            .map_err(|_| format!("无法计算备份相对路径: {}", source_path.display()))?;
        let target_path = target_root.join(relative);

        if file_type.is_dir() {
            fs::create_dir_all(&target_path).map_err(|error| {
                format!("创建恢复目录失败 ({}): {}", target_path.display(), error)
            })?;
            restore_directory_contents(&source_path, &target_path)?;
            continue;
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("创建恢复父目录失败 ({}): {}", parent.display(), error))?;
        }
        fs::copy(&source_path, &target_path).map_err(|error| {
            format!(
                "恢复备份文件失败 ({} -> {}): {}",
                source_path.display(),
                target_path.display(),
                error
            )
        })?;
        modules::codex_session_file_time::restore_modified_time(
            &target_path,
            modules::codex_session_file_time::read_modified_time(&source_path),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn make_temp_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let base_dir =
            std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), unique));
        if base_dir.exists() {
            fs::remove_dir_all(&base_dir).expect("cleanup old temp dir");
        }
        fs::create_dir_all(&base_dir).expect("create temp dir");
        base_dir
    }

    fn cleanup_temp_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn set_test_modified_time(path: &Path, modified_at: SystemTime) {
        #[cfg(windows)]
        {
            use std::fs::OpenOptions;
            use std::os::windows::fs::OpenOptionsExt;
            const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
            OpenOptions::new()
                .access_mode(FILE_WRITE_ATTRIBUTES)
                .open(path)
                .expect("open rollout for mtime")
                .set_modified(modified_at)
                .expect("set polluted rollout mtime");
        }

        #[cfg(not(windows))]
        {
            fs::File::open(path)
                .expect("open rollout")
                .set_modified(modified_at)
                .expect("set polluted rollout mtime");
        }
    }

    fn create_local_thread_catalog_schema(data_dir: &Path) -> PathBuf {
        let sqlite_dir = data_dir.join("sqlite");
        fs::create_dir_all(&sqlite_dir).expect("create sqlite dir");
        let db_path = sqlite_dir.join("codex-dev.db");
        let connection = Connection::open(&db_path).expect("open catalog db");
        connection
            .execute(
                "CREATE TABLE local_thread_catalog (
                    host_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    display_title TEXT NOT NULL,
                    source_created_at REAL NOT NULL,
                    source_updated_at REAL NOT NULL,
                    cwd TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_detail TEXT,
                    model_provider TEXT NOT NULL,
                    git_branch TEXT,
                    observation_sequence INTEGER NOT NULL,
                    missing_candidate INTEGER NOT NULL DEFAULT 0
                        CHECK (missing_candidate IN (0, 1)),
                    PRIMARY KEY (host_id, thread_id)
                )",
                [],
            )
            .expect("create local_thread_catalog");
        connection
            .execute(
                "CREATE TABLE local_thread_catalog_hosts (
                    host_id TEXT PRIMARY KEY,
                    host_kind TEXT NOT NULL CHECK (
                        host_kind IN ('local', 'ssh', 'wsl', 'remote-control')
                    )
                )",
                [],
            )
            .expect("create catalog hosts");
        connection
            .execute(
                "CREATE TABLE local_thread_catalog_metadata (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    catalog_revision INTEGER NOT NULL DEFAULT 0
                )",
                [],
            )
            .expect("create catalog metadata");
        connection
            .execute(
                "CREATE TABLE local_thread_catalog_sync_state (
                    host_id TEXT PRIMARY KEY,
                    watermark_updated_at REAL,
                    initial_build_complete INTEGER NOT NULL DEFAULT 0,
                    observation_sequence INTEGER NOT NULL DEFAULT 0
                )",
                [],
            )
            .expect("create catalog sync state");
        connection
            .execute(
                "INSERT INTO local_thread_catalog_metadata (id, catalog_revision) VALUES (1, 0)",
                [],
            )
            .expect("insert metadata");
        connection
            .execute(
                "INSERT INTO local_thread_catalog_sync_state
                 (host_id, watermark_updated_at, initial_build_complete, observation_sequence)
                 VALUES ('local', 0, 1, 0)",
                [],
            )
            .expect("insert sync state");
        db_path
    }

    #[test]
    fn rollout_repair_updates_provider_and_preserves_session_time() {
        let data_dir = make_temp_dir("codex-session-visibility-rollout-time-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("05").join("23");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-test.jsonl");
        fs::write(
            &rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\",\"model_provider\":\"old\"}}\n{\"type\":\"event\",\"timestamp\":\"2024-01-01T00:00:00Z\"}\n",
        )
        .expect("write rollout");
        fs::write(
            data_dir.join(SESSION_INDEX_FILE),
            "{\"id\":\"s1\",\"thread_name\":\"Test\",\"updated_at\":\"2024-02-03T04:05:06Z\"}\n",
        )
        .expect("write session index");
        let polluted_modified_at = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        set_test_modified_time(&rollout_path, polluted_modified_at);

        let changes =
            collect_rollout_provider_changes(&data_dir, "relay").expect("collect rollout changes");
        assert_eq!(changes.len(), 1);

        repair_single_instance(
            &data_dir,
            "relay",
            &changes,
            false,
            false,
            false,
            false,
            false,
            false,
            &[],
        )
        .expect("repair rollout");

        let content = fs::read_to_string(&rollout_path).expect("read repaired rollout");
        assert!(content.contains("\"model_provider\":\"relay\""));
        assert_eq!(
            fs::metadata(&rollout_path)
                .expect("rollout metadata")
                .modified()
                .expect("rollout mtime"),
            UNIX_EPOCH + Duration::from_secs(1_704_067_200)
        );
        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn rollout_repair_restores_session_time_without_provider_change() {
        let data_dir = make_temp_dir("codex-session-visibility-mtime-only-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("05").join("23");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-test.jsonl");
        let rollout_content =
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\",\"model_provider\":\"relay\"}}\n{\"type\":\"event\",\"timestamp\":\"2024-01-01T00:00:00Z\"}\n";
        fs::write(&rollout_path, rollout_content).expect("write rollout");
        let polluted_modified_at = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        set_test_modified_time(&rollout_path, polluted_modified_at);

        let changes =
            collect_rollout_provider_changes(&data_dir, "relay").expect("collect rollout changes");
        assert_eq!(changes.len(), 1);
        assert!(changes[0].updated_first_line.is_none());

        repair_single_instance(
            &data_dir,
            "relay",
            &changes,
            false,
            false,
            false,
            false,
            false,
            false,
            &[],
        )
        .expect("repair rollout time");

        assert_eq!(
            fs::read_to_string(&rollout_path).expect("read repaired rollout"),
            rollout_content
        );
        assert_eq!(
            fs::metadata(&rollout_path)
                .expect("rollout metadata")
                .modified()
                .expect("rollout mtime"),
            UNIX_EPOCH + Duration::from_secs(1_704_067_200)
        );
        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_repair_marks_threads_with_first_user_message_visible() {
        let data_dir = make_temp_dir("codex-session-visibility-sqlite-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    model_provider TEXT,
                    has_user_event INTEGER,
                    first_user_message TEXT,
                    thread_source TEXT
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, model_provider, has_user_event, first_user_message, thread_source)
                 VALUES
                 ('matched-invisible', 'relay', 0, 'hello', ''),
                 ('old-invisible', 'old', 0, 'hi', NULL),
                 ('already-visible', 'relay', 1, 'visible', 'user'),
                 ('provider-only', '', 0, '', NULL)",
                [],
            )
            .expect("insert rows");
        drop(connection);

        let scan = count_sqlite_rows_to_update(&data_dir, "relay").expect("scan sqlite");
        assert_eq!(scan.rows_to_update, 3);
        assert!(!scan.skipped_unusable_database);

        let updated_rows = update_sqlite_provider(&data_dir, "relay").expect("update sqlite");
        assert_eq!(updated_rows, 3);

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        let matched_invisible = connection
            .query_row(
                "SELECT model_provider, has_user_event, thread_source FROM threads WHERE id = 'matched-invisible'",
                [],
                |row| {
                    Ok((
                        row.get::<usize, String>(0)?,
                        row.get::<usize, i64>(1)?,
                        row.get::<usize, String>(2)?,
                    ))
                },
            )
            .expect("read matched row");
        assert_eq!(
            matched_invisible,
            ("relay".to_string(), 1, "user".to_string())
        );

        let old_invisible = connection
            .query_row(
                "SELECT model_provider, has_user_event, thread_source FROM threads WHERE id = 'old-invisible'",
                [],
                |row| {
                    Ok((
                        row.get::<usize, String>(0)?,
                        row.get::<usize, i64>(1)?,
                        row.get::<usize, String>(2)?,
                    ))
                },
            )
            .expect("read old row");
        assert_eq!(old_invisible, ("relay".to_string(), 1, "user".to_string()));

        let provider_only = connection
            .query_row(
                "SELECT model_provider, has_user_event FROM threads WHERE id = 'provider-only'",
                [],
                |row| Ok((row.get::<usize, String>(0)?, row.get::<usize, i64>(1)?)),
            )
            .expect("read provider-only row");
        assert_eq!(provider_only, ("relay".to_string(), 0));

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_repair_marks_threads_with_title_or_preview_visible() {
        let data_dir = make_temp_dir("codex-session-visibility-preview-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    model_provider TEXT,
                    has_user_event INTEGER,
                    first_user_message TEXT,
                    preview TEXT,
                    title TEXT,
                    thread_source TEXT
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads
                 (id, model_provider, has_user_event, first_user_message, preview, title, thread_source)
                 VALUES
                 ('preview-only', 'relay', 0, '', 'visible preview', '', ''),
                 ('title-only', 'relay', 0, '', '', 'Visible title', NULL),
                 ('empty', 'relay', 0, '', '', '', NULL)",
                [],
            )
            .expect("insert rows");
        drop(connection);

        let scan = count_sqlite_rows_to_update(&data_dir, "relay").expect("scan sqlite");
        assert_eq!(scan.rows_to_update, 2);

        let updated_rows = update_sqlite_provider(&data_dir, "relay").expect("update sqlite");
        assert_eq!(updated_rows, 2);

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        let visible_rows = connection
            .query_row(
                "SELECT COUNT(*) FROM threads
                 WHERE id IN ('preview-only', 'title-only')
                   AND has_user_event = 1
                   AND thread_source = 'user'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count visible rows");
        assert_eq!(visible_rows, 2);
        let empty_visible = connection
            .query_row(
                "SELECT has_user_event FROM threads WHERE id = 'empty'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read empty row");
        assert_eq!(empty_visible, 0);

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_repair_keeps_provider_only_schema_working() {
        let data_dir = make_temp_dir("codex-session-provider-only-sqlite-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT)",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, model_provider) VALUES ('old', 'old'), ('same', 'relay')",
                [],
            )
            .expect("insert rows");
        drop(connection);

        let scan = count_sqlite_rows_to_update(&data_dir, "relay").expect("scan sqlite");
        assert_eq!(scan.rows_to_update, 1);
        let updated_rows = update_sqlite_provider(&data_dir, "relay").expect("update sqlite");
        assert_eq!(updated_rows, 1);

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        let old_provider = connection
            .query_row(
                "SELECT model_provider FROM threads WHERE id = 'old'",
                [],
                |row| row.get::<usize, String>(0),
            )
            .expect("read old provider");
        assert_eq!(old_provider, "relay");

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_path_repair_removes_extended_prefixes_for_sidebar_matching() {
        let data_dir = make_temp_dir("codex-session-visibility-sqlite-path-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    cwd TEXT,
                    rollout_path TEXT
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, cwd, rollout_path) VALUES
                 ('extended', '\\\\?\\C:\\Users\\demo\\project',
                  '\\\\?\\C:\\Users\\demo\\.codex\\sessions\\rollout.jsonl'),
                 ('plain', 'C:\\Users\\demo\\other',
                  'C:\\Users\\demo\\.codex\\sessions\\plain.jsonl')",
                [],
            )
            .expect("insert rows");
        drop(connection);

        assert_eq!(
            count_sqlite_thread_paths_to_update(&data_dir).expect("count path repairs"),
            1
        );
        assert_eq!(
            normalize_sqlite_thread_paths(&data_dir).expect("normalize paths"),
            1
        );

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        let row = connection
            .query_row(
                "SELECT cwd, rollout_path FROM threads WHERE id = 'extended'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .expect("read normalized row");
        assert_eq!(row.0, "C:\\Users\\demo\\project");
        assert_eq!(row.1, "C:\\Users\\demo\\.codex\\sessions\\rollout.jsonl");

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn local_thread_catalog_repair_backfills_missing_visible_threads() {
        let data_dir = make_temp_dir("codex-session-visibility-catalog-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("27");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-thread-1.jsonl");
        fs::write(
            &rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\",\"model_provider\":\"relay\",\"cwd\":\"C:/Users/demo/project\"},\"timestamp\":\"2026-06-27T01:02:03Z\"}\n{\"type\":\"event\",\"timestamp\":\"2026-06-27T01:05:03Z\"}\n",
        )
        .expect("write rollout");
        let rollout_path_string = rollout_path.to_string_lossy().to_string();

        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open state db");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    model_provider TEXT NOT NULL,
                    cwd TEXT NOT NULL,
                    title TEXT NOT NULL,
                    has_user_event INTEGER NOT NULL DEFAULT 0,
                    archived INTEGER NOT NULL DEFAULT 0,
                    git_branch TEXT,
                    first_user_message TEXT NOT NULL DEFAULT '',
                    thread_source TEXT,
                    preview TEXT NOT NULL DEFAULT ''
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads
                 (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                  has_user_event, archived, git_branch, first_user_message, thread_source, preview)
                 VALUES
                 ('thread-1', ?1, 1782493323, 1782493503, 'subagent', 'relay',
                  '\\\\?\\C:\\Users\\demo\\project', 'Recovered child thread', 1, 0, 'main',
                  'hello', 'user', 'hello')",
                [rollout_path_string.as_str()],
            )
            .expect("insert thread");
        drop(connection);

        let catalog_path = create_local_thread_catalog_schema(&data_dir);
        assert_eq!(
            count_missing_local_thread_catalog_rows(&data_dir).expect("count missing catalog"),
            1
        );

        let repaired =
            repair_local_thread_catalog_from_sqlite(&data_dir).expect("repair catalog from sqlite");
        assert_eq!(repaired, 1);

        let connection = Connection::open(&catalog_path).expect("reopen catalog");
        let row = connection
            .query_row(
                "SELECT host_id, thread_id, display_title, source_created_at, source_updated_at,
                        cwd, source_kind, source_detail, model_provider, git_branch,
                        observation_sequence, missing_candidate
                 FROM local_thread_catalog WHERE thread_id = 'thread-1'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, f64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                    ))
                },
            )
            .expect("read catalog row");
        assert_eq!(row.0, "local");
        assert_eq!(row.1, "thread-1");
        assert_eq!(row.2, "Recovered child thread");
        assert_eq!(row.3, 1_782_493_323.0);
        assert_eq!(row.4, 1_782_493_503.0);
        assert_eq!(row.5, "C:\\Users\\demo\\project");
        assert_eq!(row.6, "subagent");
        assert_eq!(row.7, rollout_path_string);
        assert_eq!(row.8, "relay");
        assert_eq!(row.9, "main");
        assert_eq!(row.10, 1);
        assert_eq!(row.11, 0);

        let revision = connection
            .query_row(
                "SELECT catalog_revision FROM local_thread_catalog_metadata WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read catalog revision");
        assert_eq!(revision, 1);

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_repair_normalizes_subagent_json_source_from_parent_for_catalog() {
        let data_dir = make_temp_dir("codex-session-visibility-subagent-source-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("28");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let child_rollout_path = rollout_dir.join("rollout-child-thread.jsonl");
        fs::write(
            &child_rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"child-thread\",\"model_provider\":\"relay\",\"cwd\":\"D:/repo\",\"source\":{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"parent-thread\",\"depth\":1,\"agent_path\":null,\"agent_nickname\":\"Maxwell\",\"agent_role\":\"worker\"}}},\"thread_source\":\"subagent\"},\"timestamp\":\"2026-06-28T01:00:00Z\"}\n",
        )
        .expect("write child rollout");
        let child_rollout_path_string = child_rollout_path.to_string_lossy().to_string();

        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open state db");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT,
                    created_at INTEGER,
                    updated_at INTEGER,
                    source TEXT,
                    model_provider TEXT,
                    cwd TEXT,
                    title TEXT,
                    has_user_event INTEGER,
                    archived INTEGER,
                    first_user_message TEXT,
                    thread_source TEXT,
                    preview TEXT
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads
                 (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                  has_user_event, archived, first_user_message, thread_source, preview)
                 VALUES
                 ('parent-thread', 'sessions/parent.jsonl', 100, 200, 'cli', 'relay',
                  'D:\\repo', 'Parent', 1, 0, 'parent prompt', 'user', 'parent prompt'),
                 ('child-thread', ?1, 110, 210,
                  '{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"parent-thread\",\"depth\":1,\"agent_path\":null,\"agent_nickname\":\"Maxwell\",\"agent_role\":\"worker\"}}}',
                  'relay', '\\\\?\\D:\\repo', 'Child worker', 0, 0, 'child prompt', 'subagent',
                  'child prompt')",
                [child_rollout_path_string.as_str()],
            )
            .expect("insert threads");
        drop(connection);

        assert_eq!(
            count_sqlite_thread_source_metadata_rows_to_update(&data_dir)
                .expect("count source metadata repairs"),
            1
        );

        let catalog_path = create_local_thread_catalog_schema(&data_dir);
        let changes =
            collect_rollout_provider_changes(&data_dir, "relay").expect("collect rollout changes");
        assert_eq!(changes.len(), 1);
        assert!(changes[0].updated_first_line.is_some());

        let repaired = repair_single_instance(
            &data_dir,
            "relay",
            &changes,
            true,
            true,
            true,
            false,
            false,
            true,
            &[],
        )
        .expect("repair source metadata");
        assert!(repaired.sqlite_rows_updated >= 2);
        assert_eq!(repaired.local_thread_catalog_rows_repaired, 2);

        let connection = Connection::open(&db_path).expect("reopen state db");
        let child = connection
            .query_row(
                "SELECT source, thread_source, has_user_event FROM threads WHERE id = 'child-thread'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
        )
            .expect("read child thread");
        assert_eq!(child, ("cli".to_string(), "subagent".to_string(), 1));
        drop(connection);

        let first_line = fs::read_to_string(&child_rollout_path)
            .expect("read child rollout")
            .lines()
            .next()
            .expect("first line")
            .to_string();
        let parsed: JsonValue = serde_json::from_str(&first_line).expect("parse first line");
        let payload = parsed.get("payload").expect("payload");
        assert_eq!(
            payload.get("source").and_then(JsonValue::as_str),
            Some("cli")
        );
        assert_eq!(
            payload.get("thread_source").and_then(JsonValue::as_str),
            Some("subagent")
        );

        let connection = Connection::open(&catalog_path).expect("reopen catalog");
        let catalog = connection
            .query_row(
                "SELECT source_kind, cwd, missing_candidate
                 FROM local_thread_catalog
                 WHERE thread_id = 'child-thread'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .expect("read child catalog row");
        assert_eq!(catalog, ("cli".to_string(), "D:\\repo".to_string(), 0));

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_source_repair_uses_parent_thread_source_when_source_is_empty() {
        let data_dir = make_temp_dir("codex-session-visibility-subagent-thread-source-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open state db");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    source TEXT,
                    thread_source TEXT
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, source, thread_source) VALUES
                 ('parent-thread', '', 'vscode'),
                 ('child-thread',
                  '{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"parent-thread\",\"depth\":1}}}',
                  'subagent')",
                [],
            )
            .expect("insert threads");
        drop(connection);

        assert_eq!(
            count_sqlite_thread_source_metadata_rows_to_update(&data_dir)
                .expect("count source metadata repairs"),
            1
        );

        let repaired =
            repair_sqlite_thread_source_metadata(&data_dir).expect("repair source metadata");
        assert_eq!(repaired, 1);

        let connection = Connection::open(&db_path).expect("reopen state db");
        let source = connection
            .query_row(
                "SELECT source FROM threads WHERE id = 'child-thread'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read child source");
        assert_eq!(source, "vscode");

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn local_thread_catalog_repair_normalizes_existing_extended_paths_without_duplicates() {
        let data_dir = make_temp_dir("codex-session-visibility-catalog-path-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open state db");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    model_provider TEXT NOT NULL,
                    cwd TEXT NOT NULL,
                    title TEXT NOT NULL,
                    has_user_event INTEGER NOT NULL DEFAULT 0,
                    archived INTEGER NOT NULL DEFAULT 0
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads
                 (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                  has_user_event, archived)
                 VALUES
                 ('thread-1', 'sessions/rollout-thread-1.jsonl', 100, 200, 'vscode', 'relay',
                  '\\\\?\\C:\\Users\\demo\\project', 'Thread', 1, 0)",
                [],
            )
            .expect("insert state row");
        drop(connection);

        let catalog_path = create_local_thread_catalog_schema(&data_dir);
        let connection = Connection::open(&catalog_path).expect("open catalog");
        connection
            .execute(
                "INSERT INTO local_thread_catalog
                 (host_id, thread_id, display_title, source_created_at, source_updated_at, cwd,
                  source_kind, source_detail, model_provider, git_branch, observation_sequence,
                  missing_candidate)
                 VALUES ('local', 'thread-1', 'Thread', 100, 200,
                         '\\\\?\\C:\\Users\\demo\\project', 'vscode',
                         'sessions/rollout-thread-1.jsonl', 'relay', NULL, 1, 0)",
                [],
            )
            .expect("insert catalog row");
        drop(connection);

        assert_eq!(
            count_missing_local_thread_catalog_rows(&data_dir).expect("count missing catalog"),
            0
        );
        assert_eq!(
            repair_local_thread_catalog_from_sqlite(&data_dir).expect("repair catalog from sqlite"),
            1
        );

        let connection = Connection::open(&catalog_path).expect("reopen catalog");
        let count = connection
            .query_row("SELECT COUNT(*) FROM local_thread_catalog", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count catalog rows");
        assert_eq!(count, 1);
        let cwd = connection
            .query_row(
                "SELECT cwd FROM local_thread_catalog WHERE thread_id = 'thread-1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read catalog cwd");
        assert_eq!(cwd, "C:\\Users\\demo\\project");

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_backup_restore_replaces_db_and_clears_sidecars() {
        let data_dir = make_temp_dir("codex-session-visibility-sqlite-backup-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT)",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, model_provider) VALUES ('thread-1', 'old')",
                [],
            )
            .expect("insert old row");
        drop(connection);

        let backup_dir = backup_instance_files(
            &data_dir,
            &[],
            true,
            false,
            false,
            false,
            "default",
            "relay",
        )
        .expect("backup db");

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        connection
            .execute(
                "UPDATE threads SET model_provider = 'new' WHERE id = 'thread-1'",
                [],
            )
            .expect("mutate db after backup");
        drop(connection);
        for path in sqlite_sidecar_paths(&db_path) {
            fs::write(path, b"stale wal/shm").expect("write stale sidecar");
        }

        restore_instance_files_from_backup(&data_dir, &backup_dir, true).expect("restore db");
        for path in sqlite_sidecar_paths(&db_path) {
            assert!(
                !path.exists(),
                "stale sidecar should be removed: {:?}",
                path
            );
        }

        let connection = Connection::open(&db_path).expect("open restored sqlite");
        let provider = connection
            .query_row(
                "SELECT model_provider FROM threads WHERE id = 'thread-1'",
                [],
                |row| row.get::<usize, String>(0),
            )
            .expect("read restored provider");
        assert_eq!(provider, "old");

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn resolve_target_modified_prefers_rollout_activity_when_index_drifts() {
        let data_dir = make_temp_dir("codex-session-visibility-index-drift-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("08");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-test.jsonl");
        fs::write(
            &rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\",\"model_provider\":\"relay\"}}\n{\"type\":\"event\",\"timestamp\":\"2024-01-01T00:00:00Z\"}\n",
        )
        .expect("write rollout");
        fs::write(
            data_dir.join(SESSION_INDEX_FILE),
            "{\"id\":\"s1\",\"thread_name\":\"Test\",\"updated_at\":\"2026-03-16T23:36:58.7406859Z\"}\n",
        )
        .expect("write session index");

        let session_index_map = read_session_index_map(&data_dir).expect("read session index");
        let target =
            resolve_target_modified_at_ms(Some("s1"), &session_index_map, &rollout_path, None)
                .expect("resolve target modified");

        assert_eq!(target, 1_704_067_200_000);
        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_timestamp_repair_syncs_from_rollout_activity() {
        let data_dir = make_temp_dir("codex-session-visibility-sqlite-time-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("08");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-test.jsonl");
        fs::write(
            &rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\",\"model_provider\":\"relay\"}}\n{\"type\":\"event\",\"timestamp\":\"2024-02-03T04:05:06Z\"}\n",
        )
        .expect("write rollout");
        let rollout_path_string = rollout_path.to_string_lossy().to_string();

        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT,
                    updated_at INTEGER,
                    updated_at_ms INTEGER
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, rollout_path, updated_at, updated_at_ms) VALUES
                 ('thread-1', ?1, 1_800_000_000, 1_800_000_000_000)",
                [rollout_path_string.as_str()],
            )
            .expect("insert row");
        drop(connection);

        let updated = repair_sqlite_thread_timestamps(&data_dir).expect("repair sqlite timestamps");
        assert_eq!(updated, 1);

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        let (updated_at, updated_at_ms) = connection
            .query_row(
                "SELECT updated_at, updated_at_ms FROM threads WHERE id = 'thread-1'",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .expect("read repaired timestamps");
        assert_eq!(updated_at, 1_706_933_106);
        assert_eq!(updated_at_ms, 1_706_933_106_000);

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn sqlite_timestamp_repair_handles_schema_without_updated_at_ms() {
        let data_dir = make_temp_dir("codex-session-visibility-sqlite-time-narrow-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("08");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-test.jsonl");
        fs::write(
            &rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\",\"model_provider\":\"relay\"}}\n{\"type\":\"event\",\"timestamp\":\"2024-02-03T04:05:06Z\"}\n",
        )
        .expect("write rollout");
        let rollout_path_string = rollout_path.to_string_lossy().to_string();

        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT,
                    updated_at INTEGER
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, rollout_path, updated_at) VALUES
                 ('thread-1', ?1, 1_800_000_000)",
                [rollout_path_string.as_str()],
            )
            .expect("insert row");
        drop(connection);

        let updated = repair_sqlite_thread_timestamps(&data_dir).expect("repair sqlite timestamps");
        assert_eq!(updated, 1);

        let connection = Connection::open(&db_path).expect("reopen sqlite");
        let updated_at = connection
            .query_row(
                "SELECT updated_at FROM threads WHERE id = 'thread-1'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read repaired timestamp");
        assert_eq!(updated_at, 1_706_933_106);

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn session_index_repair_appends_missing_sqlite_threads() {
        let data_dir = make_temp_dir("codex-session-visibility-index-test");
        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    updated_at INTEGER
                )",
                [],
            )
            .expect("create threads table");
        connection
            .execute(
                "INSERT INTO threads (id, title, updated_at) VALUES
                 ('indexed-thread', 'Indexed', 1_700_000_000),
                 ('missing-thread', 'Missing chat', 1_800_000_000)",
                [],
            )
            .expect("insert rows");
        drop(connection);

        fs::write(
            data_dir.join(SESSION_INDEX_FILE),
            "{\"id\":\"indexed-thread\",\"thread_name\":\"Indexed\",\"updated_at\":\"2024-01-01T00:00:00.0000000Z\"}\n",
        )
        .expect("write session index");

        let missing =
            count_missing_session_index_entries(&data_dir).expect("count missing index entries");
        assert_eq!(missing, 1);

        let added = reconcile_session_index_from_sqlite(&data_dir).expect("reconcile index");
        assert_eq!(added, 1);

        let index_map = read_session_index_map(&data_dir).expect("read session index");
        assert!(index_map.contains_key("missing-thread"));
        assert_eq!(
            index_map
                .get("missing-thread")
                .and_then(|entry| entry.get("thread_name"))
                .and_then(JsonValue::as_str),
            Some("Missing chat")
        );
        assert_eq!(
            count_missing_session_index_entries(&data_dir).expect("recount missing index entries"),
            0
        );

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn rollout_repair_syncs_cwd_from_session_index_workspace_root() {
        let data_dir = make_temp_dir("codex-session-visibility-cwd-sync-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("27");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        let rollout_path = rollout_dir.join("rollout-test.jsonl");
        fs::write(
            &rollout_path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\",\"model_provider\":\"relay\",\"cwd\":\"C:/Users/demo/Documents/Codex/2026-06-27/tmp\"}}\n{\"type\":\"event\",\"timestamp\":\"2026-06-27T01:02:03Z\"}\n",
        )
        .expect("write rollout");
        fs::write(
            data_dir.join(SESSION_INDEX_FILE),
            "{\"id\":\"thread-1\",\"thread_name\":\"Thread\",\"updated_at\":\"2026-06-27T01:02:03Z\",\"cwd\":\"C:/Users/demo/Documents/Codex\"}\n",
        )
        .expect("write session index");

        let changes =
            collect_rollout_provider_changes(&data_dir, "relay").expect("collect rollout changes");
        assert_eq!(changes.len(), 1);
        assert!(changes[0].updated_first_line.is_some());

        repair_single_instance(
            &data_dir,
            "relay",
            &changes,
            false,
            false,
            false,
            false,
            false,
            false,
            &[],
        )
        .expect("repair rollout cwd");

        let first_line = fs::read_to_string(&rollout_path)
            .expect("read repaired rollout")
            .lines()
            .next()
            .expect("first line")
            .to_string();
        let parsed: JsonValue = serde_json::from_str(&first_line).expect("parse first line");
        assert_eq!(
            parsed
                .get("payload")
                .and_then(|payload| payload.get("cwd"))
                .and_then(JsonValue::as_str),
            Some("C:\\Users\\demo\\Documents\\Codex")
        );

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn detects_active_rollouts_missing_from_sqlite_threads() {
        let data_dir = make_temp_dir("codex-session-visibility-missing-sqlite-test");
        let rollout_dir = data_dir.join("sessions").join("2026").join("06").join("27");
        fs::create_dir_all(&rollout_dir).expect("create rollout dir");
        fs::write(
            rollout_dir.join("rollout-thread-1.jsonl"),
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\",\"model_provider\":\"relay\",\"cwd\":\"C:/repo\"}}\n",
        )
        .expect("write rollout 1");
        fs::write(
            rollout_dir.join("rollout-thread-2.jsonl"),
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-2\",\"model_provider\":\"relay\",\"cwd\":\"C:/repo\"}}\n",
        )
        .expect("write rollout 2");

        let db_path = data_dir.join(STATE_DB_FILE);
        let connection = Connection::open(&db_path).expect("open sqlite");
        connection
            .execute("CREATE TABLE threads (id TEXT PRIMARY KEY)", [])
            .expect("create threads table");
        connection
            .execute("INSERT INTO threads (id) VALUES ('thread-1')", [])
            .expect("insert one row");
        drop(connection);

        assert_eq!(
            count_missing_sqlite_thread_rows(&data_dir).expect("count missing sqlite threads"),
            1
        );

        cleanup_temp_dir(&data_dir);
    }

    #[test]
    fn global_state_repair_restores_local_project_roots_and_host_selection() {
        let data_dir = make_temp_dir("codex-session-visibility-global-state-test");
        fs::write(
            data_dir.join(GLOBAL_STATE_FILE),
            "{\"selected-remote-host-id\":\"remote-control:MacBook-Air.local\",\"project-order\":[]}\n",
        )
        .expect("write global state");
        let roots = vec!["C:/Users/demo/Documents/Codex".to_string()];

        let result =
            repair_global_state_project_roots(&data_dir, &roots).expect("repair global state");
        assert_eq!(result.repaired_project_root_count, 1);
        assert!(result.reset_remote_host_selection);

        let parsed: JsonValue = serde_json::from_str(
            &fs::read_to_string(data_dir.join(GLOBAL_STATE_FILE)).expect("read state"),
        )
        .expect("parse state");
        assert_eq!(
            parsed
                .get("selected-remote-host-id")
                .and_then(JsonValue::as_str),
            Some("local")
        );
        assert!(parsed
            .get("project-order")
            .and_then(JsonValue::as_array)
            .expect("project order")
            .iter()
            .any(|value| value.as_str() == Some("C:\\Users\\demo\\Documents\\Codex")));

        cleanup_temp_dir(&data_dir);
    }
}
