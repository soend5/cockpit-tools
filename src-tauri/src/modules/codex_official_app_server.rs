use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{json, Value as JsonValue};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(target_os = "macos")]
const CODEX_APP_SERVER_EXECUTABLE: &str = "/Applications/Codex.app/Contents/Resources/codex";
const CODEX_APP_SERVER_EXECUTABLE_ENV: &str = "CODEX_APP_SERVER_EXECUTABLE";
const APP_SERVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
struct AppServerLaunchSpec {
    executable: PathBuf,
    node_path: Option<PathBuf>,
}

impl AppServerLaunchSpec {
    fn direct(executable: PathBuf) -> Self {
        Self {
            executable,
            node_path: None,
        }
    }
}

pub fn rebuild_thread_metadata(codex_home: &Path) -> Result<(), String> {
    crate::modules::codex_config_format::sanitize_codex_config_toml_file(
        &codex_home.join("config.toml"),
    )?;
    let launch_spec = official_app_server_launch_spec()?;
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] starting rebuild_thread_metadata: executable={}, codex_home={}",
        launch_spec.executable.display(),
        codex_home.display()
    ));
    let mut child = build_app_server_command(&launch_spec, codex_home)
        .spawn()
        .map_err(|error| {
            format!(
                "启动官方 Codex app-server 失败 ({} / CODEX_HOME={}): {}",
                launch_spec.executable.display(),
                codex_home.display(),
                error
            )
        })?;

    let stdout = child
        .stdout
        .take()
        .ok_or("无法读取官方 app-server stdout")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("无法读取官方 app-server stderr")?;
    let mut stdin = child.stdin.take().ok_or("无法写入官方 app-server stdin")?;
    let (sender, receiver) = mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    let stderr_reader = std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            crate::modules::logger::log_warn(&format!(
                "[Codex Official AppServer][stderr] {}",
                line
            ));
        }
    });

    let result = (|| {
        send_request(
            &mut stdin,
            json!({
                "method": "initialize",
                "id": 1,
                "params": {
                    "clientInfo": {
                        "name": "cockpit-tools",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": null,
                },
            }),
        )?;
        wait_for_response(&receiver, 1)?;

        send_request(
            &mut stdin,
            json!({
                "method": "thread/list",
                "id": 2,
                "params": {
                    "cursor": null,
                    "limit": 1,
                    "sortKey": "updated_at",
                    "sortDirection": "desc",
                    "modelProviders": null,
                    "sourceKinds": [],
                    "archived": false,
                },
            }),
        )?;
        wait_for_response(&receiver, 2)?;
        Ok::<(), String>(())
    })();

    finish_child(&mut child);
    let _ = reader.join();
    let _ = stderr_reader.join();
    if let Err(error) = &result {
        crate::modules::logger::log_warn(&format!(
            "[Codex Official AppServer] rebuild_thread_metadata failed: codex_home={}, error={}",
            codex_home.display(),
            error
        ));
    } else {
        crate::modules::logger::log_info(&format!(
            "[Codex Official AppServer] rebuild_thread_metadata completed: codex_home={}",
            codex_home.display()
        ));
    }
    result
}

fn official_app_server_launch_spec() -> Result<AppServerLaunchSpec, String> {
    let mut candidates = Vec::new();
    if let Some(executable) = std::env::var_os(CODEX_APP_SERVER_EXECUTABLE_ENV) {
        if !executable.as_os_str().is_empty() {
            candidates.push(AppServerLaunchSpec::direct(PathBuf::from(executable)));
        }
    }
    if let Some(launch_spec) = configured_app_server_launch_spec() {
        candidates.push(launch_spec);
    }
    if let Some(launch_spec) = cli_runtime_app_server_launch_spec() {
        candidates.push(launch_spec);
    }
    #[cfg(target_os = "macos")]
    candidates.push(AppServerLaunchSpec::direct(PathBuf::from(
        CODEX_APP_SERVER_EXECUTABLE,
    )));

    for candidate in &candidates {
        if candidate.executable.exists() {
            return Ok(candidate.clone());
        }
    }

    let searched_paths = candidates
        .iter()
        .map(|candidate| candidate.executable.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "未找到官方 Codex app-server 可执行文件: {}",
        searched_paths
    ))
}

fn cli_runtime_app_server_launch_spec() -> Option<AppServerLaunchSpec> {
    let runtime = crate::modules::codex_wakeup::resolve_cli_runtime().ok()?;
    Some(AppServerLaunchSpec {
        executable: PathBuf::from(runtime.binary_path),
        node_path: runtime.node_path.map(PathBuf::from),
    })
}

fn configured_app_server_launch_spec() -> Option<AppServerLaunchSpec> {
    let launch_path = crate::modules::process::resolve_codex_launch_path().ok()?;
    derive_app_server_executable_from_launch_path(&launch_path).map(AppServerLaunchSpec::direct)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppServerLaunchPlatform {
    Windows,
    Macos,
    Other,
}

impl AppServerLaunchPlatform {
    fn current() -> Self {
        match std::env::consts::OS {
            "windows" => Self::Windows,
            "macos" => Self::Macos,
            _ => Self::Other,
        }
    }
}

fn derive_app_server_executable_from_launch_path(launch_path: &Path) -> Option<PathBuf> {
    derive_app_server_executable_for_platform(launch_path, AppServerLaunchPlatform::current())
}

fn derive_app_server_executable_for_platform(
    launch_path: &Path,
    platform: AppServerLaunchPlatform,
) -> Option<PathBuf> {
    match platform {
        AppServerLaunchPlatform::Windows => {
            let parent = launch_path.parent()?;
            let file_name = launch_path.file_name().and_then(|value| value.to_str())?;
            let bundled = parent.join("resources").join("codex.exe");
            if bundled.exists() || file_name == "Codex.exe" {
                return Some(bundled);
            }
            if file_name.eq_ignore_ascii_case("codex.exe") {
                return Some(launch_path.to_path_buf());
            }
            None
        }
        AppServerLaunchPlatform::Macos => {
            let parent = launch_path.parent()?;
            let is_resource_binary = launch_path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value == "codex")
                && parent
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value.eq_ignore_ascii_case("Resources"));
            if is_resource_binary {
                return Some(launch_path.to_path_buf());
            }
            if let Some(app_root) = macos_app_root_from_path(launch_path) {
                return Some(app_root.join("Contents").join("Resources").join("codex"));
            }
            Some(launch_path.to_path_buf())
        }
        AppServerLaunchPlatform::Other => Some(launch_path.to_path_buf()),
    }
}

fn macos_app_root_from_path(path: &Path) -> Option<PathBuf> {
    let path_text = path.to_string_lossy();
    let app_index = path_text.find(".app")?;
    Some(PathBuf::from(&path_text[..app_index + 4]))
}

fn build_app_server_command(launch_spec: &AppServerLaunchSpec, codex_home: &Path) -> Command {
    let mut command = if let Some(node_path) = launch_spec.node_path.as_ref() {
        let mut command = Command::new(node_path);
        command.arg(&launch_spec.executable);
        command
    } else {
        Command::new(&launch_spec.executable)
    };
    crate::modules::process::apply_managed_proxy_env_to_command(&mut command);
    command
        .args(["app-server", "--listen", "stdio://"])
        .env("CODEX_HOME", codex_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        command.creation_flags(CREATE_NO_WINDOW);
    }

    command
}

fn send_request(stdin: &mut impl Write, request: JsonValue) -> Result<(), String> {
    let line = serde_json::to_string(&request)
        .map_err(|error| format!("序列化官方 app-server 请求失败: {}", error))?;
    stdin
        .write_all(line.as_bytes())
        .and_then(|_| stdin.write_all(b"\n"))
        .and_then(|_| stdin.flush())
        .map_err(|error| format!("写入官方 app-server 请求失败: {}", error))
}

fn wait_for_response(receiver: &mpsc::Receiver<String>, request_id: i64) -> Result<(), String> {
    loop {
        let line = receiver
            .recv_timeout(APP_SERVER_RESPONSE_TIMEOUT)
            .map_err(|_| format!("等待官方 app-server 响应超时 (id={})", request_id))?;
        let Ok(value) = serde_json::from_str::<JsonValue>(&line) else {
            continue;
        };
        if value.get("id").and_then(JsonValue::as_i64) != Some(request_id) {
            continue;
        }
        if let Some(error) = value.get("error") {
            crate::modules::logger::log_warn(&format!(
                "[Codex Official AppServer] response error: id={}, error={}",
                request_id, error
            ));
            return Err(format!(
                "官方 app-server 返回错误 (id={}): {}",
                request_id, error
            ));
        }
        if value.get("result").is_some() {
            return Ok(());
        }
        return Err(format!(
            "官方 app-server 响应缺少 result (id={}): {}",
            request_id, value
        ));
    }
}

fn finish_child(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_app_launch_path_derives_bundled_app_server_binary() {
        let launch_path = PathBuf::from(
            r"C:\Program Files\WindowsApps\OpenAI.Codex_1.0.0.0_x64__demo\app\Codex.exe",
        );

        let executable = derive_app_server_executable_for_platform(
            &launch_path,
            AppServerLaunchPlatform::Windows,
        )
        .expect("derive app-server executable");

        assert_eq!(
            executable,
            PathBuf::from(
                r"C:\Program Files\WindowsApps\OpenAI.Codex_1.0.0.0_x64__demo\app\resources\codex.exe",
            )
        );
    }

    #[test]
    fn windows_cli_launch_path_uses_cli_binary_directly() {
        let launch_path = PathBuf::from(r"C:\Users\demo\AppData\Local\OpenAI\Codex\bin\codex.exe");

        let executable = derive_app_server_executable_for_platform(
            &launch_path,
            AppServerLaunchPlatform::Windows,
        )
        .expect("derive CLI app-server executable");

        assert_eq!(executable, launch_path);
    }

    #[test]
    fn windows_unknown_launch_path_without_bundled_binary_is_ignored() {
        let launch_path = PathBuf::from(r"C:\Tools\CodexLauncher\launcher.exe");

        let executable = derive_app_server_executable_for_platform(
            &launch_path,
            AppServerLaunchPlatform::Windows,
        );

        assert!(executable.is_none());
    }

    #[test]
    fn macos_app_launch_path_derives_resources_binary() {
        let launch_path = PathBuf::from("/Applications/Codex.app/Contents/MacOS/Codex");

        let executable =
            derive_app_server_executable_for_platform(&launch_path, AppServerLaunchPlatform::Macos)
                .expect("derive macOS app-server executable");

        assert_eq!(
            executable,
            PathBuf::from("/Applications/Codex.app/Contents/Resources/codex")
        );
    }

    #[test]
    fn macos_resources_binary_uses_launch_path_directly() {
        let launch_path = PathBuf::from("/Applications/Codex.app/Contents/Resources/codex");

        let executable =
            derive_app_server_executable_for_platform(&launch_path, AppServerLaunchPlatform::Macos)
                .expect("derive macOS resource executable");

        assert_eq!(executable, launch_path);
    }

    #[test]
    fn macos_cli_launch_path_uses_cli_binary_directly() {
        let launch_path = PathBuf::from("/usr/local/bin/codex");

        let executable =
            derive_app_server_executable_for_platform(&launch_path, AppServerLaunchPlatform::Macos)
                .expect("derive macOS CLI executable");

        assert_eq!(executable, launch_path);
    }

    #[test]
    fn other_platform_launch_path_uses_binary_directly() {
        let launch_path = PathBuf::from("/opt/codex/bin/codex");

        let executable =
            derive_app_server_executable_for_platform(&launch_path, AppServerLaunchPlatform::Other)
                .expect("derive other platform executable");

        assert_eq!(executable, launch_path);
    }

    #[test]
    fn app_server_command_wraps_cli_runtime_with_node_when_required() {
        let launch_spec = AppServerLaunchSpec {
            executable: PathBuf::from(r"C:\Tools\codex\codex.js"),
            node_path: Some(PathBuf::from(r"C:\Tools\node\node.exe")),
        };
        let codex_home = PathBuf::from(r"C:\Users\demo\.codex");

        let command = build_app_server_command(&launch_spec, &codex_home);
        let args = command.get_args().collect::<Vec<_>>();

        assert_eq!(command.get_program(), Path::new(r"C:\Tools\node\node.exe"));
        assert_eq!(
            args.first().copied(),
            Some(Path::new(r"C:\Tools\codex\codex.js").as_os_str())
        );
        assert!(args.windows(3).any(|window| window
            == [
                std::ffi::OsStr::new("app-server"),
                std::ffi::OsStr::new("--listen"),
                std::ffi::OsStr::new("stdio://"),
            ]));
        assert_eq!(
            command.get_envs().find_map(|(key, value)| {
                if key == "CODEX_HOME" {
                    value.map(|item| item.to_os_string())
                } else {
                    None
                }
            }),
            Some(codex_home.into_os_string())
        );
    }
}
