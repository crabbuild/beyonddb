struct ManagedChild(Child, PathBuf);

impl Deref for ManagedChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ManagedChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let status = self.0.try_wait();
        if std::thread::panicking() {
            eprintln!("child {} status before cleanup: {status:?}", self.0.id());
        }
        if !matches!(status, Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
        // TempDir is dropped after the children. Preserve the cause of an
        // SDK failure before cleanup removes the serving process's diagnostics.
        if std::thread::panicking()
            && let Ok(log) = fs::read_to_string(&self.1)
        {
            eprintln!("child {} log:\n{log}", self.0.id());
        }
    }
}

fn run(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn start(config: &Path, log: &Path, bootstrap: bool, s3: SocketAddr) -> ManagedChild {
    // Preserve the original owner's diagnostics when a replacement starts.
    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let mut command = Command::new(config.parent().unwrap().join("beyonddb"));
    command
        .arg(config)
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .env("AWS_ACCESS_KEY_ID", "crab")
        .env("AWS_SECRET_ACCESS_KEY", "crab")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ALLOW_HTTP", "true")
        .env("AWS_ENDPOINT_URL_S3", format!("http://{s3}"))
        .env("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false");
    if bootstrap {
        command.arg("--bootstrap").stdin(Stdio::piped());
    }
    let mut child = command.spawn().unwrap();
    if bootstrap {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n")
            .unwrap();
    }
    ManagedChild(child, log.to_owned())
}

#[track_caller]
fn wait_healthy(child: &mut Child, address: SocketAddr, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "server exited {status}: {}",
                fs::read_to_string(log).unwrap()
            );
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(250)) {
            stream
                .set_read_timeout(Some(Duration::from_millis(250)))
                .unwrap();
            stream
                .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut response = String::new();
            if stream.read_to_string(&mut response).is_ok() && response.contains("200 OK") {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "server did not become healthy: {}",
        fs::read_to_string(log).unwrap()
    );
}

fn stop(child: &mut Child, log: &Path) {
    run(Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string()));
    // ExtendDB drains first; Cellar then has its own 40-second stop policy.
    let deadline = Instant::now() + Duration::from_secs(80);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{}", fs::read_to_string(log).unwrap());
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    child.kill().unwrap();
    panic!("server did not stop: {}", fs::read_to_string(log).unwrap());
}

struct RustfsContainer {
    name: String,
    log: PathBuf,
    data_dir: Option<PathBuf>,
}

impl RustfsContainer {
    fn start(address: SocketAddr, log: PathBuf) -> Self {
        let name = format!("beyonddb-test-{}", uuid::Uuid::now_v7());
        // Colima can share the host home while its own volume disk is full.
        // Each test owns a fresh child of the explicitly selected bind root.
        let data_dir = std::env::var_os("BEYONDDB_TEST_RUSTFS_BIND_ROOT").map(|root| {
            let path = PathBuf::from(root).join(&name);
            fs::create_dir_all(&path).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o777)).unwrap();
            }
            path
        });
        let volume = data_dir.as_ref().map_or_else(
            || "/data".to_owned(),
            |path| format!("{}:/data", path.display()),
        );
        let container = Self {
            name,
            log,
            data_dir,
        };
        run(Command::new("docker").args([
            "run",
            "--detach",
            "--name",
            &container.name,
            "--publish",
            &format!("{address}:9000"),
            "--env",
            "RUSTFS_ACCESS_KEY=crab",
            "--env",
            "RUSTFS_SECRET_KEY=crab",
            "--env",
            "RUSTFS_CONSOLE_ENABLE=false",
            "--env",
            "RUSTFS_OBS_LOG_DIRECTORY=/data/logs",
            "--volume",
            &volume,
            "ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858",
        ]));
        container
    }

    fn is_running(&self) -> bool {
        let output = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Running}}", &self.name])
            .output()
            .unwrap();
        output.status.success() && output.stdout == b"true\n"
    }
}

impl Drop for RustfsContainer {
    fn drop(&mut self) {
        if let Ok(output) = Command::new("docker").args(["logs", &self.name]).output() {
            let mut log = output.stdout;
            log.extend_from_slice(&output.stderr);
            let _ = fs::write(&self.log, &log);
        }
        let mut command = Command::new("docker");
        if std::thread::panicking() {
            // Failed recovery fixtures retain their object data for startup
            // replay. Stop serving, but preserve the named container and volume.
            eprintln!("retained RustFS container for replay: {}", self.name);
            if let Some(path) = &self.data_dir {
                eprintln!("retained RustFS bind data: {}", path.display());
            }
            command.args(["stop", &self.name]);
        } else {
            command.args(["rm", "--force", "--volumes", &self.name]);
        }
        let removed = command.output().is_ok_and(|output| output.status.success());
        if removed
            && !std::thread::panicking()
            && let Some(path) = &self.data_dir
        {
            let _ = fs::remove_dir_all(path);
        }
    }
}

struct ProcessFixture {
    child: ManagedChild,
    _rustfs: RustfsContainer,
    sdk: aws_sdk_dynamodb::Client,
    config: PathBuf,
    log: PathBuf,
    s3: SocketAddr,
    peer: SocketAddr,
    public: SocketAddr,
    // Children must stop before their object store and temporary files disappear.
    root: tempfile::TempDir,
}

async fn process_fixture(initial_partitions: u32) -> ProcessFixture {
    process_fixture_with_cache(initial_partitions, false).await
}

async fn process_fixture_with_cache(
    initial_partitions: u32,
    auth_cache_enabled: bool,
) -> ProcessFixture {
    let root = tempfile::tempdir().unwrap();
    fs::copy(env!("CARGO_BIN_EXE_beyonddb"), root.path().join("beyonddb")).unwrap();
    let s3 = free_addr();
    let rustfs = RustfsContainer::start(s3, root.path().join("rustfs.log"));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let ready = Command::new("aws")
            .args([
                "--endpoint-url",
                &format!("http://{s3}"),
                "s3api",
                "list-buckets",
            ])
            .env("AWS_ACCESS_KEY_ID", "crab")
            .env("AWS_SECRET_ACCESS_KEY", "crab")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .output()
            .unwrap();
        if ready.status.success() {
            break;
        }
        assert!(rustfs.is_running(), "RustFS exited before S3 readiness");
        assert!(Instant::now() < deadline, "RustFS did not become S3 ready");
        std::thread::sleep(Duration::from_millis(100));
    }
    run(Command::new("aws")
        .args([
            "--endpoint-url",
            &format!("http://{s3}"),
            "s3api",
            "create-bucket",
            "--bucket",
            "beyonddb-test",
        ])
        .env("AWS_ACCESS_KEY_ID", "crab")
        .env("AWS_SECRET_ACCESS_KEY", "crab")
        .env("AWS_DEFAULT_REGION", "us-east-1"));
    fs::write(root.path().join("encryption.key"), [23; 32]).unwrap();
    fs::write(root.path().join("peer.key"), [24; 32]).unwrap();
    fs::write(
        root.path().join("policy.json"),
        json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": [
                    "arn:aws:dynamodb:us-east-1:123456789012:table/ProcessData",
                    "arn:aws:dynamodb:us-east-1:123456789012:table/*"
                ]
            }]
        })
        .to_string(),
    )
    .unwrap();
    let peer = free_addr();
    let public = free_addr();
    let config = root.path().join("config.json");
    fs::write(
        &config,
        json!({
            "node_name": "bdb-process-owner",
            "data_dir": root.path().join("data"),
            "encryption_key_file": root.path().join("encryption.key"),
            "region": "us-east-1",
            "peer_bind": peer,
            "peer_advertise": peer.to_string(),
            "peer_key_file": root.path().join("peer.key"),
            "public_bind": public,
            "public_endpoint": format!("http://{public}"),
            "accounts": ["123456789012"],
            "initial_partitions": initial_partitions,
            "max_active_cells": 64,
            "auth_cache_enabled": auth_cache_enabled,
            "fleet": { "bucket": "beyonddb-test", "endpoint": format!("http://{s3}"), "prefix": "beyonddb-ordered-v1" },
            "bootstrap": {
                "account_id": "123456789012",
                "access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "principal_name": "process-user",
                "policy_name": "tables",
                "policy_file": root.path().join("policy.json")
            }
        })
        .to_string(),
    )
    .unwrap();
    let log = root.path().join("server.log");
    let mut child = start(&config, &log, true, s3);
    wait_healthy(&mut child, public, &log);
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "beyonddb-process-test",
        ))
        .endpoint_url(format!("http://{public}"))
        .load()
        .await;
    let sdk = aws_sdk_dynamodb::Client::new(&sdk_config);
    ProcessFixture {
        child,
        _rustfs: rustfs,
        sdk,
        config,
        log,
        s3,
        peer,
        public,
        root,
    }
}
