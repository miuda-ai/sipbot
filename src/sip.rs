use crate::config::{AccountConfig, AnswerConfig, Config};
use crate::media::MediaSession;
use anyhow::{Context, Result};
use chrono::Local;
use rsip::headers::{
    CallId, From as UntypedFrom, To as UntypedTo, UntypedHeader, Via as UntypedVia,
};
use rsip::message::HeadersExt;
use rsip::typed::{From, To, Via};
use rsip::{Header, Method, StatusCode, Uri};
use rsipstack::dialog::DialogId;
use rsipstack::dialog::dialog::DialogState;
use rsipstack::{
    EndpointBuilder,
    dialog::authenticate::Credential,
    dialog::dialog_layer::DialogLayer,
    dialog::invitation::InviteOption,
    dialog::registration::Registration,
    transaction::{
        endpoint::Endpoint,
        key::{TransactionKey, TransactionRole},
        transaction::Transaction,
    },
    transport::{TransportLayer, udp::UdpConnection},
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const ANSWER_WAV: &[u8] = include_bytes!("../wavs/answer.wav");
// use voice_engine::net_tool::extract_rtp_addresses_from_sdp;

#[derive(Clone)]
struct CallRunner {
    dialog_layer: Arc<DialogLayer>,
    account: AccountConfig,
    global_config: Config,
}

impl CallRunner {
    async fn make_call(&self, target_uri: String, call_index: u32) -> Result<()> {
        let dialog_layer = &self.dialog_layer;
        let from: rsip::Uri =
            format!("sip:{}@{}", self.account.username, self.account.domain).try_into()?;
        let to: rsip::Uri = target_uri.as_str().try_into()?;
        let contact =
            dialog_layer.build_local_contact(Some(self.account.username.clone()), None)?;
        // Create MediaSession and Offer
        let srtp_enabled = self.account.srtp_enabled.unwrap_or(false);
        let (media_session, local_sdp) =
            MediaSession::new_offer(srtp_enabled, self.global_config.external_ip.clone(), true)
                .await?;
        debug!(
            "[{}] Generated Offer SDP:\n{}",
            self.account.username, local_sdp
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let credential = if let Some(password) = &self.account.password {
            Some(Credential {
                username: self
                    .account
                    .auth_username
                    .clone()
                    .unwrap_or(self.account.username.clone()),
                password: password.clone(),
                realm: Some(self.account.domain.clone()),
            })
        } else {
            None
        };

        let opt = InviteOption {
            destination: Some(to.host_with_port.clone().into()),
            caller: from.clone(),
            callee: to.clone(),
            contact,
            content_type: Some("application/sdp".to_string()),
            offer: Some(local_sdp.into_bytes()),
            credential,
            ..Default::default()
        };

        let (dialog, response) = tokio::select! {
            res = dialog_layer.do_invite(opt, tx) => res?,
            _ = tokio::signal::ctrl_c() => {
                info!("[{}] Ctrl-C received during call setup.", self.account.username);
                return Ok(());
            }
        };

        if let Some(res) = response {
            info!(
                "[{}] Received INVITE response: {}",
                self.account.username,
                res.status_code()
            );
            if matches!(
                res.status_code().kind(),
                rsip::status_code::StatusCodeKind::Successful
            ) {
                let answer_sdp = String::from_utf8_lossy(&res.body);
                debug!(
                    "[{}] Received Answer SDP:\n{}",
                    self.account.username, answer_sdp
                );
                media_session.set_remote_answer(&answer_sdp).await?;
                info!("[{}] Set remote answer", self.account.username);
            } else {
                warn!(
                    "[{}] Call failed with status: {}",
                    self.account.username,
                    res.status_code()
                );
                return Ok(());
            }
        } else {
            warn!("[{}] No response received", self.account.username);
            return Ok(());
        }

        let hangup_secs = self.account.hangup.as_ref().and_then(|h| h.after_secs);

        // Handle recording filename prefix
        let record_path = self.account.record.clone().map(|p| {
            let path = PathBuf::from(p);
            if call_index > 0 {
                if let Some(stem) = path.file_stem() {
                    let mut new_name = stem.to_os_string();
                    new_name.push(format!("_{}", call_index));
                    if let Some(ext) = path.extension() {
                        new_name.push(".");
                        new_name.push(ext);
                    }
                    path.with_file_name(new_name)
                } else {
                    path
                }
            } else {
                path
            }
        });

        let media_session_clone = media_session.clone();
        let answer_config = self.account.answer.clone();
        let username = self.account.username.clone();
        let keep_alive = hangup_secs.is_some();

        let play_future = async move {
            let record_path_ref = record_path.as_deref();
            if let Some(answer_config) = answer_config {
                match answer_config {
                    AnswerConfig::Play { wav_file } => {
                        let file_path = PathBuf::from(wav_file);
                        if let Err(e) = media_session_clone
                            .play_file(username, &file_path, record_path_ref, keep_alive)
                            .await
                        {
                            error!("Failed to play file: {:?}", e);
                        }
                    }
                    _ => {}
                }
            } else {
                if let Err(e) = media_session_clone
                    .play_wav_bytes(username, ANSWER_WAV, record_path_ref, keep_alive)
                    .await
                {
                    warn!("Play built-in answer stopped: {:?}", e);
                }
            }
        };

        let username_monitor = self.account.username.clone();
        let monitor_future = async move {
            while let Some(event) = rx.recv().await {
                info!("[{}] Call Status: {}", username_monitor, event);
                if matches!(event, DialogState::Terminated(..)) {
                    info!("[{}] Call terminated remotely.", username_monitor);
                    return;
                }
            }
        };

        if let Some(secs) = hangup_secs {
            info!(
                "[{}] Call established. Waiting for {} seconds (or Ctrl-C) before hanging up...",
                self.account.username, secs
            );

            let play_handle = tokio::spawn(play_future);

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                    info!("[{}] {} seconds elapsed.", self.account.username, secs);
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("[{}] Ctrl-C received.", self.account.username);
                }
                _ = monitor_future => {
                    play_handle.abort();
                    return Ok(());
                }
            }
            play_handle.abort();
        } else {
            info!(
                "[{}] Call established. Waiting for playback or Ctrl-C to hang up...",
                self.account.username
            );
            tokio::select! {
                _ = play_future => {
                    info!("[{}] Playback finished.", self.account.username);
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("[{}] Ctrl-C received.", self.account.username);
                }
                _ = monitor_future => {
                    return Ok(());
                }
            }
        }

        info!("[{}] Sending BYE...", self.account.username);
        dialog.hangup().await?;
        info!("[{}] BYE sent.", self.account.username);
        Ok(())
    }
}

pub struct SipBot {
    account: AccountConfig,
    global_config: Config,
    endpoint: Option<Arc<Endpoint>>,
    dialog_layer: Option<Arc<DialogLayer>>,
    registration: Option<Registration>,
}

impl SipBot {
    pub fn new(account: AccountConfig, global_config: Config) -> Self {
        Self {
            account,
            global_config,
            endpoint: None,
            dialog_layer: None,
            registration: None,
        }
    }

    async fn init_endpoint(&mut self) -> Result<()> {
        info!(
            "[{}] Initializing SIP bot for account: {}@{}",
            self.account.username, self.account.username, self.account.domain
        );

        // Ensure recorders directory exists
        let recorders_dir = self
            .global_config
            .recorders
            .as_deref()
            .unwrap_or("/tmp/recorders");
        if let Err(e) = tokio::fs::create_dir_all(recorders_dir).await {
            warn!(
                "[{}] Failed to create recorders directory {}: {:?}",
                self.account.username, recorders_dir, e
            );
        } else {
            info!(
                "[{}] Recorders directory: {}",
                self.account.username, recorders_dir
            );
        }

        let cancel_token = CancellationToken::new();
        let transport_layer = TransportLayer::new(cancel_token.child_token());

        // Bind to configured address or default
        let addr_str = self
            .global_config
            .addr
            .as_deref()
            .unwrap_or("0.0.0.0:35060");
        let addr = addr_str.parse().context("Invalid bind address")?;

        let udp_conn =
            UdpConnection::create_connection(addr, None, Some(cancel_token.child_token())).await?;
        let local_addr = udp_conn.get_addr();
        info!("[{}] Listening on {}", self.account.username, local_addr);

        transport_layer.add_transport(udp_conn.into());

        let endpoint = EndpointBuilder::new()
            .with_user_agent(&format!("SipBot/{}", env!("CARGO_PKG_VERSION")))
            .with_transport_layer(transport_layer)
            .build();

        let endpoint = Arc::new(endpoint);
        self.endpoint = Some(endpoint.clone());

        let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
        self.dialog_layer = Some(dialog_layer);

        let credential = if let Some(password) = &self.account.password {
            Some(Credential {
                username: self.account.username.clone(),
                password: password.clone(),
                realm: Some(self.account.domain.clone()),
            })
        } else {
            None
        };
        self.registration = Some(Registration::new(endpoint.inner.clone(), credential));

        // Start serving
        let endpoint_inner = endpoint.inner.clone();
        tokio::spawn(async move {
            if let Err(e) = endpoint_inner.serve().await {
                error!("Endpoint serve error: {:?}", e);
            }
        });

        Ok(())
    }

    fn get_recording_path(&self, call_id: &str) -> PathBuf {
        let dir = self
            .global_config
            .recorders
            .as_deref()
            .unwrap_or("/tmp/recorders");
        let now = Local::now().format("%Y%m%d%H%M%S");
        // Sanitize call_id to be safe for filename
        let safe_call_id = call_id.replace(|c: char| !c.is_alphanumeric(), "_");
        let filename = format!("{}_{}.wav", now, safe_call_id);
        Path::new(dir).join(filename)
    }

    pub async fn run_wait(&mut self) -> Result<()> {
        self.init_endpoint().await?;

        // Register
        if self.account.register.unwrap_or(true) {
            self.start_registration_loop().await?;
        }

        // Listen for incoming calls
        self.listen_loop().await?;

        Ok(())
    }

    pub async fn run_call(&mut self, total: u32, concurrent: u32) -> Result<()> {
        self.init_endpoint().await?;

        // Register
        if self.account.register.unwrap_or(true) {
            self.start_registration_loop().await?;
        }

        if let Some(target) = &self.account.target {
            info!(
                "[{}] Starting outbound call to {} (total: {}, concurrent: {})",
                self.account.username, target, total, concurrent
            );

            let runner = CallRunner {
                dialog_layer: self
                    .dialog_layer
                    .as_ref()
                    .context("DialogLayer not initialized")?
                    .clone(),
                account: self.account.clone(),
                global_config: self.global_config.clone(),
            };

            let semaphore = Arc::new(Semaphore::new(concurrent as usize));
            let mut handles = vec![];

            for i in 0..total {
                let permit = semaphore.clone().acquire_owned().await?;
                let runner = runner.clone();
                let target = target.clone();

                let handle = tokio::spawn(async move {
                    if let Err(e) = runner.make_call(target, i).await {
                        error!("Call {} failed: {:?}", i, e);
                    }
                    drop(permit);
                });
                handles.push(handle);
            }

            let calls_future = async {
                for handle in handles {
                    let _ = handle.await;
                }
            };

            tokio::select! {
                _ = self.listen_loop() => {}
                _ = calls_future => {
                    info!("[{}] All calls finished.", self.account.username);
                }
            }
        } else {
            warn!(
                "[{}] No target configured for outbound call",
                self.account.username
            );
        }

        Ok(())
    }

    pub async fn run_options(&mut self, target_override: Option<String>) -> Result<()> {
        self.init_endpoint().await?;
        let target = target_override.or(self.account.target.clone());
        if let Some(target) = target {
            info!("[{}] Sending OPTIONS to {}", self.account.username, target);
            self.send_standalone_request(Method::Options, &target)
                .await?;
        } else {
            warn!(
                "[{}] No target configured for OPTIONS",
                self.account.username
            );
        }
        Ok(())
    }

    pub async fn run_info(&mut self, target_override: Option<String>) -> Result<()> {
        self.init_endpoint().await?;
        let target = target_override.or(self.account.target.clone());
        if let Some(target) = target {
            info!("[{}] Sending INFO to {}", self.account.username, target);
            self.send_standalone_request(Method::Info, &target).await?;
        } else {
            warn!("[{}] No target configured for INFO", self.account.username);
        }
        Ok(())
    }

    async fn send_standalone_request(&self, method: Method, target_uri: &str) -> Result<()> {
        let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;

        let req_uri = Uri::try_from(target_uri)?;
        let addrs = endpoint.get_addrs();
        let local_sip_addr = addrs.first().context("No local address found")?;
        let local_socket = local_sip_addr.get_socketaddr()?;
        let local_ip = local_socket.ip();
        let local_port = local_socket.port();

        let via_str = format!(
            "SIP/2.0/UDP {}:{};branch=z9hG4bK{}",
            local_ip,
            local_port,
            generate_random_string()
        );
        let untyped_via = UntypedVia::try_from(via_str.as_str())?;
        let via = Via::try_from(untyped_via)?;

        let from_str = format!(
            "sip:{}@{};tag={}",
            self.account.username,
            self.account.domain,
            generate_random_string()
        );
        let untyped_from = UntypedFrom::try_from(from_str.as_str())?;
        let from = From::try_from(untyped_from)?;

        let to_str = target_uri;
        let untyped_to = UntypedTo::try_from(to_str)?;
        let to = To::try_from(untyped_to)?;

        let call_id_str = format!("{}@{}", generate_random_string(), local_ip);
        let call_id = CallId::try_from(call_id_str.as_str())?;

        let request = endpoint.inner.make_request(
            method,
            req_uri,
            via,
            from,
            to,
            1, // CSeq
            Some(call_id),
        );

        let key = TransactionKey::from_request(&request, TransactionRole::Client)?;
        let mut transaction =
            Transaction::new_client(key, request.clone(), endpoint.inner.clone(), None);

        info!("[{}] Sending request:\n{}", self.account.username, request);
        transaction.send().await?;

        while let Some(msg) = transaction.receive().await {
            match msg {
                rsip::SipMessage::Response(res) => {
                    info!("[{}] Received response:\n{}", self.account.username, res);
                    // Log body if present
                    if !res.body.is_empty() {
                        if let Ok(body_str) = std::str::from_utf8(&res.body) {
                            info!("[{}] Response body:\n{}", self.account.username, body_str);
                        } else {
                            info!(
                                "[{}] Response body (binary): {} bytes",
                                self.account.username,
                                res.body.len()
                            );
                        }
                    }

                    if res.status_code().code() >= 200 {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn start_registration_loop(&mut self) -> Result<()> {
        let mut registration = self
            .registration
            .take()
            .context("Registration not initialized")?;
        let username = self.account.username.clone();
        let domain = self.account.domain.clone();

        tokio::spawn(async move {
            info!("[{}] Starting registration loop", username);
            let uri_str = format!("sip:{}", domain);
            let server_uri = match Uri::try_from(uri_str.as_str()) {
                Ok(u) => u,
                Err(e) => {
                    error!("[{}] Invalid domain URI: {}", username, e);
                    return;
                }
            };

            loop {
                info!("[{}] Registering...", username);
                // Default expire 30s
                match registration.register(server_uri.clone(), Some(30)).await {
                    Ok(response) => {
                        if *response.status_code() == StatusCode::OK {
                            let expires = registration.expires();
                            info!(
                                "[{}] Registered successfully, expires in {}s",
                                username, expires
                            );
                            // Refresh before expiration (e.g., 5 seconds before)
                            let sleep_time = if expires > 5 { expires - 5 } else { expires };
                            tokio::time::sleep(Duration::from_secs(sleep_time as u64)).await;
                        } else {
                            warn!(
                                "[{}] Registration failed: {}",
                                username,
                                response.status_code()
                            );
                            tokio::time::sleep(Duration::from_secs(30)).await;
                        }
                    }
                    Err(e) => {
                        error!("[{}] Registration error: {:?}", username, e);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn listen_loop(&self) -> Result<()> {
        info!(
            "[{}] Listening for incoming calls...",
            self.account.username
        );
        let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;
        let mut incoming = endpoint.incoming_transactions()?;

        while let Some(mut transaction) = incoming.recv().await {
            match transaction.original.method {
                Method::Invite => self.handle_invite(transaction).await?,
                Method::Ack => info!("[{}] Received ACK", self.account.username),
                Method::Bye => {
                    info!("[{}] Received BYE", self.account.username);
                    let id = DialogId::try_from(&transaction.original)?;
                    let dialog = self
                        .dialog_layer
                        .as_ref()
                        .map(|d| d.get_dialog(&id))
                        .flatten();
                    if let Some(mut dlg) = dialog {
                        let _ = dlg.handle(&mut transaction).await?;
                    } else {
                        transaction
                            .reply(rsip::StatusCode::CallTransactionDoesNotExist)
                            .await
                            .ok();
                    }
                }
                Method::Options => {
                    info!("[{}] Received OPTIONS", self.account.username);
                    transaction.reply(StatusCode::OK).await?;
                }
                Method::Info => {
                    info!("[{}] Received INFO", self.account.username);
                    transaction.reply(StatusCode::OK).await?;
                }
                _ => info!(
                    "[{}] Received other method: {:?}",
                    self.account.username, transaction.original.method
                ),
            }
        }
        Ok(())
    }

    async fn handle_invite(&self, mut transaction: Transaction) -> Result<()> {
        let call_id = transaction.original.call_id_header()?.value();
        info!(
            "[{}] Handling INVITE for {} (Call-ID: {})",
            self.account.username, self.account.username, call_id
        );

        let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;
        let addrs = endpoint.get_addrs();
        let local_sip_addr = addrs.first().context("No local address found")?;
        let local_socket = local_sip_addr.get_socketaddr()?;
        let local_ip = local_socket.ip();
        let local_port = local_socket.port();

        let recording_path = self.get_recording_path(call_id);
        info!(
            "[{}] Recording will be saved to: {:?}",
            self.account.username, recording_path
        );

        let dialog_layer = self
            .dialog_layer
            .as_ref()
            .context("DialogLayer not initialized")?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let username = self.account.username.clone();

        let credential = if let Some(password) = &self.account.password {
            Some(Credential {
                username: self.account.username.clone(),
                password: password.clone(),
                realm: Some(self.account.domain.clone()),
            })
        } else {
            None
        };
        let contact_str = format!("sip:{}@{}:{}", self.account.username, local_ip, local_port);
        let server_dialog = dialog_layer.get_or_create_server_invite(
            &transaction,
            tx,
            credential,
            contact_str.try_into().ok(),
        )?;

        // Clone for spawn
        let account = self.account.clone();
        let global_config = self.global_config.clone();
        let server_dialog_clone = server_dialog.clone();
        let offer_body = transaction.original.body().clone();

        tokio::spawn(async move {
            // Spawn transaction handler
            let mut server_dialog_handler = server_dialog_clone.clone();
            tokio::spawn(async move {
                if let Err(e) = server_dialog_handler.handle(&mut transaction).await {
                    error!("Transaction handler error: {:?}", e);
                }
            });

            // Monitor loop
            let username_monitor = username.clone();
            let monitor_future = async move {
                while let Some(state) = rx.recv().await {
                    info!(%state, "[{}] Dialog state changed", username_monitor);
                    if matches!(state, DialogState::Terminated(..)) {
                        info!("[{}] Call terminated remotely", username_monitor);
                        return;
                    }
                }
            };

            // Call logic
            let call_logic = async move {
                // Stage 1: Ringing (Alerting)
                let mut media_session: Option<MediaSession> = None;
                let mut local_sdp: Option<String> = None;

                if !offer_body.is_empty() {
                    if let Ok(body_str) = std::str::from_utf8(&offer_body) {
                        let srtp_enabled = account.srtp_enabled.unwrap_or(false);
                        match MediaSession::new(
                            body_str,
                            srtp_enabled,
                            global_config.external_ip.clone(),
                        )
                        .await
                        {
                            Ok((session, sdp)) => {
                                media_session = Some(session);
                                local_sdp = Some(sdp);
                            }
                            Err(e) => {
                                error!(
                                    "[{}] Failed to create media session: {:?}",
                                    account.username, e
                                );
                            }
                        }
                    }
                }

                if let Some(ref cfg) = account.ring {
                    if let Some(ref wav) = cfg.ringback {
                        info!(
                            "[{}] Stage 1: Ringing with media (183) - Playing {}",
                            account.username, wav
                        );

                        // Send 183 with SDP if we have local SDP
                        if let Some(sdp) = local_sdp.as_ref() {
                            let headers = vec![Header::ContentType("application/sdp".into())];
                            if let Err(e) = server_dialog_clone
                                .ringing(Some(headers), Some(sdp.clone().into_bytes()))
                            {
                                error!("Ringing error: {:?}", e);
                                return;
                            }
                        } else {
                            if let Err(e) = server_dialog_clone.ringing(None, None) {
                                error!("Ringing error: {:?}", e);
                                return;
                            }
                        }

                        if let Some(media) = &mut media_session {
                            // Play file with timeout
                            let _ = tokio::time::timeout(
                                Duration::from_secs(cfg.duration_secs),
                                media.play_file(
                                    account.username.clone(),
                                    std::path::Path::new(wav),
                                    None,
                                    false,
                                ),
                            )
                            .await;
                        } else {
                            tokio::time::sleep(Duration::from_secs(cfg.duration_secs)).await;
                        }
                    } else {
                        info!("[{}] Stage 1: Sending 180 Ringing", account.username);
                        if let Err(e) = server_dialog_clone.ringing(None, None) {
                            error!("Ringing error: {:?}", e);
                            return;
                        }
                        tokio::time::sleep(Duration::from_secs(cfg.duration_secs)).await;
                    }
                }

                // Stage 2: Answer or Reject
                // Always answer with configured or default media
                {
                    info!("[{}] Stage 2: Answering (200 OK)", account.username);
                    let mut headers = vec![];
                    let mut body = None;
                    // Add SDP if we have local SDP
                    if let Some(sdp) = local_sdp.as_ref() {
                        body = Some(sdp.clone().into_bytes());
                        headers.push(Header::ContentType("application/sdp".into()));
                    }

                    if let Err(e) = server_dialog_clone.accept(Some(headers), body) {
                        error!("Accept error: {:?}", e);
                        return;
                    }

                    if media_session.is_none() {
                        warn!(
                            "[{}] No media session established (missing SDP?)",
                            account.username
                        );
                    }

                    let username_media = account.username.clone();
                    let answer_config = account.answer.clone();
                    let hangup_config = account.hangup.clone();
                    let keep_alive = hangup_config.is_some();

                    let media_future = async {
                        if let Some(mut media) = media_session {
                            if let Some(cfg) = answer_config {
                                match cfg {
                                    AnswerConfig::Echo => {
                                        info!("[{}] Stage 2: Starting Echo", username_media);
                                        media
                                            .start_echo(
                                                username_media.clone(),
                                                Some(&recording_path),
                                            )
                                            .await
                                    }
                                    AnswerConfig::Play { wav_file } => {
                                        info!("[{}] Stage 2: Playing {}", username_media, wav_file);
                                        media
                                            .play_file(
                                                username_media.clone(),
                                                std::path::Path::new(&wav_file),
                                                Some(&recording_path),
                                                keep_alive,
                                            )
                                            .await
                                    }
                                }
                            } else {
                                // Default answer
                                info!("[{}] Stage 2: Playing default answer", username_media);
                                media
                                    .play_wav_bytes(
                                        username_media,
                                        ANSWER_WAV,
                                        Some(&recording_path),
                                        keep_alive,
                                    )
                                    .await
                            }
                        } else {
                            Ok(())
                        }
                    };

                    if let Some(ref hangup) = hangup_config {
                        if let Some(secs) = hangup.after_secs {
                            info!(
                                "[{}] Stage 3: Will hangup after {} seconds",
                                account.username, secs
                            );
                            tokio::select! {
                                res = media_future => {
                                    if let Err(e) = res {
                                        error!("[{}] Media error: {:?}", account.username, e);
                                    }
                                    info!("[{}] Media finished", account.username);
                                }
                                _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                                    info!("[{}] Hangup timer expired", account.username);
                                }
                            }
                            info!("[{}] Stage 3: Sending BYE", account.username);
                            if let Err(e) = server_dialog_clone.bye().await {
                                error!("[{}] Failed to send BYE: {:?}", account.username, e);
                            }
                        } else {
                            if let Err(e) = media_future.await {
                                error!("[{}] Media error: {:?}", account.username, e);
                            }
                        }
                    } else {
                        if let Err(e) = media_future.await {
                            error!("[{}] Media error: {:?}", account.username, e);
                        }
                    }
                }
            };

            tokio::select! {
                _ = monitor_future => {
                    // Terminated early
                }
                _ = call_logic => {
                    // Finished normally
                }
            }
        });

        Ok(())
    }
}

fn generate_random_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    format!("{:x}", since_the_epoch.as_nanos())
}
