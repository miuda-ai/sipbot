use crate::config::{AccountConfig, AnswerConfig, Config};
use crate::media::MediaSession;
use anyhow::{Context, Result};
use chrono::Local;
use rsip::headers::{
    CallId, Contact as UntypedContact, From as UntypedFrom, To as UntypedTo, UntypedHeader,
    Via as UntypedVia,
};
use rsip::message::HeadersExt;
use rsip::typed::{From, To, Via};
use rsip::{Header, Method, StatusCode, Uri};
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
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
// use voice_engine::net_tool::extract_rtp_addresses_from_sdp;

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

    pub async fn run_call(&mut self) -> Result<()> {
        self.init_endpoint().await?;

        // Register
        if self.account.register.unwrap_or(true) {
            self.start_registration_loop().await?;
        }

        if let Some(target) = &self.account.target {
            info!(
                "[{}] Starting outbound call to {}",
                self.account.username, target
            );
            self.make_call(target).await?;
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

    async fn make_call(&self, target_uri: &str) -> Result<()> {
        let dialog_layer = self
            .dialog_layer
            .as_ref()
            .context("DialogLayer not initialized")?;
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
        let _via = Via::try_from(untyped_via)?;

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

        let contact_str = format!(
            "<sip:{}@{}:{}>",
            self.account.username, local_ip, local_port
        );
        let contact_header = UntypedContact::try_from(contact_str.as_str())?;

        // Create MediaSession and Offer
        let (media_session, local_sdp) = MediaSession::new_offer().await?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let username = self.account.username.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                info!("[{}] Call Status: {}", username, event);
            }
        });

        let opt = InviteOption {
            destination: Some(req_uri.host_with_port.clone().into()),
            caller: from.uri.clone(),
            callee: to.uri.clone(),
            call_id: Some(call_id.value().to_string()),
            contact: contact_header.uri()?.clone(),
            caller_display_name: None,
            caller_params: Default::default(),
            content_type: Some("application/sdp".to_string()),
            offer: Some(local_sdp.into_bytes()),
            headers: Default::default(),
            credential: None,
            support_prack: false,
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
            if *res.status_code() == StatusCode::OK {
                info!("[{}] Call answered!", self.account.username);

                let body = res.body();
                if !body.is_empty() {
                    let remote_sdp = String::from_utf8_lossy(body);
                    media_session.set_remote_answer(&remote_sdp).await?;
                }
            } else {
                warn!(
                    "[{}] Call failed: {}",
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

        let media_session_clone = media_session.clone();
        let answer_config = self.account.answer.clone();

        let play_future = async move {
            if let Some(answer_config) = answer_config {
                match answer_config {
                    AnswerConfig::Play { wav_file } => {
                        let file_path = PathBuf::from(wav_file);
                        if let Err(e) = media_session_clone.play_file(&file_path, None).await {
                            error!("Failed to play file: {:?}", e);
                        }
                    }
                    _ => {}
                }
            }
        };

        if let Some(secs) = hangup_secs {
            info!(
                "[{}] Call established. Waiting for {} seconds (or Ctrl-C) before hanging up...",
                self.account.username, secs
            );

            tokio::spawn(play_future);

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                    info!("[{}] {} seconds elapsed.", self.account.username, secs);
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("[{}] Ctrl-C received.", self.account.username);
                }
            }
        } else {
            info!(
                "[{}] Call established. Waiting for playback or Ctrl-C to hang up...",
                self.account.username
            );
            if self.account.answer.is_some() {
                tokio::select! {
                    _ = play_future => {
                        info!("[{}] Playback finished.", self.account.username);
                    }
                    _ = tokio::signal::ctrl_c() => {
                        info!("[{}] Ctrl-C received.", self.account.username);
                    }
                }
            } else {
                tokio::signal::ctrl_c().await?;
                info!("[{}] Ctrl-C received.", self.account.username);
            }
        }

        info!("[{}] Sending BYE...", self.account.username);
        dialog.bye().await?;
        info!("[{}] BYE sent.", self.account.username);

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
                Method::Invite => self.handle_invite(&mut transaction).await?,
                Method::Ack => info!("[{}] Received ACK", self.account.username),
                Method::Bye => {
                    info!("[{}] Received BYE", self.account.username);
                    transaction.reply(StatusCode::OK).await?;
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

    async fn send_bye(&self, invite: &rsip::Request) -> Result<()> {
        let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;

        // Destination URI: Use From header from INVITE (simplification)
        let dest_uri = invite.from_header()?.uri()?.clone();

        // From: Us (The To header from INVITE)
        let to_header = invite.to_header()?;
        let mut from = From::from(to_header.uri()?.clone());

        if let Some(tag) = to_header.tag()? {
            from = from.with_tag(tag.clone());
        } else {
            from = from.with_tag(generate_random_string().into());
        }

        // To: Them (The From header from INVITE)
        let from_header = invite.from_header()?;
        let mut to = To::from(from_header.uri()?.clone());

        if let Some(tag) = from_header.tag()? {
            to = to.with_tag(tag.clone());
        }

        // Call-ID: Same as INVITE
        let call_id = invite.call_id_header()?.clone();

        let addrs = endpoint.get_addrs();
        let local_sip_addr = addrs.first().context("No local address found")?;
        let local_socket = local_sip_addr.get_socketaddr()?;

        let via_str = format!(
            "SIP/2.0/UDP {}:{};branch=z9hG4bK{}",
            local_socket.ip(),
            local_socket.port(),
            generate_random_string()
        );
        let untyped_via = UntypedVia::try_from(via_str.as_str())?;
        let via = Via::try_from(untyped_via)?;

        let request = endpoint.inner.make_request(
            Method::Bye,
            dest_uri,
            via,
            from,
            to,
            20, // CSeq
            Some(call_id.into()),
        );

        let key = TransactionKey::from_request(&request, TransactionRole::Client)?;
        let mut transaction = Transaction::new_client(key, request, endpoint.inner.clone(), None);

        transaction.send().await?;
        Ok(())
    }

    async fn handle_invite(&self, transaction: &mut Transaction) -> Result<()> {
        let call_id = transaction.original.call_id_header()?.value();
        info!(
            "[{}] Handling INVITE for {} (Call-ID: {})",
            self.account.username, self.account.username, call_id
        );

        let recording_path = self.get_recording_path(call_id);
        info!(
            "[{}] Recording will be saved to: {:?}",
            self.account.username, recording_path
        );

        // Stage 1: Ringing (Alerting)
        let mut media_session: Option<MediaSession> = None;
        let mut local_sdp: Option<String> = None;

        if !transaction.original.body.is_empty() {
            if let Ok(body_str) = std::str::from_utf8(&transaction.original.body) {
                match MediaSession::new(body_str).await {
                    Ok((session, sdp)) => {
                        media_session = Some(session);
                        local_sdp = Some(sdp);
                    }
                    Err(e) => {
                        error!(
                            "[{}] Failed to create media session: {:?}",
                            self.account.username, e
                        );
                    }
                }
            }
        }

        if let Some(ref cfg) = self.account.ring {
            if let Some(ref wav) = cfg.ringback {
                info!(
                    "[{}] Stage 1: Ringing with media (183) - Playing {}",
                    self.account.username, wav
                );

                // Send 183 with SDP if we have local SDP
                if let Some(sdp) = local_sdp.as_ref() {
                    let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;

                    let mut response = endpoint.inner.make_response(
                        &transaction.original,
                        StatusCode::SessionProgress,
                        None,
                    );
                    response.body = sdp.clone().into_bytes();
                    response.headers.push(Header::ContentType(
                        rsip::headers::ContentType::try_from("application/sdp").unwrap(),
                    ));

                    // Update Content-Length
                    response
                        .headers
                        .retain(|h| !matches!(h, Header::ContentLength(_)));
                    response.headers.push(Header::ContentLength(
                        rsip::headers::ContentLength::from(response.body.len() as u32),
                    ));

                    // Add To tag if missing
                    let mut to = response.to_header()?.clone();
                    if to.tag()?.is_none() {
                        to = to.with_tag(generate_random_string().into())?;
                        response.headers.retain(|h| !matches!(h, Header::To(_)));
                        response.headers.push(Header::To(to));
                    }

                    transaction.respond(response).await?;
                } else {
                    transaction.reply(StatusCode::SessionProgress).await?;
                }

                if let Some(media) = &mut media_session {
                    // Play file with timeout
                    let _ = tokio::time::timeout(
                        Duration::from_secs(cfg.duration_secs),
                        media.play_file(std::path::Path::new(wav), None),
                    )
                    .await;
                } else {
                    tokio::time::sleep(Duration::from_secs(cfg.duration_secs)).await;
                }
            } else {
                info!("[{}] Stage 1: Sending 180 Ringing", self.account.username);
                transaction.reply(StatusCode::Ringing).await?;
                tokio::time::sleep(Duration::from_secs(cfg.duration_secs)).await;
            }
        }

        // Stage 2: Answer or Reject
        if let Some(ref cfg) = self.account.answer {
            info!("[{}] Stage 2: Answering (200 OK)", self.account.username);

            let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;

            let addrs = endpoint.get_addrs();
            let local_sip_addr = addrs.first().context("No local address found")?;
            let local_socket = local_sip_addr.get_socketaddr()?;

            let contact_str = format!(
                "<sip:{}@{}:{}>",
                self.account.username,
                local_socket.ip(),
                local_socket.port()
            );
            let contact_header = UntypedContact::try_from(contact_str.as_str())?;

            let mut response =
                endpoint
                    .inner
                    .make_response(&transaction.original, StatusCode::OK, None);

            // Add To tag if missing
            let mut to = response.to_header()?.clone();
            if to.tag()?.is_none() {
                to = to.with_tag(generate_random_string().into())?;
                response.headers.retain(|h| !matches!(h, Header::To(_)));
                response.headers.push(Header::To(to));
            }

            response.headers.push(Header::Contact(contact_header));

            // Add SDP if we have local SDP
            if let Some(sdp) = local_sdp.as_ref() {
                response.body = sdp.clone().into_bytes();
                response.headers.push(Header::ContentType(
                    rsip::headers::ContentType::try_from("application/sdp").unwrap(),
                ));

                // Update Content-Length
                response
                    .headers
                    .retain(|h| !matches!(h, Header::ContentLength(_)));
                response
                    .headers
                    .push(Header::ContentLength(rsip::headers::ContentLength::from(
                        response.body.len() as u32,
                    )));
            }

            transaction.respond(response).await?;

            // If media session was not created (e.g. no SDP in INVITE?), try to create it now?
            // But we need remote address. If INVITE had no SDP, we expect ACK to have SDP.
            // Handling late negotiation is complex. I'll assume INVITE has SDP for now.

            if media_session.is_none() {
                // Try to parse again? No, we already tried.
                warn!(
                    "[{}] No media session established (missing SDP?)",
                    self.account.username
                );
            }

            let media_future = async {
                if let Some(mut media) = media_session {
                    match cfg {
                        AnswerConfig::Echo => {
                            info!("[{}] Stage 2: Starting Echo", self.account.username);
                            media.start_echo(Some(&recording_path)).await
                        }
                        AnswerConfig::Play { wav_file } => {
                            info!("[{}] Stage 2: Playing {}", self.account.username, wav_file);
                            media
                                .play_file(std::path::Path::new(wav_file), Some(&recording_path))
                                .await
                        }
                    }
                } else {
                    Ok(())
                }
            };

            if let Some(ref hangup) = self.account.hangup {
                if let Some(secs) = hangup.after_secs {
                    info!(
                        "[{}] Stage 3: Will hangup after {} seconds",
                        self.account.username, secs
                    );
                    tokio::select! {
                        res = media_future => {
                            if let Err(e) = res {
                                error!("[{}] Media error: {:?}", self.account.username, e);
                            }
                            info!("[{}] Media finished", self.account.username);
                        }
                        _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                            info!("[{}] Hangup timer expired", self.account.username);
                        }
                    }
                    info!("[{}] Stage 3: Sending BYE", self.account.username);
                    if let Err(e) = self.send_bye(&transaction.original).await {
                        error!("[{}] Failed to send BYE: {:?}", self.account.username, e);
                    }
                } else {
                    media_future.await?;
                }
            } else {
                media_future.await?;
            }
        } else {
            // No Answer config -> Reject
            if let Some(ref hangup) = self.account.hangup {
                info!(
                    "[{}] No answer config, rejecting with code {}",
                    self.account.username, hangup.code
                );
                match StatusCode::try_from(hangup.code) {
                    Ok(code) => transaction.reply(code).await?,
                    Err(_) => {
                        error!(
                            "[{}] Invalid status code: {}",
                            self.account.username, hangup.code
                        );
                        transaction.reply(StatusCode::ServerInternalError).await?;
                    }
                }
            } else {
                warn!(
                    "[{}] No answer/hangup config. Rejecting with 603 Decline.",
                    self.account.username
                );
                transaction.reply(StatusCode::Decline).await?;
            }
        }
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
