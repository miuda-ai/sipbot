use crate::config::{AccountConfig, AnswerConfig, Config, ReinviteAction};
use crate::media::MediaSession;
use crate::stats::CallStats;
use anyhow::{Context, Result};
use chrono::Local;
use rand::RngExt;
use rsipstack::dialog::DialogId;
use rsipstack::dialog::dialog::{Dialog, DialogState};
use rsipstack::rsip::headers::ToTypedHeader;
use rsipstack::rsip::message::HeadersExt;
use rsipstack::rsip::{Header, Method, StatusCode, Transport, Uri};
use rsipstack::sip::{Host, HostWithPort};
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
    transport::{SipAddr, SipConnection, TransportLayer, tcp_listener::TcpListenerConnection, udp::UdpConnection},
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const ANSWER_WAV: &[u8] = include_bytes!("../wavs/play.wav");
const RINGING_WAV: &[u8] = include_bytes!("../wavs/ringing.wav");

#[derive(Clone)]
struct CallRunner {
    dialog_layer: Arc<DialogLayer>,
    account: AccountConfig,
    global_config: Config,
    stats: Arc<CallStats>,
    cancel_token: CancellationToken,
    current_media_session: Arc<tokio::sync::Mutex<Option<MediaSession>>>,
}

struct CallGuard {
    stats: Arc<CallStats>,
    start_time: std::time::Instant,
    media_session: Option<MediaSession>,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.stats.inc_finished();
        self.stats.add_duration(self.start_time.elapsed());
        if let Some(media) = self.media_session.take() {
            let media_clone = media.clone();
            tokio::spawn(async move {
                media_clone.stop().await;
            });
        }
    }
}

impl CallRunner {
    fn build_record_path(&self, call_index: u32) -> Option<PathBuf> {
        self.account.record.clone().map(|p| {
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
        })
    }

    /// Handle early media (183) response
    async fn handle_early_media(
        &self,
        media_session: &MediaSession,
        answer_sdp: &str,
    ) -> Option<tokio::task::JoinHandle<()>> {
        debug!(
            "[{}] Received Early Media (183) SDP:\n{}",
            self.account.username, answer_sdp
        );
        info!(
            "[{}] EARLY_MEDIA_183_SDP\n{}",
            self.account.username, answer_sdp
        );
        match media_session
            .set_remote_answer_typed(answer_sdp, rustrtc::sdp::SdpType::Pranswer)
            .await
        {
            Ok(codec_name) => {
                info!(
                    "[{}] Early media (183) remote description set, codec: {}",
                    self.account.username, codec_name
                );
            }
            Err(e) => {
                warn!(
                    "[{}] Failed to set remote answer for early media (183): {:?}",
                    self.account.username, e
                );
                return None;
            }
        }

        info!(
            "[{}] Early media session established",
            self.account.username
        );
        let media_clone = media_session.clone();
        let username = self.account.username.clone();
        let answer_config = self.account.answer.clone();

        // Caller mode (outbound `call`, target set):
        // - Do NOT start file/echo TX playback here. The final 200 OK may
        //   negotiate a different codec than the 183 early media; starting
        //   send-side playback with the 183 codec can leave the sender stamped
        //   for the wrong codec after answer.
        // - DO open the local device when configured for Local, so the caller
        //   can hear the remote CRBT / ringback on 183. Mic is muted until
        //   200 OK (see start_media_playback).
        // - Otherwise only observe RX so early-media RTP is counted in stats.
        let is_caller = self.account.target.is_some();
        if is_caller {
            if matches!(answer_config, Some(AnswerConfig::Local)) {
                info!(
                    "[{}] Playing remote early media (CRBT/彩铃) on local device",
                    username
                );
                media_clone.set_audio_silent(true).await;
                #[cfg(feature = "local-device")]
                {
                    return Some(tokio::spawn(async move {
                        if let Err(e) = media_clone
                            .play_local_device(username.clone(), None, true, None)
                            .await
                        {
                            error!(
                                "[{}] Failed to play remote early media on local device: {:?}",
                                username, e
                            );
                        }
                    }));
                }
                #[cfg(not(feature = "local-device"))]
                {
                    warn!(
                        "[{}] Local device support is disabled; cannot play remote early media",
                        username
                    );
                    let _rx = media_clone
                        .start_rx_observer(format!("{username}-early-media"))
                        .await;
                    return None;
                }
            }

            let _rx = media_clone
                .start_rx_observer(format!("{username}-early-media"))
                .await;
            return None;
        }

        let rx_observer = media_clone
            .start_rx_observer(format!("{username}-early-media"))
            .await;

        Some(tokio::spawn(async move {
            // Keep observing inbound RTP while early media plays so RX stats
            // reflect any audio the remote sends as 183 (ringback/busy/...).
            let _rx = rx_observer;
            match answer_config {
                Some(AnswerConfig::Echo) => {
                    let _ = media_clone.start_echo(username, None).await;
                }
                Some(AnswerConfig::Play { wav_file }) => {
                    let _ = media_clone
                        .play_file(username, Path::new(&wav_file), None, true)
                        .await;
                }
                Some(AnswerConfig::Local) => {
                    #[cfg(feature = "local-device")]
                    if let Err(e) = media_clone
                        .play_local_device(username.clone(), None, true, None)
                        .await
                    {
                        error!(
                            "[{}] Failed to play local device during early media: {:?}, falling back to file",
                            username, e
                        );
                        let _ = media_clone
                            .play_wav_bytes(username, RINGING_WAV, None, true)
                            .await;
                    }
                    #[cfg(not(feature = "local-device"))]
                    {
                        let _ = media_clone
                            .play_wav_bytes(username, RINGING_WAV, None, true)
                            .await;
                    }
                }
                None => {}
            }
        }))
    }

    /// Start media playback based on configuration
    async fn start_media_playback(
        username: String,
        media_session: MediaSession,
        record_path: Option<PathBuf>,
        media_task: Option<tokio::task::JoinHandle<()>>,
        keep_alive: bool,
        answer_config: Option<AnswerConfig>,
    ) {
        if let Some(task) = media_task {
            // Early media already opened the local device (e.g. to hear remote
            // CRBT). Unmute mic now that the call is answered, attach recorder
            // if needed, then keep the session until hangup.
            media_session.set_audio_silent(false).await;
            info!("[{}] Using already started early media.", username);
            #[cfg(feature = "local-device")]
            {
                let _ = media_session
                    .play_local_device(username.clone(), record_path.as_deref(), keep_alive, None)
                    .await;
            }
            let _ = task.await;
            return;
        }

        let record_path_ref = record_path.as_deref();
        if let Some(answer_config) = &answer_config {
            match answer_config {
                AnswerConfig::Play { wav_file } => {
                    let file_path = PathBuf::from(wav_file);
                    if let Err(e) = media_session
                        .play_file(username, &file_path, record_path_ref, keep_alive)
                        .await
                    {
                        error!("Failed to play file: {:?}", e);
                    }
                }
                AnswerConfig::Local => {
                    media_session.set_audio_silent(false).await;
                    #[cfg(feature = "local-device")]
                    if let Err(e) = media_session
                        .play_local_device(username.clone(), record_path_ref, keep_alive, None)
                        .await
                    {
                        error!("Failed to play local device: {:?}, falling back to file", e);
                        if let Err(e) = media_session
                            .play_wav_bytes(username, RINGING_WAV, record_path_ref, keep_alive)
                            .await
                        {
                            error!("Fallback failed: {:?}", e);
                        }
                    }
                    #[cfg(not(feature = "local-device"))]
                    {
                        error!("Local device support is disabled in this build");
                        if let Err(e) = media_session
                            .play_wav_bytes(username, RINGING_WAV, record_path_ref, keep_alive)
                            .await
                        {
                            error!("Fallback failed: {:?}", e);
                        }
                    }
                }
                _ => {}
            }
        } else {
            if let Err(e) = media_session
                .play_wav_bytes(username, RINGING_WAV, record_path_ref, keep_alive)
                .await
            {
                warn!("Play built-in answer stopped: {:?}", e);
            }
        }
    }

    async fn make_call(&self, target_uri: String, call_index: u32) -> Result<()> {
        self.stats.inc_current();
        let mut _guard = CallGuard {
            stats: self.stats.clone(),
            start_time: std::time::Instant::now(),
            media_session: None,
        };

        debug!(
            "[{}] Account config: username={}, domain={}, target={:?}",
            self.account.username, self.account.username, self.account.domain, self.account.target
        );

        let dialog_layer = &self.dialog_layer;
        let from: rsipstack::rsip::Uri = if let Some(from_str) = &self.account.from_user {
            if from_str.starts_with("sip:") {
                let mut uri: rsipstack::rsip::Uri = from_str.as_str().try_into()?;
                if uri.auth.as_ref().map_or(true, |a| a.user.is_empty()) {
                    uri.auth = Some(rsipstack::rsip::Auth {
                        user: self.account.username.clone(),
                        password: None,
                    });
                }
                uri
            } else {
                format!("sip:{}@{}", from_str, self.account.domain).try_into()?
            }
        } else {
            format!("sip:{}@{}", self.account.username, self.account.domain).try_into()?
        };
        let to: rsipstack::rsip::Uri = target_uri.as_str().try_into()?;
        let contact =
            dialog_layer.build_local_contact(Some(self.account.username.clone()), None)?;

        info!(
            "[{}] Calling {} from {} (contact: {})",
            self.account.username, to, from, contact
        );
        // Create MediaSession and Offer
        let srtp_enabled = self.account.srtp_enabled.unwrap_or(false);
        let webrtc_enabled = self.account.webrtc_enabled.unwrap_or(false);
        let nack_enabled = self.account.nack_enabled.unwrap_or(false);
        let jitter_buffer_enabled = self.account.jitter_buffer_enabled.unwrap_or(false);
        let (media_session, local_sdp) = MediaSession::new_offer(
            srtp_enabled,
            webrtc_enabled,
            nack_enabled,
            jitter_buffer_enabled,
            self.global_config.external_ip.clone(),
            self.account.codecs.clone(),
            true,
            self.stats.clone(),
            self.account.audio_quality.clone(),
            self.account.ts_jump_tolerance_ms,
        )
        .await?;
        _guard.media_session = Some(media_session.clone());
        {
            let mut guard = self.current_media_session.lock().await;
            *guard = Some(media_session.clone());
        }

        if local_sdp.is_empty() {
            anyhow::bail!("[{}] Generated empty Offer SDP", self.account.username);
        }

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

        let destination = if let Some(proxy) = &self.account.proxy {
            let proxy_uri = Uri::try_from(normalize_sip_addr(proxy).as_str())?;
            SipAddr::try_from(&proxy_uri)?
        } else {
            SipAddr::try_from(&to)?
        };

        let mut custom_headers = vec![];
        if let Some(headers) = &self.account.headers {
            for header_str in headers {
                if let Some((name, value)) = header_str.split_once(':') {
                    let name = name.trim();
                    let value = value.trim();
                    custom_headers.push(Header::Other(name.to_string(), value.into()));
                }
            }
        }

        let opt = InviteOption {
            destination: Some(destination),
            caller: from.clone(),
            callee: to.clone(),
            contact,
            content_type: Some("application/sdp".to_string()),
            offer: Some(local_sdp.into_bytes()),
            credential,
            headers: Some(custom_headers),
            ..Default::default()
        };

        let dialog;
        let response;
        let mut media_task = None;
        let mut should_cancel;
        let cancel_before_ringring: bool;
        {
            let mut rng = rand::rng();
            should_cancel = rng.random_range(0..100) <= self.account.cancel_prob;
            cancel_before_ringring = rng.random();
        }

        let invite_started = std::time::Instant::now();
        let invite = dialog_layer.do_invite(opt, tx);
        tokio::pin!(invite);

        let mut latest_dialog_id = None;

        loop {
            tokio::select! {
                res = &mut invite => {
                    let (dial , resp) = res?;
                    response = resp;
                    dialog = dial;
                    break;
                }
                state = rx.recv() => {
                    if let Some(state) = state {
                        if let DialogState::Trying(d_id) = &state {
                            latest_dialog_id = Some(d_id.clone());
                        } else if let DialogState::Early(d_id, res) = &state {
                             let code: u16 = res.status_code().clone().into();
                            self.stats.add_status(code);
                            latest_dialog_id = Some(d_id.clone());

                            if code == 183 {
                                let answer_sdp = String::from_utf8_lossy(&res.body);
                                if !answer_sdp.is_empty() && media_task.is_none() {
                                    media_task = self.handle_early_media(&media_session, &answer_sdp).await;
                                }
                            }
                        }

                        if should_cancel {
                            if cancel_before_ringring && let DialogState::Trying(mut dialog_id) = state {
                                tracing::info!("[{}] Canceling call before ringring", self.account.username);
                                dialog_id.remote_tag.clear();
                                let dialog = dialog_layer.get_dialog(&dialog_id).expect("dialog not found");
                                should_cancel = false;
                                let _ = dialog.hangup().await;
                            }else if !cancel_before_ringring && let DialogState::Early(mut dialog_id, _) = state{
                                tracing::info!("[{}] Canceling call after ringring", self.account.username);
                                dialog_id.remote_tag.clear();
                                let dialog = dialog_layer.get_dialog(&dialog_id).expect("dialog not found");
                                should_cancel = false;
                                let _ = dialog.hangup().await;
                            }
                        }
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    info!("[{}] Cancellation requested during INVITE phase.", self.account.username);
                    if let Some(mut dialog_id) = latest_dialog_id {
                         dialog_id.remote_tag.clear();
                         if let Some(dialog) = dialog_layer.get_dialog(&dialog_id) {
                              info!("[{}] Cancelling pending INVITE...", self.account.username);
                             let _ = dialog.hangup().await;
                         }
                    }
                    return Ok(());
                }
            }
        }

        if let Some(res) = response {
            self.stats.add_status(res.status_code().clone().into());
            info!(
                "[{}] Received INVITE response: {}",
                self.account.username,
                res.status_code()
            );
            if matches!(
                res.status_code().kind(),
                rsipstack::rsip::status_code::StatusCodeKind::Successful
            ) {
                let answer_sdp = String::from_utf8_lossy(&res.body);
                debug!(
                    "[{}] Received 200 OK Answer SDP:\n{}",
                    self.account.username, answer_sdp
                );
                let codec_name = match media_session.set_remote_answer(&answer_sdp).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(
                            "[{}] set_remote_answer on 200 OK FAILED: {:?}",
                            self.account.username, e
                        );
                        error!(
                            "[{}] 200 OK Answer SDP was:\n{}",
                            self.account.username, answer_sdp
                        );
                        return Err(e);
                    }
                };
                info!(
                    "[{}] 200 OK remote description set (supports reinvite after 183), codec: {}",
                    self.account.username, codec_name
                );
                info!(
                    "[{}] Call established: From={}, To={}, Preferred Codec={}",
                    self.account.username, from, to, codec_name
                );
                self.stats.add_setup_latency(invite_started.elapsed());
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
        let record_path = self.build_record_path(call_index);
        let keep_alive = hangup_secs.is_some();

        let username_clone = self.account.username.clone();
        let answer_config_clone = self.account.answer.clone();
        let media_session_clone = media_session.clone();

        let play_future = Self::start_media_playback(
            username_clone,
            media_session_clone,
            record_path,
            media_task,
            keep_alive,
            answer_config_clone,
        );

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

        let dtmf_flows = self
            .account
            .dtmf_flows
            .as_deref()
            .and_then(|s| crate::config::parse_dtmf_flows(s).ok());
        let dtmf_media = media_session.clone();
        let dtmf_username = self.account.username.clone();
        let dtmf_cancel = self.cancel_token.clone();
        let dtmf_future = async move {
            if let Some(ref flows) = dtmf_flows {
                info!(
                    "[{}] DTMF flow: {} entries scheduled",
                    dtmf_username,
                    flows.len()
                );
                for entry in flows {
                    tokio::select! {
                        _ = tokio::time::sleep(entry.delay) => {}
                        _ = dtmf_cancel.cancelled() => return,
                    }
                    info!(
                        "[{}] Sending DTMF '{}' (after {:.1}s)",
                        dtmf_username,
                        entry.digit,
                        entry.delay.as_secs_f64()
                    );
                    let _: Result<(), anyhow::Error> = dtmf_media.send_dtmf(entry.digit).await;
                    info!("[{}] DTMF '{}' sent", dtmf_username, entry.digit);
                }
            }
        };

        let reinvite_flows = self
            .account
            .reinvite_flows
            .as_deref()
            .and_then(|s| crate::config::parse_reinvite_flows(s).ok());
        let reinvite_media = media_session.clone();
        let reinvite_dialog = dialog.clone();
        let reinvite_username = self.account.username.clone();
        let reinvite_cancel = self.cancel_token.clone();
        let reinvite_future = async move {
            if let Some(ref flows) = reinvite_flows {
                info!(
                    "[{}] Re-INVITE flow: {} entries scheduled",
                    reinvite_username,
                    flows.len()
                );
                for entry in flows {
                    tokio::select! {
                        _ = tokio::time::sleep(entry.delay) => {}
                        _ = reinvite_cancel.cancelled() => {
                            info!("[{}] Re-INVITE flow cancelled", reinvite_username);
                            return;
                        }
                    }

                    let is_hold = matches!(entry.action, ReinviteAction::Hold);
                    info!(
                        "[{}] Sending re-INVITE {} (after {:.1}s)",
                        reinvite_username,
                        if is_hold { "HOLD" } else { "RESUME" },
                        entry.delay.as_secs_f64()
                    );

                    let offer_sdp = match reinvite_media.create_reinvite_offer(is_hold).await {
                        Ok(sdp) => sdp,
                        Err(e) => {
                            warn!(
                                "[{}] Failed to create re-INVITE offer: {:?}",
                                reinvite_username, e
                            );
                            continue;
                        }
                    };

                    let headers = vec![Header::ContentType("application/sdp".into())];
                    let body = offer_sdp.into_bytes();

                    let response = reinvite_dialog.reinvite(Some(headers), Some(body)).await;

                    match response {
                        Ok(Some(resp))
                            if matches!(
                                resp.status_code().kind(),
                                rsipstack::rsip::status_code::StatusCodeKind::Successful
                            ) =>
                        {
                            let answer_sdp = String::from_utf8_lossy(&resp.body).to_string();
                            if !answer_sdp.is_empty() {
                                if let Err(e) = reinvite_media.set_remote_answer(&answer_sdp).await
                                {
                                    warn!(
                                        "[{}] Failed to set remote answer for re-INVITE: {:?}",
                                        reinvite_username, e
                                    );
                                }
                            }
                            reinvite_media.set_audio_silent(is_hold).await;
                            info!(
                                "[{}] Re-INVITE {} completed successfully",
                                reinvite_username,
                                if is_hold { "HOLD" } else { "RESUME" }
                            );
                        }
                        Ok(Some(resp)) => {
                            warn!(
                                "[{}] Re-INVITE rejected: {}",
                                reinvite_username,
                                resp.status_code()
                            );
                        }
                        Ok(None) => {
                            warn!("[{}] Re-INVITE got no response", reinvite_username);
                        }
                        Err(e) => {
                            warn!("[{}] Re-INVITE failed: {:?}", reinvite_username, e);
                        }
                    }
                }
            }
        };

        let info_flows = self
            .account
            .info_flows
            .as_deref()
            .and_then(|s| crate::config::parse_info_flows(s).ok());
        let info_dialog = dialog.clone();
        let info_username = self.account.username.clone();
        let info_cancel = self.cancel_token.clone();
        let info_future = async move {
            if let Some(ref flows) = info_flows {
                info!(
                    "[{}] INFO flow: {} entries scheduled",
                    info_username,
                    flows.len()
                );
                for entry in flows {
                    tokio::select! {
                        _ = tokio::time::sleep(entry.delay) => {}
                        _ = info_cancel.cancelled() => {
                            info!("[{}] INFO flow cancelled", info_username);
                            return;
                        }
                    }
                    info!(
                        "[{}] Sending SIP INFO {} ({} bytes, after {:.1}s)",
                        info_username,
                        entry.content_type,
                        entry.body.len(),
                        entry.delay.as_secs_f64()
                    );
                    let headers = vec![Header::ContentType(rsipstack::sip::ContentType(
                        entry.content_type.clone(),
                    ))];
                    let body = entry.body.as_bytes().to_vec();
                    match info_dialog
                        .request(Method::Info, Some(headers), Some(body))
                        .await
                    {
                        Ok(Some(resp)) => {
                            info!(
                                "[{}] SIP INFO response: {}",
                                info_username,
                                resp.status_code()
                            );
                        }
                        Ok(None) => {
                            warn!("[{}] SIP INFO got no response", info_username);
                        }
                        Err(e) => {
                            warn!("[{}] SIP INFO failed: {:?}", info_username, e);
                        }
                    }
                }
            }
        };

        if let Some(secs) = hangup_secs {
            info!(
                "[{}] Call established. Waiting for {} seconds (or Ctrl-C) before hanging up...",
                self.account.username, secs
            );

            let play_handle = tokio::spawn(play_future);
            let dtmf_handle = tokio::spawn(dtmf_future);
            let reinvite_handle = tokio::spawn(reinvite_future);
            let info_handle = tokio::spawn(info_future);

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                    info!("[{}] {} seconds elapsed.", self.account.username, secs);
                }
                _ = monitor_future => {
                    play_handle.abort();
                    dtmf_handle.abort();
                    reinvite_handle.abort();
                    info_handle.abort();
                    return Ok(());
                }
                _ = self.cancel_token.cancelled() => {
                    info!("[{}] Cancellation requested.", self.account.username);
                }
            }
            play_handle.abort();
            dtmf_handle.abort();
            reinvite_handle.abort();
            info_handle.abort();
        } else {
            info!(
                "[{}] Call established. Waiting for playback to hang up...",
                self.account.username
            );
            let dtmf_handle = tokio::spawn(dtmf_future);
            let reinvite_handle = tokio::spawn(reinvite_future);
            let info_handle = tokio::spawn(info_future);
            tokio::select! {
                _ = play_future => {
                    info!("[{}] Playback finished.", self.account.username);
                }
                _ = monitor_future => {
                    dtmf_handle.abort();
                    reinvite_handle.abort();
                    info_handle.abort();
                    return Ok(());
                }
                _ = self.cancel_token.cancelled() => {
                    info!("[{}] Cancellation requested.", self.account.username);
                }
            }
            dtmf_handle.abort();
            reinvite_handle.abort();
            info_handle.abort();
        }

        info!("[{}] Sending BYE...", self.account.username);
        dialog.hangup().await?;
        info!("[{}] BYE sent.", self.account.username);
        media_session.stop().await;
        Ok(())
    }
}

pub struct SipBot {
    pub account: AccountConfig,
    global_config: Config,
    endpoint: Option<Arc<Endpoint>>,
    dialog_layer: Option<Arc<DialogLayer>>,
    registration: Option<Registration>,
    stats: Arc<CallStats>,
    pub verbose: bool,
    pub is_wait: bool,
    pub cancel_token: CancellationToken,
    transport_token: CancellationToken,
    pub current_media_session: Arc<tokio::sync::Mutex<Option<MediaSession>>>,
    /// serve 模式下用于上报注册状态到 Web UI。
    pub status_board: Option<Arc<crate::status::StatusBoard>>,
    /// serve 模式下采集 SIP 信令(收发均记录)到 Web UI。
    pub trace_inspector: Option<Box<dyn rsipstack::transaction::endpoint::MessageInspector>>,
    /// serve 模式: 通话记录表(每通话详情)。
    pub call_registry: Option<Arc<crate::web::state::CallRegistry>>,
    /// serve 模式: WebSocket 事件广播。
    pub ws_events: Option<tokio::sync::broadcast::Sender<crate::web::state::WsEvent>>,
}

impl SipBot {
    pub fn new(
        account: AccountConfig,
        global_config: Config,
        stats: Arc<CallStats>,
        verbose: bool,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            account,
            global_config,
            endpoint: None,
            dialog_layer: None,
            registration: None,
            stats,
            verbose,
            is_wait: false,
            transport_token: CancellationToken::new(),
            cancel_token,
            current_media_session: Arc::new(tokio::sync::Mutex::new(None)),
            status_board: None,
            trace_inspector: None,
            call_registry: None,
            ws_events: None,
        }
    }

    async fn init_endpoint(&mut self) -> Result<()> {
        info!(
            "[{}] Initializing SIP bot for account: {}@{}",
            self.account.username, self.account.username, self.account.domain
        );

        // Ensure recorders directory exists
        if let Some(recorders_dir) = &self.global_config.recorders {
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
        }

        let transport_layer = TransportLayer::new(self.transport_token.clone());
        // Account-level transport (serve mode): udp | tcp | ws | wss.
        // Falls back to global ws_url (legacy) then udp.
        let transport_kind = self
            .account
            .transport
            .as_deref()
            .and_then(crate::config::TransportKind::parse);
        let uses_ws = match transport_kind {
            Some(crate::config::TransportKind::Ws)
            | Some(crate::config::TransportKind::Wss) => true,
            Some(_) => false,
            None => self.global_config.ws_url.is_some(),
        };

        if uses_ws {
            // ── WebSocket transport mode ──
            let ws_url = self
                .account
                .transport_ws_url
                .clone()
                .or_else(|| self.global_config.ws_url.clone())
                .context("ws/wss transport requires a websocket url")?;
            let (transport, host, port, path) = parse_ws_url(&ws_url)?;

            transport_layer.set_ws_path(&path);

            let host_parsed: Host = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                Host::IpAddr(ip)
            } else {
                Host::Domain(host.clone().into())
            };

            let target = SipAddr {
                r#type: Some(transport),
                addr: HostWithPort {
                    host: host_parsed,
                    port: Some(port.into()),
                },
            };

            // Pre-establish WS connection so its local addr appears in get_addrs
            // ensuring Via/Contact carry WS transport type.
            let (_conn, _addr) = transport_layer.lookup(&target, None).await?;
            info!(
                "[{}] WebSocket connected to {}",
                self.account.username, ws_url
            );

            info!(
                "[{}] WebSocket connected to {}",
                self.account.username, ws_url
            );

            // Set proxy so registration & invites use WebSocket transport
            let transport_param = if transport == Transport::Wss {
                "wss"
            } else {
                "ws"
            };
            self.account.proxy = Some(format!("{}:{};transport={}", host, port, transport_param));

            // WS transport implies WebRTC mode
            self.account.webrtc_enabled = Some(true);
        } else if transport_kind == Some(crate::config::TransportKind::Tcp) {
            // ── TCP listener transport mode ──
            let addr_str = self
                .account
                .transport_addr
                .clone()
                .or_else(|| self.global_config.addr.clone())
                .unwrap_or_else(|| "0.0.0.0:35060".to_string());
            let addr: std::net::SocketAddr = addr_str.parse().context("Invalid bind address")?;
            let tcp_listener = TcpListenerConnection::new(addr, None).await?;
            info!(
                "[{}] Listening (TCP) on {}",
                self.account.username,
                tcp_listener.get_addr()
            );
            transport_layer.add_transport(SipConnection::TcpListener(tcp_listener));
        } else {
            // ── UDP transport mode (original) ──
            let addr_str = self
                .account
                .transport_addr
                .clone()
                .or_else(|| self.global_config.addr.clone())
                .unwrap_or_else(|| "0.0.0.0:35060".to_string());
            let addr: std::net::SocketAddr = addr_str.parse().context("Invalid bind address")?;

            let mut udp_conn =
                UdpConnection::create_connection(addr, None, Some(self.transport_token.clone()))
                    .await?;
            if let Some(ip) = &self.global_config.external_ip {
                if let Ok(sock) = udp_conn.get_addr().get_socketaddr() {
                    let external: std::net::SocketAddr = format!("{}:{}", ip, sock.port())
                        .parse()
                        .context("Invalid external address")?;
                    udp_conn.external = Some(SipAddr {
                        r#type: Some(Transport::Udp),
                        addr: external.into(),
                    });
                }
            }
            let local_addr = udp_conn.get_addr();
            info!("[{}] Listening on {}", self.account.username, local_addr);

            transport_layer.add_transport(udp_conn.into());

            // Store local addr for WebRTC contact later
        }

        let mut builder = EndpointBuilder::new();
        builder
            .with_user_agent(&format!("SipBot/{}", env!("CARGO_PKG_VERSION")))
            .with_transport_layer(transport_layer)
            .with_cancel_token(self.transport_token.clone());
        if let Some(inspector) = self.trace_inspector.take() {
            builder.with_inspector(inspector);
        }
        let endpoint = builder.build();

        let endpoint = Arc::new(endpoint);
        self.endpoint = Some(endpoint.clone());

        let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
        self.dialog_layer = Some(dialog_layer);

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
        self.registration = Some(Registration::new(endpoint.inner.clone(), credential));

        // Pin a stable registration Contact (username@advertised-ip:port).
        // Otherwise rsipstack rebuilds the Contact per attempt from the 401
        // response's received/rport (loopback source when registering via a
        // local proxy) or the Via host, so the Contact flaps between the
        // loopback and external addresses and the registrar accumulates two
        // AOR entries — incoming calls then fork to both contacts.
        {
            let username = self.account.username.clone();
            let addrs = endpoint.inner.transport_layer.get_addrs();
            if let Some(first_addr) = addrs.first() {
                if let Ok(sock) = first_addr.get_socketaddr() {
                    let host = self
                        .global_config
                        .external_ip
                        .clone()
                        .unwrap_or_else(|| sock.ip().to_string());
                    let contact_str = format!("sip:{}@{}:{}", username, host, sock.port());
                    if let Ok(contact_uri) = rsipstack::rsip::Uri::try_from(contact_str.as_str()) {
                        if let Some(ref mut reg) = self.registration {
                            reg.contact = Some(rsipstack::rsip::typed::Contact {
                                display_name: None,
                                uri: contact_uri,
                                params: vec![],
                            });
                            info!(
                                "[{}] Registration contact pinned: {}",
                                username, contact_str
                            );
                        }
                    }
                }
            }
        }

        // Add +sip.ice Contact param (RFC 5656) when WebRTC is enabled over UDP.
        // For WS transport the server auto-detects WebRTC from the Via/Contact transport.
        if !uses_ws && self.account.webrtc_enabled.unwrap_or(false) {
            if let Some(ref mut reg) = self.registration {
                let username = self.account.username.clone();
                let addrs = endpoint.inner.transport_layer.get_addrs();
                if let Some(first_addr) = addrs.first() {
                    if let Ok(sock) = first_addr.get_socketaddr() {
                        let local_str = format!("sip:{}@{}:{}", username, sock.ip(), sock.port());
                        if let Ok(contact_uri) = rsipstack::rsip::Uri::try_from(local_str.as_str())
                        {
                            use rsipstack::rsip::Param;
                            use rsipstack::rsip::uri::OtherParam;
                            reg.contact = Some(rsipstack::rsip::typed::Contact {
                                display_name: None,
                                uri: contact_uri,
                                params: vec![Param::Other(OtherParam::new("+sip.ice"), None)],
                            });
                            info!(
                                "[{}] WebRTC enabled, set +sip.ice in registration contact",
                                username
                            );
                        }
                    }
                }
            }
        }

        // Start serving
        let endpoint_inner = endpoint.inner.clone();
        tokio::spawn(async move {
            if let Err(e) = endpoint_inner.serve().await {
                error!("Endpoint serve error: {:?}", e);
            }
        });

        Ok(())
    }

    fn get_recording_path(&self, call_id: &str) -> Option<PathBuf> {
        let now = Local::now().format("%Y%m%d%H%M%S");
        // Sanitize call_id to be safe for filename
        let safe_call_id = call_id.replace(|c: char| !c.is_alphanumeric(), "_");
        // Honor an explicit per-account recording path (e.g. `wait --record out.wav`).
        // The value is used as a base name; timestamp + call id are appended so that
        // multiple incoming calls do not overwrite each other.
        if let Some(rec) = self.account.record.as_deref() {
            let path = Path::new(rec);
            let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
            let file_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("record.wav");
            let stem = file_name.strip_suffix(".wav").unwrap_or(file_name);
            let filename = format!("{}_{}_{}.wav", stem, now, safe_call_id);
            return Some(
                parent
                    .map(|p| p.join(&filename))
                    .unwrap_or_else(|| PathBuf::from(filename)),
            );
        }
        let dir = self.global_config.recorders.as_deref()?;
        let filename = format!("{}_{}.wav", now, safe_call_id);
        Some(Path::new(dir).join(filename))
    }

    pub async fn run_wait(&mut self) -> Result<()> {
        self.is_wait = true;
        self.init_endpoint().await?;

        // Register
        if self.account.register.unwrap_or(false) {
            self.start_registration_loop().await?;
        }

        let monitor_stats = self.stats.clone();
        let monitor_token = self.cancel_token.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut last_was_zero = false;
            loop {
                tokio::select! {
                    _ = monitor_token.cancelled() => break,
                    _ = interval.tick() => {}
                }
                let current = monitor_stats.current();
                if current > 0 {
                    monitor_stats.print_summary();
                    last_was_zero = false;
                } else if !last_was_zero {
                    monitor_stats.print_summary();
                    last_was_zero = true;
                }
            }
        });

        // Listen for incoming calls
        self.listen_loop().await?;

        info!(
            "[{}] Listen loop exited, giving tasks 200ms to start cleanup",
            self.account.username
        );
        // Give spawned call tasks time to send BYE before closing transport
        tokio::time::sleep(Duration::from_millis(200)).await;

        if self.stats.current() > 0 {
            info!(
                "[{}] Waiting for {} active calls to finish cleanup...",
                self.account.username,
                self.stats.current()
            );
            let mut wait_count = 0;
            while self.stats.current() > 0 && wait_count < 50 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                wait_count += 1;
                if wait_count % 10 == 0 {
                    info!(
                        "[{}] Still waiting... {} calls active",
                        self.account.username,
                        self.stats.current()
                    );
                }
            }
            if self.stats.current() > 0 {
                warn!(
                    "[{}] {} calls still active after waiting 5 seconds",
                    self.account.username,
                    self.stats.current()
                );
            } else {
                info!(
                    "[{}] All active calls finished cleanup.",
                    self.account.username
                );
            }
        } else {
            info!(
                "[{}] No active calls at listen loop exit",
                self.account.username
            );
        }
        info!("[{}] Cancelling transport token", self.account.username);
        self.transport_token.cancel();

        Ok(())
    }

    pub async fn run_call(&mut self, total: u32, cps: u32) -> Result<()> {
        self.stats.add_total_planned(total);
        self.stats.set_total_planned(total);
        self.init_endpoint().await?;

        // Register
        if self.account.register.unwrap_or(false) {
            self.start_registration_loop().await?;
        }

        if let Some(target) = &self.account.target {
            let monitor_stats = self.stats.clone();
            let monitor_token = self.cancel_token.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! {
                        _ = monitor_token.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    monitor_stats.print_progress();
                }
            });

            let runner = CallRunner {
                dialog_layer: self
                    .dialog_layer
                    .as_ref()
                    .context("DialogLayer not initialized")?
                    .clone(),
                account: self.account.clone(),
                global_config: self.global_config.clone(),
                stats: self.stats.clone(),
                cancel_token: self.cancel_token.clone(),
                current_media_session: self.current_media_session.clone(),
            };

            let mut handles = vec![];
            let delay = if cps > 0 {
                Duration::from_secs_f64(1.0 / cps as f64)
            } else {
                Duration::from_millis(1)
            };

            for i in 0..total {
                let runner = runner.clone();
                let target = target.clone();

                let handle = tokio::spawn(async move {
                    if let Err(e) = runner.make_call(target, i).await {
                        debug!("Call {} failed: {:?}", i, e);
                    }
                    Ok::<(), anyhow::Error>(())
                });
                handles.push(handle);

                if i < total - 1 && cps > 0 {
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.cancel_token.cancelled() => {
                            info!("[{}] Cancellation requested, checking existing calls...", self.account.username);
                            break;
                        }
                    }
                }
            }

            let calls_future = futures::future::join_all(handles);
            tokio::pin!(calls_future);

            tokio::select! {
                _ = self.listen_loop() => {
                    if self.cancel_token.is_cancelled() {
                        info!("[{}] Listen loop stopped due to cancellation, waiting for active calls to finish cleanup...", self.account.username);
                    }
                    calls_future.await;
                }
                _ = &mut calls_future => {
                    println!(); // New line after progress
                    info!("[{}] All calls finished.", self.account.username);
                }
            }
            self.transport_token.cancel();
        } else {
            warn!(
                "[{}] No target configured for outbound call",
                self.account.username
            );
        }

        Ok(())
    }

    pub async fn send_dtmf(&self, digit: char) {
        let session = self.current_media_session.lock().await;
        if let Some(ref media) = *session {
            let _: Result<()> = media.send_dtmf(digit).await;
        }
    }

    pub async fn run_options(&mut self, target_override: Option<String>) -> Result<()> {
        self.stats.add_total_planned(1);
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
        self.transport_token.cancel();
        Ok(())
    }

    pub async fn run_info(&mut self, target_override: Option<String>) -> Result<()> {
        self.stats.add_total_planned(1);
        self.init_endpoint().await?;
        let target = target_override.or(self.account.target.clone());
        if let Some(target) = target {
            info!("[{}] Sending INFO to {}", self.account.username, target);
            self.send_standalone_request(Method::Info, &target).await?;
        } else {
            warn!("[{}] No target configured for INFO", self.account.username);
        }
        self.transport_token.cancel();
        Ok(())
    }

    async fn send_standalone_request(&self, method: Method, target_uri: &str) -> Result<()> {
        let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;

        let req_uri = Uri::try_from(target_uri)?;
        let addrs = endpoint.get_addrs();
        let local_sip_addr = addrs.first().context("No local address found")?;
        let local_socket = local_sip_addr.get_socketaddr()?;
        let local_ip = local_socket.ip();

        let via = endpoint.inner.get_via(None, None)?;

        let from_str = if let Some(from_val) = &self.account.from_user {
            if from_val.starts_with("sip:") {
                let mut uri: rsipstack::rsip::Uri = from_val.as_str().try_into()?;
                if uri.auth.as_ref().map_or(true, |a| a.user.is_empty()) {
                    uri.auth = Some(rsipstack::rsip::Auth {
                        user: self.account.username.clone(),
                        password: None,
                    });
                }
                format!("{};tag={}", uri, generate_random_string())
            } else {
                format!(
                    "sip:{}@{};tag={}",
                    from_val,
                    self.account.domain,
                    generate_random_string()
                )
            }
        } else {
            format!(
                "sip:{}@{};tag={}",
                self.account.username,
                self.account.domain,
                generate_random_string()
            )
        };
        let untyped_from = rsipstack::rsip::From::try_from(from_str.as_str())?;
        let from = untyped_from.typed()?;

        let to_str = target_uri;
        let untyped_to = rsipstack::rsip::To::try_from(to_str)?;
        let to = untyped_to.typed()?;

        let call_id_str = format!("{}@{}", generate_random_string(), local_ip);
        let call_id = rsipstack::rsip::CallId::try_from(call_id_str.as_str())?;

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
                rsipstack::rsip::SipMessage::Response(res) => {
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
                        self.stats.add_status(res.status_code().clone().into());
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
        let proxy = self.account.proxy.clone();
        let verbose = self.verbose;
        let is_wait = self.is_wait;
        let cancel_token = self.cancel_token.clone();
        let status_board = self.status_board.clone();
        let transport_kind = self
            .account
            .transport
            .as_deref()
            .and_then(crate::config::TransportKind::parse)
            .unwrap_or_default()
            .as_str()
            .to_string();

        let report = {
            let username = username.clone();
            let domain = domain.clone();
            move |registered: bool, expires: Option<u64>, last_error: Option<String>| {
                if let Some(board) = &status_board {
                    board.set(crate::status::RegistrationStatus {
                        username: username.clone(),
                        domain: domain.clone(),
                        registered,
                        expires,
                        transport: Some(transport_kind.clone()),
                        last_error,
                        updated_at: Some(crate::status::now_epoch_secs()),
                    });
                }
            }
        };

        tokio::spawn(async move {
            info!("[{}] Starting registration loop", username);
            let target = proxy.unwrap_or(domain);
            let server_uri = match Uri::try_from(normalize_sip_addr(&target).as_str()) {
                Ok(u) => u,
                Err(e) => {
                    error!("[{}] Invalid domain URI: {}", username, e);
                    return;
                }
            };

            loop {
                if cancel_token.is_cancelled() {
                    break;
                }
                if is_wait && !verbose {
                    println!("[{}] Registering...", username);
                } else {
                    info!("[{}] Registering...", username);
                }
                // Default expire 600s
                let register_result = tokio::select! {
                    r = registration.register(server_uri.clone(), Some(600)) => r,
                    _ = cancel_token.cancelled() => break,
                };
                match register_result {
                    Ok(response) => {
                        if *response.status_code() == StatusCode::OK {
                            let expires = registration.expires();
                            report(true, Some(expires as u64), None);
                            if is_wait && !verbose {
                                println!(
                                    "[{}] Registered successfully, expires in {}s",
                                    username, expires
                                );
                            } else {
                                info!(
                                    "[{}] Registered successfully, expires in {}s",
                                    username, expires
                                );
                            }
                            // Refresh before expiration (e.g., 5 seconds before)
                            let sleep_time = if expires > 5 { expires - 5 } else { expires };
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(sleep_time as u64)) => {}
                                _ = cancel_token.cancelled() => break,
                            }
                        } else {
                            report(
                                false,
                                None,
                                Some(format!("registration failed: {}", response.status_code())),
                            );
                            warn!(
                                "[{}] Registration failed: {}",
                                username,
                                response.status_code()
                            );
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                                _ = cancel_token.cancelled() => break,
                            }
                        }
                    }
                    Err(e) => {
                        report(false, None, Some(format!("registration error: {:?}", e)));
                        error!("[{}] Registration error: {:?}", username, e);
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                            _ = cancel_token.cancelled() => break,
                        }
                    }
                }
            }
            info!("[{}] Registration loop stopped", username);
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

        loop {
            tokio::select! {
                transaction = incoming.recv() => {
                    match transaction {
                        Some(transaction) => {
                            if let Err(e) = self.handle_incoming_transaction(transaction).await {
                                error!(
                                    "[{}] Error handling incoming transaction: {:?}",
                                    self.account.username, e
                                );
                            }
                        }
                        None => break,
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    info!("[{}] Listen loop stopped due to cancellation.", self.account.username);
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handle REFER request (RFC 3515)
    ///
    /// For testing transfer scenarios, this implementation:
    /// 1. Accepts the REFER with 202 Accepted (default)
    /// 2. Or rejects with configured status code (e.g., 405 for 3PCC fallback testing)
    /// 3. Sends NOTIFY with 100 Trying
    /// 4. Sends NOTIFY with 200 OK (simulated success)
    async fn handle_refer(&self, mut transaction: Transaction) -> Result<()> {
        let refer_to = transaction.original.headers.iter().find_map(|h| {
            if let Header::Other(name, value) = h {
                if name.to_string().eq_ignore_ascii_case("refer-to") {
                    return Some(value.to_string());
                }
            }
            None
        });

        info!("[{}] REFER target: {:?}", self.account.username, refer_to);

        // Check if we should reject REFER (for testing 3PCC fallback)
        if let Some(reject_code) = self.account.refer_reject {
            let status_code =
                StatusCode::try_from(reject_code).unwrap_or(StatusCode::MethodNotAllowed);
            info!(
                "[{}] Rejecting REFER with {}",
                self.account.username, status_code
            );
            transaction.reply(status_code).await?;
            return Ok(());
        }

        // Reply 202 Accepted to REFER
        transaction.reply(StatusCode::Accepted).await?;
        info!("[{}] Sent 202 Accepted for REFER", self.account.username);

        // Get dialog for sending NOTIFY
        let dialog_layer = self
            .dialog_layer
            .as_ref()
            .context("DialogLayer not initialized")?;

        let dialog_id = DialogId::try_from((&transaction.original, TransactionRole::Server))?;

        // Spawn task to send NOTIFY sequence
        let dialog_layer_clone = dialog_layer.clone();
        let username = self.account.username.clone();
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            // Wait a bit for REFER to be processed
            tokio::time::sleep(Duration::from_millis(100)).await;

            if let Some(dialog) = dialog_layer_clone.get_dialog(&dialog_id) {
                if let Dialog::Invite(server_dialog) = dialog {
                    // Send NOTIFY 100 Trying
                    if let Err(e) = server_dialog
                        .notify_refer(StatusCode::Trying, "active")
                        .await
                    {
                        warn!("[{}] Failed to send NOTIFY 100: {:?}", username, e);
                        return;
                    }
                    info!("[{}] Sent NOTIFY 100 Trying", username);

                    // Simulate some delay
                    tokio::time::sleep(Duration::from_millis(200)).await;

                    // Check if cancelled
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    // Send NOTIFY 200 OK (success)
                    if let Err(e) = server_dialog
                        .notify_refer(StatusCode::OK, "terminated;reason=noresource")
                        .await
                    {
                        warn!("[{}] Failed to send NOTIFY 200: {:?}", username, e);
                        return;
                    }
                    info!("[{}] Sent NOTIFY 200 OK", username);
                }
            } else {
                warn!("[{}] Dialog not found for NOTIFY", username);
            }
        });

        Ok(())
    }

    /// Check if an INVITE is a re-INVITE within an existing dialog
    /// by looking for a `tag` parameter in the To header.
    fn is_reinvite(transaction: &Transaction) -> bool {
        transaction
            .original
            .to_header()
            .ok()
            .is_some_and(|to| to.value().to_string().contains("tag="))
    }

    /// Rewrite an incoming request's Contact whose host is unspecified (e.g. `0.0.0.0`)
    /// to the top Via's `received` IP and `rport` port (RFC 3581), falling back to the
    /// Via sent-by address and finally to the default SIP port 5060.
    fn fix_unspecified_contact(request: &mut rsipstack::rsip::Request) {
        let needs_fix = request
            .typed_contact_headers()
            .ok()
            .and_then(|cs| cs.first().cloned())
            .is_some_and(|c| {
                matches!(&c.uri.host_with_port.host, Host::IpAddr(ip) if ip.is_unspecified())
            });
        if !needs_fix {
            return;
        }

        let via = request
            .top_via_header()
            .ok()
            .and_then(|v| v.typed().ok());
        let Some(via) = via else {
            return;
        };
        let ip = via
            .received()
            .and_then(|r| r.ok())
            .or_else(|| match via.sent_by().host {
                Host::IpAddr(ip) if !ip.is_unspecified() => Some(ip),
                _ => None,
            });
        let Some(ip) = ip else {
            warn!("Contact host is unspecified but no usable address in Via, leaving as-is");
            return;
        };
        let port = via
            .rport()
            .flatten()
            .or_else(|| via.sent_by().port.map(|p| p.0))
            .unwrap_or(5060);

        if let Ok(contact) = request.contact_header_mut()
            && let Ok(mut typed) = rsipstack::rsip::typed::Contact::parse(contact.value())
        {
            let old = typed.uri.host_with_port.to_string();
            typed.uri.host_with_port = HostWithPort {
                host: Host::IpAddr(ip),
                port: Some(rsipstack::rsip::Port(port)),
            };
            let new_value = typed.to_string();
            contact.replace(new_value);
            info!("Fixed unspecified Contact host {} -> {}:{}", old, ip, port);
        }
    }

    /// Handle a re-INVITE (e.g., for hold/resume scenarios).
    ///
    /// Parses the offer for direction attribute:
    /// - `a=sendonly` / `a=inactive` → hold (mute echo/audio)
    /// - `a=sendrecv` → resume (unmute echo/audio)
    ///
    /// Renegotiates SDP and replies 200 OK with the answer SDP body.
    async fn handle_reinvite(&self, mut transaction: Transaction) -> Result<()> {
        let offer_body = String::from_utf8_lossy(transaction.original.body()).to_string();
        let offer_lower = offer_body.to_lowercase();

        let is_hold = offer_lower.contains("a=sendonly") || offer_lower.contains("a=inactive");

        info!(
            "[{}] Received re-INVITE: {}",
            self.account.username,
            if is_hold {
                "HOLD (a=sendonly/inactive)"
            } else {
                "RESUME (a=sendrecv)"
            }
        );

        // Renegotiate SDP and get answer, toggle audio_silent
        let answer_sdp = {
            let guard = self.current_media_session.lock().await;
            match guard.as_ref() {
                Some(media) => {
                    if is_hold {
                        media.set_audio_silent(true).await;
                    } else {
                        media.set_audio_silent(false).await;
                    }
                    if !offer_body.is_empty() {
                        match media.renegotiate(&offer_body).await {
                            Ok(answer) => Some(answer),
                            Err(e) => {
                                warn!(
                                    "[{}] SDP renegotiation failed: {:?}",
                                    self.account.username, e
                                );
                                None
                            }
                        }
                    } else {
                        None
                    }
                }
                None => {
                    warn!(
                        "[{}] No active media session for re-INVITE",
                        self.account.username
                    );
                    None
                }
            }
        };

        // Reply 200 OK with answer SDP body for proper re-INVITE negotiation
        if let Some(sdp) = answer_sdp {
            let headers = vec![Header::ContentType("application/sdp".into())];
            transaction
                .reply_with(StatusCode::OK, headers, Some(sdp.into_bytes()))
                .await?;
        } else {
            transaction.reply(StatusCode::OK).await?;
        }
        info!("[{}] Re-INVITE replied 200 OK", self.account.username);
        Ok(())
    }

    async fn handle_incoming_transaction(&self, mut transaction: Transaction) -> Result<()> {
        match transaction.original.method {
            Method::Invite => {
                Self::fix_unspecified_contact(&mut transaction.original);
                if Self::is_reinvite(&transaction) {
                    self.handle_reinvite(transaction).await?
                } else {
                    self.handle_invite(transaction).await?
                }
            }
            Method::Ack => info!("[{}] Received ACK", self.account.username),
            Method::Bye => {
                info!("[{}] Received BYE", self.account.username);
                let id = DialogId::try_from((&transaction.original, TransactionRole::Server))?;
                let dialog = self.dialog_layer.as_ref().and_then(|d| d.get_dialog(&id));
                if let Some(mut dlg) = dialog {
                    let _ = dlg.handle(&mut transaction).await?;
                } else {
                    transaction
                        .reply(rsipstack::rsip::StatusCode::CallTransactionDoesNotExist)
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
            Method::Update => {
                info!("[{}] Received UPDATE", self.account.username);
                transaction.reply(StatusCode::OK).await?;
            }
            Method::Refer => {
                info!("[{}] Received REFER", self.account.username);
                self.handle_refer(transaction).await?;
            }
            _ => info!(
                "[{}] Received other method: {:?}",
                self.account.username, transaction.original.method
            ),
        }
        Ok(())
    }

    async fn handle_invite(&self, mut transaction: Transaction) -> Result<()> {
        let call_id = transaction.original.call_id_header()?.value().to_string();
        let caller = transaction.original.from_header()?.uri()?.to_string();
        let callee = transaction.original.to_header()?.uri()?.to_string();

        info!(
            "[{}] Handling INVITE for {} (Call-ID: {}) from: {}",
            self.account.username, caller, call_id, callee
        );

        let endpoint = self.endpoint.as_ref().context("Endpoint not initialized")?;
        let addrs = endpoint.get_addrs();
        let local_sip_addr = addrs.first().context("No local address found")?;
        let local_socket = local_sip_addr.get_socketaddr()?;
        let local_ip = local_socket.ip();
        let local_port = local_socket.port();

        let recording_path = self.get_recording_path(&call_id);
        if let Some(path) = &recording_path {
            info!(
                "[{}] Recording will be saved to: {:?}",
                self.account.username, path
            );
        }

        let dialog_layer = self
            .dialog_layer
            .as_ref()
            .context("DialogLayer not initialized")?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let username = self.account.username.clone();
        let invite_received_at = std::time::Instant::now();

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
        let stats_clone = self.stats.clone();
        let cancel_token = self.cancel_token.clone();
        self.stats.add_total_planned(1);
        self.stats.inc_current();
        let username_log = self.account.username.clone();
        let bot_media_session = self.current_media_session.clone();
        let call_registry = self.call_registry.clone();
        let ws_events = self.ws_events.clone();
        tokio::spawn(async move {
            info!("[{}] Call task started", username_log);
            let mut _call_guard = CallGuard {
                stats: stats_clone.clone(),
                start_time: std::time::Instant::now(),
                media_session: None,
            };

            // Spawn transaction handler
            let mut server_dialog_handler = server_dialog_clone.clone();
            tokio::spawn(async move {
                if let Err(e) = server_dialog_handler.handle(&mut transaction).await {
                    error!("Transaction handler error: {:?}", e);
                }
            });

            let shared_media_session: Arc<Mutex<Option<MediaSession>>> = Arc::new(Mutex::new(None));
            let shared_media_monitor = shared_media_session.clone();

            // Monitor loop
            let call_token = CancellationToken::new();
            let call_token_for_monitor = call_token.clone();
            let call_token_for_logic = call_token.clone();
            let call_token_for_join = call_token.clone();

            let username_monitor = username.clone();
            let monitor_future = async move {
                loop {
                    tokio::select! {
                        _ = call_token_for_monitor.cancelled() => {
                            return;
                        }
                        state = rx.recv() => {
                            let Some(state) = state else {
                                return;
                            };

                            info!(%state, "[{}] Dialog state changed", username_monitor);
                            match state {
                                DialogState::Early(_, _) => {
                                    info!("[{}] Call is ringing", username_monitor);
                                }
                                DialogState::Confirmed(_, _) => {
                                    let codec = {
                                        let m = shared_media_monitor.lock().await;
                                        m.as_ref()
                                            .map(|s| s.get_negotiated_codec())
                                            .unwrap_or_else(|| "Unknown".to_string())
                                    };
                                    info!(
                                        "[{}] Call is confirmed (Negotiated Codec: {})",
                                        username_monitor, codec
                                    );
                                }
                                DialogState::Updated(_, _, tx_handle) => {
                                    info!("[{}] Call is updated", username_monitor);
                                    tx_handle.reply(rsipstack::rsip::StatusCode::OK).await.ok();
                                }
                                DialogState::Options(_, _, tx_handle) => {
                                    info!("[{}] Call is options", username_monitor);
                                    tx_handle.reply(rsipstack::rsip::StatusCode::OK).await.ok();
                                }
                                DialogState::Terminated(..) => {
                                    info!("[{}] Call terminated remotely", username_monitor);
                                    call_token_for_monitor.cancel();
                                    return;
                                }
                                _ => {
                                    info!("[{}] Dialog state changed: {}", username_monitor, state);
                                }
                            }
                        }
                    }
                }
            };
            // Call logic
            let stats_for_decrement = stats_clone.clone();
            let call_id_str = call_id.clone();
            let caller_user = caller_user_part(&caller);
            let call_logic = async move {
                info!("[{}] Call logic started", account.username);

                // ── serve mode: per-call stats + call record + DTMF observer ──
                let is_serve = call_registry.is_some();
                let call_stats: Arc<CallStats> = if is_serve {
                    Arc::new(CallStats::new())
                } else {
                    stats_clone.clone()
                };
                let call_record = call_registry
                    .as_ref()
                    .map(|reg| reg.get_or_create(&call_id_str));
                if let Some(rec) = &call_record {
                    let mut r = rec.lock().unwrap();
                    r.direction = Some(crate::web::state::CallDirection::Inbound);
                    r.caller = caller.clone();
                    r.callee = callee.clone();
                    r.account = account.username.clone();
                    r.strategy = account.strategy.clone();
                    r.stats = Some(call_stats.clone());
                    if let Some(p) = &recording_path {
                        r.recording = Some(p.display().to_string());
                    }
                }
                let dtmf_observer: Arc<CallDtmfObserver> = Arc::new(CallDtmfObserver {
                    record: call_record.clone(),
                    events: ws_events.clone(),
                    call_id: call_id_str.clone(),
                });
                // UI hangup control
                let control_token = tokio_util::sync::CancellationToken::new();
                if let Some(rec) = &call_record {
                    rec.lock().unwrap().control = Some(control_token.clone());
                }
                // UI-triggered DTMF → current media session
                let (dtmf_req_tx, mut dtmf_req_rx) = tokio::sync::mpsc::unbounded_channel::<char>();
                if let Some(rec) = &call_record {
                    rec.lock().unwrap().dtmf_tx = Some(dtmf_req_tx);
                }
                {
                    let shared = shared_media_session.clone();
                    tokio::spawn(async move {
                        while let Some(digit) = dtmf_req_rx.recv().await {
                            let media = shared.lock().await.clone();
                            if let Some(m) = media {
                                let _: Result<(), anyhow::Error> = m.send_dtmf(digit).await;
                            }
                        }
                    });
                }

                let finish_record = |state: crate::web::state::CallState, reason: String| {
                    if let Some(rec) = &call_record {
                        let mut r = rec.lock().unwrap();
                        r.state = Some(state);
                        r.ended_at_ms = Some(crate::web::state::now_ms());
                        if r.end_reason.is_none() {
                            r.end_reason = Some(reason);
                        }
                    }
                };

                // ── caller match (serve strategy routing) ──
                if let Some(pattern) = &account.match_caller {
                    if !caller_matches(pattern, &caller_user) {
                        info!(
                            "[{}] Caller '{}' does not match '{}' — rejecting 403",
                            account.username, caller_user, pattern
                        );
                        let _ = server_dialog_clone.reject(Some(StatusCode::Forbidden), None);
                        stats_clone.add_status(403);
                        finish_record(
                            crate::web::state::CallState::Rejected,
                            "403 caller mismatch".to_string(),
                        );
                        return;
                    }
                }

                // Random rejection check
                if let Some(prob) = account.reject_prob {
                    let should_reject = {
                        let mut rng = rand::rng();
                        rng.random_range(1..=100) <= prob
                    };
                    if should_reject {
                        info!(
                            "[{}] Randomly rejecting call (prob: {}%)",
                            account.username, prob
                        );
                        let status_code = account
                            .hangup
                            .as_ref()
                            .filter(|h| h.code >= 300)
                            .map(|h| StatusCode::from(h.code))
                            .unwrap_or(StatusCode::TemporarilyUnavailable);

                        let code: u16 = status_code.clone().into();
                        if let Err(e) = server_dialog_clone.reject(Some(status_code), None) {
                            error!("Reject error: {:?}", e);
                        }
                        stats_clone.add_status(code);
                        finish_record(
                            crate::web::state::CallState::Rejected,
                            format!("random reject {}", code),
                        );
                        return;
                    }
                }

                // ── plain reject stage (no tone): respond code immediately ──
                if let Some(reject_cfg) = &account.reject {
                    if reject_cfg.tone.is_none() {
                        let sc = StatusCode::try_from(reject_cfg.code)
                            .unwrap_or(StatusCode::BusyHere);
                        info!(
                            "[{}] Reject stage: responding {}",
                            account.username, reject_cfg.code
                        );
                        if let Err(e) = server_dialog_clone.reject(Some(sc), None) {
                            error!("Reject error: {:?}", e);
                        }
                        stats_clone.add_status(reject_cfg.code);
                        finish_record(
                            crate::web::state::CallState::Rejected,
                            format!("reject {}", reject_cfg.code),
                        );
                        return;
                    }
                }

                // Stage 1: Ringing (Alerting)
                let mut media_session: Option<MediaSession> = None;
                let mut local_sdp: Option<String> = None;
                // All sessions created for this call ( jumped sessions keep RX alive )
                let mut live_sessions: Vec<MediaSession> = Vec::new();

                if !offer_body.is_empty() {
                    if let Ok(body_str) = std::str::from_utf8(&offer_body) {
                        let srtp_enabled = account.srtp_enabled.unwrap_or(false);
                        let webrtc_enabled = account.webrtc_enabled.unwrap_or(false);
                        let nack_enabled = account.nack_enabled.unwrap_or(false);
                        let jitter_buffer_enabled = account.jitter_buffer_enabled.unwrap_or(false);
                        match MediaSession::new(
                            body_str,
                            srtp_enabled,
                            webrtc_enabled,
                            nack_enabled,
                            jitter_buffer_enabled,
                            global_config.external_ip.clone(),
                            account.codecs.clone(),
                            call_stats.clone(),
                            account.audio_quality.clone(),
                            account.ts_jump_tolerance_ms,
                        )
                        .await
                        {
                            Ok((session, sdp, codec_name)) => {
                                session.dtmf_notify.set(dtmf_observer.clone());
                                {
                                    let mut m = shared_media_session.lock().await;
                                    *m = Some(session.clone());
                                }
                                {
                                    let mut m = bot_media_session.lock().await;
                                    *m = Some(session.clone());
                                }
                                media_session = Some(session.clone());
                                live_sessions.push(session.clone());
                                _call_guard.media_session = Some(session);
                                local_sdp = Some(sdp);
                                if let Some(rec) = &call_record {
                                    rec.lock().unwrap().codec = Some(codec_name.clone());
                                }
                                info!(
                                    "[{}] Media session established. Preferred Codec: {}",
                                    account.username, codec_name
                                );
                            }
                            Err(e) => {
                                error!(
                                    "[{}] Failed to create media session: {:?}",
                                    account.username, e
                                );
                                if let Err(e) = server_dialog_clone
                                    .reject(Some(StatusCode::TemporarilyUnavailable), None)
                                {
                                    error!("Reject error: {:?}", e);
                                }
                                stats_clone.add_status(StatusCode::TemporarilyUnavailable.into());
                                finish_record(
                                    crate::web::state::CallState::Failed,
                                    "media session failed".to_string(),
                                );
                                return;
                            }
                        }
                    }
                }

                // ── reject-with-tone stage: 183 + tone → reject code ──
                if let Some(reject_cfg) = &account.reject {
                    if reject_cfg.tone.is_some() {
                        info!(
                            "[{}] Reject stage: playing tone then {}",
                            account.username, reject_cfg.code
                        );
                        if let (Some(session), Some(sdp)) = (media_session.as_ref(), local_sdp.as_ref()) {
                            let headers = vec![Header::ContentType("application/sdp".into())];
                            if server_dialog_clone
                                .ringing(Some(headers), Some(sdp.clone().into_bytes()))
                                .is_ok()
                            {
                                stats_clone.add_status(183);
                            }
                            let file = reject_cfg.tone.clone().unwrap_or_default();
                            let play = session.play_file_once(
                                account.username.clone(),
                                std::path::Path::new(&file),
                                None,
                            );
                            tokio::pin!(play);
                            let _ = tokio::select! {
                                _ = &mut play => {}
                                _ = tokio::time::sleep(Duration::from_secs(
                                    reject_cfg.delay_secs.unwrap_or(u64::MAX).min(3600),
                                )) => {}
                                _ = cancel_token.cancelled() => {}
                                _ = call_token_for_logic.cancelled() => {}
                            };
                        }
                        let sc =
                            StatusCode::try_from(reject_cfg.code).unwrap_or(StatusCode::BusyHere);
                        if let Err(e) = server_dialog_clone.reject(Some(sc), None) {
                            error!("Reject error: {:?}", e);
                        }
                        stats_clone.add_status(reject_cfg.code);
                        for s in live_sessions {
                            s.stop().await;
                        }
                        finish_record(
                            crate::web::state::CallState::Rejected,
                            format!("reject {}", reject_cfg.code),
                        );
                        return;
                    }
                }

                // Stage 0: Early Media (183)
                if let Some(ref early) = account.early_media {
                    let play_local = early.local.unwrap_or(false);
                    let wav_file = early.wav_file.as_deref();

                    if play_local || wav_file.is_some() {
                        info!(
                            "[{}] Stage 0: Early Media (183) - {}",
                            account.username,
                            if play_local {
                                "Local Device".to_string()
                            } else {
                                format!("Playing {}", wav_file.unwrap_or(""))
                            }
                        );

                        // Send 183 with SDP if we have local SDP
                        if let Some(sdp) = local_sdp.as_ref() {
                            let headers = vec![Header::ContentType("application/sdp".into())];
                            if let Err(e) = server_dialog_clone
                                .ringing(Some(headers), Some(sdp.clone().into_bytes()))
                            {
                                error!("Early ring error: {:?}", e);
                            }
                            stats_clone.add_status(183);
                        }

                        if let Some(media) = &mut media_session {
                            if play_local {
                                #[cfg(feature = "local-device")]
                                {
                                    let res = tokio::select! {
                                        res = media.play_local_device(account.username.clone(), None, false, None) => res,
                                        _ = cancel_token.cancelled() => Ok(()),
                                        _ = call_token_for_logic.cancelled() => Ok(()),
                                    };
                                    if let Err(e) = res {
                                        error!(
                                            "Failed to play local device in early media: {:?}",
                                            e
                                        );
                                    }
                                }
                                #[cfg(not(feature = "local-device"))]
                                {
                                    error!("Local device support is disabled in this build");
                                }
                            } else if let Some(wav) = wav_file {
                                let res = tokio::select! {
                                    res = media.play_file(
                                        account.username.clone(),
                                        std::path::Path::new(wav),
                                        None,
                                        false,
                                    ) => res,
                                    _ = cancel_token.cancelled() => Ok(()),
                                    _ = call_token_for_logic.cancelled() => Ok(()),
                                };
                                if let Err(e) = res {
                                    error!("Failed to play early media file: {:?}", e);
                                }
                            }
                        }
                    }
                }

                // Stage 1: Ringing (Wait with optional Ringing/Ringback)
                if let Some(ref cfg) = account.ring {
                    let play_local = cfg.local.unwrap_or(false);
                    if play_local || cfg.ringback.is_some() {
                        let builtin = cfg.ringback.as_deref() == Some("");
                        info!(
                            "[{}] Stage 1: Ringing with media (183) - {}",
                            account.username,
                            if play_local {
                                "Local Device".to_string()
                            } else if builtin {
                                "Playing built-in ringing.wav".to_string()
                            } else {
                                format!("Playing {}", cfg.ringback.as_deref().unwrap_or(""))
                            }
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
                            stats_clone.add_status(183);
                        } else {
                            if let Err(e) = server_dialog_clone.ringing(None, None) {
                                error!("Ringing error: {:?}", e);
                                return;
                            }
                            stats_clone.add_status(180);
                        }

                        if let Some(media) = &mut media_session {
                            if play_local {
                                #[cfg(feature = "local-device")]
                                {
                                    let _ = tokio::select! {
                                        _ = media.play_local_device(
                                            account.username.clone(),
                                            None,
                                            true, // keep_alive=true so it doesn't stop
                                            cfg.duration_secs,
                                        ) => {},
                                        _ = cancel_token.cancelled() => {},
                                        _ = call_token_for_logic.cancelled() => {},
                                    };
                                }
                                #[cfg(not(feature = "local-device"))]
                                {
                                    error!("Local device support is disabled in this build");
                                    tokio::select! {
                                        _ = tokio::time::sleep(Duration::from_secs(cfg.duration_secs.unwrap_or(5))) => {}
                                        _ = cancel_token.cancelled() => {}
                                        _ = call_token_for_logic.cancelled() => {}
                                    }
                                }
                            } else if builtin {
                                // Play built-in ringing.wav to the end, then answer
                                let play = media.play_wav_bytes_once(
                                    account.username.clone(),
                                    RINGING_WAV,
                                    None,
                                );
                                tokio::pin!(play);
                                match cfg.duration_secs {
                                    Some(secs) => {
                                        let _ = tokio::select! {
                                            _ = &mut play => {},
                                            _ = tokio::time::sleep(Duration::from_secs(secs)) => {},
                                            _ = cancel_token.cancelled() => {},
                                            _ = call_token_for_logic.cancelled() => {},
                                        };
                                    }
                                    None => {
                                        let _ = tokio::select! {
                                            _ = &mut play => {},
                                            _ = cancel_token.cancelled() => {},
                                            _ = call_token_for_logic.cancelled() => {},
                                        };
                                    }
                                }
                            } else if let Some(wav) = cfg.ringback.as_ref() {
                                // Play file (once, to the end if no duration set)
                                let play = media.play_file_once(
                                    account.username.clone(),
                                    std::path::Path::new(wav),
                                    None,
                                );
                                tokio::pin!(play);
                                match cfg.duration_secs {
                                    Some(secs) => {
                                        let _ = tokio::select! {
                                            _ = &mut play => {},
                                            _ = tokio::time::sleep(Duration::from_secs(secs)) => {},
                                            _ = cancel_token.cancelled() => {},
                                            _ = call_token_for_logic.cancelled() => {},
                                        };
                                    }
                                    None => {
                                        let _ = tokio::select! {
                                            _ = &mut play => {},
                                            _ = cancel_token.cancelled() => {},
                                            _ = call_token_for_logic.cancelled() => {},
                                        };
                                    }
                                }
                            }
                        } else {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(cfg.duration_secs.unwrap_or(5))) => {}
                                _ = cancel_token.cancelled() => {}
                                _ = call_token_for_logic.cancelled() => {}
                            }
                        }
                    } else {
                        info!("[{}] Stage 1: Sending 180 Ringing", account.username);
                        if let Err(e) = server_dialog_clone.ringing(None, None) {
                            error!("Ringing error: {:?}", e);
                            return;
                        }
                        stats_clone.add_status(180);
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(cfg.duration_secs.unwrap_or(5))) => {}
                            _ = cancel_token.cancelled() => {}
                            _ = call_token_for_logic.cancelled() => {}
                        }
                    }
                }

                // Stage 2: Answer or Reject
                if cancel_token.is_cancelled() {
                    info!(
                        "[{}] Stage 2: Cancellation requested, rejecting call",
                        account.username
                    );
                    let _ = server_dialog_clone.reject(Some(StatusCode::BusyHere), None);
                    for s in live_sessions {
                        s.stop().await;
                    }
                    finish_record(
                        crate::web::state::CallState::Terminated,
                        "cancelled".to_string(),
                    );
                    return;
                }

                // ── sdp_jump: answer with a NEW session so the 200 OK SDP
                //    differs from the 183 SDP (new SSRC/codec/ts/seq) ──
                let sdp_jump_enabled = account.sdp_jump.unwrap_or(false);
                if sdp_jump_enabled && !offer_body.is_empty() {
                    if let Ok(body_str) = std::str::from_utf8(&offer_body) {
                        let jump_codecs = if account.jump_codecs.is_some() {
                            account.jump_codecs.clone()
                        } else {
                            account.codecs.clone()
                        };
                        match MediaSession::new(
                            body_str,
                            account.srtp_enabled.unwrap_or(false),
                            account.webrtc_enabled.unwrap_or(false),
                            account.nack_enabled.unwrap_or(false),
                            account.jitter_buffer_enabled.unwrap_or(false),
                            global_config.external_ip.clone(),
                            jump_codecs,
                            call_stats.clone(),
                            account.audio_quality.clone(),
                            account.ts_jump_tolerance_ms,
                        )
                        .await
                        {
                            Ok((session_b, sdp_b, codec_b)) => {
                                session_b.dtmf_notify.set(dtmf_observer.clone());
                                let old_info =
                                    media_session.as_ref().and_then(|s| s.local_stream_info());
                                let new_info = session_b.local_stream_info();
                                info!(
                                    "[{}] sdp_jump: answering with new session (codec {}) old_stream={:?} new_stream={:?}",
                                    account.username, codec_b, old_info, new_info
                                );
                                if let Some(rec) = &call_record {
                                    let mut r = rec.lock().unwrap();
                                    let t_ms =
                                        crate::web::state::now_ms().saturating_sub(r.started_at_ms);
                                    r.sdp_200 = Some(sdp_b.clone());
                                    r.codec = Some(codec_b);
                                    r.jump_events.push(crate::web::state::JumpEvent {
                                        t_ms,
                                        reason: "sdp_jump(200 OK new SDP)".to_string(),
                                        old_ssrc: old_info.map(|i| i.0),
                                        new_ssrc: new_info.map(|i| i.0),
                                    });
                                }
                                {
                                    let mut m = shared_media_session.lock().await;
                                    *m = Some(session_b.clone());
                                }
                                {
                                    let mut m = bot_media_session.lock().await;
                                    *m = Some(session_b.clone());
                                }
                                // Old session (183 early media) no longer needed;
                                // pre-answer only session A exists in live_sessions.
                                if let Some(old) = media_session.take() {
                                    old.stop().await;
                                    live_sessions.clear();
                                }
                                media_session = Some(session_b.clone());
                                live_sessions.push(session_b.clone());
                                _call_guard.media_session = Some(session_b);
                                local_sdp = Some(sdp_b);
                            }
                            Err(e) => {
                                warn!(
                                    "[{}] sdp_jump session failed, falling back: {:?}",
                                    account.username, e
                                );
                            }
                        }
                    }
                }

                // Always answer with configured or default media
                info!("[{}] Stage 2: Answering (200 OK)", account.username);
                let mut headers = vec![];
                let mut body = None;
                // Add SDP if we have local SDP
                if let Some(sdp) = local_sdp.as_ref() {
                    body = Some(sdp.clone().into_bytes());
                    headers.push(Header::ContentType("application/sdp".into()));
                }
                // Add custom headers
                if let Some(custom_headers) = &account.headers {
                    for header_str in custom_headers {
                        if let Some((name, value)) = header_str.split_once(':') {
                            let name = name.trim();
                            let value = value.trim();
                            headers.push(Header::Other(name.to_string(), value.into()));
                        }
                    }
                }

                if let Err(e) = server_dialog_clone.accept(Some(headers), body) {
                    error!("Accept error: {:?}", e);
                    return;
                }
                stats_clone.add_status(200);
                stats_clone.add_setup_latency(invite_received_at.elapsed());

                if media_session.is_none() {
                    warn!(
                        "[{}] No media session established (missing SDP?)",
                        account.username
                    );
                }

                let username_media = account.username.clone();
                let answer_config = account.answer.clone();
                let hangup_config = account.hangup.clone();
                let hangup_mode = hangup_config
                    .as_ref()
                    .map(|h| h.effective_mode())
                    .unwrap_or(crate::config::HangupMode::Playback);
                let keep_alive = !matches!(hangup_mode, crate::config::HangupMode::Playback);

                // ── announce stage ("XX来电") + in-call stream jump ──
                let mut bridged = false;
                if let Some(announce_cfg) = &account.announce {
                    if let Some(session) = media_session.clone() {
                        let file = announce_cfg.file.replace("{{caller}}", &caller_user);
                        info!(
                            "[{}] Announce stage: playing {}",
                            account.username, file
                        );
                        let play = session.play_file_once(
                            username_media.clone(),
                            std::path::Path::new(&file),
                            None,
                        );
                        tokio::pin!(play);
                        let _ = tokio::select! {
                            _ = &mut play => {}
                            _ = tokio::time::sleep(Duration::from_secs(120)) => {}
                            _ = cancel_token.cancelled() => {}
                            _ = call_token_for_logic.cancelled() => {}
                        };

                        if announce_cfg.jump_after
                            && !call_token_for_logic.is_cancelled()
                            && !offer_body.is_empty()
                        {
                            if let Ok(body_str) = std::str::from_utf8(&offer_body) {
                                let jump_codecs = announce_cfg
                                    .jump_codec
                                    .as_ref()
                                    .map(|c| vec![c.clone()])
                                    .or_else(|| account.jump_codecs.clone())
                                    .or_else(|| account.codecs.clone());
                                match MediaSession::new(
                                    body_str,
                                    account.srtp_enabled.unwrap_or(false),
                                    account.webrtc_enabled.unwrap_or(false),
                                    account.nack_enabled.unwrap_or(false),
                                    account.jitter_buffer_enabled.unwrap_or(false),
                                    global_config.external_ip.clone(),
                                    jump_codecs,
                                    call_stats.clone(),
                                    account.audio_quality.clone(),
                                    account.ts_jump_tolerance_ms,
                                )
                                .await
                                {
                                    Ok((jump_session, _sdp_j, codec_j)) => {
                                        jump_session.dtmf_notify.set(dtmf_observer.clone());
                                        let old_info =
                                            session.local_stream_info();
                                        let new_info =
                                            jump_session.local_stream_info();
                                        info!(
                                            "[{}] Stream JUMP (no re-INVITE): old={:?} new={:?} codec={}",
                                            account.username, old_info, new_info, codec_j
                                        );
                                        // Keep receiving on the old session,
                                        // transmit via the jump session.
                                        session
                                            .start_rx_bridge_to(
                                                &jump_session,
                                                username_media.clone(),
                                            )
                                            .await;
                                        {
                                            let mut m = shared_media_session.lock().await;
                                            *m = Some(jump_session.clone());
                                        }
                                        {
                                            let mut m = bot_media_session.lock().await;
                                            *m = Some(jump_session.clone());
                                        }
                                        if let Some(rec) = &call_record {
                                            let mut r = rec.lock().unwrap();
                                            let t_ms = crate::web::state::now_ms()
                                                .saturating_sub(r.started_at_ms);
                                            r.jump_events.push(crate::web::state::JumpEvent {
                                                t_ms,
                                                reason: "announce jump_after (in-call)"
                                                    .to_string(),
                                                old_ssrc: old_info.map(|i| i.0),
                                                new_ssrc: new_info.map(|i| i.0),
                                            });
                                        }
                                        media_session = Some(jump_session.clone());
                                        live_sessions.push(jump_session.clone());
                                        _call_guard.media_session = Some(jump_session);
                                        bridged = true;
                                    }
                                    Err(e) => warn!(
                                        "[{}] jump session failed, staying put: {:?}",
                                        account.username, e
                                    ),
                                }
                            }
                        }
                    }
                }

                let park_token = call_token_for_logic.clone();
                let username_media = account.username.clone();
                let media_future = async move {                    if let Some(media) = media_session {
                        if let Some(cfg) = answer_config {
                            match cfg {
                                AnswerConfig::Echo => {
                                    if bridged {
                                        // The RX bridge already loops remote audio
                                        // back through the jump session.
                                        info!(
                                            "[{}] Stage 2: Echo via jump bridge",
                                            username_media
                                        );
                                        if let Some(path) = recording_path.as_deref() {
                                            media
                                                .init_recording(&username_media, path)
                                                .await;
                                        }
                                        park_token.cancelled().await;
                                        Ok(())
                                    } else {
                                        info!("[{}] Stage 2: Starting Echo", username_media);
                                        media
                                            .start_echo(
                                                username_media.clone(),
                                                recording_path.as_deref(),
                                            )
                                            .await
                                    }
                                }
                                AnswerConfig::Play { wav_file } => {
                                    info!("[{}] Stage 2: Playing {}", username_media, wav_file);
                                    media
                                        .play_file(
                                            username_media.clone(),
                                            std::path::Path::new(&wav_file),
                                            recording_path.as_deref(),
                                            keep_alive,
                                        )
                                        .await
                                }
                                AnswerConfig::Local => {
                                    info!("[{}] Stage 2: Starting Local Audio", username_media);
                                    #[cfg(feature = "local-device")]
                                    {
                                        if let Err(e) = media
                                            .play_local_device(
                                                username_media.clone(),
                                                recording_path.as_deref(),
                                                keep_alive,
                                                None, // No timeout for answer phase
                                            )
                                            .await
                                        {
                                            warn!(
                                                "[{}] Failed to start local audio: {:?}, falling back to default answer",
                                                username_media, e
                                            );
                                            media
                                                .play_wav_bytes(
                                                    username_media,
                                                    ANSWER_WAV,
                                                    recording_path.as_deref(),
                                                    keep_alive,
                                                )
                                                .await
                                        } else {
                                            Ok(())
                                        }
                                    }
                                    #[cfg(not(feature = "local-device"))]
                                    {
                                        warn!(
                                            "[{}] Local device support is disabled in this build, falling back to default answer",
                                            username_media
                                        );
                                        media
                                            .play_wav_bytes(
                                                username_media,
                                                ANSWER_WAV,
                                                recording_path.as_deref(),
                                                keep_alive,
                                            )
                                            .await
                                    }
                                }
                            }
                        } else {
                            // Default answer
                            info!("[{}] Stage 2: Playing default answer", username_media);
                            media
                                .play_wav_bytes(
                                    username_media,
                                    ANSWER_WAV,
                                    recording_path.as_deref(),
                                    keep_alive,
                                )
                                .await
                        }
                    } else {
                        Ok(())
                    }
                };

                // Scheduled DTMF flows after answer (sent through the current
                // TX session — follows the jump when one occurs).
                let dtmf_flows = account
                    .dtmf_flows
                    .as_deref()
                    .and_then(|s| crate::config::parse_dtmf_flows(s).ok());
                let dtmf_media_shared = shared_media_session.clone();
                let dtmf_username = account.username.clone();
                let dtmf_cancel = cancel_token.clone();
                let dtmf_future = async move {
                    if let Some(flows) = dtmf_flows {
                        for entry in flows {
                            tokio::select! {
                                _ = tokio::time::sleep(entry.delay) => {}
                                _ = dtmf_cancel.cancelled() => return,
                            }
                            let media = dtmf_media_shared.lock().await.clone();
                            if let Some(m) = media {
                                info!(
                                    "[{}] Sending DTMF '{}' (after {:.1}s)",
                                    dtmf_username,
                                    entry.digit,
                                    entry.delay.as_secs_f64()
                                );
                                let _: Result<(), anyhow::Error> = m.send_dtmf(entry.digit).await;
                            }
                        }
                    }
                };

                let wait_timeout = match hangup_mode {
                    crate::config::HangupMode::After(secs) => Some(secs),
                    _ => None,
                };

                match hangup_mode {
                    crate::config::HangupMode::Remote => {
                        info!(
                            "[{}] Stage 3: waiting for remote hangup (no BYE)",
                            account.username
                        );
                        let media_handle = tokio::spawn(media_future);
                        let dtmf_handle = tokio::spawn(dtmf_future);
                        let mut ui_requested_bye = false;
                        tokio::select! {
                            _ = call_token_for_logic.cancelled() => {
                                info!("[{}] Call ended remotely.", account.username);
                            }
                            _ = control_token.cancelled() => {
                                info!("[{}] UI requested hangup — sending BYE.", account.username);
                                ui_requested_bye = true;
                            }
                            _ = cancel_token.cancelled() => {
                                info!("[{}] Cancellation requested during call.", account.username);
                            }
                        }
                        media_handle.abort();
                        dtmf_handle.abort();
                        if ui_requested_bye {
                            match server_dialog_clone.bye().await {
                                Ok(_) => info!("[{}] BYE sent successfully", account.username),
                                Err(e) => debug!("[{}] BYE send result: {:?}", account.username, e),
                            }
                        }
                    }
                    _ => {
                        match wait_timeout {
                            Some(secs) => {
                                info!(
                                    "[{}] Stage 3: Will hangup after {} seconds",
                                    account.username, secs
                                );
                                let dtmf_handle = tokio::spawn(dtmf_future);
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
                                    _ = control_token.cancelled() => {
                                        info!("[{}] UI requested hangup", account.username);
                                    }
                                    _ = cancel_token.cancelled() => {
                                        info!("[{}] Cancellation requested during call.", account.username);
                                    }
                                    _ = call_token_for_logic.cancelled() => {
                                        info!("[{}] Call ended remotely during playback.", account.username);
                                    }
                                }
                                dtmf_handle.abort();
                            }
                            None => {
                                let dtmf_handle = tokio::spawn(dtmf_future);
                                tokio::select! {
                                    res = media_future => {
                                        if let Err(e) = res {
                                            error!("[{}] Media error: {:?}", account.username, e);
                                        }
                                    }
                                    _ = control_token.cancelled() => {
                                        info!("[{}] UI requested hangup", account.username);
                                    }
                                    _ = cancel_token.cancelled() => {
                                        info!("[{}] Cancellation requested during call.", account.username);
                                    }
                                    _ = call_token_for_logic.cancelled() => {
                                        info!("[{}] Call ended remotely.", account.username);
                                    }
                                }
                                dtmf_handle.abort();
                            }
                        }
                    }
                }

                for s in live_sessions {
                    s.stop().await;
                }

                if matches!(hangup_mode, crate::config::HangupMode::Remote) {
                    info!(
                        "[{}] Remote hangup mode: skipping BYE (peer should hang up)",
                        account.username
                    );
                } else {
                    // Send BYE explicitly before dropping the guard
                    info!("[{}] About to send BYE", account.username);
                    match server_dialog_clone.bye().await {
                        Ok(_) => {
                            info!("[{}] BYE sent successfully", account.username);
                        }
                        Err(e) => {
                            debug!("[{}] BYE send result: {:?}", account.username, e);
                        }
                    }
                }
                info!("[{}] Call logic completed", account.username);
            };

            info!("[{}] Running call logic and monitor", username_log);
            let call_logic_with_shutdown = async move {
                call_logic.await;
                call_token_for_join.cancel();
            };
            tokio::join!(monitor_future, call_logic_with_shutdown);
            info!("[{}] Call logic finished, decrementing stats", username_log);

            // Decrement stats AFTER BYE is sent
            stats_for_decrement.dec_current();
            info!("[{}] Call task completed", username_log);
        });

        Ok(())
    }
}

/// Normalize a server address: strip `sip:` prefix if present, then add it back.
/// Allows users to provide addresses as `sip:192.168.1.1:5060` or `192.168.1.1:5060`.
fn normalize_sip_addr(addr: &str) -> String {
    let stripped = addr.strip_prefix("sip:").unwrap_or(addr);
    format!("sip:{}", stripped)
}

/// Extract the user part from a URI string (`sip:1001@host` → `1001`).
fn caller_user_part(uri: &str) -> String {
    let stripped = uri.trim().trim_start_matches("sip:").trim_start_matches("sips:");
    let user = stripped.split(['@', ';', ':', '>', '?']).next().unwrap_or("");
    user.to_string()
}

/// Caller match: patterns separated by `|`. A trailing `*` is a prefix
/// wildcard; otherwise exact (case-insensitive) match. Empty pattern list
/// matches everything.
fn caller_matches(pattern: &str, caller_user: &str) -> bool {
    let caller_user = caller_user.to_lowercase();
    pattern
        .split('|')
        .map(|p| p.trim().to_lowercase())
        .filter(|p| !p.is_empty())
        .any(|p| {
            if let Some(prefix) = p.strip_suffix('*') {
                caller_user.starts_with(prefix)
            } else {
                caller_user == p
            }
        })
}

/// Forwards DTMF events into the serve-mode call record and WebSocket feed.
struct CallDtmfObserver {
    record: Option<Arc<std::sync::Mutex<crate::web::state::CallRecord>>>,
    events: Option<tokio::sync::broadcast::Sender<crate::web::state::WsEvent>>,
    call_id: String,
}

impl crate::media::DtmfObserver for CallDtmfObserver {
    fn on_dtmf(&self, dir: &'static str, digit: char, end: bool) {
        if !end {
            return; // log completed digits only
        }
        let started_at_ms = self
            .record
            .as_ref()
            .map(|r| r.lock().unwrap().started_at_ms)
            .unwrap_or(0);
        let entry = crate::web::state::DtmfEventLog {
            t_ms: crate::web::state::now_ms().saturating_sub(started_at_ms),
            digit: digit.to_string(),
            dir: dir.to_string(),
        };
        if let Some(rec) = &self.record {
            rec.lock().unwrap().dtmf_events.push(entry.clone());
        }
        if let Some(tx) = &self.events {
            let _ = tx.send(crate::web::state::WsEvent::Dtmf {
                call_id: self.call_id.clone(),
                entry,
            });
        }
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

/// Parse a WebSocket URL into (Transport, host, port, path).
///
/// Supported formats:
///   wss://host:8443/ws   → (Wss, "host", 8443, "/ws")
///   ws://host:8080/ws    → (Ws,  "host", 8080, "/ws")
///   wss://host:8443      → (Wss, "host", 8443, "/ws")   (default path)
///
/// Default port: 443 for wss, 80 for ws.
/// Default path: "/ws".
fn parse_ws_url(url: &str) -> Result<(Transport, String, u16, String)> {
    let url = url.trim();
    let (scheme, rest) = url
        .split_once("://")
        .context("Invalid ws-url: missing :// (expected: wss://host:port/path)")?;

    let transport = match scheme {
        "wss" => Transport::Wss,
        "ws" => Transport::Ws,
        _ => anyhow::bail!("Invalid ws-url scheme '{}': expected 'ws' or 'wss'", scheme),
    };

    let (host_port, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/ws"),
    };

    let path = if path.is_empty() { "/ws" } else { path };

    let (host, port) = match host_port.rfind(':') {
        Some(idx) => (
            host_port[..idx].to_string(),
            host_port[idx + 1..].parse::<u16>()?,
        ),
        None => (
            host_port.to_string(),
            if transport == Transport::Wss {
                443u16
            } else {
                80u16
            },
        ),
    };

    Ok((transport, host, port, path.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sip_addr() {
        assert_eq!(
            normalize_sip_addr("192.168.1.1:5060"),
            "sip:192.168.1.1:5060"
        );
        assert_eq!(
            normalize_sip_addr("sip:192.168.1.1:5060"),
            "sip:192.168.1.1:5060"
        );
        assert_eq!(normalize_sip_addr("example.com"), "sip:example.com");
        assert_eq!(normalize_sip_addr("sip:example.com"), "sip:example.com");
        assert_eq!(normalize_sip_addr(""), "sip:");
    }

    fn make_invite_with_contact(contact: &str, via: &str) -> rsipstack::rsip::Request {
        let raw = format!(
            "INVITE sip:bot@192.168.1.2:5060 SIP/2.0\r\n\
             Via: {}\r\n\
             From: <sip:caller@example.com>;tag=abc\r\n\
             To: <sip:bot@example.com>\r\n\
             Call-ID: fix-test@192.168.1.2\r\n\
             CSeq: 1 INVITE\r\n\
             Max-Forwards: 70\r\n\
             Contact: {}\r\n\
             Content-Length: 0\r\n\
             \r\n",
            via, contact
        );
        raw.try_into().unwrap()
    }

    #[test]
    fn test_fix_unspecified_contact_with_received_rport() {
        let mut req = make_invite_with_contact(
            "<sip:caller@0.0.0.0:5060>",
            "SIP/2.0/UDP 10.0.0.1:5060;branch=z9hG4bK1;rport=51372;received=203.0.113.5",
        );
        SipBot::fix_unspecified_contact(&mut req);
        let contact = req.contact_header().unwrap().value().to_string();
        assert!(contact.contains("sip:caller@203.0.113.5:51372"), "{}", contact);
    }

    #[test]
    fn test_fix_unspecified_contact_fallback_via_sent_by() {
        let mut req = make_invite_with_contact(
            "sip:caller@0.0.0.0",
            "SIP/2.0/UDP 203.0.113.9:6060;branch=z9hG4bK2",
        );
        SipBot::fix_unspecified_contact(&mut req);
        let contact = req.contact_header().unwrap().value().to_string();
        assert!(contact.contains("sip:caller@203.0.113.9:6060"), "{}", contact);
    }

    #[test]
    fn test_fix_unspecified_contact_keeps_valid_contact() {
        let mut req = make_invite_with_contact(
            "<sip:caller@198.51.100.7:5090>",
            "SIP/2.0/UDP 203.0.113.9:5060;branch=z9hG4bK3;rport=40000;received=203.0.113.9",
        );
        SipBot::fix_unspecified_contact(&mut req);
        let contact = req.contact_header().unwrap().value().to_string();
        assert!(contact.contains("198.51.100.7:5090"), "{}", contact);
    }
}
