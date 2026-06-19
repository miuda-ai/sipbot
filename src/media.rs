use crate::audio_quality::{AudioQualityAnalyzer, AudioQualityConfig};
use crate::dtmf;
use crate::recorder::Recorder;
use crate::stats::CallStats;
use anyhow::{Context, Result};
use audio_codec::{CodecType, Decoder, Resampler, resample};
use bytes::Bytes;
#[cfg(feature = "local-device")]
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
#[cfg(feature = "local-device")]
use ringbuf::{HeapRb, traits::*};
use rustrtc::config::{
    AudioCapability, MediaCapabilities, RtcConfiguration, RtcpMuxPolicy, TransportMode,
};
use rustrtc::media::MediaError;
use rustrtc::media::MediaKind;
use rustrtc::media::frame::{AudioFrame, MediaSample};
use rustrtc::media::track::{MediaStreamTrack, SampleStreamSource, sample_track};
use rustrtc::peer_connection::{
    PeerConnection, PeerConnectionEvent, RtpCodecParameters, RtpSenderBuilder, TransceiverDirection,
};
use rustrtc::rtp::RtcpPacket;
use rustrtc::sdp::{SdpType, SessionDescription};
use rustrtc::transports::ice::stun::random_u32;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

fn create_encoder_for(ct: CodecType, channels: u16) -> Box<dyn audio_codec::Encoder> {
    if ct == CodecType::Opus {
        let app = if channels == 2 {
            audio_codec::opus::OpusApplication::Audio
        } else {
            audio_codec::opus::OpusApplication::Voip
        };
        let mut enc =
            audio_codec::opus::OpusEncoder::new_with_application(ct.samplerate(), channels, app);
        if channels == 2 {
            enc.set_bitrate(64000);
        } else {
            enc.set_bitrate(48000);
        }
        return Box::new(enc);
    }
    audio_codec::create_encoder(ct)
}

pub trait AudioSource: Send + Sync {
    fn next_chunk(&mut self, chunk_size: usize) -> Option<Vec<i16>>;
    fn sample_rate(&self) -> u32;
    fn channels(&self) -> u16;
    fn description(&self) -> String;
}

pub struct FileAudioSource {
    samples: Vec<i16>,
    offset: usize,
    sample_rate: u32,
    channels: u16,
    path: String,
}

impl FileAudioSource {
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut reader = hound::WavReader::open(path)?;
        let spec = reader.spec();
        let samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap_or(0)).collect();

        Ok(Self {
            samples,
            offset: 0,
            sample_rate: spec.sample_rate,
            channels: spec.channels,
            path: path.display().to_string(),
        })
    }

    pub fn from_bytes(wav_bytes: &[u8], name: &str) -> Result<Self> {
        let cursor = Cursor::new(wav_bytes);
        let mut reader = hound::WavReader::new(cursor)?;
        let spec = reader.spec();
        let samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap_or(0)).collect();

        Ok(Self {
            samples,
            offset: 0,
            sample_rate: spec.sample_rate,
            channels: spec.channels,
            path: name.to_string(),
        })
    }
}

impl AudioSource for FileAudioSource {
    fn next_chunk(&mut self, chunk_size: usize) -> Option<Vec<i16>> {
        if self.offset >= self.samples.len() {
            return None;
        }

        let end = (self.offset + chunk_size).min(self.samples.len());
        let mut chunk = self.samples[self.offset..end].to_vec();
        self.offset = end;

        // Pad with silence if needed
        if chunk.len() < chunk_size {
            chunk.resize(chunk_size, 0);
        }

        Some(chunk)
    }

    fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    fn channels(&self) -> u16 {
        self.channels
    }

    fn description(&self) -> String {
        format!("file:{}", self.path)
    }
}

// Removed: codec_from_name - use CodecType::try_from(&str) instead

fn get_codec_type(pt: Option<u8>, caps: &Option<MediaCapabilities>) -> CodecType {
    // First try: match by payload type
    let by_pt = pt.and_then(|p| {
        caps.as_ref()
            .and_then(|c| c.audio.iter().find(|a| a.payload_type == p))
            .and_then(|a| {
                let name = a.codec_name.to_lowercase();
                match name.as_str() {
                    "opus" => Some(CodecType::Opus),
                    "pcmu" => Some(CodecType::PCMU),
                    "pcma" => Some(CodecType::PCMA),
                    "g722" => Some(CodecType::G722),
                    "g729" => Some(CodecType::G729),
                    _ => CodecType::try_from(a.codec_name.as_str()).ok(),
                }
            })
    });

    if by_pt.is_some() {
        return by_pt.unwrap();
    }

    // Second try: for dynamic PTs (96-127), try matching by standard PT ranges
    if let Some(p) = pt {
        if let Ok(ct) = CodecType::try_from(p) {
            return ct;
        }
    }

    // Third try: if PT didn't match, check if there's a single codec capability
    // This handles the case where local and remote use different dynamic PTs
    if let Some(c) = caps {
        if c.audio.len() == 1 {
            let name = c.audio[0].codec_name.to_lowercase();
            match name.as_str() {
                "opus" => return CodecType::Opus,
                "pcmu" => return CodecType::PCMU,
                "pcma" => return CodecType::PCMA,
                "g722" => return CodecType::G722,
                "g729" => return CodecType::G729,
                _ => {
                    if let Ok(ct) = CodecType::try_from(c.audio[0].codec_name.as_str()) {
                        return ct;
                    }
                }
            }
        }
    }

    CodecType::PCMU
}

fn get_codec_channels(pt: Option<u8>, caps: &Option<MediaCapabilities>, ct: CodecType) -> u16 {
    if let (Some(p), Some(c)) = (pt, caps.as_ref())
        && let Some(cap) = c.audio.iter().find(|a| a.payload_type == p)
    {
        return (cap.channels as u16).max(1);
    }
    ct.channels()
}

fn get_audio_caps(codecs: &Option<Vec<String>>, nack_enabled: bool) -> Vec<AudioCapability> {
    let mut caps = Vec::new();
    let codec_list = if let Some(list) = codecs {
        list.clone()
    } else {
        vec![
            "pcmu".to_string(),
            "pcma".to_string(),
            "g722".to_string(),
            "g729".to_string(),
            "opus".to_string(),
        ]
    };

    for codec in codec_list {
        let mut cap = match codec.to_lowercase().as_str() {
            "pcmu" => AudioCapability::pcmu(),
            "pcma" => AudioCapability::pcma(),
            "opus" => AudioCapability::opus(),
            _ => {
                if let Ok(ct) = CodecType::try_from(codec.as_str()) {
                    AudioCapability {
                        payload_type: ct.payload_type(),
                        codec_name: format!("{:?}", ct).to_uppercase(),
                        clock_rate: ct.clock_rate(),
                        channels: ct.channels() as u8,
                        rtcp_fbs: vec!["nack".to_string()],
                        ..Default::default()
                    }
                } else {
                    continue;
                }
            }
        };
        if !nack_enabled {
            cap.rtcp_fbs.retain(|fb| fb != "nack");
        }
        caps.push(cap);
    }

    if caps.is_empty() {
        caps.push(AudioCapability::pcmu());
    }
    caps
}

fn now_ntp_short_16_16() -> u32 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let ntp_seconds = duration.as_secs().saturating_add(2_208_988_800);
    let ntp_fraction = (duration.subsec_nanos() as u64 * (1u64 << 32) / 1_000_000_000u64) as u32;

    (((ntp_seconds as u32) & 0xFFFF) << 16) | (ntp_fraction >> 16)
}

fn rtt_from_report_block(
    last_sender_report: u32,
    delay_since_last_sender_report: u32,
) -> Option<Duration> {
    if last_sender_report == 0 {
        return None;
    }

    let now = now_ntp_short_16_16();
    let rtt_ntp = now
        .wrapping_sub(last_sender_report)
        .wrapping_sub(delay_since_last_sender_report);

    let rtt_seconds = rtt_ntp as f64 / 65_536.0;
    if !(0.0..=10.0).contains(&rtt_seconds) {
        return None;
    }
    Some(Duration::from_secs_f64(rtt_seconds))
}

#[derive(Clone)]
pub struct MediaSession {
    pc: Arc<PeerConnection>,
    audio_source: Arc<SampleStreamSource>,
    recorder: Arc<Mutex<Option<Recorder>>>,
    stats: Arc<CallStats>,
    jitter_buffer_enabled: bool,
    last_nack_sent: Arc<std::sync::atomic::AtomicU64>,
    last_nack_recv: Arc<std::sync::atomic::AtomicU64>,
    last_nack_recovered: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(feature = "local-device")]
    local_playback_tx: Arc<Mutex<Option<ringbuf::HeapProd<i16>>>>,
    #[cfg(feature = "local-device")]
    output_sample_rate: Arc<std::sync::atomic::AtomicU32>,
    #[cfg(feature = "local-device")]
    output_resampler: Arc<Mutex<Option<audio_codec::Resampler>>>,
    #[cfg(feature = "local-device")]
    local_stop_tx: Arc<Mutex<Option<std::sync::mpsc::Sender<()>>>>,
    tracked_mids: Arc<Mutex<std::collections::HashSet<String>>>,
    echo_tracked_mids: Arc<Mutex<std::collections::HashSet<String>>>,
    cancel_token: CancellationToken,
    telephone_event_pt: Arc<std::sync::atomic::AtomicU8>,
    audio_quality_enabled: bool,
    audio_quality_config: Option<AudioQualityConfig>,
    audio_silent: Arc<std::sync::atomic::AtomicBool>,
}

impl MediaSession {
    async fn try_track_record_mid(&self, mid: &str) -> bool {
        let mut mids = self.tracked_mids.lock().await;
        if mids.contains(mid) {
            return false;
        }
        mids.insert(mid.to_string());
        true
    }

    async fn try_track_echo_mid(&self, mid: &str) -> bool {
        let mut mids = self.echo_tracked_mids.lock().await;
        if mids.contains(mid) {
            return false;
        }
        mids.insert(mid.to_string());
        true
    }

    fn spawn_rtcp_rtt_collectors(&self) {
        let transceivers = self.pc.get_transceivers();
        for transceiver in transceivers {
            if let Some(sender) = transceiver.sender() {
                let sender_ssrc = sender.ssrc();
                let stats = self.stats.clone();
                let mut rtcp_rx = sender.subscribe_rtcp();
                let token = self.cancel_token.child_token();

                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = token.cancelled() => break,
                            packet = rtcp_rx.recv() => {
                                match packet {
                                    Ok(RtcpPacket::ReceiverReport(rr)) => {
                                        for block in rr.report_blocks {
                                            if block.ssrc != sender_ssrc {
                                                continue;
                                            }
                                            if let Some(rtt) = rtt_from_report_block(
                                                block.last_sender_report,
                                                block.delay_since_last_sender_report,
                                            ) {
                                                stats.add_rtcp_rtt(rtt);
                                            }
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                });
            }
        }
    }

    pub async fn new(
        remote_sdp: &str,
        srtp_enabled: bool,
        nack_enabled: bool,
        jitter_buffer_enabled: bool,
        external_ip: Option<String>,
        codecs: Option<Vec<String>>,
        stats: Arc<CallStats>,
        _audio_quality_config: Option<AudioQualityConfig>,
    ) -> Result<(Self, String, String)> {
        let mut config = RtcConfiguration::default();
        if let Some(ip) = external_ip {
            config.external_ip = Some(ip);
        }
        config.transport_mode = if srtp_enabled {
            TransportMode::Srtp
        } else {
            config.certificates = vec![];
            TransportMode::Rtp
        };
        let mut audio_caps = get_audio_caps(&codecs, nack_enabled);
        let remote_sdp_upper = remote_sdp.to_uppercase();

        // Extract PT mappings from remote SDP for dynamic payload types
        let mut remote_pt_map: std::collections::HashMap<String, u8> =
            std::collections::HashMap::new();
        for line in remote_sdp.lines() {
            if line.to_lowercase().starts_with("a=rtpmap:") {
                // Format: a=rtpmap:111 opus/48000/2
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Some(pt_and_codec) = parts[1].split_once(' ') {
                        let pt_str = pt_and_codec.0;
                        let codec_spec = pt_and_codec.1;
                        if let Ok(pt) = pt_str.parse::<u8>() {
                            let codec_name =
                                codec_spec.split('/').next().unwrap_or("").to_uppercase();
                            remote_pt_map.insert(codec_name, pt);
                        }
                    }
                }
            }
        }

        audio_caps.retain(|cap| {
            let pt = cap.payload_type;
            let name = cap.codec_name.to_uppercase();
            let pt_str = pt.to_string();

            // Check if PT is in m=audio line
            let pt_in_mline =
                if let Some(mline) = remote_sdp_upper.lines().find(|l| l.starts_with("M=AUDIO")) {
                    mline.split_whitespace().skip(3).any(|s| s == pt_str)
                } else {
                    false
                };

            // Check if name is in rtpmap
            let name_in_rtpmap = remote_sdp_upper.contains(&format!(" {}/", name));

            pt_in_mline || name_in_rtpmap
        });

        // Update payload types from remote SDP to match remote's dynamic PT assignments
        for cap in &mut audio_caps {
            let name = cap.codec_name.to_uppercase();
            if let Some(&remote_pt) = remote_pt_map.get(&name) {
                cap.payload_type = remote_pt;
            }
        }

        if audio_caps.is_empty() {
            info!("No matching codecs found in offer, falling back to PCMU");
            audio_caps = vec![AudioCapability::pcmu()];
        }

        // Decide sender params based on the best common capability.
        // We use the first capability because they are ordered by local preference
        // and we have already filtered them to intersect with the remote offer.
        let chosen_cap = audio_caps.first().unwrap();
        let chosen_codec_name = chosen_cap.codec_name.clone();
        let chosen_params = RtpCodecParameters {
            payload_type: chosen_cap.payload_type,
            clock_rate: chosen_cap.clock_rate,
            channels: chosen_cap.channels,
        };

        config.rtcp_mux_policy = RtcpMuxPolicy::Negotiate;
        config.ice_servers = vec![]; // No STUN/TURN servers
        config.media_capabilities = Some(MediaCapabilities {
            audio: audio_caps,
            video: vec![],
            application: None,
            ..Default::default()
        });

        let pc = Arc::new(PeerConnection::new(config.clone()));
        let ssrc_id = random_u32();
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 1000);
        let audio_source = Arc::new(source);

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));

        let remote_desc = SessionDescription::parse(SdpType::Offer, remote_sdp)?;
        pc.set_remote_description(remote_desc).await?;

        // Attach sender so that the PeerConnection has a listener mapping for incoming packets
        let transceivers = pc.get_transceivers();
        if let Some(t) = transceivers.first() {
            let mut builder = RtpSenderBuilder::new(track.clone(), ssrc_id)
                .stream_id("audio".to_string())
                .params(chosen_params);
            if nack_enabled {
                builder = builder
                    .nack(pc.config().nack_buffer_size)
                    .bitrate_controller();
            }
            let sender = builder.build();
            t.set_sender(Some(sender));
            t.set_direction(TransceiverDirection::SendRecv);
        } else {
            let params = RtpCodecParameters {
                payload_type: chosen_params.payload_type,
                clock_rate: chosen_params.clock_rate,
                channels: 1,
            };
            pc.add_track(track.clone(), params)?;
        }

        pc.wait_for_gathering_complete().await;

        let answer = pc.create_answer().await?;
        let sdp_str = answer.to_sdp_string();
        let answer = SessionDescription::parse(SdpType::Answer, &sdp_str)?;

        pc.set_local_description(answer.clone())?;

        let local_sdp = pc
            .local_description()
            .context("Failed to get local description")?
            .to_sdp_string();

        if local_sdp.is_empty() || !local_sdp.contains("m=audio") {
            anyhow::bail!("Failed to generate valid audio answer SDP: {}", local_sdp);
        }

        let telephone_event_pt =
            dtmf::parse_telephone_event_pt(remote_sdp).unwrap_or(dtmf::DTMF_TELEPHONE_EVENT_PT);
        let local_sdp = inject_telephone_event_sdp(&local_sdp, telephone_event_pt);

        let cancel_token = CancellationToken::new();
        let session = Self {
            pc: pc.clone(),
            audio_source: audio_source.clone(),
            recorder: recorder.clone(),
            stats: stats.clone(),
            jitter_buffer_enabled,
            last_nack_sent: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_nack_recv: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_nack_recovered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "local-device")]
            local_playback_tx: Arc::new(Mutex::new(None)),
            #[cfg(feature = "local-device")]
            output_sample_rate: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            #[cfg(feature = "local-device")]
            output_resampler: Arc::new(Mutex::new(None)),
            #[cfg(feature = "local-device")]
            local_stop_tx: Arc::new(Mutex::new(None)),
            tracked_mids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            echo_tracked_mids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            cancel_token: cancel_token.clone(),
            telephone_event_pt: Arc::new(std::sync::atomic::AtomicU8::new(telephone_event_pt)),
            audio_quality_enabled: _audio_quality_config
                .as_ref()
                .map(|c| c.enabled)
                .unwrap_or(false),
            audio_quality_config: _audio_quality_config.clone(),
            audio_silent: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        // Spawn a background task to listen for incoming track events so there is
        // always a listener for incoming RTP packets (prevents "No listener found")
        let bg_session = session.clone();
        let bg_token = cancel_token.clone();
        tokio::spawn(async move {
            let pc = bg_session.pc.clone();
            loop {
                tokio::select! {
                    _ = bg_token.cancelled() => break,
                    event = pc.recv() => {
                        if let Some(event) = event {
                            if let PeerConnectionEvent::Track(transceiver) = event {
                                let mid = transceiver
                                    .mid()
                                    .clone()
                                    .unwrap_or_else(|| "unknown".to_string());
                                if !bg_session.try_track_record_mid(&mid).await {
                                    continue;
                                }

                                if let Some(receiver) = transceiver.receiver().as_ref() {
                                    spawn_track_recorder(
                                        bg_session.clone(),
                                        receiver.track(),
                                        bg_token.child_token(),
                                    );
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        });

        // Also attach recorders for any already-present receivers
        for transceiver in pc.get_transceivers() {
            if let Some(receiver) = transceiver.receiver().as_ref() {
                let mid = transceiver
                    .mid()
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                if !session.try_track_record_mid(&mid).await {
                    continue;
                }
                spawn_track_recorder(
                    session.clone(),
                    receiver.track(),
                    cancel_token.child_token(),
                );
            }
        }

        session.spawn_rtcp_rtt_collectors();

        Ok((session, local_sdp, chosen_codec_name))
    }

    pub async fn new_offer(
        srtp_enabled: bool,
        nack_enabled: bool,
        jitter_buffer_enabled: bool,
        external_ip: Option<String>,
        codecs: Option<Vec<String>>,
        send_audio: bool,
        stats: Arc<CallStats>,
        _audio_quality_config: Option<AudioQualityConfig>,
    ) -> Result<(Self, String)> {
        let mut config = RtcConfiguration::default();
        if let Some(ip) = external_ip {
            config.external_ip = Some(ip);
        }
        config.transport_mode = if srtp_enabled {
            TransportMode::Srtp
        } else {
            config.certificates = vec![];
            TransportMode::Rtp
        };
        let audio_caps = get_audio_caps(&codecs, nack_enabled);
        config.rtcp_mux_policy = RtcpMuxPolicy::Negotiate;
        // config.bundle_policy = BundlePolicy::MaxBundle;
        config.ice_servers = vec![];
        config.media_capabilities = Some(MediaCapabilities {
            audio: audio_caps,
            video: vec![],
            application: None,
            ..Default::default()
        });

        let pc = Arc::new(PeerConnection::new(config));
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 1000);
        let audio_source = Arc::new(source);

        if send_audio {
            let params = pc
                .config()
                .media_capabilities
                .as_ref()
                .and_then(|c| c.audio.first())
                .map(|a| RtpCodecParameters {
                    payload_type: a.payload_type,
                    clock_rate: a.clock_rate,
                    channels: a.channels,
                })
                .unwrap_or(RtpCodecParameters {
                    payload_type: 0,
                    clock_rate: 8000,
                    channels: 1,
                });
            pc.add_track(track.clone(), params)?;
            for t in pc.get_transceivers() {
                t.set_direction(TransceiverDirection::SendRecv);
            }
        } else {
            pc.add_transceiver(rustrtc::MediaKind::Audio, TransceiverDirection::RecvOnly);
        }

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));

        pc.wait_for_gathering_complete().await;

        let offer = pc.create_offer().await?;
        let sdp_str = offer.to_sdp_string();
        let offer = SessionDescription::parse(SdpType::Offer, &sdp_str)?;

        pc.set_local_description(offer.clone())?;

        let local_sdp = pc
            .local_description()
            .context("Failed to get local description")?
            .to_sdp_string();

        if local_sdp.is_empty() || !local_sdp.contains("m=audio") {
            anyhow::bail!("Failed to generate valid audio offer SDP: {}", local_sdp);
        }

        let telephone_event_pt = dtmf::DTMF_TELEPHONE_EVENT_PT;
        let local_sdp = inject_telephone_event_sdp(&local_sdp, telephone_event_pt);

        let cancel_token = CancellationToken::new();
        let session = Self {
            pc,
            audio_source,
            recorder,
            stats,
            jitter_buffer_enabled,
            last_nack_sent: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_nack_recv: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_nack_recovered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "local-device")]
            local_playback_tx: Arc::new(Mutex::new(None)),
            #[cfg(feature = "local-device")]
            output_sample_rate: Arc::new(std::sync::atomic::AtomicU32::new(8000)),
            #[cfg(feature = "local-device")]
            output_resampler: Arc::new(Mutex::new(None)),
            #[cfg(feature = "local-device")]
            local_stop_tx: Arc::new(Mutex::new(None)),
            tracked_mids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            echo_tracked_mids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            cancel_token,
            telephone_event_pt: Arc::new(std::sync::atomic::AtomicU8::new(telephone_event_pt)),
            audio_quality_enabled: _audio_quality_config
                .as_ref()
                .map(|c| c.enabled)
                .unwrap_or(false),
            audio_quality_config: _audio_quality_config.clone(),
            audio_silent: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        session.spawn_rtcp_rtt_collectors();

        Ok((session, local_sdp))
    }

    pub async fn send_dtmf(&self, digit: char) -> Result<()> {
        let pt = self.telephone_event_pt.load(Ordering::Relaxed);
        if pt == 0 {
            return Ok(());
        }
        let Some(event) = dtmf::digit_to_event(digit) else {
            return Ok(());
        };
        let start_ev = dtmf::DtmfEvent {
            event,
            end: false,
            volume: 10,
            duration: 0,
        };
        let start_frame = MediaSample::Audio(AudioFrame {
            rtp_timestamp: random_u32(),
            data: Bytes::copy_from_slice(&dtmf::encode_dtmf(start_ev)),
            clock_rate: dtmf::DTMF_CLOCK_RATE,
            payload_type: Some(pt),
            marker: true,
            ..Default::default()
        });
        self.audio_source.send(start_frame).await?;
        let end_ev = dtmf::DtmfEvent {
            event,
            end: true,
            volume: 10,
            duration: 480,
        };
        let end_frame = MediaSample::Audio(AudioFrame {
            rtp_timestamp: random_u32(),
            data: Bytes::copy_from_slice(&dtmf::encode_dtmf(end_ev)),
            clock_rate: dtmf::DTMF_CLOCK_RATE,
            payload_type: Some(pt),
            marker: false,
            ..Default::default()
        });
        self.audio_source.send(end_frame).await?;
        self.stats.inc_tx_dtmf();
        info!("[DTMF] Sent digit '{}' (event={})", digit, event);
        Ok(())
    }

    pub fn get_telephone_event_pt(&self) -> u8 {
        self.telephone_event_pt.load(Ordering::Relaxed)
    }

    pub fn get_local_sdp(&self) -> Option<String> {
        self.pc.local_description().map(|d| d.to_sdp_string())
    }

    /// Set whether audio sending is silent (muted).
    /// When true, echo/playback is stopped; when false, it resumes.
    pub async fn set_audio_silent(&self, silent: bool) {
        self.audio_silent.store(silent, Ordering::Relaxed);
        info!(
            "[MediaSession] Audio silent set to {}",
            if silent { "true (muted)" } else { "false (unmuted)" }
        );
    }

    /// Renegotiate SDP with a remote re-INVITE offer.
    /// Returns the new local answer SDP string.
    pub async fn renegotiate(&self, remote_sdp: &str) -> Result<String> {
        info!("[MediaSession] Renegotiating SDP with remote offer");
        let remote_desc = SessionDescription::parse(SdpType::Offer, remote_sdp)?;
        self.pc.set_remote_description(remote_desc).await?;

        let answer = self.pc.create_answer().await?;
        let sdp_str = answer.to_sdp_string();
        let answer_desc = SessionDescription::parse(SdpType::Answer, &sdp_str)?;
        self.pc.set_local_description(answer_desc)?;

        let local = self
            .pc
            .local_description()
            .context("Failed to get local description after renegotiation")?
            .to_sdp_string();

        let pt = self.telephone_event_pt.load(Ordering::Relaxed);
        Ok(inject_telephone_event_sdp(&local, pt))
    }

    pub async fn set_remote_answer(&self, remote_sdp: &str) -> Result<String> {
        let remote_desc = SessionDescription::parse(SdpType::Answer, remote_sdp)?;

        // In RTP mode, rustrtc now supports setting remote description multiple times
        // (e.g., 183 early media followed by 200 OK reinvite)
        // Only skip if the SDP content is exactly the same
        if let Some(current_remote) = self.pc.remote_description() {
            if current_remote.to_sdp_string() == remote_sdp {
                tracing::debug!("Remote description unchanged, skipping set_remote_description");
                return Ok(self.get_codec_name_from_sdp(remote_sdp));
            }
            tracing::debug!("Remote description changed, updating (supports reinvite scenario)");
        } else {
            tracing::debug!("Setting remote description for the first time");
        }

        self.pc.set_remote_description(remote_desc).await?;

        if let Some(pt) = dtmf::parse_telephone_event_pt(remote_sdp) {
            self.telephone_event_pt.store(pt, Ordering::Relaxed);
            info!("[DTMF] Remote supports telephone-event at PT {}", pt);
        }

        Ok(self.get_codec_name_from_sdp(remote_sdp))
    }

    pub fn get_negotiated_codec(&self) -> String {
        let pt = self
            .pc
            .get_transceivers()
            .first()
            .and_then(|t| t.sender().as_ref().map(|s| s.params().payload_type));

        let ct = get_codec_type(pt, &self.pc.config().media_capabilities);
        format!("{:?}", ct).to_uppercase()
    }

    fn get_codec_name_from_sdp(&self, sdp: &str) -> String {
        let mut codec_name = "Unknown".to_string();
        if let Some(mline) = sdp
            .lines()
            .find(|l| l.to_lowercase().starts_with("m=audio"))
        {
            if let Some(pt_str) = mline.split_whitespace().nth(3) {
                if let Ok(pt) = pt_str.parse::<u8>() {
                    let rtpmap_prefix = format!("a=rtpmap:{} ", pt);
                    if let Some(rtpmap) = sdp.lines().find(|l| l.starts_with(&rtpmap_prefix)) {
                        if let Some(spec) = rtpmap.split_whitespace().nth(1) {
                            codec_name = spec.split('/').next().unwrap_or("Unknown").to_uppercase();
                        }
                    } else {
                        codec_name = CodecType::try_from(pt)
                            .ok()
                            .map(|ct| format!("{:?}", ct).to_uppercase())
                            .unwrap_or_else(|| "Unknown".to_string());
                    }
                }
            }
        }
        codec_name
    }

    /// Setup transceivers for recording by spawning track recorders for existing receivers
    async fn setup_transceivers_for_recording(&self, child_token: CancellationToken) {
        let transceivers = self.pc.get_transceivers();
        for transceiver in transceivers {
            if let Some(receiver) = transceiver.receiver().as_ref() {
                let mid = transceiver
                    .mid()
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                if !self.try_track_record_mid(&mid).await {
                    continue;
                }
                spawn_track_recorder(self.clone(), receiver.track(), child_token.clone());
            }
        }
    }

    /// Initialize recording if path is provided
    async fn init_recorder(&self, username: &str, recording_path: Option<&Path>) {
        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(username.to_string(), path.to_path_buf()));
        }
    }

    /// Spawn a task to handle incoming PC track events
    fn spawn_track_event_handler(
        &self,
        username: String,
        child_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let pc = self.pc.clone();
        let session = self.clone();

        tokio::spawn(async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    let mid = transceiver
                        .mid()
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    if !session.try_track_record_mid(&mid).await {
                        tracing::debug!(
                            "[{}] Track {} already being recorded, skipping",
                            username,
                            mid
                        );
                        continue;
                    }

                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            session.clone(),
                            receiver.track(),
                            child_token.clone(),
                        );
                    }
                }
            }
        })
    }

    fn spawn_audio_loop(
        &self,
        username: String,
        track: Arc<dyn MediaStreamTrack>,
        token: CancellationToken,
    ) {
        let audio_source = self.audio_source.clone();
        let recorder = self.recorder.clone();
        let stats = self.stats.clone();
        let jitter_buffer_enabled = self.jitter_buffer_enabled;
        let session = self.clone();
        let telephone_event_pt = self.telephone_event_pt.load(Ordering::Relaxed);
        let audio_quality_enabled = self.audio_quality_enabled;
        let audio_quality_config = self.audio_quality_config.clone();
        let audio_silent = self.audio_silent.clone();

        tokio::spawn(async move {
            let mut decoder: Option<Box<dyn Decoder + Send>> = None;
            let mut current_pt: Option<u8> = None;
            let mut recorder_resampler: Option<Resampler> = None;
            let mut aq = if audio_quality_enabled {
                audio_quality_config.map(AudioQualityAnalyzer::new)
            } else {
                None
            };
            let te_pt = if telephone_event_pt > 0 {
                Some(telephone_event_pt)
            } else {
                None
            };

            let mut last_seq: Option<u16> = None;
            let mut last_timestamp: Option<u32> = None;
            let mut sync_interval = tokio::time::interval(Duration::from_secs(1));

            if jitter_buffer_enabled {
                use rustrtc::media::JitterBuffer;
                let mut jb =
                    JitterBuffer::new(Duration::from_millis(20), Duration::from_millis(200), 100);
                loop {
                    let wait = jb.next_pop_wait().unwrap_or(Duration::from_millis(100));
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = sync_interval.tick() => {
                            session.sync_nack_stats();
                        }
                        res = track.recv() => {
                            match res {
                                Ok(sample) => {
                                    jb.push(sample);
                                }
                                Err(e) => {
                                    if !matches!(e, MediaError::EndOfStream) {
                                        error!("[{}] Failed to receive sample: {:?}", username, e);
                                    }
                                    return;
                                }
                            }
                        }
                        _ = tokio::time::sleep(wait) => {
                            while let Some(mut sample) = jb.pop() {
                                let is_te = if let MediaSample::Audio(ref frame) = sample {
                                    te_pt.is_some() && frame.payload_type == te_pt
                                } else {
                                    false
                                };
                                if is_te {
                                    if let MediaSample::Audio(ref frame) = sample {
                                        if frame.data.len() >= 4 {
                                            if let Some(ev) = dtmf::decode_dtmf(&frame.data) {
                                                if let Some(digit) = dtmf::event_to_digit(ev.event) {
                                                    info!("[{}] RX DTMF (jb): digit='{}' event={} end={}", username, digit, ev.event, ev.end);
                                                    stats.inc_rx_dtmf();
                                                }
                                            }
                                        }
                                    }
                                    continue;
                                }
                                if let MediaSample::Audio(ref frame) = sample {
                                    if current_pt != frame.payload_type {
                                        current_pt = frame.payload_type;
                                        if let Some(pt) = current_pt {
                                            let ct = get_codec_type(Some(pt), &session.pc.config().media_capabilities);
                                            let d = audio_codec::create_decoder(ct);
                                            decoder = Some(d);
                                            let rate = ct.samplerate();
                                            if rate != 16000 {
                                                recorder_resampler = Some(Resampler::new(rate as usize, 16000));
                                            } else {
                                                recorder_resampler = None;
                                            }
                                        }
                                    }
                                }

                                if let Some(ref mut dec) = decoder {
                                    let ct = if let MediaSample::Audio(ref mut frame) = sample {
                                        let ct = get_codec_type(frame.payload_type, &session.pc.config().media_capabilities);
                                        frame.clock_rate = ct.clock_rate();
                                        ct
                                    } else {
                                        CodecType::PCMU
                                    };

                                    let rtp_clock_rate = ct.clock_rate();

                                    Self::process_sample(
                                        &username,
                                        sample,
                                        &audio_source,
                                        &recorder,
                                        &stats,
                                        dec,
                                        &mut recorder_resampler,
                                        &mut last_seq,
                                        &mut last_timestamp,
                                        rtp_clock_rate,
                                        ct.samplerate(),
                                        te_pt,
                                        aq.as_mut(),
                                        &audio_silent,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
            } else {
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = sync_interval.tick() => {
                            session.sync_nack_stats();
                        }
                        res = track.recv() => {
                            match res {
                                Ok(mut sample) => {
                                    let is_te = if let MediaSample::Audio(ref frame) = sample {
                                        te_pt.is_some() && frame.payload_type == te_pt
                                    } else {
                                        false
                                    };
                                    if is_te {
                                        if let MediaSample::Audio(ref frame) = sample {
                                            if frame.data.len() >= 4 {
                                                if let Some(ev) = dtmf::decode_dtmf(&frame.data) {
                                                    if let Some(digit) = dtmf::event_to_digit(ev.event) {
                                                        info!("[{}] RX DTMF: digit='{}' event={} end={}", username, digit, ev.event, ev.end);
                                                        stats.inc_rx_dtmf();
                                                    }
                                                }
                                            }
                                        }
                                        continue;
                                    }
                                    if let MediaSample::Audio(ref frame) = sample {
                                        if current_pt != frame.payload_type {
                                            current_pt = frame.payload_type;
                                            if let Some(pt) = current_pt {
                                                let ct = get_codec_type(Some(pt), &session.pc.config().media_capabilities);
                                                let d = audio_codec::create_decoder(ct);
                                                decoder = Some(d);
                                                let rate = ct.samplerate();
                                                if rate != 16000 {
                                                    recorder_resampler = Some(Resampler::new(rate as usize, 16000));
                                                } else {
                                                    recorder_resampler = None;
                                                }
                                            }
                                        }
                                    }

                                    if let Some(ref mut dec) = decoder {
                                        let ct = if let MediaSample::Audio(ref mut frame) = sample {
                                            let ct = get_codec_type(frame.payload_type, &session.pc.config().media_capabilities);
                                            frame.clock_rate = ct.clock_rate();
                                            ct
                                        } else {
                                            CodecType::PCMU
                                        };

                                        let rtp_clock_rate = ct.clock_rate();

                                        Self::process_sample(
                                            &username,
                                            sample,
                                            &audio_source,
                                            &recorder,
                                            &stats,
                                            dec,
                                            &mut recorder_resampler,
                                            &mut last_seq,
                                            &mut last_timestamp,
                                            rtp_clock_rate,
                                            ct.samplerate(),
                                            te_pt,
                                            aq.as_mut(),
                                            &audio_silent,
                                        )
                                        .await;
                                    }
                                }
                                Err(e) => {
                                    if !matches!(e, MediaError::EndOfStream) {
                                        error!("[{}] Failed to receive sample: {:?}", username, e);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    async fn process_sample(
        username: &str,
        mut sample: MediaSample,
        audio_source: &SampleStreamSource,
        recorder: &Mutex<Option<Recorder>>,
        stats: &CallStats,
        decoder: &mut Box<dyn Decoder + Send>,
        recorder_resampler: &mut Option<Resampler>,
        last_seq: &mut Option<u16>,
        last_timestamp: &mut Option<u32>,
        rtp_clock_rate: u32,
        actual_sample_rate: u32,
        telephone_event_pt: Option<u8>,
        audio_quality: Option<&mut AudioQualityAnalyzer>,
        audio_silent: &Arc<std::sync::atomic::AtomicBool>,
    ) {
        // Check for DTMF telephone-event packets
        if let MediaSample::Audio(ref frame) = sample {
            if let Some(te_pt) = telephone_event_pt {
                if frame.payload_type == Some(te_pt) && frame.data.len() >= 4 {
                    if let Some(ev) = dtmf::decode_dtmf(&frame.data) {
                        if let Some(digit) = dtmf::event_to_digit(ev.event) {
                            info!(
                                "[{}] RX DTMF: digit='{}' event={} end={}",
                                username, digit, ev.event, ev.end
                            );
                            stats.inc_rx_dtmf();
                        }
                    }
                    // Don't echo telephone-event packets back
                    return;
                }
            }
        }

        let decoded = if let MediaSample::Audio(ref frame) = sample {
            let decoded_data = decoder.decode(&frame.data);
            // Debug: calculate output RMS every 50 packets
            static DEBUG_COUNTER: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let count = DEBUG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count % 50 == 0 {
                let rms = if !decoded_data.is_empty() {
                    let sum: f64 = decoded_data.iter().map(|&s| (s as f64).powi(2)).sum();
                    (sum / decoded_data.len() as f64).sqrt()
                } else {
                    0.0
                };
                tracing::debug!(
                    "[{}] Audio decode: packet_len={}, decoded_len={}, rms={:.2}",
                    username,
                    frame.data.len(),
                    decoded_data.len(),
                    rms
                );
            }
            Some(decoded_data)
        } else {
            None
        };

        // Validate timestamp continuity and rewrite if needed to fix interleaved streams
        if let MediaSample::Audio(ref mut frame) = sample {
            if let Some(ref decoded_data) = decoded {
                if let Some(last_ts) = *last_timestamp {
                    // Calculate expected timestamp based on last timestamp + samples
                    let ticks = (decoded_data.len() as u64 * rtp_clock_rate as u64
                        / actual_sample_rate as u64) as u32;
                    let expected_ts = last_ts.wrapping_add(ticks);
                    let ts_diff = frame.rtp_timestamp.wrapping_sub(expected_ts);

                    // Allow up to 10 seconds of jump to handle legitimate gaps
                    let max_reasonable_jump: u32 = rtp_clock_rate * 10;

                    // Rewrite packets with large forward jumps
                    if ts_diff > max_reasonable_jump && ts_diff < (u32::MAX / 2) {
                        tracing::debug!(
                            "[{}] Rewriting timestamp (forward jump): seq={:?} original_ts={} -> expected_ts={} diff={} (>{:.1}s)",
                            username,
                            frame.sequence_number,
                            frame.rtp_timestamp,
                            expected_ts,
                            ts_diff,
                            ts_diff as f32 / rtp_clock_rate as f32
                        );
                        frame.rtp_timestamp = expected_ts;
                    }
                    // Rewrite packets with large backward jumps
                    else if ts_diff > (u32::MAX / 2) {
                        let backward_diff = last_ts.wrapping_sub(frame.rtp_timestamp);
                        if backward_diff > max_reasonable_jump {
                            tracing::debug!(
                                "[{}] Rewriting timestamp (backward jump): seq={:?} original_ts={} -> expected_ts={} diff=-{} (>{:.1}s)",
                                username,
                                frame.sequence_number,
                                frame.rtp_timestamp,
                                expected_ts,
                                backward_diff,
                                backward_diff as f32 / rtp_clock_rate as f32
                            );
                            frame.rtp_timestamp = expected_ts;
                        }
                    }
                }
            }
        }

        // Record RX
        {
            if let MediaSample::Audio(frame) = &sample {
                if let Some(seq) = frame.sequence_number {
                    if let Some(last) = *last_seq {
                        let expected = last.wrapping_add(1);
                        if seq != expected {
                            let diff = seq.wrapping_sub(last) as i16;

                            if diff > 1 {
                                // Gap detected, these are lost
                                stats.inc_rx_lost((diff - 1) as u64);
                                *last_seq = Some(seq);
                            } else if diff < 0 {
                                // Out of order packet
                                // We don't increment recovered here because sync_nack_stats handles it from rustrtc
                            }
                        } else {
                            *last_seq = Some(seq);
                        }
                    } else {
                        *last_seq = Some(seq);
                    }
                }

                // Update last timestamp
                *last_timestamp = Some(frame.rtp_timestamp);

                stats.inc_rx(1, frame.data.len() as u64);
                // Also record TX since we are echoing
                stats.inc_tx(1, frame.data.len() as u64);

                // Run audio quality analysis on decoded audio
                if let Some(ref decoded_data) = decoded {
                    if let Some(aq) = audio_quality {
                        let r = aq.analyze_frame(
                            decoded_data,
                            frame.rtp_timestamp,
                            rtp_clock_rate,
                            actual_sample_rate,
                            20,
                        );
                        stats.add_audio_quality_stats(
                            if r.sample_count_mismatch { 1 } else { 0 },
                            if r.is_shrill { 1 } else { 0 },
                            if r.is_muffled { 1 } else { 0 },
                            if r.clipping_ratio > 0.01 { 1 } else { 0 },
                            if r.is_silence { 1 } else { 0 },
                            1,
                        );
                    }
                }

                if let Ok(rec) = recorder.try_lock() {
                    if let Some(r) = rec.as_ref() {
                        if let Some(ref decoded_data) = decoded {
                            let resampled = if let Some(resampler) = recorder_resampler {
                                resampler.resample(decoded_data)
                            } else {
                                decoded_data.clone()
                            };
                            r.record_rx(&resampled);
                            r.record_tx(&resampled);
                        }
                    }
                }
            } else {
                tracing::error!("RX SAMPLE: NOT AUDIO");
            }
        }

        // Echo (skip if audio is silenced due to hold)
        if audio_silent.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!("[{}] Audio silent, skipping echo", username);
        } else if let Err(e) = audio_source.send(sample).await {
            tracing::error!("[{}] Failed to send echo sample: {:?}", username, e);
        }
    }

    #[cfg(feature = "local-device")]
    pub async fn play_local_device(
        &self,
        username: String,
        recording_path: Option<&Path>,
        _keep_alive: bool,
        timeout_secs: Option<u64>,
    ) -> Result<()> {
        info!(
            "[{}] Using local audio device for playback and capture",
            username
        );

        // Check if local playback is already active
        {
            let stop_tx = self.local_stop_tx.lock().await;
            if stop_tx.is_some() {
                info!(
                    "[{}] Local audio already active, updating recorder and continuing",
                    username
                );
                self.init_recorder(&username, recording_path).await;
                // Wait indefinitely since the streams are already running
                // This will be cancelled by cancel_token or remote hangup
                self.cancel_token.cancelled().await;
                return Ok(());
            }
        }

        self.init_recorder(&username, recording_path).await;

        let host = cpal::default_host();
        let input_device = host
            .default_input_device()
            .context("No input device found")?;
        let output_device = host
            .default_output_device()
            .context("No output device found")?;

        let input_config = input_device.default_input_config()?;
        let output_config = output_device.default_output_config()?;

        // Try to find a 48000Hz config if possible for both input and output
        #[cfg(target_os = "macos")]
        let requested_rate = 48000;
        #[cfg(not(target_os = "macos"))]
        let requested_rate = 48000;

        let input_config: cpal::StreamConfig = match input_device.supported_input_configs() {
            Ok(configs) => configs
                .filter(|c| c.channels() <= 2) // Prefer mono or stereo
                .find(|c| {
                    c.max_sample_rate() >= requested_rate && c.min_sample_rate() <= requested_rate
                })
                .map(|c| c.with_sample_rate(requested_rate).into())
                .unwrap_or_else(|| input_config.into()),
            Err(_) => input_config.into(),
        };

        let output_config: cpal::StreamConfig = match output_device.supported_output_configs() {
            Ok(configs) => configs
                .filter(|c| c.channels() <= 2)
                .find(|c| {
                    c.max_sample_rate() >= requested_rate && c.min_sample_rate() <= requested_rate
                })
                .map(|c| c.with_sample_rate(requested_rate).into())
                .unwrap_or_else(|| output_config.into()),
            Err(_) => output_config.into(),
        };

        self.output_sample_rate.store(
            output_config.sample_rate,
            std::sync::atomic::Ordering::Relaxed,
        );

        let output_channels = output_config.channels as usize;

        // Setup output buffer (RTP -> Speaker)
        // Buffer size: 500ms for extra safety against jitter
        let rb = HeapRb::<i16>::new(output_config.sample_rate as usize / 2);
        let (prod, mut cons) = rb.split();

        {
            let mut tx = self.local_playback_tx.lock().await;
            *tx = Some(prod);
        }

        // Setup input (Mic -> RTP)
        let audio_source = self.audio_source.clone();
        let stats = self.stats.clone();
        let input_sample_rate = input_config.sample_rate;
        let input_channels = input_config.channels;

        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel::<Vec<f32>>(500);

        // Move stream creation to a separate thread to avoid Send issues with cpal::Stream on some platforms
        let (stream_stop_tx, stream_stop_rx) = std::sync::mpsc::channel();
        {
            let mut stop_tx = self.local_stop_tx.lock().await;
            *stop_tx = Some(stream_stop_tx.clone());
        }

        struct StreamGuard(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for StreamGuard {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        let mut _guard = StreamGuard(Some(stream_stop_tx.clone()));

        let (init_tx, init_rx) = tokio::sync::oneshot::channel::<Result<()>>();

        std::thread::spawn(move || {
            let mut is_playing = false;
            let pre_roll_samples = (output_config.sample_rate as usize / 10).max(480); // 100ms or at least 10ms frame

            let output_stream_res = output_device.build_output_stream(
                &output_config,
                move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                    if !is_playing {
                        if cons.occupied_len() >= pre_roll_samples {
                            is_playing = true;
                            tracing::debug!(
                                "Local playback started (buffer: {} samples)",
                                cons.occupied_len()
                            );
                        } else {
                            data.fill(0.0);
                            return;
                        }
                    }

                    if cons.occupied_len() == 0 {
                        is_playing = false;
                        tracing::debug!("Local playback underrun! Buffer empty.");
                        data.fill(0.0);
                        return;
                    }

                    for frame in data.chunks_mut(output_channels) {
                        match cons.try_pop() {
                            Some(sample) => {
                                let f_sample = sample as f32 / 32768.0;
                                for s in frame.iter_mut() {
                                    *s = f_sample;
                                }
                            }
                            None => {
                                // This shouldn't happen during chunk processing if occupied_len was > 0
                                for s in frame.iter_mut() {
                                    *s = 0.0;
                                }
                            }
                        }
                    }
                },
                |err| error!("Output stream error: {:?}", err),
                None,
            );

            let output_stream = match output_stream_res {
                Ok(s) => s,
                Err(e) => {
                    let _ = init_tx.send(Err(anyhow::anyhow!(
                        "Failed to build output stream: {:?}",
                        e
                    )));
                    return;
                }
            };

            let input_stream_res = input_device.build_input_stream(
                &input_config,
                move |data: &[f32], _: &cpal::InputCallbackInfo| {
                    let _ = input_tx.try_send(data.to_vec());
                },
                |err| error!("Input stream error: {:?}", err),
                None,
            );

            let input_stream = match input_stream_res {
                Ok(s) => s,
                Err(e) => {
                    let _ = init_tx.send(Err(anyhow::anyhow!(
                        "Failed to build input stream: {:?}",
                        e
                    )));
                    return;
                }
            };

            if let Err(e) = output_stream.play() {
                let _ = init_tx.send(Err(anyhow::anyhow!(
                    "Failed to play output stream: {:?}",
                    e
                )));
                return;
            }
            if let Err(e) = input_stream.play() {
                let _ = init_tx.send(Err(anyhow::anyhow!("Failed to play input stream: {:?}", e)));
                return;
            }

            let _ = init_tx.send(Ok(()));
            let _ = stream_stop_rx.recv();
        });

        init_rx
            .await
            .context("Audio initialization thread panicked or dropped")??;

        let child_token = self.cancel_token.child_token();

        // Handle existing transceivers
        self.setup_transceivers_for_recording(child_token.clone())
            .await;

        let session_input_clone = self.clone();
        let rx_task = self.spawn_track_event_handler(username.clone(), child_token.clone());

        let username_input = username.clone();
        let input_task = async move {
            let pt = session_input_clone
                .pc
                .get_transceivers()
                .first()
                .and_then(|t| t.sender().as_ref().map(|s| s.params().payload_type));
            let ct = get_codec_type(pt, &session_input_clone.pc.config().media_capabilities);

            let target_sample_rate = ct.samplerate();
            let target_channels =
                get_codec_channels(pt, &session_input_clone.pc.config().media_capabilities, ct);

            let mut encoder = create_encoder_for(ct, target_channels);

            let mut resampler = if input_sample_rate != target_sample_rate || input_channels != 1 {
                Some(audio_codec::Resampler::new(
                    input_sample_rate as usize,
                    target_sample_rate as usize,
                ))
            } else {
                None
            };

            let mut rtp_timestamp: u32 = random_u32();
            let samples_per_frame = (target_sample_rate / 50) as usize; // 20ms
            let mut input_buffer: Vec<i16> =
                Vec::with_capacity(samples_per_frame * target_channels as usize);

            while let Some(data) = input_rx.recv().await {
                // Convert f32 to i16 and mix down to mono if multiple mic channels
                // (Most mic inputs are mono or handled as mono here)
                let mut mono_samples: Vec<i16> =
                    Vec::with_capacity(data.len() / input_channels as usize);
                if input_channels == 1 {
                    for &s in &data {
                        // Clamp to avoid overflow noise
                        let sample = (s * 32767.0).clamp(-32768.0, 32767.0) as i16;
                        mono_samples.push(sample);
                    }
                } else {
                    for chunk in data.chunks(input_channels as usize) {
                        let avg: f32 = chunk.iter().sum::<f32>() / input_channels as f32;
                        let sample = (avg * 32767.0).clamp(-32768.0, 32767.0) as i16;
                        mono_samples.push(sample);
                    }
                }

                let resampled = if let Some(r) = resampler.as_mut() {
                    r.resample(&mono_samples)
                } else {
                    mono_samples
                };

                // Expand to negotiated channels if needed (Mono to Stereo)
                if target_channels == 2 {
                    for &s in &resampled {
                        input_buffer.push(s);
                        input_buffer.push(s);
                    }
                } else {
                    input_buffer.extend(resampled);
                }

                while input_buffer.len() >= samples_per_frame * target_channels as usize {
                    let frame: Vec<i16> = input_buffer
                        .drain(0..samples_per_frame * target_channels as usize)
                        .collect();
                    let encoded = encoder.encode(&frame);
                    if encoded.is_empty() {
                        continue;
                    }

                    stats.inc_tx(1, encoded.len() as u64);
                    let sample = MediaSample::Audio(AudioFrame {
                        rtp_timestamp,
                        data: encoded.into(),
                        clock_rate: ct.clock_rate(),
                        ..Default::default()
                    });
                    if let Err(e) = audio_source.send(sample).await {
                        error!("[{}] Failed to send mic sample: {:?}", username_input, e);
                        return;
                    }

                    let ticks = (samples_per_frame as u64 * ct.clock_rate() as u64
                        / target_sample_rate as u64) as u32;
                    rtp_timestamp = rtp_timestamp.wrapping_add(ticks);
                }
            }
        };

        tokio::pin!(rx_task);
        tokio::pin!(input_task);

        let mut sync_interval = tokio::time::interval(Duration::from_secs(1));
        let timeout_future = if let Some(secs) = timeout_secs {
            Some(tokio::time::sleep(Duration::from_secs(secs)))
        } else {
            None
        };

        if let Some(timeout) = timeout_future {
            tokio::pin!(timeout);
            loop {
                tokio::select! {
                    _ = sync_interval.tick() => {
                        self.sync_nack_stats();
                    }
                    _ = &mut rx_task => {
                        break;
                    }
                    _ = &mut input_task => {
                        break;
                    }
                    _ = &mut timeout => {
                        info!("[{}] Local audio timeout reached", username);
                        break;
                    }
                    _ = self.cancel_token.cancelled() => {
                        break;
                    }
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }
        } else {
            loop {
                tokio::select! {
                    _ = sync_interval.tick() => {
                        self.sync_nack_stats();
                    }
                    _ = &mut rx_task => {
                        break;
                    }
                    _ = &mut input_task => {
                        break;
                    }
                    _ = self.cancel_token.cancelled() => {
                        break;
                    }
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }
        }

        let _ = stream_stop_tx.send(());

        // Clear all local audio state to allow future calls to restart streams properly
        {
            let mut stop_tx = self.local_stop_tx.lock().await;
            *stop_tx = None;
        }
        {
            let mut playback_tx = self.local_playback_tx.lock().await;
            *playback_tx = None;
        }
        {
            let mut resampler = self.output_resampler.lock().await;
            *resampler = None;
        }

        Ok(())
    }

    pub async fn play_file(
        &self,
        username: String,
        file_path: &Path,
        recording_path: Option<&Path>,
        keep_alive: bool,
    ) -> Result<()> {
        info!("[{}] Playing file: {:?}", username, file_path);

        self.init_recorder(&username, recording_path).await;

        let mut reader = hound::WavReader::open(file_path).context("Failed to open WAV file")?;
        let spec = reader.spec();
        let raw_samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap_or(0)).collect();

        let pt = self
            .pc
            .get_transceivers()
            .first()
            .and_then(|t| t.sender().as_ref().map(|s| s.params().payload_type));
        let target_sample_rate =
            get_codec_type(pt, &self.pc.config().media_capabilities).samplerate();

        let samples = if spec.sample_rate != target_sample_rate || spec.channels != 1 {
            info!(
                "[{}] Resampling audio from {}Hz {}ch to {}Hz 1ch",
                username, spec.sample_rate, spec.channels, target_sample_rate
            );
            resample_audio(
                raw_samples,
                spec.sample_rate,
                target_sample_rate,
                spec.channels,
            )?
        } else {
            raw_samples
        };

        let child_token = self.cancel_token.child_token();

        // Handle existing transceivers
        self.setup_transceivers_for_recording(child_token.clone())
            .await;

        let rx_task = self.spawn_track_event_handler(username.clone(), child_token.clone());

        let play_fut = self.play_samples(username.clone(), samples);
        tokio::pin!(play_fut);
        tokio::pin!(rx_task);

        let mut play_done = false;
        let mut sync_interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = sync_interval.tick() => {
                    self.sync_nack_stats();
                }
                res = &mut play_fut, if !play_done => {
                    play_done = true;
                    if let Err(e) = res {
                        error!("[{}] Playback error: {:?}", username, e);
                    }
                    if !keep_alive {
                        return Ok(());
                    }
                    info!("[{}] Playback finished, keeping alive...", username);
                }
                _ = &mut rx_task => {
                    return Ok(())
                }
                _ = self.cancel_token.cancelled() => {
                    return Ok(())
                }
            }
        }
    }

    pub async fn play_wav_bytes(
        &self,
        username: String,
        wav_bytes: &[u8],
        recording_path: Option<&Path>,
        keep_alive: bool,
    ) -> Result<()> {
        info!("[{}] Playing embedded wav...", username);

        self.init_recorder(&username, recording_path).await;

        let cursor = Cursor::new(wav_bytes);
        let mut reader = hound::WavReader::new(cursor).context("Failed to read WAV bytes")?;
        let spec = reader.spec();
        let raw_samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap_or(0)).collect();

        let pt = self
            .pc
            .get_transceivers()
            .first()
            .and_then(|t| t.sender().as_ref().map(|s| s.params().payload_type));
        let target_sample_rate =
            get_codec_type(pt, &self.pc.config().media_capabilities).samplerate();

        let samples = if spec.sample_rate != target_sample_rate || spec.channels != 1 {
            info!(
                "[{}] Resampling audio from {}Hz {}ch to {}Hz 1ch",
                username, spec.sample_rate, spec.channels, target_sample_rate
            );
            resample_audio(
                raw_samples,
                spec.sample_rate,
                target_sample_rate,
                spec.channels,
            )?
        } else {
            raw_samples
        };

        let child_token = self.cancel_token.child_token();

        self.setup_transceivers_for_recording(child_token.clone())
            .await;

        let rx_task = self.spawn_track_event_handler(username.clone(), child_token.clone());

        let play_fut = self.play_samples(username.clone(), samples);
        tokio::pin!(play_fut);
        tokio::pin!(rx_task);

        let mut play_done = false;
        let mut sync_interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = sync_interval.tick() => {
                    self.sync_nack_stats();
                }
                res = &mut play_fut, if !play_done => {
                    play_done = true;
                    if let Err(e) = res {
                        error!("[{}] Playback error: {:?}", username, e);
                    }
                    if !keep_alive {
                        return Ok(());
                    }
                    info!("[{}] Playback finished, keeping alive...", username);
                }
                _ = &mut rx_task => {
                    return Ok(());
                }
                _ = self.cancel_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }

    async fn play_samples(&self, username: String, samples: Vec<i16>) -> Result<()> {
        let pt = self
            .pc
            .get_transceivers()
            .first()
            .and_then(|t| t.sender().as_ref().map(|s| s.params().payload_type));
        let ct = get_codec_type(pt, &self.pc.config().media_capabilities);

        let sample_rate = ct.samplerate();
        let clock_rate = ct.clock_rate();
        let channels = get_codec_channels(pt, &self.pc.config().media_capabilities, ct);
        let mut encoder = create_encoder_for(ct, channels);
        let payload_type = pt.unwrap_or(ct.payload_type());

        // 20ms frame size in samples per channel
        let chunk_size = (sample_rate / 50) as usize; // 20ms

        info!(
            "[{}] Playback started: {} samples using {:?} at {}Hz (clock {}Hz, pt={}, channels={}, chunk_size={})",
            username,
            samples.len(),
            ct,
            sample_rate,
            clock_rate,
            payload_type,
            channels,
            chunk_size
        );

        // Pre-process: Resample audio for recording (16000Hz target) if needed
        let record_samples = if sample_rate != 16000 {
            let mut resampler = Resampler::new(sample_rate as usize, 16000);
            resampler.resample(&samples)
        } else {
            samples.clone()
        };

        // Recording background task to avoid blocking the main loop
        let recorder_clone = self.recorder.clone();
        let (rec_tx, mut rec_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<i16>>();
        let rec_handle = tokio::spawn(async move {
            while let Some(data) = rec_rx.recv().await {
                let rec = recorder_clone.lock().await;
                if let Some(r) = rec.as_ref() {
                    r.record_tx(&data);
                }
            }
        });

        // Use ticker-based timing with Skip missed ticks behavior for precise RTP intervals
        let mut ticker = interval(Duration::from_millis(20));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let record_chunk_size = if sample_rate != 16000 {
            // Resample changes sample count: 20ms of samples at sample_rate becomes 20ms at 16000Hz
            (chunk_size as u32 * 16000 / sample_rate) as usize
        } else {
            chunk_size
        };

        let mut chunk_index: usize = 0;
        let num_chunks = (samples.len() + chunk_size - 1) / chunk_size;
        let num_record_chunks = (record_samples.len() + record_chunk_size - 1) / record_chunk_size;
        let mut sent_chunks = 0u64;
        let mut rtp_timestamp: u32 = random_u32();

        loop {
            ticker.tick().await;

            // Loop audio: wrap around when all chunks have been played
            if num_chunks == 0 {
                break;
            }
            let ci = chunk_index % num_chunks;
            let start = ci * chunk_size;
            let end = (start + chunk_size).min(samples.len());
            let chunk = &samples[start..end];

            let final_chunk = if chunk.len() == chunk_size {
                chunk.to_vec()
            } else {
                let mut v = chunk.to_vec();
                v.resize(chunk_size, 0);
                v
            };

            // Send pre-resampled chunk to recorder (non-blocking, also loops)
            if num_record_chunks > 0 {
                let ri = chunk_index % num_record_chunks;
                let rstart = ri * record_chunk_size;
                let rend = (rstart + record_chunk_size).min(record_samples.len());
                let _ = rec_tx.send(record_samples[rstart..rend].to_vec());
            }

            chunk_index += 1;

            // Expand mono source to interleaved stereo when negotiated codec uses 2 channels.
            let audio_to_encode = if channels == 2 {
                let mut stereo = Vec::with_capacity(final_chunk.len() * 2);
                for &s in &final_chunk {
                    stereo.push(s);
                    stereo.push(s);
                }
                stereo
            } else {
                final_chunk
            };

            // Debug: calculate input RMS
            let input_rms = if !audio_to_encode.is_empty() {
                let sum: f64 = audio_to_encode.iter().map(|&s| (s as f64).powi(2)).sum();
                (sum / audio_to_encode.len() as f64).sqrt()
            } else {
                0.0
            };

            // Encode and send
            let encoded = encoder.encode(&audio_to_encode);

            // Debug log every 50 frames (only in verbose mode)
            if sent_chunks % 50 == 0 {
                debug!(
                    "[{}] Audio encode: input_rms={:.2}, input_len={}, encoded_len={}",
                    username,
                    input_rms,
                    audio_to_encode.len(),
                    encoded.len()
                );
            }

            if encoded.is_empty() {
                continue;
            }

            self.stats.inc_tx(1, encoded.len() as u64);

            let frame = AudioFrame {
                data: Bytes::from(encoded),
                clock_rate,
                rtp_timestamp,
                payload_type: Some(payload_type),
                ..Default::default()
            };

            let ticks = (chunk_size as u64 * clock_rate as u64 / sample_rate as u64) as u32;
            rtp_timestamp = rtp_timestamp.wrapping_add(ticks);

            if let Err(e) = self.audio_source.send_audio(frame).await {
                error!("[{}] Failed to send audio: {:?}", username, e);
                break;
            }

            sent_chunks += 1;
            if sent_chunks % 500 == 0 {
                info!(
                    "[{}] Sent {} chunks (loop #{})",
                    username,
                    sent_chunks,
                    (chunk_index - 1) / num_chunks
                );
            }
        }

        drop(rec_tx);
        let _ = rec_handle.await;

        info!("[{}] Playback finished successfully", username);
        Ok(())
    }

    pub async fn start_echo(&self, username: String, _recording_path: Option<&Path>) -> Result<()> {
        info!("[{}] Starting echo service", username);

        // Handle existing transceivers
        let transceivers = self.pc.get_transceivers();
        info!(
            "[{}] Found {} existing transceivers for echo",
            username,
            transceivers.len()
        );
        for transceiver in transceivers {
            if let Some(receiver) = transceiver.receiver().as_ref() {
                let mid = transceiver
                    .mid()
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                if !self.try_track_echo_mid(&mid).await {
                    continue;
                }
                self.spawn_audio_loop(
                    username.clone(),
                    receiver.track(),
                    self.cancel_token.child_token(),
                );
            }
        }

        let session_rx = self.clone();
        let username_rx = username.clone();
        let cancel_token = self.cancel_token.clone();
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                event = self.pc.recv() => {
                    if let Some(event) = event {
                        match event {
                            PeerConnectionEvent::Track(transceiver) => {
                                let mid = transceiver
                                    .mid()
                                    .clone()
                                    .unwrap_or_else(|| "unknown".to_string());
                                if !session_rx.try_track_echo_mid(&mid).await {
                                    tracing::debug!(
                                        "[{}] Track {} already being echoed, skipping",
                                        username_rx,
                                        mid
                                    );
                                    continue;
                                }
                                if let Some(receiver) = transceiver.receiver().as_ref() {
                                    self.spawn_audio_loop(username_rx.clone(), receiver.track(), cancel_token.child_token());
                                }
                            }
                            _ => {
                                info!("[{}] Received PC event: Other", username_rx);
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
        self.cancel_token.cancel();
        self.pc.close();
    }

    pub fn sync_nack_stats(&self) {
        let mut total_sent = 0;
        let mut total_recv = 0;
        let mut total_recovered = 0;

        let transceivers = self.pc.get_transceivers();
        for transceiver in transceivers {
            if let Some(sender) = transceiver.sender() {
                if let Some(handler) = sender.nack_handler() {
                    total_recv += handler.get_nack_count();
                }
            }
            if let Some(receiver) = transceiver.receiver() {
                if let Some(handler) = receiver.nack_handler() {
                    total_sent += handler.get_nack_count();
                    total_recovered += handler.get_recovered_count();
                }
            }
        }

        if total_sent > 0 || total_recv > 0 || total_recovered > 0 {
            info!(
                "sync_nack_stats: total_sent={}, total_recv={}, total_recovered={}",
                total_sent, total_recv, total_recovered
            );
        }

        let last_sent = self
            .last_nack_sent
            .swap(total_sent, std::sync::atomic::Ordering::Relaxed);
        if total_sent > last_sent {
            self.stats.inc_nack_sent(total_sent - last_sent);
        }

        let last_recv = self
            .last_nack_recv
            .swap(total_recv, std::sync::atomic::Ordering::Relaxed);
        if total_recv > last_recv {
            self.stats.inc_nack_recv(total_recv - last_recv);
        }

        let last_recovered = self
            .last_nack_recovered
            .swap(total_recovered, std::sync::atomic::Ordering::Relaxed);
        if total_recovered > last_recovered {
            self.stats
                .inc_nack_recovered(total_recovered - last_recovered);
        }
    }
}

fn resample_audio(
    samples: Vec<i16>,
    source_rate: u32,
    target_rate: u32,
    channels: u16,
) -> Result<Vec<i16>> {
    if source_rate == target_rate && channels == 1 {
        return Ok(samples);
    }

    // Convert to f32 and mix down to mono with safe clamping
    let mut mono_samples: Vec<f32> = Vec::with_capacity(samples.len() / channels as usize);
    if channels == 1 {
        for s in samples {
            mono_samples.push(s as f32);
        }
    } else {
        for chunk in samples.chunks(channels as usize) {
            let sum: f32 = chunk.iter().map(|&s| s as f32).sum();
            mono_samples.push(sum / channels as f32);
        }
    }

    let samples_i16: Vec<i16> = mono_samples
        .into_iter()
        .map(|s| {
            if s > 32767.0 {
                32767
            } else if s < -32768.0 {
                -32768
            } else {
                s as i16
            }
        })
        .collect();

    if source_rate == target_rate {
        return Ok(samples_i16);
    }
    let buf = resample(&samples_i16, source_rate, target_rate);
    Ok(buf)
}

fn inject_telephone_event_sdp(sdp: &str, pt: u8) -> String {
    if sdp.to_lowercase().contains("telephone-event") {
        return sdp.to_string();
    }
    let pt_str = pt.to_string();
    let mut out = String::new();
    for line in sdp.lines() {
        if line.to_lowercase().starts_with("m=audio") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let mut new = parts[..3].join(" ");
                new.push_str(&format!(" {}", pt_str));
                for p in &parts[3..] {
                    new.push_str(&format!(" {}", p));
                }
                out.push_str(&new);
                out.push('\n');
            } else {
                out.push_str(line);
                out.push('\n');
            }
        } else if line.starts_with("a=rtpmap:") {
            out.push_str(line);
            out.push('\n');
            out.push_str(&dtmf::build_rtpmap_line(pt));
            out.push('\n');
            out.push_str(&dtmf::build_fmtp_line(pt));
            out.push('\n');
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn spawn_track_recorder(
    session: MediaSession,
    track: Arc<dyn MediaStreamTrack>,
    token: CancellationToken,
) {
    let recorder = session.recorder.clone();
    let stats = session.stats.clone();
    let jitter_buffer_enabled = session.jitter_buffer_enabled;
    let telephone_event_pt = session.telephone_event_pt.load(Ordering::Relaxed);
    let audio_quality_enabled = session.audio_quality_enabled;
    let audio_quality_config = session.audio_quality_config.clone();
    #[cfg(feature = "local-device")]
    let local_playback_tx = session.local_playback_tx.clone();
    #[cfg(feature = "local-device")]
    let output_sample_rate = session.output_sample_rate.clone();
    #[cfg(feature = "local-device")]
    let output_resampler = session.output_resampler.clone();
    tokio::spawn(async move {
        let mut decoder: Option<Box<dyn Decoder + Send>> = None;
        let mut current_pt: Option<u8> = None;
        let mut recorder_resampler: Option<Resampler> = None;
        let mut aq = if audio_quality_enabled {
            audio_quality_config.map(AudioQualityAnalyzer::new)
        } else {
            None
        };
        let te_pt = if telephone_event_pt > 0 {
            Some(telephone_event_pt)
        } else {
            None
        };

        let mut last_seq: Option<u16> = None;
        let mut last_timestamp: Option<u32> = None;

        if jitter_buffer_enabled {
            use rustrtc::media::JitterBuffer;
            let mut jb =
                JitterBuffer::new(Duration::from_millis(20), Duration::from_millis(200), 100);
            loop {
                let wait = jb.next_pop_wait().unwrap_or(Duration::from_millis(100));
                tokio::select! {
                    _ = token.cancelled() => {
                        info!("RX task cancelled");
                        break;
                    },
                    res = track.recv() => {
                        match res {
                            Ok(sample) => {
                                jb.push(sample);
                            }
                            Err(_) => {
                                break;
                            },
                        }
                    }
                    _ = tokio::time::sleep(wait) => {
                        while let Some(mut sample) = jb.pop() {
                            let is_te = if let MediaSample::Audio(ref frame) = sample {
                                te_pt.is_some() && frame.payload_type == te_pt
                            } else {
                                false
                            };
                            if is_te {
                                if let MediaSample::Audio(ref frame) = sample {
                                    if frame.data.len() >= 4 {
                                        if let Some(ev) = dtmf::decode_dtmf(&frame.data) {
                                            if let Some(digit) = dtmf::event_to_digit(ev.event) {
                                                info!("[RX] DTMF (jb): digit='{}' event={} end={}", digit, ev.event, ev.end);
                                                stats.inc_rx_dtmf();
                                            }
                                        }
                                    }
                                }
                                continue;
                            }
                            if let MediaSample::Audio(ref frame) = sample {
                                if current_pt != frame.payload_type {
                                    current_pt = frame.payload_type;
                                    if let Some(pt) = current_pt {
                                        #[cfg(feature = "local-device")]
                                        {
                                            let mut res_lock = output_resampler.lock().await;
                                            *res_lock = None;
                                        }
                                        let ct = get_codec_type(Some(pt), &session.pc.config().media_capabilities);
                                        let d = audio_codec::create_decoder(ct);
                                        decoder = Some(d);
                                        let rate = ct.samplerate();
                                        if rate != 16000 {
                                            recorder_resampler = Some(Resampler::new(rate as usize, 16000));
                                        } else {
                                            recorder_resampler = None;
                                        }
                                    }
                                }
                            }

                                if let Some(ref mut dec) = decoder {
                                    let ct = if let MediaSample::Audio(ref mut frame) = sample {
                                        let ct = get_codec_type(frame.payload_type, &session.pc.config().media_capabilities);
                                        frame.clock_rate = ct.clock_rate();
                                        ct
                                    } else {
                                        CodecType::PCMU
                                    };

                                    let rtp_clock_rate = ct.clock_rate();
                                    let channels = ct.channels();

                                    #[cfg(feature = "local-device")]
                                    let mut resampler_lock = output_resampler.lock().await;
                                    process_recorded_sample(
                                        sample,
                                        &recorder,
                                        &stats,
                                        dec,
                                        &mut recorder_resampler,
                                        &mut last_seq,
                                        &mut last_timestamp,
                                        #[cfg(feature = "local-device")]
                                        &local_playback_tx,
                                        #[cfg(feature = "local-device")]
                                        &output_sample_rate,
                                        #[cfg(feature = "local-device")]
                                        &mut *resampler_lock,
                                        rtp_clock_rate,
                                        channels,
                                        ct.samplerate(),
                                        te_pt,
                                        aq.as_mut(),
                                    )
                                    .await;
                                }
                        }
                    }
                }
            }
        } else {
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        info!("RX task cancelled");
                        break;
                    },
                    res = track.recv() => {
                        match res {
                            Ok(mut sample) => {
                                let is_te = if let MediaSample::Audio(ref frame) = sample {
                                    te_pt.is_some() && frame.payload_type == te_pt
                                } else {
                                    false
                                };
                                if is_te {
                                    if let MediaSample::Audio(ref frame) = sample {
                                        if frame.data.len() >= 4 {
                                            if let Some(ev) = dtmf::decode_dtmf(&frame.data) {
                                                if let Some(digit) = dtmf::event_to_digit(ev.event) {
                                                    info!("[RX] DTMF: digit='{}' event={} end={}", digit, ev.event, ev.end);
                                                    stats.inc_rx_dtmf();
                                                }
                                            }
                                        }
                                    }
                                    continue;
                                }
                                if let MediaSample::Audio(ref frame) = sample {
                                    if current_pt != frame.payload_type {
                                        current_pt = frame.payload_type;
                                        if let Some(pt) = current_pt {
                                            #[cfg(feature = "local-device")]
                                            {
                                                let mut res_lock = output_resampler.lock().await;
                                                *res_lock = None;
                                            }
                                            let ct = get_codec_type(Some(pt), &session.pc.config().media_capabilities);
                                            let d = audio_codec::create_decoder(ct);
                                            decoder = Some(d);
                                            let rate = ct.samplerate();
                                            if rate != 16000 {
                                                recorder_resampler = Some(Resampler::new(rate as usize, 16000));
                                            } else {
                                                recorder_resampler = None;
                                            }
                                        }
                                    }
                                }

                                if let Some(ref mut dec) = decoder {
                                    let ct = if let MediaSample::Audio(ref mut frame) = sample {
                                        let ct = get_codec_type(frame.payload_type, &session.pc.config().media_capabilities);
                                        frame.clock_rate = ct.clock_rate();
                                        ct
                                    } else {
                                        CodecType::PCMU
                                    };

                                    let rtp_clock_rate = ct.clock_rate();
                                    let channels = ct.channels();

                                    #[cfg(feature = "local-device")]
                                    let mut resampler_lock = output_resampler.lock().await;
                                    process_recorded_sample(
                                        sample,
                                        &recorder,
                                        &stats,
                                        dec,
                                        &mut recorder_resampler,
                                        &mut last_seq,
                                        &mut last_timestamp,
                                        #[cfg(feature = "local-device")]
                                        &local_playback_tx,
                                        #[cfg(feature = "local-device")]
                                        &output_sample_rate,
                                        #[cfg(feature = "local-device")]
                                        &mut *resampler_lock,
                                        rtp_clock_rate,
                                        channels,
                                        ct.samplerate(),
                                        te_pt,
                                        aq.as_mut(),
                                    )
                                    .await;
                                }
                            }
                            Err(_) => {
                                break;
                            },
                        }
                    }
                }
            }
        }
    });
}

async fn process_recorded_sample(
    sample: MediaSample,
    recorder: &Mutex<Option<Recorder>>,
    stats: &CallStats,
    decoder: &mut Box<dyn Decoder + Send>,
    recorder_resampler: &mut Option<Resampler>,
    last_seq: &mut Option<u16>,
    last_timestamp: &mut Option<u32>,
    #[cfg(feature = "local-device")] local_playback_tx: &Mutex<Option<ringbuf::HeapProd<i16>>>,
    #[cfg(feature = "local-device")] output_sample_rate: &std::sync::atomic::AtomicU32,
    #[cfg(feature = "local-device")] output_resampler: &mut Option<audio_codec::Resampler>,
    rtp_clock_rate: u32,
    #[allow(unused_variables)] channels: u16,
    actual_sample_rate: u32,
    _telephone_event_pt: Option<u8>,
    audio_quality: Option<&mut AudioQualityAnalyzer>,
) {
    let decoded = if let MediaSample::Audio(ref frame) = sample {
        let decoded_data = decoder.decode(&frame.data);
        // Debug: calculate output RMS every 50 packets
        static DEBUG_COUNTER: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let count = DEBUG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 50 == 0 {
            let rms = if !decoded_data.is_empty() {
                let sum: f64 = decoded_data.iter().map(|&s| (s as f64).powi(2)).sum();
                (sum / decoded_data.len() as f64).sqrt()
            } else {
                0.0
            };
            tracing::debug!(
                "[RX] Audio decode: packet_len={}, decoded_len={}, rms={:.2}",
                frame.data.len(),
                decoded_data.len(),
                rms
            );
        }
        Some(decoded_data)
    } else {
        None
    };

    if let MediaSample::Audio(frame) = &sample {
        if let Some(seq) = frame.sequence_number {
            if let Some(last) = *last_seq {
                let expected = last.wrapping_add(1);
                if seq != expected {
                    let diff = seq.wrapping_sub(last) as i16;

                    if diff > 1 {
                        tracing::warn!(
                            "Sequence gap detected: last={} current={} gap={}",
                            last,
                            seq,
                            diff - 1
                        );
                        stats.inc_rx_lost((diff - 1) as u64);
                        *last_seq = Some(seq);
                    } else if diff < 0 {
                        tracing::debug!("Out of order packet: last={} current={}", last, seq);
                    }
                } else {
                    *last_seq = Some(seq);
                }
            } else {
                *last_seq = Some(seq);
            }
        }

        // Update last timestamp
        *last_timestamp = Some(frame.rtp_timestamp);

        stats.inc_rx(1, frame.data.len() as u64);
        let decoded = decoded.as_ref().unwrap();

        // Audio quality analysis
        if let Some(aq) = audio_quality {
            let r = aq.analyze_frame(
                decoded,
                frame.rtp_timestamp,
                rtp_clock_rate,
                actual_sample_rate,
                20,
            );
            stats.add_audio_quality_stats(
                if r.sample_count_mismatch { 1 } else { 0 },
                if r.is_shrill { 1 } else { 0 },
                if r.is_muffled { 1 } else { 0 },
                if r.clipping_ratio > 0.01 { 1 } else { 0 },
                if r.is_silence { 1 } else { 0 },
                1,
            );
        }

        if frame.sequence_number.unwrap_or(0) % 100 == 0 {
            tracing::debug!(
                "RX Audio: seq={:?} pt={} rate={} ticks={} decoded_len={} data_len={}",
                frame.sequence_number,
                frame.payload_type.unwrap_or(0),
                actual_sample_rate,
                rtp_clock_rate,
                decoded.len(),
                frame.data.len()
            );
        }

        if let Ok(rec) = recorder.try_lock() {
            if let Some(r) = rec.as_ref() {
                let resampled = if let Some(resampler) = recorder_resampler {
                    resampler.resample(&decoded)
                } else {
                    decoded.clone()
                };
                r.record_rx(&resampled);
            }
        }

        #[cfg(feature = "local-device")]
        {
            let mut tx = local_playback_tx.lock().await;
            if let Some(prod) = tx.as_mut() {
                let target_rate = output_sample_rate.load(std::sync::atomic::Ordering::Relaxed);
                if target_rate > 0 {
                    let mono_decoded = if channels == 2
                        && decoded.len() % 2 == 0
                        && decoded.len() > (actual_sample_rate as usize / 50)
                    {
                        let mut mono = Vec::with_capacity(decoded.len() / 2);
                        for chunk in decoded.chunks(2) {
                            let sum: i32 = chunk.iter().map(|&s| s as i32).sum();
                            mono.push((sum / 2) as i16);
                        }
                        mono
                    } else {
                        decoded.clone()
                    };

                    if target_rate != actual_sample_rate {
                        let need_new = match output_resampler.as_ref() {
                            Some(_) => false,
                            None => true,
                        };

                        if need_new {
                            *output_resampler = Some(audio_codec::Resampler::new(
                                actual_sample_rate as usize,
                                target_rate as usize,
                            ));
                        }

                        if let Some(resampler) = output_resampler.as_mut() {
                            let resampled = resampler.resample(&mono_decoded);
                            let capacity = prod.capacity();
                            let occupied = prod.occupied_len();
                            let pushed = prod.push_slice(&resampled);

                            if pushed < resampled.len() {
                                tracing::trace!(
                                    "Local playback buffer full! ({} / {}), dropped {} output samples",
                                    occupied,
                                    capacity,
                                    resampled.len() - pushed
                                );
                            }
                        }
                    } else {
                        if output_resampler.is_some() {
                            *output_resampler = None;
                        }
                        let capacity = prod.capacity();
                        let occupied = prod.occupied_len();
                        let pushed = prod.push_slice(&mono_decoded);

                        if pushed < mono_decoded.len() {
                            tracing::warn!(
                                "Local playback buffer full! ({} / {}), dropped {} output samples",
                                occupied,
                                capacity,
                                mono_decoded.len() - pushed
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resample_audio_identity() {
        let samples = vec![100, 200, 300, 400];
        let result = resample_audio(samples.clone(), 8000, 8000, 1).unwrap();
        assert_eq!(result, samples);
    }

    #[test]
    fn test_resample_audio_stereo_to_mono() {
        // 8000Hz stereo -> 8000Hz mono
        // Left: 100, Right: 200 -> Avg: 150
        let samples = vec![100, 200, 300, 500];
        let result = resample_audio(samples, 8000, 8000, 2).unwrap();
        assert_eq!(result, vec![150, 400]);
    }

    #[test]
    fn test_resample_audio_resampling() {
        // 16000Hz mono -> 8000Hz mono
        // Just checking it produces output of roughly half size
        let samples: Vec<i16> = (0..1600).map(|i| (i % 1000) as i16).collect();
        let result = resample_audio(samples.clone(), 16000, 8000, 1).unwrap();

        // 1600 samples at 16k is 0.1s
        // 0.1s at 8k is 800 samples
        // The resampler works in chunks, so exact size might vary slightly due to padding/buffering
        // but should be close.
        assert!(result.len() >= 800);
        assert!(result.len() < 1200); // Allow some padding overhead
    }

    #[test]
    fn test_get_audio_caps_default() {
        let caps = get_audio_caps(&None, false);
        // Now defaults to all supported codecs
        assert!(caps.len() >= 4);
        assert_eq!(caps[0].codec_name, "PCMU");
    }

    #[test]
    fn test_get_audio_caps_multiple() {
        let codecs = Some(vec![
            "opus".to_string(),
            "g722".to_string(),
            "pcmu".to_string(),
        ]);
        let caps = get_audio_caps(&codecs, false);
        assert_eq!(caps.len(), 3);
        assert_eq!(caps[0].codec_name, "opus");
        assert_eq!(caps[1].codec_name, "G722");
        assert_eq!(caps[2].codec_name, "PCMU");
    }

    #[tokio::test]
    async fn test_media_session_offer() {
        let stats = Arc::new(CallStats::new());
        let codecs = Some(vec!["opus".to_string(), "pcmu".to_string()]);
        let (_session, sdp) = MediaSession::new_offer(
            false,
            false,
            false,
            None,
            codecs.clone(),
            true,
            stats.clone(),
            None,
        )
        .await
        .unwrap();
        assert!(sdp.contains("m=audio"));
        assert!(sdp.contains("a=sendrecv")); // We set direction to SendRecv
        assert!(sdp.to_lowercase().contains("opus"));
        assert!(sdp.to_lowercase().contains("pcmu"));

        // Check if we can create an answer session
        let (_answer_session, answer_sdp, _) =
            MediaSession::new(&sdp, false, false, false, None, codecs, stats, None)
                .await
                .unwrap();
        assert!(answer_sdp.contains("m=audio"));
        assert!(answer_sdp.contains("a=sendrecv"));
    }

    #[tokio::test]
    async fn test_media_session_negotiation_g729() {
        let stats = Arc::new(CallStats::new());

        // Create an offer with G.729
        let offer_codecs = Some(vec!["g729".to_string()]);
        let (_offerer, offer_sdp) = MediaSession::new_offer(
            false,
            false,
            false,
            None,
            offer_codecs,
            true,
            stats.clone(),
            None,
        )
        .await
        .unwrap();

        assert!(offer_sdp.contains("G729"));

        // Answer without specifying codecs (should support all by default now)
        let (_answerer, answer_sdp, _) = MediaSession::new(
            &offer_sdp,
            false,
            false,
            false,
            None,
            None,
            stats.clone(),
            None,
        )
        .await
        .unwrap();

        // The answer should contain G729 because it was in the offer and we support it by default
        assert!(
            answer_sdp.contains("G729"),
            "Answer SDP should contain G729. SDP: {}",
            answer_sdp
        );
        // The answer should NOT contain PCMU because it was not in the offer
        assert!(
            !answer_sdp.contains("PCMU"),
            "Answer SDP should NOT contain PCMU if not offered. SDP: {}",
            answer_sdp
        );
    }

    #[tokio::test]
    async fn test_echo_tracking_independent_from_recorder_tracking() {
        let stats = Arc::new(CallStats::new());
        let (session, _sdp) =
            MediaSession::new_offer(false, false, false, None, None, false, stats, None)
                .await
                .unwrap();

        assert!(session.try_track_record_mid("audio-0").await);
        assert!(!session.try_track_record_mid("audio-0").await);

        // Echo tracking must be independent from recorder tracking.
        assert!(session.try_track_echo_mid("audio-0").await);
        assert!(!session.try_track_echo_mid("audio-0").await);
    }

    #[test]
    fn test_inject_telephone_event_sdp_adds_rtpmap() {
        let sdp = "v=0\nm=audio 4000 RTP/AVP 0 8\nc=IN IP4 127.0.0.1\na=rtpmap:0 PCMU/8000\n";
        let result = inject_telephone_event_sdp(sdp, 101);
        assert!(result.contains("telephone-event"));
        assert!(result.contains("a=rtpmap:101 telephone-event/8000"));
        assert!(result.contains("a=fmtp:101 0-16"));
    }

    #[test]
    fn test_inject_telephone_event_sdp_no_duplicate() {
        let sdp = "v=0\nm=audio 4000 RTP/AVP 0 101\nc=IN IP4 127.0.0.1\na=rtpmap:0 PCMU/8000\na=rtpmap:101 telephone-event/8000\n";
        let result = inject_telephone_event_sdp(sdp, 101);
        // Count occurrences of telephone-event in result (should be exactly 1)
        let count = result.matches("telephone-event").count();
        assert_eq!(
            count, 1,
            "telephone-event should not be duplicated: {}",
            result
        );
    }

    #[tokio::test]
    async fn test_media_session_offer_contains_telephone_event() {
        let stats = Arc::new(CallStats::new());
        let codecs = Some(vec!["pcmu".to_string()]);
        let (_session, sdp) =
            MediaSession::new_offer(false, false, false, None, codecs, true, stats, None)
                .await
                .unwrap();
        // The SDP should contain telephone-event capability
        assert!(
            sdp.to_lowercase().contains("telephone-event"),
            "Offer SDP should contain telephone-event. SDP: {}",
            sdp
        );
    }

    #[tokio::test]
    async fn test_media_session_answer_contains_telephone_event() {
        let stats = Arc::new(CallStats::new());
        let offer_codecs = Some(vec!["pcmu".to_string()]);
        let (_offerer, offer_sdp) = MediaSession::new_offer(
            false,
            false,
            false,
            None,
            offer_codecs,
            true,
            stats.clone(),
            None,
        )
        .await
        .unwrap();
        let (_answerer, answer_sdp, _) =
            MediaSession::new(&offer_sdp, false, false, false, None, None, stats, None)
                .await
                .unwrap();
        assert!(
            answer_sdp.to_lowercase().contains("telephone-event"),
            "Answer SDP should contain telephone-event. SDP: {}",
            answer_sdp
        );
    }

    #[test]
    fn test_audio_quality_analyzer_flow() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig {
            enabled: true,
            sample_rate_check: true,
            clipping_threshold: 0.95,
            shrill_threshold: 0.65,
            muffled_threshold: 0.15,
            silence_threshold_rms: 50.0,
            report_interval: 100,
        });
        // Analyze a frame with a simple sine wave
        let pcm: Vec<i16> = (0..160)
            .map(|i| {
                let t = i as f64 / 8000.0;
                (std::f64::consts::TAU * 440.0 * t)
                    .sin()
                    .mul_add(8000.0, 0.0) as i16
            })
            .collect();
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(!r.is_silence);
        assert!(r.rms > 100.0);
        assert_eq!(r.actual_samples, 160);
        let s = aq.summary();
        assert!(s.contains("frames=1"));
    }

    #[test]
    fn test_call_stats_with_audio_quality() {
        let stats = CallStats::new();
        stats.add_audio_quality_stats(1, 2, 3, 4, 5, 100);
        assert_eq!(
            stats
                .audio_quality_sample_rate_mismatches
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            stats
                .audio_quality_shrill_count
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            stats
                .audio_quality_muffled_count
                .load(std::sync::atomic::Ordering::Relaxed),
            3
        );
        assert_eq!(
            stats
                .audio_quality_clipping_frames
                .load(std::sync::atomic::Ordering::Relaxed),
            4
        );
        assert_eq!(
            stats
                .audio_quality_silence_frames
                .load(std::sync::atomic::Ordering::Relaxed),
            5
        );
        assert_eq!(
            stats
                .audio_quality_total_frames
                .load(std::sync::atomic::Ordering::Relaxed),
            100
        );
    }

    #[test]
    fn test_dtmf_stats_counters() {
        let stats = CallStats::new();
        stats.inc_rx_dtmf();
        stats.inc_rx_dtmf();
        stats.inc_tx_dtmf();
        assert_eq!(
            stats
                .rx_dtmf_events
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            stats
                .tx_dtmf_events
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}
