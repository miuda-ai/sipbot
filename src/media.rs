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
use rustrtc::sdp::{SdpType, SessionDescription};
use rustrtc::transports::ice::stun::random_u32;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

fn codec_from_name(name: &str) -> Option<CodecType> {
    match name.to_lowercase().as_str() {
        "pcmu" => Some(CodecType::PCMU),
        "pcma" => Some(CodecType::PCMA),
        "g722" => Some(CodecType::G722),
        "g729" => Some(CodecType::G729),
        #[cfg(feature = "opus")]
        "opus" => Some(CodecType::Opus),
        _ => None,
    }
}

fn get_codec_type(pt: Option<u8>, caps: &Option<MediaCapabilities>) -> CodecType {
    pt.and_then(|p| {
        caps.as_ref()
            .and_then(|c| c.audio.iter().find(|a| a.payload_type == p))
            .and_then(|a| codec_from_name(&a.codec_name))
    })
    .unwrap_or_else(|| match pt {
        Some(0) => CodecType::PCMU,
        Some(8) => CodecType::PCMA,
        Some(9) => CodecType::G722,
        Some(18) => CodecType::G729,
        #[cfg(feature = "opus")]
        Some(111) => CodecType::Opus,
        _ => CodecType::PCMU,
    })
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
            #[cfg(feature = "opus")]
            "opus".to_string(),
        ]
    };

    for codec in codec_list {
        let mut cap = match codec.to_lowercase().as_str() {
            "pcmu" => AudioCapability::pcmu(),
            "pcma" => AudioCapability::pcma(),
            #[cfg(feature = "opus")]
            "opus" => AudioCapability::opus(),
            _ => {
                if let Some(ct) = codec_from_name(&codec) {
                    AudioCapability {
                        payload_type: match ct {
                            CodecType::PCMU => 0,
                            CodecType::PCMA => 8,
                            CodecType::G722 => 9,
                            CodecType::G729 => 18,
                            #[cfg(feature = "opus")]
                            CodecType::Opus => 111,
                            _ => 0,
                        },
                        codec_name: format!("{:?}", ct).to_uppercase(),
                        clock_rate: if ct == CodecType::G722 {
                            16000
                        } else {
                            ct.clock_rate()
                        },
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
    tracked_mids: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl MediaSession {
    pub async fn new(
        remote_sdp: &str,
        srtp_enabled: bool,
        nack_enabled: bool,
        jitter_buffer_enabled: bool,
        external_ip: Option<String>,
        codecs: Option<Vec<String>>,
        stats: Arc<CallStats>,
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
        });

        let pc = Arc::new(PeerConnection::new(config.clone()));
        let ssrc_id = random_u32();
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 1000);
        let audio_source = Arc::new(source);

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));

        let remote_desc = SessionDescription::parse(SdpType::Offer, remote_sdp)?;
        pc.set_remote_description(remote_desc).await?;

        // Attach track to the active transceiver
        let transceivers = pc.get_transceivers();
        if let Some(t) = transceivers.first() {
            let params = t
                .sender()
                .as_ref()
                .map(|s| s.params())
                .unwrap_or(RtpCodecParameters {
                    payload_type: 0,
                    clock_rate: 8000,
                    channels: 1,
                });

            let mut builder = RtpSenderBuilder::new(track.clone(), ssrc_id)
                .stream_id("audio".to_string())
                .params(params);
            if nack_enabled {
                builder = builder
                    .nack(pc.config().nack_buffer_size)
                    .bitrate_controller();
            }
            let sender = builder.build();
            t.set_sender(Some(sender));
            t.set_direction(TransceiverDirection::SendRecv);
        } else {
            // This shouldn't happen if we set remote description with audio
            let params = RtpCodecParameters {
                payload_type: 0,
                clock_rate: 8000,
                channels: 1,
            };
            pc.add_track(track.clone(), params)?;
        }

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
            tracked_mids: Arc::new(Mutex::new(std::collections::HashSet::new())),
        };

        // Spawn a background task to listen for incoming track events so there is
        // always a listener for incoming RTP packets (prevents "No listener found")
        let bg_session = session.clone();
        tokio::spawn(async move {
            let pc = bg_session.pc.clone();
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    let mid = transceiver
                        .mid()
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    {
                        let mut mids = bg_session.tracked_mids.lock().await;
                        if mids.contains(&mid) {
                            continue;
                        }
                        mids.insert(mid);
                    }

                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            bg_session.clone(),
                            receiver.track(),
                            CancellationToken::new(),
                        );
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
                {
                    let mut mids = session.tracked_mids.lock().await;
                    if mids.contains(&mid) {
                        continue;
                    }
                    mids.insert(mid);
                }
                spawn_track_recorder(session.clone(), receiver.track(), CancellationToken::new());
            }
        }

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

        Ok((
            Self {
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
                tracked_mids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            },
            local_sdp,
        ))
    }

    pub async fn set_remote_answer(&self, remote_sdp: &str) -> Result<String> {
        let remote_desc = SessionDescription::parse(SdpType::Answer, remote_sdp)?;
        self.pc.set_remote_description(remote_desc).await?;

        let mut codec_name = "Unknown".to_string();
        if let Some(mline) = remote_sdp
            .lines()
            .find(|l| l.to_lowercase().starts_with("m=audio"))
        {
            if let Some(pt_str) = mline.split_whitespace().nth(3) {
                if let Ok(pt) = pt_str.parse::<u8>() {
                    let rtpmap_prefix = format!("a=rtpmap:{} ", pt);
                    if let Some(rtpmap) = remote_sdp.lines().find(|l| l.starts_with(&rtpmap_prefix))
                    {
                        if let Some(spec) = rtpmap.split_whitespace().nth(1) {
                            codec_name = spec.split('/').next().unwrap_or("Unknown").to_uppercase();
                        }
                    } else {
                        codec_name = match pt {
                            0 => "PCMU",
                            8 => "PCMA",
                            9 => "G722",
                            18 => "G729",
                            _ => "Unknown",
                        }
                        .to_string();
                    }

                    // Special case for Opus if it wasn't found in rtpmap via simple string match or handled by static map
                    // (Though Opus should always have rtpmap 111 or similar)
                    #[cfg(feature = "opus")]
                    if codec_name == "Unknown" && pt == 111 {
                        codec_name = "OPUS".to_string();
                    }
                }
            }
        }
        Ok(codec_name)
    }

    fn spawn_audio_loop(&self, username: String, track: Arc<dyn MediaStreamTrack>) {
        let audio_source = self.audio_source.clone();
        let recorder = self.recorder.clone();
        let stats = self.stats.clone();
        let jitter_buffer_enabled = self.jitter_buffer_enabled;
        let session = self.clone();

        tokio::spawn(async move {
            let mut decoder: Option<Box<dyn Decoder + Send>> = None;
            let mut current_pt: Option<u8> = None;
            let mut recorder_resampler: Option<Resampler> = None;

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
                        _ = sync_interval.tick() => {
                            session.sync_nack_stats();
                        }
                        res = track.recv() => {
                            match res {
                                Ok(mut sample) => {
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
    ) {
        let decoded = if let MediaSample::Audio(ref frame) = sample {
            Some(decoder.decode(&frame.data))
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

                let rec = recorder.lock().await;
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
            } else {
                tracing::error!("RX SAMPLE: NOT AUDIO");
            }
        }

        // Echo
        if let Err(e) = audio_source.send(sample).await {
            tracing::error!("[{}] Failed to send echo sample: {:?}", username, e);
        }
    }

    #[cfg(feature = "local-device")]
    pub async fn play_local_device(
        &self,
        username: String,
        recording_path: Option<&Path>,
        _keep_alive: bool,
    ) -> Result<()> {
        info!(
            "[{}] Using local audio device for playback and capture",
            username
        );

        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(username.clone(), path.to_path_buf()));
        }

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
                        tracing::warn!("Local playback underrun! Buffer empty.");
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

        let pc = self.pc.clone();
        let cancel_token = CancellationToken::new();
        let child_token = cancel_token.clone();

        // Handle existing transceivers
        let transceivers = pc.get_transceivers();
        for transceiver in transceivers {
            if let Some(receiver) = transceiver.receiver().as_ref() {
                let mid = transceiver
                    .mid()
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                {
                    let mut mids = self.tracked_mids.lock().await;
                    if mids.contains(&mid) {
                        continue;
                    }
                    mids.insert(mid);
                }
                spawn_track_recorder(self.clone(), receiver.track(), child_token.clone());
            }
        }

        let username_rx = username.clone();
        let session_rx = self.clone();
        let session_input_clone = self.clone();
        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    let mid = transceiver
                        .mid()
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    {
                        let mut mids = session_rx.tracked_mids.lock().await;
                        if mids.contains(&mid) {
                            tracing::debug!(
                                "[{}] Track {} already being recorded, skipping",
                                username_rx,
                                mid
                            );
                            continue;
                        }
                        mids.insert(mid);
                    }

                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            session_rx.clone(),
                            receiver.track(),
                            child_token.clone(),
                        );
                    }
                }
            }
        };

        let username_input = username.clone();
        let input_task = async move {
            let pt = session_input_clone
                .pc
                .get_transceivers()
                .first()
                .and_then(|t| t.sender().as_ref().map(|s| s.params().payload_type));
            let ct = get_codec_type(pt, &session_input_clone.pc.config().media_capabilities);

            let target_sample_rate = ct.samplerate();
            let target_channels = ct.channels();

            let mut encoder = audio_codec::create_encoder(ct);

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
                _ = tokio::signal::ctrl_c() => {
                    break;
                }
            }
        }

        let _ = stream_stop_tx.send(());
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

        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(username.clone(), path.to_path_buf()));
        }

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

        let pc = self.pc.clone();
        let cancel_token = CancellationToken::new();
        let child_token = cancel_token.clone();

        // Handle existing transceivers
        let transceivers = pc.get_transceivers();
        info!("[{}] Found {} transceivers", username, transceivers.len());
        for transceiver in transceivers {
            if let Some(receiver) = transceiver.receiver().as_ref() {
                info!(
                    "[{}] Transceiver has receiver, track id={}",
                    username,
                    receiver.track().id()
                );
                let mid = transceiver
                    .mid()
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                {
                    let mut mids = self.tracked_mids.lock().await;
                    if mids.contains(&mid) {
                        continue;
                    }
                    mids.insert(mid);
                }
                spawn_track_recorder(self.clone(), receiver.track(), child_token.clone());
            } else {
                info!("[{}] Transceiver has NO receiver", username);
            }
        }

        let username_rx = username.clone();
        let session_rx = self.clone();
        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    info!("[{}] Received PC event: Track", username_rx);
                    let mid = transceiver
                        .mid()
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    {
                        let mut mids = session_rx.tracked_mids.lock().await;
                        if mids.contains(&mid) {
                            tracing::debug!(
                                "[{}] Track {} already being recorded, skipping",
                                username_rx,
                                mid
                            );
                            continue;
                        }
                        mids.insert(mid);
                    }
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            session_rx.clone(),
                            receiver.track(),
                            child_token.clone(),
                        );
                    }
                }
            }
        };

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

        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(username.clone(), path.to_path_buf()));
        }

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

        let pc = self.pc.clone();
        let _username_rx = username.clone();
        let cancel_token = CancellationToken::new();
        let child_token = cancel_token.clone();

        // Handle existing transceivers
        let transceivers = pc.get_transceivers();
        info!("[{}] Found {} transceivers", username, transceivers.len());
        for (i, transceiver) in transceivers.iter().enumerate() {
            info!(
                "[{}] Transceiver {}: mid={:?} direction={:?}",
                username,
                i,
                transceiver.mid(),
                transceiver.direction()
            );
            if let Some(receiver) = transceiver.receiver().as_ref() {
                info!(
                    "[{}] Transceiver {} has receiver, track id={}",
                    username,
                    i,
                    receiver.track().id()
                );
                let mid = transceiver
                    .mid()
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                {
                    let mut mids = self.tracked_mids.lock().await;
                    if mids.contains(&mid) {
                        continue;
                    }
                    mids.insert(mid);
                }
                spawn_track_recorder(self.clone(), receiver.track(), child_token.clone());
            } else {
                info!("[{}] Transceiver {} has NO receiver", username, i);
            }
        }

        let session_rx = self.clone();
        let username_rx = username.clone();
        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    let mid = transceiver
                        .mid()
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    {
                        let mut mids = session_rx.tracked_mids.lock().await;
                        if mids.contains(&mid) {
                            tracing::debug!(
                                "[{}] Track {} already being recorded, skipping",
                                username_rx,
                                mid
                            );
                            continue;
                        }
                        mids.insert(mid);
                    }

                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            session_rx.clone(),
                            receiver.track(),
                            child_token.clone(),
                        );
                    }
                }
            }
        };

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
        let channels = ct.channels();
        let mut encoder = audio_codec::create_encoder(ct);
        let payload_type = pt.unwrap_or(match ct {
            CodecType::PCMU => 0,
            CodecType::PCMA => 8,
            CodecType::G722 => 9,
            CodecType::G729 => 18,
            #[cfg(feature = "opus")]
            CodecType::Opus => 111,
            _ => 0,
        });

        let mut ticker = interval(Duration::from_millis(20));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let chunk_size = (sample_rate / 50) as usize; // 20ms
        let mut rtp_timestamp: u32 = random_u32();
        let mut sent_chunks = 0;

        let mut recorder_resampler = if sample_rate != 16000 {
            Some(Resampler::new(sample_rate as usize, 16000))
        } else {
            None
        };

        let total_chunks = (samples.len() + chunk_size - 1) / chunk_size;

        info!(
            "[{}] Playback started: {} samples ({} chunks) using {:?} at {}Hz (clock {}Hz, pt={})",
            username,
            samples.len(),
            total_chunks,
            ct,
            sample_rate,
            clock_rate,
            payload_type
        );

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

        for chunk in samples.chunks(chunk_size) {
            ticker.tick().await;

            let final_chunk = if chunk.len() == chunk_size {
                chunk.to_vec()
            } else {
                let mut v = chunk.to_vec();
                v.resize(chunk_size, 0);
                v
            };

            // Send to recorder (non-blocking)
            let recorder_samples = if let Some(resampler) = recorder_resampler.as_mut() {
                resampler.resample(&final_chunk)
            } else {
                final_chunk.clone()
            };
            let _ = rec_tx.send(recorder_samples);

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

            let encoded = encoder.encode(&audio_to_encode);
            if encoded.is_empty() {
                continue;
            }

            self.stats.inc_tx(1, encoded.len() as u64);

            let frame = AudioFrame {
                data: Bytes::from(encoded),
                clock_rate,
                rtp_timestamp,
                payload_type: Some(payload_type),
                sequence_number: None,
            };

            let ticks = (chunk_size as u64 * clock_rate as u64 / sample_rate as u64) as u32;
            rtp_timestamp = rtp_timestamp.wrapping_add(ticks);

            if let Err(e) = self.audio_source.send_audio(frame).await {
                error!("[{}] Failed to send audio: {:?}", username, e);
                break;
            }

            sent_chunks += 1;
            if sent_chunks % 100 == 0 {
                info!(
                    "[{}] Sent {}/{} chunks",
                    username, sent_chunks, total_chunks
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
                {
                    let mut mids = self.tracked_mids.lock().await;
                    if mids.contains(&mid) {
                        continue;
                    }
                    mids.insert(mid);
                }
                self.spawn_audio_loop(username.clone(), receiver.track());
            }
        }

        let session_rx = self.clone();
        let username_rx = username.clone();
        while let Some(event) = self.pc.recv().await {
            match event {
                PeerConnectionEvent::Track(transceiver) => {
                    let mid = transceiver
                        .mid()
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    {
                        let mut mids = session_rx.tracked_mids.lock().await;
                        if mids.contains(&mid) {
                            tracing::debug!(
                                "[{}] Track {} already being recorded, skipping",
                                username_rx,
                                mid
                            );
                            continue;
                        }
                        mids.insert(mid);
                    }
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        self.spawn_audio_loop(username_rx.clone(), receiver.track());
                    }
                }
                _ => {
                    info!("[{}] Received PC event: Other", username_rx);
                }
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
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

fn spawn_track_recorder(
    session: MediaSession,
    track: Arc<dyn MediaStreamTrack>,
    token: CancellationToken,
) {
    let recorder = session.recorder.clone();
    let stats = session.stats.clone();
    let jitter_buffer_enabled = session.jitter_buffer_enabled;
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
    mut sample: MediaSample,
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
    channels: u16,
    actual_sample_rate: u32,
) {
    let decoded = if let MediaSample::Audio(ref frame) = sample {
        Some(decoder.decode(&frame.data))
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
                        "Recording: Rewriting timestamp (forward jump): seq={:?} original_ts={} -> expected_ts={} diff={} (>{:.1}s)",
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
                            "Recording: Rewriting timestamp (backward jump): seq={:?} original_ts={} -> expected_ts={} diff=-{} (>{:.1}s)",
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

        let rec = recorder.lock().await;
        if let Some(r) = rec.as_ref() {
            let resampled = if let Some(resampler) = recorder_resampler {
                resampler.resample(&decoded)
            } else {
                decoded.clone()
            };
            r.record_rx(&resampled);
        }

        #[cfg(feature = "local-device")]
        {
            let mut tx = local_playback_tx.lock().await;
            if let Some(prod) = tx.as_mut() {
                let target_rate = output_sample_rate.load(std::sync::atomic::Ordering::Relaxed);
                if target_rate > 0 {
                    // Mix to mono if stereo
                    // Note: 'channels' refers to the negotiated codec channels.
                    // For Opus, it is often 2 in SDP but the decoder might return mono (1 channel).
                    // We check if the decoded length is twice the expected mono samples for 20ms (standard frame).
                    // If it's just one-channel's worth of samples, we don't mix.
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
                        // Check if we need to recreate the resampler
                        let need_new = match output_resampler.as_ref() {
                            Some(_) => {
                                // For now, we don't have a way to check existing resampler rates.
                                // But if it's already there, we check if the PT (and thus input rate) changed
                                // Actually, it's safer to just check if source/target rate changed if we can.
                                // Since we don't have access to resampler internals, we rely on the caller reset.
                                false
                            }
                            None => true,
                        };

                        // We reset the resampler if it's None or if the source rate doesn't match
                        // Wait, we need to store the current source rate to detect change.
                        // Let's use a simpler approach: recreate it if it's the first time
                        // or if we detect a change in pt (handled in spawn_track_recorder).

                        if need_new {
                            *output_resampler = Some(audio_codec::Resampler::new(
                                actual_sample_rate as usize,
                                target_rate as usize,
                            ));
                        }

                        if let Some(resampler) = output_resampler.as_mut() {
                            let resampled = resampler.resample(&mono_decoded);
                            // Push the slice. If it's full, we just drop to avoid blocking the RX task.
                            let capacity = prod.capacity();
                            let occupied = prod.occupied_len();
                            let pushed = prod.push_slice(&resampled);

                            if pushed < resampled.len() {
                                tracing::warn!(
                                    "Local playback buffer full! ({} / {}), dropped {} output samples",
                                    occupied,
                                    capacity,
                                    resampled.len() - pushed
                                );
                            } else if occupied > capacity.get() * 8 / 10 {
                                tracing::debug!(
                                    "Local playback buffer high: {} / {}",
                                    occupied,
                                    capacity
                                );
                            }
                        }
                    } else {
                        // If no resampling needed, clear the old resampler if any
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
                        } else if occupied > capacity.get() * 8 / 10 {
                            tracing::debug!(
                                "Local playback buffer high: {} / {}",
                                occupied,
                                capacity
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
            #[cfg(feature = "opus")]
            "opus".to_string(),
            "g722".to_string(),
            "pcmu".to_string(),
        ]);
        let caps = get_audio_caps(&codecs, false);
        #[cfg(feature = "opus")]
        {
            assert_eq!(caps.len(), 3);
            assert_eq!(caps[0].codec_name, "opus");
            assert_eq!(caps[1].codec_name, "G722");
            assert_eq!(caps[2].codec_name, "PCMU");
        }
        #[cfg(not(feature = "opus"))]
        {
            assert_eq!(caps.len(), 2);
            assert_eq!(caps[0].codec_name, "G722");
            assert_eq!(caps[1].codec_name, "PCMU");
        }
    }

    #[tokio::test]
    async fn test_media_session_offer() {
        let stats = Arc::new(CallStats::new());
        let codecs = Some(vec![
            #[cfg(feature = "opus")]
            "opus".to_string(),
            "pcmu".to_string(),
        ]);
        let (_session, sdp) = MediaSession::new_offer(
            false,
            false,
            false,
            None,
            codecs.clone(),
            true,
            stats.clone(),
        )
        .await
        .unwrap();
        assert!(sdp.contains("m=audio"));
        assert!(sdp.contains("a=sendrecv")); // We set direction to SendRecv
        #[cfg(feature = "opus")]
        assert!(sdp.to_lowercase().contains("opus"));
        assert!(sdp.to_lowercase().contains("pcmu"));

        // Check if we can create an answer session
        let (_answer_session, answer_sdp, _) =
            MediaSession::new(&sdp, false, false, false, None, codecs, stats)
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
        let (_offerer, offer_sdp) =
            MediaSession::new_offer(false, false, false, None, offer_codecs, true, stats.clone())
                .await
                .unwrap();

        assert!(offer_sdp.contains("G729"));

        // Answer without specifying codecs (should support all by default now)
        let (_answerer, answer_sdp, _) =
            MediaSession::new(&offer_sdp, false, false, false, None, None, stats.clone())
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
}
