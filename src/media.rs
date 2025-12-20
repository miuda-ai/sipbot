use crate::recorder::Recorder;
use crate::stats::CallStats;
use anyhow::{Context, Result};
use bytes::Bytes;
use rubato::{FftFixedIn, Resampler};
use rustrtc::config::{
    AudioCapability, MediaCapabilities, RtcConfiguration, RtcpMuxPolicy, TransportMode,
};
use rustrtc::media::MediaError;
use rustrtc::media::MediaKind;
use rustrtc::media::frame::{AudioFrame, AudioSampleFormat, MediaSample};
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
use tracing::{debug, error, info};
use voice_engine::media::codecs::pcmu::{PcmuDecoder, PcmuEncoder};
use voice_engine::media::codecs::{Decoder, Encoder};

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
}

impl MediaSession {
    pub async fn new(
        remote_sdp: &str,
        srtp_enabled: bool,
        nack_enabled: bool,
        jitter_buffer_enabled: bool,
        external_ip: Option<String>,
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
        let mut audio_caps = AudioCapability::pcmu();
        if nack_enabled {
            info!("NACK enabled for media session (incoming)");
            config.nack_buffer_size = 200;
        } else {
            audio_caps.rtcp_fbs.retain(|fb| fb != "nack");
        }
        config.rtcp_mux_policy = RtcpMuxPolicy::Negotiate;
        config.ice_servers = vec![]; // No STUN/TURN servers
        config.media_capabilities = Some(MediaCapabilities {
            audio: vec![audio_caps],
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
            // Try to use sender() as Option
            if let Some(_sender) = t.sender() {
                let params = RtpCodecParameters {
                    payload_type: 0,
                    clock_rate: 8000,
                    channels: 1,
                };
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
            } else {
                debug!("Transceiver has no sender, setting one");
                let params = RtpCodecParameters {
                    payload_type: 0,
                    clock_rate: 8000,
                    channels: 1,
                };
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
            }
            t.set_direction(TransceiverDirection::SendRecv);
        } else {
            let params = RtpCodecParameters {
                payload_type: 0,
                clock_rate: 8000,
                channels: 1,
            };
            pc.add_track(track.clone(), params)?;
        }

        pc.wait_for_gathering_complete().await;

        let answer = pc.create_answer()?;
        let sdp_str = answer.to_sdp_string();
        let answer = SessionDescription::parse(SdpType::Answer, &sdp_str)?;

        pc.set_local_description(answer.clone())?;

        let local_sdp = pc
            .local_description()
            .context("Failed to get local description")?
            .to_sdp_string();

        if local_sdp.is_empty() {
            anyhow::bail!("Failed to gather ICE candidates");
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
            },
            local_sdp,
        ))
    }

    pub async fn new_offer(
        srtp_enabled: bool,
        nack_enabled: bool,
        jitter_buffer_enabled: bool,
        external_ip: Option<String>,
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
        let mut audio_caps = AudioCapability::pcmu();
        if nack_enabled {
            info!("NACK enabled for media session (outgoing)");
            config.nack_buffer_size = 200;
        } else {
            audio_caps.rtcp_fbs.retain(|fb| fb != "nack");
        }
        config.rtcp_mux_policy = RtcpMuxPolicy::Negotiate;
        // config.bundle_policy = BundlePolicy::MaxBundle;
        config.ice_servers = vec![];
        config.media_capabilities = Some(MediaCapabilities {
            audio: vec![audio_caps],
            video: vec![],
            application: None,
        });

        let pc = Arc::new(PeerConnection::new(config));
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 1000);
        let audio_source = Arc::new(source);

        if send_audio {
            let params = RtpCodecParameters {
                payload_type: 0,
                clock_rate: 8000,
                channels: 1,
            };
            pc.add_track(track.clone(), params)?;
            for t in pc.get_transceivers() {
                t.set_direction(TransceiverDirection::SendRecv);
            }
        } else {
            pc.add_transceiver(rustrtc::MediaKind::Audio, TransceiverDirection::RecvOnly);
        }

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));

        pc.wait_for_gathering_complete().await;

        let offer = pc.create_offer()?;
        let sdp_str = offer.to_sdp_string();
        let offer = SessionDescription::parse(SdpType::Offer, &sdp_str)?;

        pc.set_local_description(offer.clone())?;

        let local_sdp = pc
            .local_description()
            .context("Failed to get local description")?
            .to_sdp_string();

        if local_sdp.is_empty() {
            anyhow::bail!("Failed to gather ICE candidates");
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
            },
            local_sdp,
        ))
    }

    pub async fn set_remote_answer(&self, remote_sdp: &str) -> Result<()> {
        let remote_desc = SessionDescription::parse(SdpType::Answer, remote_sdp)?;
        self.pc.set_remote_description(remote_desc).await?;
        Ok(())
    }

    fn spawn_audio_loop(&self, username: String, track: Arc<dyn MediaStreamTrack>) {
        let audio_source = self.audio_source.clone();
        let recorder = self.recorder.clone();
        let stats = self.stats.clone();
        let jitter_buffer_enabled = self.jitter_buffer_enabled;
        let session = self.clone();

        tokio::spawn(async move {
            let mut decoder = PcmuDecoder::new();
            let mut last_seq: Option<u16> = None;
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
                            while let Some(sample) = jb.pop() {
                                Self::process_sample(&username, sample, &audio_source, &recorder, &stats, &mut decoder, &mut last_seq).await;
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
                                Ok(sample) => {
                                    Self::process_sample(
                                        &username,
                                        sample,
                                        &audio_source,
                                        &recorder,
                                        &stats,
                                        &mut decoder,
                                        &mut last_seq,
                                    )
                                    .await;
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
        sample: MediaSample,
        audio_source: &Arc<SampleStreamSource>,
        recorder: &Arc<Mutex<Option<Recorder>>>,
        stats: &Arc<CallStats>,
        decoder: &mut PcmuDecoder,
        last_seq: &mut Option<u16>,
    ) {
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

                stats.inc_rx(1, frame.data.len() as u64);
                // Also record TX since we are echoing
                stats.inc_tx(1, frame.data.len() as u64);

                let rec = recorder.lock().await;
                if let Some(r) = rec.as_ref() {
                    let decoded = decoder.decode(&frame.data);
                    r.record_rx(&decoded);
                    r.record_tx(&decoded);
                }
            } else {
                error!("RX SAMPLE: NOT AUDIO");
            }
        }

        // Echo
        if let Err(e) = audio_source.send(sample).await {
            error!("[{}] Failed to send echo sample: {:?}", username, e);
        }
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

        let samples = if spec.sample_rate != 8000 || spec.channels != 1 {
            info!(
                "[{}] Resampling audio from {}Hz {}ch to 8000Hz 1ch",
                username, spec.sample_rate, spec.channels
            );
            resample_audio(raw_samples, spec.sample_rate, spec.channels)?
        } else {
            raw_samples
        };

        let pc = self.pc.clone();
        let cancel_token = CancellationToken::new();
        let child_token = cancel_token.clone();

        let recording = recording_path.is_some();
        // Handle existing transceivers
        let transceivers = pc.get_transceivers();
        info!("[{}] Found {} transceivers", username, transceivers.len());
        if recording {
            for transceiver in transceivers {
                if let Some(receiver) = transceiver.receiver().as_ref() {
                    info!(
                        "[{}] Transceiver has receiver, track id={}",
                        username,
                        receiver.track().id()
                    );
                    spawn_track_recorder(self.clone(), receiver.track(), child_token.clone());
                } else {
                    info!("[{}] Transceiver has NO receiver", username);
                }
            }
        }

        let username_rx = username.clone();
        let session_rx = self.clone();
        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    info!("[{}] Received PC event: Track", username_rx);
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

        let samples = if spec.sample_rate != 8000 || spec.channels != 1 {
            info!(
                "[{}] Resampling audio from {}Hz {}ch to 8000Hz 1ch",
                username, spec.sample_rate, spec.channels
            );
            resample_audio(raw_samples, spec.sample_rate, spec.channels)?
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
                spawn_track_recorder(self.clone(), receiver.track(), child_token.clone());
            } else {
                info!("[{}] Transceiver {} has NO receiver", username, i);
            }
        }

        let session_rx = self.clone();
        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
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
        let mut encoder = PcmuEncoder::new();
        let mut ticker = interval(Duration::from_millis(20));
        let chunk_size = 160; // 20ms at 8000Hz
        let mut rtp_timestamp = 0;
        let total_chunks = samples.chunks(chunk_size).count();
        let mut sent_chunks = 0;

        info!(
            "[{}] Playback started: {} samples ({} chunks)",
            username,
            samples.len(),
            total_chunks
        );

        for chunk in samples.chunks(chunk_size) {
            ticker.tick().await;

            // Record TX
            {
                let rec = self.recorder.lock().await;
                if let Some(r) = rec.as_ref() {
                    r.record_tx(chunk);
                }
            }

            let encoded = encoder.encode(chunk);
            self.stats.inc_tx(1, encoded.len() as u64);

            let frame = AudioFrame {
                data: Bytes::from(encoded),
                sample_rate: 8000,
                channels: 1,
                samples: chunk.len() as u32,
                rtp_timestamp,
                format: AudioSampleFormat::Unspecified,
                payload_type: Some(0),
                sequence_number: None,
            };
            rtp_timestamp += chunk.len() as u32;

            self.audio_source.send_audio(frame).await?;
            sent_chunks += 1;
            if sent_chunks % 100 == 0 {
                info!(
                    "[{}] Sent {}/{} chunks",
                    username, sent_chunks, total_chunks
                );
            }
        }
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
                self.spawn_audio_loop(username.clone(), receiver.track());
            }
        }

        while let Some(event) = self.pc.recv().await {
            match event {
                PeerConnectionEvent::Track(transceiver) => {
                    let mid = transceiver.mid();
                    info!("[{}] Received PC event: Track (mid: {:?})", username, mid);
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        self.spawn_audio_loop(username.clone(), receiver.track());
                    }
                }
                _ => {
                    info!("[{}] Received PC event: Other", username);
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

fn resample_audio(samples: Vec<i16>, source_rate: u32, channels: u16) -> Result<Vec<i16>> {
    if source_rate == 8000 && channels == 1 {
        return Ok(samples);
    }

    // Convert to f32 and mix down to mono
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

    if source_rate == 8000 {
        return Ok(mono_samples.into_iter().map(|s| s as i16).collect());
    }

    // Resample
    let chunk_size_in = 1024;
    let mut resampler = FftFixedIn::<f32>::new(source_rate as usize, 8000, chunk_size_in, 1, 1)?;

    let num_chunks = (mono_samples.len() + chunk_size_in - 1) / chunk_size_in;
    // Pad with zeros
    mono_samples.resize(num_chunks * chunk_size_in, 0.0);

    let mut result = Vec::new();
    let mut input_buffer = vec![vec![0.0; chunk_size_in]];

    for i in 0..num_chunks {
        let start = i * chunk_size_in;
        let end = start + chunk_size_in;
        input_buffer[0].copy_from_slice(&mono_samples[start..end]);

        let out = resampler.process(&input_buffer, None)?;
        result.extend_from_slice(&out[0]);
    }

    Ok(result.into_iter().map(|s| s as i16).collect())
}

fn spawn_track_recorder(
    session: MediaSession,
    track: Arc<dyn MediaStreamTrack>,
    token: CancellationToken,
) {
    let recorder = session.recorder.clone();
    let stats = session.stats.clone();
    let jitter_buffer_enabled = session.jitter_buffer_enabled;
    tokio::spawn(async move {
        let mut decoder = PcmuDecoder::new();
        let mut last_seq: Option<u16> = None;

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
                        while let Some(sample) = jb.pop() {
                            process_recorded_sample(sample, &recorder, &stats, &mut decoder, &mut last_seq).await;
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
                            Ok(sample) => {
                                process_recorded_sample(sample, &recorder, &stats, &mut decoder, &mut last_seq).await;
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
    recorder: &Arc<Mutex<Option<Recorder>>>,
    stats: &Arc<CallStats>,
    decoder: &mut PcmuDecoder,
    last_seq: &mut Option<u16>,
) {
    if let MediaSample::Audio(frame) = &sample {
        if let Some(seq) = frame.sequence_number {
            if let Some(last) = *last_seq {
                let expected = last.wrapping_add(1);
                if seq != expected {
                    let diff = seq.wrapping_sub(last) as i16;

                    if diff > 1 {
                        stats.inc_rx_lost((diff - 1) as u64);
                        *last_seq = Some(seq);
                    } else if diff < 0 {
                        // Out of order packet
                    }
                } else {
                    *last_seq = Some(seq);
                }
            } else {
                *last_seq = Some(seq);
            }
        }

        stats.inc_rx(1, frame.data.len() as u64);
        let rec = recorder.lock().await;
        if let Some(r) = rec.as_ref() {
            let decoded = decoder.decode(&frame.data);
            r.record_rx(&decoded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resample_audio_identity() {
        let samples = vec![100, 200, 300, 400];
        let result = resample_audio(samples.clone(), 8000, 1).unwrap();
        assert_eq!(result, samples);
    }

    #[test]
    fn test_resample_audio_stereo_to_mono() {
        // 8000Hz stereo -> 8000Hz mono
        // Left: 100, Right: 200 -> Avg: 150
        let samples = vec![100, 200, 300, 500];
        let result = resample_audio(samples, 8000, 2).unwrap();
        assert_eq!(result, vec![150, 400]);
    }

    #[test]
    fn test_resample_audio_resampling() {
        // 16000Hz mono -> 8000Hz mono
        // Just checking it produces output of roughly half size
        let samples: Vec<i16> = (0..1600).map(|i| (i % 1000) as i16).collect();
        let result = resample_audio(samples.clone(), 16000, 1).unwrap();

        // 1600 samples at 16k is 0.1s
        // 0.1s at 8k is 800 samples
        // The resampler works in chunks, so exact size might vary slightly due to padding/buffering
        // but should be close.
        assert!(result.len() >= 800);
        assert!(result.len() < 1200); // Allow some padding overhead
    }

    #[tokio::test]
    async fn test_media_session_offer() {
        let stats = Arc::new(CallStats::new());
        let (_session, sdp) =
            MediaSession::new_offer(false, false, false, None, true, stats.clone())
                .await
                .unwrap();
        assert!(sdp.contains("m=audio"));
        assert!(sdp.contains("a=sendrecv")); // We set direction to SendRecv

        // Check if we can create an answer session
        let (_answer_session, answer_sdp) =
            MediaSession::new(&sdp, false, false, false, None, stats)
                .await
                .unwrap();
        assert!(answer_sdp.contains("m=audio"));
        assert!(answer_sdp.contains("a=sendrecv"));
    }
}
