use crate::recorder::Recorder;
use crate::stats::CallStats;
use anyhow::{Context, Result};
use bytes::Bytes;
use rubato::{FftFixedIn, Resampler};
use rustrtc::config::{
    AudioCapability, BundlePolicy, MediaCapabilities, RtcConfiguration, TransportMode,
};
use rustrtc::media::MediaKind;
use rustrtc::media::frame::{AudioFrame, AudioSampleFormat, MediaSample};
use rustrtc::media::track::{MediaStreamTrack, SampleStreamSource, sample_track};
use rustrtc::peer_connection::{
    PeerConnection, PeerConnectionEvent, RtpCodecParameters, RtpSender, TransceiverDirection,
};
use rustrtc::sdp::{SdpType, SessionDescription};
use rustrtc::transports::ice::stun::random_u32;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use voice_engine::media::codecs::pcmu::{PcmuDecoder, PcmuEncoder};
use voice_engine::media::codecs::{Decoder, Encoder};

#[derive(Clone)]
pub struct MediaSession {
    pc: Arc<PeerConnection>,
    audio_source: Arc<SampleStreamSource>,
    recorder: Arc<Mutex<Option<Recorder>>>,
    stats: Arc<CallStats>,
    sample_tx: broadcast::Sender<MediaSample>,
}

impl MediaSession {
    pub async fn new(
        remote_sdp: &str,
        srtp_enabled: bool,
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
            TransportMode::Rtp
        };
        config.ice_servers = vec![]; // No STUN/TURN servers
        config.media_capabilities = Some(MediaCapabilities {
            audio: vec![AudioCapability::pcmu()],
            video: vec![],
            application: None,
        });

        let pc = Arc::new(PeerConnection::new(config));
        let ssrc_id = random_u32();
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 1000);
        let audio_source = Arc::new(source);

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));

        let (sample_tx, _) = broadcast::channel(100);
        let sample_tx_clone = sample_tx.clone();
        let pc_clone = pc.clone();
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            while let Some(event) = pc_clone.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        let track = receiver.track();
                        let tx = sample_tx_clone.clone();
                        let s = stats_clone.clone();
                        tokio::spawn(async move {
                            info!("Track receiver task started");
                            let mut last_seq: Option<u16> = None;
                            loop {
                                match track.recv().await {
                                    Ok(sample) => {
                                        if let MediaSample::Audio(frame) = &sample {
                                            if let Some(seq) = frame.sequence_number {
                                                if let Some(last) = last_seq {
                                                    let expected = last.wrapping_add(1);
                                                    if seq != expected {
                                                        let lost = if seq > expected {
                                                            (seq - expected) as u64
                                                        } else {
                                                            (u16::MAX - expected + seq + 1) as u64
                                                        };
                                                        if lost < 1000 {
                                                            s.inc_rx_lost(lost);
                                                        }
                                                    }
                                                }
                                                last_seq = Some(seq);
                                            }
                                            s.inc_rx(1, frame.data.len() as u64);
                                        }

                                        if let Err(_) = tx.send(sample) {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Track recv error: {:?}", e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        let remote_desc = SessionDescription::parse(SdpType::Offer, remote_sdp)?;
        pc.set_remote_description(remote_desc).await?;

        // Attach track to the active transceiver
        let transceivers = pc.get_transceivers();
        if let Some(t) = transceivers.first() {
            let params = RtpCodecParameters {
                payload_type: 0,
                clock_rate: 8000,
                channels: 1,
            };
            let sender = Arc::new(RtpSender::new(
                track.clone(),
                ssrc_id,
                "audio".to_string(),
                params,
            ));
            t.set_sender(Some(sender));
            t.set_direction(TransceiverDirection::SendRecv);
        } else {
            let params = RtpCodecParameters {
                payload_type: 0,
                clock_rate: 8000,
                channels: 1,
            };
            pc.add_track(track.clone(), params)?;
        }

        let answer = pc.create_answer()?;
        let sdp_str = answer.to_sdp_string();
        let answer = SessionDescription::parse(SdpType::Answer, &sdp_str)?;

        pc.set_local_description(answer.clone())?;
        pc.wait_for_gathering_complete().await;

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
                sample_tx,
            },
            local_sdp,
        ))
    }

    pub async fn new_offer(
        srtp_enabled: bool,
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
            TransportMode::Rtp
        };
        config.bundle_policy = BundlePolicy::MaxBundle;
        config.ice_servers = vec![];
        config.media_capabilities = Some(MediaCapabilities {
            audio: vec![AudioCapability {
                payload_type: 0,
                codec_name: "PCMU".to_string(),
                clock_rate: 8000,
                channels: 1,
                fmtp: None,
            }],
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

        let (sample_tx, _sample_rx) = broadcast::channel(1000);
        let sample_tx_clone = sample_tx.clone();
        let pc_clone = pc.clone();
        tokio::spawn(async move {
            while let Some(event) = pc_clone.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        let track = receiver.track();
                        let tx = sample_tx_clone.clone();
                        tokio::spawn(async move {
                            info!("Track receiver task started (new_offer)");
                            let mut count = 0;
                            loop {
                                match track.recv().await {
                                    Ok(sample) => {
                                        count += 1;
                                        if count % 100 == 0 {
                                            info!(
                                                "Received {} samples from track (new_offer)",
                                                count
                                            );
                                        }
                                        if let Err(_) = tx.send(sample) {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Track recv error (new_offer): {:?}", e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        let offer = pc.create_offer()?;
        let sdp_str = offer.to_sdp_string();
        let offer = SessionDescription::parse(SdpType::Offer, &sdp_str)?;

        pc.set_local_description(offer.clone())?;
        pc.wait_for_gathering_complete().await;

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
                sample_tx,
            },
            local_sdp,
        ))
    }

    pub async fn set_remote_answer(&self, remote_sdp: &str) -> Result<()> {
        let remote_desc = SessionDescription::parse(SdpType::Answer, remote_sdp)?;
        self.pc.set_remote_description(remote_desc).await?;
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

        let samples = if spec.sample_rate != 8000 || spec.channels != 1 {
            info!(
                "[{}] Resampling audio from {}Hz {}ch to 8000Hz 1ch",
                username, spec.sample_rate, spec.channels
            );
            resample_audio(raw_samples, spec.sample_rate, spec.channels)?
        } else {
            raw_samples
        };

        let recorder_clone = self.recorder.clone();
        let sample_tx = self.sample_tx.clone();
        let child_token = CancellationToken::new();
        let rx_task = async move {
            run_recorder_loop(sample_tx.subscribe(), recorder_clone, child_token).await
        };

        let play_fut = self.play_samples(username.clone(), samples);
        tokio::pin!(play_fut);
        tokio::pin!(rx_task);

        loop {
            tokio::select! {
                res = &mut play_fut => {
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

        let recorder_clone = self.recorder.clone();
        let sample_tx = self.sample_tx.clone();
        let child_token = CancellationToken::new();
        let rx_task = async move {
            run_recorder_loop(sample_tx.subscribe(), recorder_clone, child_token).await
        };

        let play_fut = self.play_samples(username.clone(), samples);
        tokio::pin!(play_fut);
        tokio::pin!(rx_task);

        loop {
            tokio::select! {

                res = &mut play_fut => {
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

    pub async fn start_echo(&self, username: String, recording_path: Option<&Path>) -> Result<()> {
        info!("[{}] Starting echo service", username);

        let audio_source = self.audio_source.clone();
        let recorder = self.recorder.clone();
        let sample_tx = self.sample_tx.clone();

        // 1. Subscription for recording
        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(username.clone(), path.to_path_buf()));

            let recorder_clone = recorder.clone();
            let sample_tx_clone = sample_tx.clone();
            tokio::spawn(async move {
                run_recorder_loop(
                    sample_tx_clone.subscribe(),
                    recorder_clone,
                    CancellationToken::new(),
                )
                .await;
            });
        }

        // 2. Subscription for echoing
        tokio::spawn(run_echo_loop(sample_tx.subscribe(), audio_source, username));

        Ok(())
    }

    pub async fn stop(&self) {
        self.pc.close();
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

async fn run_recorder_loop(
    mut rx: broadcast::Receiver<MediaSample>,
    recorder: Arc<Mutex<Option<Recorder>>>,
    token: CancellationToken,
) {
    let mut decoder = PcmuDecoder::new();
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("Recorder task cancelled");
                break;
            },
            res = rx.recv() => {
                match res {
                    Ok(sample) => {
                        if let MediaSample::Audio(frame) = &sample {
                            let rec = recorder.lock().await;
                            if let Some(r) = rec.as_ref() {
                                let decoded = decoder.decode(&frame.data);
                                r.record_rx(&decoded);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        error!("Recorder loop lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    },
                }
            }
        }
    }
}

async fn run_echo_loop(
    mut rx: broadcast::Receiver<MediaSample>,
    audio_source: Arc<SampleStreamSource>,
    username: String,
) {
    let username_clone = username.clone();
    let mut count = 0;
    info!("[{}] Echo loop started", username_clone);
    loop {
        match rx.recv().await {
            Ok(sample) => {
                if let Err(e) = audio_source.send(sample).await {
                    error!("[{}] Failed to send echo sample: {:?}", username_clone, e);
                    break;
                }
                count += 1;
                if count % 100 == 0 {
                    info!("[{}] Echoed {} samples", username_clone, count);
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                error!("[{}] Echo loop lagged, skipping samples", username_clone);
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                break;
            }
        }
    }
    info!(
        "[{}] Echo loop finished after {} samples",
        username_clone, count
    );
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
        let (_session, sdp) = MediaSession::new_offer(false, None, true, stats.clone())
            .await
            .unwrap();
        assert!(sdp.contains("m=audio"));
        assert!(sdp.contains("a=sendrecv")); // We set direction to SendRecv

        // Check if we can create an answer session
        let (_answer_session, answer_sdp) =
            MediaSession::new(&sdp, false, None, stats).await.unwrap();
        assert!(answer_sdp.contains("m=audio"));
        assert!(answer_sdp.contains("a=sendrecv"));
    }
}
