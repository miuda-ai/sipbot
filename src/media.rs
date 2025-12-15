use crate::recorder::Recorder;
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
}

impl MediaSession {
    pub async fn new(
        remote_sdp: &str,
        srtp_enabled: bool,
        external_ip: Option<String>,
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
        let (source, track, _feedback) = sample_track(MediaKind::Audio, ssrc_id as usize);
        let audio_source = Arc::new(source);

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));
        let _recorder_clone = recorder.clone();

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
                let sender = Arc::new(RtpSender::new(
                    track.clone(),
                    ssrc_id,
                    "audio".to_string(),
                    params,
                ));
                t.set_sender(Some(sender));
            } else {
                info!("Transceiver has no sender, setting one");
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
            }
            t.set_direction(TransceiverDirection::SendRecv);
        } else {
            info!("No transceivers found, adding track");
            // Fallback
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

        info!("Local SDP (Answer): {}", local_sdp);

        Ok((
            Self {
                pc,
                audio_source,
                recorder,
            },
            local_sdp,
        ))
    }

    pub async fn new_offer(
        srtp_enabled: bool,
        external_ip: Option<String>,
        send_audio: bool,
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
        config.ice_servers = vec![]; // No STUN/TURN servers
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

        // Create audio track
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 11111);
        let audio_source = Arc::new(source);

        if send_audio {
            // Add real track
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

        let pc = self.pc.clone();
        let recorder = self.recorder.clone();
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
                spawn_track_recorder(receiver.track(), recorder.clone(), child_token.clone());
            } else {
                info!("[{}] Transceiver has NO receiver", username);
            }
        }

        let username_rx = username.clone();
        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    info!("[{}] Received PC event: Track", username_rx);
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            receiver.track(),
                            recorder.clone(),
                            child_token.clone(),
                        );
                    }
                }
            }
        };
        tokio::pin!(rx_task);

        let play_fut = self.play_samples(username.clone(), samples);
        tokio::pin!(play_fut);

        let mut play_done = false;

        loop {
            tokio::select! {
                res = &mut play_fut, if !play_done => {
                    play_done = true;
                    if let Err(e) = res {
                        cancel_token.cancel();
                        return Err(e);
                    }
                    if !keep_alive {
                        cancel_token.cancel();
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
        let recorder = self.recorder.clone();
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
                spawn_track_recorder(receiver.track(), recorder.clone(), child_token.clone());
            } else {
                info!("[{}] Transceiver {} has NO receiver", username, i);
            }
        }

        let rx_task = async move {
            while let Some(event) = pc.recv().await {
                if let PeerConnectionEvent::Track(transceiver) = event {
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        spawn_track_recorder(
                            receiver.track(),
                            recorder.clone(),
                            child_token.clone(),
                        );
                    }
                }
            }
        };
        tokio::pin!(rx_task);

        let play_fut = self.play_samples(username.clone(), samples);
        tokio::pin!(play_fut);

        let mut play_done = false;

        loop {
            tokio::select! {
                res = &mut play_fut, if !play_done => {
                    play_done = true;
                    if let Err(e) = res {
                        cancel_token.cancel();
                        return Err(e);
                    }
                    if !keep_alive {
                        cancel_token.cancel();
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
        let non_zero = samples.iter().filter(|&&s| s != 0).count();
        info!(
            "[{}] play_samples total: {}, non-zero: {}",
            username,
            samples.len(),
            non_zero
        );

        let mut encoder = PcmuEncoder::new();
        let mut ticker = interval(Duration::from_millis(20));
        let chunk_size = 160; // 20ms at 8000Hz
        let mut timestamp = Duration::from_secs(0);
        let mut seq_no = 0u16;

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

            if !encoded.is_empty() {
                let all_ff = encoded.iter().all(|&b| b == 0xFF);
                if all_ff {
                    let input_silence = chunk.iter().all(|&s| s == 0);
                    if !input_silence {
                        error!("ENCODER BUG: Produced all 0xFF for non-silent input!");
                    }
                }
            }

            let frame = AudioFrame {
                data: Bytes::from(encoded),
                sample_rate: 8000,
                channels: 1,
                samples: chunk.len() as u32,
                timestamp,
                format: AudioSampleFormat::Unspecified,
                payload_type: Some(0),
                sequence_number: Some(seq_no),
            };
            seq_no = seq_no.wrapping_add(1);
            timestamp += Duration::from_millis(20);

            self.audio_source.send_audio(frame).await?;
        }
        Ok(())
    }

    pub async fn start_echo(
        &mut self,
        username: String,
        recording_path: Option<&Path>,
    ) -> Result<()> {
        info!("[{}] Starting echo...", username);

        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(username.clone(), path.to_path_buf()));
        }

        let audio_source = self.audio_source.clone();
        let recorder = self.recorder.clone();

        // Handle existing transceivers
        let transceivers = self.pc.get_transceivers();
        info!(
            "[{}] Found {} existing transceivers for echo",
            username,
            transceivers.len()
        );
        for transceiver in transceivers {
            if let Some(receiver) = transceiver.receiver().as_ref() {
                let track = receiver.track();
                info!("[{}] Existing Track ID: {}", username, track.id());
                let audio_source = audio_source.clone();
                let username_clone = username.clone();
                let recorder = recorder.clone();
                tokio::spawn(async move {
                    let mut decoder = PcmuDecoder::new();
                    loop {
                        match track.recv().await {
                            Ok(sample) => {
                                // Record RX
                                {
                                    let rec = recorder.lock().await;
                                    if let Some(r) = rec.as_ref() {
                                        if let MediaSample::Audio(frame) = &sample {
                                            let decoded = decoder.decode(&frame.data);
                                            r.record_rx(&decoded);
                                            // Also record TX since we are echoing
                                            r.record_tx(&decoded);
                                        } else {
                                            error!("RX SAMPLE: NOT AUDIO");
                                        }
                                    }
                                }

                                // Echo
                                if let Err(e) = audio_source.send(sample).await {
                                    error!(
                                        "[{}] Failed to send echo sample: {:?}",
                                        username_clone, e
                                    );
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(
                                    "[{}] Failed to receive sample for echo: {:?}",
                                    username_clone, e
                                );
                                break;
                            }
                        }
                    }
                });
            }
        }

        while let Some(event) = self.pc.recv().await {
            match event {
                PeerConnectionEvent::Track(transceiver) => {
                    let mid = transceiver.mid();
                    info!("[{}] Received PC event: Track (mid: {:?})", username, mid);
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        let track = receiver.track();
                        info!("[{}] Track ID: {}", username, track.id());
                        let audio_source = audio_source.clone();
                        let username_clone = username.clone();
                        let recorder = recorder.clone();
                        tokio::spawn(async move {
                            let mut decoder = PcmuDecoder::new();
                            loop {
                                match track.recv().await {
                                    Ok(sample) => {
                                        // Record RX
                                        {
                                            let rec = recorder.lock().await;
                                            if let Some(r) = rec.as_ref() {
                                                if let MediaSample::Audio(frame) = &sample {
                                                    let decoded = decoder.decode(&frame.data);
                                                    r.record_rx(&decoded);
                                                    // Also record TX since we are echoing
                                                    r.record_tx(&decoded);
                                                } else {
                                                    error!("RX SAMPLE: NOT AUDIO");
                                                }
                                            }
                                        }

                                        // Echo
                                        if let Err(e) = audio_source.send(sample).await {
                                            error!(
                                                "[{}] Failed to send echo sample: {:?}",
                                                username_clone, e
                                            );
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "[{}] Failed to receive sample for echo: {:?}",
                                            username_clone, e
                                        );
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
                _ => {
                    info!("[{}] Received PC event: Other", username);
                }
            }
        }

        Ok(())
    }
    pub async fn start_recording(&mut self, _path: &Path) -> Result<()> {
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

fn spawn_track_recorder(
    track: Arc<dyn MediaStreamTrack>,
    recorder: Arc<Mutex<Option<Recorder>>>,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let mut decoder = PcmuDecoder::new();
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("RX task cancelled");
                    break;
                },
                res = track.recv() => {
                    match res {
                        Ok(sample) => {
                            let rec = recorder.lock().await;
                            if let Some(r) = rec.as_ref() {
                                if let MediaSample::Audio(frame) = &sample {
                                    let decoded = decoder.decode(&frame.data);
                                    r.record_rx(&decoded);
                                }
                            }
                        }
                        Err(_) => {
                            break;
                        },
                    }
                }
            }
        }
    });
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
        let (_session, sdp) = MediaSession::new_offer(false, None, true).await.unwrap();
        assert!(sdp.contains("m=audio"));
        assert!(sdp.contains("a=sendrecv")); // We set direction to SendRecv

        // Check if we can create an answer session
        let (_answer_session, answer_sdp) = MediaSession::new(&sdp, false, None).await.unwrap();
        assert!(answer_sdp.contains("m=audio"));
        assert!(answer_sdp.contains("a=sendrecv"));
    }
}
