use crate::recorder::Recorder;
use anyhow::{Context, Result};
use bytes::Bytes;
use rubato::{FftFixedIn, Resampler};
use rustrtc::config::{AudioCapability, MediaCapabilities, RtcConfiguration, TransportMode};
use rustrtc::media::MediaKind;
use rustrtc::media::frame::{AudioFrame, AudioSampleFormat};
use rustrtc::media::track::{MediaStreamTrack, SampleStreamSource, sample_track};
use rustrtc::peer_connection::{PeerConnection, PeerConnectionEvent, RtpCodecParameters};
use rustrtc::sdp::{SdpType, SessionDescription};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{error, info};
use voice_engine::media::codecs::Encoder;
use voice_engine::media::codecs::pcmu::PcmuEncoder;

#[derive(Clone)]
pub struct MediaSession {
    pc: Arc<PeerConnection>,
    audio_source: Arc<SampleStreamSource>,
    recorder: Arc<Mutex<Option<Recorder>>>,
}

impl MediaSession {
    pub async fn new(remote_sdp: &str) -> Result<(Self, String)> {
        let mut config = RtcConfiguration::default();
        config.transport_mode = TransportMode::Rtp;
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
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 12345);
        let audio_source = Arc::new(source);

        // Add track
        let codec_params = RtpCodecParameters {
            payload_type: 0,
            clock_rate: 8000,
            channels: 1,
        };
        pc.add_track(track.clone(), codec_params)?;

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));
        let _recorder_clone = recorder.clone();

        let remote_desc = SessionDescription::parse(SdpType::Offer, remote_sdp)?;
        pc.set_remote_description(remote_desc).await?;

        let answer = pc.create_answer()?;
        pc.set_local_description(answer.clone())?;

        let local_sdp = answer.to_sdp_string();

        Ok((
            Self {
                pc,
                audio_source,
                recorder,
            },
            local_sdp,
        ))
    }

    pub async fn new_offer() -> Result<(Self, String)> {
        let mut config = RtcConfiguration::default();
        config.transport_mode = TransportMode::Rtp;
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
        let (source, track, _feedback) = sample_track(MediaKind::Audio, 12345);
        let audio_source = Arc::new(source);

        // Add track
        let codec_params = RtpCodecParameters {
            payload_type: 0,
            clock_rate: 8000,
            channels: 1,
        };
        pc.add_track(track.clone(), codec_params)?;

        let recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));

        let offer = pc.create_offer()?;
        pc.set_local_description(offer.clone())?;

        let local_sdp = offer.to_sdp_string();

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

    pub async fn play_file(&self, file_path: &Path, recording_path: Option<&Path>) -> Result<()> {
        info!("Playing file: {:?}", file_path);

        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(path.to_path_buf()));
        }

        let mut reader = hound::WavReader::open(file_path).context("Failed to open WAV file")?;
        let spec = reader.spec();
        let raw_samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap_or(0)).collect();

        let samples = if spec.sample_rate != 8000 || spec.channels != 1 {
            info!(
                "Resampling audio from {}Hz {}ch to 8000Hz 1ch",
                spec.sample_rate, spec.channels
            );
            resample_audio(raw_samples, spec.sample_rate, spec.channels)?
        } else {
            raw_samples
        };
        let mut encoder = PcmuEncoder::new();

        let mut ticker = interval(Duration::from_millis(20));
        let chunk_size = 160; // 20ms at 8000Hz

        let mut seq_no = 0u16;
        let mut timestamp = Duration::from_secs(0);

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

        // Stop recorder
        {
            let mut rec = self.recorder.lock().await;
            if let Some(r) = rec.take() {
                r.stop();
            }
        }

        Ok(())
    }

    pub async fn start_echo(&mut self, recording_path: Option<&Path>) -> Result<()> {
        info!("Starting echo...");

        if let Some(path) = recording_path {
            let mut rec = self.recorder.lock().await;
            *rec = Some(Recorder::new(path.to_path_buf()));
        }

        let audio_source = self.audio_source.clone();
        let _recorder = self.recorder.clone();

        while let Some(event) = self.pc.recv().await {
            match event {
                PeerConnectionEvent::Track(transceiver) => {
                    if let Some(receiver) = transceiver.receiver().as_ref() {
                        let track = receiver.track();
                        let audio_source = audio_source.clone();
                        tokio::spawn(async move {
                            loop {
                                match track.recv().await {
                                    Ok(sample) => {
                                        // Echo
                                        if let Err(e) = audio_source.send(sample).await {
                                            error!("Failed to send echo sample: {:?}", e);
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
                _ => {}
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
