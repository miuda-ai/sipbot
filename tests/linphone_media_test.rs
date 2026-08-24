//! Reproduce the Linphone-caller interop scenario at the media layer:
//! Linphone INVITE SDP (BUNDLE, rtcp-mux, sdes:mid, opus 96 / pcmu 0 /
//! telephone-event) -> MediaSession::new -> playback; assert we send RTP.

use sipbot::media::MediaSession;
use sipbot::stats::CallStats;
use std::sync::Arc;
use std::time::Duration;

fn sine_wav_bytes(secs: f32) -> Vec<u8> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 8000,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut buf = Vec::new();
    let mut writer = hound::WavWriter::new(std::io::Cursor::new(&mut buf), spec).unwrap();
    let n = (8000.0 * secs) as usize;
    for i in 0..n {
        let v = (i as f32 * 2.0 * std::f32::consts::PI * 440.0 / 8000.0).sin();
        writer.write_sample((v * 8000.0) as i16).unwrap();
    }
    writer.finalize().unwrap();
    buf
}

#[tokio::test]
async fn linphone_offer_media_session_sends_rtp() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let offer = "v=0\r\n\
o=alice 1285 868 IN IP4 127.0.0.1\r\n\
s=Talk\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
a=rtcp-xr:rcvr-rtt=all:10000 stat-summary=loss,dup,jitt,TTL voip-metrics\r\n\
a=group:BUNDLE as\r\n\
a=record:off\r\n\
m=audio 55315 RTP/AVP 96 0 101 97\r\n\
a=rtpmap:96 opus/48000/2\r\n\
a=fmtp:96 useinbandfec=1\r\n\
a=rtpmap:101 telephone-event/48000\r\n\
a=rtpmap:97 telephone-event/8000\r\n\
a=rtcp-mux\r\n\
a=mid:as\r\n\
a=extmap:1 urn:ietf:params:rtp-hdrext:sdes:mid\r\n\
a=rtcp:52551\r\n\
a=sendrecv\r\n";

    let stats = Arc::new(CallStats::new());
    let (session, _answer_sdp, codec) = MediaSession::new(
        offer,
        false,
        false,
        false,
        false,
        None,
        None,
        stats.clone(),
        None,
        80,
    )
    .await
    .expect("MediaSession::new failed");

    println!("negotiated codec: {}", codec);

    // Drive playback of a 10s sine wav; RTP should flow to 127.0.0.1:55315.
    let wav = sine_wav_bytes(10.0);
    let play = session.play_wav_bytes_once("linphone-test".into(), &wav, None);
    tokio::pin!(play);
    let _ = tokio::select! {
        _ = &mut play => {},
        _ = tokio::time::sleep(Duration::from_secs(6)) => {},
    };

    let tx = stats.tx_packets.load(std::sync::atomic::Ordering::Relaxed);
    println!("tx_packets = {}", tx);
    assert!(tx > 0, "no RTP sent for Linphone-style offer");
}
