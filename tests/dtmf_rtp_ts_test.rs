//! End-to-end verification that sending DTMF does not corrupt the outgoing
//! RTP timestamp timeline.
//!
//! A UDP socket stands in for the remote endpoint. We capture the RTP packets
//! the `MediaSession` actually emits while playing a 3s tone, inject a DTMF
//! digit in the middle, and assert:
//!   * the telephone-event start/end packets share one timestamp (RFC 4733),
//!   * the DTMF timestamp continues the audio timeline (no huge jump), and
//!   * the audio timestamps stay contiguous across the DTMF event.

use sipbot::media::MediaSession;
use sipbot::stats::CallStats;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

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

#[derive(Debug, Clone, Copy)]
struct Rtp {
    pt: u8,
    marker: bool,
    seq: u16,
    ts: u32,
}

fn parse_rtp(buf: &[u8]) -> Option<Rtp> {
    if buf.len() < 12 || (buf[0] >> 6) != 2 {
        return None;
    }
    Some(Rtp {
        pt: buf[1] & 0x7f,
        marker: (buf[1] & 0x80) != 0,
        seq: u16::from_be_bytes([buf[2], buf[3]]),
        ts: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
    })
}

#[tokio::test]
async fn dtmf_does_not_jump_outgoing_rtp_timestamp() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // The "remote" endpoint we send RTP to.
    let sock = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let remote_port = sock.local_addr().unwrap().port();

    let offer = format!(
        "v=0\r\n\
         o=alice 1 1 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         c=IN IP4 127.0.0.1\r\n\
         t=0 0\r\n\
         m=audio {remote_port} RTP/AVP 0 101\r\n\
         a=rtpmap:0 PCMU/8000\r\n\
         a=rtpmap:101 telephone-event/8000\r\n\
         a=sendrecv\r\n"
    );

    let stats = Arc::new(CallStats::new());
    let (session, _answer, codec) = MediaSession::new(
        &offer,
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

    // Play a 3s tone (looping disabled), and inject DTMF ~1s in.
    let wav = sine_wav_bytes(3.0);
    let player = session.clone();
    let play = tokio::spawn(async move {
        player
            .play_wav_bytes_once("dtmf-e2e".into(), &wav, None)
            .await
    });

    let dtmf_sender = session.clone();
    let dtmf_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        dtmf_sender.send_dtmf('5').await
    });

    let te_pt = session.get_telephone_event_pt();
    println!("telephone-event pt = {}", te_pt);

    let mut packets: Vec<Rtp> = Vec::new();
    let recv_deadline = tokio::time::Instant::now() + Duration::from_millis(3200);
    let mut buf = [0u8; 1500];
    loop {
        let now = tokio::time::Instant::now();
        if now >= recv_deadline {
            break;
        }
        match tokio::time::timeout(recv_deadline - now, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if let Some(p) = parse_rtp(&buf[..n]) {
                    packets.push(p);
                }
            }
            _ => break,
        }
    }

    let _ = dtmf_task.await;
    play.abort();
    let _ = play.await;

    assert!(!packets.is_empty(), "no RTP received on the wire");

    let audio: Vec<Rtp> = packets.iter().copied().filter(|p| p.pt != te_pt).collect();
    let dtmf: Vec<Rtp> = packets.iter().copied().filter(|p| p.pt == te_pt).collect();

    println!(
        "captured {} packets ({} audio, {} dtmf)",
        packets.len(),
        audio.len(),
        dtmf.len()
    );
    assert!(!audio.is_empty(), "no audio RTP received");
    assert!(
        dtmf.len() >= 2,
        "expected >=2 telephone-event packets, got {}",
        dtmf.len()
    );

    // RFC 4733: all packets of one event share the same timestamp.
    let dtmf_ts = dtmf[0].ts;
    assert!(
        dtmf.iter().all(|p| p.ts == dtmf_ts),
        "DTMF packets must share one timestamp: {:?}",
        dtmf.iter().map(|p| p.ts).collect::<Vec<_>>()
    );
    // Exactly one start (marker) and one end for each event is expected here.
    assert_eq!(
        dtmf.iter().filter(|p| p.marker).count(),
        1,
        "exactly one DTMF start (marker) packet"
    );

    // Locate the DTMF event position in the audio stream.
    // `packets` preserves arrival order; find the first DTMF packet index.
    let first_dtmf_idx = packets.iter().position(|p| p.pt == te_pt).unwrap();
    let prev_audio_ts = packets[..first_dtmf_idx]
        .iter()
        .rev()
        .find(|p| p.pt != te_pt)
        .map(|p| p.ts);
    let next_audio_ts = packets[first_dtmf_idx..]
        .iter()
        .find(|p| p.pt != te_pt)
        .map(|p| p.ts);

    let clock = 8000u32;
    // Continuity threshold: a single RTP packet is 20ms. Allow a generous 500ms
    // to absorb scheduling jitter, but reject the multi-second jumps the bug
    // produced (random 32-bit timestamps).
    let tolerance = clock / 2;

    // Signed distance between two RTP timestamps (handles wrap-around).
    fn abs_delta(a: u32, b: u32) -> u32 {
        let d = a.wrapping_sub(b);
        if d < 0x8000_0000 { d } else { d.wrapping_neg() }
    }

    if let Some(prev) = prev_audio_ts {
        let d = abs_delta(dtmf_ts, prev);
        println!("prev_audio_ts={} dtmf_ts={} delta={}", prev, dtmf_ts, d);
        assert!(
            d <= tolerance,
            "DTMF timestamp diverged from the audio timeline by {} ticks (> {} allowed)",
            d,
            tolerance
        );
    }
    if let Some(next) = next_audio_ts {
        let d = abs_delta(next, dtmf_ts);
        println!("next_audio_ts={} dtmf_ts={} delta={}", next, dtmf_ts, d);
        assert!(
            d <= tolerance,
            "audio timestamp diverged after DTMF by {} ticks (> {} allowed)",
            d,
            tolerance
        );
        // Regression: the first audio packet after the DTMF burst must never
        // carry a timestamp *behind* the event packet. The old code advanced
        // the event timestamp by wall-clock elapsed on top of the already
        // marked "next frame" grid slot, so this packet stepped backwards and
        // Wireshark flagged the stream.
        let signed = next.wrapping_sub(dtmf_ts);
        assert!(
            signed < clock,
            "audio after DTMF stepped backwards / overshot: next={} dtmf={} delta={}",
            next,
            dtmf_ts,
            signed
        );
    }

    // The wire sequence numbers must stay contiguous across the DTMF burst
    // (telephone-event packets occupy sequence numbers too).
    for w in packets.windows(2) {
        assert_eq!(
            w[1].seq.wrapping_sub(w[0].seq),
            1,
            "wire seq gap across DTMF: seq {} -> {}",
            w[0].seq,
            w[1].seq
        );
    }

    // Whole-stream sanity: consecutive audio packets must never jump more than
    // a few hundred ms (catches a corrupted post-DTMF offset even if the
    // first-after-DTMF packet happened to look fine).
    let max_audio_jump = clock; // 1s
    for w in audio.windows(2) {
        let d = w[1].ts.wrapping_sub(w[0].ts);
        assert!(
            d <= max_audio_jump,
            "audio timestamp discontinuity between seq {} and {}: {} ticks",
            w[0].seq,
            w[1].seq,
            d
        );
    }
}
