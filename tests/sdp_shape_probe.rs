//! Regression test for the SDP offer shape emitted toward WebRTC/SIP peers:
//! - a complete offer (end-of-candidates) must not advertise trickle ICE
//! - Opus fmtp parameters must not leak onto G.722/G.729
//! - msid must be signalled exactly once, via the a=ssrc msid: attribute

use sipbot::media::MediaSession;
use sipbot::stats::CallStats;
use std::sync::Arc;

#[tokio::test]
async fn offer_sdp_shape() -> anyhow::Result<()> {
    let (_session, sdp) = MediaSession::new_offer(
        false,
        true,
        false,
        false,
        None,
        None,
        true,
        Arc::new(CallStats::new()),
        None,
        500,
    )
    .await?;

    println!("===== OFFER SDP =====\n{sdp}\n=====================");

    assert!(
        sdp.contains("a=end-of-candidates"),
        "offer must be a complete (vanilla) ICE offer"
    );
    assert!(
        !sdp.contains("a=ice-options:trickle"),
        "complete offer must not advertise trickle"
    );
    assert!(
        !sdp.contains("a=fmtp:9 ") && !sdp.contains("a=fmtp:18 "),
        "G722/G729 must not carry Opus fmtp parameters"
    );
    assert!(
        !sdp.lines().any(|l| l.starts_with("a=msid:")),
        "msid must be carried by a=ssrc only, not duplicated at media level"
    );
    let msid_count = sdp.matches(" msid:").count();
    assert_eq!(msid_count, 1, "exactly one msid attribute expected");

    Ok(())
}
