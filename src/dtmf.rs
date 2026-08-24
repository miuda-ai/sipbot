

pub const DTMF_TELEPHONE_EVENT_PT: u8 = 101;
pub const DTMF_CLOCK_RATE: u32 = 8000;
pub const DTMF_EVENT_DURATION_INC: u16 = 160;

const DTMF_DIGITS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '*', '#', 'A', 'B', 'C', 'D',
];

pub fn digit_to_event(digit: char) -> Option<u8> {
    DTMF_DIGITS.iter().position(|&d| d == digit).map(|i| i as u8)
}

pub fn event_to_digit(event: u8) -> Option<char> {
    DTMF_DIGITS.get(event as usize).copied()
}

#[derive(Debug, Clone)]
pub struct DtmfEvent {
    pub event: u8,
    pub end: bool,
    pub volume: u8,
    pub duration: u16,
}

pub const DTMF_PAYLOAD_SIZE: usize = 4;

pub fn encode_dtmf(ev: DtmfEvent) -> [u8; 4] {
    let mut buf = [0u8; 4];
    let e_bit = if ev.end { 0x80u8 } else { 0 };
    buf[0] = ev.event;
    buf[1] = e_bit | (ev.volume & 0x3F);
    buf[2] = (ev.duration >> 8) as u8;
    buf[3] = (ev.duration & 0xFF) as u8;
    buf
}

pub fn decode_dtmf(data: &[u8]) -> Option<DtmfEvent> {
    if data.len() < 4 {
        return None;
    }
    let event = data[0];
    let end = (data[1] & 0x80) != 0;
    let volume = data[1] & 0x3F;
    let duration = u16::from_be_bytes([data[2], data[3]]);
    Some(DtmfEvent {
        event,
        end,
        volume,
        duration,
    })
}

pub fn build_rtpmap_line(pt: u8, clock_rate: u32) -> String {
    format!("a=rtpmap:{} telephone-event/{}", pt, clock_rate)
}

pub fn build_fmtp_line(pt: u8) -> String {
    format!("a=fmtp:{} 0-16", pt)
}

/// A telephone-event payload type as advertised in an SDP `rtpmap` attribute,
/// with its clock rate. RFC 4733 requires telephone-event to use the same
/// clock rate as the audio codec it accompanies (8000 for PCMU/PCMA/G722,
/// 48000 for opus).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelephoneEventInfo {
    pub pt: u8,
    pub clock_rate: u32,
}

/// Parse ALL telephone-event `a=rtpmap:` entries from an SDP document,
/// preserving their advertised payload type and clock rate.
pub fn parse_telephone_events(sdp: &str) -> Vec<TelephoneEventInfo> {
    let mut events = Vec::new();
    for line in sdp.lines() {
        let lower = line.to_lowercase().trim().to_string();
        if !lower.contains("telephone-event") {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(pt) = parts
            .next()
            .and_then(|s| s.split(':').nth(1))
            .and_then(|s| s.parse::<u8>().ok())
        else {
            continue;
        };
        let clock_rate = parts
            .next()
            .and_then(|s| s.split('/').nth(1))
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(DTMF_CLOCK_RATE);
        events.push(TelephoneEventInfo { pt, clock_rate });
    }
    events
}

pub fn parse_telephone_event_pt(sdp: &str) -> Option<u8> {
    parse_telephone_events(sdp).first().map(|e| e.pt)
}

pub fn sdp_has_telephone_event(sdp: &str) -> bool {
    sdp.to_lowercase().contains("telephone-event")
}

pub fn inject_telephone_event_caps(sdp: &str, pt: u8) -> String {
    let mut lines: Vec<String> = sdp.lines().map(|l| l.to_string()).collect();
    if sdp_has_telephone_event(sdp) {
        return sdp.to_string();
    }

    let mut mline_idx = None;
    let mut last_rtpmap_idx = None;

    for (i, line) in lines.iter().enumerate() {
        let lower = line.to_lowercase();
        if lower.starts_with("m=audio") {
            mline_idx = Some(i);
        }
        if lower.starts_with("a=rtpmap:") {
            last_rtpmap_idx = Some(i);
        }
    }

    if let Some(mut idx) = mline_idx {
        if let Some(rtpmap_end) = last_rtpmap_idx {
            idx = rtpmap_end;
        }

        let mut new_pt = pt;
        if let Some(mline) = mline_idx {
            let parts: Vec<&str> = lines[mline].split_whitespace().collect();
            if parts.len() >= 4 {
                let audio_pts: Vec<&str> = parts[3..].to_vec();
                while audio_pts.contains(&new_pt.to_string().as_str()) {
                    new_pt += 1;
                }
            }
        }

        let mline = &mut lines[mline_idx.unwrap()];
        let payloads: Vec<&str> = mline.split_whitespace().collect();
        let mut new_mline = payloads[..3].join(" ");
        new_mline.push_str(&format!(" {}", new_pt));
        for pt_str in &payloads[3..] {
            new_mline.push_str(&format!(" {}", pt_str));
        }
        *mline = new_mline;

        lines.insert(idx + 1, build_rtpmap_line(new_pt, DTMF_CLOCK_RATE));
        lines.insert(idx + 2, build_fmtp_line(new_pt));
    }

    // RFC 4566 mandates CRLF line endings; belle-sip (Linphone) rejects LF-only SDP.
    let mut out = lines.join("\r\n");
    out.push_str("\r\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digit_event_roundtrip() {
        for (i, &ch) in DTMF_DIGITS.iter().enumerate() {
            assert_eq!(digit_to_event(ch), Some(i as u8));
            assert_eq!(event_to_digit(i as u8), Some(ch));
        }
    }

    #[test]
    fn test_invalid_digit() {
        assert_eq!(digit_to_event('X'), None);
        assert_eq!(digit_to_event(' '), None);
    }

    #[test]
    fn test_invalid_event() {
        assert_eq!(event_to_digit(16), None);
        assert_eq!(event_to_digit(255), None);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = DtmfEvent {
            event: 1,
            end: false,
            volume: 10,
            duration: 160,
        };
        let encoded = encode_dtmf(original.clone());
        assert_eq!(encoded.len(), 4);
        let decoded = decode_dtmf(&encoded).unwrap();
        assert_eq!(decoded.event, original.event);
        assert_eq!(decoded.end, original.end);
        assert_eq!(decoded.volume, original.volume);
        assert_eq!(decoded.duration, original.duration);
    }

    #[test]
    fn test_encode_decode_with_end_bit() {
        let original = DtmfEvent {
            event: 9,
            end: true,
            volume: 6,
            duration: 480,
        };
        let encoded = encode_dtmf(original.clone());
        let decoded = decode_dtmf(&encoded).unwrap();
        assert_eq!(decoded.event, 9);
        assert!(decoded.end);
        assert_eq!(decoded.volume, 6);
        assert_eq!(decoded.duration, 480);
    }

    #[test]
    fn test_decode_short_buffer() {
        assert!(decode_dtmf(&[0u8; 3]).is_none());
        assert!(decode_dtmf(&[]).is_none());
    }

    #[test]
    fn test_encode_all_digits() {
        for digit in DTMF_DIGITS {
            let ev = digit_to_event(*digit).unwrap();
            let encoded = encode_dtmf(DtmfEvent {
                event: ev,
                end: true,
                volume: 6,
                duration: 800,
            });
            let decoded = decode_dtmf(&encoded).unwrap();
            assert_eq!(event_to_digit(decoded.event), Some(*digit));
        }
    }

    #[test]
    fn test_rtpmap_line() {
        let line = build_rtpmap_line(101, 8000);
        assert_eq!(line, "a=rtpmap:101 telephone-event/8000");
    }

    #[test]
    fn test_rtpmap_line_opus_clock() {
        let line = build_rtpmap_line(110, 48000);
        assert_eq!(line, "a=rtpmap:110 telephone-event/48000");
    }

    #[test]
    fn test_parse_telephone_events_rates() {
        let sdp = "a=rtpmap:111 opus/48000/2\n\
                   a=rtpmap:110 telephone-event/48000\n\
                   a=rtpmap:126 telephone-event/8000\n";
        let events = parse_telephone_events(sdp);
        assert_eq!(
            events,
            vec![
                TelephoneEventInfo { pt: 110, clock_rate: 48000 },
                TelephoneEventInfo { pt: 126, clock_rate: 8000 },
            ]
        );
    }

    #[test]
    fn test_parse_telephone_events_none() {
        let sdp = "a=rtpmap:0 PCMU/8000\n";
        assert!(parse_telephone_events(sdp).is_empty());
    }


    #[test]
    fn test_fmtp_line() {
        let line = build_fmtp_line(101);
        assert_eq!(line, "a=fmtp:101 0-16");
    }

    #[test]
    fn test_parse_telephone_event_pt() {
        let sdp = "a=rtpmap:101 telephone-event/8000\n";
        assert_eq!(parse_telephone_event_pt(sdp), Some(101));
    }

    #[test]
    fn test_parse_telephone_event_pt_no_match() {
        let sdp = "a=rtpmap:0 PCMU/8000\n";
        assert_eq!(parse_telephone_event_pt(sdp), None);
    }

    #[test]
    fn test_sdp_has_telephone_event() {
        assert!(sdp_has_telephone_event("telephone-event/8000"));
        assert!(!sdp_has_telephone_event("PCMU/8000"));
    }

    #[test]
    fn test_inject_telephone_event_caps_new() {
        let sdp = "v=0\nm=audio 4000 RTP/AVP 0 8\na=rtpmap:0 PCMU/8000\na=rtpmap:8 PCMA/8000\n";
        let result = inject_telephone_event_caps(sdp, 101);
        assert!(result.contains("telephone-event"));
        assert!(result.contains("101"));
        assert!(result.contains("fmtp:101"));
    }

    #[test]
    fn test_inject_telephone_event_caps_already_present() {
        let sdp = "v=0\nm=audio 4000 RTP/AVP 0 101\na=rtpmap:0 PCMU/8000\na=rtpmap:101 telephone-event/8000\n";
        let result = inject_telephone_event_caps(sdp, 101);
        assert_eq!(result, sdp);
    }

    #[test]
    fn test_multiple_packets_sequence() {
        let events = vec![
            DtmfEvent { event: 1, end: false, volume: 10, duration: 0 },
            DtmfEvent { event: 1, end: false, volume: 10, duration: 160 },
            DtmfEvent { event: 1, end: false, volume: 10, duration: 320 },
            DtmfEvent { event: 1, end: true, volume: 10, duration: 480 },
        ];
        for ev in &events {
            let encoded = encode_dtmf(ev.clone());
            let decoded = decode_dtmf(&encoded).unwrap();
            assert_eq!(decoded.event, ev.event);
            assert_eq!(decoded.duration, ev.duration);
            assert_eq!(decoded.end, ev.end);
        }
    }

    #[test]
    fn test_volume_range() {
        for vol in [0, 6, 20, 36] {
            let ev = DtmfEvent {
                event: 0,
                end: true,
                volume: vol,
                duration: 160,
            };
            let encoded = encode_dtmf(ev.clone());
            let decoded = decode_dtmf(&encoded).unwrap();
            assert_eq!(decoded.volume, vol);
        }
    }
}
