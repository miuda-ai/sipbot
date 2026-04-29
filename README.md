# SipBot

[![Crates.io](https://img.shields.io/crates/v/sipbot.svg)](https://crates.io/crates/sipbot)
[![Documentation](https://docs.rs/sipbot/badge.svg)](https://docs.rs/sipbot)

A flexible, high-performance SIP bot implementation in Rust, designed for testing and simulating SIP call flows. It uses [rsipstack](https://github.com/restsend/rsipstack) for signaling and supports customizable call handling stages including ringing, answering, media playback, echo, and automatic hangup.

The media transport uses [rustrtc](https://github.com/restsend/rustrtc).

## Features

- **Multi-Account Support**: Configure multiple SIP accounts in a single instance.
- **Flexible Call Flow Stages**:
  - **Ringing (Stage 1)**: Send `180 Ringing` or `183 Session Progress` with custom ringback tone (WAV).
  - **Answer (Stage 2)**: Auto-answer calls with `200 OK`.
  - **Media Handling**:
    - **Play**: Play a specified `.wav` file.
    - **Echo**: Echo received RTP packets back to the sender (Latency testing).
    - **Local**: Use local audio device for playback and capture (Mic/Speaker).
  - **Hangup (Stage 3)**: Automatically hang up after a configurable duration or reject calls with specific SIP codes.
- **Outbound Calls**: Ability to initiate calls to a target URI.
- **Call Recording**: (Experimental) Record call audio to WAV files. (Requires configuration)
- **DTMF (RFC 2833)**: Send DTMF digits via keyboard during single calls; receive and log DTMF events from remote endpoints.
- **Audio Quality Analysis**: Per-call audio quality monitoring with RMS, clipping, DC offset, zero-crossing rate, spectral tilt, shrill/muffled classification, and sample-rate mismatch detection.
- **Registration**: Supports SIP registration with authentication (WIP).

## Quick start

Install the `sipbot` from `crates.io`.

```bash
cargo install sipbot
```

## Build from source

Linux users can build a static binary with musl for maximum compatibility:
```bash
cargo build -r --target x86_64-unknown-linux-musl --no-default-features
```

### CLI Usage

You can also run `sipbot` with CLI arguments for quick testing.

#### Global Options

- `-C, --conf <FILE>`: Path to the configuration file.
- `-E, --external <IP>`: External IP address for SDP (NAT traversal).
- `-v, --verbose`: Enable verbose logging.

#### Initiate a Call

```bash
cargo run -- call -t sip:user@domain -u sipbot --play audio.wav --hangup 10 --total 10 --cps 2
```

- `-t, --target <TARGET>`: Target URI (e.g., sip:user@domain).
- `-u, --username <USER>`: Username (e.g., sipbot). Alias: `--caller`.
- `--auth-user <USER>`: Auth username (optional).
- `--password <PASS>`: Auth password.
- `--register [DOMAIN]`: Register to SIP server before calling (optional domain).
- `--hangup <SECONDS>`: Hangup after seconds.
- `--play <FILE>`: Play file (wav).
- `--local`: Use local audio device for playback and capture.
- `--record <FILE>`: Record to file (wav). If multiple calls are made, the filename will be suffixed with the call index (e.g., `record_1.wav`).
- `--srtp`: Enable SRTP/SDES.
- `--nack`: Enable NACK.
- `--jitter`: Enable Jitter Buffer.
- `--total <COUNT>`: Total number of calls to make (default: 1).
- `--cps <COUNT>`: Calls per second (default: 1).
- `--cancel-prob <PROB>`: Cancel probability (0-99%) (default: 0).
- `--codecs <LIST>`: Codecs to use (e.g., opus,g722,pcmu).
- `--audio-quality`: Enable per-call audio quality analysis (RMS, clipping, DC offset, spectral tilt, shrill/muffled detection).
- `--csv-output <FILE>`: Output periodic statistics to CSV file.
- `--csv-interval <SECONDS>`: CSV output interval in seconds (default: 5).
- `--from <USER>`: From URI user part for outbound calls (e.g., anonymous).
- `-H, --header <HEADER>`: Add custom SIP header (e.g., `-H 'X-Custom: value'`). Can be used multiple times.

> **Tip**: When making a single call (`--total 1`), you can send DTMF digits by typing them in the terminal. Supported: `0-9`, `*`, `#`, `A-D`. Press `q` to quit the DTMF reader.

```bash
cargo run -- wait --addr 0.0.0.0:5060 -u sipbot --answer welcome.wav
```

- `-a, --addr <ADDR>`: Bind address (e.g., 0.0.0.0:5060).
- `-u, --username <USER>`: Username (e.g., sipbot).
- `-d, --domain <DOMAIN>`: Domain/Realm (e.g., 127.0.0.1). Alias: `--realm`.
- `--auth-user <USER>`: Auth username (optional).
- `-p, --password <PASS>`: Password for registration.
- `--register [DOMAIN]`: Register to SIP server (optional domain).
- `--ringback <FILE>`: Ringback file (wav).
- `--ring-duration <SECONDS>`: Ring duration in seconds.
- `--answer <FILE>`: Answer and play file (wav).
- `--echo`: Answer and echo.
- `--local`: Answer and use local audio device.
- `--hangup <SECONDS>`: Hangup after seconds.
- `--reject <CODE>`: Reject with code (e.g. 486, 603).
- `--reject-prob <PROB>`: Randomly reject call with probability 1-99% (default code 480).
- `--srtp`: Enable SRTP/SDES.
- `--nack`: Enable NACK.
- `--jitter`: Enable Jitter Buffer.
- `--codecs <LIST>`: Codecs to use (e.g., opus,g722,pcmu).
- `--audio-quality`: Enable per-call audio quality analysis.
- `-H, --header <HEADER>`: Add custom SIP header (e.g., `-H 'X-Custom: value'`). Can be used multiple times.

> **Note**: By default, `wait` mode does not record calls. To enable recording, you must use a configuration file and specify the `recorders` directory.

#### Other Commands

- `options`: Send OPTIONS request.
- `info`: Send INFO request.

## Typical Usage Examples

### 1. Echo Test (Latency & Connectivity)

Answer incoming calls and echo the audio back to the caller. This is perfect for testing network latency and packet loss.

```bash
sipbot wait --username echo-bot --echo
```

### 2. Intercom Mode (Local Audio)

Use your computer's microphone and speakers to talk to a SIP caller.

```bash
sipbot wait --username intercom --local
```

### 3. Automated Announcement

Answer calls, play a welcome message, and hang up after 10 seconds.

```bash
sipbot wait --username announcement --answer welcome.wav --hangup 10
```

### 4. Custom SIP Headers

Add custom SIP headers to outgoing calls and incoming responses:

```bash
# Outgoing call with custom headers
sipbot call -t sip:user@domain -u caller -H 'X-Call-Type: test' -H 'X-Session-ID: 12345' --hangup 5

# Incoming call handling with custom headers in response
sipbot wait --username responder --answer welcome.wav -H 'X-Server-ID: bot-01' -H 'X-Region: US-West'
```

```bash
sipbot wait --username announcer --answer welcome.wav --hangup 10
```

### 4. Load Testing (Outbound)

Make 100 calls to a target, with 5 calls per second, playing an audio file for 30 seconds each.

```bash
sipbot call -t sip:100@192.168.1.10 --play music.wav --total 100 --cps 5 --hangup 30
```

### 5. High-Quality Audio (G.722)

SipBot supports G.722 (16kHz) for high-definition audio. It automatically negotiates the best codec.

```bash
# Make a call using G.722 if supported by the remote end
sipbot call -t sip:hd-user@domain --play high_res.wav
```

### 6. Call Recording

Record all incoming calls to a specific directory. (Requires `recorders` path in `config.toml`)

```bash
# In config.toml:
# recorders = "./wavs"

sipbot wait --username recorder-bot
```

### 7. Send DTMF Digits During a Call

Make a single call and send DTMF digits interactively from the terminal:

```bash
sipbot call -t sip:user@domain -u sipbot --hangup 30
# Type digits (0-9, *, #, A-D) and press Enter to send DTMF
# Press 'q' to quit the DTMF reader
```

### 8. Audio Quality Analysis

Enable per-call audio quality analysis to detect clipping, silence, shrill/muffled audio, and sample rate mismatches:

```bash
# Outbound call with audio quality monitoring
sipbot call -t sip:user@domain -u sipbot --hangup 30 --audio-quality

# Wait for calls with audio quality monitoring
sipbot wait --username quality-bot --echo --audio-quality
```

## Configuration

Create a `config.toml` file in the root directory. The configuration allows you to define the behavior for each account.

### Example `config.toml`

```toml
# Global settings
addr = "0.0.0.0:5060"           # Local bind address
external_ip = "1.2.3.4"         # External IP for SDP (NAT traversal)
recorders = "/tmp/recorders"    # Directory for recordings (Required for recording)

[[accounts]]
username = "1001"
domain = "sip.example.com"
password = "secretpassword"
register = true                 # Enable registration
reject_prob = 20                # Randomly reject 20% of calls with 480
codecs = ["opus", "g722", "pcmu"]  # Preferred codecs
headers = [                     # Custom SIP headers
    "X-Server-ID: sipbot-01",
    "X-Region: US-West",
    "X-Environment: production"
]

# --- Call Handling Flow ---

# Stage 1: Ringing
# Wait for 5 seconds before answering.
# If 'ringback' is provided, sends 183 Session Progress and plays the file.
# If 'ringback' is omitted, sends 180 Ringing.
[accounts.ring]
duration_secs = 5
# ringback = "sounds/ringback.wav" 

# Stage 2: Answer
# Answer the call (200 OK) and perform an action.
[accounts.answer]
action = "play"                 # Options: "play", "echo", "local"
wav_file = "sounds/welcome.wav" # Required if action is "play"

# [accounts.answer]
# action = "echo"               # Alternative: Echo test

# [accounts.answer]
# action = "local"              # Alternative: Use local audio device (Mic/Speaker)

# Stage 3: Hangup
# Automatically hang up after the media finishes or a timeout.
[accounts.hangup]
code = 200                      # SIP code (not fully used for BYE yet, mainly for rejection)
after_secs = 10                 # Send BYE after 10 seconds
```

### Configuration Reference

- **`addr`**: (Optional) The local IP and port to bind to. Defaults to `0.0.0.0:35060`.
- **`external_ip`**: (Optional) The external IP address to use in SDP offers/answers (useful for NAT).
- **`recorders`**: (Optional) Path to save call recordings.
- **`accounts`**: List of account configurations.
    - `username`: SIP username.
    - `auth_username`: (Optional) SIP authentication username.
    - `domain`: SIP domain/registrar.
    - `password`: SIP password.
    - `proxy`: (Optional) SIP proxy server address.
    - `register`: (Bool) Whether to register with the domain.
    - `reject_prob`: (Optional) Probability (1-99) to randomly reject incoming calls with 480.
    - `cancel_prob`: (Optional) Probability (1-99) to randomly cancel outgoing calls.
    - `target`: (Optional) URI to call on startup (for outbound bot).
    - `record`: (Optional) Recording file path.
    - `srtp_enabled`: (Bool) Enable SRTP/SDES.
    - `nack_enabled`: (Bool) Enable RTP NACK.
    - `jitter_buffer_enabled`: (Bool) Enable Jitter Buffer.
    - `codecs`: (Array) List of preferred codecs (e.g., `["opus", "g722", "pcmu"]`).
    - `headers`: (Array) List of custom SIP headers to include in INVITE requests and 200 OK responses (e.g., `["X-Custom-Header: value", "X-Call-ID: 12345"]`).
    - `audio_quality`: (Optional) Enable per-call audio quality analysis. Example:
        ```toml
        [accounts.audio_quality]
        enabled = true
        sample_rate_check = true
        clipping_threshold = 0.95
        shrill_threshold = 0.65
        muffled_threshold = 0.15
        silence_threshold_rms = 50.0
        report_interval = 100
        ```
    - `codecs`: (Optional) List of preferred codecs (e.g., `["opus", "g722", "pcmu"]`).
    - **`early_media`**: Configuration for the early media phase (183).
        - `wav_file`: (Optional) Path to WAV file.
        - `local`: (Optional) `true` to use local audio device for capture and playback.
    - **`ring`**: Configuration for the ringing phase.
        - `duration_secs`: How long to stay in ringing state.
        - `ringback`: (Optional) Path to WAV file for early media (183).
        - `local`: (Optional) `true` to use local audio device for capture and playback.
    - **`answer`**: Configuration for the answered phase.
        - `action`: `play`, `echo` or `local`.
        - `wav_file`: Path to WAV file (if action is `play`).
    - **`hangup`**: Configuration for ending the call.
        - `code`: SIP status code (used for rejection if no answer config exists).
        - `after_secs`: (Optional) Time in seconds to wait before sending BYE.

## Benchmarking and Testing

`SipBot` is designed to facilitate SIP performance testing and benchmarking by simulating multiple callers and callees with customizable behaviors.

### Example Scenario

To run a benchmark, you typically need two instances of `SipBot`: one for caller and another for callee. they will register on sip server with alice and bob respectively.

#### Callee Configuration (`callee.toml`)

This bot listens for incoming calls, rings for a few seconds, and randomly rejects 50% of the calls to test error handling.

```toml
# Local bind address for the callee
addr = "0.0.0.0:3333" 

[[accounts]]
register = true
proxy = "127.0.0.1:15060" # sip server address
username = "alice"
domain = "127.0.0.1"
password = "123456"
reject_prob = 20   # 20% probability to reject incoming calls

[accounts.hangup]
code = 486         # reject with code 486

[accounts.ring]
duration_secs = 3  # Wait for 3 seconds in ringing state before answering
```

#### Caller Configuration (`caller.toml`)

This bot initiates calls to a target, with a 30% chance of canceling the call before it is answered.

```toml
# Local bind address for the caller
addr = "0.0.0.0:4444" 

[[accounts]]
register = true
proxy = "127.0.0.1:15060"
username = "bob"
domain = "127.0.0.1"
password = "123456"
target = "sip:alice@127.0.0.1" # The destination to call

# Probability to randomly cancel calls
cancel_prob = 30

[accounts.hangup]
after_secs = 5     # If the call is answered, hang up (send BYE) after 5 seconds
code = 486         # currectly not used for caller
```

> Note: if cancel_prob is set, caller will randomly deside cancel before or after ringing 50:50.

### How to Run

0. **Start the Sip Server**

1. **Start the Callee**:
   The callee should be running first to receive calls.

   ```bash
   cargo run -- wait --conf callee.toml
   ```

2. **Start the Caller**:
   The caller initiates the calls. You can use `--total` and `--cps` to control the load.

   ```bash
   # Make 100 total calls, with a rate of 3 calls per second
   sipbot --conf caller.toml call --total 100 --cps 3
   ```

## License

MIT
