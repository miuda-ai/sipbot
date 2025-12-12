# SipBot

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
    - **Hangup (Stage 3)**: Automatically hang up after a configurable duration or reject calls with specific SIP codes.
- **Outbound Calls**: Ability to initiate calls to a target URI.
- **Call Recording**: (Experimental) Record call audio to WAV files.
- **Registration**: Supports SIP registration with authentication (WIP).

## Quick start

Install the `sipbot` from `crates.io`.

```bash
cargo install sipbot
```

### CLI Usage

You can also run `sipbot` with CLI arguments for quick testing.

#### Global Options
- `-C, --conf <FILE>`: Path to the configuration file.
- `-E, --external-ip <IP>`: External IP address for SDP (NAT traversal).

#### Initiate a Call
```bash
cargo run -- call --target sip:user@domain --caller sip:me@mydomain --play-file audio.wav --hangup 10
```
- `--target`: The SIP URI to call.
- `--caller`: The caller's SIP URI or username.
- `--play-file`: Path to a WAV file to play when the call is answered.
- `--hangup`: Hangup after specified seconds.

#### Wait for Calls
```bash
cargo run -- wait --addr 0.0.0.0:5060 --username sipbot --answer-file welcome.wav
```

## Configuration

Create a `config.toml` file in the root directory. The configuration allows you to define the behavior for each account.

### Example `config.toml`

```toml
# Global settings
addr = "0.0.0.0:5060"           # Local bind address
external_ip = "1.2.3.4"         # External IP for SDP (NAT traversal)
recorders = "/tmp/recorders"    # Directory for recordings

[[accounts]]
username = "1001"
domain = "sip.example.com"
password = "secretpassword"
register = true                 # Enable registration

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
action = "play"                 # Options: "play", "echo"
wav_file = "sounds/welcome.wav" # Required if action is "play"

# [accounts.answer]
# action = "echo"               # Alternative: Echo test

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
    - `domain`: SIP domain/registrar.
    - `password`: SIP password.
    - `register`: (Bool) Whether to register with the domain.
    - `target`: (Optional) URI to call on startup (for outbound bot).
    - **`ring`**: Configuration for the ringing phase.
        - `duration_secs`: How long to stay in ringing state.
        - `ringback`: (Optional) Path to WAV file for early media (183).
    - **`answer`**: Configuration for the answered phase.
        - `action`: `play` or `echo`.
        - `wav_file`: Path to WAV file (if action is `play`).
    - **`hangup`**: Configuration for ending the call.
        - `code`: SIP status code (used for rejection if no answer config exists).
        - `after_secs`: (Optional) Time in seconds to wait before sending BYE.

## License

MIT
