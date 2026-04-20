use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::future::join_all;
use sipbot::config::{AccountConfig, Config};
use sipbot::csv_stats::{CsvStatsRecorder, write_final_summary};
use sipbot::sip;
use sipbot::stats::CallStats;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoLocal;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short = 'C', long, global = true)]
    conf: Option<PathBuf>,

    /// External IP address
    #[arg(short = 'E', long, global = true)]
    external: Option<String>,

    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initiate a call
    Call {
        /// Target URI (e.g., sip:user@domain)
        #[arg(short, long)]
        target: Option<String>,
        /// Username (e.g., sipbot)
        #[arg(short, long, alias = "caller")]
        username: Option<String>,
        /// Auth username (optional)
        #[arg(long)]
        auth_user: Option<String>,
        /// Auth password
        #[arg(long)]
        password: Option<String>,
        /// Register to SIP server before calling (optional domain)
        #[arg(long, num_args = 0..=1, default_missing_value = "")]
        register: Option<String>,
        /// From URI user part for outbound calls (e.g., anonymous)
        #[arg(long)]
        from: Option<String>,
        /// Hangup after seconds
        #[arg(long)]
        hangup: Option<u64>,
        /// Play file (wav)
        #[arg(long)]
        play: Option<String>,
        /// Use local audio device for playback and capture
        #[arg(long)]
        local: bool,
        /// Record to file (wav)
        #[arg(long)]
        record: Option<String>,
        /// Enable SRTP/SDES
        #[arg(long)]
        srtp: bool,
        /// Enable NACK
        #[arg(long)]
        nack: bool,
        /// Enable Jitter Buffer
        #[arg(long)]
        jitter: bool,
        /// Total number of calls to make
        #[arg(long, default_value = "1")]
        total: u32,
        /// Calls per second
        #[arg(long, default_value = "1")]
        cps: u32,
        /// Cancel probability (0-99%)
        #[arg(long, default_value = "0")]
        cancel_prob: u8,
        /// Codecs to use (e.g., opus,g722,pcmu)
        #[arg(long, value_delimiter = ',')]
        codecs: Option<Vec<String>>,
        /// Custom headers (e.g., -H 'X-Custom: value')
        #[arg(short = 'H', long = "header")]
        headers: Option<Vec<String>>,
        /// Output statistics to CSV file
        #[arg(long)]
        csv_output: Option<String>,
        /// CSV output interval in seconds
        #[arg(long, default_value = "5")]
        csv_interval: u64,
        /// Bind address (e.g., 0.0.0.0:5060)
        #[arg(short, long)]
        addr: Option<String>,
    },
    /// Wait for incoming calls
    Wait {
        /// Bind address (e.g., 0.0.0.0:5060)
        #[arg(short, long, default_value = "0.0.0.0:5060")]
        addr: String,
        /// Username (e.g., sipbot)
        #[arg(short, long)]
        username: Option<String>,
        /// Auth username (optional)
        #[arg(long)]
        auth_user: Option<String>,
        /// Domain/Realm (e.g., 127.0.0.1)
        #[arg(short, long, alias = "realm")]
        domain: Option<String>,
        /// Password for registration
        #[arg(short, long)]
        password: Option<String>,
        /// Register to SIP server (optional domain)
        #[arg(long, num_args = 0..=1, default_missing_value = "")]
        register: Option<String>,
        /// Ringback file (wav)
        #[arg(long)]
        ringback: Option<String>,
        /// Ring duration in seconds
        #[arg(long)]
        ring_duration: Option<u64>,
        /// Answer and play file (wav)
        #[arg(long)]
        answer: Option<String>,
        /// Answer and echo
        #[arg(long)]
        echo: bool,
        /// Answer and use local audio device
        #[arg(long)]
        local: bool,
        /// Hangup after seconds
        #[arg(long)]
        hangup: Option<u64>,
        /// Reject with code (e.g. 486, 603)
        #[arg(long)]
        reject: Option<u16>,
        /// Reject probability (1-99%)
        #[arg(long)]
        reject_prob: Option<u8>,
        /// Enable SRTP/SDES
        #[arg(long)]
        srtp: bool,
        /// Enable NACK
        #[arg(long)]
        nack: bool,
        /// Enable Jitter Buffer
        #[arg(long)]
        jitter: bool,
        /// Codecs to use (e.g., opus,g722,pcmu)
        #[arg(long, value_delimiter = ',')]
        codecs: Option<Vec<String>>,
        /// Custom headers (e.g., -H 'X-Custom: value')
        #[arg(short = 'H', long = "header")]
        headers: Option<Vec<String>>,
    },
    /// Send OPTIONS request
    Options {
        /// Target URI (e.g., sip:user@domain)
        target: Option<String>,
    },
    /// Send INFO request
    Info {
        /// Target URI (e.g., sip:user@domain)
        target: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let log_level = match &args.verbose {
        true => "info",
        _ => "error",
    };

    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f%:z".to_string());
    tracing_subscriber::fmt()
        .with_timer(timer)
        .with_line_number(true)
        .with_file(true)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level)),
        )
        .init();

    let config_path = if let Some(path) = args.conf {
        path
    } else {
        let home = std::env::home_dir().expect("Can't get home directory");
        PathBuf::from(home).join(".sipbot.toml")
    };

    let mut config = if config_path.exists() {
        info!("Loading configuration from {:?}", config_path);
        Config::load(&config_path).await?
    } else {
        match &args.command {
            Commands::Options { .. } | Commands::Info { .. } => {
                info!(
                    "Configuration file not found, using default configuration for standalone command"
                );
                Config {
                    addr: Some("0.0.0.0:0".to_string()),
                    external_ip: None,
                    recorders: None,
                    accounts: vec![AccountConfig {
                        username: "sipbot".to_string(),
                        auth_username: None,
                        domain: "127.0.0.1".to_string(),
                        password: None,
                        proxy: None,
                        register: Some(false),
                        from_user: None,
                        target: None,
                        record: None,
                        srtp_enabled: None,
                        nack_enabled: None,
                        jitter_buffer_enabled: None,
                        reject_prob: None,
                        cancel_prob: 0,
                        early_media: None,
                        ring: None,
                        answer: None,
                        hangup: None,
                        codecs: None,
                        headers: None,
                        refer_reject: None,
                    }],
                }
            }
            Commands::Call {
                target: Some(_),
                codecs,
                addr,
                ..
            } => {
                info!(
                    "Configuration file not found, using default configuration for standalone command"
                );
                Config {
                    addr: Some(addr.clone().unwrap_or_else(|| "0.0.0.0:0".to_string())),
                    external_ip: None,
                    recorders: None,
                    accounts: vec![AccountConfig {
                        username: "sipbot".to_string(),
                        auth_username: None,
                        domain: "127.0.0.1".to_string(),
                        password: None,
                        proxy: None,
                        register: Some(false),
                        from_user: None,
                        target: None,
                        record: None,
                        srtp_enabled: None,
                        nack_enabled: None,
                        jitter_buffer_enabled: None,
                        reject_prob: None,
                        cancel_prob: 0,
                        early_media: None,
                        ring: None,
                        answer: None,
                        hangup: None,
                        codecs: codecs.clone(),
                        headers: None,
                        refer_reject: None,
                    }],
                }
            }
            Commands::Wait {
                addr,
                username,
                auth_user,
                domain,
                password,
                register,
                ringback,
                ring_duration,
                answer,
                echo,
                local,
                hangup,
                reject,
                reject_prob,
                srtp,
                nack,
                jitter,
                codecs,
                headers,
            } => {
                info!("Configuration file not found, using default configuration for wait command");

                let ring_config = if ringback.is_some() || ring_duration.is_some() {
                    Some(sipbot::config::RingConfig {
                        duration_secs: ring_duration.unwrap_or(5),
                        ringback: ringback.clone(),
                        local: Some(*local),
                    })
                } else {
                    None
                };

                let answer_config = if *echo {
                    Some(sipbot::config::AnswerConfig::Echo)
                } else if *local {
                    Some(sipbot::config::AnswerConfig::Local)
                } else if let Some(file) = answer {
                    Some(sipbot::config::AnswerConfig::Play {
                        wav_file: file.clone(),
                    })
                } else {
                    None
                };

                let hangup_config = if let Some(code) = reject {
                    Some(sipbot::config::HangupConfig {
                        code: *code,
                        after_secs: None,
                    })
                } else if let Some(secs) = hangup {
                    Some(sipbot::config::HangupConfig {
                        code: 200,
                        after_secs: Some(*secs),
                    })
                } else {
                    None
                };

                let is_register = register.is_some() || password.is_some();
                info!("Parsed config: nack={}, jitter={}", *nack, *jitter);
                let reg_target = if let Some(r) = register {
                    if r.is_empty() { None } else { Some(r.clone()) }
                } else {
                    None
                };

                Config {
                    addr: Some(addr.clone()),
                    external_ip: None,
                    recorders: None,
                    accounts: vec![AccountConfig {
                        username: username.clone().unwrap_or("sipbot".to_string()),
                        auth_username: auth_user.clone(),
                        domain: domain
                            .clone()
                            .or(reg_target.clone())
                            .unwrap_or("127.0.0.1".to_string()),
                        password: password.clone(),
                        proxy: reg_target,
                        register: Some(is_register),
                        from_user: None,
                        target: None,
                        record: None,
                        srtp_enabled: Some(*srtp),
                        nack_enabled: Some(*nack),
                        jitter_buffer_enabled: Some(*jitter),
                        reject_prob: *reject_prob,
                        cancel_prob: 0,
                        early_media: None,
                        ring: ring_config,
                        answer: answer_config,
                        hangup: hangup_config,
                        codecs: codecs.clone(),
                        headers: headers.clone(),
                        refer_reject: None,
                    }],
                }
            }
            _ => {
                info!("Loading configuration from {:?}", config_path);
                Config::load(&config_path).await?
            }
        }
    };

    if let Some(external_ip) = args.external {
        config.external_ip = Some(external_ip);
    }

    let (
        command_name,
        target_override,
        caller_override,
        auth_user_override,
        password_override,
        hangup_override,
        play_file_override,
        record_override,
        srtp_override,
        nack_override,
        jitter_buffer_override,
        local_override,
        total_calls,
        cps,
        register_override,
        proxy_override,
        cancel_prob_override,
        codecs_override,
        headers_override,
        csv_output_path,
        csv_interval,
        from_override,
        addr_override,
    ) = match &args.command {
        Commands::Call {
            target,
            username,
            auth_user,
            password,
            register,
            from,
            hangup,
            play,
            local,
            record,
            srtp,
            nack,
            jitter,
            total,
            cps,
            cancel_prob,
            codecs,
            headers,
            csv_output,
            csv_interval,
            addr,
        } => {
            let is_register = register.is_some() || password.is_some();
            let reg_target = if let Some(r) = register {
                if r.is_empty() { None } else { Some(r.clone()) }
            } else {
                None
            };
            (
                "call",
                target.clone(),
                username.clone(),
                auth_user.clone(),
                password.clone(),
                *hangup,
                play.clone(),
                record.clone(),
                *srtp,
                Some(*nack),
                Some(*jitter),
                *local,
                *total,
                *cps,
                is_register,
                reg_target,
                *cancel_prob,
                codecs.clone(),
                headers.clone(),
                csv_output.clone(),
                *csv_interval,
                from.clone(),
                addr.clone(),
            )
        }
        Commands::Wait {
            srtp,
            nack,
            jitter,
            password,
            register,
            username,
            auth_user,
            local,
            codecs,
            headers,
            ..
        } => {
            let is_register = register.is_some() || password.is_some();
            let reg_target = if let Some(r) = register {
                if r.is_empty() { None } else { Some(r.clone()) }
            } else {
                None
            };
            (
                "wait",
                None,
                username.clone(),
                auth_user.clone(),
                password.clone(),
                None,
                None,
                None,
                *srtp,
                Some(*nack),
                Some(*jitter),
                *local,
                1,
                1,
                is_register,
                reg_target,
                0,
                codecs.clone(),
                headers.clone(),
                None,
                5, // default csv_interval
                None,
                None,
            )
        }
        Commands::Options { target } => (
            "options",
            target.clone(),
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            1,
            1,
            false,
            None,
            0,
            None,
            None,
            None,
            5,
            None,
            None,
        ),
        Commands::Info { target } => (
            "info",
            target.clone(),
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            1,
            1,
            false,
            None,
            0,
            None,
            None,
            None,
            5,
            None,
            None,
        ),
    };

    if let Some(addr) = &addr_override {
        config.addr = Some(addr.clone());
    }

    let mut handles = vec![];
    let global_config = config.clone();
    let shared_stats = Arc::new(CallStats::new());
    let cancel_token = CancellationToken::new();

    // Start CSV recorder if requested
    let _csv_handle = if let Some(ref csv_path) = csv_output_path {
        if command_name == "call" {
            let csv_recorder = Arc::new(CsvStatsRecorder::new(
                shared_stats.clone(),
                csv_path.clone(),
                csv_interval,
            ));
            println!("[*] CSV Output: {} (interval: {}s)", csv_path, csv_interval);
            Some(csv_recorder.spawn())
        } else {
            None
        }
    } else {
        None
    };

    if command_name == "call" {
        println!(
            "[*] Target:    {}",
            target_override.as_deref().unwrap_or("")
        );
        println!("[*] Rate:      {} CPS", cps);
        println!("[*] Duration:  {}s per call", hangup_override.unwrap_or(0));
        println!("[*] Press Ctrl+C to stop and see the report.\n");
    }

    for mut account in config.accounts {
        let global_config = global_config.clone();
        let target_override = target_override.clone();
        let caller_override = caller_override.clone();
        let auth_user_override = auth_user_override.clone();
        let password_override = password_override.clone();
        let play_file_override = play_file_override.clone();
        let record_override = record_override.clone();
        let stats = shared_stats.clone();

        if let Some(target) = &target_override {
            account.target = Some(target.clone());
        }

        if let Some(record) = &record_override {
            account.record = Some(record.clone());
        }

        if srtp_override {
            account.srtp_enabled = Some(true);
        }

        if let Some(nack) = nack_override {
            account.nack_enabled = Some(nack);
        }

        if let Some(jb) = jitter_buffer_override {
            account.jitter_buffer_enabled = Some(jb);
        }

        if let Some(codecs) = &codecs_override {
            account.codecs = Some(codecs.clone() as Vec<String>);
        }

        if let Some(headers) = &headers_override {
            account.headers = Some(headers.clone() as Vec<String>);
        }

        if let Some(play_file) = &play_file_override {
            account.answer = Some(sipbot::config::AnswerConfig::Play {
                wav_file: play_file.clone(),
            });
        } else if local_override {
            account.answer = Some(sipbot::config::AnswerConfig::Local);
        } else if cps == 1 && command_name == "call" {
            account.answer = Some(sipbot::config::AnswerConfig::Local);
        }

        if cancel_prob_override > 0 {
            account.cancel_prob = cancel_prob_override;
        }

        if let Some(hangup) = hangup_override {
            if let Some(ref mut h) = account.hangup {
                h.after_secs = Some(hangup);
            } else {
                account.hangup = Some(sipbot::config::HangupConfig {
                    code: 0,
                    after_secs: Some(hangup),
                });
            }
        }

        if let Some(caller) = &caller_override {
            if caller.starts_with("sip:") {
                if let Ok(uri) = rsipstack::rsip::Uri::try_from(caller.as_str()) {
                    if let Some(user) = uri.user() {
                        account.username = user.to_string();
                    }
                    let host = uri.host_with_port.to_string();
                    account.domain = host;
                }
            } else {
                account.username = caller.clone();
            }
        }

        if let Some(from) = &from_override {
            account.from_user = Some(from.clone());
        }

        if let Some(auth_user) = &auth_user_override {
            account.auth_username = Some(auth_user.clone());
        }

        if let Some(password) = &password_override {
            account.password = Some(password.clone());
        }

        info!(
            "[{}] Final account config: username={}, domain={}",
            account.username, account.username, account.domain
        );

        if register_override {
            account.register = Some(true);
        }

        if let Some(proxy) = &proxy_override {
            account.proxy = Some(proxy.clone() as String);
        }

        let verbose = args.verbose;
        let token = cancel_token.clone();
        let handle = tokio::spawn(async move {
            let mut bot = sip::SipBot::new(account, global_config, stats, verbose, token);
            match command_name {
                "call" => {
                    if let Err(e) = bot.run_call(total_calls, cps).await {
                        error!("Bot call error: {:?}", e);
                    }
                }
                "wait" => {
                    if let Err(e) = bot.run_wait().await {
                        error!("Bot wait error: {:?}", e);
                    }
                }
                "options" => {
                    if let Err(e) = bot.run_options(target_override).await {
                        error!("Bot options error: {:?}", e);
                    }
                }
                "info" => {
                    if let Err(e) = bot.run_info(target_override).await {
                        error!("Bot info error: {:?}", e);
                    }
                }
                _ => {}
            }
        });
        handles.push(handle);
    }

    let all_bots = join_all(handles);
    tokio::pin!(all_bots);

    match tokio::select! {
        _ = &mut all_bots => { Ok(()) }
        _ = tokio::signal::ctrl_c() => {
            println!("\n[!] Ctrl+C received, shutting down...");
            Err(()) }
    } {
        Ok(_) => info!("All bots finished."),
        Err(_) => {
            info!("Cancelled, hanging up active calls...");
            cancel_token.cancel();
            // Wait for bots to finish cleanup
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                    error!("Timeout waiting for bots to finish cleanup.");
                }
                _ = &mut all_bots => {}
                _ = tokio::signal::ctrl_c() => {}
            }

            if shared_stats.current() > 0 {
                tracing::warn!(
                    "Exiting with {} active calls still tracked in stats (should be 0). This may happen if some calls did not hang up gracefully or tasks were dropped.",
                    shared_stats.current()
                );
            } else {
                info!("All active calls cleared.");
            }
        }
    }

    shared_stats.print_summary();

    // Write final summary to file if CSV output was requested
    if let Some(ref csv_path) = csv_output_path {
        let summary_path = if csv_path.ends_with(".csv") {
            csv_path.replace(".csv", "_summary.txt")
        } else {
            format!("{}_summary.txt", csv_path)
        };
        if let Err(e) = write_final_summary(&shared_stats, &summary_path).await {
            error!("Failed to write final summary: {}", e);
        }
    }

    Ok(())
}
