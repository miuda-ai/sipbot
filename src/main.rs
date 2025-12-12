use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use sipbot::config::{AccountConfig, Config};
use sipbot::sip;
use std::path::PathBuf;
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
    external_ip: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initiate a call
    Call {
        /// Target URI (e.g., sip:user@domain)
        #[arg(short, long)]
        target: Option<String>,
        /// Caller (username or full URI e.g. sip:user@domain)
        #[arg(short, long)]
        caller: Option<String>,
        /// Auth username (optional)
        #[arg(long)]
        auth_user: Option<String>,
        /// Auth password
        #[arg(long)]
        password: Option<String>,
        /// Hangup after seconds
        #[arg(long)]
        hangup: Option<u64>,
        /// Play file (wav)
        #[arg(long)]
        play_file: Option<String>,
        /// Enable SRTP/SDES
        #[arg(long)]
        srtp: bool,
    },
    /// Wait for incoming calls
    Wait {
        /// Bind address (e.g., 0.0.0.0:5060)
        #[arg(short, long)]
        addr: Option<String>,
        /// Username (e.g., sipbot)
        #[arg(short, long)]
        username: Option<String>,
        /// Domain (e.g., 127.0.0.1)
        #[arg(short, long)]
        domain: Option<String>,
        /// Ringback file (wav)
        #[arg(long)]
        ringback: Option<String>,
        /// Ring duration in seconds
        #[arg(long)]
        ring_duration: Option<u64>,
        /// Answer and play file (wav)
        #[arg(long)]
        answer_file: Option<String>,
        /// Answer and echo
        #[arg(long)]
        echo: bool,
        /// Hangup after seconds
        #[arg(long)]
        hangup_after: Option<u64>,
        /// Reject with code (e.g. 486, 603)
        #[arg(long)]
        reject_code: Option<u16>,
        /// Enable SRTP/SDES
        #[arg(long)]
        srtp: bool,
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
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f%:z".to_string());
    tracing_subscriber::fmt()
        .with_timer(timer)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let config_path = if let Some(path) = args.conf {
        path
    } else {
        let home = std::env::var("HOME").context("HOME environment variable not set")?;
        PathBuf::from(home).join(".sipbot.toml")
    };

    let mut config = if config_path.exists() {
        info!("Loading configuration from {:?}", config_path);
        Config::load(&config_path).await?
    } else {
        match &args.command {
            Commands::Options { .. }
            | Commands::Info { .. }
            | Commands::Call {
                target: Some(_), ..
            } => {
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
                        target: None,
                        srtp_enabled: None,
                        early_media: None,
                        ring: None,
                        answer: None,
                        hangup: None,
                    }],
                }
            }
            Commands::Wait {
                addr,
                username,
                domain,
                ringback,
                ring_duration,
                answer_file,
                echo,
                hangup_after,
                reject_code,
                srtp,
            } => {
                info!("Configuration file not found, using default configuration for wait command");

                let ring_config = if ringback.is_some() || ring_duration.is_some() {
                    Some(sipbot::config::RingConfig {
                        duration_secs: ring_duration.unwrap_or(5),
                        ringback: ringback.clone(),
                    })
                } else {
                    None
                };

                let answer_config = if *echo {
                    Some(sipbot::config::AnswerConfig::Echo)
                } else if let Some(file) = answer_file {
                    Some(sipbot::config::AnswerConfig::Play {
                        wav_file: file.clone(),
                    })
                } else {
                    None
                };

                let hangup_config = if let Some(code) = reject_code {
                    Some(sipbot::config::HangupConfig {
                        code: *code,
                        after_secs: None,
                    })
                } else if let Some(secs) = hangup_after {
                    Some(sipbot::config::HangupConfig {
                        code: 200,
                        after_secs: Some(*secs),
                    })
                } else {
                    None
                };

                Config {
                    addr: addr.clone().or(Some("0.0.0.0:5060".to_string())),
                    external_ip: None,
                    recorders: None,
                    accounts: vec![AccountConfig {
                        username: username.clone().unwrap_or("sipbot".to_string()),
                        auth_username: None,
                        domain: domain.clone().unwrap_or("127.0.0.1".to_string()),
                        password: None,
                        proxy: None,
                        register: Some(false),
                        target: None,
                        srtp_enabled: Some(*srtp),
                        early_media: None,
                        ring: ring_config,
                        answer: answer_config,
                        hangup: hangup_config,
                    }],
                }
            }
            _ => {
                info!("Loading configuration from {:?}", config_path);
                Config::load(&config_path).await?
            }
        }
    };

    if let Some(external_ip) = args.external_ip {
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
        srtp_override,
    ) = match args.command {
        Commands::Call {
            target,
            caller,
            auth_user,
            password,
            hangup,
            play_file,
            srtp,
        } => (
            "call", target, caller, auth_user, password, hangup, play_file, srtp,
        ),
        Commands::Wait { srtp, .. } => ("wait", None, None, None, None, None, None, srtp),
        Commands::Options { target } => ("options", target, None, None, None, None, None, false),
        Commands::Info { target } => ("info", target, None, None, None, None, None, false),
    };

    let mut handles = vec![];
    let global_config = config.clone();

    for mut account in config.accounts {
        let global_config = global_config.clone();
        let target_override = target_override.clone();
        let caller_override = caller_override.clone();
        let auth_user_override = auth_user_override.clone();
        let password_override = password_override.clone();
        let play_file_override = play_file_override.clone();

        if let Some(target) = &target_override {
            account.target = Some(target.clone());
        }

        if srtp_override {
            account.srtp_enabled = Some(true);
        }

        if let Some(play_file) = &play_file_override {
            account.answer = Some(sipbot::config::AnswerConfig::Play {
                wav_file: play_file.clone(),
            });
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
                if let Ok(uri) = rsip::Uri::try_from(caller.as_str()) {
                    if let Some(user) = uri.user() {
                        account.username = user.to_string();
                    }
                    let host = uri.host();
                    account.domain = host.to_string();
                }
            } else {
                account.username = caller.clone();
            }
        }

        if let Some(auth_user) = &auth_user_override {
            account.auth_username = Some(auth_user.clone());
        }

        if let Some(password) = &password_override {
            account.password = Some(password.clone());
        }

        let handle = tokio::spawn(async move {
            let mut bot = sip::SipBot::new(account, global_config);
            match command_name {
                "call" => {
                    if let Err(e) = bot.run_call().await {
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

    // Wait for all bots
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}
