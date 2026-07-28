use anyhow::Result;
use sipbot::config::{AccountConfig, AnswerConfig, Config};
use sipbot::sip::SipBot;
use sipbot::stats::CallStats;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_call_flow() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 1. Configure Server (Wait)
    let server_addr = "127.0.0.1:5080";
    let server_config = Config {
        addr: Some(server_addr.to_string()),
        external_ip: None,
        recorders: Some("/tmp/recorders_test".to_string()),
        ws_url: None,
        accounts: vec![AccountConfig {
            username: "server".to_string(),
            domain: "127.0.0.1".to_string(),
            register: Some(false),
            answer: Some(AnswerConfig::Echo),
            ..Default::default()
        }],
    };

    // 2. Configure Client (Call)
    let client_addr = "127.0.0.1:5081";
    let client_config = Config {
        addr: Some(client_addr.to_string()),
        external_ip: None,
        recorders: None,
        ws_url: None,
        accounts: vec![AccountConfig {
            username: "client".to_string(),
            domain: "127.0.0.1".to_string(),
            register: Some(false),
            target: Some(format!("sip:server@{}", server_addr)),
            hangup: Some(sipbot::config::HangupConfig {
                code: 200,
                after_secs: Some(5),
            }),
            ..Default::default()
        }],
    };

    // 3. Start Server
    let server_handle = tokio::spawn(async move {
        let stats = Arc::new(CallStats::new());
        let mut bot = SipBot::new(
            server_config.accounts[0].clone(),
            server_config,
            stats,
            false,
            CancellationToken::new(),
        );
        if let Err(e) = bot.run_wait().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Give server time to start
    sleep(Duration::from_secs(1)).await;

    // 4. Start Client
    let client_handle = tokio::spawn(async move {
        let stats = Arc::new(CallStats::new());
        let mut bot = SipBot::new(
            client_config.accounts[0].clone(),
            client_config,
            stats,
            false,
            CancellationToken::new(),
        );
        if let Err(e) = bot.run_call(1, 1).await {
            eprintln!("Client error: {:?}", e);
            panic!("Client failed: {:?}", e);
        }
    });

    // 5. Wait for call flow to complete
    // The client logic waits 5 seconds after answer, then exits run_call (mostly).
    // We can wait a bit longer here.

    // We need to ensure the client actually finishes successfully.
    // Since run_call returns Ok(()), we can await the handle.

    match tokio::time::timeout(Duration::from_secs(10), client_handle).await {
        Ok(res) => match res {
            Ok(_) => println!("Client finished successfully"),
            Err(e) => panic!("Client task panicked: {:?}", e),
        },
        Err(_) => panic!("Test timed out"),
    }

    // Cleanup server
    server_handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_options_flow() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 1. Configure Server (Wait)
    let server_addr = "127.0.0.1:5070";
    let server_config = Config {
        addr: Some(server_addr.to_string()),
        external_ip: None,
        recorders: None,
        ws_url: None,
        accounts: vec![AccountConfig {
            username: "server".to_string(),
            domain: "127.0.0.1".to_string(),
            register: Some(false),
            ..Default::default()
        }],
    };

    // 2. Configure Client (Options)
    let client_addr = "127.0.0.1:5071";
    let client_config = Config {
        addr: Some(client_addr.to_string()),
        external_ip: None,
        recorders: None,
        ws_url: None,
        accounts: vec![AccountConfig {
            username: "client".to_string(),
            domain: "127.0.0.1".to_string(),
            register: Some(false),
            target: Some(format!("sip:server@{}", server_addr)),
            ..Default::default()
        }],
    };

    // 3. Start Server
    let server_handle = tokio::spawn(async move {
        let stats = Arc::new(CallStats::new());
        let mut bot = SipBot::new(
            server_config.accounts[0].clone(),
            server_config,
            stats,
            false,
            CancellationToken::new(),
        );
        if let Err(e) = bot.run_wait().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Give server time to start
    sleep(Duration::from_secs(1)).await;

    // 4. Start Client
    let client_handle = tokio::spawn(async move {
        let stats = Arc::new(CallStats::new());
        let mut bot = SipBot::new(
            client_config.accounts[0].clone(),
            client_config,
            stats,
            false,
            CancellationToken::new(),
        );
        if let Err(e) = bot.run_options(None).await {
            eprintln!("Client error: {:?}", e);
            panic!("Client failed: {:?}", e);
        }
    });

    // 5. Wait for completion
    match tokio::time::timeout(Duration::from_secs(5), client_handle).await {
        Ok(res) => match res {
            Ok(_) => println!("Client finished successfully"),
            Err(e) => panic!("Client task panicked: {:?}", e),
        },
        Err(_) => panic!("Test timed out"),
    }

    // Cleanup server
    server_handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_wait_echo_tx_rx_stats() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 1. Configure Server (Wait + Echo)
    let server_addr = "127.0.0.1:5090";
    let server_config = Config {
        addr: Some(server_addr.to_string()),
        external_ip: None,
        recorders: None,
        ws_url: None,
        accounts: vec![AccountConfig {
            username: "server-echo".to_string(),
            domain: "127.0.0.1".to_string(),
            register: Some(false),
            answer: Some(AnswerConfig::Echo),
            ..Default::default()
        }],
    };

    // 2. Configure Client (Call)
    let client_addr = "127.0.0.1:5091";
    let client_config = Config {
        addr: Some(client_addr.to_string()),
        external_ip: None,
        recorders: None,
        ws_url: None,
        accounts: vec![AccountConfig {
            username: "client-echo".to_string(),
            domain: "127.0.0.1".to_string(),
            register: Some(false),
            target: Some(format!("sip:server-echo@{}", server_addr)),
            hangup: Some(sipbot::config::HangupConfig {
                code: 200,
                after_secs: Some(4),
            }),
            ..Default::default()
        }],
    };

    let server_stats = Arc::new(CallStats::new());
    let server_cancel = CancellationToken::new();
    let server_cancel_for_task = server_cancel.clone();
    let server_stats_for_task = server_stats.clone();

    // 3. Start Server
    let server_handle = tokio::spawn(async move {
        let mut bot = SipBot::new(
            server_config.accounts[0].clone(),
            server_config,
            server_stats_for_task,
            false,
            server_cancel_for_task,
        );
        if let Err(e) = bot.run_wait().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Give server time to start
    sleep(Duration::from_secs(1)).await;

    // 4. Start Client
    let client_handle = tokio::spawn(async move {
        let stats = Arc::new(CallStats::new());
        let mut bot = SipBot::new(
            client_config.accounts[0].clone(),
            client_config,
            stats,
            false,
            CancellationToken::new(),
        );
        if let Err(e) = bot.run_call(1, 1).await {
            eprintln!("Client error: {:?}", e);
            panic!("Client failed: {:?}", e);
        }
    });

    // 5. Wait for call completion
    match tokio::time::timeout(Duration::from_secs(12), client_handle).await {
        Ok(res) => match res {
            Ok(_) => println!("Client finished successfully"),
            Err(e) => panic!("Client task panicked: {:?}", e),
        },
        Err(_) => panic!("Test timed out waiting for client"),
    }

    // Give echo pipeline a brief moment to flush stats updates.
    sleep(Duration::from_millis(300)).await;

    // 6. Assert server echo media stats
    let rx = server_stats
        .rx_packets
        .load(std::sync::atomic::Ordering::Relaxed);
    let tx = server_stats
        .tx_packets
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(rx > 0, "Expected server RX packets > 0, got {}", rx);
    assert!(
        tx > 0,
        "Expected server TX packets > 0 in echo mode, got {}",
        tx
    );
    let echo_ratio = tx as f64 / rx as f64;
    assert!(
        echo_ratio >= 0.30,
        "Expected practical echo ratio >= 0.30 in integration test, got tx={} rx={} ratio={:.3}",
        tx,
        rx,
        echo_ratio
    );

    // 7. Shutdown server gracefully
    server_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), server_handle).await;

    Ok(())
}
