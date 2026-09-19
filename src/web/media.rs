use crate::config::Config;
use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;

use super::state::ServeState;

const MAX_UPLOAD_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Serialize)]
pub struct MediaFile {
    pub name: String,
    pub size: u64,
    /// seconds, from the wav header (hound)
    pub duration: Option<f64>,
    pub sample_rate: Option<u32>,
    pub channels: Option<u16>,
}

pub fn media_dir(config: &Config) -> PathBuf {
    PathBuf::from(config.media_directory())
}

fn safe_name(name: &str) -> Option<String> {
    let name = name.trim();
    if name.is_empty() || name.contains("..") || name.contains('/') || name.contains('\\') {
        return None;
    }
    let lower = name.to_lowercase();
    if !lower.ends_with(".wav") {
        return None;
    }
    Some(name.to_string())
}

pub async fn list(config: &Config) -> Vec<MediaFile> {
    let dir = media_dir(config);
    let mut out = vec![];
    if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()).map(|e| e.to_lowercase()) != Some("wav".into()) {
                continue;
            }
            let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
            let (duration, sample_rate, channels) = wav_info(&path);
            out.push(MediaFile {
                name: entry.file_name().to_string_lossy().to_string(),
                size,
                duration,
                sample_rate,
                channels,
            });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn wav_info(path: &PathBuf) -> (Option<f64>, Option<u32>, Option<u16>) {
    match hound::WavReader::open(path) {
        Ok(reader) => {
            let spec = reader.spec();
            let frames = reader.duration() as f64;
            let rate = spec.sample_rate;
            (
                Some(frames / rate as f64),
                Some(rate),
                Some(spec.channels),
            )
        }
        Err(_) => (None, None, None),
    }
}

/// GET /api/media
pub async fn api_list(State(state): State<Arc<ServeState>>) -> impl IntoResponse {
    let config = state.current_config();
    let _ = &config;
    axum::Json(serde_json::json!({ "media": list(&config).await }))
}

/// GET /media/{file} — serve the wav for preview/playback.
pub async fn serve_file(
    State(state): State<Arc<ServeState>>,
    Path(file): Path<String>,
) -> axum::response::Response {
    let Some(name) = safe_name(&file) else {
        return (StatusCode::BAD_REQUEST, "invalid file name").into_response();
    };
    let path = media_dir(&state.current_config()).join(name);
    match tokio::fs::read(&path).await {
        Ok(bytes) => (
            [(axum::http::header::CONTENT_TYPE, "audio/wav")],
            bytes,
        )
            .into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

/// POST /api/media/{file} — multipart upload; overwrites the file if present.
pub async fn upload(
    State(state): State<Arc<ServeState>>,
    Path(file): Path<String>,
    mut multipart: Multipart,
) -> axum::response::Response {
    let Some(name) = safe_name(&file) else {
        return (StatusCode::BAD_REQUEST, "invalid file name").into_response();
    };
    let mut data: Option<Vec<u8>> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() == Some("file") || data.is_none() {
            match field.bytes().await {
                Ok(bytes) => {
                    if bytes.len() as u64 > MAX_UPLOAD_BYTES {
                        return (
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "file exceeds 10MB limit",
                        )
                            .into_response();
                    }
                    data = Some(bytes.to_vec());
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("read upload: {}", e),
                    )
                        .into_response();
                }
            }
        }
    }
    let Some(data) = data else {
        return (StatusCode::BAD_REQUEST, "missing file field").into_response();
    };

    // Validate the wav parses.
    let dir = media_dir(&state.current_config());
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("create media dir: {}", e),
        )
            .into_response();
    }
    let path = dir.join(&name);
    // validate via hound before writing
    if let Err(e) = hound::WavReader::new(std::io::Cursor::new(&data)) {
        return (
            StatusCode::BAD_REQUEST,
            format!("not a valid wav file: {}", e),
        )
            .into_response();
    }
    match tokio::fs::write(&path, &data).await {
        Ok(_) => axum::Json(serde_json::json!({ "ok": true, "name": name })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("write file: {}", e),
        )
            .into_response(),
    }
}

/// DELETE /api/media/{file}
pub async fn delete(
    State(state): State<Arc<ServeState>>,
    Path(file): Path<String>,
) -> axum::response::Response {
    let Some(name) = safe_name(&file) else {
        return (StatusCode::BAD_REQUEST, "invalid file name").into_response();
    };
    let path = media_dir(&state.current_config()).join(name);
    match tokio::fs::remove_file(&path).await {
        Ok(_) => axum::Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("delete: {}", e)).into_response(),
    }
}

// Body type re-export to keep axum imports tidy.
#[allow(dead_code)]
type _Body = Body;
