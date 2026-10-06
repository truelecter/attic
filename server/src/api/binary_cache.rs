//! Nix Binary Cache server.
//!
//! This module implements the Nix Binary Cache API.
//!
//! The implementation is based on the specifications at <https://github.com/fzakaria/nix-http-binary-cache-api-spec>.

use std::collections::VecDeque;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Extension, Path},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use futures::stream::BoxStream;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::ReaderStream;
use tracing::instrument;

use super::byte_range::{self, RangeRequest};
use crate::database::AtticDatabase;
use crate::database::entity::chunk::ChunkModel;
use crate::error::{ErrorKind, ServerResult};
use crate::narinfo::NarInfo;
use crate::nix_manifest;
use crate::storage::{Download, StorageBackend, StorageBackendImpl};
use crate::{RequestState, State};
use attic::cache::CacheName;
use attic::io::merge_chunks;
use attic::mime;
use attic::nix_store::StorePathHash;

/// Nix cache information.
///
/// An example of a correct response is as follows:
///
/// ```text
/// StoreDir: /nix/store
/// WantMassQuery: 1
/// Priority: 40
/// ```
#[derive(Debug, Clone, Serialize)]
struct NixCacheInfo {
    /// Whether this binary cache supports bulk queries.
    #[serde(rename = "WantMassQuery")]
    want_mass_query: bool,

    /// The Nix store path this binary cache uses.
    #[serde(rename = "StoreDir")]
    store_dir: PathBuf,

    /// The priority of the binary cache.
    ///
    /// A lower number denotes a higher priority.
    /// <https://cache.nixos.org> has a priority of 40.
    #[serde(rename = "Priority")]
    priority: i32,
}

impl IntoResponse for NixCacheInfo {
    fn into_response(self) -> Response {
        match nix_manifest::to_string(&self) {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", mime::NIX_CACHE_INFO)
                .body(body)
                .unwrap()
                .into_response(),
            Err(e) => e.into_response(),
        }
    }
}

/// Gets information on a cache.
#[instrument(skip_all, fields(cache_name))]
async fn get_nix_cache_info(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(cache_name): Path<CacheName>,
) -> ServerResult<NixCacheInfo> {
    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, &cache_name, |cache, permission| {
            permission.require_pull()?;
            Ok(cache)
        })
        .await?;

    req_state.set_public_cache(cache.is_public);

    let info = NixCacheInfo {
        want_mass_query: true,
        store_dir: cache.store_dir.into(),
        priority: cache.priority,
    };

    Ok(info)
}

/// Gets various information on a store path hash.
///
/// `/:cache/:path`, which may be one of
/// - GET `/:cache/{storePathHash}.narinfo`
/// - HEAD `/:cache/{storePathHash}.narinfo`
/// - GET `/:cache/{storePathHash}.ls` (not implemented)
#[instrument(skip_all, fields(cache_name, path))]
#[axum_macros::debug_handler]
async fn get_store_path_info(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path((cache_name, path)): Path<(CacheName, String)>,
) -> ServerResult<NarInfo> {
    let components: Vec<&str> = path.splitn(2, '.').collect();

    if components.len() != 2 {
        return Err(ErrorKind::NotFound.into());
    }

    // TODO: Other endpoints
    if components[1] != "narinfo" {
        return Err(ErrorKind::NotFound.into());
    }

    let store_path_hash = StorePathHash::new(components[0].to_string())?;

    tracing::debug!(
        "Received request for {}.narinfo in {:?}",
        store_path_hash.as_str(),
        cache_name
    );

    let (object, cache, nar, _) = state
        .database()
        .await?
        .find_object_and_chunks_by_store_path_hash(&cache_name, &store_path_hash, false)
        .await?;

    let permission = req_state
        .auth
        .get_permission_for_cache(&cache_name, cache.is_public);
    permission.require_pull()?;

    req_state.set_public_cache(cache.is_public);

    let mut narinfo = object.to_nar_info(&nar)?;

    if narinfo.signature().is_none() {
        let keypair = cache.keypair()?;
        narinfo.sign(&keypair);
    }

    Ok(narinfo)
}

/// Gets a NAR.
///
/// - GET `:cache/nar/{storePathHash}.nar`
///
/// Here we use the store path hash not the NAR hash or file hash
/// for better logging. In reality, the files are deduplicated by
/// content-addressing.
///
/// When the stored size of every chunk is known, the response carries
/// `Content-Length` and honors single byte ranges, so clients can resume
/// interrupted downloads.
#[instrument(skip_all, fields(cache_name, path))]
async fn get_nar(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path((cache_name, path)): Path<(CacheName, String)>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    let components: Vec<&str> = path.splitn(2, '.').collect();

    if components.len() != 2 {
        return Err(ErrorKind::NotFound.into());
    }

    if components[1] != "nar" {
        return Err(ErrorKind::NotFound.into());
    }

    let store_path_hash = StorePathHash::new(components[0].to_string())?;

    tracing::debug!(
        "Received request for {}.nar in {:?}",
        store_path_hash.as_str(),
        cache_name
    );

    let database = state.database().await?;

    let (object, cache, _nar, chunks) = database
        .find_object_and_chunks_by_store_path_hash(&cache_name, &store_path_hash, true)
        .await?;

    let permission = req_state
        .auth
        .get_permission_for_cache(&cache_name, cache.is_public);
    permission.require_pull()?;

    req_state.set_public_cache(cache.is_public);

    // TODO: Fully kill chunk recovery
    if chunks.iter().any(Option::is_none) {
        // at least one of the chunks is missing :(
        return Err(ErrorKind::IncompleteNar.into());
    }

    database.bump_object_last_accessed(object.id).await?;

    let mut chunks = chunks;
    let storage = state.storage().await?.clone();

    // A single chunk may be served from a URL (e.g., presigned S3), which
    // handles range requests on its own.
    let mut single_reader = None;
    if let [Some(chunk)] = chunks.as_slice() {
        match storage
            .download_file_db(&chunk.remote_file.0, false)
            .await?
        {
            Download::Url(url) => return Ok(Redirect::temporary(&url).into_response()),
            Download::AsyncRead(reader) => single_reader = Some(reader),
        }
    }

    // The NAR is served as the concatenation of the stored chunks, so its
    // size is known when the stored size of every chunk is.
    let sizes: Option<Vec<u64>> = chunks
        .iter()
        .map(|chunk| {
            let size = chunk.as_ref().unwrap().file_size?;
            u64::try_from(size).ok()
        })
        .collect();

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime::NAR));

    let (status, slices) = match sizes {
        // Without the size, the NAR can only be streamed whole.
        None => {
            let slices = (0..chunks.len())
                .map(|index| (index, 0, None))
                .collect::<Vec<_>>();
            (StatusCode::OK, slices)
        }
        Some(sizes) => {
            let total: u64 = sizes.iter().sum();
            response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

            match byte_range::parse(&headers, total) {
                RangeRequest::Full => {
                    response_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(total));

                    let slices = sizes
                        .iter()
                        .enumerate()
                        .map(|(index, &size)| (index, 0, Some(size)))
                        .collect();
                    (StatusCode::OK, slices)
                }
                RangeRequest::Partial(range) => {
                    response_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(range.len()));
                    response_headers.insert(
                        header::CONTENT_RANGE,
                        header_value(format!("bytes {}-{}/{}", range.start, range.end, total)),
                    );

                    let slices = byte_range::slice_chunks(&sizes, range)
                        .into_iter()
                        .map(|slice| (slice.index, slice.skip, Some(slice.take)))
                        .collect();
                    (StatusCode::PARTIAL_CONTENT, slices)
                }
                RangeRequest::Unsatisfiable => {
                    response_headers.insert(
                        header::CONTENT_RANGE,
                        header_value(format!("bytes */{total}")),
                    );
                    return Ok(
                        (StatusCode::RANGE_NOT_SATISFIABLE, response_headers).into_response()
                    );
                }
            }
        }
    };

    let parts: VecDeque<NarPart> = slices
        .into_iter()
        .map(|(index, skip, take)| NarPart {
            chunk: chunks[index].take().unwrap(),
            skip,
            take,
            reader: single_reader.take(),
        })
        .collect();

    // TODO: Make num_prefetch configurable
    // The ideal size depends on the average chunk size
    let merged = merge_chunks(parts, NarPart::into_stream, storage, 2).map_err(|e| {
        tracing::error!(%e, "Stream error");
        e
    });

    Ok((status, response_headers, Body::from_stream(merged)).into_response())
}

/// A slice of one stored chunk of a NAR to send.
struct NarPart {
    chunk: ChunkModel,

    /// Bytes to skip at the start of the chunk.
    skip: u64,

    /// Bytes to send after skipping, or the rest of the chunk if unset.
    take: Option<u64>,

    /// The chunk's file, if already opened.
    reader: Option<Box<dyn AsyncRead + Unpin + Send>>,
}

impl NarPart {
    async fn into_stream(
        self,
        storage: Arc<StorageBackendImpl>,
    ) -> Result<BoxStream<'static, Result<Bytes, IoError>>, IoError> {
        let mut reader = match self.reader {
            Some(reader) => reader,
            None => match storage
                .download_file_db(&self.chunk.remote_file.0, true)
                .await
                .map_err(IoError::other)?
            {
                Download::Url(_) => {
                    return Err(IoError::other("URLs not supported for NAR reassembly"));
                }
                Download::AsyncRead(reader) => reader,
            },
        };

        if self.skip > 0 {
            let skipped =
                tokio::io::copy(&mut (&mut reader).take(self.skip), &mut tokio::io::sink()).await?;

            if skipped != self.skip {
                return Err(IoError::new(
                    IoErrorKind::UnexpectedEof,
                    "chunk is shorter than its recorded size",
                ));
            }
        }

        Ok(match self.take {
            Some(take) => Box::pin(ReaderStream::new(reader.take(take))),
            None => Box::pin(ReaderStream::new(reader)),
        })
    }
}

fn header_value(value: String) -> HeaderValue {
    HeaderValue::try_from(value).expect("header value is ASCII")
}

pub fn get_router() -> Router {
    Router::new()
        .route("/{cache}/nix-cache-info", get(get_nix_cache_info))
        .route("/{cache}/{path}", get(get_store_path_info))
        .route("/{cache}/nar/{path}", get(get_nar))
}
