//! Byte range requests for NAR downloads.
//!
//! A NAR is served as the concatenation of its stored chunks, and the
//! stored size of every chunk is known, so a byte range of the NAR maps to
//! a run of consecutive chunk slices. This lets clients resume interrupted
//! downloads (Nix does so with `Range: bytes=<offset>-`).

use axum::http::{HeaderMap, header};

/// An inclusive byte range within a body of known length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// How to answer a request, based on its `Range` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeRequest {
    /// Send the whole body with 200.
    Full,

    /// Send this part of the body with 206.
    Partial(ByteRange),

    /// The range starts past the end of the body: answer 416.
    Unsatisfiable,
}

/// A part of one stored chunk to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkSlice {
    /// Index of the chunk in the NAR.
    pub index: usize,

    /// Bytes to skip at the start of the chunk.
    pub skip: u64,

    /// Bytes to send after skipping.
    pub take: u64,
}

/// Interprets the request's `Range` header against a body of `total` bytes.
///
/// Only a single range in `bytes` is honored. Multiple ranges, other units,
/// malformed values and `If-Range` requests (we have no validators to check
/// them against) get the full body, as RFC 9110 allows.
pub(crate) fn parse(headers: &HeaderMap, total: u64) -> RangeRequest {
    if headers.contains_key(header::IF_RANGE) {
        return RangeRequest::Full;
    }

    match headers.get(header::RANGE).map(|v| v.to_str()) {
        Some(Ok(value)) => parse_value(value, total),
        _ => RangeRequest::Full,
    }
}

fn parse_value(value: &str, total: u64) -> RangeRequest {
    let Some((unit, spec)) = value.split_once('=') else {
        return RangeRequest::Full;
    };

    if !unit.trim().eq_ignore_ascii_case("bytes") || spec.contains(',') {
        return RangeRequest::Full;
    }

    let Some((first, last)) = spec.trim().split_once('-') else {
        return RangeRequest::Full;
    };

    if first.is_empty() {
        // Suffix range: the last `suffix` bytes.
        let Some(suffix) = parse_digits(last) else {
            return RangeRequest::Full;
        };

        if suffix == 0 || total == 0 {
            return RangeRequest::Unsatisfiable;
        }

        return RangeRequest::Partial(ByteRange {
            start: total.saturating_sub(suffix),
            end: total - 1,
        });
    }

    let Some(start) = parse_digits(first) else {
        return RangeRequest::Full;
    };

    let end = if last.is_empty() {
        u64::MAX
    } else {
        match parse_digits(last) {
            Some(end) if end >= start => end,
            _ => return RangeRequest::Full,
        }
    };

    if start >= total {
        return RangeRequest::Unsatisfiable;
    }

    RangeRequest::Partial(ByteRange {
        start,
        end: end.min(total - 1),
    })
}

fn parse_digits(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    s.parse().ok()
}

/// Maps `range` onto chunks with the given stored sizes.
pub(crate) fn slice_chunks(sizes: &[u64], range: ByteRange) -> Vec<ChunkSlice> {
    let mut slices = Vec::new();
    let mut offset = 0;

    for (index, &size) in sizes.iter().enumerate() {
        if offset > range.end {
            break;
        }

        let chunk_end = offset + size;

        if chunk_end > range.start {
            let skip = range.start.saturating_sub(offset);
            let take = chunk_end.min(range.end + 1) - offset - skip;

            if take > 0 {
                slices.push(ChunkSlice { index, skip, take });
            }
        }

        offset = chunk_end;
    }

    slices
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::http::HeaderValue;

    fn range(start: u64, end: u64) -> RangeRequest {
        RangeRequest::Partial(ByteRange { start, end })
    }

    fn slice(index: usize, skip: u64, take: u64) -> ChunkSlice {
        ChunkSlice { index, skip, take }
    }

    #[test]
    fn test_parse_value() {
        // Resume from an offset, as Nix does
        assert_eq!(range(100, 999), parse_value("bytes=100-", 1000));
        assert_eq!(range(0, 999), parse_value("bytes=0-", 1000));

        // Closed ranges, clamped to the body
        assert_eq!(range(10, 19), parse_value("bytes=10-19", 1000));
        assert_eq!(range(990, 999), parse_value("bytes=990-5000", 1000));
        assert_eq!(range(999, 999), parse_value("bytes=999-999", 1000));

        // Suffix ranges
        assert_eq!(range(900, 999), parse_value("bytes=-100", 1000));
        assert_eq!(range(0, 999), parse_value("bytes=-5000", 1000));

        // Case-insensitive unit, surrounding whitespace
        assert_eq!(range(5, 999), parse_value("Bytes = 5-", 1000));

        // Past the end
        assert_eq!(
            RangeRequest::Unsatisfiable,
            parse_value("bytes=1000-", 1000)
        );
        assert_eq!(
            RangeRequest::Unsatisfiable,
            parse_value("bytes=1000-1001", 1000)
        );
        assert_eq!(RangeRequest::Unsatisfiable, parse_value("bytes=-0", 1000));
        assert_eq!(RangeRequest::Unsatisfiable, parse_value("bytes=0-", 0));

        // Ignored: full body
        for value in [
            "bytes=0-10,20-30",
            "items=0-10",
            "bytes=20-10",
            "bytes=abc-",
            "bytes=+5-",
            "bytes=5",
            "bytes=-",
            "garbage",
        ] {
            assert_eq!(RangeRequest::Full, parse_value(value, 1000), "{value}");
        }
    }

    #[test]
    fn test_parse_headers() {
        let mut headers = HeaderMap::new();
        assert_eq!(RangeRequest::Full, parse(&headers, 1000));

        headers.insert(header::RANGE, HeaderValue::from_static("bytes=10-"));
        assert_eq!(range(10, 999), parse(&headers, 1000));

        // No validators to compare If-Range with
        headers.insert(header::IF_RANGE, HeaderValue::from_static("\"etag\""));
        assert_eq!(RangeRequest::Full, parse(&headers, 1000));
    }

    #[test]
    fn test_slice_chunks() {
        let sizes = [10, 20, 30];
        let r = |start, end| ByteRange { start, end };

        // Whole NAR
        assert_eq!(
            vec![slice(0, 0, 10), slice(1, 0, 20), slice(2, 0, 30)],
            slice_chunks(&sizes, r(0, 59))
        );

        // Starting inside a chunk
        assert_eq!(
            vec![slice(1, 5, 15), slice(2, 0, 30)],
            slice_chunks(&sizes, r(15, 59))
        );

        // Starting exactly at a chunk boundary
        assert_eq!(vec![slice(2, 0, 30)], slice_chunks(&sizes, r(30, 59)));

        // Ending inside a chunk
        assert_eq!(
            vec![slice(0, 9, 1), slice(1, 0, 5)],
            slice_chunks(&sizes, r(9, 14))
        );

        // Within a single chunk
        assert_eq!(vec![slice(1, 2, 3)], slice_chunks(&sizes, r(12, 14)));

        // Last byte
        assert_eq!(vec![slice(2, 29, 1)], slice_chunks(&sizes, r(59, 59)));

        // Empty chunks are skipped
        assert_eq!(
            vec![slice(0, 0, 10), slice(2, 0, 5)],
            slice_chunks(&[10, 0, 5], r(0, 14))
        );
    }
}
