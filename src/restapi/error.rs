use crate::storage::error::Error;
use serde_derive::Serialize;
use std::convert::Infallible;
use warp::http::StatusCode;
use warp::reject::{Reject, Rejection};

#[derive(Debug)]
struct NotFound {
    series: String,
}

impl Reject for NotFound {}

pub fn not_found<S: AsRef<str>>(series: S) -> Rejection {
    warp::reject::custom(NotFound {
        series: series.as_ref().to_owned(),
    })
}

#[derive(Debug)]
struct BadRequest {
    reason: String,
}

impl Reject for BadRequest {}

pub fn bad_request<S: AsRef<str>>(reason: S) -> Rejection {
    warp::reject::custom(BadRequest {
        reason: reason.as_ref().to_owned(),
    })
}

#[derive(Debug)]
struct InternalError {
    error: Error,
}

impl Reject for InternalError {}

pub fn internal(error: Error) -> Rejection {
    warp::reject::custom(InternalError { error: error })
}

#[derive(Debug)]
struct Conflict {
    series: String,
}

impl Reject for Conflict {}

pub fn conflict<S: AsRef<str>>(series: S) -> Rejection {
    warp::reject::custom(Conflict {
        series: series.as_ref().to_owned(),
    })
}

#[derive(Serialize)]
struct ErrorMessage {
    code: u16,
    message: String,
}

pub async fn handle(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message;

    if let Some(not_found) = err.find::<NotFound>() {
        code = StatusCode::NOT_FOUND;
        message = format!("series '{}' not found", not_found.series);
    } else if let Some(internal) = err.find::<InternalError>() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("internal error: {}", internal.error);
    } else if let Some(bad_request) = err.find::<BadRequest>() {
        code = StatusCode::BAD_REQUEST;
        message = format!("{}", bad_request.reason);
    } else if let Some(conflict) = err.find::<Conflict>() {
        code = StatusCode::CONFLICT;
        message = format!("'{}' already exists", conflict.series);
    } else if let Some(_) = err.find::<warp::filters::body::BodyDeserializeError>() {
        message = "invalid json body".to_owned();
        code = StatusCode::BAD_REQUEST;
    } else {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "unhandled rejection".to_string();
    }

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}
