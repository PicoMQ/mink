use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

use super::reply::Failure;

#[derive(rust_embed::RustEmbed)]
#[cfg_attr(not(test), folder = "console/")]
#[cfg_attr(test, folder = "tests/console/")]
#[allow_missing = true]
struct Console;

const INDEX: &str = "index.html";
const HINT: &str = "<!doctype html><html><body style=\"font-family: sans-serif\">\
<h3>Mink admin</h3>\
<p>This binary was built without the console. Build it with\
<code> cd console && npm install && npm run build</code> and recompile,\
or use the Docker image. The <code>/admin</code> API is available.</p>\
</body></html>";

pub(super) fn is_console(path: &str) -> bool {
    !matches!(
        path.trim_start_matches('/').split('/').next(),
        Some("admin" | "metrics" | "health" | "ready" | "catalog" | "s3")
    )
}

pub(super) async fn serve(method: Method, uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path().trim_start_matches('/');

    if !is_console(uri.path()) {
        return Failure::missing(format!("no route /{path}")).into_response();
    }

    if method != Method::GET && method != Method::HEAD {
        let mut response =
            Failure::not_allowed(format!("{method} is not allowed on /{path}")).into_response();
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }

    if path.ends_with(".gz") {
        return Failure::missing(format!("no asset /{path}")).into_response();
    }

    if let Some(response) = asset(path, &headers) {
        return response;
    }

    if path.starts_with("assets/") {
        return Failure::missing(format!("no asset /{path}")).into_response();
    }

    asset(INDEX, &headers).unwrap_or_else(|| {
        ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], HINT).into_response()
    })
}

fn asset(path: &str, headers: &HeaderMap) -> Option<Response> {
    let file = Console::get(path)?;
    let compressed = accepts_gzip(headers)
        .then(|| Console::get(&format!("{path}.gz")))
        .flatten();
    let gzip = compressed.is_some();
    let file = compressed.unwrap_or(file);
    let tag = format!("\"{}\"", hex::encode(file.metadata.sha256_hash()));

    let fresh = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|candidate| candidate.trim() == tag));
    let mut response = match fresh {
        true => StatusCode::NOT_MODIFIED.into_response(),
        false => Body::from(file.data).into_response(),
    };

    let headers = response.headers_mut();
    if gzip {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime(path)));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache(path)));
    headers.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&tag).expect("a hex tag is a header"),
    );
    Some(response)
}

fn mime(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("txt") => "text/plain; charset=utf-8",
        Some("map" | "json") => "application/json",
        _ => "application/octet-stream",
    }
}

fn cache(path: &str) -> &'static str {
    match path.starts_with("assets/") {
        true => "public, max-age=31536000, immutable",
        false => "no-cache",
    }
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|coding| {
            let mut parts = coding.split(';').map(str::trim);
            let gzip = parts
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("gzip"));
            let refused = parts.any(|parameter| {
                parameter
                    .strip_prefix("q=")
                    .and_then(|weight| weight.parse::<f32>().ok())
                    .is_some_and(|weight| weight == 0.0)
            });

            gzip && !refused
        })
}

#[cfg(test)]
mod tests {
    use reqwest::header::{
        ACCEPT_ENCODING, ALLOW, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, ETAG, IF_NONE_MATCH,
    };
    use reqwest::{Method, StatusCode};

    use crate::Role;
    use crate::admin::tests::admin;

    #[tokio::test]
    async fn assets_fall_back_to_index_and_cache_by_path() {
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let client = reqwest::Client::builder().no_gzip().build().unwrap();
        let get = async |path: &str, gzip: bool| {
            let mut request = client.get(format!("{base}{path}"));
            if gzip {
                request = request.header(ACCEPT_ENCODING, "gzip, br");
            }
            request.send().await.unwrap()
        };

        let index = get("/tables/orders/files", false).await;
        let zipped = get("/", true).await;
        let script = get("/assets/app.js", false).await;
        let missing_asset = get("/assets/none.js", false).await;
        let missing_route = get("/admin/nothing", false).await;
        let compressed = get("/index.html.gz", false).await;
        let posted = client
            .request(Method::POST, format!("{base}/tables"))
            .send()
            .await
            .unwrap();
        let tag = index.headers()[ETAG].clone();
        let unchanged = client
            .get(format!("{base}/"))
            .header(IF_NONE_MATCH, tag)
            .send()
            .await
            .unwrap();
        let text = node.services.metrics.render();
        node.shutdown().await.unwrap();

        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(index.headers()[CACHE_CONTROL], "no-cache");
        assert!(
            index.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert!(index.text().await.unwrap().contains("Mink Console"));
        assert_eq!(zipped.headers()[CONTENT_ENCODING], "gzip");
        assert_eq!(
            script.headers()[CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
        assert_eq!(script.headers()[CONTENT_TYPE], "text/javascript");
        assert_eq!(missing_asset.status(), StatusCode::NOT_FOUND);
        assert_eq!(missing_route.status(), StatusCode::NOT_FOUND);
        assert_eq!(compressed.status(), StatusCode::NOT_FOUND);
        assert_eq!(posted.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(posted.headers()[ALLOW], "GET, HEAD");
        assert_eq!(unchanged.status(), StatusCode::NOT_MODIFIED);
        assert!(
            text.contains(r#"mink_admin_requests_total{status="2xx"} 0"#),
            "{text}"
        );
        assert!(
            text.contains(r#"mink_admin_requests_total{status="4xx"} 1"#),
            "{text}"
        );
    }
}
