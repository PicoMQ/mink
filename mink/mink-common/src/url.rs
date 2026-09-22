//! Hides the password in a connection URL for logging and percent-encodes strings for query components.

const REDACTED: &str = "<redacted>";

pub fn redact(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some((userinfo, host)) = rest.split_once('@') else {
        return url.to_owned();
    };

    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:{REDACTED}@{host}"),
        None => url.to_owned(),
    }
}

pub fn encode(part: &str) -> String {
    url::form_urlencoded::byte_serialize(part.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_hides_password() {
        assert_eq!(
            redact("postgres://mink:hunter2@db:5432/mink"),
            "postgres://mink:<redacted>@db:5432/mink"
        );
        assert_eq!(redact("file:///tmp"), "file:///tmp");
    }

    #[test]
    fn encode_escapes_space() {
        assert_eq!(encode("a b"), "a+b");
    }
}
