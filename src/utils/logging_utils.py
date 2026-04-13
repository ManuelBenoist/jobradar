from urllib.parse import parse_qs, urlparse, urlunparse, urlencode


def mask_url(url: str) -> str:
    """Mask sensitive query parameters in a URL for safe logging."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query, keep_blank_values=True)

    for secret in ("app_id", "app_key", "access_token"):
        if secret in query_params:
            query_params[secret] = ["*****"]

    return urlunparse(parsed._replace(query=urlencode(query_params, doseq=True)))
