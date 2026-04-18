import logging
import sys
from urllib.parse import parse_qs, urlparse, urlunparse, urlencode


class ColoredFormatter(logging.Formatter):
    """Formatter with ANSI colors and semantic emojis for ingestion logging."""

    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    LEVEL_CONFIG = {
        logging.DEBUG: ("\x1b[34m", "🔧"),
        logging.INFO: ("\x1b[32m", "✅"),
        logging.WARNING: ("\x1b[33m", "⚠️"),
        logging.ERROR: ("\x1b[31m", "❌"),
        logging.CRITICAL: ("\x1b[41m", "🔥"),
    }

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

    def format(self, record: logging.LogRecord) -> str:
        color, emoji = self.LEVEL_CONFIG.get(
            record.levelno, self.LEVEL_CONFIG[logging.INFO]
        )
        original_levelname = record.levelname
        record.levelname = f"{emoji} {record.levelname}"
        formatted_message = super().format(record)
        record.levelname = original_levelname

        if record.levelno >= logging.ERROR:
            return f"{self.BOLD}{color}{formatted_message}{self.RESET}"
        return f"{color}{formatted_message}{self.RESET}"


def configure_logging(level: int = logging.INFO) -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColoredFormatter())
    root_logger.addHandler(handler)
    root_logger.setLevel(level)


def mask_url(url: str) -> str:
    """Mask sensitive query parameters in a URL for safe logging."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query, keep_blank_values=True)

    for secret in ("app_id", "app_key", "access_token"):
        if secret in query_params:
            query_params[secret] = ["*****"]

    return urlunparse(parsed._replace(query=urlencode(query_params, doseq=True)))
