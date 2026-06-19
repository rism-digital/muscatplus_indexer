import logging
import os
import sys


class ColorFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.DEBUG: "\x1b[36m",  # cyan
        logging.INFO: "\x1b[32m",  # green
        logging.WARNING: "\x1b[33m",  # yellow
        logging.ERROR: "\x1b[31m",  # red
        logging.CRITICAL: "\x1b[1;31m",  # bold red
    }
    TIMESTAMP_COLOR = "\x1b[90m"  # dark gray
    RESET = "\x1b[0m"

    def __init__(self, *args, use_colors: bool = True, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.use_colors = use_colors

    def format(self, record: logging.LogRecord) -> str:
        if not self._should_use_colors():
            return super().format(record)

        original_levelname = record.levelname
        color = self.LEVEL_COLORS.get(record.levelno, "")
        if color:
            record.levelname = f"{color}{original_levelname}{self.RESET}"

        try:
            formatted = super().format(record)
        finally:
            record.levelname = original_levelname

        if formatted.startswith("["):
            ts_end = formatted.find("]")
            if ts_end != -1:
                timestamp = formatted[: ts_end + 1]
                remainder = formatted[ts_end + 1 :]
                formatted = f"{self.TIMESTAMP_COLOR}{timestamp}{self.RESET}{remainder}"

        location_start = formatted.rfind(" (")
        if location_start != -1 and formatted.endswith(")"):
            prefix = formatted[:location_start]
            location = formatted[location_start:]
            return f"{prefix}{self.TIMESTAMP_COLOR}{location}{self.RESET}"

        return formatted

    def _should_use_colors(self) -> bool:
        if not self.use_colors:
            return False
        if os.getenv("NO_COLOR"):
            return False

        color_mode = os.getenv("MUSCAT_LOG_COLOR", "auto").lower()
        if color_mode == "never":
            return False
        if color_mode == "always":
            return True

        return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
