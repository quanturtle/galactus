import logging
import os
import zlib
from typing import TextIO

RESET = "\033[0m"
DIM = "\033[2m"

LEVEL_COLORS = {
    "DEBUG": "\033[36m",       # cyan
    "INFO": "\033[32m",        # green
    "WARNING": "\033[33m",     # yellow
    "ERROR": "\033[31m",       # red
    "CRITICAL": "\033[1;31m",  # bold red
}

NAME_COLORS = (
    "\033[35m",  # magenta
    "\033[34m",  # blue
    "\033[36m",  # cyan
    "\033[95m",  # bright magenta
    "\033[94m",  # bright blue
    "\033[96m",  # bright cyan
)

PLAIN_FORMAT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
COLOR_FORMAT = f"{DIM}%(asctime)s{RESET} %(levelname_colored)s %(name_colored)s — %(message)s"
DATEFMT = "%H:%M:%S"


class ColoredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        level_color = LEVEL_COLORS.get(record.levelname, "")
        # crc32 gives a stable color across runs, unlike built-in hash()
        name_color = NAME_COLORS[zlib.crc32(record.name.encode()) % len(NAME_COLORS)]
        record.levelname_colored = f"{level_color}{record.levelname:<8s}{RESET}"
        record.name_colored = f"{name_color}{record.name}{RESET}"
        return super().format(record)


def _color_enabled(stream: TextIO) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return hasattr(stream, "isatty") and stream.isatty()


def setup_logging(level: str | int = "INFO") -> None:
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler()
    if _color_enabled(handler.stream):
        handler.setFormatter(ColoredFormatter(COLOR_FORMAT, DATEFMT))
    else:
        handler.setFormatter(logging.Formatter(PLAIN_FORMAT, DATEFMT))
    root.addHandler(handler)
