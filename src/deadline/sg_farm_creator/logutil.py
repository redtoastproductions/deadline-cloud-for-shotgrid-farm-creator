import logging
from datetime import datetime
from pathlib import Path

from deadline.client import config

_DEFAULT_FORMATTER = logging.Formatter(
    fmt="%(asctime)s - [%(levelname)-7s] %(module)s:%(funcName)s - %(message)s",
    datefmt="%y-%m-%d %Hh%Mm%Ss",
)


def add_file_handler(
    file: Path | None = None,
    logger: logging.Logger = logging.getLogger(),
    fmt: logging.Formatter = _DEFAULT_FORMATTER,
    level=logging.DEBUG,
):
    """Configure and add a file handler to the specified logger.
    
    If a handler with the same output file already exists on the logger, replace it.
    
    Args:
        file: The file to log to. Defaults to a file at `~/.deadline/logs/blender`.
        logger: The logger to add the handler to. Defaults to the root logger.
        fmt: The format string to use for the log messages on the new handler.
        level: The level to set the handler to. Defaults to DEBUG.
    """
    
    if file is None:
        today_stamp = datetime.strftime(datetime.now(), "%Y-%m-%d")
        file = Path.home() / ".deadline" / "logs" / "farm_creator" / f"farm_creator_{today_stamp}.log"
        file.parent.mkdir(parents=True, exist_ok=True)
    
    handler = logging.FileHandler(file, mode="a")
    handler.setFormatter(fmt)
    handler.setLevel(level)
    
    # Find the handlers with the same file as the one we're adding, if any, and remove it.
    existing = []
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and h.baseFilename == handler.baseFilename:
            existing.append(h)
    for h in existing:
        logger.removeHandler(h)
        logger.info(f"Removed existing file handler: {h}")
    
    logger.addHandler(handler)
    logger.info(f"Added file handler to handlers: {logger.handlers}")


def get_deadline_config_level():
    """Get the current log level set in the Deadline configuration file."""
    return config.config_file.get_setting("settings.log_level")
