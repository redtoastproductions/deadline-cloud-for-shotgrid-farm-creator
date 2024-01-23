import logging

import deadline_cloud_util
import logutil


# Logging to file
logutil.add_file_handler()
logger = logging.getLogger(__name__)
logger.setLevel(logutil.get_deadline_config_level())

# Logging to stdout
log_handler = logging.StreamHandler()
log_fmt = logging.Formatter(
    "%(asctime)s - [%(levelname)-7s] "
    "[%(module)s:%(funcName)s:%(lineno)d] %(message)s"
)
log_handler.setFormatter(log_fmt)
logger.addHandler(log_handler)


if __name__ == "__main__":
    pass
