import logging
from typing import Iterable

from interceptor.constants import LOKI_HOST, LOKI_PORT, LOG_LEVEL

logger = logging.getLogger("interceptor")


if LOKI_HOST:
    from multiprocessing import Queue
    from logging_loki import LokiQueueHandler
    handler = LokiQueueHandler(
        Queue(-1),
        url=f"{LOKI_HOST.replace('https://', 'http://')}:"
            f"{LOKI_PORT}/loki/api/v1/push",
        tags={"application": "interceptor"},
        version="1",
    )

    logger.setLevel(logging.INFO if LOG_LEVEL == 'info' else logging.DEBUG)
    try:
        logger.addHandler(handler)
    except ValueError as exc:
        logger.error("Can't connect to loki")


def get_centry_logger(hostname: str, labels: dict = None, stop_words: Iterable = tuple()) -> logging.Logger:
    from centry_loki import log_loki
    try:
        context = {
            "url": f"{LOKI_HOST.replace('https://', 'http://')}:"
                   f"{LOKI_PORT}/loki/api/v1/push",
            "hostname": hostname, "labels": labels
        }
        centry_logger = log_loki.get_logger(context, secrets=stop_words)
    except KeyError:
        centry_logger = logger
        centry_logger.warning("Failed setup logger for test. Used default logger")
        if stop_words:
            try:
                from centry_loki.formatters import SecretFormatter
                SecretFormatter(secrets=stop_words).patch_logger(centry_logger)
            except ImportError:
                ...
    return centry_logger
