from __future__ import annotations

from typing import Callable

import logging
from tenacity import RetryCallState, retry, stop_after_attempt, wait_exponential_jitter

logger = logging.getLogger(__name__)


def log_retry(retry_state: RetryCallState) -> None:
    if retry_state.outcome and retry_state.outcome.failed:
        exc = retry_state.outcome.exception()
        if exc:
            logger.warning("retrying after error: %s", exc)


def retryable(
    attempts: int = 5,
    base: float = 0.5,
    max_wait: float = 8.0,
) -> Callable:
    return retry(
        reraise=True,
        stop=stop_after_attempt(attempts),
        wait=wait_exponential_jitter(initial=base, max=max_wait),
        after=log_retry,
    )
