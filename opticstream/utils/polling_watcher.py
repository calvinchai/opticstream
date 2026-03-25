from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K")


@dataclass
class StabilityRecord:
    fingerprint: object
    stable_since: float | None = None


_shutdown_requested = False


def _signal_handler(signum: int, frame: object) -> None:
    global _shutdown_requested
    logger.info("Received signal %s. Initiating graceful shutdown...", signum)
    _shutdown_requested = True


class PollingStableWatcher(Generic[T, K]):
    """
    Polling-only watcher with stability gating.

    A candidate is processed only after:
    - it is discovered by ``discover_candidates()``
    - its fingerprint remains unchanged for ``stability_seconds``

    The watcher is generic and domain-agnostic. Domain code supplies:
    - ``discover_candidates``: returns the current set of candidates
    - ``candidate_key``: stable identity for a candidate
    - ``fingerprint``: changes whenever the candidate's relevant on-disk state changes
    - ``process``: dispatch logic for a stable candidate

    Notes
    -----
    - After ``process(candidate)`` is attempted, the candidate is removed from the
      internal stability map for this cycle. If it still exists on disk and is still
      discoverable later, it may re-enter tracking, but domain-level dedupe should
      prevent duplicate work.
    - This watcher intentionally does not know anything about state services,
      event emitters, flows, or refresh hooks.
    """

    def __init__(
        self,
        *,
        discover_candidates: Callable[[], Iterable[T]],
        candidate_key: Callable[[T], K],
        fingerprint: Callable[[T], object],
        process: Callable[[T], int],
        poll_interval: int = 5,
        stability_seconds: int = 15,
        running_message: str = "Polling watcher running",
    ) -> None:
        if poll_interval <= 0:
            raise ValueError(f"poll_interval must be > 0, got {poll_interval}")
        if stability_seconds < 0:
            raise ValueError(f"stability_seconds must be >= 0, got {stability_seconds}")

        self.discover_candidates = discover_candidates
        self.candidate_key = candidate_key
        self.fingerprint = fingerprint
        self.process = process
        self.poll_interval = poll_interval
        self.stability_seconds = stability_seconds
        self.running_message = running_message

        self._stability: dict[K, StabilityRecord] = {}

    def run(self) -> None:
        global _shutdown_requested
        _shutdown_requested = False

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        logger.info("%s", self.running_message)
        logger.info(
            "poll_interval=%ss; stability_seconds=%ss",
            self.poll_interval,
            self.stability_seconds,
        )
        logger.info("Press Ctrl+C to stop")

        iteration = 0
        try:
            while not _shutdown_requested:
                iteration += 1
                logger.info("--- iteration %s ---", iteration)
                self._run_iteration()

                if _shutdown_requested:
                    break

                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt, shutting down")
        finally:
            logger.info("Watcher stopped after %s iterations", iteration)

    def _run_iteration(self) -> None:
        now = time.time()
        seen_keys: set[K] = set()

        for candidate in self.discover_candidates():
            key = self.candidate_key(candidate)
            seen_keys.add(key)

            try:
                current_fingerprint = self.fingerprint(candidate)
            except Exception as exc:
                logger.warning("Failed to fingerprint candidate %r: %s", candidate, exc)
                continue

            record = self._stability.get(key)

            if record is None:
                self._stability[key] = StabilityRecord(
                    fingerprint=current_fingerprint,
                    stable_since=now if self.stability_seconds == 0 else None,
                )
                logger.info("Discovered new candidate: %r", candidate)
                continue

            if record.fingerprint != current_fingerprint:
                record.fingerprint = current_fingerprint
                record.stable_since = now if self.stability_seconds == 0 else None
                logger.info(
                    "Candidate changed; resetting stability timer: %r", candidate
                )
                continue

            if record.stable_since is None:
                record.stable_since = now
                logger.info("Candidate became stable: %r", candidate)
                continue

            stable_for = now - record.stable_since
            if stable_for < self.stability_seconds:
                continue

            try:
                dispatched = self.process(candidate)
                logger.info(
                    "Processed candidate=%r dispatched=%s", candidate, dispatched
                )
            except Exception as exc:
                logger.exception("Error processing candidate %r: %s", candidate, exc)
            finally:
                # Remove after an attempt. If the candidate is still present on disk,
                # it will be rediscovered and domain dedupe should prevent rework.
                self._stability.pop(key, None)

        disappeared_keys = set(self._stability) - seen_keys
        for key in disappeared_keys:
            self._stability.pop(key, None)
