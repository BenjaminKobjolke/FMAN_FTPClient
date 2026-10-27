"""Retries for failed transfers and the end-of-batch "retry failed" prompt."""
import threading

from fman import Task, load_json
from fman.url import basename

from .ftp_session import ftputil

DEFAULT_TRANSFER_RETRIES = 2
MAX_TRANSFER_RETRIES = 10
_MAX_LISTED_FAILURES = 20
# Errors a reconnect can fix; cancellation and bugs still propagate.
TRANSIENT_ERRORS = (OSError, ftputil.error.FTPError)


def parse_transfer_retries(value) -> int | None:
    """Return `value` as a retry count, or None when it is not one."""
    try:
        retries = int(value)
    except (TypeError, ValueError):
        return None
    return retries if 0 <= retries <= MAX_TRANSFER_RETRIES else None


def transfer_retries() -> int:
    """Return the retries per failed transfer from `FTP Settings.json`."""
    settings = load_json('FTP Settings.json', default={})
    retries = parse_transfer_retries(
        settings.get('transfer_retries', DEFAULT_TRANSFER_RETRIES))
    return DEFAULT_TRANSFER_RETRIES if retries is None else retries


class _Batch:
    """The file units of one fman copy or move, gathered before any runs."""
    def __init__(self):
        self.started = False
        self.units = 0
        self._finished = 0
        self._failed = {}
        self._lock = threading.Lock()

    def finish(self, key, build, failed):
        with self._lock:
            self._finished += 1
            if failed:
                self._failed[key] = build

    def take_failed_when_done(self):
        """Return the failed units once every unit ran, else None."""
        with self._lock:
            if self._finished < self.units:
                return None
            failed, self._failed = self._failed, {}
            return failed


# shortcut: an operation aborted while fman gathers its files leaves an
# unstarted batch that the next operation joins, so that one shows no retry
# prompt (fman's own per-file errors still do). Upgrade if seen in practice.
_current = None


def current_batch():
    global _current
    if _current is None or _current.started:
        _current = _Batch()
    return _current


class TrackedUnit(Task):
    """One file's transfer, remembered for the end-of-batch retry prompt."""
    def __init__(self, batch, key, build):
        self._unit = build()
        super().__init__(self._unit.get_title(), size=self._unit.get_size())
        self._batch = batch
        self._key = key
        self._build = build
        # fman gathers on one thread before any unit runs.
        batch.units += 1

    def __call__(self):
        self._batch.started = True
        failed = False
        try:
            self.run(self._unit)
        except TRANSIENT_ERRORS:
            failed = True
            raise
        finally:
            self._batch.finish(self._key, self._build, failed)


class _RetryFailed(Task):
    """Offer to retry every unit of the batch that still failed."""
    def __init__(self, batch):
        super().__init__('Checking failed transfers', size=0)
        self._batch = batch
        # Only the last check of a batch finds every unit finished.
        self.waits_for_earlier_tasks = True

    def __call__(self):
        global _current
        self._batch.started = True
        failed = self._batch.take_failed_when_done()
        if failed is None:
            return
        if _current is self._batch:
            _current = None
        if not failed:
            return
        # Imported late: the plugin's test fakes of fman define no buttons.
        from fman import NO, YES
        while failed and self.show_alert(
                _failed_message(failed), YES | NO, YES) & YES:
            failed = self._retry(failed)

    def _retry(self, failed):
        still_failed = {}
        for key, build in failed.items():
            self.check_canceled()
            try:
                self.run(build())
            except TRANSIENT_ERRORS:
                still_failed[key] = build
        return still_failed


def _failed_message(failed):
    names = [basename(src_url) for src_url, _dst_url in failed]
    listed = names[:_MAX_LISTED_FAILURES]
    if len(names) > len(listed):
        listed.append('…')
    return '%d files failed:\n%s\n\nRetry them?' % (
        len(names), '\n'.join(listed))


def with_retry_check(batch, tasks):
    yield from tasks
    yield _RetryFailed(batch)
