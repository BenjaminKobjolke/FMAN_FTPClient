import collections
import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class _ConnectionPools:
    connections: dict
    timestamps: dict
    threads: dict
    last_noop_checks: dict
    base_urls: dict
    in_use: collections.Counter
    timeout: float


def _adopt_dead_thread_connection(
    pools: _ConnectionPools,
    current_hash: int,
    hash_for_thread: Callable[[int], int],
) -> None:
    """Adopt a matching idle connection; call while holding the pool lock."""
    if current_hash in pools.connections:
        return

    current_thread_id = threading.get_ident()
    live_threads = {thread.ident for thread in threading.enumerate()}
    for conn_hash, ident in pools.threads.items():
        if (
            ident not in live_threads
            and conn_hash not in pools.in_use
            and conn_hash == hash_for_thread(ident)
        ):
            for connection_map in (
                pools.connections,
                pools.timestamps,
                pools.last_noop_checks,
                pools.base_urls,
            ):
                connection_map[current_hash] = connection_map.pop(conn_hash)
            pools.threads.pop(conn_hash)
            pools.threads[current_hash] = current_thread_id
            return


def _cleanup_stale_connections(
    pools: _ConnectionPools,
    remove_connection: Callable[[int], None],
) -> None:
    """Remove dead-thread and long-idle connections under the pool lock."""
    current_time = time.time()
    live_threads = {thread.ident for thread in threading.enumerate()}
    stale_hashes = [
        conn_hash
        for conn_hash, timestamp in pools.timestamps.items()
        if conn_hash not in pools.in_use
        and (
            (
                pools.threads.get(conn_hash) is not None
                and pools.threads[conn_hash] not in live_threads
            )
            or current_time - timestamp > pools.timeout
        )
    ]
    for conn_hash in stale_hashes:
        remove_connection(conn_hash)
