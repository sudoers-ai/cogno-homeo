"""
cogno_homeo.lane — a single-consumer lane: ONE leader in the deployment drains a
shared queue, one job at a time, each job under its OWN deadline.

Many processes may push jobs; exactly one drains them — whichever holds the lane's
**leadership lock**. It is resilience, not storage: the lane exists for work that
must never run twice AT ONCE because what it spends belongs to the whole deployment
(a model server, a provider's rate limit, a paid API), and it has to keep moving
when a process dies and when a process HANGS.

The kernel owns the loop and four guarantees; the host owns what a job is and does:

1. **One leader.** Only the holder of the :class:`LeadershipLock` drains.
   ``lead()`` takes the lock when nobody holds it and confirms it is still held when
   this holder already has it; a process that dies drops its lock with it, and the
   next one to ask takes over, continuing from the queue.
2. **One job at a time, in order.** The oldest runnable row first (the queue's own
   order), removed once it RAN — succeeded, raised or timed out. A job is tried once;
   a host that wants another try pushes it again.
3. **A visible heartbeat.** The leader beats on every loop and every ``beat_s``
   while a job runs, naming the job. A lane that stopped — a leader hung without
   dying, still holding its lock — shows as a STALE beat over waiting jobs
   (:func:`stalled`), never as silence.
4. **A deadline per job that gives the lane UP.** A job past its deadline is
   cancelled WITHOUT being awaited (a driver that ignores the cancel would otherwise
   hold the lane exactly as long as it hangs), the host's ``on_timeout`` runs, and the
   lock is RELEASED — also when ``on_timeout`` raises. The lane then waits
   ``lead_retry_s`` before asking again, so another process can take over: a hung
   leader cannot retain leadership.

What the kernel does NOT know: what a row means, how long a job deserves, which
kinds may run now, what a timeout must undo, where the queue lives. Those are
callbacks and ports. Pure code with no I/O of its own: the durable queue, lock and
beat (say a table, a session advisory lock and a row) are the host's adapter behind
the three Protocols below — the way the breaker's shared ``StateStore`` is.

Operational events reach the host through callbacks (``on_error``, ``on_timeout``,
``on_beat_error``) — the ``MetricsSink`` convention of this kernel: this module's own
``logging`` is DEBUG only.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Collection,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

logger = logging.getLogger(__name__)


# ── the ports ────────────────────────────────────────────────────────────────────────


@runtime_checkable
class LeadershipLock(Protocol):
    """The lane's leadership: ONE holder in the deployment at a time.

    ``lead()`` answers "am I the one that drains?": it TAKES the lock when nobody
    holds it and CONFIRMS it when this holder already does. A holder whose lock was
    lost behind its back (with the connection that held it, say) must answer
    ``False`` — or take it again — never a remembered ``True``. ``release()`` gives it
    up; releasing a lock this holder does not hold is a no-op."""

    async def lead(self) -> bool: ...

    async def release(self) -> None: ...


@runtime_checkable
class JobQueue(Protocol):
    """The consumer side of the lane's queue.

    A row is a ``Mapping`` that carries the queue's own ``id`` (and a ``kind`` when
    kinds are filtered). ``peek`` returns the OLDEST row whose kind is in ``kinds``
    (any kind when ``None``) without taking it; ``remove`` drops it once it ran.
    Pushing is the producers' business and not part of this port."""

    async def peek(
        self, *, kinds: Optional[Collection[str]] = None
    ) -> Optional[Mapping[str, Any]]: ...

    async def remove(self, job_id: Any) -> None: ...


@runtime_checkable
class Heartbeat(Protocol):
    """Where the leader's beat is written: WHO leads and WHAT it runs (``""``
    between jobs). Readable by anyone — that is its whole purpose."""

    async def beat(self, *, leader: str, running: str = "") -> None: ...


def stalled(*, queued: Optional[int], beat_age_s: Optional[float], stale_after_s: float) -> bool:
    """A STALLED lane: jobs are waiting and the beat is missing or older than
    ``stale_after_s``. An empty queue is never stalled, however old its beat — nobody
    is waiting for anything."""
    return bool(queued) and (beat_age_s is None or float(beat_age_s) > stale_after_s)


def process_name() -> str:
    """``host:pid`` — who leads, as a beat names it: a machine and a process."""
    return f"{socket.gethostname()}:{os.getpid()}"


# ── the lane ─────────────────────────────────────────────────────────────────────────


def _as_is(row: Mapping[str, Any]) -> Any:
    return row


def _no_label(_job: Any) -> str:
    return ""


def _any_kind() -> Optional[Collection[str]]:
    return None


def _ignore_result(_job: Any, _result: Any) -> None:
    return None


def _debug_error(_job: Any, exc: BaseException) -> None:
    logger.debug("event=lane_job_failed error=%s", type(exc).__name__)


async def _nothing_to_undo(_job: Any, deadline_s: float) -> None:
    logger.debug("event=lane_job_timeout deadline_s=%.3f", deadline_s)


def _debug_beat_error(exc: BaseException) -> None:
    logger.debug("event=lane_beat_failed error=%s", type(exc).__name__)


class LeaderLane:
    """Drain a :class:`JobQueue` while holding a :class:`LeadershipLock`, beating on a
    :class:`Heartbeat` — the four guarantees are in the module docstring.

    One object may be all three ports (a durable adapter usually is: one table, one
    lock, one beat row), and :class:`InMemoryLaneQueue` is.

    * ``work(job)`` does ONE job; ``deadline(job)`` says how many seconds it may take,
      asked before it starts (the host knows what a job weighs; the kernel does not).
    * ``parse(row)`` turns a queue row into the host's own job (the row itself by
      default) — every other callback receives that job.
    * ``label(job)`` names the running job on the beat.
    * ``kinds()`` says which kinds may run NOW (``None`` = any), asked before every
      peek: a budget that ran out holds some kinds back without dropping them.
    * ``on_result(job, result)`` / ``on_error(job, exc)`` receive a job that finished
      or raised — a job's own failure never stops the lane.
    * ``on_timeout(job, deadline_s)`` runs after an overrun job was cancelled and
      BEFORE the lock is released — the place to mark the job's work as interrupted.
    * ``on_beat_error(exc)`` receives a beat that failed: a missed beat shows as a
      stale lane, never as a crash.
    * ``housekeeping()`` runs on the LEADER at most every ``housekeeping_s`` of
      ``clock``. It owns its errors: one that raises ends :meth:`run`, which releases
      the lock on its way out, so another process takes over.
    * ``name`` is who leads on the beat (``host:pid`` by default).
    """

    def __init__(
        self,
        *,
        queue: JobQueue,
        lock: LeadershipLock,
        heartbeat: Heartbeat,
        work: Callable[[Any], Awaitable[Any]],
        deadline: Callable[[Any], Awaitable[float]],
        parse: Callable[[Mapping[str, Any]], Any] = _as_is,
        label: Callable[[Any], str] = _no_label,
        kinds: Callable[[], Optional[Collection[str]]] = _any_kind,
        on_result: Callable[[Any, Any], None] = _ignore_result,
        on_error: Callable[[Any, BaseException], None] = _debug_error,
        on_timeout: Callable[[Any, float], Awaitable[None]] = _nothing_to_undo,
        on_beat_error: Callable[[BaseException], None] = _debug_beat_error,
        housekeeping: Optional[Callable[[], Awaitable[Any]]] = None,
        beat_s: float = 10.0,
        name: Optional[str] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.queue = queue
        self.lock = lock
        self.heartbeat = heartbeat
        self._work = work
        self._deadline = deadline
        self._parse = parse
        self._label = label
        self._kinds = kinds
        self._on_result = on_result
        self._on_error = on_error
        self._on_timeout = on_timeout
        self._on_beat_error = on_beat_error
        self._housekeeping = housekeeping
        self.beat_s = float(beat_s)
        #: Who leads, as the beat names it — a machine and a process, nothing about a job.
        self.name = name if name is not None else process_name()
        self._clock = clock
        #: The job running right now, if any — for a log line, never a decision.
        self.current: Optional[Any] = None
        #: Set when a job overran its deadline and the lane was given up; :meth:`run`
        #: clears it and waits ``lead_retry_s`` before asking for the lock again.
        self.released = False

    async def run(
        self, *, poll_s: float = 2.0, lead_retry_s: float = 30.0, housekeeping_s: float = 300.0
    ) -> None:
        """Drain forever — but only while THIS lane leads, and one job at a time.

        Every loop: after a give-up, wait ``lead_retry_s``; not leading → wait
        ``lead_retry_s`` and ask again; leading → beat, run the housekeeping when its
        period has passed, then drain one job or wait ``poll_s``. The period is counted
        from the origin of ``clock``, so under the default monotonic clock the first
        housekeeping runs at once — except in the first ``housekeeping_s`` of that
        clock (on Linux, of the machine's uptime).

        On the way out — cancellation included — the lock is handed back at once, so
        the next process does not wait for this one's connection to time out."""
        swept_at = 0.0
        try:
            while True:
                if self.released:
                    self.released = False
                    await asyncio.sleep(lead_retry_s)
                if not await self.lock.lead():
                    await asyncio.sleep(lead_retry_s)
                    continue
                await self.beat("")
                if self._housekeeping is not None and self._clock() - swept_at >= housekeeping_s:
                    swept_at = self._clock()
                    await self._housekeeping()
                if not await self.drain_one():
                    await asyncio.sleep(poll_s)
        finally:
            with contextlib.suppress(Exception):
                await self.lock.release()

    async def beat(self, running: str = "") -> None:
        """One beat, naming ``running`` (``""`` between jobs). A failure goes to
        ``on_beat_error`` and nowhere else: a missed beat is a stale lane, not a crash."""
        try:
            await self.heartbeat.beat(leader=self.name, running=running)
        except Exception as exc:  # noqa: BLE001 — a missed beat shows as a stale lane
            self._on_beat_error(exc)

    async def _beat_while(self, job: Any) -> None:
        running = self._label(job)
        while True:
            await self.beat(running)
            await asyncio.sleep(self.beat_s)

    async def drain_one(self) -> bool:
        """Run the oldest runnable job, if any, under its deadline. Returns whether there
        was one. Does NOT ask for the lock — :meth:`run` does; a caller that drains by
        hand is the one that decided it leads."""
        row = await self.queue.peek(kinds=self._kinds())
        if row is None:
            return False
        job = self._parse(row)
        self.current = job
        deadline = await self._deadline(job)
        beat = asyncio.ensure_future(self._beat_while(job))
        work = asyncio.ensure_future(self._work(job))
        try:
            done, _ = await asyncio.wait({work}, timeout=deadline)
            if work in done:
                exc = work.exception()
                if exc is not None:
                    self._on_error(job, exc)
                else:
                    self._on_result(job, work.result())
            else:
                # HUNG: cancel without awaiting — a driver that ignores the cancel would
                # hold the lane exactly as long as it hangs — let the host undo, and give
                # the lane UP whatever the undo did.
                work.cancel()
                try:
                    await self._on_timeout(job, deadline)
                finally:
                    await self.lock.release()
                    self.released = True
        finally:
            beat.cancel()
            self.current = None
            await self.queue.remove(row["id"])
        return True


# ── the in-process double ────────────────────────────────────────────────────────────


class Lease:
    """The in-memory leadership slot: ONE holder at a time, shared by every handle
    built over it — two handles over one lease model two processes of a deployment."""

    def __init__(self) -> None:
        self.holder: Optional[object] = None


class InMemoryLaneQueue:
    """The lane's in-process double: a :class:`JobQueue`, a :class:`LeadershipLock`
    (over a :class:`Lease`) and a :class:`Heartbeat` in one object. Handles built over
    one ``lease`` and one ``rows`` list compete for the lane like processes do.

    ``push(**fields)`` appends a row under the next ``id``. When ``unique`` names
    fields, a push whose values for them match a WAITING row is refused (``False``) —
    the idempotency a durable queue gets from a UNIQUE constraint, so a push that
    races itself is one job; once that row is removed the same values are a new job.
    ``status()`` is ``{leader, beat_age_s, running, queued}`` by ``clock``."""

    def __init__(
        self,
        *,
        lease: Optional[Lease] = None,
        rows: Optional[list[dict[str, Any]]] = None,
        beat: Optional[dict[str, Any]] = None,
        unique: Sequence[str] = (),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lease = lease if lease is not None else Lease()
        self._rows: list[dict[str, Any]] = rows if rows is not None else []
        self._beat: dict[str, Any] = beat if beat is not None else {}
        self._unique = tuple(unique)
        self.clock = clock

    async def push(self, **fields: Any) -> bool:
        if self._unique:
            key = tuple(fields.get(k) for k in self._unique)
            if any(tuple(r.get(k) for k in self._unique) == key for r in self._rows):
                return False
        next_id = max((r["id"] for r in self._rows), default=0) + 1
        self._rows.append({**fields, "id": next_id})
        return True

    async def peek(
        self, *, kinds: Optional[Collection[str]] = None
    ) -> Optional[dict[str, Any]]:
        for r in self._rows:
            if kinds is None or r.get("kind") in kinds:
                return dict(r)
        return None

    async def remove(self, job_id: Any) -> None:
        self._rows[:] = [r for r in self._rows if r["id"] != int(job_id)]

    async def size(self) -> int:
        return len(self._rows)

    async def lead(self) -> bool:
        if self._lease.holder is None:
            self._lease.holder = self
        return self._lease.holder is self

    async def release(self) -> None:
        if self._lease.holder is self:
            self._lease.holder = None

    async def beat(self, *, leader: str, running: str = "") -> None:
        self._beat.update(leader=leader, at=self.clock(), running=running or "")

    async def status(self) -> dict[str, Any]:
        at = self._beat.get("at")
        return {
            "leader": self._beat.get("leader", ""),
            "beat_age_s": (self.clock() - at) if at is not None else None,
            "running": self._beat.get("running", ""),
            "queued": len(self._rows),
        }
