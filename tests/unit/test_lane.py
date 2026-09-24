"""
The single-consumer lane (``cogno_homeo.lane``) — each guarantee against the failure it
prevents, and a control that PRODUCES that failure wherever one can be produced:

* ONE leader drains, one job at a time, the oldest runnable first;
* a push that repeats a waiting job is one job;
* a job that raises is reported and removed, and the lane goes on;
* a HUNG job is cancelled without being awaited, the host undoes, the lock is RELEASED
  and another leader takes over — a hung leader does not retain the lane;
* the heartbeat names the leader and the running job, and a stale beat over waiting
  jobs is a STALLED lane;
* housekeeping runs on the leader only, at most once per period;
* a lane that stops hands the lock back at once.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass

import pytest

from cogno_homeo import (
    Heartbeat,
    InMemoryLaneQueue,
    JobQueue,
    LeaderLane,
    LeadershipLock,
    Lease,
)
from cogno_homeo.lane import process_name, stalled


def _const(seconds):
    async def deadline(_job):
        return seconds

    return deadline


class Recorder:
    """Work that records what finished, in what order, and how many ran AT ONCE."""

    def __init__(self, *, fail=(), hang=()):
        self.ran: list = []
        self.active = 0
        self.max_active = 0
        self.fail, self.hang = set(fail), set(hang)
        self.tasks: list = []

    async def __call__(self, job):
        self.tasks.append(asyncio.current_task())
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0)
            if job["name"] in self.hang:
                await asyncio.sleep(3600)
            if job["name"] in self.fail:
                raise RuntimeError(job["name"])
            self.ran.append(job["name"])
            return job["name"].upper()
        finally:
            self.active -= 1

    async def stop(self):
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


def _lane(queue, work, **kw):
    kw.setdefault("deadline", _const(5.0))
    kw.setdefault("name", "p1")
    return LeaderLane(queue=queue, lock=queue, heartbeat=queue, work=work, **kw)


async def _until(predicate, *, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        if loop.time() > end:
            raise AssertionError("the condition never became true")
        await asyncio.sleep(0.001)


async def _stop(*tasks):
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# ── the ports ────────────────────────────────────────────────────────────────────────


def test_the_in_memory_double_is_all_three_ports_and_a_bare_object_is_none():
    q = InMemoryLaneQueue()
    assert isinstance(q, LeadershipLock) and isinstance(q, JobQueue) and isinstance(q, Heartbeat)
    assert not isinstance(object(), LeadershipLock)
    assert not isinstance(object(), JobQueue)
    assert not isinstance(object(), Heartbeat)


async def test_only_ONE_holder_of_a_lease_leads_until_it_releases():
    lease = Lease()
    a, b = InMemoryLaneQueue(lease=lease), InMemoryLaneQueue(lease=lease)
    assert await a.lead() is True
    assert await b.lead() is False, "two leaders at once"
    assert await a.lead() is True, "the holder keeps the lane while alive"
    await b.release()
    assert lease.holder is a, "releasing a lock one does not hold must be a no-op"
    await a.release()
    assert await b.lead() is True and await a.lead() is False


# ── one leader, one job at a time, in order ──────────────────────────────────────────


async def test_only_the_LEADER_drains_one_job_at_a_time_oldest_first():
    lease, rows = Lease(), []
    qa, qb = InMemoryLaneQueue(lease=lease, rows=rows), InMemoryLaneQueue(lease=lease, rows=rows)
    for name in ("j1", "j2", "j3"):
        await qa.push(name=name)
    work_a, work_b = Recorder(), Recorder()
    a, b = _lane(qa, work_a, name="a"), _lane(qb, work_b, name="b")
    assert await qa.lead()
    tasks = [asyncio.ensure_future(lane.run(poll_s=0.001, lead_retry_s=0.001)) for lane in (a, b)]
    try:
        await _until(lambda: not rows)
    finally:
        await _stop(*tasks)
    assert work_a.ran == ["j1", "j2", "j3"], "not drained oldest first"
    assert work_b.ran == [], "a process that does not lead drained"
    assert work_a.max_active == 1, "two jobs ran at once"
    assert lease.holder is None, "stopping did not hand the lane back"


async def test_a_kind_held_back_WAITS_in_the_queue_and_runs_once_allowed():
    q = InMemoryLaneQueue()
    await q.push(name="r1", kind="slow")
    await q.push(name="c1", kind="fast")
    await q.push(name="c2", kind="fast")
    allowed: dict = {"kinds": ("fast",)}
    work = Recorder()
    lane = _lane(q, work, kinds=lambda: allowed["kinds"])
    while await lane.drain_one():
        pass
    assert work.ran == ["c1", "c2"] and await q.size() == 1, "the held-back kind was dropped"
    allowed["kinds"] = None
    assert await lane.drain_one()
    assert work.ran == ["c1", "c2", "r1"]
    assert await lane.drain_one() is False, "an empty queue ran something"


# ── idempotency ──────────────────────────────────────────────────────────────────────


async def test_a_push_that_repeats_a_WAITING_job_is_one_row_and_a_new_job_after_it_ran():
    q = InMemoryLaneQueue(unique=("item", "kind"))
    assert await q.push(item="i1", kind="k", actor="x") is True
    assert await q.push(item="i1", kind="k", actor="y") is False
    assert await q.push(item="i1", kind="other") is True
    assert await q.size() == 2
    first = await q.peek()
    assert first is not None and first["actor"] == "x", "the repeat replaced the waiting row"
    await q.remove(first["id"])
    assert await q.push(item="i1", kind="k") is True, "a job that ran must be pushable again"
    # the control: without `unique` the same push IS two rows
    free = InMemoryLaneQueue()
    assert await free.push(item="i1", kind="k") and await free.push(item="i1", kind="k")
    assert await free.size() == 2


# ── a job's own failure ──────────────────────────────────────────────────────────────


async def test_a_job_that_RAISES_is_reported_removed_and_the_next_one_runs():
    q = InMemoryLaneQueue()
    await q.push(name="bad")
    await q.push(name="good")
    errors, results = [], []
    lane = _lane(q, Recorder(fail={"bad"}),
                 on_error=lambda job, exc: errors.append((job["name"], type(exc).__name__)),
                 on_result=lambda job, result: results.append((job["name"], result)))
    assert await lane.drain_one() and await lane.drain_one()
    assert errors == [("bad", "RuntimeError")]
    assert results == [("good", "GOOD")]
    assert await q.size() == 0 and lane.current is None


async def test_the_default_callbacks_report_at_DEBUG_and_never_stop_the_lane(caplog):
    q = InMemoryLaneQueue()
    await q.push(name="bad")
    await q.push(name="stuck")
    await q.push(name="good")
    work = Recorder(fail={"bad"}, hang={"stuck"})
    lane = LeaderLane(queue=q, lock=q, heartbeat=q, work=work, deadline=_const(0.02))
    with caplog.at_level(logging.DEBUG, logger="cogno_homeo.lane"):
        assert await lane.drain_one() and await lane.drain_one() and await lane.drain_one()
    await work.stop()
    assert work.ran == ["good"]
    assert "event=lane_job_failed error=RuntimeError" in caplog.text
    assert "event=lane_job_timeout" in caplog.text
    assert all(r.levelno == logging.DEBUG for r in caplog.records if r.name == "cogno_homeo.lane")


async def test_parse_hands_the_hosts_OWN_job_to_every_callback():
    @dataclass(frozen=True)
    class Job:
        id: int
        name: str

    q = InMemoryLaneQueue()
    await q.push(name="j")
    seen: list = []

    async def work(job):
        seen.append(("work", job))
        return 1

    async def deadline(job):
        seen.append(("deadline", job))
        return 5.0

    lane = LeaderLane(queue=q, lock=q, heartbeat=q, work=work, deadline=deadline,
                      parse=lambda row: Job(row["id"], row["name"]),
                      on_result=lambda job, result: seen.append(("result", job)))
    assert await lane.drain_one()
    job = Job(1, "j")
    assert seen == [("deadline", job), ("work", job), ("result", job)]


# ── the deadline: a hung job gives the lane UP ───────────────────────────────────────


async def test_a_HUNG_job_is_cancelled_the_lane_RELEASED_and_another_leader_finishes_the_queue():
    lease, rows = Lease(), []
    qa, qb = InMemoryLaneQueue(lease=lease, rows=rows), InMemoryLaneQueue(lease=lease, rows=rows)
    await qa.push(name="stuck")
    await qa.push(name="next")
    undone = []

    async def on_timeout(job, deadline_s):
        undone.append((job["name"], deadline_s, lease.holder is qa))

    stuck = Recorder(hang={"stuck"})
    hung = _lane(qa, stuck, deadline=_const(0.02), on_timeout=on_timeout)
    work_b = Recorder()
    healthy = _lane(qb, work_b)
    assert await qa.lead()
    assert await hung.drain_one()
    assert undone == [("stuck", 0.02, True)], "the host must undo BEFORE the lock goes"
    assert hung.released and lease.holder is None, "the lane was not given up"
    assert [r["name"] for r in rows] == ["next"], "the hung job stayed in the queue"
    await _until(lambda: stuck.active == 0)
    assert stuck.tasks[0].cancelled(), "the overrun job was not cancelled"
    assert await qb.lead()
    assert await healthy.drain_one()
    assert work_b.ran == ["next"]


async def test_CONTROL_a_deadline_that_does_not_pass_leaves_the_lane_HELD_by_the_hung_job():
    lease = Lease()
    q = InMemoryLaneQueue(lease=lease)
    await q.push(name="stuck")
    stuck = Recorder(hang={"stuck"})
    held = _lane(q, stuck, deadline=_const(3600.0))
    assert await q.lead()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(held.drain_one(), timeout=0.05)
    assert held.released is False and lease.holder is q, "the failure the deadline prevents"
    await stuck.stop()


async def test_a_job_that_IGNORES_its_cancel_does_not_hold_the_lane():
    lease = Lease()
    q = InMemoryLaneQueue(lease=lease)
    await q.push(name="deaf")
    let_go = asyncio.Event()
    cancels: list = []
    tasks: list = []

    async def deaf(_job):
        tasks.append(asyncio.current_task())
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancels.append(1)
            await let_go.wait()          # swallows the cancel and keeps "running"

    lane = _lane(q, deaf, deadline=_const(0.02))
    assert await q.lead()
    assert await asyncio.wait_for(lane.drain_one(), timeout=1.0), "the lane waited on a deaf job"
    assert lane.released and lease.holder is None
    await _until(lambda: cancels == [1])
    assert not tasks[0].done(), "the control: the job really is still running"
    let_go.set()
    await asyncio.wait_for(tasks[0], timeout=1.0)


async def test_an_undo_that_RAISES_still_gives_the_lane_up():
    lease = Lease()
    q = InMemoryLaneQueue(lease=lease)
    await q.push(name="stuck")

    async def broken_undo(_job, _deadline_s):
        raise RuntimeError("undo failed")

    stuck = Recorder(hang={"stuck"})
    lane = _lane(q, stuck, deadline=_const(0.02), on_timeout=broken_undo)
    assert await q.lead()
    with pytest.raises(RuntimeError, match="undo failed"):
        await lane.drain_one()
    assert lane.released and lease.holder is None, "a failed undo kept the lane"
    assert await q.size() == 0
    await stuck.stop()


async def test_after_giving_the_lane_up_it_WAITS_so_another_process_takes_over():
    lease, rows = Lease(), []
    qa, qb = InMemoryLaneQueue(lease=lease, rows=rows), InMemoryLaneQueue(lease=lease, rows=rows)
    await qa.push(name="stuck")
    await qa.push(name="next")
    stuck, work_b = Recorder(hang={"stuck"}), Recorder()
    hung = _lane(qa, stuck, deadline=_const(0.02), name="a")
    healthy = _lane(qb, work_b, name="b")
    assert await qa.lead()
    ta = asyncio.ensure_future(hung.run(poll_s=0.001, lead_retry_s=60.0))
    tb = None
    try:
        await _until(lambda: [r["name"] for r in rows] == ["next"] and lease.holder is None)
        tb = asyncio.ensure_future(healthy.run(poll_s=0.001, lead_retry_s=0.001))
        await _until(lambda: work_b.ran == ["next"])
        assert lease.holder is qb, "the lane did not move to the other process"
        assert stuck.ran == []
    finally:
        await _stop(ta, *([tb] if tb else []))
        await stuck.stop()


# ── the heartbeat ────────────────────────────────────────────────────────────────────


async def test_the_leader_BEATS_naming_itself_and_the_job_it_runs():
    now = [100.0]
    q = InMemoryLaneQueue(clock=lambda: now[0])
    started, finish = asyncio.Event(), asyncio.Event()

    async def slow(_job):
        started.set()
        await finish.wait()

    await q.push(name="j", label="the-job")
    lane = _lane(q, slow, name="host:1", label=lambda job: job["label"], beat_s=0.001)
    task = asyncio.ensure_future(lane.run(poll_s=0.001, lead_retry_s=0.001))
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await _until(lambda: q._beat.get("running") == "the-job")
        status = await q.status()
        assert status == {"leader": "host:1", "beat_age_s": 0.0, "running": "the-job",
                          "queued": 1}
        assert lane.current is not None and lane.current["name"] == "j"
        finish.set()
        await _until(lambda: q._beat.get("running") == "" and not q._rows)
        assert lane.current is None
    finally:
        await _stop(task)


async def test_jobs_waiting_under_a_STALE_beat_are_a_stalled_lane_and_an_empty_queue_never_is():
    now = [0.0]
    q = InMemoryLaneQueue(clock=lambda: now[0])
    lane = _lane(q, Recorder())
    await lane.beat("")
    await q.push(name="j")
    fresh = await q.status()
    assert not stalled(queued=fresh["queued"], beat_age_s=fresh["beat_age_s"], stale_after_s=120.0)
    now[0] = 121.0
    old = await q.status()
    assert stalled(queued=old["queued"], beat_age_s=old["beat_age_s"], stale_after_s=120.0)
    assert stalled(queued=1, beat_age_s=None, stale_after_s=120.0), "a lane that never beat"
    assert not stalled(queued=1, beat_age_s=120.0, stale_after_s=120.0), "the bound is strict"
    # the control: the same stale beat over an EMPTY queue is not stalled
    assert not stalled(queued=0, beat_age_s=121.0, stale_after_s=120.0)
    assert not stalled(queued=None, beat_age_s=None, stale_after_s=120.0)
    assert (await InMemoryLaneQueue().status())["beat_age_s"] is None


async def test_a_beat_that_FAILS_is_reported_and_the_job_still_runs():
    class BrokenBeat(InMemoryLaneQueue):
        async def beat(self, *, leader, running=""):
            raise ConnectionError("the beat's store is gone")

    q = BrokenBeat()
    await q.push(name="j")
    seen: list = []
    work = Recorder()
    lane = _lane(q, work, on_beat_error=lambda exc: seen.append(type(exc).__name__))
    await lane.beat("")
    assert await lane.drain_one()
    assert work.ran == ["j"]
    assert seen and set(seen) == {"ConnectionError"}
    # the default reports at DEBUG and raises nothing either
    await LeaderLane(queue=q, lock=q, heartbeat=q, work=work, deadline=_const(1.0)).beat("")


def test_the_default_leader_name_is_the_machine_and_the_process():
    q = InMemoryLaneQueue()
    lane = LeaderLane(queue=q, lock=q, heartbeat=q, work=Recorder(), deadline=_const(1.0))
    assert lane.name == process_name() == f"{socket.gethostname()}:{os.getpid()}"
    assert _lane(q, Recorder(), name="custom").name == "custom"


# ── housekeeping and shutdown ────────────────────────────────────────────────────────


async def test_housekeeping_runs_on_the_LEADER_at_most_once_per_period():
    now = [1000.0]
    ran: list = []

    async def sweep():
        ran.append(now[0])

    lease = Lease()
    q = InMemoryLaneQueue(lease=lease)
    lane = _lane(q, Recorder(), housekeeping=sweep, clock=lambda: now[0])
    task = asyncio.ensure_future(lane.run(poll_s=0.001, lead_retry_s=0.001, housekeeping_s=300.0))
    try:
        await _until(lambda: len(ran) == 1)
        await asyncio.sleep(0.02)
        assert ran == [1000.0], "housekeeping ran twice inside one period"
        now[0] += 300.0
        await _until(lambda: len(ran) == 2)
    finally:
        await _stop(task)
    # the control: a lane that does NOT lead never runs it
    other: list = []

    async def other_sweep():
        other.append(1)

    held = Lease()
    holder = InMemoryLaneQueue(lease=held)
    assert await holder.lead()
    follower = _lane(InMemoryLaneQueue(lease=held), Recorder(), housekeeping=other_sweep,
                     clock=lambda: now[0])
    task = asyncio.ensure_future(follower.run(poll_s=0.001, lead_retry_s=0.001))
    try:
        await asyncio.sleep(0.02)
    finally:
        await _stop(task)
    assert other == [] and held.holder is holder


async def test_a_housekeeping_that_RAISES_ends_the_run_and_hands_the_lock_back():
    lease = Lease()
    q = InMemoryLaneQueue(lease=lease)

    async def broken():
        raise RuntimeError("sweep failed")

    lane = _lane(q, Recorder(), housekeeping=broken, clock=lambda: 1000.0)
    with pytest.raises(RuntimeError, match="sweep failed"):
        await asyncio.wait_for(lane.run(poll_s=0.001, lead_retry_s=0.001), timeout=1.0)
    assert lease.holder is None


async def test_a_lane_that_is_CANCELLED_hands_the_lock_back_at_once():
    lease = Lease()
    q = InMemoryLaneQueue(lease=lease)
    lane = _lane(q, Recorder())
    task = asyncio.ensure_future(lane.run(poll_s=0.001, lead_retry_s=0.001))
    await _until(lambda: lease.holder is q)
    await _stop(task)
    assert lease.holder is None
