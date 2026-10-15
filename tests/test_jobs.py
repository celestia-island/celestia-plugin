"""Job table lifecycle: submit → progress → cancel/succeed/fail."""

import threading
import time

from celestia_plugin.jobs import CancelledError, JobTable


def drain(table, deadline=5.0):
    """Wait until no job is queued/running, then return all snapshots."""
    end = time.time() + deadline
    while time.time() < end:
        rows = table.list()
        if all(r["state"] in ("succeeded", "failed", "cancelled") for r in rows):
            return rows
        time.sleep(0.01)
    raise AssertionError("jobs did not settle in time")


def test_submit_success_with_result():
    table = JobTable()
    job = table.submit("add", lambda job: {"sum": 40 + 2})
    rows = drain(table)
    assert rows[0]["state"] == "succeeded"
    assert rows[0]["result"] == {"sum": 40 + 2}
    assert rows[0]["job_id"] == job.id
    assert rows[0]["finished_at"] >= rows[0]["started_at"]


def test_progress_is_visible_while_running():
    table = JobTable()
    seen = []

    def work(job):
        for frac in (0.1, 0.5, 0.9):
            job.report_progress(frac, f"step {frac}")
            seen.append((job.progress, job.progress_message))
            time.sleep(0.01)
        return "done"

    table.submit("work", work)
    drain(table)
    assert seen[0] == (0.1, "step 0.1")
    assert seen[-1] == (0.9, "step 0.9")


def test_progress_frac_range_enforced():
    table = JobTable()
    holder = {}

    def work(job):
        holder["job"] = job
        try:
            job.report_progress(1.5)
        except ValueError:
            return "rejected"
        return "accepted-bad"

    table.submit("work", work)
    drain(table)
    assert holder["job"].result == "rejected"


def test_cancel_cooperative():
    table = JobTable()
    started = threading.Event()

    def work(job):
        started.set()
        for _ in range(500):
            job.check_cancelled()
            time.sleep(0.01)
        return "never"

    job = table.submit("work", work)
    assert started.wait(5)
    ack = table.cancel(job.id)
    rows = drain(table)
    assert ack["accepted"] is True
    assert rows[0]["state"] == "cancelled"
    assert rows[0]["result"] is None


def test_cancel_unknown_and_terminal():
    table = JobTable()
    assert table.cancel("nonexistent")["accepted"] is False
    job = table.submit("quick", lambda _job: 1)
    drain(table)
    assert table.cancel(job.id)["accepted"] is False
    assert "already" in table.cancel(job.id)["reason"]


def test_failure_captures_exception():
    table = JobTable()

    def work(_job):
        raise RuntimeError("boom")

    table.submit("work", work)
    rows = drain(table)
    assert rows[0]["state"] == "failed"
    assert "RuntimeError: boom" in rows[0]["error"]


def test_max_workers_bounds_concurrency():
    table = JobTable(max_workers=1)
    running = []
    peak = []

    def work(_job):
        running.append(1)
        peak.append(len(running))
        time.sleep(0.05)
        running.pop()
        return None

    for _ in range(4):
        table.submit("w", work)
    drain(table)
    assert max(peak) == 1


def test_max_queued_bounds_live_jobs():
    import pytest

    from celestia_plugin.jobs import QueueFullError

    table = JobTable(max_workers=1, max_queued=1)
    release = threading.Event()
    table.submit("blocker", lambda _job: release.wait(5) or None)
    time.sleep(0.05)  # the blocker occupies the single live slot
    with pytest.raises(QueueFullError):
        table.submit("second", lambda _job: 1)
    release.set()
    drain(table)
    # slot freed → submits pass again
    table.submit("after", lambda _job: 1)
    drain(table)


def test_max_queued_zero_is_rejected():
    import pytest

    with pytest.raises(ValueError):
        JobTable(max_queued=0)


def test_max_workers_zero_is_rejected():
    import pytest
    with pytest.raises(ValueError):
        JobTable(max_workers=0)


def test_event_sink_receives_lifecycle():
    events = []
    table = JobTable()
    table.set_event_sink(lambda kind, payload: events.append(kind))
    table.submit("w", lambda _job: 1)
    drain(table)
    assert "jobs.submitted" in events
    assert "jobs.started" in events
    assert "jobs.succeeded" in events


def test_snapshot_hides_result_of_failed_jobs():
    table = JobTable()

    def work(_job):
        raise ValueError("no result for you")

    table.submit("work", work)
    rows = drain(table)
    assert rows[0]["result"] is None
    assert rows[0]["error"]


def test_cancelled_error_is_caught_as_cancelled_state():
    table = JobTable()
    started = threading.Event()

    def work(job):
        started.set()
        raise CancelledError("explicit")

    table.submit("work", work)
    started.wait(5)
    rows = drain(table)
    assert rows[0]["state"] == "cancelled"
