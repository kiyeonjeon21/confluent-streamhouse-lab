"""Experiment E6: what readers see when parts of the pipeline stop, change, or receive bad data.

Each scenario acts on the bench pipeline only, and records every observation
with a timestamp through a `Recorder`, so the sequence can be read back exactly:
what Flink reported, what Lightning served, and how long each change took.

The outside world is injected as a `World`, so the scenarios run against fakes
in unit tests. `cli.py` wires it to Flink, Kafka, Lightning, and the CLI.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from streamhouse_lab.measure import Recorder


@dataclass
class World:
    mt_status: Callable[[], dict]  # phase, detail, version, stopped of bench_passenger_state
    set_stopped: Callable[[bool], None]
    probe_state: Callable[[], str | None]  # probe passenger's inbound_arrival in Lightning
    probe_source: Callable[[], str | None]  # probe flight's estimated_time in Lightning
    write_marker: Callable[[int], str]  # writes a probe flight update, returns the expected text
    counts: Callable[[], dict]  # risk -> passengers, from a Flink snapshot of the output topic
    lightning_sample: Callable[[str], int]  # rows Lightning returns for a risk class (at most 5)
    run_sql: Callable[[str], None]
    write_new_passenger: Callable[[], str]  # all-new flights and itinerary; returns the passenger id
    passenger_visible: Callable[[str], bool]
    write_unreadable: Callable[[], None]
    topic_status: Callable[[], dict]  # Lightning and Tableflow status of the bench topics
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep


def wait_until(
    world: World, probe: Callable[[], bool], *, timeout: float, interval: float = 2.0
) -> float | None:
    """Seconds until `probe()` is true, or None after `timeout`."""
    start = world.clock()
    while True:
        if probe():
            return world.clock() - start
        if world.clock() - start > timeout:
            return None
        world.sleep(interval)


def observe(world: World, recorder: Recorder, step: str, **extra) -> dict:
    state = {"mt": world.mt_status(), "probe_state": world.probe_state(), **extra}
    recorder.event("step", step=step, **state)
    return state


def stop_resume(world: World, recorder: Recorder, *, writes: int = 3, gap: float = 20.0,
                hold: float = 60.0, timeout: float = 600.0) -> dict:  # fmt: skip
    """Stop the materialized table, keep writing, then resume and time the catch-up."""
    before = observe(world, recorder, "before")
    started = world.clock()
    world.set_stopped(True)
    stop_s = wait_until(world, lambda: world.mt_status().get("phase") == "STOPPED", timeout=timeout)
    observe(world, recorder, "stopped", stop_s=stop_s, call_to_stopped_s=world.clock() - started)

    expected = None
    for i in range(writes):
        expected = world.write_marker(100 + i)
        world.sleep(gap)
        observe(
            world, recorder, f"write_{i}_while_stopped", expected=expected, probe_source=world.probe_source()
        )
    world.sleep(hold)
    stale = observe(world, recorder, "held_stopped", expected=expected, counts=world.counts())

    resumed_at = world.clock()
    world.set_stopped(False)
    running_s = wait_until(world, lambda: world.mt_status().get("phase") == "RUNNING", timeout=timeout)
    caught_up_s = wait_until(
        world, lambda: (world.probe_state() or "").startswith(expected or "?"), timeout=timeout, interval=1.0
    )
    after = observe(
        world, recorder, "resumed",
        running_s=running_s, caught_up_s=caught_up_s, since_resume_call_s=world.clock() - resumed_at,
        counts=world.counts(),
    )  # fmt: skip
    result = {
        "stop_s": stop_s,
        "lightning_stale_while_stopped": stale["probe_state"] == before["probe_state"],
        "running_s": running_s,
        "caught_up_s": caught_up_s,
        "counts_after": after["counts"],
    }
    recorder.event("result", scenario="stop-resume", **result)
    return result


def evolve(world: World, recorder: Recorder, evolve_sql: str, *, settle: float = 90.0,
           timeout: float = 600.0) -> dict:  # fmt: skip
    """CREATE OR ALTER with a stricter filter; look for zombie rows and for lost join state."""
    before_status = world.mt_status()
    before = observe(
        world, recorder, "before",
        counts=world.counts(), lightning_no_connection=world.lightning_sample("NO_CONNECTION"),
    )  # fmt: skip
    started = world.clock()
    world.run_sql(evolve_sql)
    version_s = wait_until(
        world,
        lambda: (
            (s := world.mt_status()).get("version") != before_status.get("version")
            and s.get("phase") == "RUNNING"
        ),
        timeout=timeout,
    )
    observe(world, recorder, "evolved", version_s=version_s, since_call_s=world.clock() - started)
    world.sleep(settle)
    after = observe(
        world, recorder, "settled",
        counts=world.counts(), lightning_no_connection=world.lightning_sample("NO_CONNECTION"),
    )  # fmt: skip

    # Existing passenger: its itinerary was read by the old query, whose state is gone.
    expected = world.write_marker(200)
    old_key_s = wait_until(
        world, lambda: (world.probe_state() or "").startswith(expected), timeout=180.0, interval=2.0
    )
    observe(world, recorder, "existing_passenger_update", expected=expected, seen_s=old_key_s)
    # New passenger: flights and itinerary all written after the evolution.
    new_id = world.write_new_passenger()
    new_key_s = wait_until(world, lambda: world.passenger_visible(new_id), timeout=180.0, interval=2.0)
    observe(world, recorder, "new_passenger", passenger=new_id, seen_s=new_key_s)
    result = {
        "version_s": version_s,
        "no_connection_before": (before["counts"] or {}).get("NO_CONNECTION"),
        "no_connection_after": (after["counts"] or {}).get("NO_CONNECTION"),
        "lightning_no_connection_after": after["lightning_no_connection"],
        "existing_passenger_updated_s": old_key_s,
        "new_passenger_visible_s": new_key_s,
    }
    recorder.event("result", scenario="evolve", **result)
    return result


def unreadable_record(world: World, recorder: Recorder, *, watch: float = 600.0,
                      interval: float = 30.0) -> dict:  # fmt: skip
    """Write one record whose value cannot be deserialized into the bench source topic,
    then watch Flink, Lightning, and Tableflow, and whether a valid write still flows."""
    observe(world, recorder, "before", topics=world.topic_status(), probe_source=world.probe_source())
    world.write_unreadable()
    written = world.clock()
    expected = None
    while world.clock() - written < watch:
        world.sleep(interval)
        if expected is None and world.clock() - written >= 60:
            expected = world.write_marker(300)  # a valid write one minute after the bad one
        observe(
            world, recorder, "after_unreadable",
            since_s=world.clock() - written, topics=world.topic_status(),
            probe_source=world.probe_source(), expected=expected,
        )  # fmt: skip
    final = observe(world, recorder, "end", topics=world.topic_status(), probe_source=world.probe_source())
    result = {
        "mt_phase": final["mt"].get("phase"),
        "valid_write_in_lightning_source": bool(expected)
        and (final["probe_source"] or "").startswith(expected),
        "valid_write_in_lightning_state": bool(expected)
        and (final["probe_state"] or "").startswith(expected),
        "topics": final["topics"],
    }
    recorder.event("result", scenario="unreadable-record", **result)
    return result
