"""Deterministic River Air datasets for Phase 4 experiments.

Pure functions only: the same seed and size always give the same records, so a
measurement can be repeated on an identical dataset. Records match the Avro
schemas Flink registers for the bench_ tables: the key holds the primary key
column, the value holds every other column, and timestamps are naive datetimes
(Avro local-timestamp-millis).

Shape, modelled on the quick start: inbound flights land at SFO, onward flights
leave SFO for a handful of destinations, and each passenger takes one inbound
flight and, usually, one onward flight. Connection times are spread so that
roughly a quarter of connecting passengers are AT_RISK (under 45 minutes).
"""

from __future__ import annotations

import datetime as dt
import random
from collections.abc import Iterator
from dataclasses import dataclass

HUB = "SFO"
ORIGINS = ("ORD", "JFK", "DEN", "ATL", "BOS", "IAD", "DFW", "MSP")
DESTINATIONS = ("SEA", "PDX", "LAX", "SAN", "PHX", "SLC")
SERVICE_DAY = dt.datetime(2026, 12, 1)
PASSENGERS_PER_INBOUND = 150
CONNECTING_SHARE = 0.85
# Minutes between inbound arrival and onward departure, with weights.
CONNECTION_BUCKETS = ((20, 0.10), (35, 0.15), (60, 0.35), (120, 0.25), (300, 0.15))

# The passenger whose inbound flight E1 moves to create freshness markers.
PROBE_PASSENGER = "B-P000000"


@dataclass(frozen=True)
class Record:
    topic: str
    key: dict
    value: dict


@dataclass(frozen=True)
class Dataset:
    flights: list[Record]
    itineraries: list[Record]

    def records(self) -> Iterator[Record]:
        # Flights first, so every itinerary's flights exist when the join sees it.
        yield from self.flights
        yield from self.itineraries


def flight_record(flight_id: str, origin: str, destination: str, scheduled: dt.datetime,
                  estimated: dt.datetime | None = None, status: str = "ON_TIME") -> Record:  # fmt: skip
    return Record(
        topic="bench_flight_status",
        key={"flight_id": flight_id},
        value={
            "origin": origin,
            "destination": destination,
            "scheduled_time": scheduled,
            "estimated_time": estimated or scheduled,
            "status": status,
        },
    )


def generate(passengers: int, seed: int) -> Dataset:
    if passengers < 1:
        raise ValueError("passengers must be at least 1")
    rng = random.Random(seed)  # noqa: S311 - reproducible test data, not security
    inbound_count = -(-passengers // PASSENGERS_PER_INBOUND)

    flights: list[Record] = []
    inbound: list[tuple[str, dt.datetime]] = []
    for i in range(inbound_count):
        flight_id = f"B-IN{i:05d}"
        arrival = SERVICE_DAY + dt.timedelta(minutes=360 + rng.randrange(0, 14 * 60, 5))
        flights.append(flight_record(flight_id, rng.choice(ORIGINS), HUB, arrival))
        inbound.append((flight_id, arrival))

    onward_by_slot: dict[tuple[str, dt.datetime], str] = {}
    minutes, weights = zip(*CONNECTION_BUCKETS, strict=True)
    itineraries: list[Record] = []
    for p in range(passengers):
        passenger_id = f"B-P{p:06d}"
        inbound_id, arrival = inbound[p // PASSENGERS_PER_INBOUND]
        connecting_id = None
        if p == 0 or rng.random() < CONNECTING_SHARE:
            gap = rng.choices(minutes, weights)[0]
            destination = rng.choice(DESTINATIONS)
            departure = arrival + dt.timedelta(minutes=gap)
            slot = (destination, departure)
            if slot not in onward_by_slot:
                onward_id = f"B-OUT{len(onward_by_slot):05d}"
                onward_by_slot[slot] = onward_id
                flights.append(flight_record(onward_id, HUB, destination, departure))
            connecting_id = onward_by_slot[slot]
        itineraries.append(
            Record(
                topic="bench_passenger_itineraries",
                key={"passenger_id": passenger_id},
                value={"inbound_flight_id": inbound_id, "connecting_flight_id": connecting_id},
            )
        )
    return Dataset(flights=flights, itineraries=itineraries)


def probe_flight(dataset: Dataset) -> Record:
    """The inbound flight of PROBE_PASSENGER, whose estimated_time E1 rewrites."""
    probe = next(r for r in dataset.itineraries if r.key["passenger_id"] == PROBE_PASSENGER)
    flight_id = probe.value["inbound_flight_id"]
    return next(r for r in dataset.flights if r.key["flight_id"] == flight_id)


def marker_update(flight: Record, sample: int) -> Record:
    """A delay of (sample + 1) minutes. Each sample's estimated_time is unique, so the
    reader can tell exactly which write it is seeing."""
    scheduled = flight.value["scheduled_time"]
    return flight_record(
        flight.key["flight_id"],
        flight.value["origin"],
        flight.value["destination"],
        scheduled,
        scheduled + dt.timedelta(minutes=sample + 1),
        "DELAYED",
    )


def expected_risk(dataset: Dataset) -> dict[str, int]:
    """Risk counts that bench_passenger_state should converge to, computed with the same
    rules as the materialized table, so a run can confirm the dataset fully landed."""
    flights = {r.key["flight_id"]: r.value for r in dataset.flights}
    counts = {"OK": 0, "AT_RISK": 0, "NO_CONNECTION": 0}
    for itinerary in dataset.itineraries:
        connecting = itinerary.value["connecting_flight_id"]
        if connecting is None:
            counts["NO_CONNECTION"] += 1
            continue
        inbound = flights[itinerary.value["inbound_flight_id"]]["estimated_time"]
        onward = flights[connecting]["estimated_time"]
        minutes = int((onward - inbound).total_seconds() // 60)
        counts["AT_RISK" if minutes < 45 else "OK"] += 1
    return counts
