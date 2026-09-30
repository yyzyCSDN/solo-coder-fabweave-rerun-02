"""Cross-plant transfer scan chains.

A transfer moves material through four mandatory scan stages in order:
``loaded`` -> ``departed`` -> ``arrived`` -> ``unloaded``.  The chain is an
append-only state machine over :class:`fabweave.core.events.EventLog`, so it
inherits optimistic concurrency, event-time monotonicity and event-id
idempotency.

Rules enforced here:

* a stage scan is only accepted when its predecessor was scanned or
  explicitly skipped;
* rescanning a recorded stage replays the original scan instead of booking
  the stage a second time;
* a stage can only be jumped over through :meth:`TransferChain.skip_stage`,
  which demands a non-empty reason;
* the scanned manifest and unit count must stay consistent from load through
  unload;
* only a completed (``unloaded``) transfer can be linked back into the
  material genealogy.

The departure hold gate lives in the workflow layer
(``record_transfer_scan``), which composes this chain with hold release.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import EventLog
from fabweave.genealogy.carrier import GenealogyGraph, ancestry

STAGES: tuple[str, ...] = ("loaded", "departed", "arrived", "unloaded")
TERMINAL_STAGE = "unloaded"
_SCAN_KIND = "transfer_stage_scanned"
_SKIP_KIND = "transfer_stage_skipped"
_LINK_KIND = "transfer_genealogy_linked"


@dataclass(frozen=True, slots=True)
class StageRecord:
    stage: str
    event_id: str
    occurred_at: int
    sequence: int
    material_ids: tuple[str, ...] = ()
    units: int | None = None
    skipped: bool = False
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class TransferStatus:
    transfer_id: str
    scanned: tuple[str, ...]
    skipped: tuple[str, ...]
    pending: tuple[str, ...]
    current_stage: str | None
    transferred: bool

    @property
    def completed(self) -> tuple[str, ...]:
        return tuple(stage for stage in STAGES if stage in set(self.scanned) | set(self.skipped))


@dataclass(frozen=True, slots=True)
class GenealogyLink:
    transfer_id: str
    origin_id: str
    received_id: str
    units: int | None
    ancestry_ids: tuple[str, ...]
    graph: GenealogyGraph
    event_sequence: int
    replay: bool


def _stream(transfer_id: str) -> str:
    return f"transfer:{transfer_id}"


def _normalize_materials(material_ids: Any) -> tuple[str, ...]:
    if material_ids is None:
        return ()
    if isinstance(material_ids, str):
        raise ValueError("material_ids must be a collection of identifiers")
    values = tuple(material_ids)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("material identifiers must be non-empty strings")
    return values


def _normalize_units(units: Any) -> int | None:
    if units is None:
        return None
    if not isinstance(units, int) or units < 0:
        raise ValueError("units must be a non-negative integer")
    return units


class TransferChain:
    """Append-only state machine for one cross-plant transfer per stream."""

    def __init__(self, log: EventLog) -> None:
        self._log = log

    # ------------------------------------------------------------------ read

    def _records(self, transfer_id: str) -> dict[str, StageRecord]:
        records: dict[str, StageRecord] = {}
        for event in self._log.read(_stream(transfer_id)):
            if event.kind == _SCAN_KIND:
                records[event.data["stage"]] = StageRecord(
                    event.data["stage"],
                    event.event_id,
                    event.occurred_at,
                    event.sequence,
                    tuple(event.data.get("materials", ())),
                    event.data.get("units"),
                )
            elif event.kind == _SKIP_KIND:
                records[event.data["stage"]] = StageRecord(
                    event.data["stage"],
                    event.event_id,
                    event.occurred_at,
                    event.sequence,
                    skipped=True,
                    reason=event.data["reason"],
                )
        return records

    def status(self, transfer_id: str) -> TransferStatus:
        records = self._records(transfer_id)
        scanned = tuple(stage for stage in STAGES if stage in records and not records[stage].skipped)
        skipped = tuple(stage for stage in STAGES if stage in records and records[stage].skipped)
        recorded = set(records)
        pending = tuple(stage for stage in STAGES if stage not in recorded)
        return TransferStatus(
            transfer_id,
            scanned,
            skipped,
            pending,
            pending[0] if pending else None,
            TERMINAL_STAGE in scanned,
        )

    def material_manifest(self, transfer_id: str) -> tuple[str, ...]:
        """Materials recorded at ``loaded``; required by the departure gate."""
        loaded = self._records(transfer_id).get("loaded")
        if loaded is None or loaded.skipped:
            raise ValueError("transfer has not been loaded")
        return loaded.material_ids

    def record(self, transfer_id: str, stage: str) -> StageRecord | None:
        """Return the scan or skip recorded for ``stage``, if any."""
        return self._records(transfer_id).get(stage)

    # ----------------------------------------------------------------- scans

    def _assess_scan(
        self,
        transfer_id: str,
        stage: str,
        scan_id: str,
        at: int,
        material_ids: tuple[str, ...],
        units: int | None,
    ) -> tuple[tuple[str, ...], dict[str, Any], StageRecord | None]:
        if stage not in STAGES:
            raise ValueError("unknown transfer stage")
        if not isinstance(scan_id, str) or not scan_id.strip():
            raise ValueError("scan id is required")
        records = self._records(transfer_id)
        loaded = records.get("loaded")
        if stage == "loaded" and not material_ids:
            raise ValueError("the load scan requires a material manifest")

        existing = records.get(stage)
        if existing is not None:
            if existing.skipped:
                return (f"stage_skipped:{stage}",), {"stage": stage}, existing
            if not material_ids:
                return ("material_manifest_required",), {"stage": stage}, existing
            if material_ids != existing.material_ids:
                return (
                    "material_manifest_mismatch",
                ), {
                    "stage": stage,
                    "manifest": existing.material_ids,
                    "scanned": material_ids,
                }, existing
            if stage == TERMINAL_STAGE and existing.units is not None and (
                units is None or units != existing.units
            ):
                if units is None:
                    return ("unloaded_units_required",), {"stage": stage}, existing
                return (
                    "quantity_conservation_violated",
                ), {"loaded_units": existing.units, "unloaded_units": units}, existing
            if units is not None and existing.units is not None and units != existing.units:
                return (
                    "quantity_conservation_violated",
                ), {"loaded_units": existing.units, "unloaded_units": units}, existing
            evidence = {
                "stage": stage,
                "event_id": existing.event_id,
                "event_sequence": existing.sequence,
                "scanned_at": existing.occurred_at,
                "duplicate": True,
            }
            return (), evidence, existing

        index = STAGES.index(stage)
        if index > 0:
            predecessor = STAGES[index - 1]
            if predecessor not in records:
                reasons = (f"stage_out_of_order:{stage}",)
                evidence = {"stage": stage, "expected_stage": predecessor, "pending": self.status(transfer_id).pending}
                return reasons, evidence, None
            if loaded is None or loaded.skipped:
                return ("transfer_not_loaded",), {"stage": stage}, None
            if not material_ids:
                return ("material_manifest_required",), {"stage": stage}, None
            if material_ids != loaded.material_ids:
                return (
                    "material_manifest_mismatch",
                ), {
                    "stage": stage,
                    "manifest": loaded.material_ids,
                    "scanned": material_ids,
                }, None
            if stage == TERMINAL_STAGE and loaded.units is not None:
                if units is None:
                    return ("unloaded_units_required",), {"stage": stage}, None
                if units != loaded.units:
                    return (
                        "quantity_conservation_violated",
                    ), {"loaded_units": loaded.units, "unloaded_units": units}, None

        evidence: dict[str, Any] = {"stage": stage, "duplicate": False}
        return (), evidence, None

    def scan_decision(
        self,
        transfer_id: str,
        stage: str,
        scan_id: str,
        at: int,
        material_ids: tuple[str, ...] | list[str] | None = None,
        units: int | None = None,
    ) -> Decision:
        """Validate a scan without appending, preserving rejection reasons."""
        materials = _normalize_materials(material_ids)
        count = _normalize_units(units)
        reasons, evidence, _ = self._assess_scan(transfer_id, stage, scan_id, at, materials, count)
        return deny(*reasons, **evidence) if reasons else allow(**evidence)

    def scan(
        self,
        transfer_id: str,
        stage: str,
        scan_id: str,
        at: int,
        material_ids: tuple[str, ...] | list[str] | None = None,
        units: int | None = None,
    ) -> StageRecord:
        materials = _normalize_materials(material_ids)
        count = _normalize_units(units)
        reasons, _, existing = self._assess_scan(transfer_id, stage, scan_id, at, materials, count)
        if reasons:
            raise ValueError(f"transfer scan rejected: {', '.join(reasons)}")
        if existing is not None:
            return existing
        stream = _stream(transfer_id)
        expected = len(self._log.read(stream))
        event = self._log.append(
            stream,
            expected,
            scan_id,
            at,
            _SCAN_KIND,
            {"stage": stage, "materials": list(materials), "units": count},
        )
        return StageRecord(stage, event.event_id, event.occurred_at, event.sequence, materials, count)

    # ----------------------------------------------------------------- skips

    def skip_stage(
        self,
        transfer_id: str,
        stage: str,
        request_id: str,
        at: int,
        *,
        reason: str,
    ) -> StageRecord:
        """Record that ``stage`` was jumped over, with a mandatory reason."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("a skipped stage requires a non-empty reason")
        if stage not in STAGES:
            raise ValueError("unknown transfer stage")
        if stage == STAGES[0]:
            raise ValueError("first_stage_cannot_be_skipped")
        if stage == TERMINAL_STAGE:
            raise ValueError("terminal_stage_cannot_be_skipped")
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("skip request id is required")

        records = self._records(transfer_id)
        existing = records.get(stage)
        if existing is not None:
            if existing.skipped and existing.event_id == request_id:
                return existing
            code = "stage_already_scanned" if not existing.skipped else "stage_already_skipped"
            raise ValueError(f"{code}:{stage}")
        index = STAGES.index(stage)
        if index > 0 and STAGES[index - 1] not in records:
            raise ValueError(f"stage_out_of_order:{stage}")

        stream = _stream(transfer_id)
        expected = len(self._log.read(stream))
        event = self._log.append(stream, expected, request_id, at, _SKIP_KIND, {"stage": stage, "reason": reason.strip()})
        return StageRecord(stage, event.event_id, event.occurred_at, event.sequence, skipped=True, reason=reason.strip())

    # ------------------------------------------------------------ genealogy

    def link_genealogy(
        self,
        transfer_id: str,
        received_id: str,
        request_id: str,
        at: int,
        parent_of: Mapping[str, str | None],
    ) -> GenealogyLink:
        """Link the destination material to its origin after ``unloaded``.

        The origin must already exist in the supplied genealogy so the
        received material connects to the original lineage.  A multi-material
        manifest must be merged into one identifier first.
        """
        if not isinstance(received_id, str) or not received_id.strip():
            raise ValueError("received material id is required")
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("link request id is required")

        stream = _stream(transfer_id)
        linked = [event for event in self._log.read(stream) if event.kind == _LINK_KIND]
        if linked:
            event = linked[0]
            if event.event_id != request_id or event.data["received"] != received_id:
                raise ValueError("genealogy link already recorded for transfer")
            origin_id = event.data["origins"][0]
            new_map = {**dict(parent_of), received_id: origin_id}
            graph = GenealogyGraph(new_map)
            return GenealogyLink(
                transfer_id,
                origin_id,
                received_id,
                event.data.get("units"),
                tuple(ancestry(new_map, received_id)),
                graph,
                event.sequence,
                replay=True,
            )

        status = self.status(transfer_id)
        if not status.transferred:
            raise ValueError("transfer is not complete; genealogy cannot be linked")
        records = self._records(transfer_id)
        origins = records["loaded"].material_ids
        if not origins:
            raise ValueError("loaded manifest is empty")
        if len(origins) > 1:
            raise ValueError("multiple origin materials require a merge before genealogy link")
        origin_id = origins[0]
        if origin_id not in parent_of:
            raise ValueError(f"origin material missing from genealogy: {origin_id}")
        if received_id in parent_of and parent_of[received_id] is not None:
            raise ValueError("received material already linked to an origin")

        new_map = {**dict(parent_of), received_id: origin_id}
        graph = GenealogyGraph(new_map)  # rejects cycles
        ancestry_ids = tuple(ancestry(new_map, received_id))
        if origin_id not in ancestry_ids:  # fail-closed invariant
            raise ValueError("received material does not reach its origin")

        units = records["loaded"].units
        expected = len(self._log.read(stream))
        event = self._log.append(
            stream,
            expected,
            request_id,
            at,
            _LINK_KIND,
            {"origins": list(origins), "received": received_id, "units": units},
        )
        return GenealogyLink(transfer_id, origin_id, received_id, units, ancestry_ids, graph, event.sequence, replay=False)
