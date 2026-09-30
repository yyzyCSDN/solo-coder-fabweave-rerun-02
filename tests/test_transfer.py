from __future__ import annotations

import pytest

from fabweave.core.events import EventLog
from fabweave.holds.release import HoldState
from fabweave.transfer.chain import STAGES, TransferChain
from fabweave.workflows.control import record_transfer_scan

MATERIALS = ("MAT-1",)


def _loaded_chain(transfer_id: str = "TR-1", units: int = 25) -> tuple[TransferChain, EventLog]:
    log = EventLog()
    chain = TransferChain(log)
    chain.scan(transfer_id, "loaded", "SCAN-LOAD", 100, MATERIALS, units)
    return chain, log


def test_scan_chain_advances_only_in_canonical_order():
    chain, log = _loaded_chain()
    decision = chain.scan_decision("TR-1", "arrived", "SCAN-ARR", 130, MATERIALS)
    assert not decision.allowed
    assert decision.reasons == ("stage_out_of_order:arrived",)
    assert decision.evidence["expected_stage"] == "departed"
    assert len(log.read("transfer:TR-1")) == 1  # rejected decision books nothing

    chain.scan("TR-1", "departed", "SCAN-DEPART", 110, MATERIALS)
    chain.scan("TR-1", "arrived", "SCAN-ARR", 120, MATERIALS)
    chain.scan("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS, 25)

    status = chain.status("TR-1")
    assert status.scanned == STAGES and status.pending == () and status.current_stage is None
    assert status.transferred
    assert [event.data["stage"] for event in log.read("transfer:TR-1")] == list(STAGES)


def test_repeated_scan_replays_without_booking_a_second_time():
    chain, log = _loaded_chain()
    first = chain.scan("TR-1", "departed", "SCAN-DEPART", 110, MATERIALS)

    # Same reader rescanning (even with a fresh scan id) replays the original
    # scan: identical record, no extra event.
    replay = chain.scan("TR-1", "departed", "SCAN-DEPART-DUP", 111, MATERIALS)
    assert replay == first
    assert len(log.read("transfer:TR-1")) == 2

    decision = chain.scan_decision("TR-1", "departed", "SCAN-DEPART-DUP2", 112, MATERIALS)
    assert decision.allowed and decision.evidence["duplicate"] is True
    assert decision.evidence["event_id"] == "SCAN-DEPART"


def test_repeated_scan_with_changed_content_is_rejected_not_rebooked():
    chain, _ = _loaded_chain()
    chain.scan("TR-1", "departed", "SCAN-DEPART", 110, MATERIALS)

    swapped = chain.scan_decision("TR-1", "departed", "SCAN-DEPART-DUP", 111, ("MAT-2",))
    assert swapped.reasons == ("material_manifest_mismatch",)
    assert swapped.evidence["manifest"] == MATERIALS

    chain.scan("TR-1", "arrived", "SCAN-ARR", 120, MATERIALS)
    shortage = chain.scan_decision("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS, 24)
    assert shortage.reasons == ("quantity_conservation_violated",)
    assert shortage.evidence == {"loaded_units": 25, "unloaded_units": 24}


def test_manifest_must_match_at_every_gate_and_unload_keeps_quantity():
    chain, _ = _loaded_chain()
    chain.scan("TR-1", "departed", "SCAN-DEPART", 110, MATERIALS)
    decision = chain.scan_decision("TR-1", "arrived", "SCAN-ARR", 120, ("MAT-2",))
    assert decision.reasons == ("material_manifest_mismatch",)
    with pytest.raises(ValueError, match="material_manifest_mismatch"):
        chain.scan("TR-1", "arrived", "SCAN-ARR", 120, ("MAT-2",))

    chain.scan("TR-1", "arrived", "SCAN-ARR", 120, MATERIALS)
    missing_units = chain.scan_decision("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS)
    assert missing_units.reasons == ("unloaded_units_required",)

    with pytest.raises(ValueError, match="quantity_conservation"):
        chain.scan("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS, 24)
    chain.scan("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS, 25)
    assert chain.status("TR-1").transferred


def test_gate_without_manifest_is_fail_closed():
    chain, _ = _loaded_chain()
    decision = chain.scan_decision("TR-1", "departed", "SCAN-DEPART", 110)
    assert decision.reasons == ("material_manifest_required",)
    with pytest.raises(ValueError):
        TransferChain(EventLog()).scan_decision("TR-1", "loaded", "SCAN", 1, ())



def test_skipping_a_stage_requires_a_reason_and_then_chain_resumes():
    log = EventLog()
    chain = TransferChain(log)
    chain.scan("TR-1", "loaded", "SCAN-LOAD", 100, MATERIALS, 25)

    with pytest.raises(ValueError):
        chain.skip_stage("TR-1", "departed", "SKIP-1", 105, reason="  ")
    with pytest.raises(ValueError):
        chain.skip_stage("TR-1", "loaded", "SKIP-0", 105, reason="not anchored")
    with pytest.raises(ValueError):
        chain.skip_stage("TR-1", "unloaded", "SKIP-9", 140, reason="terminal")

    skipped = chain.skip_stage("TR-1", "departed", "SKIP-1", 105, reason="gate-reader offline")
    assert skipped.skipped and skipped.reason == "gate-reader offline"
    assert chain.skip_stage("TR-1", "departed", "SKIP-1", 105, reason="gate-reader offline") == skipped

    # A scanned stage cannot be retroactively skipped.
    with pytest.raises(ValueError, match="stage_already_skipped"):
        chain.skip_stage("TR-1", "departed", "SKIP-2", 106, reason="again")

    chain.scan("TR-1", "arrived", "SCAN-ARR", 120, MATERIALS)
    chain.scan("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS, 25)

    status = chain.status("TR-1")
    assert status.skipped == ("departed",)
    assert status.scanned == ("loaded", "arrived", "unloaded")
    assert status.transferred and status.completed == STAGES


def test_active_hold_blocks_departure_and_replay_does_not_rebook():
    chain, log = _loaded_chain()
    holds = {"MAT-1": HoldState("H-1", 1, frozenset({"QA"}))}

    blocked = record_transfer_scan(
        chain, "TR-1", "departed", "SCAN-DEPART", 110, MATERIALS,
        holds=holds, expected_hold_versions={"MAT-1": 1},
    )
    assert not blocked.allowed
    assert "departure_hold_active:MAT-1" in blocked.reasons
    assert "MAT-1:missing_disposition:QA" in blocked.reasons
    assert blocked.evidence["held_materials"] == ("MAT-1",)
    assert len(log.read("transfer:TR-1")) == 1  # load only; exit was refused

    stale = record_transfer_scan(
        chain, "TR-1", "departed", "SCAN-DEPART", 110, MATERIALS,
        holds=holds, expected_hold_versions={"MAT-1": 0},
    )
    assert "MAT-1:stale_hold_version" in stale.reasons

    # QA disposition completes: the same scan now passes the gate.
    released_holds = {"MAT-1": HoldState("H-1", 1, frozenset({"QA"}), frozenset({"QA"}))}
    allowed = record_transfer_scan(
        chain, "TR-1", "departed", "SCAN-DEPART", 110, MATERIALS,
        holds=released_holds, expected_hold_versions={"MAT-1": 1},
    )
    assert allowed.allowed and allowed.evidence["event_sequence"] == 2

    # Even if a fresh hold lands afterwards, replaying the exit does not
    # re-evaluate the gate and does not book a second departure.
    replay = record_transfer_scan(
        chain, "TR-1", "departed", "SCAN-DEPART-DUP", 115, MATERIALS,
        holds=holds, expected_hold_versions={"MAT-1": 1},
    )
    assert replay.allowed and replay.evidence["duplicate"] is True
    assert replay.evidence["event_id"] == "SCAN-DEPART"
    assert len(log.read("transfer:TR-1")) == 2


def test_genealogy_reconnects_to_origin_lineage_only_after_completion():
    chain, log = _loaded_chain()
    chain.scan("TR-1", "departed", "SCAN-DEPART", 110, MATERIALS)
    chain.scan("TR-1", "arrived", "SCAN-ARR", 120, MATERIALS)

    parent_of = {"RAW-1": None, "MAT-1": "RAW-1"}
    with pytest.raises(ValueError, match="not complete"):
        chain.link_genealogy("TR-1", "MAT-2", "LINK-1", 125, parent_of)

    chain.scan("TR-1", "unloaded", "SCAN-UNLOAD", 130, MATERIALS, 25)
    link = chain.link_genealogy("TR-1", "MAT-2", "LINK-1", 140, parent_of)
    assert link.origin_id == "MAT-1" and link.units == 25
    assert link.ancestry_ids == ("MAT-1", "RAW-1")
    # The received material is reachable from the original lineage.
    assert "MAT-2" in link.graph.related("RAW-1", "downstream", 5)

    replay = chain.link_genealogy("TR-1", "MAT-2", "LINK-1", 141, parent_of)
    assert replay.replay and replay.event_sequence == link.event_sequence
    assert len(log.read("transfer:TR-1")) == 5  # 4 scans + 1 link

    with pytest.raises(ValueError, match="already linked"):
        chain.link_genealogy("TR-1", "MAT-3", "LINK-2", 142, {**parent_of, "MAT-2": "MAT-1"})


def test_multi_material_manifest_must_merge_before_genealogy_link():
    log = EventLog()
    chain = TransferChain(log)
    materials = ("MAT-1", "MAT-2")
    chain.scan("TR-1", "loaded", "SCAN-LOAD", 100, materials, 25)
    chain.scan("TR-1", "departed", "SCAN-DEPART", 110, materials)
    chain.scan("TR-1", "arrived", "SCAN-ARR", 120, materials)
    chain.scan("TR-1", "unloaded", "SCAN-UNLOAD", 130, materials, 25)
    with pytest.raises(ValueError, match="merge"):
        chain.link_genealogy("TR-1", "MAT-9", "LINK-1", 140, {"MAT-1": None, "MAT-2": None})


def test_scan_validates_inputs_and_event_time():
    chain, _ = _loaded_chain()
    with pytest.raises(ValueError):
        TransferChain(EventLog()).scan("TR-1", "loaded", "SCAN", 1, (), 1)
    with pytest.raises(ValueError):
        chain.scan("TR-1", "departed", "SCAN", 110, MATERIALS, -1)
    with pytest.raises(ValueError):
        chain.scan("TR-1", "shipped", "SCAN", 110, MATERIALS)
    with pytest.raises(ValueError):
        chain.scan("TR-1", "departed", "SCAN-BACK", 99, MATERIALS)
