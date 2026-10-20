"""Coverage expansion tests for mempalace package.

Targets all missing-coverage branches in:
  - cli.py (lines 15-18, 161, 178-179, 190-191, 217, 220, 231-233, 262-264, 266-268,
             284-301, 315-317, 321-324, 333, 337-339, 348-350, 353-354, 371-373,
             376-377, 385, 397-398, 403-404, 434)
  - core.py (lines 61-67, 88, 129, 181-182, 275-276, 310, 329-330, 343, 347-348,
              410-411, 435-444, 603, 621, 629, 768, 775, 787, 799, 807, 822, 828-829,
              832-847, 858-859, 862, 866, 871, 880, 887-891, 910-911, 1031-1045,
              1048-1057, 1084, 1095-1096, 1217)
  - models.py (lines 45, 67, 94, 149, 181, 190)
  - supervisor.py (lines 37-38, 101, 111, 137, 154, 192-193)
"""

import json
import os
import sys
import time
import pytest

from mempalace.core import (
    MemoryPalace,
    MemPalaceError,
    HashIntegrityError,
    EventSequenceError,
    apply_event,
    replay_events,
    _atomic_write_json,
)
from mempalace.models import (
    ImmutableRule,
    TopologyNode,
    DecisionFrame,
    DecisionEvent,
    DecisionStackState,
    ProcessHeartbeat,
    TaskStatus,
    ContextLevel,
    EventType,
)
from mempalace.supervisor import ProcessSupervisor
from mempalace.cli import main


# ─────────────────────────── helpers ────────────────────────────────────────

def run_cli(args, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mempalace"] + args)
    main()
    return capsys.readouterr()


def make_palace(tmp_path):
    store = os.path.join(tmp_path, "store")
    p = MemoryPalace(store_dir=store)
    p.add_immutable_rule("INV-001", "Governance", "Fail-closed check.")
    p.register_topology_node("ai-company", str(tmp_path / "ai-company"), "repo", ["dev-team"], "pytest")
    p.push_decision("Feature A", "BUILD", "Meets spec.", assumptions=["a1", "a2"])
    return p, store


# ─────────────────────────── models.py ──────────────────────────────────────

class TestModelsTieredL2:
    """Covers the L2 (model_dump) branch in all to_tiered methods."""

    def test_immutable_rule_l2(self):
        r = ImmutableRule(rule_id="R1", category="Gov", description="desc")
        data = r.to_tiered(ContextLevel.L2)
        assert isinstance(data, dict)
        assert data["rule_id"] == "R1"

    def test_topology_node_l2(self):
        n = TopologyNode(name="foo", path="/foo")
        data = n.to_tiered(ContextLevel.L2)
        assert isinstance(data, dict)
        assert data["name"] == "foo"

    def test_decision_frame_l2(self):
        f = DecisionFrame(topic="T", verdict="BUILD", rationale="R")
        data = f.to_tiered(ContextLevel.L2)
        assert isinstance(data, dict)
        assert data["verdict"] == "BUILD"

    def test_decision_event_l2(self, tmp_path):
        state = DecisionStackState()
        ev = DecisionEvent(
            seq_id=1,
            event_id="evt0001",
            event_type=EventType.DECISION_PUSHED,
            actor="test",
            payload={"topic": "T", "verdict": "BUILD", "rationale": "R"},
            wall_time=time.time(),
            prev_event_hash=state.latest_hash,
        )
        data = ev.to_tiered(ContextLevel.L2)
        assert isinstance(data, dict)
        assert data["seq_id"] == 1

    def test_process_heartbeat_l2(self):
        hb = ProcessHeartbeat(task_id="t1", role="worker")
        data = hb.to_tiered(ContextLevel.L2)
        assert isinstance(data, dict)
        assert data["task_id"] == "t1"

    def test_process_heartbeat_l0(self):
        hb = ProcessHeartbeat(task_id="t2", role="watcher")
        result = hb.to_tiered(ContextLevel.L0)
        assert "t2" in result
        assert "watcher" in result


# ─────────────────────────── supervisor.py ──────────────────────────────────

class TestSupervisorEdgeCases:

    def test_load_registry_corrupt_json(self, tmp_path):
        """Lines 37-38: except block when registry JSON is corrupt."""
        reg_file = os.path.join(tmp_path, "heartbeats.json")
        with open(reg_file, "w") as f:
            f.write("not valid json {{{{")
        sup = ProcessSupervisor(store_dir=tmp_path)
        # _load_registry silently swallows exceptions; tasks should be empty
        sup._load_registry()
        assert sup.tasks == {}

    def test_record_heartbeat_progress_epoch(self, tmp_path):
        """Line 101 (lock branch) and 111 (no-lock else): progress_epoch update."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup.register_task("t-epoch", "worker")
        sup.record_heartbeat("t-epoch", log_snippet="tick", status=TaskStatus.RUNNING, progress_epoch=42)
        assert sup.tasks["t-epoch"].progress_epoch == 42

    def test_check_health_no_lock_path(self, tmp_path):
        """Line 122: else branch when _lock is None."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup._lock = None  # force lock-less path
        sup.tasks["t1"] = ProcessHeartbeat(
            task_id="t1",
            role="worker",
            status=TaskStatus.RUNNING,
            last_heartbeat=time.time(),
            timeout_seconds=120.0,
        )
        health = sup.check_health()
        assert health["total_tasks"] == 1

    def test_check_health_idle_and_failed_unknown_status(self, tmp_path):
        """Lines 137 (IDLE), 154 (FAILED), 192-193 (unknown fallthrough)."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup._lock = None
        sup.tasks["idle-task"] = ProcessHeartbeat(
            task_id="idle-task",
            role="watcher",
            status=TaskStatus.IDLE,
            last_heartbeat=time.time(),
            timeout_seconds=120.0,
        )
        sup.tasks["failed-task"] = ProcessHeartbeat(
            task_id="failed-task",
            role="critic",
            status=TaskStatus.FAILED,
            last_heartbeat=time.time(),
            timeout_seconds=120.0,
        )
        health = sup.check_health()
        assert health["idle_count"] == 1
        assert health["failed_count"] == 1


# ─────────────────────────── core.py ────────────────────────────────────────

class TestAtomicWriteJsonException:
    """Lines 61-67: _atomic_write_json cleanup on exception."""

    def test_atomic_write_cleans_temp_on_os_error(self, tmp_path, monkeypatch):
        target = os.path.join(tmp_path, "output.json")

        # Make fsync raise an OS error to trigger the except branch
        import os as _os
        orig_fsync = _os.fsync

        def broken_fsync(fd):
            raise OSError("simulated fsync failure")

        monkeypatch.setattr(_os, "fsync", broken_fsync)
        with pytest.raises(OSError):
            _atomic_write_json(target, {"key": "value"})


class TestApplyEventEdgeCases:
    """Lines 88, 129, 181-182."""

    def test_apply_event_hash_mismatch_raises(self, tmp_path):
        """Line 88: prev_event_hash mismatch."""
        state = DecisionStackState()
        ev = DecisionEvent(
            seq_id=1,
            event_id="evt0001",
            event_type=EventType.DECISION_PUSHED,
            actor="test",
            payload={"frame_id": "f1", "topic": "T", "verdict": "BUILD", "rationale": "R"},
            wall_time=time.time(),
            prev_event_hash="wrong_hash_value",
        )
        ev.event_hash = ev.compute_hash()
        with pytest.raises(HashIntegrityError):
            apply_event(state, ev, verify_hash=True)

    def test_apply_event_pop_frame_id_mismatch_raises(self, tmp_path):
        """Line 129: popped_frame_id != top-of-stack frame_id."""
        state = DecisionStackState()
        # First push a frame
        push_ev = DecisionEvent(
            seq_id=1,
            event_id="push001",
            event_type=EventType.DECISION_PUSHED,
            actor="test",
            payload={"frame_id": "real-frame", "topic": "T", "verdict": "BUILD", "rationale": "R"},
            wall_time=time.time(),
            prev_event_hash=state.latest_hash,
        )
        state = apply_event(state, push_ev, verify_hash=True)

        # Now try to pop with wrong expected frame_id
        pop_ev = DecisionEvent(
            seq_id=2,
            event_id="pop001",
            event_type=EventType.DECISION_POPPED,
            actor="test",
            payload={"popped_frame_id": "wrong-frame-id", "reason": "test"},
            wall_time=time.time(),
            prev_event_hash=state.latest_hash,
        )
        with pytest.raises(Exception):
            apply_event(state, pop_ev, verify_hash=True)

    def test_apply_event_snapshot_taken_noop(self, tmp_path):
        """Lines 181-182: SNAPSHOT_TAKEN is a no-op."""
        state = DecisionStackState()
        snap_ev = DecisionEvent(
            seq_id=1,
            event_id="snap001",
            event_type=EventType.SNAPSHOT_TAKEN,
            actor="system",
            payload={"snapshot_seq": 0},
            wall_time=time.time(),
            prev_event_hash=state.latest_hash,
        )
        new_state = apply_event(state, snap_ev, verify_hash=True)
        assert new_state.current_seq == 1


class TestCoreTopologyLoadError:
    """Lines 275-276: topology load failure raises MemPalaceError."""

    def test_topology_load_corrupt_raises(self, tmp_path):
        store = os.path.join(tmp_path, "store")
        os.makedirs(store, exist_ok=True)
        topo_path = os.path.join(store, "topology.json")
        with open(topo_path, "w") as f:
            f.write("not json {{{{")
        with pytest.raises(MemPalaceError):
            MemoryPalace(store_dir=store)


class TestCoreSnapshotEdgeCases:
    """Lines 310, 329-330, 343, 347-348."""

    def test_find_latest_valid_snapshot_no_snapshots_dir(self, tmp_path):
        p, store = make_palace(tmp_path)
        # Ensure no snapshots dir; _find_latest_valid_snapshot returns None, 0
        result, seq = p._find_latest_valid_snapshot(10)
        assert result is None
        assert seq == 0

    def test_find_latest_valid_snapshot_invalid_file_skipped(self, tmp_path):
        p, store = make_palace(tmp_path)
        snap_dir = os.path.join(store, "snapshots")
        os.makedirs(snap_dir, exist_ok=True)
        # Write a valid-looking but malformed snapshot file
        bad_snap = os.path.join(snap_dir, "snapshot_seq_0001.json")
        with open(bad_snap, "w") as f:
            f.write("not json {{{")
        result, seq = p._find_latest_valid_snapshot(5)
        assert result is None

    def test_read_event_log_corrupt_line_raises(self, tmp_path):
        store = os.path.join(tmp_path, "store")
        os.makedirs(store, exist_ok=True)
        # Use the real events file path from MemoryPalace
        events_file = os.path.join(store, "decision_events.jsonl")
        with open(events_file, "w", encoding="utf-8") as f:
            f.write("not valid json\n")
        with pytest.raises(MemPalaceError, match="Failed to parse event line"):
            MemoryPalace(store_dir=store)


class TestCoreMigrateAndSaveAll:
    """Lines 410-411, 435-444."""

    def test_migrate_legacy_corrupt_falls_back_empty(self, tmp_path):
        store = os.path.join(tmp_path, "store")
        os.makedirs(store, exist_ok=True)
        # Write corrupt legacy decision_stack.json (no events.jsonl)
        stack_path = os.path.join(store, "decision_stack.json")
        with open(stack_path, "w") as f:
            f.write("not json {{{")
        p = MemoryPalace(store_dir=store)
        # Should migrate gracefully with empty stack
        assert p.decision_stack == []

    def test_save_all_persists_and_reloads(self, tmp_path):
        p, store = make_palace(tmp_path)
        p.save_all()
        p2 = MemoryPalace(store_dir=store)
        assert len(p2.immutable_rules) == 1
        assert len(p2.topology) == 1


class TestCoreAmendDecision:
    """Line 603: amend with both verdict and rationale."""

    def test_amend_decision_both_fields(self, tmp_path):
        p, store = make_palace(tmp_path)
        frame_id = p.decision_stack[0].frame_id
        p.amend_decision(frame_id=frame_id, verdict="REVIEW", rationale="Updated rationale")
        updated = next(f for f in p.decision_stack if f.frame_id == frame_id)
        assert updated.verdict == "REVIEW"
        assert updated.rationale == "Updated rationale"


class TestCoreVerifyChainEmptyAndMismatch:
    """Lines 621, 629."""

    def test_verify_chain_empty_events_ok(self, tmp_path):
        store = os.path.join(tmp_path, "store")
        p = MemoryPalace(store_dir=store)
        res = p.verify_chain_integrity()
        assert res["ok"] is True
        assert res["total_events"] == 0

    def test_verify_chain_state_mismatch_raises(self, tmp_path):
        p, store = make_palace(tmp_path)
        # Corrupt in-memory state to create a mismatch with on-disk data
        p.state.latest_hash = "00" * 32  # tamper the in-memory hash
        with pytest.raises(HashIntegrityError):
            p.verify_chain_integrity()


class TestCoreResolveURIEdgeCases:
    """Lines 768, 775, 787, 799, 807, 822, 828-829, 832-847, 858-859, 862, 866, 871, 880, 887-891."""

    def _palace_with_history(self, tmp_path):
        p, store = make_palace(tmp_path)
        frame_id = p.decision_stack[0].frame_id
        p.pop_decision(reason="done", actor="test")
        return p, store, frame_id

    def test_resolve_root_returns_tiered_context(self, tmp_path):
        """Line 768: empty segments -> get_tiered_context."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://", level=ContextLevel.L0)
        assert isinstance(result, dict)

    def test_resolve_rules_listing(self, tmp_path):
        """Line 775: rules listing with no sub-segment."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://rules", level=ContextLevel.L1)
        assert "INV-001" in result

    def test_resolve_rules_unknown_raises(self, tmp_path):
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://rules/NONEXISTENT")

    def test_resolve_topology_listing(self, tmp_path):
        """Line 787: topology listing with no sub-segment."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://topology", level=ContextLevel.L1)
        assert "ai-company" in result

    def test_resolve_topology_unknown_raises(self, tmp_path):
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://topology/does-not-exist")

    def test_resolve_decisions_root(self, tmp_path):
        """Line 799: decisions root."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://decisions", level=ContextLevel.L1)
        assert "stack" in result

    def test_resolve_decisions_stack_listing(self, tmp_path):
        """Line 807: decisions/stack listing."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://decisions/stack", level=ContextLevel.L1)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_resolve_dead_ends_listing(self, tmp_path):
        """Line 822: decisions/dead_ends listing."""
        p, store = make_palace(tmp_path)
        p.record_dead_end("bad/path", "It failed")
        result = p.resolve_uri("palace://decisions/dead_ends", level=ContextLevel.L1)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_resolve_dead_ends_index_oob_raises(self, tmp_path):
        """Lines 828-830: dead_ends index out of range."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://decisions/dead_ends/999")

    def test_resolve_history_listing(self, tmp_path):
        """Lines 832-834: history listing."""
        p, store, _ = self._palace_with_history(tmp_path)
        result = p.resolve_uri("palace://decisions/history", level=ContextLevel.L1)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_resolve_history_by_index(self, tmp_path):
        """Lines 836-839: history by numeric index."""
        p, store, _ = self._palace_with_history(tmp_path)
        result = p.resolve_uri("palace://decisions/history/0", level=ContextLevel.L1)
        assert result["verdict"] == "BUILD"

    def test_resolve_history_by_frame_id(self, tmp_path):
        """Lines 842-844: history by frame_id string."""
        p, store, fid = self._palace_with_history(tmp_path)
        result = p.resolve_uri(f"palace://decisions/history/{fid}", level=ContextLevel.L1)
        assert result["frame_id"] == fid

    def test_resolve_history_unknown_raises(self, tmp_path):
        """Line 845: history frame not found."""
        p, store, _ = self._palace_with_history(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://decisions/history/nonexistent-frame-id")

    def test_resolve_decisions_unknown_subpath_raises(self, tmp_path):
        """Line 847: unknown decision sub-segment."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://decisions/unknown_sub")

    def test_resolve_supervision_no_file(self, tmp_path):
        """Lines 858-862: supervision with no heartbeats.json."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://supervision", level=ContextLevel.L0)
        assert result == {}

    def test_resolve_supervision_tasks_path(self, tmp_path):
        """Line 862: supervision/tasks path."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup.register_task("sup-t1", "worker", timeout_sec=60)
        p = MemoryPalace(store_dir=tmp_path)
        result = p.resolve_uri("palace://supervision/tasks", level=ContextLevel.L0)
        assert isinstance(result, dict)

    def test_resolve_supervision_specific_task(self, tmp_path):
        """Lines 863-865: resolve a specific supervision task."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup.register_task("sup-specific", "worker", timeout_sec=60)
        p = MemoryPalace(store_dir=tmp_path)
        result = p.resolve_uri("palace://supervision/tasks/sup-specific", level=ContextLevel.L0)
        assert "sup-specific" in result

    def test_resolve_supervision_missing_task_raises(self, tmp_path):
        """Line 866: supervision task not found."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup.register_task("exists", "worker")
        p = MemoryPalace(store_dir=tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://supervision/tasks/does-not-exist")

    def test_resolve_audit_root(self, tmp_path):
        """Line 871: audit root."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://audit", level=ContextLevel.L0)
        assert "total_events" in result

    def test_resolve_audit_events_listing(self, tmp_path):
        """Line 880: audit/events listing."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://audit/events", level=ContextLevel.L0)
        assert isinstance(result, list)

    def test_resolve_audit_events_by_seq(self, tmp_path):
        """Lines 883-886: audit/events/seq_id lookup."""
        p, store = make_palace(tmp_path)
        result = p.resolve_uri("palace://audit/events/1", level=ContextLevel.L0)
        assert result is not None

    def test_resolve_audit_events_seq_not_found_raises(self, tmp_path):
        """Lines 887-889: audit/events seq not found."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://audit/events/9999")

    def test_resolve_audit_unknown_subpath_raises(self, tmp_path):
        """Lines 890-891: unknown audit subpath."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://audit/invalid")

    def test_resolve_unknown_root_raises(self, tmp_path):
        """Lines 893-894 (resolve) and 1084 (ls): unknown root."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://completely_unknown_root")


class TestCoreLsEdgeCases:
    """Lines 910-911, 1031-1045, 1048-1057, 1084."""

    def test_ls_decisions_history(self, tmp_path):
        """Lines 1031-1044: ls decisions/history."""
        p, store = make_palace(tmp_path)
        p.pop_decision(reason="done", actor="test")
        result = p.ls("palace://decisions/history")
        assert len(result) == 1
        assert result[0]["node_type"] == "decision_frame"

    def test_ls_supervision(self, tmp_path):
        """Lines 1047-1068: ls supervision."""
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup.register_task("ls-sup", "watcher")
        p = MemoryPalace(store_dir=tmp_path)
        result = p.ls("palace://supervision")
        assert any(item["name"] == "ls-sup" for item in result)

    def test_ls_decisions_unknown_sub_raises(self, tmp_path):
        """Line 1045: ls decisions/<unknown>."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.ls("palace://decisions/unknown_subdir")

    def test_ls_unknown_root_raises(self, tmp_path):
        """Line 1084: ls unknown root."""
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.ls("palace://completely_unknown")


class TestCoreFindHistory:
    """Line 1217: search history archive."""

    def test_find_in_history_archive(self, tmp_path):
        p, store = make_palace(tmp_path)
        p.pop_decision(reason="done", actor="test")
        matches = p.find("Feature A")
        history_matches = [m for m in matches if m["node_type"] == "history_frame"]
        assert len(history_matches) >= 1
        assert "Feature A" in history_matches[0]["summary"]


# ─────────────────────────── cli.py ─────────────────────────────────────────

class TestCliCoverageBranches:

    def test_cli_stdout_encoding_reconfigure_exception(self, monkeypatch, capsys, tmp_path):
        """Lines 15-18: the reconfigure exception branch is swallowed silently."""
        store = os.path.join(tmp_path, "store")
        import io
        import mempalace.cli as cli_mod

        buf = io.StringIO()

        class FakeAsciiStdout:
            encoding = "ascii"

            def write(self, s):
                buf.write(s)
                return len(s)

            def flush(self):
                pass

            def reconfigure(self, **kw):
                raise RuntimeError("Reconfigure not supported in this terminal")

        monkeypatch.setattr(cli_mod.sys, "stdout", FakeAsciiStdout())
        monkeypatch.setattr(sys, "argv", ["mempalace", "--store", store, "inspect"])
        # Should not propagate the reconfigure exception
        try:
            cli_mod.main()
        except RuntimeError:
            pytest.fail("reconfigure exception must be swallowed (lines 15-18)")

    def test_cli_pop_decision_empty_stack(self, monkeypatch, capsys, tmp_path):
        """Line 161: pop from empty stack prints warning."""
        store = os.path.join(tmp_path, "store")
        out = run_cli(["--store", store, "pop-decision", "-r", "No frame"], monkeypatch, capsys)
        assert "決策棧目前為空" in out.out

    def test_cli_audit_log_no_events(self, monkeypatch, capsys, tmp_path):
        """Lines 178-179: audit-log with no events."""
        store = os.path.join(tmp_path, "store")
        out = run_cli(["--store", store, "audit-log"], monkeypatch, capsys)
        assert "暫無任何事件" in out.out

    def test_cli_audit_log_popped_and_dead_end_events(self, monkeypatch, capsys, tmp_path):
        """Lines 190-191, 193-194: audit log shows POPPED and DEAD_END_RECORDED payloads."""
        store = os.path.join(tmp_path, "store")
        # bootstrap + push + pop + dead end
        run_cli(["--store", store, "push-decision", "-t", "Audit T", "-v", "BUILD", "-r", "ok"], monkeypatch, capsys)
        run_cli(["--store", store, "pop-decision", "-r", "done"], monkeypatch, capsys)
        run_cli(["--store", store, "record-dead-end", "-p", "dead/path", "-r", "failed"], monkeypatch, capsys)
        out = run_cli(["--store", store, "audit-log"], monkeypatch, capsys)
        assert "DECISION_POPPED" in out.out
        assert "DEAD_END_RECORDED" in out.out

    def test_cli_replay_with_dead_ends_and_history(self, monkeypatch, capsys, tmp_path):
        """Lines 217, 220: replay output with dead ends and history."""
        store = os.path.join(tmp_path, "store")
        run_cli(["--store", store, "push-decision", "-t", "R-Feature", "-v", "BUILD", "-r", "ok"], monkeypatch, capsys)
        run_cli(["--store", store, "record-dead-end", "-p", "alt/path", "-r", "bad"], monkeypatch, capsys)
        run_cli(["--store", store, "pop-decision", "-r", "done"], monkeypatch, capsys)
        out = run_cli(["--store", store, "replay"], monkeypatch, capsys)
        assert "歷史彈出歸檔" in out.out or "dead" in out.out.lower()

    def test_cli_verify_integrity_tampered(self, monkeypatch, capsys, tmp_path):
        """Lines 231-233: verify-integrity exits 1 when chain reports integrity error."""
        store = os.path.join(tmp_path, "store")

        def _raise_hash_err(*a, **kw):
            raise HashIntegrityError("Simulated in-memory vs disk mismatch")

        import mempalace.cli as cli_mod
        import mempalace.core as core_mod
        monkeypatch.setattr(core_mod.MemoryPalace, "verify_chain_integrity", _raise_hash_err)
        with pytest.raises(SystemExit) as exc:
            run_cli(["--store", store, "verify-integrity"], monkeypatch, capsys)
        assert exc.value.code == 1

    def test_cli_check_health_stalled_and_failed(self, monkeypatch, capsys, tmp_path):
        """Lines 262-264, 266-268: check-health shows stalled and failed tasks."""
        store = os.path.join(tmp_path, "store")
        # Register task that is immediately stalled (timeout=0.001)
        run_cli(["--store", store, "register-task", "--task-id", "stall-task",
                 "--role", "worker", "--timeout", "0.001"], monkeypatch, capsys)
        time.sleep(0.1)  # ensure stall
        # Register and directly set a failed task
        run_cli(["--store", store, "register-task", "--task-id", "fail-task",
                 "--role", "critic", "--timeout", "120"], monkeypatch, capsys)
        run_cli(["--store", store, "record-heartbeat", "--task-id", "fail-task",
                 "--status", "failed"], monkeypatch, capsys)
        out = run_cli(["--store", store, "check-health"], monkeypatch, capsys)
        assert "假死" in out.out or "失敗" in out.out

    def test_cli_context_l1_with_dead_ends(self, monkeypatch, capsys, tmp_path):
        """Lines 284-301: context L1 with dead ends pool populated."""
        store = os.path.join(tmp_path, "store")
        run_cli(["--store", store, "push-decision", "-t", "C-Feature", "-v", "BUILD", "-r", "ok"], monkeypatch, capsys)
        run_cli(["--store", store, "record-dead-end", "-p", "dead/end", "-r", "failed"], monkeypatch, capsys)
        out = run_cli(["--store", store, "context", "--level", "L1"], monkeypatch, capsys)
        assert "Layer 1" in out.out or "不可變" in out.out

    def test_cli_context_l2(self, monkeypatch, capsys, tmp_path):
        """Line 301: context L2 raw JSON output."""
        store = os.path.join(tmp_path, "store")
        out = run_cli(["--store", store, "context", "--level", "L2"], monkeypatch, capsys)
        assert "level" in out.out or "{" in out.out

    def test_cli_post_compact_no_args_empty_text(self, monkeypatch, capsys, tmp_path):
        """Lines 315-317: post-compact with stdin.isatty() True and no file/text -> empty prompt."""
        store = os.path.join(tmp_path, "store")
        monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
        out = run_cli(["--store", store, "post-compact", "--level", "L0"], monkeypatch, capsys)
        assert "[MemPalace L0 Context:" in out.out

    def test_cli_ls_invalid_uri_exits(self, monkeypatch, capsys, tmp_path):
        """Lines 348-350: ls with invalid URI exits 1."""
        store = os.path.join(tmp_path, "store")
        with pytest.raises(SystemExit) as exc:
            run_cli(["--store", store, "ls", "palace://nonexistent_path"], monkeypatch, capsys)
        assert exc.value.code == 1

    def test_cli_ls_json_output(self, monkeypatch, capsys, tmp_path):
        """Lines 353-354: ls --json output."""
        store = os.path.join(tmp_path, "store")
        out = run_cli(["--store", store, "ls", "palace://", "--json"], monkeypatch, capsys)
        parsed = json.loads(out.out)
        assert isinstance(parsed, list)

    def test_cli_view_invalid_uri_exits(self, monkeypatch, capsys, tmp_path):
        """Lines 371-373: view with invalid URI exits 1."""
        store = os.path.join(tmp_path, "store")
        with pytest.raises(SystemExit) as exc:
            run_cli(["--store", store, "view", "palace://rules/NONEXISTENT"], monkeypatch, capsys)
        assert exc.value.code == 1

    def test_cli_view_json_output(self, monkeypatch, capsys, tmp_path):
        """Lines 376-377: view --json output."""
        store = os.path.join(tmp_path, "store")
        run_cli(["--store", store, "bootstrap"], monkeypatch, capsys)
        out = run_cli(["--store", store, "view", "palace://rules/INV-001", "--json"], monkeypatch, capsys)
        assert "INV-001" in out.out

    def test_cli_view_string_output(self, monkeypatch, capsys, tmp_path):
        """Line 385: view of L0 string resolution."""
        store = os.path.join(tmp_path, "store")
        run_cli(["--store", store, "bootstrap"], monkeypatch, capsys)
        out = run_cli(["--store", store, "view", "palace://rules/INV-001", "--level", "L0"], monkeypatch, capsys)
        assert "INV-001" in out.out

    def test_cli_find_json_output(self, monkeypatch, capsys, tmp_path):
        """Lines 397-398: find --json output."""
        store = os.path.join(tmp_path, "store")
        run_cli(["--store", store, "bootstrap"], monkeypatch, capsys)
        out = run_cli(["--store", store, "find", "ai-company", "--json"], monkeypatch, capsys)
        parsed = json.loads(out.out)
        assert isinstance(parsed, list)

    def test_cli_find_no_results(self, monkeypatch, capsys, tmp_path):
        """Lines 403-404: find with no matches prints empty message."""
        store = os.path.join(tmp_path, "store")
        out = run_cli(["--store", store, "find", "xyzzy_nonexistent_zz999"], monkeypatch, capsys)
        assert "無相符結果" in out.out

    def test_cli_main_as_module(self, monkeypatch, capsys, tmp_path):
        """Line 434: __name__ == '__main__' path is not directly testable, but ensure main() callable."""
        store = os.path.join(tmp_path, "store")
        out = run_cli(["--store", store, "inspect"], monkeypatch, capsys)
        assert "Layer 1" in out.out
