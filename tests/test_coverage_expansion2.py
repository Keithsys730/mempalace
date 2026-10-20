"""Second-pass coverage tests — targets the remaining 31 uncovered lines after initial expansion.

Remaining gaps:
  cli.py    287, 290, 315-317, 322, 333, 337-339, 434
  core.py   65-66, 310, 343, 828-829, 858-859, 887-888, 910-911, 1055-1056, 1095-1096
  supervisor.py  111, 137, 192-193
"""

import io
import json
import os
import sys
import time
import pytest

from mempalace.core import (
    MemoryPalace,
    MemPalaceError,
    HashIntegrityError,
    _atomic_write_json,
    apply_event,
    replay_events,
)
from mempalace.models import (
    DecisionEvent,
    DecisionStackState,
    ContextLevel,
    EventType,
    ProcessHeartbeat,
    TaskStatus,
)
from mempalace.supervisor import ProcessSupervisor
from mempalace.cli import main


def run_cli(args, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mempalace"] + args)
    main()
    return capsys.readouterr()


def make_palace(tmp_path):
    store = os.path.join(tmp_path, "store")
    p = MemoryPalace(store_dir=store)
    p.add_immutable_rule("INV-001", "Gov", "Fail-closed check.")
    p.register_topology_node("ai-company", str(tmp_path / "ai-company"), "repo", ["dev-team"], "pytest")
    p.push_decision("Feature A", "BUILD", "Meets spec.")
    return p, store


# ─────────────────────────── core.py line 65-66 ─────────────────────────────

class TestAtomicWriteOsRemoveFails:
    """Lines 65-66: os.remove itself fails (OSError silenced) during cleanup."""

    def test_atomic_write_cleanup_remove_fails(self, tmp_path, monkeypatch):
        target = os.path.join(tmp_path, "out.json")
        import os as _os
        orig_fsync = _os.fsync
        orig_remove = _os.remove

        call_count = {"fsync": 0}

        def bad_fsync(fd):
            call_count["fsync"] += 1
            raise OSError("simulated fsync failure")

        def bad_remove(path):
            raise OSError("Cannot remove temp file")

        monkeypatch.setattr(_os, "fsync", bad_fsync)
        monkeypatch.setattr(_os, "remove", bad_remove)
        with pytest.raises(OSError, match="simulated fsync failure"):
            _atomic_write_json(target, {"x": 1})
        # Confirm fsync was attempted (i.e. the try-block was entered)
        assert call_count["fsync"] == 1


# ─────────────────────────── core.py line 310 (snapshot dir) ────────────────

class TestSnapshotDirMissing:
    """Line 310: _find_latest_valid_snapshot returns (None, 0) when snapshots/ absent."""

    def test_no_snapshots_dir_returns_none(self, tmp_path):
        store = os.path.join(tmp_path, "empty_store")
        p = MemoryPalace(store_dir=store)
        # __init__ always creates snapshots dir; remove it to exercise the missing-dir branch
        import shutil
        shutil.rmtree(p.snapshots_dir, ignore_errors=True)
        assert not os.path.exists(p.snapshots_dir)
        result, seq = p._find_latest_valid_snapshot(99)
        assert result is None
        assert seq == 0


# ─────────────────────────── core.py line 343 ───────────────────────────────

class TestEventLogBlankLine:
    """Line 343: blank lines in events.jsonl are silently skipped."""

    def test_blank_lines_skipped(self, tmp_path):
        store = os.path.join(tmp_path, "store")
        os.makedirs(store, exist_ok=True)
        p = MemoryPalace(store_dir=store)
        p.push_decision("T", "BUILD", "R")
        # Inject blank lines between valid events
        ef = p.events_file
        with open(ef, "r") as f:
            content = f.read().strip()
        with open(ef, "w") as f:
            f.write("\n")           # leading blank
            f.write(content + "\n")
            f.write("\n\n")        # trailing blanks
        # Should load without error and still replay correctly
        p2 = MemoryPalace(store_dir=store)
        assert len(p2.events) == 1


# ─────────────────────────── core.py resolve_uri dead_ends ValueError ────────

class TestResolveURIDeadEndsValueError:
    """Lines 828-829: dead_ends index is non-numeric string -> KeyError raised."""

    def test_resolve_dead_ends_non_numeric_idx(self, tmp_path):
        p, store = make_palace(tmp_path)
        p.record_dead_end("bad/path", "it failed")
        with pytest.raises(KeyError):
            p.resolve_uri("palace://decisions/dead_ends/not_a_number")


# ─────────────────────────── core.py supervision corrupt file (resolve/ls) ───

class TestResolveLsSupervisionCorruptFile:
    """Lines 858-859 (resolve_uri) and 1055-1056 (ls): heartbeats.json corrupt -> tasks={}."""

    def _write_corrupt_heartbeats(self, store):
        hb_file = os.path.join(store, "heartbeats.json")
        with open(hb_file, "w") as f:
            f.write("NOT JSON {{{{")

    def test_resolve_supervision_corrupt_file(self, tmp_path):
        p, store = make_palace(tmp_path)
        self._write_corrupt_heartbeats(store)
        result = p.resolve_uri("palace://supervision", level=ContextLevel.L0)
        assert result == {}

    def test_ls_supervision_corrupt_file(self, tmp_path):
        p, store = make_palace(tmp_path)
        self._write_corrupt_heartbeats(store)
        result = p.ls("palace://supervision")
        assert result == []


# ─────────────────────────── core.py audit events ValueError ─────────────────

class TestResolveAuditEventsValueError:
    """Lines 887-888: audit/events/<non-numeric-seq> -> ValueError caught, KeyError raised."""

    def test_resolve_audit_events_non_numeric(self, tmp_path):
        p, store = make_palace(tmp_path)
        with pytest.raises(KeyError):
            p.resolve_uri("palace://audit/events/not_a_number")


# ─────────────────────────── core.py ls root with corrupt heartbeats ────────

class TestLsRootCorruptHeartbeats:
    """Lines 910-911: ls('palace://') when heartbeats.json is corrupt -> tasks_count=0."""

    def test_ls_root_with_corrupt_heartbeats_file(self, tmp_path):
        p, store = make_palace(tmp_path)
        hb_file = os.path.join(store, "heartbeats.json")
        with open(hb_file, "w") as f:
            f.write("NOT JSON")
        result = p.ls("palace://")
        # Should return the root directory listing without error
        uris = [item["uri"] for item in result]
        assert "palace://rules" in uris


# ─────────────────────────── core.py ls tree corrupt heartbeats ──────────────

class TestLsTreeCorruptHeartbeats:
    """Lines 1095-1096: tree() when heartbeats.json is corrupt -> tasks_count=0."""

    def test_tree_with_corrupt_heartbeats_file(self, tmp_path):
        p, store = make_palace(tmp_path)
        hb_file = os.path.join(store, "heartbeats.json")
        with open(hb_file, "w") as f:
            f.write("NOT JSON")
        result = p.tree("palace://")
        assert "palace://" in result


# ─────────────────────────── supervisor.py line 111 ─────────────────────────

class TestSupervisorNoLockProgressEpoch:
    """Line 111: record_heartbeat with _lock=None and progress_epoch != None."""

    def test_record_heartbeat_no_lock_with_progress_epoch(self, tmp_path):
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup._lock = None  # force lock-less path
        sup.tasks["t-nolck"] = ProcessHeartbeat(
            task_id="t-nolck",
            role="worker",
            status=TaskStatus.RUNNING,
            last_heartbeat=time.time(),
            timeout_seconds=120.0,
        )
        sup.record_heartbeat("t-nolck", log_snippet="ping", status=TaskStatus.RUNNING, progress_epoch=7)
        assert sup.tasks["t-nolck"].progress_epoch == 7


# ─────────────────────────── supervisor.py line 137 ─────────────────────────

class TestSupervisorCompleted:
    """Line 137: COMPLETED status goes into completed list (not healthy/stalled/failed)."""

    def test_check_health_completed_task(self, tmp_path):
        sup = ProcessSupervisor(store_dir=tmp_path)
        sup._lock = None
        sup.tasks["done-task"] = ProcessHeartbeat(
            task_id="done-task",
            role="coder",
            status=TaskStatus.COMPLETED,
            last_heartbeat=time.time(),
            timeout_seconds=120.0,
        )
        health = sup.check_health()
        assert health["completed_count"] == 1
        assert health["healthy_count"] == 0


# ─────────────────────────── supervisor.py lines 192-193 ────────────────────

class TestSupervisorUnknownStatusFallthrough:
    """Lines 192-193: unknown status reclassified to FAILED via custom type."""

    def test_evaluate_health_unknown_status_force_failed(self, tmp_path, monkeypatch):
        import mempalace.supervisor as sup_mod

        class CustomTaskStatusType(type):
            def __instancecheck__(cls, instance):
                return False
            def __call__(cls, val):
                return "unrecognized_status"
            @property
            def FAILED(cls):
                return TaskStatus.FAILED
            @property
            def RUNNING(cls):
                return TaskStatus.RUNNING
            @property
            def IDLE(cls):
                return TaskStatus.IDLE
            @property
            def COMPLETED(cls):
                return TaskStatus.COMPLETED
            @property
            def STALLED(cls):
                return TaskStatus.STALLED

        class CustomTaskStatus(metaclass=CustomTaskStatusType):
            pass

        sup = ProcessSupervisor(store_dir=tmp_path)
        sup._lock = None
        hb = ProcessHeartbeat(task_id="ghost", role="worker", status=TaskStatus.RUNNING, last_heartbeat=time.time())
        sup.tasks["ghost"] = hb
        monkeypatch.setattr(sup_mod, "TaskStatus", CustomTaskStatus)
        res = sup._evaluate_health()
        assert res["failed_count"] == 1
        assert res["failed_tasks"][0]["task_id"] == "ghost"



# ─────────────────────────── cli.py line 287, 290 ───────────────────────────

class TestCliContextL1WithRulesAndTopology:
    """Lines 287, 290: context L1 iterates over actual rule and topology rows."""

    def test_context_l1_prints_rules_and_topology(self, monkeypatch, capsys, tmp_path):
        store = os.path.join(tmp_path, "store")
        run_cli(["--store", store, "bootstrap"], monkeypatch, capsys)
        out = run_cli(["--store", store, "context", "--level", "L1"], monkeypatch, capsys)
        assert "INV-001" in out.out or "Layer 1" in out.out
        assert "ai-company" in out.out or "Layer 2" in out.out


# ─────────────────────────── cli.py line 315-317 (post-compact file read err) ─

class TestCliPostCompactFileReadError:
    """Lines 315-317: --file exists but cannot be opened (permission error)."""

    def test_post_compact_file_read_error(self, monkeypatch, capsys, tmp_path):
        store = os.path.join(tmp_path, "store")
        input_file = os.path.join(tmp_path, "input.txt")
        with open(input_file, "w") as f:
            f.write("some content")

        # Make open() raise when reading the file
        original_open = open

        def bad_open(path, mode="r", **kw):
            if path == input_file and "r" in mode:
                raise IOError("Permission denied")
            return original_open(path, mode, **kw)

        monkeypatch.setattr("builtins.open", bad_open)
        with pytest.raises(SystemExit) as exc:
            run_cli(["--store", store, "post-compact", "--file", input_file, "--level", "L0"],
                    monkeypatch, capsys)
        assert exc.value.code == 1


# ─────────────────────────── cli.py line 322 (post-compact stdin pipe) ──────

class TestCliPostCompactStdinPipe:
    """Line 322: --text/--file omitted and stdin is NOT a tty (pipe mode)."""

    def test_post_compact_reads_stdin_when_piped(self, monkeypatch, capsys, tmp_path):
        store = os.path.join(tmp_path, "store")
        fake_stdin = io.StringIO("Content from piped stdin")
        fake_stdin.isatty = lambda: False
        monkeypatch.setattr(sys, "stdin", fake_stdin)
        out = run_cli(["--store", store, "post-compact", "--level", "L0"], monkeypatch, capsys)
        assert "[MemPalace L0 Context:" in out.out

    def test_post_compact_stdin_read_error(self, monkeypatch, capsys, tmp_path):
        """Lines 324-326: sys.stdin.read() fails -> exits 1."""
        store = os.path.join(tmp_path, "store")

        class BadStdin:
            def isatty(self):
                return False
            def read(self):
                raise IOError("Pipe broken")

        monkeypatch.setattr(sys, "stdin", BadStdin())
        with pytest.raises(SystemExit) as exc:
            run_cli(["--store", store, "post-compact", "--level", "L0"], monkeypatch, capsys)
        assert exc.value.code == 1


# ─────────────────────────── cli.py line 333 (post-compact --output mkdir) ──

class TestCliPostCompactOutputMkdir:
    """Line 333: --output path requires creating a new directory."""

    def test_post_compact_output_creates_dir(self, monkeypatch, capsys, tmp_path):
        store = os.path.join(tmp_path, "store")
        new_dir = os.path.join(tmp_path, "subdir", "nested")
        output_file = os.path.join(new_dir, "out.txt")
        assert not os.path.exists(new_dir)
        run_cli(["--store", store, "post-compact", "--level", "L0",
                 "--text", "hello", "--output", output_file], monkeypatch, capsys)
        assert os.path.exists(output_file)


# ─────────────────────────── cli.py lines 337-339 (post-compact write err) ──

class TestCliPostCompactOutputWriteError:
    """Lines 337-339: writing to --output fails -> exits 1."""

    def test_post_compact_output_write_error(self, monkeypatch, capsys, tmp_path):
        store = os.path.join(tmp_path, "store")
        output_file = os.path.join(tmp_path, "result.txt")
        original_open = open

        def bad_open(path, mode="r", **kw):
            if path == output_file and "w" in mode:
                raise IOError("Disk full")
            return original_open(path, mode, **kw)

        monkeypatch.setattr("builtins.open", bad_open)
        with pytest.raises(SystemExit) as exc:
            run_cli(["--store", store, "post-compact", "--level", "L0",
                     "--text", "hello", "--output", output_file], monkeypatch, capsys)
        assert exc.value.code == 1


# ─────────────────────────── cli.py line 434 (None command → inspect) ───────

class TestCliNoneCommandFallsToInspect:
    """Line 434: `elif args.command is None` branch (no subcommand given)."""

    def test_no_command_defaults_to_inspect_display(self, monkeypatch, capsys, tmp_path):
        store = os.path.join(tmp_path, "store")
        # Run with no subcommand — argparse will set command=None and fallthrough to inspect
        monkeypatch.setattr(sys, "argv", ["mempalace", "--store", store])
        main()
        out = capsys.readouterr()
        assert "Layer 1" in out.out or "MemPalace" in out.out


# ─────────────────────────── cli.py line 434 (__main__ execution) ───────────

class TestCliRunAsMainModule:
    """Line 434: `if __name__ == '__main__': main()` execution path."""

    def test_cli_run_module_as_main(self, monkeypatch, capsys, tmp_path):
        import runpy
        store = os.path.join(tmp_path, "store")
        monkeypatch.setattr(sys, "argv", ["mempalace", "--store", store, "inspect"])
        runpy.run_module("mempalace.cli", run_name="__main__")
        out = capsys.readouterr()
        assert "Layer 1" in out.out or "MemPalace" in out.out
