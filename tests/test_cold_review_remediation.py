"""Failing reproduction tests for MemPalace L3 Cold Review findings (ORD-0127).

These tests establish baseline deterministic failure (RED) before surgical minimal fixes.
"""

import os
import json
import time
import pytest
import filelock
from mempalace.core import MemoryPalace, MemPalaceError, HashIntegrityError
from mempalace.supervisor import ProcessSupervisor
from mempalace.models import ContextLevel, TaskStatus


def test_snapshot_state_tamper_detection(tmp_path):
    """[P0-1 Repro] Snapshot state payload tampering must be detected by verify_chain_integrity."""
    store_dir = str(tmp_path / "store")
    p = MemoryPalace(store_dir=store_dir, snapshot_interval=2)
    p.push_decision("D1", "BUILD", "R1")
    p.push_decision("D2", "BUILD", "R2")  # Snapshot seq 2 created
    
    # 1. Tamper with in-memory state: verify_chain_integrity must detect divergence
    p.state.active_stack[0].topic = "POISONED_IN_MEMORY"
    with pytest.raises(HashIntegrityError, match="diverged|differs"):
        p.verify_chain_integrity()
    
    # 2. Tamper with snapshot state on disk: opening palace must fail-closed on tampered snapshot
    snap_files = [f for f in os.listdir(p.snapshots_dir) if f.startswith("snapshot_seq_")]
    assert len(snap_files) == 1
    snap_path = os.path.join(p.snapshots_dir, snap_files[0])
    with open(snap_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["state"]["active_stack"][0]["topic"] = "POISONED_ON_DISK"
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
        
    with pytest.raises(HashIntegrityError, match="Snapshot tampering detected"):
        MemoryPalace(store_dir=store_dir, snapshot_interval=2)


def test_partial_write_tail_truncation_recovery(tmp_path):
    """[P0-2 Repro] Incomplete EOF line in event log must be recoverable via repair protocol."""
    store_dir = str(tmp_path / "store")
    p = MemoryPalace(store_dir=store_dir)
    p.push_decision("D1", "BUILD", "R1")
    p.push_decision("D2", "BUILD", "R2")
    
    # Simulate process crash leaving incomplete JSON at EOF
    with open(p.events_file, "a", encoding="utf-8") as f:
        f.write('{"seq_id": 3, "event_id": "evt_crash", "event_type": "DEC')
        
    # MUST SUCCEED with repair_truncated_tail=True instead of permanently crashing
    p2 = MemoryPalace(store_dir=store_dir, repair_truncated_tail=True)
    assert len(p2.events) == 2
    assert p2.state.current_seq == 2


def test_supervisor_rejects_resuscitation_from_stalled(tmp_path):
    """[P1-3 Repro] Supervisor must reject record_heartbeat attempting to revive stalled/terminal task."""
    store_dir = str(tmp_path / "store")
    sup = ProcessSupervisor(store_dir=store_dir)
    sup.register_task("worker-1", "researcher", timeout_sec=0.1)
    time.sleep(0.15)
    
    health = sup.check_health()
    assert "worker-1" in [t["task_id"] for t in health["stalled_tasks"]]
    
    # 1. Stalled task with allow_stalled_revive=False must raise ValueError
    with pytest.raises(ValueError, match="Cannot revive stalled task"):
        sup.record_heartbeat("worker-1", log_snippet="late revive", status=TaskStatus.RUNNING, allow_stalled_revive=False)
        
    # 2. Terminal task (COMPLETED / FAILED) must NEVER be revivable to RUNNING
    sup.register_task("worker-done", "coder")
    sup.record_heartbeat("worker-done", status=TaskStatus.COMPLETED)
    with pytest.raises(ValueError, match="Cannot revive task 'worker-done' from terminal status"):
        sup.record_heartbeat("worker-done", status=TaskStatus.RUNNING)
        
    # 3. Epoch regression must be rejected
    sup.register_task("worker-epoch", "tester")
    sup.record_heartbeat("worker-epoch", progress_epoch=5)
    with pytest.raises(ValueError, match="Progress epoch regression"):
        sup.record_heartbeat("worker-epoch", progress_epoch=3)


def test_l1_context_token_ceiling_on_dead_ends(tmp_path):
    """[P1-4 Repro] L1 context package must bound dead_ends_pool to prevent token explosion."""
    store_dir = str(tmp_path / "store")
    p = MemoryPalace(store_dir=store_dir)
    
    # Append 100 dead ends
    for i in range(100):
        p.state.dead_ends_pool.append({
            "path": f"bad/path/{i}",
            "reason": f"Failure reason with detail and stack trace info {i}" * 5
        })
        
    ctx_l1 = p.get_tiered_context(level=ContextLevel.L1, max_dead_ends=5)
    assert len(ctx_l1["dead_ends_pool"]) <= 5
    payload_str = json.dumps(ctx_l1)
    assert len(payload_str) < 3000, f"L1 payload too large: {len(payload_str)} characters"


def test_lock_timeout_wrapped_in_mempalace_error(tmp_path):
    """[P1-5 Repro] FileLock timeout must be wrapped as LockTimeoutError (subclass of MemPalaceError)."""
    import mempalace.core as core_mod
    assert hasattr(core_mod, "LockTimeoutError"), "LockTimeoutError missing from mempalace.core"
    LockTimeoutError = getattr(core_mod, "LockTimeoutError")
    
    store_dir = str(tmp_path / "store")
    p = MemoryPalace(store_dir=store_dir)
    
    # Hold lock externally
    ext_lock = filelock.FileLock(p.lock_file)
    ext_lock.acquire()
    p._lock.timeout = 0.1
    
    try:
        with pytest.raises(LockTimeoutError) as exc_info:
            p.push_decision("Should fail", "BUILD", "Testing lock timeout wrapping")
        # Must be catchable via MemPalaceError
        assert issubclass(LockTimeoutError, MemPalaceError)
        assert isinstance(exc_info.value, MemPalaceError)
    finally:
        ext_lock.release()


def test_lock_timeout_across_queries_and_supervisor(tmp_path):
    """[P1-LOCK-01 Repro] All lock acquisitions in MemoryPalace and ProcessSupervisor must wrap LockTimeoutError."""
    import mempalace.core as core_mod
    LockTimeoutError = getattr(core_mod, "LockTimeoutError")
    
    store_dir = str(tmp_path / "store")
    p = MemoryPalace(store_dir=store_dir)
    sup = ProcessSupervisor(store_dir=store_dir)
    
    # 1. MemoryPalace lock contention on queries
    ext_lock = filelock.FileLock(p.lock_file)
    ext_lock.acquire()
    p._lock.timeout = 0.05
    try:
        with pytest.raises(LockTimeoutError) as exc_info:
            p.get_tiered_context()
        assert isinstance(exc_info.value, MemPalaceError)
    finally:
        ext_lock.release()
        
    # 2. ProcessSupervisor lock contention
    ext_sup_lock = filelock.FileLock(sup.lock_file)
    ext_sup_lock.acquire()
    sup._lock.timeout = 0.05
    try:
        with pytest.raises(LockTimeoutError) as exc_info:
            sup.check_health()
        assert isinstance(exc_info.value, MemPalaceError)
    finally:
        ext_sup_lock.release()


def test_partial_write_tail_truncation_with_trailing_newlines(tmp_path):
    """[P1-WAL-02 Repro] Incomplete EOF line with trailing newlines must be cleanly repaired."""
    store_dir = str(tmp_path / "store")
    p = MemoryPalace(store_dir=store_dir)
    p.push_decision("D1", "BUILD", "R1")
    
    # Append corrupted JSON with trailing newlines
    with open(p.events_file, "a", encoding="utf-8") as f:
        f.write('{"seq_id": 2, "corrupted": true\n\n')
        
    p2 = MemoryPalace(store_dir=store_dir, repair_truncated_tail=True)
    assert len(p2.events) == 1
    assert p2.state.current_seq == 1
