import pytest
import os
import json
import time
import math
import shutil
import concurrent.futures
from mempalace.core import (
    MemoryPalace,
    MemPalaceError,
    HashIntegrityError,
    EventSequenceError,
    EventSemanticError,
    RuleConflictError,
    apply_event,
    replay_events
)
from mempalace.models import (
    DecisionFrame,
    DecisionEvent,
    EventType,
    DecisionStackState,
    TaskStatus
)
from mempalace.supervisor import ProcessSupervisor


def test_mempalace_3_layers(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    # 1. Layer 1
    r = palace.add_immutable_rule("INV-001", "Safety", "Fail-closed check.")
    assert r.rule_id == "INV-001"
    assert len(palace.immutable_rules) == 1

    # 2. Layer 2
    n = palace.register_topology_node("knowledge-island", str(tmp_path / "knowledge-island"), "repo", ["daily"], "pytest")
    assert n.name == "knowledge-island"
    assert len(palace.topology) == 1

    # 3. Layer 3 (Stack + Event Sourcing)
    f1 = palace.push_decision("Spec Approval", "BUILD", "Meets acceptance criteria.")
    f2 = palace.push_decision("Security Audit", "REVIEW", "Clean review marker.")
    assert len(palace.decision_stack) == 2
    assert len(palace.events) == 2

    # Verify context package
    pkg = palace.get_active_context_package(max_decision_frames=2)
    assert len(pkg["immutable_rules"]) == 1
    assert len(pkg["topology"]) == 1
    assert len(pkg["active_decision_stack"]) == 2
    assert pkg["event_lineage"]["total_events"] == 2
    assert pkg["event_lineage"]["current_seq"] == 2

    # Pop stack
    popped = palace.pop_decision(reason="Audit completed")
    assert popped.frame_id == f2.frame_id
    assert len(palace.decision_stack) == 1
    assert len(palace.events) == 3

    # Reload from disk (reconstruct from JSONL event log)
    palace2 = MemoryPalace(store_dir=store_dir)
    assert len(palace2.immutable_rules) == 1
    assert len(palace2.topology) == 1
    assert len(palace2.decision_stack) == 1
    assert len(palace2.events) == 3

    # Verify integrity
    integrity = palace2.verify_chain_integrity()
    assert integrity["ok"] is True
    assert integrity["total_events"] == 3


def test_deterministic_replay_and_state_reconstruction(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    palace.push_decision("Architecture Decision 1", "BUILD", "Clean baseline")
    palace.push_decision("Optimization Branch", "EVALUATE", "Benchmark candidate")
    palace.pop_decision("Branch evaluated and discarded")
    palace.push_decision("Final Architecture", "BUILD", "Chosen approach")

    assert len(palace.events) == 4
    assert len(palace.decision_stack) == 2
    assert palace.decision_stack[0].topic == "Architecture Decision 1"
    assert palace.decision_stack[1].topic == "Final Architecture"

    # Replay all events using pure replay_events function
    replayed = replay_events(palace.events, verify_hash=True)
    assert replayed.current_seq == 4
    assert len(replayed.active_stack) == 2
    assert len(replayed.history_archive) == 1
    assert replayed.history_archive[0].topic == "Optimization Branch"
    assert replayed.latest_hash == palace.state.latest_hash


def test_time_travel_replay(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    palace.push_decision("Step 1", "BUILD", "Initial setup")       # Seq 1
    palace.push_decision("Step 2", "EVALUATE", "Experimental test") # Seq 2
    palace.pop_decision("Experiment ended")                         # Seq 3
    palace.push_decision("Step 3", "BUILD", "Production rollout")   # Seq 4

    # Time travel to Seq 1
    s1 = palace.replay(to_seq=1)
    assert s1.current_seq == 1
    assert len(s1.active_stack) == 1
    assert s1.active_stack[0].topic == "Step 1"

    # Time travel to Seq 2
    s2 = palace.replay(to_seq=2)
    assert s2.current_seq == 2
    assert len(s2.active_stack) == 2
    assert s2.active_stack[1].topic == "Step 2"

    # Time travel to Seq 3 (after pop)
    s3 = palace.replay(to_seq=3)
    assert s3.current_seq == 3
    assert len(s3.active_stack) == 1
    assert len(s3.history_archive) == 1
    assert s3.history_archive[0].topic == "Step 2"

    # Time travel to Seq 4 (tip)
    s4 = palace.replay(to_seq=4)
    assert s4.current_seq == 4
    assert len(s4.active_stack) == 2
    assert s4.active_stack[1].topic == "Step 3"


def test_dead_ends_retention_across_pops(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    # Push decision with dead_ends
    palace.push_decision(
        topic="Algorithmic Exploration",
        verdict="EVALUATE",
        rationale="Testing greedy approach",
        dead_ends=["greedy_search_on_large_graph"]
    )
    # Record standalone dead end
    palace.record_dead_end("brute_force_recursion", "O(2^N) stack overflow")

    # Pop decision
    palace.pop_decision("Greedy approach abandoned due to local minima")

    assert len(palace.decision_stack) == 0
    assert len(palace.dead_ends_pool) == 2
    paths = [d["path"] for d in palace.dead_ends_pool]
    assert "greedy_search_on_large_graph" in paths
    assert "brute_force_recursion" in paths


def test_hash_chain_tamper_detection(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    palace.push_decision("Valid Decision 1", "BUILD", "OK")
    palace.push_decision("Valid Decision 2", "BUILD", "OK")

    events_file = palace.events_file
    with open(events_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Tamper with payload of first event
    corrupted_data = json.loads(lines[0])
    corrupted_data["payload"]["verdict"] = "REJECT"  # Tampered without updating hash
    lines[0] = json.dumps(corrupted_data) + "\n"

    with open(events_file, "w", encoding="utf-8") as f:
        f.writelines(lines)

    # Reload should fail closed on hash integrity check
    with pytest.raises(HashIntegrityError):
        MemoryPalace(store_dir=store_dir)


def test_sequence_gap_detection(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    palace.push_decision("Valid Decision 1", "BUILD", "OK")
    palace.push_decision("Valid Decision 2", "BUILD", "OK")

    events_file = palace.events_file
    with open(events_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Tamper with seq_id of second event
    corrupted_data = json.loads(lines[1])
    corrupted_data["seq_id"] = 5  # Gap from 1 to 5
    corrupted_data["event_hash"] = DecisionEvent(**corrupted_data).compute_hash()
    lines[1] = json.dumps(corrupted_data) + "\n"

    with open(events_file, "w", encoding="utf-8") as f:
        f.writelines(lines)

    with pytest.raises(EventSequenceError):
        MemoryPalace(store_dir=store_dir)


def test_legacy_bootstrap_migration(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    os.makedirs(store_dir, exist_ok=True)

    legacy_stack = [
        {
            "frame_id": "frame_0001",
            "topic": "Legacy Task 1",
            "decided_by": "orchestrator",
            "verdict": "BUILD",
            "rationale": "Legacy rationale 1",
            "assumptions": [],
            "dead_ends_explored": [],
            "pending_questions": [],
            "timestamp": 1787146000.0
        },
        {
            "frame_id": "frame_0002",
            "topic": "Legacy Task 2",
            "decided_by": "orchestrator",
            "verdict": "REVIEW",
            "rationale": "Legacy rationale 2",
            "assumptions": [],
            "dead_ends_explored": [],
            "pending_questions": [],
            "timestamp": 1787146100.0
        }
    ]

    with open(os.path.join(store_dir, "decision_stack.json"), "w", encoding="utf-8") as f:
        json.dump(legacy_stack, f)

    # Initialize palace - should migrate legacy stack into event log
    palace = MemoryPalace(store_dir=store_dir)
    assert os.path.exists(palace.events_file)
    assert len(palace.events) == 2
    assert len(palace.decision_stack) == 2
    assert palace.decision_stack[0].topic == "Legacy Task 1"
    assert palace.decision_stack[1].topic == "Legacy Task 2"

    integrity = palace.verify_chain_integrity()
    assert integrity["ok"] is True
    assert integrity["total_events"] == 2


def test_snapshot_creation(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir, snapshot_interval=2)

    palace.push_decision("Task 1", "BUILD", "Rationale 1")
    assert len(os.listdir(palace.snapshots_dir)) == 0

    # Second push hits interval=2 -> snapshot taken
    palace.push_decision("Task 2", "BUILD", "Rationale 2")
    snapshots = os.listdir(palace.snapshots_dir)
    assert len(snapshots) == 1
    assert "snapshot_seq_000002.json" in snapshots


def test_assumption_falsification_and_amendment(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    frame = palace.push_decision(
        topic="Database Selection",
        verdict="EVALUATE",
        rationale="Initial choice Postgres",
        assumptions=["postgres_is_fast", "sqlite_has_write_lock_bottleneck"]
    )
    assert len(palace.decision_stack[0].assumptions) == 2

    # Emit assumption falsification
    event1 = DecisionEvent(
        seq_id=2,
        event_id="evt_falsify_1",
        event_type=EventType.ASSUMPTION_FALSIFIED,
        actor="RD-Agent",
        payload={"frame_id": frame.frame_id, "assumption": "sqlite_has_write_lock_bottleneck"},
        prev_event_hash=palace.state.latest_hash
    )
    palace._append_event(event1)
    palace.events.append(event1)
    palace.state = apply_event(palace.state, event1)

    assert len(palace.decision_stack[0].assumptions) == 1
    assert "sqlite_has_write_lock_bottleneck" not in palace.decision_stack[0].assumptions

    # Emit decision amended
    event2 = DecisionEvent(
        seq_id=3,
        event_id="evt_amend_1",
        event_type=EventType.DECISION_AMENDED,
        actor="Lead-Architect",
        payload={"frame_id": frame.frame_id, "verdict": "BUILD", "rationale": "Switched to SQLite with WAL mode"},
        prev_event_hash=palace.state.latest_hash
    )
    palace._append_event(event2)
    palace.events.append(event2)
    palace.state = apply_event(palace.state, event2)

    assert palace.decision_stack[0].verdict == "BUILD"
    assert palace.decision_stack[0].rationale == "Switched to SQLite with WAL mode"


def test_process_supervisor():
    sup = ProcessSupervisor(default_timeout_sec=0.1)

    # 1. Register running task
    sup.register_task("task-1", "RD-Coder")
    sup.record_heartbeat("task-1", "Writing models.py...")

    import time
    time.sleep(0.15)  # Wait for timeout

    # 2. Check health - should flag stalled
    health = sup.check_health()
    assert health["total_tasks"] == 1
    assert health["stalled_count"] == 1
    assert health["stalled_tasks"][0]["task_id"] == "task-1"


# =========================================================================
# Adversarial & Hardened Regression Tests (P0-01 ~ P2-03)
# =========================================================================

def test_p0_01_concurrent_writers_no_ledger_forks(tmp_path):
    """P0-01: Concurrent writers must commit sequentially without fork or sequence gaps."""
    store_dir = os.path.join(tmp_path, "store")
    # Initialize base store
    MemoryPalace(store_dir=store_dir)

    num_writers = 8
    pushes_per_writer = 5

    def worker_push(worker_id):
        palace = MemoryPalace(store_dir=store_dir)
        for i in range(pushes_per_writer):
            palace.push_decision(
                topic=f"Topic from worker {worker_id}",
                verdict="BUILD",
                rationale=f"Worker {worker_id} push #{i}",
                decided_by=f"worker_{worker_id}"
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_writers) as executor:
        futures = [executor.submit(worker_push, wid) for wid in range(num_writers)]
        concurrent.futures.wait(futures)
        for f in futures:
            f.result()  # Raise exception if any worker failed

    # Reload store and verify integrity
    final_palace = MemoryPalace(store_dir=store_dir)
    total_expected = num_writers * pushes_per_writer
    assert len(final_palace.events) == total_expected
    assert final_palace.state.current_seq == total_expected

    # Verify strictly monotonic seq_ids: 1, 2, ..., total_expected
    seq_ids = [e.seq_id for e in final_palace.events]
    assert seq_ids == list(range(1, total_expected + 1))

    integrity = final_palace.verify_chain_integrity()
    assert integrity["ok"] is True
    assert integrity["total_events"] == total_expected


def test_p0_02_canonical_hashing_wall_time_and_nan_rejection():
    """P0-02: Events with different wall_time must produce different hashes; non-finite floats rejected."""
    e1 = DecisionEvent(
        seq_id=1,
        event_id="evt_1",
        event_type=EventType.DECISION_PUSHED,
        actor="orch",
        payload={"topic": "T"},
        wall_time=100.0,
        prev_event_hash="0" * 64
    )
    e2 = DecisionEvent(
        seq_id=1,
        event_id="evt_1",
        event_type=EventType.DECISION_PUSHED,
        actor="orch",
        payload={"topic": "T"},
        wall_time=200.0,
        prev_event_hash="0" * 64
    )
    # Different wall_time -> different hash
    assert e1.event_hash != e2.event_hash

    # Non-finite floats must be rejected
    with pytest.raises(ValueError, match="wall_time must be a finite float"):
        DecisionEvent(
            seq_id=1,
            event_id="evt_nan",
            event_type=EventType.DECISION_PUSHED,
            actor="orch",
            payload={},
            wall_time=float("nan"),
            prev_event_hash="0" * 64
        )

    with pytest.raises(ValueError, match="wall_time must be a finite float"):
        DecisionEvent(
            seq_id=1,
            event_id="evt_inf",
            event_type=EventType.DECISION_PUSHED,
            actor="orch",
            payload={},
            wall_time=float("inf"),
            prev_event_hash="0" * 64
        )


def test_p0_03_projection_immutability(tmp_path):
    """P0-03: Mutating returned public properties does not corrupt internal state."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    palace.add_immutable_rule("INV-01", "Governance", "Do not modify.")
    palace.register_topology_node("service_a", "/path/a")
    palace.push_decision("Topic A", "BUILD", "Clean")
    palace.record_dead_end("/bad/path", "Dead end")

    # 1. Mutate decision_stack
    stack = palace.decision_stack
    stack[0].verdict = "TAMPERED_REJECT"
    stack[0].topic = "TAMPERED_TOPIC"
    assert palace.decision_stack[0].verdict == "BUILD"
    assert palace.decision_stack[0].topic == "Topic A"

    # 2. Mutate dead_ends_pool
    pool = palace.dead_ends_pool
    pool[0]["path"] = "/tampered/path"
    assert palace.dead_ends_pool[0]["path"] == "/bad/path"

    # 3. Mutate immutable_rules
    rules = palace.immutable_rules
    rules["INV-01"].description = "Tampered rule description"
    assert palace.immutable_rules["INV-01"].description == "Do not modify."

    # 4. Mutate topology
    topo = palace.topology
    topo["service_a"].path = "/tampered/topo"
    assert palace.topology["service_a"].path == "/path/a"

    # 5. Mutate active context package
    pkg = palace.get_active_context_package()
    pkg["active_decision_stack"][0]["verdict"] = "TAMPERED"
    assert palace.decision_stack[0].verdict == "BUILD"

    # Verification must remain clean
    assert palace.verify_chain_integrity()["ok"] is True


def test_p0_04_supervisor_repeated_sweeps_and_failed_states():
    """P0-04: Stalled and failed tasks must not revert to healthy on subsequent sweeps."""
    sup = ProcessSupervisor(default_timeout_sec=0.1)

    sup.register_task("task-stalled", "Agent-1", timeout_sec=0.1)
    sup.register_task("task-failed", "Agent-2", timeout_sec=5.0)
    sup.register_task("task-healthy", "Agent-3", timeout_sec=5.0)

    sup.record_heartbeat("task-failed", status=TaskStatus.FAILED, log_snippet="OOM crash")

    time.sleep(0.15)  # Expire timeout only for task-stalled

    # First sweep
    h1 = sup.check_health()
    assert h1["stalled_count"] == 1
    assert h1["failed_count"] == 1
    assert h1["healthy_count"] == 1
    assert h1["stalled_tasks"][0]["task_id"] == "task-stalled"

    # Immediate second sweep without heartbeats -> stalled/failed must STAY stalled/failed!
    h2 = sup.check_health()
    assert h2["stalled_count"] == 1
    assert h2["failed_count"] == 1
    assert h2["healthy_count"] == 1

    # Revive stalled task with heartbeat
    sup.record_heartbeat("task-stalled", status=TaskStatus.RUNNING, log_snippet="Working fine")
    h3 = sup.check_health()
    assert h3["healthy_count"] == 2
    assert h3["stalled_count"] == 0
    assert h3["failed_count"] == 1



def test_p0_05_layer1_fail_closed_and_l2_non_interference(tmp_path):
    """P0-05: Layer 1 is fail-closed, rejects duplicate rule IDs, and is not erased by L2 updates."""
    store_dir = os.path.join(tmp_path, "store")
    palace_a = MemoryPalace(store_dir=store_dir)
    palace_b = MemoryPalace(store_dir=store_dir)

    # 1. Add L1 rule in A
    palace_a.add_immutable_rule("INV-001", "Core", "Governance rule 1")

    # 2. Reject duplicate rule_id
    with pytest.raises(RuleConflictError):
        palace_a.add_immutable_rule("INV-001", "Core", "Attempt overwrite")

    # 3. Process B registers an L2 node; must not wipe L1 rule
    palace_b.register_topology_node("service_x", "/path/x")

    # Reload fresh palace from disk
    palace_c = MemoryPalace(store_dir=store_dir)
    assert "INV-001" in palace_c.immutable_rules
    assert "service_x" in palace_c.topology

    # 4. Fail-closed on corrupted immutable_rules.json
    rules_file = palace_c._file_path("immutable_rules")
    with open(rules_file, "w", encoding="utf-8") as f:
        f.write("{ INVALID JSON CONTENT ...")

    with pytest.raises(MemPalaceError, match="Fail-closed"):
        MemoryPalace(store_dir=store_dir)


def test_p1_01_supervisor_shared_durable_registry(tmp_path):
    """P1-01: ProcessSupervisor shares durable state across separate instances."""
    store_dir = os.path.join(tmp_path, "store")
    sup1 = ProcessSupervisor(store_dir=store_dir, default_timeout_sec=5.0)
    sup1.register_task("shared-task-1", "Runner", pid=1234)

    # Second supervisor instance pointing to same store
    sup2 = ProcessSupervisor(store_dir=store_dir)
    health = sup2.check_health()
    assert health["total_tasks"] == 1
    assert health["healthy_count"] == 1
    assert health["healthy_tasks"][0]["task_id"] == "shared-task-1"
    assert health["healthy_tasks"][0]["pid"] == 1234


def test_p1_03_disk_tamper_detection(tmp_path):
    """P1-03: Tampering directly with the on-disk JSONL log is detected by verify_chain_integrity."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    palace.push_decision("Task 1", "BUILD", "Clean")
    palace.push_decision("Task 2", "BUILD", "Clean")

    # Tamper with file on disk
    with open(palace.events_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
    data = json.loads(lines[0])
    data["payload"]["verdict"] = "TAMPERED_VERDICT"
    lines[0] = json.dumps(data) + "\n"
    with open(palace.events_file, "w", encoding="utf-8") as f:
        f.writelines(lines)

    # verify_chain_integrity re-reads disk and catches tampering
    with pytest.raises(HashIntegrityError):
        palace.verify_chain_integrity()


def test_p1_04_semantic_transition_preconditions(tmp_path):
    """P1-04: Invalid state transitions raise EventSemanticError."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    # 1. Pop from empty stack
    assert palace.pop_decision() is None

    # Construct invalid DECISION_POPPED on empty stack
    invalid_pop_evt = DecisionEvent(
        seq_id=1,
        event_id="evt_invalid_pop",
        event_type=EventType.DECISION_POPPED,
        actor="orch",
        payload={"popped_frame_id": "non_existent"},
        prev_event_hash="0" * 64
    )
    with pytest.raises(EventSemanticError, match="decision stack is empty"):
        apply_event(DecisionStackState(), invalid_pop_evt)

    # 2. Push frame
    frame = palace.push_decision("Architecture", "BUILD", "Ok", assumptions=["A1"])

    # 3. Amend non-existent frame
    with pytest.raises(EventSemanticError, match="Target frame 'frame_ghost' not found"):
        palace.amend_decision("frame_ghost", verdict="REJECT")

    # 4. Falsify non-existent assumption
    with pytest.raises(EventSemanticError, match="Assumption 'ghost_assumption' not found"):
        palace.falsify_assumption("ghost_assumption", frame_id=frame.frame_id)


def test_p1_06_snapshot_interval_validation(tmp_path):
    """P1-06: Snapshot interval <= 0 is rejected during initialization."""
    store_dir = os.path.join(tmp_path, "store")
    with pytest.raises(ValueError, match="snapshot_interval must be a positive integer or None"):
        MemoryPalace(store_dir=store_dir, snapshot_interval=0)

    with pytest.raises(ValueError, match="snapshot_interval must be a positive integer or None"):
        MemoryPalace(store_dir=store_dir, snapshot_interval=-5)

    # None is valid (disabled snapshots)
    p_none = MemoryPalace(store_dir=store_dir, snapshot_interval=None)
    assert p_none.snapshot_interval is None


def test_p2_01_frame_id_uniqueness(tmp_path):
    """P2-01: Frame IDs are distinct across pushes and pops."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    f1 = palace.push_decision("Task 1", "BUILD", "Rationale 1")
    palace.pop_decision("Done")
    f2 = palace.push_decision("Task 2", "BUILD", "Rationale 2")

    assert f1.frame_id != f2.frame_id


def test_p2_02_snapshot_fast_forward_recovery(tmp_path):
    """P2-02: Snapshot fast-forwards state recovery accurately."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir, snapshot_interval=3)

    for i in range(1, 8):
        palace.push_decision(f"Task {i}", "BUILD", f"Rationale {i}")

    # Snapshots should exist for seq 3 and seq 6
    snapshots = [f for f in os.listdir(palace.snapshots_dir) if f.endswith(".json")]
    assert len(snapshots) == 2
    assert "snapshot_seq_000003.json" in snapshots
    assert "snapshot_seq_000006.json" in snapshots

    # Reload store — must leverage snapshot and match tip
    reloaded = MemoryPalace(store_dir=store_dir, snapshot_interval=3)
    assert reloaded.state.current_seq == 7
    assert len(reloaded.decision_stack) == 7
    assert reloaded.verify_chain_integrity()["ok"] is True


def test_get_workspace_root_resolution(tmp_path, monkeypatch):
    """B2: get_workspace_root resolves AI_WORKSPACE_DIR, MEMPALACE_WORKSPACE_DIR, or fallback."""
    from pathlib import Path
    from mempalace.core import get_workspace_root

    # 1. AI_WORKSPACE_DIR takes highest precedence
    ws_ai = tmp_path / "ws_ai"
    ws_ai.mkdir()
    monkeypatch.setenv("AI_WORKSPACE_DIR", str(ws_ai))
    monkeypatch.setenv("MEMPALACE_WORKSPACE_DIR", str(tmp_path / "ws_mp"))
    assert get_workspace_root() == ws_ai.resolve()

    # 2. MEMPALACE_WORKSPACE_DIR takes precedence when AI_WORKSPACE_DIR absent
    monkeypatch.delenv("AI_WORKSPACE_DIR", raising=False)
    ws_mp = tmp_path / "ws_mp"
    ws_mp.mkdir()
    assert get_workspace_root() == ws_mp.resolve()

    # 3. Fallback when neither env var is present or env var is whitespace only
    monkeypatch.setenv("AI_WORKSPACE_DIR", "   \t\n")
    monkeypatch.setenv("MEMPALACE_WORKSPACE_DIR", "   ")
    fallback = Path(__file__).resolve().parents[2]
    assert get_workspace_root() == fallback


def test_load_topology_dynamic_relocation(tmp_path, monkeypatch):
    """B4: _load_topology dynamically relocates relative and foreign paths, and persists portably."""
    from pathlib import Path

    mock_ws = tmp_path / "mock_workspace"
    mock_ws.mkdir()
    mock_repo = mock_ws / "ai-company"
    mock_repo.mkdir()

    monkeypatch.setenv("AI_WORKSPACE_DIR", str(mock_ws))

    store_dir = str(tmp_path / "store")
    os.makedirs(store_dir, exist_ok=True)
    topo_file = os.path.join(store_dir, "topology.json")

    # 1. Test relative path in topology
    seed_data = {
        "ai-company": {
            "name": "ai-company",
            "path": "ai-company",
            "node_type": "repo",
            "key_apis": ["dev-team"],
            "verification_command": "pytest"
        },
        "foreign-repo": {
            "name": "foreign-repo",
            "path": "Z:\\foreign\\nonexistent\\foreign-repo",
            "node_type": "repo",
            "key_apis": ["api"],
            "verification_command": "pytest"
        },
        "unmatched-rel": {
            "name": "unmatched-rel",
            "path": "sub/nonexistent",
            "node_type": "repo",
            "key_apis": [],
            "verification_command": None
        },
        "candidate-rel": {
            "name": "ai-company",
            "path": "obsolete_subpath",
            "node_type": "repo",
            "key_apis": [],
            "verification_command": None
        }
    }
    with open(topo_file, "w", encoding="utf-8") as f:
        json.dump(seed_data, f)

    palace = MemoryPalace(store_dir=store_dir)
    assert palace.topology["ai-company"].path == str(mock_repo)
    # Foreign repo where candidate does not exist keeps path safely
    assert palace.topology["foreign-repo"].path == "Z:\\foreign\\nonexistent\\foreign-repo"
    # Unmatched relative path falls back to ws_root / rel_path
    assert palace.topology["unmatched-rel"].path == str(mock_ws / "sub" / "nonexistent")
    # Candidate exists when rel_candidate does not -> auto-relocates to candidate
    assert palace.topology["candidate-rel"].path == str(mock_repo)

    # 2. Foreign path that DOES have a candidate in active workspace auto-relocates
    foreign_with_candidate = {
        "ai-company": {
            "name": "ai-company",
            "path": "X:\\old_machine\\ai-company",
            "node_type": "repo",
            "key_apis": ["dev-team"],
            "verification_command": "pytest"
        }
    }
    with open(topo_file, "w", encoding="utf-8") as f:
        json.dump(foreign_with_candidate, f)

    palace2 = MemoryPalace(store_dir=store_dir)
    assert palace2.topology["ai-company"].path == str(mock_repo)

    # 3. [P1-01 Fix Verification] Persisting topology nodes under ws_root writes portable relative paths to disk
    palace2.register_topology_node("new-service", str(mock_ws / "new-service"), "repo")
    palace2.save_all()

    with open(topo_file, "r", encoding="utf-8") as f:
        saved_on_disk = json.load(f)
    assert saved_on_disk["ai-company"]["path"] == "ai-company"
    assert saved_on_disk["new-service"]["path"] == "new-service"
    # Verify no Windows drive letters in saved JSON
    raw_saved = json.dumps(saved_on_disk)
    assert "X:" not in raw_saved
    assert str(mock_ws) not in raw_saved

    # 4. Verification of exception tolerance in _dump_topology_portable
    from mempalace.core import _dump_topology_portable, TopologyNode
    bad_node = TopologyNode(name="bad", path="X:\\invalid\0nullbyte")
    dumped_bad = _dump_topology_portable({"bad": bad_node}, mock_ws)
    assert dumped_bad["bad"]["path"] == "X:\\invalid\0nullbyte"


def test_zero_hardcoded_paths_contract():
    """B1 & Rule 8: Enforce zero hardcoded drive letters in production code and seed topology."""
    from pathlib import Path
    import re

    repo_root = Path(__file__).resolve().parents[1]
    production_dir = repo_root / "mempalace"
    topology_file = repo_root / "data" / "memory_store" / "topology.json"

    # Match Windows drive paths like C:\ or D:/ while excluding URI schemes like palace:// or http://
    pattern = re.compile(r'(?<![a-zA-Z0-9])[a-zA-Z]:[\\/](?!/)')

    # Audit production code
    for py_file in production_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        matches = pattern.findall(content)
        assert not matches, f"Found hardcoded drive path in {py_file}: {matches}"

    # Audit seed topology
    if topology_file.exists():
        content = topology_file.read_text(encoding="utf-8")
        matches = pattern.findall(content)
        assert not matches, f"Found hardcoded drive path in {topology_file}: {matches}"





