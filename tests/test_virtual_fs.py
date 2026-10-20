"""Tests for MemPalace Virtual Filesystem (palace://) and Tiered Context Loading (L0/L1/L2)."""

import pytest
import os
import json
from mempalace.core import MemoryPalace
from mempalace.models import ContextLevel, TaskStatus
from mempalace.supervisor import ProcessSupervisor


@pytest.fixture
def populated_palace(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    # Layer 1: Invariants
    palace.add_immutable_rule("INV-001", "Governance", "Fail-closed on all unprovable state checks.")
    palace.add_immutable_rule("INV-002", "Safety", "Block destructive git commands.")

    # Layer 2: Topology
    palace.register_topology_node("knowledge-island", str(tmp_path / "knowledge-island"), "repo", ["price_zones"], "pytest")
    palace.register_topology_node("mempalace", str(tmp_path / "mempalace"), "repo", ["event_replay"], "pytest tests -v")

    # Layer 3: Decisions & Dead Ends
    palace.push_decision("Architecture V2", "BUILD", "Clean modular design", assumptions=["Deterministic"])
    palace.push_decision("Refactor Storage", "REVIEW", "Performance verification", assumptions=["Lock safe"])
    palace.record_dead_end("brute_force_scan", "O(N^2) latency is unacceptable")

    # Supervision
    supervisor = ProcessSupervisor(store_dir=store_dir)
    supervisor.register_task("task-worker-1", "researcher", timeout_sec=60.0)
    supervisor.record_heartbeat("task-worker-1", log_snippet="Scanning files", status=TaskStatus.RUNNING)

    return palace


def test_tiered_context_levels(populated_palace):
    """Verify L0, L1, and L2 tiered context output characteristics."""
    # L0: Abstract fast-filter
    ctx_l0 = populated_palace.get_tiered_context(level=ContextLevel.L0)
    assert ctx_l0["level"] == "L0"
    assert "Rules: 2" in ctx_l0["summary"]
    assert "Topology: 2 repos" in ctx_l0["summary"]
    assert "Active Stack: 2" in ctx_l0["summary"]
    assert "Dead-ends: 1" in ctx_l0["summary"]
    assert ctx_l0["tip_decision"] == "[REVIEW] Refactor Storage (by orchestrator)"
    assert len(ctx_l0["latest_hash_prefix"]) == 16

    # L1: Overview structured context
    ctx_l1 = populated_palace.get_tiered_context(level=ContextLevel.L1)
    assert ctx_l1["level"] == "L1"
    assert len(ctx_l1["immutable_rules"]) == 2
    assert len(ctx_l1["topology"]) == 2
    assert len(ctx_l1["active_decision_stack"]) == 2
    assert len(ctx_l1["dead_ends_pool"]) == 1
    assert ctx_l1["active_decision_stack"][-1]["verdict"] == "REVIEW"
    assert ctx_l1["event_lineage"]["total_events"] == 3

    # L2: Full raw detail
    ctx_l2 = populated_palace.get_tiered_context(level=ContextLevel.L2)
    assert ctx_l2["level"] == "L2"
    assert "INV-001" in ctx_l2["immutable_rules"]
    assert "knowledge-island" in ctx_l2["topology"]
    assert len(ctx_l2["events"]) == 3
    assert ctx_l2["events"][0]["event_type"] == "DECISION_PUSHED"


def test_uri_normalization(populated_palace):
    """Verify URI normalizer handles variations cleanly."""
    assert populated_palace._normalize_uri("") == "palace://"
    assert populated_palace._normalize_uri("palace://") == "palace://"
    assert populated_palace._normalize_uri("palace:/rules") == "palace://rules"
    assert populated_palace._normalize_uri("palace:rules/INV-001") == "palace://rules/INV-001"
    assert populated_palace._normalize_uri("rules\\INV-001") == "palace://rules/INV-001"
    assert populated_palace._normalize_uri("palace://topology/knowledge-island/") == "palace://topology/knowledge-island"


def test_ls_root_and_layers(populated_palace):
    """Verify virtual directory listing across all memory layers."""
    # Root listing
    root_ls = populated_palace.ls("palace://")
    names = [item["name"] for item in root_ls]
    assert "rules" in names
    assert "topology" in names
    assert "decisions" in names
    assert "supervision" in names
    assert "audit" in names

    # Rules listing
    rules_ls = populated_palace.ls("palace://rules")
    assert len(rules_ls) == 2
    assert rules_ls[0]["uri"] == "palace://rules/INV-001"
    assert rules_ls[0]["node_type"] == "rule"

    # Topology listing
    topo_ls = populated_palace.ls("palace://topology")
    assert len(topo_ls) == 2
    assert any(t["name"] == "knowledge-island" for t in topo_ls)

    # Decisions subdirectories
    dec_ls = populated_palace.ls("palace://decisions")
    dec_names = [d["name"] for d in dec_ls]
    assert "stack" in dec_names
    assert "dead_ends" in dec_names
    assert "history" in dec_names

    # Decisions stack listing
    stack_ls = populated_palace.ls("palace://decisions/stack")
    assert len(stack_ls) == 2
    assert stack_ls[0]["verdict"] == "BUILD"
    assert stack_ls[1]["verdict"] == "REVIEW"

    # Dead ends listing
    de_ls = populated_palace.ls("palace://decisions/dead_ends")
    assert len(de_ls) == 1
    assert de_ls[0]["path"] == "brute_force_scan"

    # Audit events listing
    audit_ls = populated_palace.ls("palace://audit")
    assert len(audit_ls) == 3
    assert audit_ls[0]["seq_id"] == 1


def test_resolve_uri_specific_nodes(populated_palace):
    """Verify resolving specific URIs at different context tiers."""
    # Rule resolution
    r_l0 = populated_palace.resolve_uri("palace://rules/INV-001", level=ContextLevel.L0)
    assert isinstance(r_l0, str)
    assert "[INV-001]" in r_l0

    r_l1 = populated_palace.resolve_uri("palace://rules/INV-001", level=ContextLevel.L1)
    assert isinstance(r_l1, dict)
    assert r_l1["rule_id"] == "INV-001"
    assert r_l1["enforcement_type"] == "fail-closed"

    # Topology resolution
    t_l1 = populated_palace.resolve_uri("palace://topology/knowledge-island", level=ContextLevel.L1)
    assert t_l1["name"] == "knowledge-island"
    assert t_l1["verification_command"] == "pytest"

    # Active Decision resolution by index and frame_id
    d_0 = populated_palace.resolve_uri("palace://decisions/stack/0", level=ContextLevel.L1)
    assert d_0["verdict"] == "BUILD"
    frame_id = d_0["frame_id"]
    d_by_id = populated_palace.resolve_uri(f"palace://decisions/stack/{frame_id}", level=ContextLevel.L1)
    assert d_by_id["frame_id"] == frame_id

    # Dead end resolution by index
    de_0 = populated_palace.resolve_uri("palace://decisions/dead_ends/0")
    assert de_0["path"] == "brute_force_scan"

    # Audit event resolution by seq_id
    evt_1 = populated_palace.resolve_uri("palace://audit/events/1", level=ContextLevel.L1)
    assert evt_1["seq_id"] == 1
    assert evt_1["event_type"] == "DECISION_PUSHED"

    # Supervision resolution
    task_hb = populated_palace.resolve_uri("palace://supervision/tasks/task-worker-1", level=ContextLevel.L1)
    assert task_hb["task_id"] == "task-worker-1"
    assert task_hb["status"] == "running"


def test_resolve_uri_fail_closed(populated_palace):
    """Verify invalid URIs fail closed with KeyError."""
    with pytest.raises(KeyError, match="Rule 'INV-999' not found"):
        populated_palace.resolve_uri("palace://rules/INV-999")

    with pytest.raises(KeyError, match="Topology node 'unknown_repo' not found"):
        populated_palace.resolve_uri("palace://topology/unknown_repo")

    with pytest.raises(KeyError, match="not found in active stack"):
        populated_palace.resolve_uri("palace://decisions/stack/99")

    with pytest.raises(KeyError, match="Dead end index '99' out of range"):
        populated_palace.resolve_uri("palace://decisions/dead_ends/99")

    with pytest.raises(KeyError, match="Unknown root segment 'invalid_layer'"):
        populated_palace.resolve_uri("palace://invalid_layer")


def test_tree_rendering(populated_palace):
    """Verify ASCII tree generation formatting."""
    tree_text = populated_palace.tree()
    assert "palace://" in tree_text
    assert "├── rules/ (2 rules)" in tree_text
    assert "INV-001" in tree_text
    assert "├── topology/ (2 nodes)" in tree_text
    assert "knowledge-island" in tree_text
    assert "├── decisions/" in tree_text
    assert "stack/ (2 active frames)" in tree_text
    assert "dead_ends/ (1 recorded paths)" in tree_text
    assert "└── audit/ (3 events, tip #3)" in tree_text


def test_find_cross_layer_search(populated_palace):
    """Verify recursive search across all layers."""
    # Find in topology
    topo_matches = populated_palace.find("knowledge-island")
    assert len(topo_matches) >= 1
    assert topo_matches[0]["node_type"] == "topology"
    assert topo_matches[0]["uri"] == "palace://topology/knowledge-island"

    # Find in rules
    rule_matches = populated_palace.find("Fail-closed")
    assert len(rule_matches) >= 1
    assert rule_matches[0]["node_type"] == "rule"
    assert rule_matches[0]["uri"] == "palace://rules/INV-001"

    # Find in decisions
    dec_matches = populated_palace.find("modular")
    assert len(dec_matches) >= 1
    assert dec_matches[0]["node_type"] == "decision_frame"

    # Find in dead ends
    de_matches = populated_palace.find("unacceptable")
    assert len(de_matches) >= 1
    assert de_matches[0]["node_type"] == "dead_end"

    # Search with no match
    assert len(populated_palace.find("non_existent_token_xyz")) == 0
    assert len(populated_palace.find("")) == 0
