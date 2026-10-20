"""Tests for MemPalace integrations helper module."""

import pytest
import os
from mempalace.core import MemoryPalace
from mempalace.models import ContextLevel
from mempalace.integrations import (
    format_prompt_header,
    build_handoff_envelope,
    handle_post_compact,
    reinject_hot_cache,
)


def test_format_prompt_header_l0_l1_l2(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    palace.add_immutable_rule("INV-001", "Governance", "Fail-closed check.")
    palace.push_decision("Feature Alpha", "BUILD", "Meets spec.")

    # L0 Header
    hdr_l0 = format_prompt_header(level=ContextLevel.L0, palace=palace)
    assert "[MemPalace L0 Context:" in hdr_l0
    assert "Rules: 1" in hdr_l0
    assert "BUILD" in hdr_l0
    assert "Feature Alpha" in hdr_l0

    # L1 Header
    hdr_l1 = format_prompt_header(level=ContextLevel.L1, palace=palace)
    assert "=== MemPalace L1 Active Context ===" in hdr_l1
    assert "INV-001" in hdr_l1
    assert "BUILD: Feature Alpha" in hdr_l1

    # L2 Header
    hdr_l2 = format_prompt_header(level=ContextLevel.L2, palace=palace)
    assert "=== MemPalace L2 Raw Context ===" in hdr_l2
    assert '"INV-001"' in hdr_l2


def test_build_handoff_envelope(tmp_path):
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    palace.add_immutable_rule("INV-001", "Safety", "No force pushes.")
    palace.push_decision("Task 1", "BUILD", "Ok")

    env = build_handoff_envelope(palace=palace)
    assert env["level"] == "L1"
    assert len(env["immutable_rules"]) == 1
    assert len(env["active_decision_stack"]) == 1


def test_handle_post_compact(tmp_path):
    from mempalace.integrations import handle_post_compact
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    palace.add_immutable_rule("INV-001", "Governance", "Deterministic check.")
    palace.push_decision("Feature PostCompact", "BUILD", "Recovery hook.")

    # 1. Empty or blank text
    res_empty = handle_post_compact("", level=ContextLevel.L1, palace=palace)
    assert "=== MemPalace L1 Active Context ===" in res_empty
    assert "Feature PostCompact" in res_empty

    # 2. Text without prior header
    user_prompt = "Refactor the database connection pool."
    res_fresh = handle_post_compact(user_prompt, level=ContextLevel.L1, palace=palace)
    assert res_fresh.startswith("=== MemPalace L1 Active Context ===")
    assert user_prompt in res_fresh

    # 3. Text with stale header (L1) -> replaced with fresh header
    stale_text = (
        "=== MemPalace L1 Active Context ===\n"
        "* Invariants (L1): OLD-RULE\n"
        "===================================\n\n"
        "Remaining conversation turn."
    )
    res_replaced = handle_post_compact(stale_text, level=ContextLevel.L1, palace=palace)
    assert "OLD-RULE" not in res_replaced
    assert "INV-001" in res_replaced
    assert "Remaining conversation turn." in res_replaced


def test_reinject_hot_cache(tmp_path):
    from mempalace.integrations import reinject_hot_cache
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    palace.add_immutable_rule("INV-002", "Safety", "Fail closed.")

    # 1. Empty messages list
    empty_msgs = reinject_hot_cache([], level=ContextLevel.L0, palace=palace)
    assert len(empty_msgs) == 1
    assert empty_msgs[0]["role"] == "system"
    assert "[MemPalace L0 Context:" in empty_msgs[0]["content"]

    # 2. Messages with existing system prompt
    chat_msgs = [
        {"role": "system", "content": "You are a coding assistant."},
        {"role": "user", "content": "Hello!"}
    ]
    re_msgs = reinject_hot_cache(chat_msgs, level=ContextLevel.L1, palace=palace)
    assert len(re_msgs) == 2
    assert "=== MemPalace L1 Active Context ===" in re_msgs[0]["content"]
    assert "You are a coding assistant." in re_msgs[0]["content"]
    assert re_msgs[1]["content"] == "Hello!"

    # 3. Messages without system prompt (only user message)
    user_only_msgs = [{"role": "user", "content": "Analyze this code."}]
    re_user = reinject_hot_cache(user_only_msgs, level=ContextLevel.L1, palace=palace)
    assert "=== MemPalace L1 Active Context ===" in re_user[0]["content"]
    assert "Analyze this code." in re_user[0]["content"]


def test_get_default_store_dir(tmp_path, monkeypatch):
    from pathlib import Path
    from mempalace.integrations import get_default_store_dir

    # Tier 1: Explicit argument takes highest precedence
    explicit = str(tmp_path / "explicit_store")
    monkeypatch.setenv("MEMPALACE_STORE_DIR", str(tmp_path / "env_store"))
    assert get_default_store_dir(explicit) == explicit

    # Tier 2: Environment variable takes precedence over repo root and CWD
    custom_dir = str(tmp_path / "custom_store")
    os.makedirs(custom_dir, exist_ok=True)
    monkeypatch.setenv("MEMPALACE_STORE_DIR", custom_dir)
    assert get_default_store_dir() == custom_dir

    # Tier 3: Package repo root store when env var is absent and repo store exists
    monkeypatch.delenv("MEMPALACE_STORE_DIR", raising=False)
    repo_store = Path(__file__).resolve().parents[1] / "data" / "memory_store"
    if repo_store.exists():
        assert get_default_store_dir() == str(repo_store)

    # Tier 4: CWD fallback when repo store does not exist
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    assert get_default_store_dir() == "data/memory_store"


def test_handle_post_compact_nested_brackets_p0(tmp_path):
    """Regression test for [P0-01]: L0 header with nested [BUILD] brackets must not leak."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    palace.push_decision("Feature Alpha", "BUILD", "Spec met", "Lead")

    stale_l0 = format_prompt_header(level=ContextLevel.L0, palace=palace)
    # Ensure the L0 header indeed contains nested brackets like (tip: [BUILD] Feature Alpha)
    assert "[BUILD]" in stale_l0

    user_prompt = f"{stale_l0}\n\nUser request: deploy system."
    recompacted = handle_post_compact(user_prompt, level=ContextLevel.L0, palace=palace)

    # Must contain only ONE fresh L0 header, and user request must not contain any severed header fragments
    parts = recompacted.split("\n\n")
    assert len(parts) == 2
    assert parts[0].startswith("[MemPalace L0 Context:")
    assert parts[1] == "User request: deploy system."
    assert "Feature Alpha (by Lead)" not in parts[1]


def test_handle_post_compact_only_stale_header(tmp_path):
    """Regression test for [P2-02]: text containing only stale header returns fresh_header without trailing newline."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)
    stale_l0 = format_prompt_header(level=ContextLevel.L0, palace=palace)

    recompacted = handle_post_compact(stale_l0, level=ContextLevel.L0, palace=palace)
    assert not recompacted.endswith("\n\n")
    assert recompacted.startswith("[MemPalace L0 Context:")


def test_handle_post_compact_resilient_fences(tmp_path):
    """Regression test for [P1-02]: closing fence with variable length = signs is cleanly stripped."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    stale_compacted = "=== MemPalace L1 Active Context ===\nSome content\n=========================\n\nUser task."
    recompacted = handle_post_compact(stale_compacted, level=ContextLevel.L1, palace=palace)
    assert "Some content" not in recompacted
    assert recompacted.endswith("User task.")


def test_reinject_hot_cache_multimodal_list(tmp_path):
    """Regression test for [P1-01]: messages with content as list of structured blocks."""
    store_dir = os.path.join(tmp_path, "store")
    palace = MemoryPalace(store_dir=store_dir)

    multimodal_msgs = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Analyze image and explain."},
                {"type": "image_url", "image_url": "https://example.com/img.png"}
            ]
        }
    ]

    re_msgs = reinject_hot_cache(multimodal_msgs, level=ContextLevel.L1, palace=palace)
    assert isinstance(re_msgs[0]["content"], list)
    first_block = re_msgs[0]["content"][0]
    assert first_block["type"] == "text"
    assert "=== MemPalace L1 Active Context ===" in first_block["text"]
    assert "Analyze image and explain." in first_block["text"]


