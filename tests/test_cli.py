import pytest
import os
import sys
from io import StringIO
from mempalace.cli import main


def run_cli_args(args, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mempalace"] + args)
    main()
    return capsys.readouterr()


def test_cli_full_workflow(tmp_path, monkeypatch, capsys):
    store = os.path.join(tmp_path, "store")

    # 1. Bootstrap
    out = run_cli_args(["--store", store, "bootstrap"], monkeypatch, capsys)
    assert "初始化完成" in out.out

    # 2. Push Decision
    out = run_cli_args([
        "--store", store,
        "push-decision",
        "-t", "Feature X",
        "-v", "BUILD",
        "-r", "All tests green",
        "-a", "RD-Agent"
    ], monkeypatch, capsys)
    assert "Event Seq: #1" in out.out
    assert "Feature X" in out.out

    # 3. Record dead end
    out = run_cli_args([
        "--store", store,
        "record-dead-end",
        "-p", "deprecated_endpoint",
        "-r", "Returned 404"
    ], monkeypatch, capsys)
    assert "Event Seq: #2" in out.out
    assert "deprecated_endpoint" in out.out

    # 4. Push another decision
    out = run_cli_args([
        "--store", store,
        "push-decision",
        "-t", "Feature Y",
        "-v", "REVIEW",
        "-r", "Needs audit"
    ], monkeypatch, capsys)
    assert "Event Seq: #3" in out.out

    # 5. Inspect
    out = run_cli_args(["--store", store, "inspect"], monkeypatch, capsys)
    assert "Feature X" in out.out
    assert "Feature Y" in out.out
    assert "deprecated_endpoint" in out.out
    assert "Total Events: 3" in out.out

    # 6. Audit log
    out = run_cli_args(["--store", store, "audit-log"], monkeypatch, capsys)
    assert "DECISION_PUSHED" in out.out
    assert "DEAD_END_RECORDED" in out.out
    assert "總事件數: 3 筆" in out.out

    # 7. Replay to seq 1
    out = run_cli_args(["--store", store, "replay", "--to-seq", "1"], monkeypatch, capsys)
    assert "重放終點 Seq: #1" in out.out
    assert "Feature X" in out.out
    assert "Feature Y" not in out.out

    # 8. Replay json
    out = run_cli_args(["--store", store, "replay", "--json"], monkeypatch, capsys)
    assert '"current_seq": 3' in out.out

    # 9. Verify integrity
    out = run_cli_args(["--store", store, "verify-integrity"], monkeypatch, capsys)
    assert "事件鏈完整性驗證通過 (CLEAN)" in out.out
    assert "總校驗事件數: 3" in out.out

    # 10. Pop decision
    out = run_cli_args(["--store", store, "pop-decision", "-r", "Reviewed and accepted"], monkeypatch, capsys)
    assert "DECISION_POPPED" in out.out
    assert "Feature Y" in out.out
    assert "當前棧深度: 1 層" in out.out

    # 11. Register task with supervisor via CLI
    out = run_cli_args(["--store", store, "register-task", "--task-id", "task-cli-1", "--role", "Tester", "--timeout", "60.0"], monkeypatch, capsys)
    assert "任務已向監督器註冊" in out.out
    assert "task-cli-1" in out.out

    # 12. Record heartbeat
    out = run_cli_args(["--store", store, "record-heartbeat", "--task-id", "task-cli-1", "--log", "All green", "--status", "running"], monkeypatch, capsys)
    assert "心跳記錄已更新" in out.out

    # 13. Check health — should observe persisted task
    out = run_cli_args(["--store", store, "check-health"], monkeypatch, capsys)
    assert "背景進程健康度檢查" in out.out
    assert "總監控任務數: 1" in out.out
    assert "正常: 1" in out.out

    # 14. Test context L0 and L1
    out_ctx_l0 = run_cli_args(["--store", store, "context", "--level", "L0"], monkeypatch, capsys)
    assert "MemPalace Context Package (Tier: L0)" in out_ctx_l0.out
    assert "Rules:" in out_ctx_l0.out

    out_ctx_json = run_cli_args(["--store", store, "context", "--level", "L1", "--json"], monkeypatch, capsys)
    assert '"level": "L1"' in out_ctx_json.out

    # 15. Test ls virtual filesystem
    out_ls_root = run_cli_args(["--store", store, "ls", "palace://"], monkeypatch, capsys)
    assert "rules" in out_ls_root.out
    assert "topology" in out_ls_root.out

    out_ls_rules = run_cli_args(["--store", store, "ls", "palace://rules"], monkeypatch, capsys)
    assert "INV-001" in out_ls_rules.out

    # 16. Test view specific node
    out_view = run_cli_args(["--store", store, "view", "palace://rules/INV-001"], monkeypatch, capsys)
    assert "INV-001" in out_view.out
    assert "Fail-closed" in out_view.out

    # 17. Test tree
    out_tree = run_cli_args(["--store", store, "tree"], monkeypatch, capsys)
    assert "palace://" in out_tree.out
    assert "├── rules/" in out_tree.out

    # 18. Test find
    out_find = run_cli_args(["--store", store, "find", "knowledge-island"], monkeypatch, capsys)
    assert "knowledge-island" in out_find.out
    assert "palace://topology/knowledge-island" in out_find.out

    # 19. Test post-compact CLI
    out_post_compact = run_cli_args(["--store", store, "post-compact", "--level", "L1", "--text", "User query"], monkeypatch, capsys)
    assert "=== MemPalace L1 Active Context ===" in out_post_compact.out
    assert "User query" in out_post_compact.out

    # Test file input/output in post-compact
    in_file = os.path.join(tmp_path, "compact_in.txt")
    out_file = os.path.join(tmp_path, "compact_out.txt")
    with open(in_file, "w", encoding="utf-8") as f:
        f.write("Truncated session text.")

    out_file_run = run_cli_args(["--store", store, "post-compact", "--level", "L0", "--file", in_file, "--output", out_file], monkeypatch, capsys)
    assert "已將 PostCompact 重注入內容寫入至" in out_file_run.out
    with open(out_file, "r", encoding="utf-8") as f:
        saved_content = f.read()
    assert "[MemPalace L0 Context:" in saved_content
    assert "Truncated session text." in saved_content

    # Test post-compact missing file error handling (fail-closed)
    with pytest.raises(SystemExit) as exc:
        run_cli_args(["--store", store, "post-compact", "--file", "non_existent_file_xyz.txt"], monkeypatch, capsys)
    assert exc.value.code == 1
    err_out = capsys.readouterr()
    assert "輸入檔案不存在" in err_out.err


def test_cli_env_var_store_and_bootstrap(tmp_path, monkeypatch, capsys):
    """Test CLI respects MEMPALACE_STORE_DIR and AI_WORKSPACE_DIR without explicit --store,
    and produces 100% portable topology on disk (Gate Falsifier #2)."""
    import json
    import re
    from pathlib import Path

    custom_ws = tmp_path / "custom_ws"
    custom_ws.mkdir()
    custom_store = tmp_path / "custom_store"

    monkeypatch.setenv("MEMPALACE_STORE_DIR", str(custom_store))
    monkeypatch.setenv("AI_WORKSPACE_DIR", str(custom_ws))

    # Bootstrap should pick up MEMPALACE_STORE_DIR and AI_WORKSPACE_DIR
    out = run_cli_args(["bootstrap"], monkeypatch, capsys)
    assert "初始化完成" in out.out

    # On disk: verify topology.json was created with clean relative paths and zero drive letters
    topo_disk_file = custom_store / "topology.json"
    assert topo_disk_file.exists()
    disk_content = topo_disk_file.read_text(encoding="utf-8")

    drive_pattern = re.compile(r'(?<![a-zA-Z0-9])[a-zA-Z]:[\\/](?!/)')
    assert not drive_pattern.search(disk_content), f"Found drive letter in bootstrapped topology: {disk_content}"

    disk_json = json.loads(disk_content)
    assert disk_json["ai-company"]["path"] == "ai-company"
    assert disk_json["mempalace"]["path"] == "mempalace"

    # Inspect should dynamically resolve nodes relative to custom_ws
    out_inspect = run_cli_args(["inspect"], monkeypatch, capsys)
    assert "Layer 2 專案拓撲地圖" in out_inspect.out
    assert str(custom_ws / "ai-company") in out_inspect.out





