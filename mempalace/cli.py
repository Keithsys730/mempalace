"""Command Line Interface for MemPalace with Deterministic Event Replay."""

import argparse
import sys
import os
import json
import time
from .core import MemoryPalace, HashIntegrityError, EventSequenceError, MemPalaceError, get_workspace_root
from .integrations import get_default_store_dir
from .supervisor import ProcessSupervisor
from .models import TaskStatus, ContextLevel


def main():
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(
        description="MemPalace - Multi-Agent Memory Palace & Deterministic Event Replay"
    )
    parser.add_argument("--store", "-s", default=None, help="Path to memory store directory")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # inspect
    subparsers.add_parser("inspect", help="Inspect all 3 memory layers and active handoff package")

    # context (tiered context)
    ctx_p = subparsers.add_parser("context", help="Output tiered context package (L0/L1/L2)")
    ctx_p.add_argument("--level", "-l", choices=["L0", "L1", "L2"], default="L1", help="Context tier level")
    ctx_p.add_argument("--json", action="store_true", help="Output raw JSON")

    # post-compact (context compaction recovery)
    post_compact_p = subparsers.add_parser("post-compact", help="Recover and re-inject MemPalace hot-cache context into a prompt/file after context compaction")
    post_compact_p.add_argument("--level", "-l", choices=["L0", "L1", "L2"], default="L1", help="Context tier level to re-inject")
    post_compact_p.add_argument("--text", "-t", default=None, help="Input prompt text")
    post_compact_p.add_argument("--file", "-f", default=None, help="Path to input text file to read and re-inject")
    post_compact_p.add_argument("--output", "-o", default=None, help="Optional path to write output file (defaults to stdout)")


    # ls (virtual filesystem list)
    ls_p = subparsers.add_parser("ls", help="List virtual directory contents (palace://...)")
    ls_p.add_argument("uri", nargs="?", default="palace://", help="Virtual URI to list (default: palace://)")
    ls_p.add_argument("--level", "-l", choices=["L0", "L1", "L2"], default="L0", help="Metadata detail level")
    ls_p.add_argument("--json", action="store_true", help="Output as JSON")

    # view (virtual filesystem view)
    view_p = subparsers.add_parser("view", help="View a specific palace:// URI node")
    view_p.add_argument("uri", help="Virtual URI to view (e.g. palace://rules/INV-001)")
    view_p.add_argument("--level", "-l", choices=["L0", "L1", "L2"], default="L1", help="Tiered detail level")
    view_p.add_argument("--json", action="store_true", help="Output as JSON")

    # tree (virtual filesystem tree)
    tree_p = subparsers.add_parser("tree", help="Display ASCII tree of virtual memory filesystem")
    tree_p.add_argument("--uri", "-u", default="palace://", help="Root URI to render tree from")

    # find (virtual filesystem find)
    find_p = subparsers.add_parser("find", help="Search across all memory layers for matching tokens/phrases")
    find_p.add_argument("query", help="Search query string")
    find_p.add_argument("--level", "-l", choices=["L0", "L1", "L2"], default="L1", help="Detail level")
    find_p.add_argument("--json", action="store_true", help="Output as JSON")

    # bootstrap
    subparsers.add_parser("bootstrap", help="Populate Layer 1 & 2 with current workspace defaults")

    # push-decision
    push_p = subparsers.add_parser("push-decision", help="Push a decision frame onto the Layer 3 stack (emits DECISION_PUSHED)")
    push_p.add_argument("--topic", "-t", required=True, help="Topic of the decision")
    push_p.add_argument("--verdict", "-v", required=True, help="Verdict (BUILD/EVALUATE/REVIEW/REJECT/ESCALATE)")
    push_p.add_argument("--rationale", "-r", required=True, help="Rationale description")
    push_p.add_argument("--actor", "-a", default="orchestrator", help="Deciding agent or actor")

    # pop-decision
    pop_p = subparsers.add_parser("pop-decision", help="Pop the top decision frame from the Layer 3 stack (emits DECISION_POPPED)")
    pop_p.add_argument("--reason", "-r", default="Popped decision", help="Reason for popping decision")
    pop_p.add_argument("--actor", "-a", default="orchestrator", help="Actor popping the decision")

    # record-dead-end
    de_p = subparsers.add_parser("record-dead-end", help="Record an explored dead-end to prevent repeating mistakes")
    de_p.add_argument("--path", "-p", required=True, help="Description or path of the dead end")
    de_p.add_argument("--reason", "-r", default="", help="Reason why this path failed or is invalid")

    # audit-log
    audit_p = subparsers.add_parser("audit-log", help="Display tamper-evident chronological event log and hash chain")
    audit_p.add_argument("--limit", "-n", type=int, default=20, help="Maximum number of recent events to display")

    # replay (time-travel)
    replay_p = subparsers.add_parser("replay", help="Time-travel query: replay events to reconstruct state at a given sequence number")
    replay_p.add_argument("--to-seq", "-t", type=int, default=None, help="Target sequence number to replay up to")
    replay_p.add_argument("--json", action="store_true", help="Output replayed state as JSON")

    # verify-integrity
    subparsers.add_parser("verify-integrity", help="Verify SHA-256 hash chain and sequence monotonicity from genesis to tip")

    # repair
    subparsers.add_parser("repair", help="Repair and salvage event log by truncating corrupted/incomplete EOF line")

    # supervisor register-task
    reg_p = subparsers.add_parser("register-task", help="Register a background task with the supervisor")
    reg_p.add_argument("--task-id", required=True, help="Unique task identifier")
    reg_p.add_argument("--role", required=True, help="Role of the agent/process")
    reg_p.add_argument("--timeout", type=float, default=120.0, help="Heartbeat timeout in seconds")
    reg_p.add_argument("--pid", type=int, default=None, help="Process ID")

    # supervisor record-heartbeat
    hb_p = subparsers.add_parser("record-heartbeat", help="Record heartbeat for a task")
    hb_p.add_argument("--task-id", required=True, help="Task identifier")
    hb_p.add_argument("--log", "-l", default="", help="Recent log snippet")
    hb_p.add_argument("--status", default="running", choices=["running", "idle", "stalled", "completed", "failed"], help="Task status")
    hb_p.add_argument("--epoch", type=int, default=None, help="Progress epoch")

    # supervisor check-health
    subparsers.add_parser("check-health", help="Check background task health and detect stalled processes")

    args = parser.parse_args()

    resolved_store = get_default_store_dir(args.store)
    if args.command == "repair":
        palace = MemoryPalace(store_dir=resolved_store, repair_truncated_tail=True)
        print("✅ MemPalace 事件日誌已檢驗並完成修復！")
        print(f"🔢 當前有效事件數: {len(palace.events)} (Seq #{palace.state.current_seq})")
        return

    try:
        palace = MemoryPalace(store_dir=resolved_store)
    except (HashIntegrityError, EventSequenceError, MemPalaceError) as init_err:
        if args.command == "verify-integrity":
            print(f"🔍 正在驗證 MemPalace 全量事件鏈完整性與防篡改簽名...")
            print(f"❌ 完整性校驗失敗 (CORRUPTED / TAMPERED): {init_err}")
            sys.exit(1)
        raise

    supervisor = ProcessSupervisor(store_dir=resolved_store)

    if args.command == "bootstrap":
        # Layer 1: Core Invariants
        palace.add_immutable_rule("INV-001", "Governance", "Fail-closed on all unprovable state checks.")
        palace.add_immutable_rule("INV-002", "Git Safety", "Block destructive git push/merge/pull in subagents.")
        palace.add_immutable_rule("INV-003", "Review Gate", "Production code requires deterministic CLEAN review marker.")

        # Layer 2: Project Topology
        palace.register_topology_node("ai-company", "ai-company", "repo", ["dev-team", "front-desk"], "pytest")
        palace.register_topology_node("knowledge-island", "knowledge-island", "repo", ["knowledge", "graph"], "pytest tests -v")
        palace.register_topology_node("github-scout", "github-scout", "repo", ["radar", "idea scout"], "pytest tests -v")
        palace.register_topology_node("prompt-pin", "prompt-pin", "repo", ["proxy", "slot pin"], "pytest tests -v")
        palace.register_topology_node("mempalace", "mempalace", "repo", ["event replay", "supervisor"], "pytest tests -v")

        print("✅ MemPalace 預設規約與專案拓撲初始化完成！")
        return

    elif args.command == "push-decision":
        frame = palace.push_decision(
            topic=args.topic,
            verdict=args.verdict,
            rationale=args.rationale,
            decided_by=args.actor
        )
        latest_event = palace.events[-1]
        print(f"✅ 決策已成功發布事件並壓入 Layer 3 決策棧！")
        print(f"🔢 Event Seq: #{latest_event.seq_id} ({latest_event.event_id})")
        print(f"🔒 Event Hash: {latest_event.event_hash[:16]}... (Prev: {latest_event.prev_event_hash[:16]}...)")
        print(f"🆔 Frame ID: {frame.frame_id}")
        print(f"🎯 主題: {frame.topic} [{frame.verdict}]")
        print(f"💡 理由: {frame.rationale}")
        return

    elif args.command == "pop-decision":
        frame = palace.pop_decision(reason=args.reason, actor=args.actor)
        if frame:
            latest_event = palace.events[-1]
            print(f"✅ 決策已成功彈出並發布 DECISION_POPPED 事件！")
            print(f"🔢 Event Seq: #{latest_event.seq_id} ({latest_event.event_id})")
            print(f"🔒 Event Hash: {latest_event.event_hash[:16]}...")
            print(f"🆔 彈出 Frame: {frame.frame_id} ({frame.topic})")
            print(f"📚 當前棧深度: {len(palace.decision_stack)} 層")
        else:
            print("⚠️ 決策棧目前為空。")
        return

    elif args.command == "record-dead-end":
        palace.record_dead_end(path=args.path, reason=args.reason)
        latest_event = palace.events[-1]
        print(f"🚫 死胡同路徑已記入事件日誌！")
        print(f"🔢 Event Seq: #{latest_event.seq_id}")
        print(f"📍 路徑: {args.path}")
        print(f"💡 原因: {args.reason}")
        return

    elif args.command == "audit-log":
        events = palace.events[-args.limit:] if args.limit else palace.events
        print(f"📜 MemPalace 決策事件審計日誌 (Chronological Audit Log)")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if not events:
            print("（暫無任何事件）")
            return

        for e in events:
            ev_type = e.event_type.value if hasattr(e.event_type, "value") else str(e.event_type)
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.wall_time))
            print(f"[{e.seq_id:04d}] {time_str} | {ev_type:<20} | By: {e.actor}")
            print(f"       Hash: {e.event_hash[:16]}... (Prev: {e.prev_event_hash[:16]}...)")
            if ev_type == "DECISION_PUSHED":
                p = e.payload
                print(f"       Payload -> [{p.get('frame_id')}] {p.get('topic')} [{p.get('verdict')}]: {p.get('rationale')}")
            elif ev_type == "DECISION_POPPED":
                p = e.payload
                print(f"       Payload -> Popped: {p.get('popped_frame_id')} ({p.get('topic')}), Reason: {p.get('reason')}")
            elif ev_type == "DEAD_END_RECORDED":
                p = e.payload
                print(f"       Payload -> Dead End: {p.get('path')} ({p.get('reason')})")
            print("       " + "─" * 60)
        print(f"總事件數: {len(palace.events)} 筆 (展示最近 {len(events)} 筆)")
        return

    elif args.command == "replay":
        to_seq = args.to_seq
        target_seq_label = f"#{to_seq}" if to_seq is not None else "Tip"
        replayed_state = palace.replay(to_seq=to_seq)

        if args.json:
            print(json.dumps(replayed_state.model_dump(), ensure_ascii=False, indent=2))
            return

        print(f"⏳ MemPalace 時空穿梭回放 (Time-Travel Replay to {target_seq_label})")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"🎯 重放終點 Seq: #{replayed_state.current_seq}")
        print(f"🔒 終點 Hash: {replayed_state.latest_hash}")
        print(f"📚 當時活躍決策棧 ({len(replayed_state.active_stack)} 層):")
        for f in replayed_state.active_stack:
            print(f"  • [{f.frame_id}] {f.topic} -> {f.verdict}: {f.rationale}")
        print(f"\n🚫 累積死胡同池 ({len(replayed_state.dead_ends_pool)} 個):")
        for de in replayed_state.dead_ends_pool:
            print(f"  • {de.get('path')} ({de.get('reason')})")
        print(f"\n📦 歷史彈出歸檔 ({len(replayed_state.history_archive)} 個):")
        for h in replayed_state.history_archive:
            print(f"  • [{h.frame_id}] {h.topic} (Popped)")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return

    elif args.command == "verify-integrity":
        print(f"🔍 正在驗證 MemPalace 全量事件鏈完整性與防篡改簽名...")
        try:
            res = palace.verify_chain_integrity()
            print(f"✅ 事件鏈完整性驗證通過 (CLEAN)！")
            print(f"🔢 總校驗事件數: {res['total_events']}")
            print(f"🔒 末端校驗 Hash: {res['tip_hash']}")
        except (HashIntegrityError, EventSequenceError, MemPalaceError) as err:
            print(f"❌ 完整性校驗失敗 (CORRUPTED / TAMPERED): {err}")
            sys.exit(1)
        return

    elif args.command == "register-task":
        hb = supervisor.register_task(
            task_id=args.task_id,
            role=args.role,
            timeout_sec=args.timeout,
            pid=args.pid
        )
        print(f"✅ 任務已向監督器註冊: [{hb.task_id}] Role: {hb.role} (Timeout: {hb.timeout_seconds}s, PID: {hb.pid})")
        return

    elif args.command == "record-heartbeat":
        supervisor.record_heartbeat(
            task_id=args.task_id,
            log_snippet=args.log,
            status=TaskStatus(args.status),
            progress_epoch=args.epoch
        )
        print(f"💓 任務 [{args.task_id}] 心跳記錄已更新 (Status: {args.status})")
        return

    elif args.command == "check-health":
        health = supervisor.check_health()
        print(f"💓 背景進程健康度檢查 (Process Supervision)")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"總監控任務數: {health['total_tasks']} (正常: {health['healthy_count']}, 假死: {health['stalled_count']}, 失敗: {health['failed_count']}, 完成: {health['completed_count']})")
        if health["stalled_tasks"]:
            print(f"\n⚠️ 假死任務 (Stalled Tasks):")
            for t in health["stalled_tasks"]:
                print(f"  • [{t['task_id']}] {t['role']} - 逾時 {t['elapsed_sec']}s (設定: {t['timeout_sec']}s) | PID: {t.get('pid')}")
        if health["failed_tasks"]:
            print(f"\n❌ 失敗任務 (Failed Tasks):")
            for t in health["failed_tasks"]:
                print(f"  • [{t['task_id']}] {t['role']} - Log: {t.get('last_log', '')} | PID: {t.get('pid')}")
        return

    elif args.command == "context":
        level = ContextLevel(args.level)
        pkg = palace.get_tiered_context(level=level)
        if args.json:
            print(json.dumps(pkg, ensure_ascii=False, indent=2))
            return

        print(f"🧠 MemPalace Context Package (Tier: {level.value})")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if level == ContextLevel.L0:
            print(f"📊 Summary: {pkg.get('summary')}")
            print(f"🎯 Tip Decision: {pkg.get('tip_decision')}")
            print(f"🔒 Latest Hash Prefix: {pkg.get('latest_hash_prefix')}")
        elif level == ContextLevel.L1:
            print(f"🔒 Layer 1 不可變核心規約 ({len(pkg['immutable_rules'])} 條):")
            for r in pkg["immutable_rules"]:
                print(f"  • [{r['rule_id']}] ({r['category']}) {r['description']}")
            print(f"\n🗺️ Layer 2 專案拓撲地圖 ({len(pkg['topology'])} 個):")
            for t in pkg["topology"]:
                print(f"  • [{t['name']}] {t['path']} (Verify: {t['verification_command']})")
            print(f"\n📚 Layer 3 活躍短期決策棧 ({len(pkg['active_decision_stack'])} 層):")
            for d in pkg["active_decision_stack"]:
                print(f"  • [{d['frame_id']}] {d['topic']} -> {d['verdict']}: {d['rationale']}")
            if pkg.get("dead_ends_pool"):
                print(f"\n🚫 Layer 3 累積死胡同避坑池 ({len(pkg['dead_ends_pool'])} 個):")
                for de in pkg["dead_ends_pool"]:
                    print(f"  • {de.get('path')} ({de.get('reason')})")
            lineage = pkg.get("event_lineage", {})
            print(f"\n⚡ Event Lineage: Seq #{lineage.get('current_seq', 0)} | Total Events: {lineage.get('total_events', 0)} | Tip Hash: {lineage.get('latest_hash', '')[:16]}...")
        else:
            print(json.dumps(pkg, ensure_ascii=False, indent=2))
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return

    elif args.command == "post-compact":
        from .integrations import handle_post_compact
        content = ""
        if args.file:
            if not os.path.exists(args.file):
                print(f"❌ 輸入檔案不存在: {args.file}", file=sys.stderr)
                sys.exit(1)
            try:
                with open(args.file, "r", encoding="utf-8") as f:
                    content = f.read()
            except Exception as e:
                print(f"❌ 讀取輸入檔案失敗: {e}", file=sys.stderr)
                sys.exit(1)
        elif args.text is not None:
            content = args.text
        else:
            if not sys.stdin.isatty():
                try:
                    content = sys.stdin.read()
                except Exception as e:
                    print(f"❌ 讀取標準輸入失敗: {e}", file=sys.stderr)
                    sys.exit(1)
            else:
                content = ""

        level = ContextLevel(args.level)
        result = handle_post_compact(text_or_prompt=content, level=level, palace=palace)

        if args.output:
            try:
                out_dir = os.path.dirname(args.output)
                if out_dir and not os.path.exists(out_dir):
                    os.makedirs(out_dir, exist_ok=True)
                with open(args.output, "w", encoding="utf-8") as f:
                    f.write(result)
                print(f"✅ 已將 PostCompact 重注入內容寫入至: {args.output}")
            except Exception as e:
                print(f"❌ 寫入輸出檔案失敗: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            print(result)
        return

    elif args.command == "ls":
        level = ContextLevel(args.level)
        try:
            items = palace.ls(uri=args.uri, level=level)
        except KeyError as err:
            print(f"❌ 目錄路徑不存在: {err}")
            sys.exit(1)

        if args.json:
            print(json.dumps(items, ensure_ascii=False, indent=2))
            return

        norm = palace._normalize_uri(args.uri)
        print(f"📂 MemPalace 虛擬目錄巡檢: {norm} ({len(items)} items)")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        for item in items:
            node_type = item.get("node_type", "node")
            uri = item.get("uri", "")
            summary = item.get("summary", "")
            print(f"  • [{node_type:<10}] {uri:<35} | {summary}")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return

    elif args.command == "view":
        level = ContextLevel(args.level)
        try:
            obj = palace.resolve_uri(uri=args.uri, level=level)
        except KeyError as err:
            print(f"❌ 節點不存在: {err}")
            sys.exit(1)

        if args.json:
            print(json.dumps(obj, ensure_ascii=False, indent=2))
            return

        norm = palace._normalize_uri(args.uri)
        print(f"📄 MemPalace 節點內容: {norm} (Tier: {level.value})")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if isinstance(obj, (dict, list)):
            print(json.dumps(obj, ensure_ascii=False, indent=2))
        else:
            print(str(obj))
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return

    elif args.command == "tree":
        print(palace.tree(uri=args.uri))
        return

    elif args.command == "find":
        level = ContextLevel(args.level)
        matches = palace.find(query=args.query, level=level)
        if args.json:
            print(json.dumps(matches, ensure_ascii=False, indent=2))
            return

        print(f"🔍 MemPalace 檢索 '{args.query}' (共找到 {len(matches)} 筆相符項目)")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if not matches:
            print("（無相符結果）")
            return

        for idx, m in enumerate(matches, 1):
            print(f"[{idx}] {m['uri']} ({m['node_type']} / {m['match_field']})")
            print(f"    摘要: {m['summary']}")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return

    elif args.command == "inspect" or args.command is None:
        pkg = palace.get_active_context_package()
        print(f"🏛️ MemPalace 階層式記憶狀態檢視 (with Deterministic Event Replay)")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"🔒 Layer 1 不可變核心規約 ({len(pkg['immutable_rules'])} 條):")
        for r in pkg["immutable_rules"]:
            print(f"  • {r}")
        print(f"\n🗺️ Layer 2 專案拓撲地圖 ({len(pkg['topology'])} 個):")
        for t in pkg["topology"]:
            print(f"  • {t}")
        print(f"\n📚 Layer 3 活躍短期決策棧 ({len(pkg['active_decision_stack'])} 層):")
        for d in pkg["active_decision_stack"]:
            print(f"  • [{d['frame_id']}] {d['topic']} -> {d['verdict']}: {d['rationale']}")
        if pkg.get("dead_ends_pool"):
            print(f"\n🚫 Layer 3 累積死胡同避坑池 ({len(pkg['dead_ends_pool'])} 個):")
            for de in pkg["dead_ends_pool"]:
                print(f"  • {de.get('path')} ({de.get('reason')})")
        lineage = pkg.get("event_lineage", {})
        print(f"\n⚡ Event Lineage: Seq #{lineage.get('current_seq', 0)} | Total Events: {lineage.get('total_events', 0)} | Tip Hash: {lineage.get('latest_hash', '')[:16]}...")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

if __name__ == "__main__":
    main()


