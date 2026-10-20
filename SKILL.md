---
name: mempalace
description: Multi-Agent Hierarchical Memory Palace (Layer 1 Invariants, Layer 2 Topology, Layer 3 Event-Sourced Decision Stack) & Process Supervision. Use when managing multi-agent handoffs, inspecting cross-repo architecture topology, recording or popping short-term decisions, time-travel event replay, or supervising background task heartbeats and stall detection.
argument-hint: "[tree | ls [URI] | view <URI> | find <query> | context --level L0/L1/L2 | push-decision -t <topic> -v <verdict> -r <text> | pop-decision | record-dead-end -p <path> -r <text> | audit-log | replay --to-seq <N> | verify-integrity | check-health | bootstrap]"
disable-model-invocation: false
compatibility: "Windows / Linux / macOS | Python 3.10+"
---

# MemPalace — Hierarchical Multi-Agent Memory Palace & Deterministic Event Replay Skill

`mempalace` provides a three-layer virtual filesystem (`palace://`), cryptographic event-sourced decision tracking (`decision_events.jsonl`), tiered L0/L1/L2 context envelopes, and process heartbeat supervision for multi-agent workflows.

## Core Commands

```bash
# 1. Inspect virtual memory filesystem tree
mempalace tree

# 2. List and view nodes across layers
mempalace ls palace://rules --level L0
mempalace view palace://topology/mempalace --level L1

# 3. Extract tiered context envelopes (L0 ~50 tokens, L1 ~300 tokens, L2 full JSON)
mempalace context --level L0
mempalace context --level L1 --json

# 4. Push/Pop decisions & record dead-ends on the event-sourced stack
mempalace push-decision --topic "Feature X" --verdict "BUILD" --rationale "Passed acceptance criteria"
mempalace record-dead-end --path "Strategy A" --reason "O(2^N) complexity"
mempalace pop-decision -r "Completed"

# 5. Audit SHA-256 hash chain & time-travel replay
mempalace verify-integrity
mempalace audit-log
mempalace replay --to-seq 1

# 6. Supervise background task heartbeats
mempalace check-health
```

## Iron Rules (不可違反之鐵律)

1. **Append-Only Event Integrity (事件鏈防篡改鐵律)**：`decision_events.jsonl` 為唯一事實來源，不可手動刪改歷史事件列或破壞 SHA-256 哈希鏈連續性。
2. **Tiered Context Discipline (分級載入紀律)**：常規換棒與標頭注入優先使用 `L0` 或 `L1`，不可無故將完整 `L2` 原始倒印塞入日常對話造成 Context 膨脹。

## Validation Command (機械驗證指令)

```bash
pytest tests -q
```
