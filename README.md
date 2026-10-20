# MemPalace 🧠 — Hierarchical Multi-Agent Memory Palace & Deterministic Event Replay Engine

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Architecture: Event-Sourcing](https://img.shields.io/badge/Architecture-Event--Sourcing-purple.svg)](https://martinfowler.com/eaaDev/EventSourcing.html)

> A lightweight, zero-vector-database, file-system based memory architecture for autonomous multi-agent teams. Features **Three-Layer Memory Partitioning (`palace://`)**, **Cryptographic Event Sourcing (SHA-256 Hash Chain)**, **Tiered Context Envelopes (L0/L1/L2)**, and **Heartbeat Process Supervision**.

```
                         Virtual Filesystem: palace://
┌─────────────────────────────────────────────────────────────────────────────┐
│ Layer 1: Invariant Rules (palace://rules/)                                  │
│   • Core safety constraints, governance axioms, immutable review policies   │
├─────────────────────────────────────────────────────────────────────────────┤
│ Layer 2: Domain Topology (palace://topology/)                               │
│   • Workspace asset maps, API boundaries, verification recipes              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Layer 3: Event-Sourced Decision Stack (palace://decisions/)                 │
│   • Append-only decision_events.jsonl (SHA-256 hash chained, monotonic seq) │
│   • Active Stack (stack/), Dead-End Lessons (dead_ends/), History (history/) │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
            ┌─────────────────────────┴─────────────────────────┐
            ▼                                                   ▼
┌───────────────────────────────┐               ┌───────────────────────────────┐
│ Tiered Context Envelopes      │               │ Process Supervision           │
│   • L0 (~50 tokens, abstract) │               │   • Heartbeat monitoring      │
│   • L1 (~300 tokens, context) │               │   • Task stall/hang detection │
│   • L2 (Full JSON/Hash chain) │               │   • Fail-closed state engine  │
└───────────────────────────────┘               └───────────────────────────────┘
```

## Quick Start / Installation

```bash
git clone https://github.com/Keithsys730/mempalace.git
cd mempalace
python -m pip install -e ".[test]"

# Verify installation and run the 132-test offline suite
pytest -q

# Use the registered CLI directly (or via `python -m mempalace.cli`)
mempalace bootstrap
mempalace tree
mempalace context --level L0
```

---

# 中文說明與架構手冊

MemPalace 為多 Agent 協同作業提供 **三層式階層記憶 (Three-Layer Memory Palace)**、**確定性事件溯源回放 (Deterministic Event Replay)** 與 **進程心跳健康度監督 (Process Supervision)**。

## 🏛️ 記憶架構與虛擬檔案系統 (`palace://`)
1. **Layer 1: 不可變核心規約 (`palace://rules/`)**：治理不可變常數、安全門禁規則。
2. **Layer 2: 領域拓撲與資產地圖 (`palace://topology/`)**：各專案檔案結構、API 邊界與驗證配方。
3. **Layer 3: 確定性事件溯源決策棧 (`palace://decisions/`)**：
   - 以追加寫入 (Append-Only) 的 `decision_events.jsonl` 為單一事實來源 (Source of Truth)。
   - 具備 SHA-256 哈希鏈與單調序列號 (`seq_id`)，防篡改 (Fail-Closed)。
   - 支援 `stack/` 活躍棧、`dead_ends/` 避坑池與 `history/` 歷史歸檔。
4. **進程心跳健康度監督 (`palace://supervision/`)**：監督多 Agent 背景任務狀態機與超時假死。
5. **事件審計與溯源 (`palace://audit/`)**：防篡改時間序列事件日誌。

## ⚡ 三級 Context 分級加載 (Tiered Context Loading)
- **`L0` (Abstract)**：~30-80 Tokens，單句極簡摘要與統計指標，適合 Prompt 標頭常駐與快篩。
- **`L1` (Overview)**：~200-400 Tokens，結構化核心脈絡、規約與活躍決策，適合規劃與換棒。
- **`L2` (Detail)**：完整原始 JSON/Schema、完整假設、時間戳與哈希鏈。

## 💻 常用 CLI 指令

安裝後可直接使用 `mempalace` 命令（或 `python -m mempalace.cli`）：

```powershell
# 1. 檢視虛擬記憶宮殿目錄樹
mempalace tree

# 2. 巡檢虛擬目錄 (ls) 與檢視節點 (view)
mempalace ls palace://rules --level L0
mempalace view palace://topology/mempalace --level L1

# 3. 全層級遞迴檢索 (find)
mempalace find "mempalace"

# 4. 輸出分級 Context Package
mempalace context --level L0
mempalace context --level L1 --json

# 5. 壓入新決策 (發布 DECISION_PUSHED 事件)
mempalace push-decision -t "Feature A" -v "BUILD" -r "Passed test criteria"

# 6. 彈出決策並保留死胡同 (發布 DECISION_POPPED 事件)
mempalace pop-decision -r "Completed"

# 7. 記錄探索失敗的死胡同路徑 (發布 DEAD_END_RECORDED 事件)
mempalace record-dead-end -p "legacy_endpoint" -r "Deprecated in v2"

# 8. 檢視防篡改事件審計日誌
mempalace audit-log

# 9. 時空穿梭回放至指定事件點
mempalace replay --to-seq 1

# 10. 全量校驗事件鏈防篡改完整性
mempalace verify-integrity

# 11. 檢查背景任務健康度與假死監控 (Process Supervision)
mempalace check-health
```

---

## AI Agent Skill Integration (Claude Code / Codex / Gemini)

This repository includes a portable [SKILL.md](SKILL.md). Mount or copy it into ~/.claude/skills/mempalace/, ~/.codex/skills/mempalace/, or ~/.gemini/config/skills/mempalace/ so any AI coding agent can query palace:// memory envelopes and manage event-sourced decisions.
