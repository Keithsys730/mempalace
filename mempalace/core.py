"""Core Hierarchical Memory Palace Manager with Deterministic Event Replay."""

import os
import json
import time
import copy
import uuid
import tempfile
from pathlib import Path
import filelock
from typing import Dict, List, Optional, Any
from .models import (
    ImmutableRule,
    TopologyNode,
    DecisionFrame,
    EventType,
    DecisionEvent,
    DecisionStackState,
    ContextLevel,
    PalaceNode,
    ProcessHeartbeat
)


class MemPalaceError(Exception):
    """Base exception for MemPalace errors."""
    pass


class HashIntegrityError(MemPalaceError):
    """Raised when an event's hash chain or payload hash validation fails."""
    pass


class EventSequenceError(MemPalaceError):
    """Raised when an event's sequence id does not match expected order."""
    pass


class EventSemanticError(MemPalaceError):
    """Raised when an event semantic precondition is violated."""
    pass


class RuleConflictError(MemPalaceError):
    """Raised when attempting to overwrite an immutable Layer 1 rule."""
    pass


def get_workspace_root() -> Path:
    """Resolve the active workspace root directory dynamically.

    Resolution order:
    1. Explicit AI_WORKSPACE_DIR environment variable
    2. Explicit MEMPALACE_WORKSPACE_DIR environment variable
    3. Dynamic parent anchor: Path(__file__).resolve().parents[2]
    """
    ai_ws = (os.environ.get("AI_WORKSPACE_DIR") or "").strip()
    mp_ws = (os.environ.get("MEMPALACE_WORKSPACE_DIR") or "").strip()
    env_ws = ai_ws or mp_ws
    if env_ws:
        return Path(env_ws).resolve()
    return Path(__file__).resolve().parents[2]


def _dump_topology_portable(topology: Dict[str, TopologyNode], ws_root: Path) -> Dict[str, Any]:
    """Serialize topology dictionary converting paths located under ws_root to portable relative paths."""
    dumped = {}
    ws_root_resolved = ws_root.resolve()
    for k, node in topology.items():
        data = node.model_dump()
        raw_path = node.path
        if raw_path:
            if not os.path.isabs(raw_path):
                data["path"] = raw_path.replace("\\", "/")
            else:
                try:
                    p = Path(raw_path).resolve()
                    if p.is_relative_to(ws_root_resolved):
                        rel = p.relative_to(ws_root_resolved)
                        data["path"] = str(rel).replace("\\", "/")
                except (ValueError, TypeError, RuntimeError):
                    pass
        dumped[k] = data
    return dumped


def _atomic_write_json(filepath: str, data: Any, indent: int = 2):
    """Write JSON data to a file atomically using a temporary file in the same directory."""
    dir_name = os.path.dirname(os.path.abspath(filepath))
    os.makedirs(dir_name, exist_ok=True)
    prefix = f".{os.path.basename(filepath)}.tmp_"
    fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix=prefix)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, filepath)
    except Exception:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise


def apply_event(state: DecisionStackState, event: DecisionEvent, verify_hash: bool = True) -> DecisionStackState:
    """Pure state transition function applying a single event to DecisionStackState.

    Args:
        state: Current state snapshot.
        event: DecisionEvent to apply.
        verify_hash: If True, asserts sequence id, hash chain integrity, and payload hash.

    Returns:
        New DecisionStackState reflecting the applied event.
    """
    if verify_hash:
        expected_seq = state.current_seq + 1
        if event.seq_id != expected_seq:
            raise EventSequenceError(
                f"Event sequence gap: expected {expected_seq}, got {event.seq_id} (event_id={event.event_id})"
            )
        if event.prev_event_hash != state.latest_hash:
            raise HashIntegrityError(
                f"Hash chain mismatch at seq {event.seq_id}: "
                f"event.prev_event_hash='{event.prev_event_hash}' != state.latest_hash='{state.latest_hash}'"
            )
        expected_hash = event.compute_hash()
        if event.event_hash != expected_hash:
            raise HashIntegrityError(
                f"Event payload tampering detected at seq {event.seq_id}: "
                f"computed hash '{expected_hash}' != event.event_hash '{event.event_hash}'"
            )

    new_state = state.model_copy(deep=True)
    new_state.current_seq = event.seq_id
    new_state.latest_hash = event.event_hash

    payload = copy.deepcopy(event.payload)
    event_type_str = event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type)

    if event_type_str == EventType.DECISION_PUSHED.value:
        frame_id = payload.get("frame_id") or f"frame_{uuid.uuid4().hex[:8]}"
        frame = DecisionFrame(
            frame_id=frame_id,
            topic=payload.get("topic", ""),
            verdict=payload.get("verdict", ""),
            rationale=payload.get("rationale", ""),
            decided_by=payload.get("decided_by", event.actor),
            assumptions=payload.get("assumptions", []),
            dead_ends_explored=payload.get("dead_ends_explored", []),
            pending_questions=payload.get("pending_questions", []),
            timestamp=payload.get("timestamp", event.wall_time)
        )
        new_state.active_stack.append(frame)

    elif event_type_str == EventType.DECISION_POPPED.value:
        if not new_state.active_stack:
            raise EventSemanticError(
                f"Cannot apply DECISION_POPPED at seq {event.seq_id}: decision stack is empty"
            )

        expected_popped_id = payload.get("popped_frame_id")
        if expected_popped_id and new_state.active_stack[-1].frame_id != expected_popped_id:
            raise EventSemanticError(
                f"Popped frame mismatch at seq {event.seq_id}: "
                f"expected frame_id '{expected_popped_id}', but top of stack is '{new_state.active_stack[-1].frame_id}'"
            )

        popped = new_state.active_stack.pop()
        if popped.dead_ends_explored:
            for de in popped.dead_ends_explored:
                if not any(d.get("path") == de for d in new_state.dead_ends_pool):
                    new_state.dead_ends_pool.append({
                        "path": de,
                        "popped_frame_id": popped.frame_id,
                        "topic": popped.topic,
                        "reason": payload.get("reason", "Popped decision")
                    })
        new_state.history_archive.append(popped)

    elif event_type_str == EventType.DEAD_END_RECORDED.value:
        new_state.dead_ends_pool.append(payload)
        if new_state.active_stack:
            path = payload.get("path", "")
            if path and path not in new_state.active_stack[-1].dead_ends_explored:
                new_state.active_stack[-1].dead_ends_explored.append(path)

    elif event_type_str == EventType.ASSUMPTION_FALSIFIED.value:
        target_frame_id = payload.get("frame_id")
        falsified = payload.get("assumption")
        found = False
        for f in new_state.active_stack:
            if (not target_frame_id or f.frame_id == target_frame_id) and falsified in f.assumptions:
                f.assumptions.remove(falsified)
                found = True
        if not found:
            raise EventSemanticError(
                f"Assumption '{falsified}' not found in active stack (target: {target_frame_id}) at seq {event.seq_id}"
            )

    elif event_type_str == EventType.DECISION_AMENDED.value:
        target_frame_id = payload.get("frame_id")
        matched = False
        for f in new_state.active_stack:
            if f.frame_id == target_frame_id:
                if "verdict" in payload:
                    f.verdict = payload["verdict"]
                if "rationale" in payload:
                    f.rationale = payload["rationale"]
                matched = True
        if not matched:
            raise EventSemanticError(
                f"Target frame '{target_frame_id}' not found in active stack for amendment at seq {event.seq_id}"
            )

    elif event_type_str == EventType.SNAPSHOT_TAKEN.value:
        pass

    return new_state


def replay_events(
    events: List[DecisionEvent],
    to_seq: Optional[int] = None,
    initial_state: Optional[DecisionStackState] = None,
    verify_hash: bool = True
) -> DecisionStackState:
    """Deterministically replay a list of events from an initial state."""
    state = initial_state.model_copy(deep=True) if initial_state else DecisionStackState()
    for event in events:
        if to_seq is not None and event.seq_id > to_seq:
            break
        state = apply_event(state, event, verify_hash=verify_hash)
    return state


class MemoryPalace:
    """Manages 3-Layer Hierarchical Memory with Deterministic Event Replay."""

    def __init__(self, store_dir: str = "data/memory_store", snapshot_interval: Optional[int] = 100):
        if snapshot_interval is not None and snapshot_interval <= 0:
            raise ValueError("snapshot_interval must be a positive integer or None")

        self.store_dir = store_dir
        self.snapshot_interval = snapshot_interval
        os.makedirs(self.store_dir, exist_ok=True)
        os.makedirs(self.snapshots_dir, exist_ok=True)

        self.lock_file = os.path.join(self.store_dir, ".mempalace.lock")
        self._lock = filelock.FileLock(self.lock_file, timeout=15.0)

        self._immutable_rules: Dict[str, ImmutableRule] = {}
        self._topology: Dict[str, TopologyNode] = {}
        self.events: List[DecisionEvent] = []
        self.state: DecisionStackState = DecisionStackState()

        with self._lock:
            self.load_all()

    @property
    def snapshots_dir(self) -> str:
        return os.path.join(self.store_dir, "snapshots")

    @property
    def events_file(self) -> str:
        return os.path.join(self.store_dir, "decision_events.jsonl")

    @property
    def immutable_rules(self) -> Dict[str, ImmutableRule]:
        """Deep copy of Layer 1 immutable rules to prevent external mutation."""
        return {k: v.model_copy(deep=True) for k, v in self._immutable_rules.items()}

    @property
    def topology(self) -> Dict[str, TopologyNode]:
        """Deep copy of Layer 2 topology to prevent external mutation."""
        return {k: v.model_copy(deep=True) for k, v in self._topology.items()}

    @property
    def decision_stack(self) -> List[DecisionFrame]:
        """Deep copy of Layer 3 active stack to prevent external mutation."""
        return [x.model_copy(deep=True) for x in self.state.active_stack]

    @property
    def dead_ends_pool(self) -> List[Dict[str, Any]]:
        """Deep copy of the cumulative dead-ends pool to prevent external mutation."""
        return copy.deepcopy(self.state.dead_ends_pool)

    def _file_path(self, name: str) -> str:
        return os.path.join(self.store_dir, f"{name}.json")

    def _load_immutable_rules(self):
        p1 = self._file_path("immutable_rules")
        if os.path.exists(p1):
            try:
                with open(p1, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._immutable_rules = {k: ImmutableRule(**v) for k, v in data.items()}
            except Exception as e:
                raise MemPalaceError(f"Fail-closed: Failed to load immutable rules from {p1}: {e}") from e
        else:
            self._immutable_rules = {}

    def _load_topology(self):
        p2 = self._file_path("topology")
        if os.path.exists(p2):
            try:
                with open(p2, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    ws_root = get_workspace_root()
                    self._topology = {}
                    for k, v in data.items():
                        node_path = v.get("path") or ""
                        node_name = v.get("name") or k
                        candidate = ws_root / node_name
                        if not os.path.isabs(node_path):
                            rel_candidate = ws_root / node_path
                            if candidate.exists() and not rel_candidate.exists():
                                v["path"] = str(candidate)
                            else:
                                v["path"] = str(rel_candidate)
                        elif not os.path.exists(node_path) and candidate.exists():
                            v["path"] = str(candidate)
                        self._topology[k] = TopologyNode(**v)
            except Exception as e:
                raise MemPalaceError(f"Fail-closed: Failed to load topology from {p2}: {e}") from e
        else:
            self._topology = {}

    def load_all(self):
        """Load Layer 1, Layer 2, and Layer 3 (via Deterministic Event Replay / Snapshots)."""
        # 1. Load Layer 1: Immutable Rules
        self._load_immutable_rules()

        # 2. Load Layer 2: Topology
        self._load_topology()

        # 3. Load Layer 3: Deterministic Event Stream & Snapshots
        self.events = self._read_event_log()

        if self.events:
            # Check for latest valid snapshot for fast-forward replay
            snapshot_state, snapshot_seq = self._find_latest_valid_snapshot(len(self.events))
            if snapshot_state is not None:
                # Replay only events after snapshot_seq
                remaining_events = [e for e in self.events if e.seq_id > snapshot_seq]
                self.state = replay_events(remaining_events, initial_state=snapshot_state, verify_hash=True)
            else:
                self.state = replay_events(self.events, verify_hash=True)
            self._sync_materialized_stack()
        elif os.path.exists(self._file_path("decision_stack")):
            # Smooth bootstrap/migration from legacy snapshot
            self._migrate_legacy_decision_stack()
        else:
            self.state = DecisionStackState()

    def _find_latest_valid_snapshot(self, max_seq: int) -> tuple[Optional[DecisionStackState], int]:
        """Find the latest valid snapshot that matches event log hash at that sequence."""
        if not os.path.exists(self.snapshots_dir):
            return None, 0

        snapshot_files = sorted(
            [f for f in os.listdir(self.snapshots_dir) if f.startswith("snapshot_seq_") and f.endswith(".json")],
            reverse=True
        )

        for sfile in snapshot_files:
            try:
                spath = os.path.join(self.snapshots_dir, sfile)
                with open(spath, "r", encoding="utf-8") as f:
                    sdata = json.load(f)
                seq = sdata.get("snapshot_seq")
                tip_hash = sdata.get("latest_hash")
                if seq is not None and seq <= max_seq and seq <= len(self.events):
                    matching_event = self.events[seq - 1]
                    if matching_event.seq_id == seq and matching_event.event_hash == tip_hash:
                        loaded_state = DecisionStackState(**sdata["state"])
                        return loaded_state, seq
            except Exception:
                continue

        return None, 0

    def _read_event_log(self) -> List[DecisionEvent]:
        """Read all decision events from the append-only JSONL log."""
        if not os.path.exists(self.events_file):
            return []
        events = []
        with open(self.events_file, "r", encoding="utf-8") as f:
            for line_idx, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    events.append(DecisionEvent(**data))
                except Exception as e:
                    raise MemPalaceError(f"Failed to parse event line {line_idx} in {self.events_file}: {line}") from e
        return events

    def _append_event_to_file(self, event: DecisionEvent):
        """Durable fsync append of a single event to the JSONL log."""
        with open(self.events_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event.model_dump(), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _commit_event(
        self,
        event_type: EventType,
        actor: str,
        payload: Dict[str, Any],
        wall_time: Optional[float] = None
    ) -> DecisionEvent:
        """Hold cross-process lock, re-read on-disk tip, construct, append durably, and update state."""
        with self._lock:
            disk_events = self._read_event_log()
            current_state = replay_events(disk_events, verify_hash=True) if disk_events else DecisionStackState()

            seq_id = current_state.current_seq + 1
            prev_event_hash = current_state.latest_hash
            w_time = wall_time if wall_time is not None else time.time()
            event_id = f"evt_{int(w_time * 1000)}_{uuid.uuid4().hex[:8]}"

            event = DecisionEvent(
                seq_id=seq_id,
                event_id=event_id,
                event_type=event_type,
                actor=actor,
                payload=payload,
                wall_time=w_time,
                prev_event_hash=prev_event_hash
            )

            new_state = apply_event(current_state, event, verify_hash=True)
            self._append_event_to_file(event)

            self.events = disk_events + [event]
            self.state = new_state
            self._sync_materialized_stack()

            if self.snapshot_interval and self.state.current_seq % self.snapshot_interval == 0:
                self.take_snapshot()

            return event

    def _sync_materialized_stack(self):
        """Write out the current active stack to decision_stack.json atomically."""
        _atomic_write_json(
            self._file_path("decision_stack"),
            [x.model_dump() for x in self.state.active_stack]
        )

    def _migrate_legacy_decision_stack(self):
        """Migrate legacy decision_stack.json into genesis event log."""
        try:
            with open(self._file_path("decision_stack"), "r", encoding="utf-8") as f:
                data = json.load(f)
            frames = [DecisionFrame(**x) for x in data]
        except Exception:
            frames = []

        self.state = DecisionStackState()
        self.events = []

        for frame in frames:
            seq_id = self.state.current_seq + 1
            event = DecisionEvent(
                seq_id=seq_id,
                event_id=f"evt_bootstrap_{seq_id:04d}",
                event_type=EventType.DECISION_PUSHED,
                actor=frame.decided_by,
                payload=frame.model_dump(),
                wall_time=frame.timestamp,
                prev_event_hash=self.state.latest_hash
            )
            self._append_event_to_file(event)
            self.events.append(event)
            self.state = apply_event(self.state, event, verify_hash=True)

        self._sync_materialized_stack()

    def save_all(self):
        """Save Layer 1 and Layer 2 rules/topology atomically under lock."""
        with self._lock:
            _atomic_write_json(
                self._file_path("immutable_rules"),
                {k: v.model_dump() for k, v in self._immutable_rules.items()}
            )
            ws_root = get_workspace_root()
            _atomic_write_json(
                self._file_path("topology"),
                _dump_topology_portable(self._topology, ws_root)
            )
            self._sync_materialized_stack()

    # Layer 1 Operations
    def add_immutable_rule(
        self,
        rule_id: str,
        category: str,
        description: str,
        enforcement_type: str = "fail-closed"
    ) -> ImmutableRule:
        with self._lock:
            self._load_immutable_rules()
            if rule_id in self._immutable_rules:
                raise RuleConflictError(
                    f"Immutable rule '{rule_id}' already exists and cannot be overwritten."
                )

            rule = ImmutableRule(
                rule_id=rule_id,
                category=category,
                description=description,
                enforcement_type=enforcement_type
            )
            self._immutable_rules[rule_id] = rule
            _atomic_write_json(
                self._file_path("immutable_rules"),
                {k: v.model_dump() for k, v in self._immutable_rules.items()}
            )
            return rule.model_copy(deep=True)

    # Layer 2 Operations
    def register_topology_node(
        self,
        name: str,
        path: str,
        node_type: str = "repo",
        key_apis: List[str] = None,
        verification_cmd: str = None
    ) -> TopologyNode:
        with self._lock:
            self._load_topology()
            node = TopologyNode(
                name=name,
                path=path,
                node_type=node_type,
                key_apis=key_apis or [],
                verification_command=verification_cmd
            )
            self._topology[name] = node
            ws_root = get_workspace_root()
            _atomic_write_json(
                self._file_path("topology"),
                _dump_topology_portable(self._topology, ws_root)
            )
            return node.model_copy(deep=True)

    # Layer 3 Operations (Event-Sourced Stack Semantics)
    def push_decision(
        self,
        topic: str,
        verdict: str,
        rationale: str,
        decided_by: str = "orchestrator",
        assumptions: List[str] = None,
        dead_ends: List[str] = None,
        pending: List[str] = None,
        frame_id: Optional[str] = None
    ) -> DecisionFrame:
        """Push a decision frame by emitting a DECISION_PUSHED event through serialized commit path."""
        fid = frame_id or f"frame_{uuid.uuid4().hex[:8]}"
        now_ts = time.time()
        frame = DecisionFrame(
            frame_id=fid,
            topic=topic,
            verdict=verdict,
            rationale=rationale,
            decided_by=decided_by,
            assumptions=assumptions or [],
            dead_ends_explored=dead_ends or [],
            pending_questions=pending or [],
            timestamp=now_ts
        )

        self._commit_event(
            event_type=EventType.DECISION_PUSHED,
            actor=decided_by,
            payload=frame.model_dump(),
            wall_time=now_ts
        )

        return frame.model_copy(deep=True)

    def pop_decision(self, reason: str = "Popped decision", actor: str = "orchestrator") -> Optional[DecisionFrame]:
        """Pop the top decision frame by emitting a DECISION_POPPED event through serialized commit path."""
        with self._lock:
            disk_events = self._read_event_log()
            current_state = replay_events(disk_events, verify_hash=True) if disk_events else DecisionStackState()

            if not current_state.active_stack:
                return None

            top_frame = current_state.active_stack[-1]
            payload = {
                "popped_frame_id": top_frame.frame_id,
                "topic": top_frame.topic,
                "reason": reason
            }

            self._commit_event(
                event_type=EventType.DECISION_POPPED,
                actor=actor,
                payload=payload
            )

            return top_frame.model_copy(deep=True)

    def record_dead_end(self, path: str, reason: str = "", actor: str = "orchestrator"):
        """Record an explored dead end to prevent future agents from repeating it."""
        payload = {
            "path": path,
            "reason": reason
        }
        self._commit_event(
            event_type=EventType.DEAD_END_RECORDED,
            actor=actor,
            payload=payload
        )

    def _append_event(self, event: DecisionEvent):
        """Append a single event with fsync durability (backward compatibility / direct append)."""
        self._append_event_to_file(event)

    def falsify_assumption(
        self,
        assumption: str,
        frame_id: Optional[str] = None,
        actor: str = "orchestrator"
    ):
        """Emit ASSUMPTION_FALSIFIED event to invalidate an active assumption."""
        payload = {"assumption": assumption}
        if frame_id:
            payload["frame_id"] = frame_id
        self._commit_event(
            event_type=EventType.ASSUMPTION_FALSIFIED,
            actor=actor,
            payload=payload
        )

    def amend_decision(
        self,
        frame_id: str,
        verdict: Optional[str] = None,
        rationale: Optional[str] = None,
        actor: str = "orchestrator"
    ):
        """Emit DECISION_AMENDED event to update verdict/rationale of an active frame."""
        payload = {"frame_id": frame_id}
        if verdict is not None:
            payload["verdict"] = verdict
        if rationale is not None:
            payload["rationale"] = rationale
        self._commit_event(
            event_type=EventType.DECISION_AMENDED,
            actor=actor,
            payload=payload
        )

    def replay(self, to_seq: Optional[int] = None) -> DecisionStackState:
        """Time-travel query: replay events up to to_seq without mutating active state."""
        with self._lock:
            events = self._read_event_log()
            return replay_events(events, to_seq=to_seq, verify_hash=True)

    def verify_chain_integrity(self) -> Dict[str, Any]:
        """Verify sequence monotonicity and SHA-256 hash chaining from disk genesis to tip."""
        with self._lock:
            disk_events = self._read_event_log()
            if not disk_events:
                return {"ok": True, "total_events": 0, "tip_seq": 0, "tip_hash": "0" * 64}

            temp_state = DecisionStackState()
            for event in disk_events:
                temp_state = apply_event(temp_state, event, verify_hash=True)

            # Assert in-memory state is synchronized with verified on-disk state
            if self.state.latest_hash != temp_state.latest_hash or self.state.current_seq != temp_state.current_seq:
                raise HashIntegrityError(
                    f"In-memory state (seq={self.state.current_seq}, hash='{self.state.latest_hash}') "
                    f"differs from on-disk verified state (seq={temp_state.current_seq}, hash='{temp_state.latest_hash}')"
                )

            return {
                "ok": True,
                "total_events": len(disk_events),
                "tip_seq": temp_state.current_seq,
                "tip_hash": temp_state.latest_hash
            }

    def take_snapshot(self) -> str:
        """Persist current state as a verified snapshot file."""
        snapshot_file = os.path.join(self.snapshots_dir, f"snapshot_seq_{self.state.current_seq:06d}.json")
        snapshot_data = {
            "snapshot_seq": self.state.current_seq,
            "latest_hash": self.state.latest_hash,
            "state": self.state.model_dump()
        }
        _atomic_write_json(snapshot_file, snapshot_data)
        return snapshot_file

    @staticmethod
    def _normalize_uri(uri: str) -> str:
        """Normalize virtual URI like 'palace://rules/INV-001' or 'rules/INV-001'."""
        if not uri:
            return "palace://"
        s = uri.strip()
        if s.startswith("palace://"):
            path = s[9:]
        elif s.startswith("palace:/"):
            path = s[8:]
        elif s.startswith("palace:"):
            path = s[7:]
        else:
            path = s
        path = path.strip("/\\").replace("\\", "/")
        return f"palace://{path}" if path else "palace://"

    def get_tiered_context(self, level: ContextLevel = ContextLevel.L1, max_decision_frames: int = 3) -> Dict[str, Any]:
        """Generate tiered context package for agent handoff at specified level (L0, L1, L2)."""
        with self._lock:
            if level == ContextLevel.L0:
                tip = self.state.active_stack[-1].to_tiered(ContextLevel.L0) if self.state.active_stack else "None"
                return {
                    "level": "L0",
                    "summary": (
                        f"Rules: {len(self._immutable_rules)} | "
                        f"Topology: {len(self._topology)} repos | "
                        f"Active Stack: {len(self.state.active_stack)} (tip: {tip}) | "
                        f"Dead-ends: {len(self.state.dead_ends_pool)} | "
                        f"Events: {len(self.events)} (seq #{self.state.current_seq})"
                    ),
                    "tip_decision": tip,
                    "rules_count": len(self._immutable_rules),
                    "topology_count": len(self._topology),
                    "active_stack_count": len(self.state.active_stack),
                    "dead_ends_count": len(self.state.dead_ends_pool),
                    "total_events": len(self.events),
                    "latest_hash_prefix": self.state.latest_hash[:16],
                }
            elif level == ContextLevel.L1:
                return {
                    "level": "L1",
                    "immutable_rules": [
                        r.to_tiered(ContextLevel.L1)
                        for r in self._immutable_rules.values()
                    ],
                    "topology": [
                        n.to_tiered(ContextLevel.L1)
                        for n in self._topology.values()
                    ],
                    "active_decision_stack": [
                        f.to_tiered(ContextLevel.L1)
                        for f in self.state.active_stack[-max_decision_frames:]
                    ],
                    "dead_ends_pool": copy.deepcopy(self.state.dead_ends_pool),
                    "event_lineage": {
                        "total_events": len(self.events),
                        "current_seq": self.state.current_seq,
                        "latest_hash": self.state.latest_hash
                    }
                }
            else:  # L2
                return {
                    "level": "L2",
                    "immutable_rules": {k: v.model_dump() for k, v in self._immutable_rules.items()},
                    "topology": {k: v.model_dump() for k, v in self._topology.items()},
                    "active_decision_stack": [x.model_dump() for x in self.state.active_stack],
                    "dead_ends_pool": copy.deepcopy(self.state.dead_ends_pool),
                    "history_archive": [x.model_dump() for x in self.state.history_archive],
                    "events": [e.model_dump() for e in self.events],
                    "event_lineage": {
                        "total_events": len(self.events),
                        "current_seq": self.state.current_seq,
                        "latest_hash": self.state.latest_hash
                    }
                }

    def get_active_context_package(self, max_decision_frames: int = 3) -> Dict[str, Any]:
        """Generate a concise, high-density, deep-copied context package for agent handoff (backward compatible)."""
        with self._lock:
            return {
                "immutable_rules": [
                    f"[{r.rule_id}] ({r.category}) {r.description}"
                    for r in self._immutable_rules.values()
                ],
                "topology": [
                    f"[{n.name}] {n.path} (Verify: {n.verification_command})"
                    for n in self._topology.values()
                ],
                "active_decision_stack": [
                    {
                        "frame_id": f.frame_id,
                        "topic": f.topic,
                        "verdict": f.verdict,
                        "rationale": f.rationale,
                        "dead_ends": list(f.dead_ends_explored),
                        "pending": list(f.pending_questions)
                    }
                    for f in self.state.active_stack[-max_decision_frames:]
                ],
                "dead_ends_pool": copy.deepcopy(self.state.dead_ends_pool),
                "event_lineage": {
                    "total_events": len(self.events),
                    "current_seq": self.state.current_seq,
                    "latest_hash": self.state.latest_hash
                }
            }

    def resolve_uri(self, uri: str, level: ContextLevel = ContextLevel.L1) -> Any:
        """Resolve a virtual URI (palace://...) to its tiered context object."""
        norm = self._normalize_uri(uri)
        path = norm[9:]  # strip 'palace://'
        segments = [seg for seg in path.split("/") if seg] if path else []

        with self._lock:
            if not segments:
                return self.get_tiered_context(level=level)

            root_seg = segments[0].lower()

            # 1. Rules
            if root_seg == "rules":
                if len(segments) == 1:
                    return {
                        r.rule_id: r.to_tiered(level)
                        for r in self._immutable_rules.values()
                    }
                rule_id = segments[1]
                if rule_id in self._immutable_rules:
                    return self._immutable_rules[rule_id].to_tiered(level)
                raise KeyError(f"Rule '{rule_id}' not found at {norm}")

            # 2. Topology
            elif root_seg == "topology":
                if len(segments) == 1:
                    return {
                        n.name: n.to_tiered(level)
                        for n in self._topology.values()
                    }
                node_name = segments[1]
                if node_name in self._topology:
                    return self._topology[node_name].to_tiered(level)
                raise KeyError(f"Topology node '{node_name}' not found at {norm}")

            # 3. Decisions
            elif root_seg == "decisions":
                if len(segments) == 1:
                    return {
                        "stack": [f.to_tiered(level) for f in self.state.active_stack],
                        "dead_ends": self.state.dead_ends_pool,
                        "history": [f.to_tiered(level) for f in self.state.history_archive],
                    }
                sub = segments[1].lower()
                if sub == "stack":
                    if len(segments) == 2:
                        return [f.to_tiered(level) for f in self.state.active_stack]
                    key = segments[2]
                    try:
                        idx = int(key)
                        if 0 <= idx < len(self.state.active_stack) or (idx < 0 and -idx <= len(self.state.active_stack)):
                            return self.state.active_stack[idx].to_tiered(level)
                    except ValueError:
                        pass
                    for f in self.state.active_stack:
                        if f.frame_id == key:
                            return f.to_tiered(level)
                    raise KeyError(f"Decision frame '{key}' not found in active stack at {norm}")

                elif sub == "dead_ends":
                    if len(segments) == 2:
                        return self.state.dead_ends_pool
                    idx_str = segments[2]
                    try:
                        idx = int(idx_str)
                        if 0 <= idx < len(self.state.dead_ends_pool) or (idx < 0 and -idx <= len(self.state.dead_ends_pool)):
                            return self.state.dead_ends_pool[idx]
                    except (ValueError, IndexError):
                        pass
                    raise KeyError(f"Dead end index '{idx_str}' out of range at {norm}")

                elif sub == "history":
                    if len(segments) == 2:
                        return [f.to_tiered(level) for f in self.state.history_archive]
                    key = segments[2]
                    try:
                        idx = int(key)
                        if 0 <= idx < len(self.state.history_archive) or (idx < 0 and -idx <= len(self.state.history_archive)):
                            return self.state.history_archive[idx].to_tiered(level)
                    except ValueError:
                        pass
                    for f in self.state.history_archive:
                        if f.frame_id == key:
                            return f.to_tiered(level)
                    raise KeyError(f"Historical frame '{key}' not found at {norm}")

                raise KeyError(f"Unknown decision subpath '{sub}' at {norm}")

            # 4. Supervision
            elif root_seg == "supervision":
                sup_file = os.path.join(self.store_dir, "heartbeats.json")
                tasks: Dict[str, Any] = {}
                if os.path.exists(sup_file):
                    try:
                        with open(sup_file, "r", encoding="utf-8") as f:
                            raw = json.load(f)
                            tasks = {k: ProcessHeartbeat(**v) for k, v in raw.items()}
                    except Exception:
                        tasks = {}

                if len(segments) == 1 or (len(segments) == 2 and segments[1].lower() == "tasks"):
                    return {k: v.to_tiered(level) for k, v in tasks.items()}
                task_id = segments[2] if len(segments) >= 3 else segments[1]
                if task_id in tasks:
                    return tasks[task_id].to_tiered(level)
                raise KeyError(f"Supervision task '{task_id}' not found at {norm}")

            # 5. Audit
            elif root_seg == "audit":
                if len(segments) == 1:
                    return {
                        "total_events": len(self.events),
                        "current_seq": self.state.current_seq,
                        "latest_hash": self.state.latest_hash,
                        "recent_events": [e.to_tiered(level) for e in self.events[-10:]],
                    }
                sub = segments[1].lower()
                if sub == "events":
                    if len(segments) == 2:
                        return [e.to_tiered(level) for e in self.events]
                    seq_str = segments[2]
                    try:
                        seq = int(seq_str)
                        for e in self.events:
                            if e.seq_id == seq:
                                return e.to_tiered(level)
                    except ValueError:
                        pass
                    raise KeyError(f"Audit event sequence '{seq_str}' not found at {norm}")

                raise KeyError(f"Unknown audit subpath '{sub}' at {norm}")

            else:
                raise KeyError(f"Unknown root segment '{root_seg}' in URI {norm}")

    def ls(self, uri: str = "palace://", level: ContextLevel = ContextLevel.L0) -> List[Dict[str, Any]]:
        """List contents of a virtual URI directory with tiered metadata."""
        norm = self._normalize_uri(uri)
        path = norm[9:]
        segments = [seg for seg in path.split("/") if seg] if path else []

        with self._lock:
            if not segments:
                sup_file = os.path.join(self.store_dir, "heartbeats.json")
                tasks_count = 0
                if os.path.exists(sup_file):
                    try:
                        with open(sup_file, "r", encoding="utf-8") as f:
                            tasks_count = len(json.load(f))
                    except Exception:
                        tasks_count = 0

                return [
                    {
                        "uri": "palace://rules",
                        "name": "rules",
                        "node_type": "directory",
                        "count": len(self._immutable_rules),
                        "summary": f"Layer 1 Invariants ({len(self._immutable_rules)} rules)"
                    },
                    {
                        "uri": "palace://topology",
                        "name": "topology",
                        "node_type": "directory",
                        "count": len(self._topology),
                        "summary": f"Layer 2 Domain Topology ({len(self._topology)} nodes)"
                    },
                    {
                        "uri": "palace://decisions",
                        "name": "decisions",
                        "node_type": "directory",
                        "count": len(self.state.active_stack),
                        "summary": f"Layer 3 Active Stack ({len(self.state.active_stack)} frames), Dead-ends ({len(self.state.dead_ends_pool)})"
                    },
                    {
                        "uri": "palace://supervision",
                        "name": "supervision",
                        "node_type": "directory",
                        "count": tasks_count,
                        "summary": f"Process Supervision ({tasks_count} tasks)"
                    },
                    {
                        "uri": "palace://audit",
                        "name": "audit",
                        "node_type": "directory",
                        "count": len(self.events),
                        "summary": f"Audit Log ({len(self.events)} events, tip #{self.state.current_seq})"
                    },
                ]

            root_seg = segments[0].lower()

            if root_seg == "rules":
                return [
                    {
                        "uri": f"palace://rules/{r.rule_id}",
                        "name": r.rule_id,
                        "node_type": "rule",
                        "category": r.category,
                        "summary": r.to_tiered(ContextLevel.L0),
                        "data": r.to_tiered(level)
                    }
                    for r in self._immutable_rules.values()
                ]

            elif root_seg == "topology":
                return [
                    {
                        "uri": f"palace://topology/{n.name}",
                        "name": n.name,
                        "node_type": "topology",
                        "path": n.path,
                        "summary": n.to_tiered(ContextLevel.L0),
                        "data": n.to_tiered(level)
                    }
                    for n in self._topology.values()
                ]

            elif root_seg == "decisions":
                if len(segments) == 1:
                    return [
                        {
                            "uri": "palace://decisions/stack",
                            "name": "stack",
                            "node_type": "directory",
                            "count": len(self.state.active_stack),
                            "summary": f"Active Decision Stack ({len(self.state.active_stack)} frames)"
                        },
                        {
                            "uri": "palace://decisions/dead_ends",
                            "name": "dead_ends",
                            "node_type": "directory",
                            "count": len(self.state.dead_ends_pool),
                            "summary": f"Dead-ends Pool ({len(self.state.dead_ends_pool)} paths)"
                        },
                        {
                            "uri": "palace://decisions/history",
                            "name": "history",
                            "node_type": "directory",
                            "count": len(self.state.history_archive),
                            "summary": f"History Archive ({len(self.state.history_archive)} frames)"
                        },
                    ]
                sub = segments[1].lower()
                if sub == "stack":
                    return [
                        {
                            "uri": f"palace://decisions/stack/{idx}",
                            "name": f"[{idx}] {f.frame_id}",
                            "node_type": "decision_frame",
                            "frame_id": f.frame_id,
                            "topic": f.topic,
                            "verdict": f.verdict,
                            "summary": f.to_tiered(ContextLevel.L0),
                            "data": f.to_tiered(level)
                        }
                        for idx, f in enumerate(self.state.active_stack)
                    ]
                elif sub == "dead_ends":
                    return [
                        {
                            "uri": f"palace://decisions/dead_ends/{idx}",
                            "name": f"[{idx}] {de.get('path', 'unknown')}",
                            "node_type": "dead_end",
                            "path": de.get("path", ""),
                            "reason": de.get("reason", ""),
                            "summary": f"[{de.get('path')}] {de.get('reason', '')}"
                        }
                        for idx, de in enumerate(self.state.dead_ends_pool)
                    ]
                elif sub == "history":
                    return [
                        {
                            "uri": f"palace://decisions/history/{idx}",
                            "name": f"[{idx}] {f.frame_id}",
                            "node_type": "decision_frame",
                            "frame_id": f.frame_id,
                            "topic": f.topic,
                            "verdict": f.verdict,
                            "summary": f.to_tiered(ContextLevel.L0),
                            "data": f.to_tiered(level)
                        }
                        for idx, f in enumerate(self.state.history_archive)
                    ]
                raise KeyError(f"Unknown decision subpath '{sub}' at {norm}")

            elif root_seg == "supervision":
                sup_file = os.path.join(self.store_dir, "heartbeats.json")
                tasks: Dict[str, Any] = {}
                if os.path.exists(sup_file):
                    try:
                        with open(sup_file, "r", encoding="utf-8") as f:
                            raw = json.load(f)
                            tasks = {k: ProcessHeartbeat(**v) for k, v in raw.items()}
                    except Exception:
                        tasks = {}
                return [
                    {
                        "uri": f"palace://supervision/tasks/{hb.task_id}",
                        "name": hb.task_id,
                        "node_type": "task",
                        "role": hb.role,
                        "status": hb.status.value if hasattr(hb.status, "value") else str(hb.status),
                        "summary": hb.to_tiered(ContextLevel.L0),
                        "data": hb.to_tiered(level)
                    }
                    for hb in tasks.values()
                ]

            elif root_seg == "audit":
                return [
                    {
                        "uri": f"palace://audit/events/{e.seq_id}",
                        "name": f"evt_{e.seq_id:04d}",
                        "seq_id": e.seq_id,
                        "node_type": "event",
                        "summary": e.to_tiered(ContextLevel.L0),
                        "data": e.to_tiered(level)
                    }
                    for e in self.events[-50:]
                ]

            else:
                raise KeyError(f"Unknown root segment '{root_seg}' in URI {norm}")

    def tree(self, uri: str = "palace://", max_depth: int = 3, level: ContextLevel = ContextLevel.L0) -> str:
        """Render an ASCII tree representing the palace hierarchy."""
        norm = self._normalize_uri(uri)
        sup_file = os.path.join(self.store_dir, "heartbeats.json")
        tasks_count = 0
        if os.path.exists(sup_file):
            try:
                with open(sup_file, "r", encoding="utf-8") as f:
                    tasks_count = len(json.load(f))
            except Exception:
                tasks_count = 0

        lines = [f"{norm}"]
        with self._lock:
            # Layer 1: rules
            rule_count = len(self._immutable_rules)
            lines.append(f"├── rules/ ({rule_count} rules)")
            rule_items = list(self._immutable_rules.values())
            for idx, r in enumerate(rule_items):
                prefix = "│   ├──" if idx < len(rule_items) - 1 else "│   └──"
                lines.append(f"{prefix} {r.rule_id}: [{r.category}] {r.description}")

            # Layer 2: topology
            topo_count = len(self._topology)
            lines.append(f"├── topology/ ({topo_count} nodes)")
            topo_items = list(self._topology.values())
            for idx, n in enumerate(topo_items):
                prefix = "│   ├──" if idx < len(topo_items) - 1 else "│   └──"
                lines.append(f"{prefix} {n.name} ({n.path})")

            # Layer 3: decisions
            stack_count = len(self.state.active_stack)
            dead_ends_count = len(self.state.dead_ends_pool)
            hist_count = len(self.state.history_archive)
            lines.append(f"├── decisions/")
            lines.append(f"│   ├── stack/ ({stack_count} active frames)")
            for idx, f in enumerate(self.state.active_stack):
                prefix = "│   │   ├──" if idx < stack_count - 1 else "│   │   └──"
                lines.append(f"{prefix} [{idx}] {f.verdict}: {f.topic} ({f.frame_id})")
            lines.append(f"│   ├── dead_ends/ ({dead_ends_count} recorded paths)")
            for idx, de in enumerate(self.state.dead_ends_pool):
                prefix = "│   │   ├──" if idx < dead_ends_count - 1 else "│   │   └──"
                lines.append(f"{prefix} [{idx}] {de.get('path')}: {de.get('reason', '')}")
            lines.append(f"│   └── history/ ({hist_count} archived frames)")

            # Supervision
            lines.append(f"├── supervision/ (tasks: {tasks_count})")

            # Audit
            lines.append(f"└── audit/ ({len(self.events)} events, tip #{self.state.current_seq})")

        return "\n".join(lines)

    def find(self, query: str, level: ContextLevel = ContextLevel.L1) -> List[Dict[str, Any]]:
        """Search recursively across all memory layers for matching tokens/phrases."""
        if not query:
            return []
        q = query.strip().lower()
        matches: List[Dict[str, Any]] = []

        with self._lock:
            # 1. Search Rules
            for r in self._immutable_rules.values():
                if (
                    q in r.rule_id.lower()
                    or q in r.category.lower()
                    or q in r.description.lower()
                ):
                    matches.append({
                        "uri": f"palace://rules/{r.rule_id}",
                        "name": r.rule_id,
                        "node_type": "rule",
                        "match_field": "rule",
                        "summary": r.to_tiered(ContextLevel.L0),
                        "data": r.to_tiered(level)
                    })

            # 2. Search Topology
            for n in self._topology.values():
                if (
                    q in n.name.lower()
                    or q in n.path.lower()
                    or (n.verification_command and q in n.verification_command.lower())
                    or any(q in api.lower() for api in n.key_apis)
                ):
                    matches.append({
                        "uri": f"palace://topology/{n.name}",
                        "name": n.name,
                        "node_type": "topology",
                        "match_field": "topology",
                        "summary": n.to_tiered(ContextLevel.L0),
                        "data": n.to_tiered(level)
                    })

            # 3. Search Decision Stack
            for idx, f in enumerate(self.state.active_stack):
                if (
                    q in f.topic.lower()
                    or q in f.verdict.lower()
                    or q in f.rationale.lower()
                    or any(q in a.lower() for a in f.assumptions)
                    or any(q in de.lower() for de in f.dead_ends_explored)
                    or any(q in pq.lower() for pq in f.pending_questions)
                ):
                    matches.append({
                        "uri": f"palace://decisions/stack/{idx}",
                        "name": f.frame_id,
                        "node_type": "decision_frame",
                        "match_field": "active_stack",
                        "summary": f.to_tiered(ContextLevel.L0),
                        "data": f.to_tiered(level)
                    })

            # 4. Search Dead Ends Pool
            for idx, de in enumerate(self.state.dead_ends_pool):
                path = de.get("path", "")
                reason = de.get("reason", "")
                topic = de.get("topic", "")
                if q in path.lower() or q in reason.lower() or q in topic.lower():
                    matches.append({
                        "uri": f"palace://decisions/dead_ends/{idx}",
                        "name": f"dead_end_{idx}",
                        "node_type": "dead_end",
                        "match_field": "dead_ends",
                        "summary": f"[{path}] {reason}",
                        "data": de
                    })

            # 5. Search History Archive
            for idx, f in enumerate(self.state.history_archive):
                if q in f.topic.lower() or q in f.rationale.lower():
                    matches.append({
                        "uri": f"palace://decisions/history/{idx}",
                        "name": f.frame_id,
                        "node_type": "history_frame",
                        "match_field": "history",
                        "summary": f.to_tiered(ContextLevel.L0),
                        "data": f.to_tiered(level)
                    })

            # 6. Search Events (last 100)
            for e in self.events[-100:]:
                payload_str = json.dumps(e.payload, ensure_ascii=False).lower()
                ev_type = (e.event_type.value if hasattr(e.event_type, "value") else str(e.event_type)).lower()
                if q in e.actor.lower() or q in ev_type or q in payload_str:
                    matches.append({
                        "uri": f"palace://audit/events/{e.seq_id}",
                        "name": f"evt_{e.seq_id:04d}",
                        "node_type": "event",
                        "match_field": "audit_event",
                        "summary": e.to_tiered(ContextLevel.L0),
                        "data": e.to_tiered(level)
                    })

        return matches




