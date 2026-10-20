"""Data models for MemPalace."""

import time
import json
import math
import uuid
import hashlib
from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class ContextLevel(str, Enum):
    L0 = "L0"  # Abstract / fast-filter (one-line summary, ~30-80 tokens)
    L1 = "L1"  # Overview / planning context (~200-400 tokens)
    L2 = "L2"  # Full raw detail (complete schemas & lineages)


class PalaceNode(BaseModel):
    uri: str
    name: str
    node_type: str  # "directory", "rule", "topology", "decision", "dead_end", "task", "event"
    summary_l0: str
    overview_l1: Dict[str, Any] = Field(default_factory=dict)
    detail_l2: Dict[str, Any] = Field(default_factory=dict)


class ImmutableRule(BaseModel):
    rule_id: str
    category: str
    description: str
    enforcement_type: str = "fail-closed"
    created_at: float = Field(default_factory=time.time)

    def to_tiered(self, level: ContextLevel = ContextLevel.L1) -> Any:
        if level == ContextLevel.L0:
            return f"[{self.rule_id}] ({self.category}) {self.description}"
        elif level == ContextLevel.L1:
            return {
                "rule_id": self.rule_id,
                "category": self.category,
                "description": self.description,
                "enforcement_type": self.enforcement_type,
            }
        return self.model_dump()


class TopologyNode(BaseModel):
    name: str
    path: str
    node_type: str = "repo"  # repo, package, skill, service
    key_apis: List[str] = Field(default_factory=list)
    verification_command: Optional[str] = None
    last_verified_at: Optional[float] = None

    def to_tiered(self, level: ContextLevel = ContextLevel.L1) -> Any:
        if level == ContextLevel.L0:
            return f"[{self.name}] {self.path} (verify: {self.verification_command})"
        elif level == ContextLevel.L1:
            return {
                "name": self.name,
                "path": self.path,
                "node_type": self.node_type,
                "verification_command": self.verification_command,
                "key_apis": self.key_apis,
            }
        return self.model_dump()


class DecisionFrame(BaseModel):
    frame_id: str = Field(default_factory=lambda: f"frame_{uuid.uuid4().hex[:8]}")
    topic: str
    decided_by: str = "orchestrator"
    verdict: str  # BUILD, EVALUATE, REVIEW, REJECT, ESCALATE
    rationale: str
    assumptions: List[str] = Field(default_factory=list)
    dead_ends_explored: List[str] = Field(default_factory=list)
    pending_questions: List[str] = Field(default_factory=list)
    timestamp: float = Field(default_factory=time.time)

    def to_tiered(self, level: ContextLevel = ContextLevel.L1) -> Any:
        if level == ContextLevel.L0:
            return f"[{self.verdict}] {self.topic} (by {self.decided_by})"
        elif level == ContextLevel.L1:
            return {
                "frame_id": self.frame_id,
                "topic": self.topic,
                "verdict": self.verdict,
                "rationale": self.rationale,
                "decided_by": self.decided_by,
                "dead_ends": self.dead_ends_explored,
                "pending": self.pending_questions,
            }
        return self.model_dump()


class EventType(str, Enum):
    DECISION_PUSHED = "DECISION_PUSHED"
    DECISION_POPPED = "DECISION_POPPED"
    DECISION_AMENDED = "DECISION_AMENDED"
    ASSUMPTION_FALSIFIED = "ASSUMPTION_FALSIFIED"
    DEAD_END_RECORDED = "DEAD_END_RECORDED"
    SNAPSHOT_TAKEN = "SNAPSHOT_TAKEN"


class DecisionEvent(BaseModel):
    seq_id: int
    event_id: str = Field(default_factory=lambda: f"evt_{uuid.uuid4().hex[:12]}")
    event_type: EventType
    actor: str = "orchestrator"
    payload: Dict[str, Any] = Field(default_factory=dict)
    wall_time: float = Field(default_factory=time.time)
    prev_event_hash: str
    event_hash: str = ""

    def compute_hash(self) -> str:
        """Compute deterministic SHA-256 hash for this event including wall_time."""
        if not math.isfinite(self.wall_time):
            raise ValueError(f"wall_time must be a finite float, got {self.wall_time}")
        canonical = {
            "seq_id": self.seq_id,
            "event_id": self.event_id,
            "event_type": self.event_type.value if hasattr(self.event_type, "value") else str(self.event_type),
            "actor": self.actor,
            "payload": self.payload,
            "wall_time": self.wall_time,
            "prev_event_hash": self.prev_event_hash,
        }
        canonical_str = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

    def model_post_init(self, __context: Any) -> None:
        if not self.event_hash:
            self.event_hash = self.compute_hash()

    def to_tiered(self, level: ContextLevel = ContextLevel.L1) -> Any:
        ev_type = self.event_type.value if hasattr(self.event_type, "value") else str(self.event_type)
        if level == ContextLevel.L0:
            return f"#{self.seq_id:04d} [{ev_type}] by {self.actor}"
        elif level == ContextLevel.L1:
            return {
                "seq_id": self.seq_id,
                "event_id": self.event_id,
                "event_type": ev_type,
                "actor": self.actor,
                "wall_time": self.wall_time,
                "event_hash_prefix": self.event_hash[:16],
            }
        return self.model_dump()


class DecisionStackState(BaseModel):
    current_seq: int = 0
    latest_hash: str = "0" * 64
    active_stack: List[DecisionFrame] = Field(default_factory=list)
    dead_ends_pool: List[Dict[str, Any]] = Field(default_factory=list)
    history_archive: List[DecisionFrame] = Field(default_factory=list)


class TaskStatus(str, Enum):
    RUNNING = "running"
    IDLE = "idle"
    STALLED = "stalled"
    COMPLETED = "completed"
    FAILED = "failed"


class ProcessHeartbeat(BaseModel):
    task_id: str
    role: str
    status: TaskStatus = TaskStatus.RUNNING
    last_heartbeat: float = Field(default_factory=time.time)
    last_log_snippet: str = ""
    timeout_seconds: float = 120.0
    pid: Optional[int] = None
    progress_epoch: int = 0

    def to_tiered(self, level: ContextLevel = ContextLevel.L1) -> Any:
        status_val = self.status.value if hasattr(self.status, "value") else str(self.status)
        if level == ContextLevel.L0:
            return f"[{self.task_id}] {self.role} ({status_val})"
        elif level == ContextLevel.L1:
            return {
                "task_id": self.task_id,
                "role": self.role,
                "status": status_val,
                "last_heartbeat": self.last_heartbeat,
                "timeout_seconds": self.timeout_seconds,
            }
        return self.model_dump()


