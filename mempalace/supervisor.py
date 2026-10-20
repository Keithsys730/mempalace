"""Process Supervision & Heartbeat Watcher for background tasks."""

import os
import time
import json
import filelock
from typing import Dict, List, Optional, Any
from .models import ProcessHeartbeat, TaskStatus
from .core import _atomic_write_json


class ProcessSupervisor:
    """Supervises concurrent agent background tasks and detects stalled processes."""

    def __init__(self, store_dir: Optional[str] = None, default_timeout_sec: float = 120.0):
        self.store_dir = store_dir
        self.default_timeout_sec = default_timeout_sec
        self.tasks: Dict[str, ProcessHeartbeat] = {}

        if self.store_dir:
            os.makedirs(self.store_dir, exist_ok=True)
            self.lock_file = os.path.join(self.store_dir, ".supervisor.lock")
            self._lock = filelock.FileLock(self.lock_file, timeout=10.0)
            self.registry_file = os.path.join(self.store_dir, "heartbeats.json")
            with self._lock:
                self._load_registry()
        else:
            self._lock = None
            self.registry_file = None

    def _load_registry(self):
        if self.registry_file and os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.tasks = {k: ProcessHeartbeat(**v) for k, v in data.items()}
            except Exception:
                self.tasks = {}

    def _save_registry(self):
        if self.registry_file and self._lock:
            _atomic_write_json(
                self.registry_file,
                {k: v.model_dump() for k, v in self.tasks.items()}
            )

    def register_task(
        self,
        task_id: str,
        role: str,
        timeout_sec: Optional[float] = None,
        pid: Optional[int] = None
    ) -> ProcessHeartbeat:
        timeout = timeout_sec or self.default_timeout_sec
        process_pid = pid or os.getpid()

        if self._lock:
            with self._lock:
                self._load_registry()
                hb = ProcessHeartbeat(
                    task_id=task_id,
                    role=role,
                    status=TaskStatus.RUNNING,
                    last_heartbeat=time.time(),
                    timeout_seconds=timeout,
                    pid=process_pid
                )
                self.tasks[task_id] = hb
                self._save_registry()
                return hb.model_copy(deep=True)
        else:
            hb = ProcessHeartbeat(
                task_id=task_id,
                role=role,
                status=TaskStatus.RUNNING,
                last_heartbeat=time.time(),
                timeout_seconds=timeout,
                pid=process_pid
            )
            self.tasks[task_id] = hb
            return hb

    def record_heartbeat(
        self,
        task_id: str,
        log_snippet: str = "",
        status: TaskStatus = TaskStatus.RUNNING,
        progress_epoch: Optional[int] = None
    ):
        status_enum = TaskStatus(status) if isinstance(status, str) else status
        if self._lock:
            with self._lock:
                self._load_registry()
                if task_id in self.tasks:
                    hb = self.tasks[task_id]
                    hb.last_heartbeat = time.time()
                    if log_snippet:
                        hb.last_log_snippet = log_snippet[-300:]
                    hb.status = status_enum
                    if progress_epoch is not None:
                        hb.progress_epoch = progress_epoch
                    self._save_registry()
        else:
            if task_id in self.tasks:
                hb = self.tasks[task_id]
                hb.last_heartbeat = time.time()
                if log_snippet:
                    hb.last_log_snippet = log_snippet[-300:]
                hb.status = status_enum
                if progress_epoch is not None:
                    hb.progress_epoch = progress_epoch

    def check_health(self) -> Dict[str, Any]:
        """Inspect all tasks and exhaustively classify status without state bleed."""
        if self._lock:
            with self._lock:
                self._load_registry()
                res = self._evaluate_health()
                self._save_registry()
                return res
        else:
            return self._evaluate_health()

    def _evaluate_health(self) -> Dict[str, Any]:
        now = time.time()
        healthy = []
        stalled = []
        failed = []
        completed = []
        idle = []

        for hb in self.tasks.values():
            status = hb.status if isinstance(hb.status, TaskStatus) else TaskStatus(hb.status)
            elapsed = max(0.0, now - hb.last_heartbeat)

            if status in (TaskStatus.COMPLETED, "completed", "finished", "done"):
                completed.append({
                    "task_id": hb.task_id,
                    "role": hb.role,
                    "status": TaskStatus.COMPLETED.value,
                    "elapsed_sec": round(elapsed, 1),
                    "pid": hb.pid
                })
            elif status in (TaskStatus.FAILED, "failed", "error"):
                failed.append({
                    "task_id": hb.task_id,
                    "role": hb.role,
                    "status": TaskStatus.FAILED.value,
                    "elapsed_sec": round(elapsed, 1),
                    "last_log": hb.last_log_snippet,
                    "pid": hb.pid
                })
            elif status in (TaskStatus.IDLE, "idle"):
                idle.append({
                    "task_id": hb.task_id,
                    "role": hb.role,
                    "status": TaskStatus.IDLE.value,
                    "elapsed_sec": round(elapsed, 1),
                    "pid": hb.pid
                })
            elif status in (TaskStatus.STALLED, "stalled"):
                stalled.append({
                    "task_id": hb.task_id,
                    "role": hb.role,
                    "status": TaskStatus.STALLED.value,
                    "elapsed_sec": round(elapsed, 1),
                    "timeout_sec": hb.timeout_seconds,
                    "last_log": hb.last_log_snippet,
                    "pid": hb.pid
                })
            elif status in (TaskStatus.RUNNING, "running"):
                if elapsed > hb.timeout_seconds:
                    hb.status = TaskStatus.STALLED
                    stalled.append({
                        "task_id": hb.task_id,
                        "role": hb.role,
                        "status": TaskStatus.STALLED.value,
                        "elapsed_sec": round(elapsed, 1),
                        "timeout_sec": hb.timeout_seconds,
                        "last_log": hb.last_log_snippet,
                        "pid": hb.pid
                    })
                else:
                    healthy.append({
                        "task_id": hb.task_id,
                        "role": hb.role,
                        "status": TaskStatus.RUNNING.value,
                        "elapsed_sec": round(elapsed, 1),
                        "pid": hb.pid
                    })
            else:
                hb.status = TaskStatus.FAILED
                failed.append({
                    "task_id": hb.task_id,
                    "role": hb.role,
                    "status": TaskStatus.FAILED.value,
                    "elapsed_sec": round(elapsed, 1),
                    "last_log": hb.last_log_snippet,
                    "pid": hb.pid
                })

        return {
            "total_tasks": len(self.tasks),
            "healthy_count": len(healthy),
            "stalled_count": len(stalled),
            "failed_count": len(failed),
            "completed_count": len(completed),
            "idle_count": len(idle),
            "healthy_tasks": healthy,
            "stalled_tasks": stalled,
            "failed_tasks": failed,
            "completed_tasks": completed,
            "idle_tasks": idle
        }

