---
name: verify
description: Verify MemPalace hierarchical memory and process supervisor.
disable-model-invocation: true
compatibility: "Windows 11 / Python 3.10+ / PowerShell"
---

# Verify MemPalace

Run all deterministic verification steps for mempalace:

## 1. Unit Test Suite
```powershell
pytest tests -v
```
Expected: 2 passed.

## 2. Memory State Check
```powershell
python -m mempalace.cli inspect
```
