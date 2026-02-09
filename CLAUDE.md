# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ray Multi-Cluster Scheduler is a control plane layer built on top of Ray's internal scheduler. It manages task distribution across heterogeneous Ray clusters (Linux x86_64, macOS ARM64), providing unified resource management, load balancing, and job orchestration.

Think of it as "Kubernetes-style control plane for Ray clusters" - Ray handles single-cluster execution, while this system decides which Ray cluster to use.

## Commands

```bash
# Dev mode install (recommended)
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run scheduler
ray-multicluster-scheduler
# or
python -m ray_multicluster_scheduler.main
```

## Architecture

```
App Layer (app/client_api/)
    ↓
Scheduler Control Plane (scheduler/)
    ↓
Common Infrastructure (common/, control_plane/)
    ↓
Ray Data Plane (Ray Clusters)
```

### Core Components

| Component | Location | Purpose |
|-----------|----------|---------|
| TaskLifecycleManager | `scheduler/lifecycle/` | Core orchestrator |
| PolicyEngine | `scheduler/policy/` | Multi-factor scheduling decisions |
| TaskQueue | `scheduler/queue/` | Global and cluster-specific queues |
| Dispatcher | `scheduler/scheduler_core/` | Task submission to target clusters |

## Key Patterns

- **Singleton**: `UnifiedScheduler`
- **Thread-safe queues**: Protected by `Lock`/`Condition`
- **Pluggable policies**: Via `PolicyEngine`
- **Lazy connection initialization**: For Ray clients

## Scheduling Rules

- **Resource threshold**: 70% for CPU, GPU, memory
- **40-second rule**: One top-level task per cluster within 40 seconds
- **Queue priority**: Preferred cluster → Tag affinity → Load balancing → Round-robin

## Reference

For comprehensive coding standards, testing workflows, and debugging guides, see:
- `.claude/CLAUDE.md` - Detailed project documentation (Chinese)
- `.claude/rules/` - Specialized workflow rules
