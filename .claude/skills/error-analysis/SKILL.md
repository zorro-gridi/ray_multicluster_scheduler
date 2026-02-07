---
name: error-analysis
description: Systematic exception and error analysis methodology with four-phase process: collect information, locate problems, identify root causes, and verify hypotheses. Use when debugging exceptions, runtime errors, test failures, or unexpected behavior. Integrates with project knowledge base for historical case reference.
disable-model-invocation: true
---

# Error Analysis Methodology

Systematic approach to analyzing exceptions, runtime errors, test failures, and unexpected behavior in the Ray multicluster scheduler codebase.

## Quick Start

When encountering an exception, follow the four-phase analysis process:

1. **Collect Information** - Gather stack traces, error context, expected behavior
2. **Locate Problem** - Trace call chains, check data flow, review concurrency controls
3. **Identify Root Cause** - Classify problem type, build root cause hypothesis
4. **Verify Hypothesis** - Design verification methods, confirm root cause

## Detailed Resources

- **[references/steps.md](references/steps.md)** - Complete four-phase analysis steps with operational guidelines
- **[references/output-format.md](references/output-format.md)** - Standard analysis report output format
- **[references/knowledge-base.md](references/knowledge-base.md)** - Project exception knowledge base reference guide

## Knowledge Base Integration

The project maintains an exception handling knowledge base at `[.knowledge-base/exceptions/](../../.knowledge-base/exceptions/)`

### Exception Categories

| Category | Directory | Typical Exceptions |
|----------|-----------|-------------------|
| Cluster Connection | `cluster-connection/` | Connection failures, timeouts |
| Resource Scheduling | `resource-scheduling/` | No healthy clusters, resource thresholds |
| Concurrency | `concurrency/` | Task duplication, state inconsistency |
| Configuration | `configuration/` | Config errors, parameter passing |
| Policy Engine | `policy-engine/` | Policy evaluation failures |
| Circuit Breaker | `circuit-breaker/` | State transitions, recovery timeouts |

### Search Similar Cases

```bash
# Search by exception type
grep -r "exception_type: ClusterConnectionError" .knowledge-base/exceptions/

# Search by symptom tags
grep -r "tags:.*connection" .knowledge-base/exceptions/

# Search by module
grep -r "module: dispatcher" .knowledge-base/exceptions/
```

## Debugging Tools

Available tools during analysis:

| Tool | Purpose |
|------|---------|
| `Read` | View source code and config files |
| `Grep` | Search code patterns and error messages |
| `Glob` | Find related files |
| `LSP` | Get symbol definitions and references |
| `Bash` | Run tests and debug commands |
| `Edit` | Fix code |

## Best Practices

1. **Evidence-based Analysis** - Analyze based on facts, not assumptions
2. **Track Analysis Process** - Record analysis steps and findings
3. **Deep Understanding** - Focus on WHY, not just WHAT
4. **Document Results** - Store analysis results in knowledge base for future reference
