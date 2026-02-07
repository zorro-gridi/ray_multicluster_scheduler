# Fix Plan: Job Status Query from Queue

## Problem Summary

When a job is submitted from the task queue (not directly submitted), the scheduler fails to query its status because the `job_cluster_mapping` and `submission_to_job_mapping` are not updated. This causes `wait_for_all_jobs` to fail with "Job status UNKNOWN".

## Root Cause

In `task_lifecycle_manager.py`, the `submit_job` function (lines 211-298) correctly updates mappings when dispatching a job:
```python
job_id = self.dispatcher.dispatch_job(converted_job_desc, decision.cluster_name)
self.job_cluster_mapping[job_id] = decision.cluster_name
self.submission_to_job_mapping[job_id] = job_desc.job_id
```

However, the `_process_job` function (lines 1016-1020 and 1055-1059) does NOT update these mappings when dispatching jobs from the queue, causing status queries to fail.

## Fix Locations

### File: `ray_multicluster_scheduler/scheduler/lifecycle/task_lifecycle_manager.py`

#### Location 1: Cluster Queue Case (after line 1020)
Add mapping updates after successfully dispatching job from cluster queue:
```python
# After line 1018: job_desc.actual_submission_id = actual_submission_id
# Add:
self.job_cluster_mapping[actual_submission_id] = source_cluster_queue
self.submission_to_job_mapping[actual_submission_id] = job_desc.job_id
self.job_scheduling_mapping[job_desc.job_id] = job_desc
```

#### Location 2: Global Queue Case (after line 1059)
Add mapping updates after successfully dispatching job from global queue:
```python
# After line 1058: job_desc.scheduling_status = "SUBMITTED"
# Add:
self.job_cluster_mapping[actual_submission_id] = decision.cluster_name
self.submission_to_job_mapping[actual_submission_id] = job_desc.job_id
self.job_scheduling_mapping[job_desc.job_id] = job_desc
```

## Testing

Run pytest to verify:
```bash
pytest
```

Specifically test job submission from queue scenarios to ensure status queries work correctly.
