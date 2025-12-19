# Ray Multi-Cluster Scheduler

A scheduler for managing tasks across multiple Ray clusters.

## Overview

The Ray Multi-Cluster Scheduler is designed to distribute tasks across multiple Ray clusters, providing a unified interface for submitting tasks and managing resources.

## Features

- Distribute tasks across multiple Ray clusters
- Monitor cluster health and resource availability
- Implement backpressure to prevent overwhelming clusters
- Provide a unified interface for task submission

## Installation

To install the Ray Multi-Cluster Scheduler, run:

```bash
pip install ray_multicluster_scheduler
```

## Usage

To start the scheduler, run:

```bash
ray-multicluster-scheduler
```

## Development

To install the package in development mode, run:

```bash
pip install -e .
```

To run tests, run:

```bash
pytest
```