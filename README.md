# Airflow Full Course

This repository provides a hands-on introduction to Apache Airflow, covering core concepts, DAG authoring, scheduling, operators, XComs, branching, asset dependencies, and orchestration. The examples are designed for both beginners and intermediate users who want to deepen their understanding of Airflow's workflow management capabilities.
---

## Table of Contents

- [Course Structure](#course-structure)
- [Running with Docker Compose](#running-with-docker-compose)
- [DAGs Overview](#dags-overview)
- [Key Concepts Covered](#key-concepts-covered)

---

## Course Structure

```
├── docker-compose.yaml      # Docker Compose setup for Airflow
├── main.py                  # Entry point or utility script
├── pyproject.toml           # Python project configuration
├── README.md                # This file
├── config/                  # Configuration files (if any)
├── dags/                    # All Airflow DAGs for the tutorial
│   ├── 1_first_dag.py
│   ├── 2_dag_versioning.py
│   ├── ...
│   └── dag_orchestrate_parent.py
├── logs/                    # Airflow logs
├── plugins/                 # Custom Airflow plugins
```

---

## Running with Docker Compose

The easiest way to start Airflow for this tutorial is using Docker Compose.

1. **Start Airflow:**
   ```sh
   docker-compose up
   ```
2. **Access the Airflow UI:**
   - Open your browser and go to [http://localhost:8080](http://localhost:8080)
   - Default credentials (unless changed):
     - Username: `airflow`
     - Password: `airflow`

3. **Stop Airflow:**
   ```sh
   docker-compose down
   ```

---

## DAGs Overview

The `dags/` folder contains a series of DAGs, each demonstrating a specific Airflow feature or pattern:

- **1_first_dag.py**: Your first simple DAG
- **2_dag_versioning.py**: How to version and update DAGs
- **3_operators.py**: Using different types of operators
- **4_XCOMs_auto.py**: Automatic XComs (cross-communication)
- **5_XCOMs_kwargs.py**: Passing data with XComs and kwargs
- **6_parallel_tasks.py**: Running tasks in parallel
- **7_branches.py**: Branching and conditional logic
- **8_schedule_preset.py**: Using schedule presets
- **9_schedule_cron.py**: Custom cron schedules
- **10_schedule_delta.py**: Scheduling with time deltas
- **11_incremental_load.py**: Incremental data loading patterns
- **12_special_dates.py**: Handling special dates in scheduling
- **14_asset_dependent.py**: Asset dependency management
- **asset_13.py**: Asset-based DAG example
- **dag_orchestrate_1.py, dag_orchestrate_2.py, dag_orchestrate_parent.py**: Orchestrating multiple DAGs

Each DAG is well-commented and can be explored in the Airflow UI.

---

## Key Concepts Covered

- **DAG Authoring**: How to define and structure DAGs
- **Operators**: BashOperator, PythonOperator, and more
- **Task Dependencies**: Setting up task order and parallelism
- **XComs**: Passing data between tasks
- **Branching**: Conditional task execution
- **Scheduling**: Using presets, cron, and custom intervals
- **Asset Dependencies**: Managing dependencies between data assets
- **DAG Orchestration**: Parent/child DAG relationships
