# REPORT

이름:신영군

---

## 01 · Flink 체크포인팅 — flink-basic

flink-conf.yaml에 설정한 값:

| 키                                 | 설정값       |
| ---------------------------------- | ------------ |
| `execution.checkpointing.interval` | 10000ms      |
| `execution.checkpointing.mode`     | EXACTLY_ONCE |
| `state.backend`                    | hashmap      |
| `taskmanager.numberOfTaskSlots`    | 2            |
| `parallelism.default`              | 6            |

시나리오 01 실행 중 Flink UI(http://localhost:8081)에서 확인한 내용:

- Jobs → Running Jobs에서 parallelism이 설정한 값으로 표시되는지
- Jobs → Checkpoints에서 체크포인트가 interval마다 기록되는지

→

체크포인팅 설정이 없으면 잡 제출이 거부되는 이유:

→since the flink is being designed for streaming data processing, the thread(or the task) is stateful, hence checkpointing is necessary to recover the state in case of failure. While we are always emphasizing the concept of fault tolerance of a data system, so the flink is being designed to madatorily set the checkpoint.

---

## 02 · Spark 배치 — spark-batch

`results/scenario_02.json`에서:

| 항목                      | 값  |
| ------------------------- | --- |
| `rows_per_second`         | 965 |
| `shuffle_partitions_used` | 6   |

Spark가 CSV를 읽어 groupBy로 집계하는 가장 기본적인 동작을 확인하는 시나리오다.

---

## 03 · Stream vs Batch

`results/scenario_03.json`에서:

| 항목                      | 값    |
| ------------------------- | ----- |
| `flink_latency_ms`        | 1459  |
| `spark_job_time_ms`       | 7709  |
| `latency_ratio`           | 5.3   |
| `flink_events_per_second` | 685.4 |
| `spark_events_per_second` | 129.7 |

**Latency 관점**: 동일한 1000개 이벤트를 처리할 때 Flink가 Spark보다 latency가 낮은 이유:

→ Flink operates on continuous event-driven stream processing, meaning it processes each event immediately as it arrives in memory. In contrast, Spark Batch is designed to collect data, launch a JVM process, read the entire dataset, and process it in a single batch, which inherently introduces high initialization and collection delay.

**Throughput 관점**: latency만 보면 Spark가 불리해 보이지만, throughput 관점에서는 다르다. Spark가 실무에서 여전히 널리 사용되는 이유와 Spark가 더 적합한 상황:

→ Spark processes data in bulk (vectorized operations and batching), which minimizes the per-record scheduling and coordination overhead. For massive offline datasets (e.g., nightly ETL pipelines, historical data analysis, machine learning), Spark delivers much higher throughput and resource efficiency compared to stream processors.

(힌트: latency = 한 건 처리까지 걸리는 시간 / throughput = 단위 시간당 처리량)

**JVM 오버헤드 관점**: `spark_job_time_ms`에는 Spark 잡을 새로 띄울 때마다 발생하는 JVM 부트업 시간이 포함된다. Flink는 TaskManager가 이미 실행 중인 상태에서 잡만 제출하므로 이 오버헤드가 없다. 이 차이를 고려하면 latency 차이가 단순히 처리 방식만의 문제가 아님을 알 수 있다. 한 줄로 정리:

→ Spark's latency metrics are inflated by JVM boot-up overhead for every job submission, whereas Flink runs on pre-allocated TaskManagers, avoiding JVM startup delays.

---

## 04 · Spark Shuffle + DAG

시나리오 04는 자동으로 두 번 spark-submit을 실행한다:

- **baseline**: `shuffle.partitions = 200`으로 측정
- **본 실행**: conf에 채운 값으로 측정

`results/scenario_04.json`에서:

| 항목                          | 값   |
| ----------------------------- | ---- |
| `shuffle_partitions_used`     | 6    |
| `job_time_ms`                 | 8182 |
| `baseline_time_ms` (200 기준) | 8221 |
| `speedup_factor`              | 1.0  |

**Spark UI 관찰** (http://localhost:8080): 실행 중인 잡 클릭 → Stages 탭에서 확인한 태스크 수:

→ 6 tasks (matching our configured spark.sql.shuffle.partitions count).

**DAG Visualization** 확인 내용:

- stage가 몇 개로 분할되었는지
- groupBy 전후로 stage 경계가 생기는 이유 (힌트: 셔플)

→ The job is split into 2 stages. The boundary is created before and after the groupBy operation because grouping requires shuffling data across different executors (nodes) based on the group key. Since shuffle operations require network data transfer, Spark inserts a stage boundary to partition the work.

`shuffle.partitions = 200`이 이 클러스터(코어 6개)에서 비효율적인 이유:

→ Our cluster only has 6 cores (3 workers × 2 cores each). Setting partitions to 200 creates 200 tasks, forcing the 6 cores to run them sequentially in tiny waves. This introduces massive task scheduling, serialization, and network context-switching overhead for a **small** dataset, which hurts performance.

---

## 05 · Pipeline + Fault Tolerance

시나리오 05는 Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작하는지 확인한다.

`results/scenario_05.json`에서:

| 항목                  | 값                                                               |
| --------------------- | ---------------------------------------------------------------- |
| `events_injected`     | 2000                                                             |
| `flink_output_sha256` | eb395e4aab6aa623087308a8aeed3854b5450d134a37b3038d46d0b1bdbe9367 |
| `spark_output_sha256` | bb68c7a165e1a9b53f602d8abd6814036a9b51f5a3f024c934371f5fa7295c3c |
| `passed`              | true                                                             |

flink-conf.yaml에는 체크포인팅 설정을 작성했지만, spark-defaults.conf에는 이에 대응하는 설정이 없다.

Spark가 fault tolerance(노드 장애 시 복구)를 제공하는 방식:

→ Spark relies on **RDD Lineage (Directed Acyclic Graph)**. Since RDDs are immutable, Spark records the exact sequence of transformations used to build the dataset. If a partition fails, Spark simply replays the lineage path for that specific partition from the original source to reconstruct it, without needing to save checkpoints.

Flink의 체크포인팅 방식과 비교했을 때의 차이점, 그리고 각 방식의 장단점:

→ Flink uses active/proactive **Checkpointing** (Chandy-Lamport algorithm) to save snapshots of in-memory states periodically.

### Flink Checkpointing:

- Pros: Fast recovery since it resumes from the latest checkpoint; provides low-latency exactly-once guarantees.
- Cons: Introduces continuous disk I/O and network overhead during normal run time to save snapshots.

### Spark Lineage:

- Pros: Zero run-time overhead because it doesn't write periodic snapshots.
- Cons: Recovery can be slow and computationally expensive if the lineage is long and the entire failed partition needs to be recomputed from scratch.
