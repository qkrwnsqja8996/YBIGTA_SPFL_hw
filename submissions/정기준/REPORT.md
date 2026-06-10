# REPORT

이름: 정기준

---

## 01 · Flink checkpoint / flink-basic

| 항목 | 설정값 |
|------|--------|
| `execution.checkpointing.interval` | `10000` |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` |
| `state.backend` | `hashmap` |
| `taskmanager.numberOfTaskSlots` | `2` |
| `parallelism.default` | `3` |

Flink UI에서 TaskManager 3대, 전체 slot 6개가 확인되었고 job은 RUNNING 상태로 정상 실행되었다. checkpoint 설정은 state와 Kafka offset을 장애 시점 이후 복구하기 위해 필요하다.

---

## 02 · Spark batch / spark-batch

| 항목 | 값 |
|------|-----|
| `rows_per_second` | `371` |
| `shuffle_partitions_used` | `6` |

CSV 전체를 읽고 `groupBy(event_type)`로 집계했다. shuffle partition 6은 클러스터의 총 코어 수(3 workers × 2 cores)에 맞춘 값이다.

---

## 03 · Stream vs Batch

| 항목 | 값 |
|------|-----|
| `flink_latency_ms` | `1425` |
| `spark_job_time_ms` | `12365` |
| `latency_ratio` | `8.7` |
| `flink_events_per_second` | `701.8` |
| `spark_events_per_second` | `80.9` |

Flink는 실행 중인 streaming job이 Kafka 이벤트를 즉시 처리하므로 latency가 낮았다. Spark는 job 제출, JVM/SparkContext 시작, DAG planning 비용이 포함되어 같은 1000건에서는 더 느리게 측정되었다. 대용량 배치에서는 Spark가 executor 병렬성과 shuffle을 활용해 높은 throughput을 낼 수 있다.

---

## 04 · Spark shuffle + DAG

| 항목 | 값 |
|------|-----|
| `shuffle_partitions_used` | `6` |
| `job_time_ms` | `12572` |
| `baseline_time_ms` | `13919` |
| `speedup_factor` | `1.11` |

`groupBy` 이후 같은 key를 모으기 위한 shuffle이 발생하고 stage가 분리된다. 기본값 200은 6코어 소규모 클러스터에 비해 task 수가 과해서 scheduling/shuffle overhead가 커진다. 6으로 낮추면 코어 수와 맞아 더 효율적이다.

---

## 05 · Pipeline + fault tolerance

| 항목 | 값 |
|------|-----|
| `events_injected` | `2000` |
| `flink_output_sha256` | `eb395e4aab6aa623087308a8aeed3854b5450d134a37b3038d46d0b1bdbe9367` |
| `spark_output_sha256` | `bb68c7a165e1a9b53f602d8abd6814036a9b51f5a3f024c934371f5fa7295c3c` |
| `passed` | `true` |

Spark는 RDD/DataFrame lineage로 유실 partition을 재계산한다. Flink는 checkpoint에 state와 offset을 저장해 streaming job을 복구한다. Spark lineage는 배치 재계산에 적합하고, Flink checkpoint는 long-running stateful stream 복구에 적합하다.
