# REPORT

이름:

---

## 01 · Flink 체크포인팅 — flink-basic

flink-conf.yaml에 설정한 값:

| 키 | 설정값 |
|------|--------|
| `execution.checkpointing.interval` | |
| `execution.checkpointing.mode` | |
| `state.backend` | |
| `taskmanager.numberOfTaskSlots` | |
| `parallelism.default` | |

시나리오 01 실행 중 Flink UI(http://localhost:8081)에서 확인한 내용:

- Jobs → Running Jobs에서 parallelism이 설정한 값으로 표시되는지
- Jobs → Checkpoints에서 체크포인트가 interval마다 기록되는지

→ 

체크포인팅 설정이 없으면 잡 제출이 거부되는 이유:

→ 

---

## 02 · Spark 배치 — spark-batch

`results/scenario_02.json`에서:

| 항목 | 값 |
|------|-----|
| `rows_per_second` | |
| `shuffle_partitions_used` | |

Spark가 CSV를 읽어 groupBy로 집계하는 가장 기본적인 동작을 확인하는 시나리오다.

---

## 03 · Stream vs Batch

`results/scenario_03.json`에서:

| 항목 | 값 |
|------|-----|
| `flink_latency_ms` | |
| `spark_job_time_ms` | |
| `latency_ratio` | |
| `flink_events_per_second` | |
| `spark_events_per_second` | |

**Latency 관점**: 동일한 1000개 이벤트를 처리할 때 Flink가 Spark보다 latency가 낮은 이유:

→ 

**Throughput 관점**: latency만 보면 Spark가 불리해 보이지만, throughput 관점에서는 다르다. Spark가 실무에서 여전히 널리 사용되는 이유와 Spark가 더 적합한 상황:

→ 

(힌트: latency = 한 건 처리까지 걸리는 시간 / throughput = 단위 시간당 처리량)

**JVM 오버헤드 관점**: `spark_job_time_ms`에는 Spark 잡을 새로 띄울 때마다 발생하는 JVM 부트업 시간이 포함된다. Flink는 TaskManager가 이미 실행 중인 상태에서 잡만 제출하므로 이 오버헤드가 없다. 이 차이를 고려하면 latency 차이가 단순히 처리 방식만의 문제가 아님을 알 수 있다. 한 줄로 정리:

→ 

---

## 04 · Spark Shuffle + DAG

시나리오 04는 자동으로 두 번 spark-submit을 실행한다:
- **baseline**: `shuffle.partitions = 200`으로 측정
- **본 실행**: conf에 채운 값으로 측정

`results/scenario_04.json`에서:

| 항목 | 값 |
|------|-----|
| `shuffle_partitions_used` | |
| `job_time_ms` | |
| `baseline_time_ms` (200 기준) | |
| `speedup_factor` | |

**Spark UI 관찰** (http://localhost:8080): 실행 중인 잡 클릭 → Stages 탭에서 확인한 태스크 수:

→ 

**DAG Visualization** 확인 내용:
- stage가 몇 개로 분할되었는지
- groupBy 전후로 stage 경계가 생기는 이유 (힌트: 셔플)

→ 

`shuffle.partitions = 200`이 이 클러스터(코어 6개)에서 비효율적인 이유:

→ 

---

## 05 · Pipeline + Fault Tolerance

시나리오 05는 Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작하는지 확인한다.

`results/scenario_05.json`에서:

| 항목 | 값 |
|------|-----|
| `events_injected` | |
| `flink_output_sha256` | |
| `spark_output_sha256` | |
| `passed` | |

flink-conf.yaml에는 체크포인팅 설정을 작성했지만, spark-defaults.conf에는 이에 대응하는 설정이 없다.

Spark가 fault tolerance(노드 장애 시 복구)를 제공하는 방식:

→ (힌트: RDD Lineage)

Flink의 체크포인팅 방식과 비교했을 때의 차이점, 그리고 각 방식의 장단점:

→ 
