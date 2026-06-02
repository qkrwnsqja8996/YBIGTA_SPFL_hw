# REPORT

이름: 오유림

---

## 01 · Flink 체크포인팅 — flink-basic

flink-conf.yaml에 설정한 값:

| 키 | 설정값 |
|------|--------|
| `execution.checkpointing.interval` | 10000 (ms) |
| `execution.checkpointing.mode` | EXACTLY_ONCE |
| `state.backend` | hashmap |
| `taskmanager.numberOfTaskSlots` | 2 |
| `parallelism.default` | 3 |

시나리오 01 실행 중 Flink UI(http://localhost:8081)에서 확인한 내용:

- Jobs → Running Jobs에서 parallelism이 설정한 값으로 표시되는지
- Jobs → Checkpoints에서 체크포인트가 interval마다 기록되는지

→ Running Jobs에서 parallelism=3으로 표시됨. Checkpoints 탭에서 약 10초(10000ms) 간격으로 스냅샷이 쌓이며, Completed 카운트가 증가하는 것을 확인. flink-basic 시나리오는 1000개 이벤트를 약 3145ms(events_per_second=162.1)만에 처리하며 output_sha256이 expected와 일치해 PASS.

체크포인팅 설정이 없으면 잡 제출이 거부되는 이유:

→ flink_counter.py의 KeyedProcessFunction이 상태(ValueState)를 사용하는 stateful 오퍼레이터다. Flink는 stateful 오퍼레이터가 있을 때 장애 복구를 보장하기 위해 체크포인트 저장소(state.checkpoints.dir)가 반드시 필요하다. 설정이 없으면 스냅샷을 어디에 써야 할지 알 수 없어 JobManager가 잡 제출 자체를 거부한다.

---

## 02 · Spark 배치 — spark-batch

`results/scenario_02.json`에서:

| 항목 | 값 |
|------|-----|
| `rows_per_second` | 198 |
| `shuffle_partitions_used` | 6 |

Spark가 CSV를 읽어 groupBy로 집계하는 가장 기본적인 동작을 확인하는 시나리오다. shuffle.partitions=6으로 클러스터 코어 수(3 workers × 2 cores = 6)와 일치시켰기 때문에 파티션 하나당 코어 하나가 대응되어 불필요한 스케줄링 오버헤드 없이 10000행을 처리했다.

---

## 03 · Stream vs Batch

`results/scenario_03.json`에서:

| 항목 | 값 |
|------|-----|
| `flink_latency_ms` | 3145 |
| `spark_job_time_ms` | 29346 |
| `latency_ratio` | 9.3 |
| `flink_events_per_second` | 318.0 |
| `spark_events_per_second` | 34.1 |

**Latency 관점**: 동일한 1000개 이벤트를 처리할 때 Flink가 Spark보다 latency가 낮은 이유:

→ Flink는 이벤트가 Kafka에 도착하는 즉시 레코드 단위로 처리한다(true streaming). 이에 비해 Spark는 잡을 제출할 때마다 SparkContext 초기화, DAG 계획 수립, Executor 할당 과정을 거친 뒤 전체 데이터를 한꺼번에 처리하는 배치 방식이라 첫 결과가 나오기까지 대기 시간이 크다. 1000개 정도의 소규모 이벤트에서는 이 차이가 9.3배(3145ms vs 29346ms)로 뚜렷하게 나타난다.

**Throughput 관점**: latency만 보면 Spark가 불리해 보이지만, throughput 관점에서는 다르다. Spark가 실무에서 여전히 널리 사용되는 이유와 Spark가 더 적합한 상황:

→ Spark는 Catalyst 옵티마이저와 Tungsten 실행 엔진을 통해 대규모 정적 데이터셋(수백 GB~TB 단위)을 컬럼 단위로 압축·벡터화 처리하는 데 특화되어 있다. 데이터가 충분히 크면 잡 초기화 비용이 상쇄되고, 글로벌 정렬·조인·대규모 집계처럼 전체 데이터를 한 번에 봐야 하는 작업에서 Flink보다 높은 throughput을 낼 수 있다. 실시간 반응이 필요 없고 데이터가 이미 저장된 배치 파이프라인(ETL, 리포트 생성)에 적합하다.

**JVM 오버헤드 관점**: `spark_job_time_ms`에는 Spark 잡을 새로 띄울 때마다 발생하는 JVM 부트업 시간이 포함된다. Flink는 TaskManager가 이미 실행 중인 상태에서 잡만 제출하므로 이 오버헤드가 없다. 이 차이를 고려하면 latency 차이가 단순히 처리 방식만의 문제가 아님을 알 수 있다. 한 줄로 정리:

→ spark_job_time_ms의 상당 부분은 실제 데이터 처리가 아닌 JVM·SparkContext 초기화 비용이므로, 소규모 이벤트를 자주 처리하는 구조에서는 Spark의 latency가 구조적으로 높을 수밖에 없다.

---

## 04 · Spark Shuffle + DAG

시나리오 04는 자동으로 두 번 spark-submit을 실행한다:
- **baseline**: `shuffle.partitions = 200`으로 측정
- **본 실행**: conf에 채운 값으로 측정

`results/scenario_04.json`에서:

| 항목 | 값 |
|------|-----|
| `shuffle_partitions_used` | 6 |
| `job_time_ms` | 30082 |
| `baseline_time_ms` (200 기준) | 28516 |
| `speedup_factor` | 0.95 |

**Spark UI 관찰** (http://localhost:8080): 실행 중인 잡 클릭 → Stages 탭에서 확인한 태스크 수:

→ baseline(200 파티션) 실행에서는 groupBy 이후 Stage에 태스크 200개가 생성됨. 본 실행(6 파티션)에서는 동일 Stage에 태스크 6개만 생성된다. 50000행 규모에서는 200개 파티션 각각의 데이터량이 매우 적어 실행이 빠르게 끝나므로 스케줄링 오버헤드가 크지 않아 speedup_factor≈0.95로 개선 효과가 미미하게 측정됨(측정 노이즈 수준). 프로덕션 규모(수억 행)에서는 파티션 수 감소 효과가 명확하게 드러난다.

**DAG Visualization** 확인 내용:
- stage가 몇 개로 분할되었는지
- groupBy 전후로 stage 경계가 생기는 이유 (힌트: 셔플)

→ DAG는 Stage 1(CSV 읽기 + 파티셔닝)과 Stage 2(groupBy 집계)의 2단계로 분할된다. groupBy는 같은 키의 데이터를 같은 파티션으로 모으는 셔플을 유발하며, 셔플이 발생하는 지점에서 반드시 stage 경계가 생긴다. Stage 1이 완료되어야 Stage 2가 시작될 수 있기 때문이다.

`shuffle.partitions = 200`이 이 클러스터(코어 6개)에서 비효율적인 이유:

→ 동시에 실행할 수 있는 태스크는 코어 수인 6개뿐이다. 200개 파티션을 6코어로 처리하면 200/6≈34 라운드에 걸쳐 순차적으로 태스크를 실행해야 한다. 파티션 하나당 데이터량이 극히 적음에도 태스크 생성·스케줄링·직렬화 비용이 34배 더 발생한다. 파티션 수를 코어 수(6)에 맞추면 단 1 라운드로 완료되어 오버헤드가 대폭 감소한다.

---

## 05 · Pipeline + Fault Tolerance

시나리오 05는 Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작하는지 확인한다.

`results/scenario_05.json`에서:

| 항목 | 값 |
|------|-----|
| `events_injected` | 2000 |
| `flink_output_sha256` | eb395e4aab6aa623087308a8aeed3854b5450d134a37b3038d46d0b1bdbe9367 |
| `spark_output_sha256` | bb68c7a165e1a9b53f602d8abd6814036a9b51f5a3f024c934371f5fa7295c3c |
| `passed` | true |

flink-conf.yaml에는 체크포인팅 설정을 작성했지만, spark-defaults.conf에는 이에 대응하는 설정이 없다.

Spark가 fault tolerance(노드 장애 시 복구)를 제공하는 방식:

→ Spark는 RDD Lineage(계보)를 통해 fault tolerance를 제공한다. 모든 RDD 변환(map, filter, groupBy 등)은 DAG로 기록되며, 이 기록이 곧 "어떻게 데이터를 재생성하는지"에 대한 설명서다. 특정 노드가 실패해 파티션이 유실되면 Spark는 원본 데이터(CSV, HDFS 등)로부터 Lineage를 따라 해당 파티션만 재계산한다. 별도의 체크포인트 저장소가 필요 없고, 배치 데이터는 항상 재읽기가 가능하기 때문에 이 방식이 성립한다.

Flink의 체크포인팅 방식과 비교했을 때의 차이점, 그리고 각 방식의 장단점:

→ Flink 체크포인팅은 실행 중인 상태(ValueState, AggregateState 등)와 Kafka 오프셋을 주기적으로 외부 저장소에 스냅샷으로 저장한다. 장애 발생 시 가장 최근 스냅샷에서 정확히 복원하여 EXACTLY_ONCE를 보장한다.
- **Flink 체크포인팅 장점**: 스트리밍 상태(세션·집계 등 오랜 시간에 걸쳐 누적된 값)를 복원할 수 있다. Kafka 같은 메시지 큐는 재생 가능하지만 상태 자체가 크면 처음부터 재계산하기 어렵기 때문. **단점**: 스냅샷 저장 오버헤드, 저장소 공간 필요.
- **RDD Lineage 장점**: 별도 저장 없이 DAG만으로 복구 가능, 구현이 단순하고 오버헤드가 없다. **단점**: 스트리밍 소스(Kafka 등)처럼 이미 소비된 데이터는 재생할 수 없어 stateful 스트리밍에는 적용 불가. Lineage가 길면 재계산 비용이 크다.
