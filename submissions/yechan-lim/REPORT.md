# REPORT

이름: 임예찬 (Yechan Lim)

> 실행 환경: Docker Desktop(WSL2 backend) · Flink 1.17.2 · Spark 3.4.4
> 클러스터: Flink JobManager×1 + TaskManager×3(슬롯 2 → 총 6슬롯), Spark Master×1 + Worker×3(2코어 → 총 6코어), Kafka×1
> 최종 결과: `results/result.json` 기준 **5/5 passed, penalty_ms = 0** (모든 시나리오 output sha256 = expected sha256)

---

## 01 · Flink 체크포인팅 — flink-basic

flink-conf.yaml에 설정한 값:

| 키 | 설정값 |
|------|--------|
| `execution.checkpointing.interval` | `10000` (10초마다 스냅샷) |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` |
| `state.checkpoints.dir` | `file:///data/flink-checkpoints` |
| `state.backend` | `hashmap` |
| `taskmanager.numberOfTaskSlots` | `2` |
| `parallelism.default` | `3` |

시나리오 01 실행 중 확인한 내용 (Flink REST `/overview`, `/jobs`):

- **Running Jobs / parallelism**: `flink_counter` 잡이 RUNNING 상태로 올라왔고, 클러스터는 TaskManager 3대 × 슬롯 2 = **가용 슬롯 6개**로 떠 있다. `parallelism.default = 3`이 가용 슬롯(6) 이하이므로 잡이 PENDING/SCHEDULED에서 멈추지 않고 정상 스케줄링되었다. (슬롯 6개 중 잡 parallelism 3만큼 사용)
- **Checkpoints**: `interval = 10000ms`로 설정되어 처리 중 주기적으로 스냅샷이 기록되었다. 1000개 이벤트를 주입하자 마지막 이벤트 주입 완료 시점부터 출력 파일에 결과가 나타나기까지 `processing_latency_ms = 4547ms`, 처리량 `events_per_second = 219.9`로 측정되었고, 출력 해시가 기댓값과 일치(`f6ff16…faa`)해 통과했다.

**체크포인팅 설정이 없으면 잡 제출이 거부되는 이유**:

→ Flink는 스트리밍 처리 중 집계값·오프셋 같은 **상태(state)** 를 메모리에 들고 처리한다. 노드 장애가 나면 이 메모리 상태가 사라지므로, 주기적으로 상태 전체를 외부 경로에 스냅샷으로 찍어 두고 장애 시 가장 최근 스냅샷에서 복구한다. 이 잡(`flink_counter`)은 Kafka 소스 + 키별 카운트라는 **상태 기반(stateful) 스트리밍**이라, 정확한 복구 보장을 위해 체크포인팅 활성화가 전제된다. 그래서 체크포인팅 관련 설정(interval 등)이 비어 있으면 JobManager가 잡 제출 자체를 거부하고, 시나리오 01이 즉시 실패한다. `mode = EXACTLY_ONCE`는 장애 복구 후에도 동일 이벤트가 두 번 집계되지 않도록(정확히 한 번) 보장하는 설정이다.

---

## 02 · Spark 배치 — spark-batch

`results/scenario_02.json`에서:

| 항목 | 값 |
|------|-----|
| `rows_processed` | 10000 |
| `rows_per_second` | 561 |
| `shuffle_partitions_used` | 6 |
| `job_time_ms` | 17817 |

Spark가 CSV(10,000행)를 읽어 `event_type`별 `groupBy` 집계(sum/count/max)를 수행하는 가장 기본적인 배치 동작을 확인하는 시나리오다. `spark.sql.shuffle.partitions = 6`이 그대로 반영되어 `shuffle_partitions_used = 6`으로 측정되었고, 출력 해시가 기댓값과 일치(`55b1f8…2df`)해 통과했다.

처리 흐름은 두 단계로 나뉜다: **(1) CSV 읽기 + 부분 집계(map-side)** → **(2) 셔플 후 키별 최종 집계(reduce-side)**. groupBy가 셔플 경계가 되어 stage가 분리되며, 두 번째 stage의 태스크 수가 `shuffle.partitions` 값(=6)과 같아진다. `rows_per_second`(561)에는 spark-submit마다 새로 뜨는 JVM 부트업 시간이 포함되어 있어, 1만 행 규모치고 절대 시간(약 17.8초)은 작업량보다 기동 오버헤드의 영향이 크다.

---

## 03 · Stream vs Batch

`results/scenario_03.json`에서:

| 항목 | 값 |
|------|-----|
| `flink_latency_ms` | 1914 |
| `spark_job_time_ms` | 17420 |
| `latency_ratio` | 9.1 |
| `flink_events_per_second` | 522.5 |
| `spark_events_per_second` | 57.4 |

**Latency 관점** — 동일한 1000개 이벤트인데 Flink latency가 Spark보다 약 9.1배 낮은 이유:

→ Flink는 이벤트가 Kafka에 도착하는 **즉시(record-at-a-time)** 처리해 상태를 갱신하므로, 마지막 이벤트가 들어온 직후 곧바로 결과가 나온다(1914ms). 반면 Spark는 데이터를 **모두 모은 뒤** 잡을 제출하고, 그때부터 JVM 기동 → DAG 생성 → 태스크 스케줄링 → 실행을 거친다(17420ms). 즉 "첫 결과가 나오기까지의 시간"에서는 항상 켜져 있는 스트리밍 엔진(Flink)이 매번 새로 시작하는 배치 엔진(Spark)보다 구조적으로 유리하다.

**Throughput 관점** — latency만 보면 Spark가 불리하지만 throughput은 다르게 봐야 한다:

→ latency는 "한 건/한 배치가 끝나기까지의 시간"이고 throughput은 "단위 시간당 처리량"이다. 이 시나리오는 1000건이라는 **작은 데이터**라 Spark의 고정 기동 비용(JVM 부트업)이 전체 시간을 지배해 throughput이 낮게(57.4 ev/s) 나온다. 하지만 데이터가 수억 건 규모로 커지면 이 고정 비용은 전체에서 차지하는 비중이 미미해지고, Spark는 파티션 단위로 **대량 데이터를 병렬·일괄 처리**하는 데 최적화되어 단위 시간당 처리량이 매우 높아진다. 그래서 "밤사이 한 번 도는 대용량 야간 배치(ETL, 집계 리포트)"처럼 즉시성보다 총처리량·비용 효율이 중요한 작업에는 Spark가 더 적합하다. 반대로 실시간 이상탐지·대시보드처럼 낮은 지연이 중요한 작업에는 Flink가 맞다.

**JVM 오버헤드 관점** — 한 줄 정리:

→ `spark_job_time_ms`(17.4초)의 상당 부분은 처리 자체가 아니라 **매 잡마다 새로 JVM을 띄우는 부트업 오버헤드**이며, TaskManager가 이미 떠 있는 Flink엔 이 비용이 없다. 따라서 이 latency 차이는 "Spark가 본질적으로 느리다"가 아니라 **"배치 엔진은 잡을 매번 새로 시작하는 구조라 짧은 작업에서 손해를 본다"** 는 뜻이다. (양쪽 출력 모두 정확해 PASS)

---

## 04 · Spark Shuffle + DAG

시나리오 04는 자동으로 두 번 spark-submit을 실행한다:
- **baseline**: `shuffle.partitions = 200`으로 측정
- **본 실행**: conf에 채운 값(=6)으로 측정

`results/scenario_04.json`에서:

| 항목 | 값 |
|------|-----|
| `shuffle_partitions_used` | 6 |
| `job_time_ms` | 16912 |
| `baseline_time_ms` (200 기준) | 17916 |
| `speedup_factor` | 1.06 |
| `rows_processed` | 50000 |
| `rows_per_second` | 2956 |

**태스크 수 관찰** (Stages 탭에 대응):

→ 집계(groupBy) 잡은 셔플을 기준으로 두 stage로 나뉜다. **셔플 이후(집계) stage의 태스크 수 = `spark.sql.shuffle.partitions`** 이다. 본 실행은 partitions=6이므로 집계 stage가 **6개 태스크**로 분할되어 6코어에 정확히 1:1로 매핑된다. baseline(200)에서는 같은 집계 stage가 **200개 태스크**로 쪼개지는데, 실제 키는 5종(event_type_0~4)뿐이라 대부분이 **빈(empty) 파티션**이고, 6개 코어가 200개 태스크를 여러 라운드에 걸쳐 순차 처리하면서 태스크 스케줄링·직렬화 오버헤드만 늘어난다.

**DAG Visualization 관찰**:

→ DAG는 `read CSV → (map-side 부분집계)` **Stage 1** 과 `exchange(shuffle) → 키별 최종 집계 → write` **Stage 2** 로 그려지며, 두 stage 사이의 경계(Exchange/셔플)가 바로 **groupBy 지점**이다. groupBy는 같은 키의 레코드를 한 파티션으로 모아야 하므로 네트워크를 통한 데이터 재분배(셔플)가 필요하고, 셔플이 일어나는 곳에서 stage가 끊긴다.

**`shuffle.partitions = 200`이 이 클러스터(코어 6개)에서 비효율적인 이유**:

→ 파티션 하나당 태스크 하나가 생기는데, 코어는 6개뿐이라 200개 태스크를 동시에 처리할 수 없고 약 34라운드(⌈200/6⌉)로 나눠 돌게 된다. 게다가 데이터 키가 5종뿐이라 200개 중 대부분은 처리할 데이터가 없는 빈 파티션이다. 결국 실제 연산량 대비 **태스크 생성·스케줄링 오버헤드가 폭증**한다. 적정값은 클러스터 코어 수의 1~2배(여기선 6)로, 코어 대비 태스크가 균형을 이뤄 오버헤드가 최소화된다.

> 참고: 이 데이터셋 규모(5만 행)에서는 두 번째 spark-submit의 JVM 재기동 비용이 partition 효과를 상당 부분 묻어버려 `speedup_factor`가 1.06으로 작게 나왔다(때로는 1 미만으로도 나올 수 있음). 핵심 학습 포인트는 절대 속도가 아니라 **"파티션 수 = 태스크 수"** 라는 구조이며, 이는 Stages 탭의 태스크 수(6 vs 200) 차이로 직접 확인된다.

---

## 05 · Pipeline + Fault Tolerance

시나리오 05는 Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작하는지 확인한다.

`results/scenario_05.json`에서:

| 항목 | 값 |
|------|-----|
| `events_injected` | 2000 |
| `flink_output_sha256` | `eb395e4a…9367` |
| `spark_output_sha256` | `bb68c7a1…5c3c` |
| `passed` | true |

2000개 이벤트가 Kafka로 주입 → Flink가 실시간으로 키별 카운트 집계 → 그 결과를 CSV로 변환 → Spark가 다시 배치 집계하는 흐름이 한 번에 통과했다. Flink 출력 해시가 기댓값과 일치하고 Spark 단계도 정상 산출되어 PASS.

**Spark가 fault tolerance(노드 장애 시 복구)를 제공하는 방식 — RDD Lineage**:

→ Spark는 상태를 외부에 스냅샷으로 저장하는 대신, 각 RDD/DataFrame이 "어떤 입력으로부터 어떤 변환(map, filter, groupBy …)을 거쳐 만들어졌는지"의 **계보(lineage, DAG)** 를 기억한다. 실행 중 특정 파티션이 있던 Executor(노드)가 죽으면, Spark는 잃어버린 그 파티션만 **lineage를 따라 원본 데이터부터 다시 계산(recompute)** 해서 복구한다. 즉 "결과를 저장해 두는" 방식이 아니라 "만드는 방법(레시피)을 저장해 두고 필요할 때 다시 만드는" 방식이다.

**Flink 체크포인팅과의 비교 / 장단점**:

→
- **Flink(체크포인팅·스냅샷)**: 무한히 흐르는 스트림에서는 "원본부터 다시 계산"이 불가능(입력이 끝나지 않음)하므로, 주기적으로 상태 전체를 외부 경로에 스냅샷으로 찍어 두고 장애 시 가장 최근 스냅샷으로 되돌린 뒤 그 지점부터 재개한다. `EXACTLY_ONCE`로 중복 없는 복구까지 보장한다. 장점: 상시 실행되는 무한 스트림에 적합하고 복구가 빠르며 정확히-한-번 보장이 가능. 단점: 주기적 스냅샷을 외부 저장소에 써야 하므로 그만큼의 I/O 오버헤드와 저장 경로 설정이 필요.
- **Spark(RDD Lineage·재계산)**: 유한한 배치 데이터에서는 원본이 그대로 남아 있어 잃은 파티션만 다시 계산하면 되므로 별도의 상태 스냅샷 설정이 필요 없다(그래서 spark-defaults.conf엔 대응 설정이 없음). 장점: 설정이 단순하고 배치에 자연스러움. 단점: 변환 사슬이 길면 재계산 비용이 커질 수 있어, 이럴 땐 `persist()`/`checkpoint()`로 중간 결과를 보존해 lineage를 끊어 준다.
- **요약**: 두 엔진의 차이는 데이터 모델의 차이에서 나온다 — **무한 스트림(Flink)은 "상태를 스냅샷"** 하고, **유한 배치(Spark)는 "계보를 따라 재계산"** 한다.
