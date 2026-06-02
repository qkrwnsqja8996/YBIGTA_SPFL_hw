# REPORT

이름: 박정현

---

## 01 · Flink 체크포인팅 — flink-basic

flink-conf.yaml에 설정한 값:

| 키 | 설정값 |
|------|--------|
| `execution.checkpointing.interval` | `10000` (10초마다 스냅샷) |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` (중복 집계 방지) |
| `state.backend` | `hashmap` (이 데이터 규모는 힙 메모리로 충분) |
| `taskmanager.numberOfTaskSlots` | `2` |
| `parallelism.default` | `3` |

> 가용 슬롯 = TM 3대 × 슬롯 2 = 6개 ≥ parallelism 3 이므로 잡이 PENDING 없이 RUNNING으로 진입한다.

시나리오 01 실행 중 Flink UI(http://localhost:8081)에서 확인한 내용:

- **Running Jobs**: `flink-counter` 잡이 RUNNING이며 Source→ParseEvent(map)→keyBy→CountingFunction 체인이 parallelism=3으로 분배되어 실행됨(출력 sink만 set_parallelism(1)). 6개 슬롯 중 일부 사용, 슬롯 부족으로 인한 SCHEDULED 정체 없음.
- **Checkpoints**: History 탭에 약 10초(interval=10000ms) 간격으로 체크포인트가 COMPLETED로 누적 기록됨. Summary에서 mode=EXACTLY_ONCE, 저장 경로는 conf의 `state.checkpoints.dir`.

측정 결과(`results/scenario_01.json`): `processing_latency_ms = 5698`, `events_per_second = 175.5`, 출력 해시가 기댓값과 일치(`f6ff1613…`).

체크포인팅 설정이 없으면 잡 제출이 거부되는 이유:

→ `jobs/flink_counter.py`의 `check_flink_config()`가 `execution.checkpointing.interval`·`execution.checkpointing.mode`·`state.checkpoints.dir`가 주석 해제되어 있는지 먼저 검사하고, 누락 시 `sys.exit(1)`로 잡을 올리지 않는다. 본질적으로 Flink는 키별 누적 카운트(ValueState)를 메모리에 들고 도는 stateful 스트리밍이므로, **상태를 어디에·얼마나 자주 스냅샷하고 복구 시 몇 번 처리(mode)를 보장할지**가 정해지지 않으면 장애 시 정확성을 보장할 수 없다. 그래서 이 설정이 잡 실행의 전제 조건이다.

---

## 02 · Spark 배치 — spark-batch

`results/scenario_02.json`에서:

| 항목 | 값 |
|------|-----|
| `rows_per_second` | `339` |
| `shuffle_partitions_used` | `6` |

Spark가 10,000행 CSV를 읽어 `event_type`별 `groupBy`로 sum/count/max를 집계하는 가장 기본적인 배치 동작을 확인했다. Spark UI(http://localhost:8080) → Completed Applications에 방금 끝난 `spark-aggregate` 잡이 나타났고, 잡의 Stages는 **CSV 읽기 + 집계 = 2개**로 나뉘었다. 집계(셔플 이후) stage의 태스크 수는 `spark.sql.shuffle.partitions` 값인 **6**과 일치했다(출력 해시도 기댓값 `55b1f846…`와 일치).

---

## 03 · Stream vs Batch

`results/scenario_03.json`에서:

| 항목 | 값 |
|------|-----|
| `flink_latency_ms` | `13362` |
| `spark_job_time_ms` | `33210` |
| `latency_ratio` | `2.5` |
| `flink_events_per_second` | `74.8` |
| `spark_events_per_second` | `30.1` |

**Latency 관점**: 동일한 1000개 이벤트를 처리할 때 Flink가 Spark보다 latency가 낮은 이유:

→ Flink는 TaskManager가 이미 떠 있는 상태에서 이벤트가 Kafka에 도착하는 **즉시 레코드 단위로 흘려보내며(상시 가동 파이프라인)** 처리한다. 반면 Spark는 데이터를 모두 모은 뒤 잡을 제출하고, 매 잡마다 JVM 기동 → SparkContext 생성 → DAG 계획 → 태스크 스케줄링 → 실행을 거친다. 그래서 "마지막 이벤트 이후 첫 결과까지"의 지연이 Spark에서 약 2.5배 길게 측정되었다.

**Throughput 관점**: latency만 보면 Spark가 불리해 보이지만 throughput 관점에서는 다르다.

→ 한 번 워밍업되면 Spark는 데이터를 파티션 단위로 클러스터 전체 코어에 뿌려 **대량을 한꺼번에 병렬 처리**하므로 단위 시간당 처리량이 매우 높다. 따라서 **대용량 야간 배치(수 GB~TB ETL, 일·시간 단위 집계/리포트)** 처럼 한 건의 지연보다 총 처리량·자원 효율이 중요한 작업에서는 Spark가 유리하다. 풍부한 SQL/DataFrame API, Catalyst 최적화, 대규모 셔플/조인 처리, 넓은 생태계 덕분에 실무 배치에서 여전히 표준으로 쓰인다. (latency = 한 건 처리까지 걸리는 시간 / throughput = 단위 시간당 처리량)

**JVM 오버헤드 관점**: 한 줄 정리:

→ `spark_job_time_ms`에는 매 잡마다 발생하는 JVM 부트업·컨텍스트 생성 고정비(수 초)가 포함되므로, 이 측정의 latency 차이는 "처리 모델(스트리밍 vs 배치)의 차이"와 "배치 엔진이 매번 잡을 새로 띄우는 구조적 고정비"가 합쳐진 결과이며 Spark가 본질적으로 느리다는 뜻은 아니다.

---

## 04 · Spark Shuffle + DAG

시나리오 04는 자동으로 두 번 spark-submit을 실행한다:
- **baseline**: `shuffle.partitions = 200`으로 측정
- **본 실행**: conf에 채운 값(6)으로 측정

`results/scenario_04.json`에서:

| 항목 | 값 |
|------|-----|
| `shuffle_partitions_used` | `6` |
| `job_time_ms` | `39459` |
| `baseline_time_ms` (200 기준) | `46899` |
| `speedup_factor` | `1.19` |

**Spark UI 관찰** (http://localhost:8080) — Stages 탭에서 확인한 태스크 수:

→ 본 실행(partition=6)에서는 groupBy 셔플 이후 stage의 태스크 수가 **6개**로, 설정한 `spark.sql.shuffle.partitions = 6`과 정확히 일치했다. baseline(200)에서는 같은 stage가 **200개** 태스크로 쪼개졌다. 즉 **셔플 파티션 수 = 셔플 이후 stage의 태스크 수**임을 직접 확인했다.

**DAG Visualization** 확인 내용:

→ 잡이 **2개 stage**로 분할되었다. Stage 1은 CSV read + map/부분 집계(ShuffleMapStage), Stage 2는 셔플 데이터를 읽어 최종 집계(ResultStage)다. 두 stage 사이에 `Exchange`(셔플) 노드가 있고, **groupBy 지점이 바로 stage 경계(셔플 경계)** 가 된다 — 같은 키를 한 파티션으로 모아야 하므로 네트워크 재분배가 일어나고, 그 지점에서 stage가 끊긴다.

`shuffle.partitions = 200`이 이 클러스터(코어 6개)에서 비효율적인 이유:

→ 동시에 처리 가능한 태스크는 worker 3대 × 2코어 = **6개**뿐인데 파티션을 200개로 두면, 대부분이 비거나 극소량인 파티션 200개가 6개 코어를 33번 이상 나눠 돌아야 한다. 실제 연산량보다 **태스크 스케줄링·직렬화·네트워크 셋업 같은 태스크당 고정비**가 지배적이 되어 오버헤드가 폭증한다. 클러스터 코어 수의 1~2배(여기선 6~12)가 적절하다. (이번 데이터셋은 50,000행으로 작아 두 번째 submit의 JVM 재기동 비용이 partition 효과를 상당 부분 상쇄해 speedup이 1.19로 작게 나왔지만, 태스크 수 차이(6 vs 200)는 Stages 탭에서 분명하게 관찰된다.)

---

## 05 · Pipeline + Fault Tolerance

시나리오 05는 Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작하는지 확인한다.

`results/scenario_05.json`에서:

| 항목 | 값 |
|------|-----|
| `events_injected` | `2000` |
| `flink_output_sha256` | `eb395e4aab6aa623087308a8aeed3854b5450d134a37b3038d46d0b1bdbe9367` |
| `spark_output_sha256` | `bb68c7a165e1a9b53f602d8abd6814036a9b51f5a3f024c934371f5fa7295c3c` |
| `passed` | `true` |

2000개 이벤트가 Kafka로 주입 → Flink가 type별로 카운트하여 `counts.txt`로 출력 → 그 출력을 CSV로 변환해 Spark가 다시 집계하는 전 과정이 정확히 통과했다(Flink 출력 해시가 기댓값과 일치, Spark 최종 집계도 정상 산출).

flink-conf.yaml에는 체크포인팅 설정을 작성했지만, spark-defaults.conf에는 이에 대응하는 설정이 없다.

Spark가 fault tolerance(노드 장애 시 복구)를 제공하는 방식:

→ **RDD Lineage(계보)** 로 복구한다. RDD는 불변(immutable)이며, 각 RDD는 "어떤 부모 RDD에 어떤 변환(map/filter/groupBy 등)을 적용해 만들어졌는지"의 계보(DAG)를 기록한다. 노드 장애로 특정 파티션이 유실되면, 전체를 주기적 스냅샷에서 되돌리는 것이 아니라 **lineage를 거슬러 올라가 유실된 그 파티션만 부모 데이터로부터 재계산(recompute)** 하여 복원한다. 따라서 Flink 같은 별도의 주기적 상태 스냅샷이 필요 없다(계보가 매우 길거나 셔플이 반복되면 `checkpoint()`로 계보를 잘라 재계산 비용을 제한할 수 있다).

Flink의 체크포인팅 방식과 비교했을 때의 차이점, 그리고 각 방식의 장단점:

→
- **Flink(주기적 체크포인팅)**: 무한히 도는 스트리밍 잡의 연산자 상태(키별 카운트 등)를 주기적으로 외부 경로에 스냅샷하고, 장애 시 가장 최근 스냅샷 + Kafka 소스 오프셋으로 복구하며 EXACTLY_ONCE를 보장한다. *장점*: 끝나지 않는 스트림·큰 누적 상태에 적합하고 복구가 빠르다. *단점*: 스냅샷 저장 오버헤드와 외부 스토리지가 필요하다.
- **Spark(lineage 재계산)**: 시작과 끝이 있는 유한 배치 DAG에 적합하다. 상태 스냅샷을 따로 두지 않고 유실 파티션만 재계산한다. *장점*: 구조가 단순하고 평상시 저장 비용이 없다. *단점*: 계보가 길면 재계산 비용이 커지고(→ checkpoint로 보완), 무한 스트림의 누적 상태 복구에는 부적합하다.
- **요약**: 유한한 배치(재계산이 저렴)에는 *lineage 재계산*이, 무한 스트리밍(상태 유실을 재계산으로 되돌리기 어려움)에는 *주기적 체크포인트*가 각 처리 모델에 맞는 fault tolerance 전략이다.
