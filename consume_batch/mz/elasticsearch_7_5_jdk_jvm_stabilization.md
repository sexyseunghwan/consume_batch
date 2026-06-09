# Elasticsearch 7.5.0 특정 노드 다운 이슈 원인 분석 및 안정화 조치

## 1. 문서 목적

본 문서는 Windows 환경에서 운영 중인 Elasticsearch 7.5.0 클러스터에서 특정 노드가 반복적으로 다운되던 이슈에 대해, 원인 분석 결과와 최종 조치 내용을 정리하기 위한 문서이다.

해당 이슈는 JVM 튜닝, 백신 비활성화, OS/리소스 점검 등 여러 조치 후에도 재현되었으나, 최종적으로 Elasticsearch 7.5.0 번들 JDK 버전 이슈로 판단하였다.

이후 Elasticsearch 실행 JDK를 `11.0.30`으로 변경하고 JVM 옵션을 표준화한 뒤, 약 2개월간 동일한 노드 다운 현상이 재발하지 않고 있다.

---

## 2. 문제 상황

Elasticsearch 클러스터 운영 중 특정 노드가 간헐적으로 다운되는 현상이 지속적으로 발생하였다.

주요 특징은 다음과 같았다.

- 특정 노드가 예고 없이 다운됨
- Elasticsearch 로그에는 명확한 애플리케이션 레벨 오류가 남지 않음
- 모니터링상 TCP wait 수치와 시스템 메모리 사용량이 증가하는 패턴이 관찰됨
- 재시작 후에는 정상적으로 클러스터에 복귀함
- 일정 시간이 지난 뒤 동일 또는 유사한 형태로 다시 발생함

초기에는 Elasticsearch 설정, JVM 옵션, OS 리소스, 보안 프로그램, 네트워크 상태 등 다양한 가능성을 열어두고 원인을 분석하였다.

---

## 3. 장애 분석 과정

### 3-1. Fatal JVM Error 로그 수집

일반 Elasticsearch 로그만으로는 원인을 확인하기 어려워, JVM fatal error 로그와 heap dump를 남길 수 있도록 옵션을 추가하였다.

```text
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:HeapDumpPath=/var/lib/elasticsearch
```

이후 장애 발생 시 `hs_err_pid*.log` 파일이 생성되었고, 해당 로그에서 다음과 같은 JVM 크래시 정보를 확인하였다.

```text
# A fatal error has been detected by the Java Runtime Environment:
#
#  EXCEPTION_ACCESS_VIOLATION (0xc0000005) at pc=0x0000000000000046
#
# JRE version: OpenJDK Runtime Environment (13.0.1+9)
# Java VM: OpenJDK 64-Bit Server VM (13.0.1+9, mixed mode, sharing, tiered, compressed oops, g1 gc, windows-amd64)
# Problematic frame:
# C  0x0000000000000046
```

### 3-2. 주요 분석 포인트

로그에서 확인된 핵심 포인트는 다음과 같다.

- `EXCEPTION_ACCESS_VIOLATION (0xc0000005)` 발생
- JVM 레벨에서 OS로부터 치명적 예외를 받음
- 문제 프레임이 Java 코드가 아닌 native 영역으로 표시됨
- 실행 주소가 `0x0000000000000046`으로 매우 낮은 비정상 주소임
- 크래시 당시 스레드가 `GCTaskThread "GC Thread#4"`로 확인됨
- Elasticsearch 7.5.0 번들 JDK인 OpenJDK `13.0.1`로 실행 중이었음

특히 `0x0000000000000046`과 같은 near-null 주소에서 실행을 시도했다는 점은 일반적인 Java 예외가 아니라 JVM, native code, JIT, GC, OS/드라이버/보안툴 조합 등에서 발생할 수 있는 프로세스 크래시 유형으로 판단하였다.

또한 크래시 스레드가 G1GC 워커 스레드였기 때문에, 기존에 적용되어 있던 custom G1GC 튜닝 옵션이 JVM 안정성에 영향을 줄 가능성도 함께 검토하였다.

---

## 4. 기존 대응 및 한계

장애 원인을 찾기 위해 다음과 같은 조치를 순차적으로 검토 및 적용하였다.

### 4-1. JVM 옵션 조정

기존 JVM 옵션에는 G1GC 관련 custom/experimental 옵션이 다수 포함되어 있었다.

예시:

```text
-XX:G1ReservePercent=40
-XX:InitiatingHeapOccupancyPercent=35
-XX:+UnlockExperimentalVMOptions
-XX:G1MixedGCLiveThresholdPercent=90
-XX:G1HeapRegionSize=8M
-XX:G1NewSizePercent=30
-XX:G1MaxNewSizePercent=40
-XX:G1HeapWastePercent=5
-XX:+AlwaysPreTouch
```

해당 옵션들은 GC pause time이나 heap 사용 패턴을 제어하기 위해 사용할 수 있으나, 운영 환경에서 충분한 검증 없이 과도하게 적용할 경우 JVM 내부 동작을 불안정하게 만들 수 있다.

특히 이번 장애는 G1GC worker thread에서 발생한 native crash였기 때문에, GC 관련 custom tuning을 제거하고 JVM 기본 정책에 더 가깝게 되돌리는 방향으로 조정하였다.

### 4-2. 백신 및 보안 프로그램 영향 검토

Windows 환경에서 JVM native crash가 발생하는 경우, 백신/보안 프로그램의 DLL injection 또는 file scan 영향도 가능성이 있다.

따라서 보안 프로그램 비활성화 또는 예외 처리도 검토하였으나, 해당 조치만으로는 현상이 완전히 해소되지 않았다.

### 4-3. OS 및 리소스 상태 점검

시스템 메모리, TCP wait, 디스크, 프로세스 상태 등을 함께 확인하였다.

다만 일반적인 OOM, disk full, Elasticsearch application error와 같은 명확한 원인은 확인되지 않았다.

---

## 5. 최종 원인 판단

최종적으로 해당 이슈는 Elasticsearch 7.5.0에서 제공되는 번들 JDK 버전과 Windows 운영 환경 조합에서 발생한 JVM 안정성 이슈로 판단하였다.

근거는 다음과 같다.

- 장애 로그에서 Java exception이 아닌 JVM fatal crash가 발생함
- `JRE version: OpenJDK Runtime Environment (13.0.1+9)`로 확인됨
- Elasticsearch 7.5.0의 기본 번들 JDK가 OpenJDK 13.0.1임
- 크래시가 G1GC worker thread에서 발생함
- custom G1GC 옵션 제거만으로는 재발 가능성을 완전히 배제하기 어려웠음
- JDK를 `11.0.30`으로 변경한 이후 약 2개월간 동일 이슈가 재발하지 않음

따라서 이번 장애는 Elasticsearch 자체의 query/indexing 로직 문제라기보다는, 번들 JDK 13.0.1 기반 JVM native crash 문제에 가까운 것으로 정리하였다.

---

## 6. 최종 조치 내용

### 6-1. Elasticsearch 실행 JDK 변경

Elasticsearch 7.5.0의 번들 JDK를 그대로 사용하지 않고, 실행 JDK를 `11.0.30`으로 변경하였다.

변경 후 약 2개월간 운영 모니터링 결과, 기존에 반복되던 특정 노드 다운 현상은 재발하지 않았다.

### 6-2. JVM 옵션 표준화

기존 노드별 JVM 옵션 편차를 제거하고, 모든 노드에 동일한 JVM 옵션을 적용하였다.

표준화 방향은 다음과 같다.

- heap size는 고정값으로 유지
- 31GB 메모리 서버 기준 compressed oops 임계값을 넘지 않도록 heap 유지
- G1GC는 유지하되 experimental/custom G1 sizing 옵션 제거
- GC log 설정은 하나의 명확한 파일 경로로 통일
- heap dump 및 fatal JVM error log 경로 명시
- 모든 노드에 동일 옵션 적용 후, 노드별 순차 재시작

---

## 7. 표준 JVM 옵션

아래 옵션을 Elasticsearch 7.5.0 클러스터 전체 노드에 동일하게 적용한다.

```text
################################################################
## Recommended JVM options for NODE-3 / Elasticsearch 7.5.0
##
## Purpose:
## - Remove custom / experimental G1GC tuning that increases JVM crash risk.
## - Keep heap fixed and below the compressed-oops threshold.
## - Keep one clear GC log configuration for postmortem analysis.
##
## Apply this consistently to every node in the cluster,
## then restart nodes one at a time.
################################################################

################################################################
## Heap
################################################################
##
## Current node memory: about 31 GB
## Current heap: about 15.8 GB
##
## Keep Xms and Xmx identical.
## Do not increase above this value on a 31 GB host.
##
## If JVM crashes continue after removing custom G1 options,
## test 14g instead to give more room to native memory and filesystem cache.
##
## Conservative fallback:
## -Xms14g
## -Xmx14g

-Xms16198m
-Xmx16198m

################################################################
## GC
################################################################
##
## Keep G1GC, but let the JVM choose most internal sizing values.
## The previous options forced young generation size and enabled
## experimental G1 knobs.
## The crash happened in a JVM G1 GC worker thread,
## so reducing non-default GC tuning is safer.

-XX:+UseG1GC
-XX:MaxGCPauseMillis=200

################################################################
## GC logging for JDK 9+
################################################################
##
## Disable the built-in/default log outputs first,
## then define one GC log target.
## This avoids duplicate GC files such as relative logs/gc.log
## and absolute D:\...gc.log.

9-:-Xlog:disable
9-:-Xlog:all=warning:stderr:utctime,level,tags
9-:-Xlog:gc*,gc+age=trace,safepoint:file=D:\Elastic\Elasticsearch\logs\gc.log:utctime,pid,tags:filecount=32,filesize=64m

################################################################
## Heap dump and fatal JVM error log
################################################################

-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=D:\Elastic\Elasticsearch\logs\heapdump.hprof
-XX:ErrorFile=D:\Elastic\Elasticsearch\logs\hs_err_pid%p.log

################################################################
## Temporary directory
################################################################

-Djava.io.tmpdir=${ES_TMPDIR}

################################################################
## JDK 8 GC logging
################################################################
##
## These lines only apply if this node is ever run with JDK 8.
## Elasticsearch 7.5.0 originally starts with bundled OpenJDK 13.0.1,
## so the JDK 9+ logging section above is the active one
## when running with JDK 11.0.30.

8:-XX:+PrintGCDetails
8:-XX:+PrintGCDateStamps
8:-XX:+PrintTenuringDistribution
8:-XX:+PrintGCApplicationStoppedTime
8:-Xloggc:D:\Elastic\Elasticsearch\logs\gc.log
8:-XX:+UseGCLogFileRotation
8:-XX:NumberOfGCLogFiles=32
8:-XX:GCLogFileSize=64m

################################################################
## Deliberately removed from the old file
################################################################
##
## Do not re-add these unless there is a tested reason.
##
## -XX:G1ReservePercent=40
## -XX:InitiatingHeapOccupancyPercent=35
## -XX:+UnlockExperimentalVMOptions
## -XX:G1MixedGCLiveThresholdPercent=90
## -XX:G1HeapRegionSize=8M
## -XX:G1NewSizePercent=30
## -XX:G1MaxNewSizePercent=40
## -XX:G1HeapWastePercent=5
## -XX:+AlwaysPreTouch
##
## Notes:
## - AlwaysPreTouch may still be added by Elasticsearch defaults.
## - The key change here is removing experimental/custom G1 sizing.
```

---

## 8. 적용 절차

클러스터 안정성을 위해 모든 노드를 동시에 재시작하지 않고, 반드시 한 대씩 순차적으로 적용한다.

1. 대상 노드의 현재 JDK 버전과 Elasticsearch 실행 JDK 경로를 확인한다.
2. Elasticsearch 실행 JDK를 `11.0.30`으로 변경한다.
3. `jvm.options` 또는 운영 환경의 JVM 옵션 파일을 표준 옵션으로 교체한다.
4. GC log, heap dump, fatal error log 경로가 실제로 존재하는지 확인한다.
5. 대상 노드 한 대만 재시작한다.
6. 클러스터 health가 `green` 또는 운영 기준상 안정 상태로 복귀하는지 확인한다.
7. shard relocation, search/indexing 지연, GC log, Elasticsearch log를 확인한다.
8. 이상 없을 경우 다음 노드에 동일 절차를 반복한다.

---

## 9. 적용 후 확인 항목

### 9-1. JDK 버전 확인

Elasticsearch 프로세스가 실제로 `11.0.30`으로 실행 중인지 확인한다.

```text
java -version
```

또는 Elasticsearch process의 java path, startup log, process command line을 통해 확인한다.

### 9-2. Elasticsearch cluster 상태 확인

```text
GET _cluster/health
GET _cat/nodes?v
GET _cat/shards?v
```

확인 항목:

- node 이탈 여부
- shard unassigned 여부
- relocation 장기 지속 여부
- search/indexing latency 변화
- heap 사용률 및 GC 빈도

### 9-3. JVM fatal error log 확인

아래 경로에 신규 fatal error log가 발생하는지 확인한다.

```text
D:\Elastic\Elasticsearch\logs\hs_err_pid%p.log
```

### 9-4. GC log 확인

아래 경로에 GC log가 정상적으로 단일 위치에 기록되는지 확인한다.

```text
D:\Elastic\Elasticsearch\logs\gc.log
```

---

## 10. 조치 결과

JDK를 `11.0.30`으로 변경하고 JVM 옵션을 표준화한 이후, 약 2개월간 동일한 특정 노드 다운 현상이 재발하지 않았다.

현재까지의 관찰 결과를 기준으로 보면, 이번 장애의 핵심 원인은 다음 두 가지 조합으로 정리할 수 있다.

1. Elasticsearch 7.5.0 번들 JDK 13.0.1 기반 JVM native crash 가능성
2. custom/experimental G1GC 튜닝 옵션으로 인한 JVM 안정성 저하 가능성

최종 조치의 핵심은 다음과 같다.

- 번들 JDK 사용 중단
- JDK `11.0.30` 적용
- custom/experimental G1GC 옵션 제거
- 모든 노드의 JVM 옵션 통일
- GC log 및 fatal error log 수집 경로 명확화

---

## 11. 재발 방지 기준

향후 Elasticsearch JVM 옵션 변경 시 다음 기준을 따른다.

- 운영 클러스터에 experimental JVM 옵션을 직접 적용하지 않는다.
- GC 관련 옵션은 명확한 성능 테스트와 rollback 계획이 있을 때만 변경한다.
- JVM 옵션은 노드별로 임의 변경하지 않고 클러스터 표준으로 관리한다.
- JDK 변경 시 최소 1개 노드에서 선적용 후 안정성 확인 기간을 둔다.
- fatal JVM crash 발생 시 `hs_err_pid*.log`, GC log, Elasticsearch log, Windows Event Log를 함께 수집한다.
- 노드 다운이 재발하면 heap을 `14g`로 낮춰 native memory와 filesystem cache 여유를 확보하는 방안도 검토한다.

---

## 12. 결론

이번 장애는 Elasticsearch application log만으로는 원인 파악이 어려운 JVM native crash 유형이었다.

초기에는 JVM 튜닝, 백신, OS 리소스 등 다양한 가능성을 검토했으나, fatal error log 분석 결과 Elasticsearch 7.5.0 번들 JDK 13.0.1 기반의 JVM 크래시 가능성이 가장 높다고 판단하였다.

JDK를 `11.0.30`으로 변경하고, JVM 옵션을 보수적으로 표준화한 뒤 현재까지 약 2개월간 동일 이슈가 재발하지 않고 있다.

따라서 현재 기준의 권장 운영안은 다음과 같다.

- Elasticsearch 7.5.0 번들 JDK를 사용하지 않는다.
- JDK `11.0.30`을 사용한다.
- custom/experimental G1GC 옵션은 제거 상태를 유지한다.
- 동일 JVM 옵션을 전체 노드에 일관되게 적용한다.
- fatal error log와 GC log를 반드시 수집해 사후 분석 가능성을 확보한다.
