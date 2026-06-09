문제상황
 

Windows OS 기준으로 Elasticsearch 클러스터를 운영중에 한번씩 특정 노드가 down 되는데,

로그가 따로 남지 않고, 모니터링 쪽에서는 TCP wait 수치와 시스템 메모리 수치만 올라가는 현상이 존재하였다.

그래서 elasticsearch cluster jvm.option 설정에 아래를 추가하여 모니터링을 진행하였다.

 



-XX:+HeapDumpOnOutOfMemoryError
# exit right after heap dump on out of memory error
-XX:+ExitOnOutOfMemoryError
# specify an alternative path for heap dumps; ensure the directory exists and
# has sufficient space
-XX:HeapDumpPath=/var/lib/elasticsearch
# specify an alternative path for JVM fatal error logs
 

그리고 장애가 발생하였을 때, hs_err_pid3876.log 가 남았고 해당 로그 파일을 분석해보려고 한다.

 

 

 



#
# A fatal error has been detected by the Java Runtime Environment:
#
#  EXCEPTION_ACCESS_VIOLATION (0xc0000005) at pc=0x0000000000000046, pid=3876, tid=2692
#
# JRE version: OpenJDK Runtime Environment (13.0.1+9) (build 13.0.1+9)
# Java VM: OpenJDK 64-Bit Server VM (13.0.1+9, mixed mode, sharing, tiered, compressed oops, g1 gc, windows-amd64)
# Problematic frame:
# C  0x0000000000000046
#
# Core dump will be written. Default location: C:\Program Files\Elastic\Elasticsearch\7.5.0\hs_err_pid3876.mdmp
#
# If you would like to submit a bug report, please visit:
#   https://github.com/AdoptOpenJDK/openjdk-build/issues


EXCEPTION_ACCESS_VIOLATION (0xc0000005) at pc=0x0000000000000046, pid=3876, tid=2692
 

위의 로그를 살펴보면 JVM 자체가 OS로부터 치명적 예외(0xC0000005) 를 받았고, 그때 CPU가 가리키던 실행 주소가 0x0000000000000046이라는 뜻이 된다.

0x0000000000000046 같은 "매우 낮은 주소"는 정상적인 코드 위치가 아니어서, 보통은 깨진 리턴 주소/함수 포인터나 JIT/네이티브 코드가 잘못된 주소로 점프한 상황일때 발생한다.

 

핵심 진단 포인트 - 요약 부
문제 프레임: C 0x0000000000000046

C는 “네이티브 프레임(C/native)“를 뜻한다. 즉, 순수 자바 바이트코드가 아니라 JVM/OS/네이티브 라이브러리 쪽에서 난 크래시라는 뜻이다.

플랫폼/버전: Windows, JRE 13.0.1 (ES 7.5.0 번들 JDK)

Windows 에서 오래된 JDK,드라이버,보안툴 조합은 네이티브 크래시를 유발하기 쉬움

 

 

 

1️⃣ Thread 영역
 



---------------  T H R E A D  ---------------
Current thread (0x0000010f11d0e000):  GCTaskThread "GC Thread#4" [stack: 0x0000000c50f00000,0x0000000c51000000] [id=2692] 
Stack: [0x0000000c50f00000,0x0000000c51000000],  sp=0x0000000c50fff218,  free space=1020k
Native frames: (J=compiled Java code, j=interpreted, Vv=VM code, C=native code)
C  0x0000000000000046
siginfo: EXCEPTION_ACCESS_VIOLATION (0xc0000005), data execution prevention violation at address 0x0000000000000046
 

크래시에서 문제가 난 스레드의 상태를 덤프하는 구간의 시작



Current thread (0x0000010f11d0e000):  GCTaskThread "GC Thread#4" [stack: 0x0000000c50f00000,0x0000000c51000000] [id=2692] 
Current thread (0x0000010f11d0e000) : *HotSpot 내부에서 이 스레드를 가리키는 *VM 핸들(주소)

GCTaskThread "GC Thread#4": GC를 실제로 수행하는 GC 워커 스레드 중 4번 → G1/Parallel GC 자주 보이는 이름. 즉. GC 중에 문제가 났을 확음

[stack: A,B]: 이 스레드의 스택 메모리 범위(하한, 상한)

[id=2692]: OS 스레드 ID (Windows의 TID)

 

스택/네이티브 프레임



Stack: [0x0000000c50f00000,0x0000000c51000000],  sp=0x0000000c50fff218,  free space=1020k
Stack: 동일한 스택 범위를 다시 표기

sp= : 현재 스택 포인터 위치

free space=1020k: 스택에서 아직 사용 가능한 여유 공간(약 1MB) → 이게 뜻하는 의미는 스택 오버플로우 때문에 문제가 생긴건 아니라는 뜻.

이 스택의 총 크기는 0x51000000 - 0x50f00000 = 1MB, 현재 사용 중인 부분은 end-sp ≈ 3.5kb

 



Native frames: (J=compiled Java code, j=interpreted, Vv=VM code, C=native code)
C  0x0000000000000046
프레임 표기 규칙 설명

J: JIT 된 자바 코드 (JVM이 실행 중에 네이티브 코드로 변환한 Java 코드)

j: 인터프리트 된 자바 코드

Vv: JVM 내부 코드

C: 네이티브(C/OS) 코드

C 0x0000000000000046

네이티브 프레임에서 프로그램 카운터(=실행주소) 가 ) 0x46을 가리킨 순간 크래시

0x46 같은 극저주소는 정상적인 코드 위치가 아님. 보통 깨진 리턴주소/함수 포인터로 엉뚱한 곳으로 점프했을 때 이런식으로 찍힘

이 한 줄 만 보이는 건 *콜스택 언와인딩이 스택/레지스터 손상 때문에 제대로 못 됐을 가능성이 높다는 신호 (정상이라면 C_ 아래에 더 많은 프레임이 이어진다.)

 

 

*HotSpot 의미

JVM(자바 가상머신)의 대표 구현체 이름

흔히 쓰는 OpenJDK/Oracle JDK의 JVM이 바로 HotSpot VM임.

이름이 HotSpot인 이유

프로그램을 실행하면서 자주 실행되는 핫한 코드 구간을 런타임에 찾아
그 부분만 JIT 컴파일로 네이티브 코드로 바꿔 성능을 끌어올리는 적응형 최적화 엔진.

Hotspot 이 하는 일

인터프리터 + JIT 컴파일러(C1,C2): 처음에는 해석 실행 → 자주돌면 컴파일해서 빠르게

Tiered Compilation: 가벼운 C1 → 최적화된 C2 단계적 승급

GC(가비지 컬렉터): G1, Parallel 등 여러 수집기 내장.

런타임/클래스로더/스레드 관리

Code Cache/ Metaspace : JIT 된 코드와 클래스 메타데이터 저장.

 

*VM 핸들이라는 표현의 의미

자바 레벨에서 말하는 Handle(JNI의 jobject 핸들) 과는 다르다.

HotSpot 소스 코드를 보면 JVM은 각 스레드를 JavaThread, WatcherThread, GCThread 같은 C++ 객체로 관리한다.

(0x0000010f11d0e000)은 이 C++ 객체의 포인터(주소)를 의미하고, 로그에서는 편의상 이를 “VM 핸들“처럼 표현한다.

즉, JVM이 내부적으로 스레드를 참조할 때 사용하는 고유한 포인터 값을 의미한다.

 



siginfo: EXCEPTION_ACCESS_VIOLATION (0xc0000005), data execution prevention violation at address 0x0000000000000046
EXCEPTION_ACCESS_VIOLATION (0xc0000005

윈도우의 표준 예외 코드로, 유효하지 않거나 권한이 맞지 않는 메모리 접근이 발생했다는 뜻이다.

data execution prevention violation

접근 유형은 읽기/쓰기/실행 중 하나인데, 이번 건은 바로 그중 “실행(execute) 시도“ 임.
즉 코드가 아닌 메모리(스택/힙 등, 실행 권한 없는 페이지) 를 명령어처럼 실행하려고 시도하다가 DEP에 막혀 크래시가 났다는 의미.

at address 0x0000000000000046

실제로 명령어를 가져오려던 실행 주소(RIP/PC)가 0x46.

이렇게 낮은 주소(near-null)는 정상적인 코드 위치가 아님

깨진 리턴 주소(스택 훼손)

잘못된 함수 포인터/가상함수 테이블(널 포인터 + 작은 오프셋 → 0x46)

주입/후킹 DLL, 보안툴 간섭, 네이티브/JIT 버그 같은 원인으로 엉뚱한곳으로 점프했을 때 나오는 현상들이다.

 

 

 

 

2️⃣ 레지스터 덤프 해석
 



Register to memory mapping:
RIP=0x0000000000000046 is an unknown value
RAX=0x00000000000000e2 is an unknown value
RBX=0x0000000000000009 is an unknown value
RCX=0x0000010f12cb1778 points into unknown readable memory: f0 67 9e e1 fe 7f 00 00
RDX=0x00000004f4f87b60 points into unknown readable memory: 2d 65 06 00 c8 28 4e 9f
RSP=0x0000000c50fff218 points into unknown readable memory: 8d 03 41 e1 fe 7f 00 00
RBP=0x0000010f12cb1610 points into unknown readable memory: 38 68 9e e1 fe 7f 00 00
RSI=0x00000004f4f87b60 points into unknown readable memory: 2d 65 06 00 c8 28 4e 9f
RDI=0x00007ffee1100000 jvm.dll
R8 =0x0000000800000000 is pointing into metadata
R9 =0x0 is NULL
R10=
[error occurred during error reporting (printing register info), id 0xc0000005, EXCEPTION_ACCESS_VIOLATION (0xc0000005) at pc=0x00007ffee1296656]
 

Windows x64 호출 규약에서는 RCX, RDX, R8, R9 가 1~4번째 인자 레지스터이고,

RAX는 보통 반환값/임시값,

RSP는 스택 포인터,

RBP는 프레임 포인터,

RIP는 현재 실행주소를 의미함.

 

레지스터 별 해석

RIP=0x0000000000000046 is an unknown value
현재 CPU가 0x46이라는 말도 안 되는 저주소에서 코드를 실행하려고 했다는 뜻.
정상 코드 영역이 아니므로, 깨진 리턴주소/함수 포인터로 엉뚱한 곳에 점프했을 가능성이 매우 큼 (DEP 위반이랑 딱 들어 맞는 정황)

RAX=0x00000000000000e2 is an unknown value
작은 상수값(0xE2). 호출 규약상 의미 없는 임시값일 확률이 큼 → 크래시 원인 단서는 아님

RBX=0x0000000000000009 is an unknown value
작은 상수 값을 의미, 루프 카운터/임시 레지스터 였을 수 있으나, 단서로는 매우 약함.

RCX=0x0000010f12cb1778 points into unknown readable memory: f0 67 9e e1 fe 7f 00 00
RCX가 가리키는 주소(0x…1778)는 “읽을 수는 있지만 정체 불명“인 메모리. 거기서 읽어온 8바이트가 f0 67 9e e1 fe 7f 00 00 인데,
이것을 리틀엔디언으로 보면 0x00007FFEE19E67F0.
-> 즉, RCX가 “어딘가의 포인터를 담은 메모리 블록“ 을 가리키고 있고, 그 안에는 상위 주소 영역(0x00007ffe…)의 코드/모듈 주소처럼 보이는 값이 들어있음.
vtable/함수 테이블처럼 “포인터를 또 담고 있는 구조체“ 의 가능성을 시사함.

RDX=0x00000004f4f87b60 points into unknown readable memory: 2d 65 06 00 c8 28 4e 9f
RDX도 정체불명 메모리를 가리킴. 그 안 데이터 2d 65 06 00 c8 28 4e 9f 는 리틀엔디언 해석 시 0x9F4E28C80006652D 인데,
이건 윈도우 사용자 프로세스 주소공간(보통 0x00007fff… 이하)에 잘 나오지 않는 이상한 큰 값이라 유효 포인터로 보이진 않음 → 메모리 내용이 이미 훼손되었을 가능성 시사.

RSP=0x0000000c50fff218 points into unknown readable memory: 8d 03 41 e1 fe 7f 00 00
스택 포인터. [RSP]에 저장된 8바이트는 0x00007FFEE141038D(리틀엔디언). 이건 “직전에 실행되던 정상 코드의 리턴주소 후보“ 같이 보이는 값 (0x00007ffe… 상위 영역은 보통 시스템 DLL이나 JVM 코드 영역)
→ 원래는 jvm/system 코드로 돌아가야 했던 흐름이 어딘가에서 망가져 0x46으로 튄 상황일 수 있음.

RBP=0x0000010f12cb1610 points into unknown readable memory: 38 68 9e e1 fe 7f 00 00
프레임 포인터. 그 위치의 값은 0x00007FFEE19E6838. 이것도 코드/모듈 주소처럼 보이는 수치.
스택프레임/콜체인이 JVM/시스템 코드 주변이었음을 뒷받침.

RSI=0x00000004f4f87b60 points into unknown readable memory: 2d 65 06 00 c8 28 4e 9f
RSI==RDX 동일 값. 호출 규약을 떠올리면, 같은 포인터 인자를 여러 레지스터/자리에서 재사용 하려던 흔적일 수 있다.

RDI=0x00007ffee1100000 jvm.dll
jvm.dll의 베이스 주소를 들고 있음. 우연히 그렇게 되었을 수도 있고, 
PIC 계산/베이스 등록 등 내부 용도로 쓰이던 중이었을 수도 있음. 어쨋든 당시 컨텍스트가 JVM 네이티브 코드 영역과 밀접함.

R8 =0x0000000800000000 is pointing into metadata
HotSpot이 이 범위를 메타스페이스/클래스 메타데이터 영역으로 인지하고 있어서 붙는 주석. 
즉, 클래스/메서드 메타데이터 관련 포인터가 인자로 오가던 타이밍이었을 가능성이 있음.

R9 =0x0 is NULL
네 번째 인자는 NULL

R10= … [error occurred during error reporting … at pc=0x00007ffee1296656]
중요한 힌트. 레지스터를 출력하는 “에러 리포팅 과정“에서조차 또 한 번 Access Violation이 발생했다는 의미 (이번에는 jvm.dll 내부서)
→ 메모리 훼손 정도가 심해서, 진단 출력 중에도 또 터질 만큼 상태가 불안정 하다는 뜻. 일반적인 자바 레벨 예외가 아니라, 네이티브 영역의 광범위한 메모리 손상에 가까움.

 

이 스냅샷이 말해주는 것 (핵심 요약)
PC(RIP)가 0x46: 거의 확실히 잘못된 점프(깨진 리턴주소/함수포인터/간접분기)

RCX/RDX/RSI에 포인터처럼 보이는 값이 오갔지만 내용이 비정상
일부는 그럴듯(0x00007ffe… 주소), 일부는 전혀 말이 안 됨(0x9F… 대형 수치) → 메모리 내용이 섞였거나 덮어써짐.

스택([RSP])에는 정상적인 리턴주소로 보이는 값도 남아 있음
즉, 원래흐름은 JVM/시스템 코드였는데 어딘가에서 분기/리턴이 꼬여 0x46으로 낙하한 모양새.

에러 리포팅 중 2차 충돌
손상 심각. DLL 후킹/보안툴/네이티브 플러그인/JIT/드라이버와의 충돌 같은 네이티브 레벨 문제 가큼

 

 

 

해결 방법
 

위의 현상을 해결하기 위해서 jvm.options 설정 변경을 아래와 같이 진행하였다.



-XX:G1ReservePercent=40 
-XX:InitiatingHeapOccupancyPercent=35
-XX:+UnlockExperimentalVMOptions 
-XX:G1MixedGCLiveThresholdPercent=90 
-XX:G1HeapRegionSize=8M 
-XX:G1NewSizePercent=30 
-XX:G1MaxNewSizePercent=40 
-XX:G1HeapWastePercent=5 
-XX:+AlwaysPreTouch
설정

의미

효과

비고

설정

의미

효과

비고

-XX:G1ReservePercent=40

G1GC가 Heap의 일부를 항상 비워두는 비율을 지정

기본값을 10% 이지만, 이를 40%로 늘림

Heap 여유 공간을 넉넉하게 유지

Full GC 또는 엄청난 pause time 발생 가능성 감소

대규모 색인/검색 요청이 몰릴 때 OOM 위험 줄어듬

기존 설정은 30이었음.

-XX:InitiatingHeapOccupancyPercent=35

G1GC가 Mixed GC 를 시작하는 Heap 사용률 threshold를 지정하는 값.

보통은 45% 정도인데, 35%로 낮춤

Heap이 꽉 차기 전에 GC를 빠르게 시작

Young/Old 영역 모두 디폴트보다 더 일찍 회수

Old 영역 축적이 빨리 정리됨 → pause time 안정화

고질적인 Old Gen 빵빵 방지.

기존 설정은 45였음.

-XX:+UnlockExperimentalVMOptions

일부 JVM의 실험적 옵션 사용을 허용하는 플래그

주로 아래 옵션들을 쓰기 위해 반드시 필요함

G1MixedGCLiveThresholdPercent

G1HeapWastePercent

G1RegionSize 조정 등

고급 G1GC 튜닝 옵션을 활성화 하는 기본 키워드

 

-XX:G1MixedGCLiveThresholdPercent=90

Mixed GC에 포함시킬 region의 live object 비율 기준 값

기본값은 65% → 90%로 상향

live ratio가 90% region만 Mixed GC에 참여

즉. GC 비용 대비 효과가 낮은 region은 건너 뜀

불필요한 Mixed GC 작업 감소

STW(Stop-The-World) 시간 감소

G1GC 효율 상승

 

-XX:G1HeapRegionSize=8M

G1GC의 Region 크기를 8MB로 고정

기본값은 JVM이 자동 결정하지만 보통 1MB ~ 32MB 사이

Region 크기가 커지면 큰 객체 처리에 유리

Lucene segment 파일 기반 workload(대용량 인덱스)에서 효율 상승

GC 처리할 Region 수 감소 → pause time 감소

대규모 인덱싱/검색 workload 에서 안정성 증가

 

-XX:G1NewSizePercent=30

전체 Heap 중 Young 영역이 차지하는 최소 비율을 30%로 산정

기본 Young 영역의 비율은 5~20% 정도

Young 영역을 넓혀서 단기적으로 생성되는 객체 처리 속도 향상

대량의 요청 객체가 Young gen 에서 빨리 처리 됨

Old Gen 오염 감소 → Old 증가 속도 완만해짐

트래픽/색인 요청 많은 ES 에서 매우 중닝

 

-XX:G1MaxNewSizePercent=40

Young 영역의 최대 비율을 40%로 설정

peak traffic 시 Young Gen 확장 여유 증가

대규모 document ingest 시 효율적

Young → Old 승격 최소화 → pause 안정화

-XX:G1NewSizePercent=30

-XX:G1MaxNewSizePercent=40

으로 설정한 뒤

최소 30% ~ 최대 40% → Young 크기 폭이 넓어짐.

 

-XX:G1HeapWastePercent=5

Heap에서 “정리하지 않아도 괜찮다고 여기는 조각(waste) 비율

기본값은 10%

Heap 단편화(fragmentation) 허용폭을 줄임

G1GC가 보다 적극적으로 조각난 heap을 정리

메모리 효율 상승

특히 segment merge/flush 반복되는 ES에서 단편화 해결에 도움이 된다.

 

-XX:+AlwaysPreTouch

JVM이 시작할 때 모든 Heap 메모리 페이지를 미리 할당하고 접근하여 평소에서는 page fault 발생을 줄이는 옵션

런타임 중 페이지 fault로 인한 응답 지연 감소

OS 메모리 레이아웃 안정화

GC 수행 시 메모리 접근 속도 향상

고부하 노드의 지연성(latency) 감소

ES 공식 문서에서도 성능 안정화를 위해 강력히 권장하는 옵션
