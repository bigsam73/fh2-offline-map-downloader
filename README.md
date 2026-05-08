# DJI FH2 On-Premise Offline Map Downloader

DJI Flight Hub 2 (FH2) On-Premise 환경에서 위도/경도 및 반경을 입력하면  
OSM 지도 타일과 SRTM 고도 데이터를 병렬로 다운로드하여 FH2에 적용합니다.

## 주요 기능

- **위도/경도 + 반경(1~10km)** 입력으로 다운로드 영역 설정
- **OSM 타일** 자동 줌 레벨 선택 + MBTiles 패키징
- **SRTM 90m 고도 데이터** 자동 다운로드 + 압축 해제
- **병렬 다운로드** (기본 16 워커) + 캐시로 속도 대폭 개선
- **FH2 설정 파일 자동 감지** (`/fh2` 경로 스캔)
- **적용 후 검증** (타일 수, 줌 레벨, 고도 파일 무결성 확인)
- **JSON 리포트** 자동 저장

## 요구사항

- Ubuntu 20.04 / 22.04 / 24.04 LTS
- Python 3.8+
- DJI FH2 On-Premise 설치 경로: `/fh2`

## 설치

```bash
git clone https://github.com/bigsam73/fh2-offline-map-downloader.git
cd fh2-offline-map-downloader
chmod +x install.sh
./install.sh
```

## 사용법

```bash
# 대화형 실행
fh2-map

# 인수 지정 실행
fh2-map --lat 38.1467 --lon 127.3139 --radius 5

# 워커 수 조정 (더 빠른 다운로드)
fh2-map --lat 38.1467 --lon 127.3139 --radius 5 --workers 32
```

## 실행 단계

| 단계 | 내용 |
|------|------|
| Step 1 | FH2 설치 경로 자동 감지 |
| Step 2 | 위도/경도/반경 입력 + 예상 용량 미리보기 |
| Step 3 | 다운로드 계획 확인 |
| Step 4 | OSM 타일 병렬 다운로드 → MBTiles 패키징 |
| Step 5 | SRTM 고도 데이터 다운로드 + 압축 해제 |
| Step 6 | FH2 적용 (심볼릭 링크 + 설정 업데이트) |
| Step 7 | 검증 (타일 수, 파일 무결성, 접근성 확인) |

## 속도 개선 방식

- `ThreadPoolExecutor` 기반 최대 16개 동시 다운로드
- 완료된 타일 캐시 재사용 (재실행 시 스킵)
- HTTP 실패 자동 재시도 3회 + fallback 미러 서버
- 청크 스트리밍 다운로드 (메모리 효율)

## 데이터 소스

- **지도**: [OpenStreetMap](https://www.openstreetmap.org/) (무료, 오픈소스)
- **고도**: [NASA SRTM](https://srtm.csi.cgiar.org/) 90m DEM (무료)

## License

MIT

