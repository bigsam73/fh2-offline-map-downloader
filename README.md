# DJI FH2 On-Premise Offline Map Downloader v2.0

DJI Flight Hub 2 (FH2) On-Premise 환경에서 위도/경도 및 반경을 입력하면
**국토지리정보원 브이월드(VWorld)** 지도 타일과 **국토지리정보원 공개DEM** 고도 데이터를
병렬로 다운로드하여 FH2에 자동 적용합니다.

## 지도/고도 데이터 소스

| 데이터 | 소스 | 방식 | 비고 |
|--------|------|------|------|
| **지도 타일** | 국토지리정보원 브이월드(VWorld) WMTS | 자동 다운로드 | 한국어 지명, 도로명 주소, 위성영상 |
| **고도 (1순위)** | 국토지리정보원 공개DEM | 수동 배치 후 자동 인식 | 5m 해상도, SRTM 대비 18배 정밀 |
| **고도 (2순위)** | SRTM CGIAR 90m | 자동 다운로드 (fallback) | NGII DEM 없을 때 자동 적용 |

## 브이월드 레이어

| 레이어 | 설명 | 용도 |
|--------|------|------|
| `Base` | 일반 지도 (한국어 지명/도로명) | 기본 항법 |
| `gray` | 회색조 지도 | 드론 항법 최적화 |
| `Satellite` | 위성영상 (고해상도) | 지형 분석 |
| `Hybrid` | 위성 + 지명 오버레이 | 정밀 작업 |

## 요구사항

- Ubuntu 20.04 / 22.04 / 24.04 LTS
- Python 3.8+
- DJI FH2 On-Premise 설치 경로: `/fh2`
- 브이월드 API 키 (내장 키 사용 가능, https://www.vworld.kr 에서 발급)

## 설치

```bash
git clone https://github.com/bigsam73/fh2-offline-map-downloader.git
cd fh2-offline-map-downloader
chmod +x install.sh
./install.sh
```

## 사용법

```bash
# 대화형 실행 (레이어 선택 포함)
fh2-map

# 기본 실행 (Base 레이어)
fh2-map --lat 38.1467 --lon 127.3139 --radius 5

# 위성영상 레이어
fh2-map --lat 38.1467 --lon 127.3139 --radius 5 --layer Satellite

# 드론 항법용 회색조 레이어
fh2-map --lat 38.1467 --lon 127.3139 --radius 5 --layer gray

# 사용자 API 키 지정
fh2-map --vworld-key YOUR_KEY --lat 38.1467 --lon 127.3139 --radius 5 --workers 32
```

## 국토지리정보원 공개DEM (5m) 적용 방법

API가 없어 수동 다운로드 후 배치가 필요합니다.

1. https://map.ngii.go.kr 접속 → 로그인
2. [공간정보받기] → [공개DEM] → 영역 선택
3. 다운로드된 파일을 아래 경로에 복사:
   ```
   <elevation_dir>/ngii_dem/
   ```
4. 스크립트 재실행 시 자동 인식 및 적용

지원 포맷: `.tif` `.tiff` `.img` `.asc` `.zip` (zip 자동 해제)

## 실행 단계

| 단계 | 내용 |
|------|------|
| Step 1 | FH2 설치 경로 자동 감지 |
| Step 2 | 위도/경도/반경 입력 + 예상 용량 미리보기 |
| Step 3 | 다운로드 계획 확인 (소스/레이어/타일 수) |
| Step 4 | 브이월드 타일 병렬 다운로드 → MBTiles 패키징 |
| Step 5 | NGII 공개DEM 자동 인식 / SRTM fallback 다운로드 |
| Step 6 | FH2 적용 (심볼릭 링크 + 설정 업데이트) |
| Step 7 | 검증 (타일 수, 줌 레벨, 고도 파일 무결성) |

## 데이터 소스 출처

- **지도**: [국토지리정보원 브이월드](https://www.vworld.kr) © 국토지리정보원 (NGII)
- **고도**: [국토지리정보원 공개DEM](https://map.ngii.go.kr) / [NASA SRTM](https://srtm.csi.cgiar.org/) (fallback)

## License

MIT

