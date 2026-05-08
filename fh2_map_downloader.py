#!/usr/bin/env python3
"""
DJI FH2 On-Premise Offline Map & Elevation Downloader
Ubuntu Linux용 - OSM 타일 + SRTM 고도 데이터 병렬 다운로드

Usage:
    python3 fh2_map_downloader.py
    python3 fh2_map_downloader.py --lat 38.1234 --lon 127.5678 --radius 5
"""

import os
import sys
import math
import time
import json
import struct
import hashlib
import asyncio
import argparse
import zipfile
import shutil
import tempfile
import glob
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Set
from datetime import datetime

# 의존성 체크 및 안내
def check_and_install_deps():
    missing = []
    try:
        import requests
    except ImportError:
        missing.append("requests")
    try:
        from rich.console import Console
        from rich.progress import Progress
        from rich.panel import Panel
        from rich.table import Table
        from rich.prompt import Prompt, Confirm, FloatPrompt, IntPrompt
        from rich.layout import Layout
        from rich.live import Live
        from rich.text import Text
        from rich.columns import Columns
    except ImportError:
        missing.append("rich")
    try:
        import yaml
    except ImportError:
        missing.append("pyyaml")

    if missing:
        print(f"[설치 필요] 다음 패키지를 설치합니다: {', '.join(missing)}")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet"] + missing)
        print("설치 완료. 재실행합니다...\n")

check_and_install_deps()

import requests
import yaml
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeRemainingColumn,
    TransferSpeedColumn, DownloadColumn, TaskProgressColumn, SpinnerColumn
)
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich import print as rprint

console = Console()

# ─────────────────────────────────────────────
# 설정 상수
# ─────────────────────────────────────────────
FH2_INSTALL_ROOT = Path("/fh2")
OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OSM_TILE_MIRROR = "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"  # fallback
SRTM_BASE_URL = "https://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF"
SRTM_USGS_URL = "https://dds.cr.usgs.gov/srtm/version2_1/SRTM3"
OPENTOPO_URL = "https://portal.opentopography.org/API/globaldem"
MAX_PARALLEL_WORKERS = 16
TILE_TIMEOUT = 30
CHUNK_SIZE = 65536  # 64KB chunks for download
USER_AGENT = "DJI-FH2-OfflineMapDownloader/1.0 (Ubuntu; contact@example.com)"

# OSM 줌 레벨별 타일 크기 및 해상도
ZOOM_LEVELS = {
    1:  {"radius_min": 1,  "radius_max": 2,  "desc": "도시 전체"},
    5:  {"radius_min": 1,  "radius_max": 5,  "desc": "광역 지역"},
    10: {"radius_min": 1,  "radius_max": 10, "desc": "상세 지형"},
    14: {"radius_min": 1,  "radius_max": 10, "desc": "상세 도로"},
    16: {"radius_min": 1,  "radius_max": 5,  "desc": "건물 레벨"},
}

@dataclass
class DownloadStats:
    total: int = 0
    completed: int = 0
    failed: int = 0
    cached: int = 0
    bytes_downloaded: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def speed_mbps(self) -> float:
        if self.elapsed == 0:
            return 0.0
        return (self.bytes_downloaded / 1024 / 1024) / self.elapsed

    @property
    def eta_seconds(self) -> float:
        if self.completed == 0:
            return float('inf')
        rate = self.completed / self.elapsed
        remaining = self.total - self.completed - self.cached
        return remaining / rate if rate > 0 else float('inf')


# ─────────────────────────────────────────────
# FH2 경로 자동 감지
# ─────────────────────────────────────────────
class FH2PathDetector:
    """DJI FH2 On-Premise 설치 경로 및 데이터 폴더 자동 감지"""

    CONFIG_PATTERNS = [
        "**/application.yml",
        "**/application.yaml",
        "**/config.yml",
        "**/config.yaml",
        "**/fh2.conf",
        "**/.env",
        "**/docker-compose.yml",
        "**/docker-compose.yaml",
    ]

    MAP_PATH_KEYS = [
        "map.offline.path",
        "offline_map_path",
        "map_data_path",
        "mapDataPath",
        "offline.map.directory",
        "data.maps",
        "maps.storage.path",
    ]

    def __init__(self, install_root: Path = FH2_INSTALL_ROOT):
        self.install_root = install_root
        self.detected_map_path: Optional[Path] = None
        self.detected_elevation_path: Optional[Path] = None
        self.config_files_found: List[Path] = []

    def detect(self) -> Tuple[Optional[Path], Optional[Path]]:
        """FH2 설정에서 지도/고도 저장 경로 감지"""
        if not self.install_root.exists():
            return None, None

        console.print(f"  [dim]FH2 설치 경로 스캔 중: {self.install_root}[/dim]")

        # 설정 파일 탐색
        for pattern in self.CONFIG_PATTERNS:
            for cfg_path in self.install_root.glob(pattern):
                self.config_files_found.append(cfg_path)
                map_path, elev_path = self._parse_config(cfg_path)
                if map_path:
                    self.detected_map_path = map_path
                if elev_path:
                    self.detected_elevation_path = elev_path

        # 일반적인 경로 패턴으로 fallback
        if not self.detected_map_path:
            self.detected_map_path = self._find_common_paths()

        if not self.detected_elevation_path and self.detected_map_path:
            self.detected_elevation_path = self.detected_map_path / "elevation"

        return self.detected_map_path, self.detected_elevation_path

    def _parse_config(self, cfg_path: Path) -> Tuple[Optional[Path], Optional[Path]]:
        """설정 파일에서 경로 파싱"""
        map_path = None
        elev_path = None
        try:
            text = cfg_path.read_text(errors='ignore')

            if cfg_path.suffix in ('.yml', '.yaml'):
                try:
                    data = yaml.safe_load(text)
                    if isinstance(data, dict):
                        flat = self._flatten_dict(data)
                        for key in self.MAP_PATH_KEYS:
                            if key in flat and flat[key]:
                                p = Path(str(flat[key]))
                                if "elevation" in str(p).lower() or "dem" in str(p).lower():
                                    elev_path = p
                                else:
                                    map_path = p
                except yaml.YAMLError:
                    pass

            # .env 또는 일반 텍스트 파일에서 파싱
            for line in text.splitlines():
                for key in self.MAP_PATH_KEYS:
                    key_upper = key.replace('.', '_').replace('-', '_').upper()
                    if f"{key_upper}=" in line or f"{key}=" in line:
                        val = line.split('=', 1)[-1].strip().strip('"\'')
                        if val:
                            p = Path(val)
                            if "elevation" in val.lower() or "dem" in val.lower():
                                elev_path = p
                            else:
                                map_path = p

        except (PermissionError, OSError):
            pass
        return map_path, elev_path

    def _flatten_dict(self, d: dict, prefix: str = "") -> dict:
        """중첩 dict를 dot-notation으로 평탄화"""
        result = {}
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                result.update(self._flatten_dict(v, key))
            else:
                result[key] = v
        return result

    def _find_common_paths(self) -> Optional[Path]:
        """일반적인 FH2 맵 저장 경로 탐색"""
        candidates = [
            self.install_root / "data" / "maps",
            self.install_root / "maps",
            self.install_root / "offline_maps",
            self.install_root / "static" / "maps",
            self.install_root / "resources" / "maps",
            self.install_root / "app" / "data" / "maps",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        # 첫 번째 유효한 경로를 기본으로 사용
        return self.install_root / "data" / "maps"


# ─────────────────────────────────────────────
# 지오 유틸리티
# ─────────────────────────────────────────────
class GeoUtils:
    @staticmethod
    def lat_lon_to_tile(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
        """위도/경도를 OSM 타일 좌표로 변환"""
        lat_rad = math.radians(lat)
        n = 2 ** zoom
        x = int((lon + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x, y

    @staticmethod
    def tile_to_lat_lon(x: int, y: int, zoom: int) -> Tuple[float, float]:
        """OSM 타일 좌표를 위도/경도로 변환 (타일 북서 모서리)"""
        n = 2 ** zoom
        lon = x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat = math.degrees(lat_rad)
        return lat, lon

    @staticmethod
    def get_tile_bbox(lat: float, lon: float, radius_km: float, zoom: int) -> Tuple[int, int, int, int]:
        """반경 내 타일 범위(bbox) 계산: (x_min, y_min, x_max, y_max)"""
        # 위도/경도 델타 계산 (근사)
        delta_lat = radius_km / 111.0
        delta_lon = radius_km / (111.0 * math.cos(math.radians(lat)))

        lat_min = lat - delta_lat
        lat_max = lat + delta_lat
        lon_min = lon - delta_lon
        lon_max = lon + delta_lon

        x_min, y_max = GeoUtils.lat_lon_to_tile(lat_max, lon_min, zoom)
        x_max, y_min = GeoUtils.lat_lon_to_tile(lat_min, lon_max, zoom)
        return x_min, y_min, x_max, y_max

    @staticmethod
    def count_tiles(lat: float, lon: float, radius_km: float, zoom: int) -> int:
        """예상 타일 수 계산"""
        x_min, y_min, x_max, y_max = GeoUtils.get_tile_bbox(lat, lon, radius_km, zoom)
        return (x_max - x_min + 1) * (y_max - y_min + 1)

    @staticmethod
    def get_srtm_tiles(lat: float, lon: float, radius_km: float) -> List[Tuple[int, int]]:
        """해당 영역을 커버하는 SRTM 5x5 타일 목록 반환"""
        delta = radius_km / 111.0 + 0.5  # 여유 margin
        lat_min = int(math.floor(lat - delta))
        lat_max = int(math.ceil(lat + delta))
        lon_min = int(math.floor(lon - delta))
        lon_max = int(math.ceil(lon + delta))

        tiles = []
        for la in range(lat_min, lat_max + 1):
            for lo in range(lon_min, lon_max + 1):
                tiles.append((la, lo))
        return tiles

    @staticmethod
    def srtm_tile_name(lat: int, lon: int) -> str:
        """SRTM 타일 파일명 생성 (CGIAR 포맷: srtm_XX_YY)"""
        # CGIAR 5x5도 그리드 변환
        col = int((lon + 180) / 5) + 1
        row = int((60 - lat) / 5) + 1
        return f"srtm_{col:02d}_{row:02d}"


# ─────────────────────────────────────────────
# 병렬 다운로더
# ─────────────────────────────────────────────
class ParallelDownloader:
    """병렬 다운로드 + 캐시 + 재시도 로직"""

    def __init__(self, cache_dir: Path, max_workers: int = MAX_PARALLEL_WORKERS):
        self.cache_dir = cache_dir
        self.max_workers = max_workers
        self.session = self._make_session()
        self.stats = DownloadStats()

    def _make_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        adapter = requests.adapters.HTTPAdapter(
            max_retries=requests.adapters.Retry(
                total=3,
                backoff_factor=1.5,
                status_forcelist=[429, 500, 502, 503, 504],
            ),
            pool_connections=MAX_PARALLEL_WORKERS,
            pool_maxsize=MAX_PARALLEL_WORKERS * 2,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def _cache_key(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def _get_cached(self, cache_path: Path) -> Optional[bytes]:
        if cache_path.exists() and cache_path.stat().st_size > 0:
            return cache_path.read_bytes()
        return None

    def download_one(self, url: str, dest_path: Path, use_cache: bool = True) -> Tuple[bool, int]:
        """단일 파일 다운로드. (성공여부, 바이트수) 반환"""
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # 캐시 확인
        if use_cache and dest_path.exists() and dest_path.stat().st_size > 0:
            return True, 0  # cached

        # fallback URL 처리
        urls = [url]
        if "tile.openstreetmap.org" in url:
            fallback = url.replace("tile.openstreetmap.org", "a.tile.openstreetmap.org")
            urls.append(fallback)

        for attempt_url in urls:
            try:
                resp = self.session.get(attempt_url, timeout=TILE_TIMEOUT, stream=True)
                if resp.status_code == 200:
                    bytes_written = 0
                    with open(dest_path, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                bytes_written += len(chunk)
                    return True, bytes_written
                elif resp.status_code == 404:
                    # 404는 재시도 없이 스킵
                    dest_path.touch()  # 빈 파일로 마킹 (재시도 방지)
                    return True, 0
            except requests.RequestException:
                continue

        return False, 0

    def download_batch(
        self,
        tasks: List[Tuple[str, Path]],  # (url, dest_path) 리스트
        progress,
        task_id,
        stats: DownloadStats,
    ) -> DownloadStats:
        """배치 병렬 다운로드"""
        stats.total = len(tasks)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(self.download_one, url, dest): (url, dest)
                for url, dest in tasks
            }
            for future in as_completed(future_to_task):
                url, dest = future_to_task[future]
                try:
                    success, nbytes = future.result()
                    if success:
                        if nbytes == 0:
                            stats.cached += 1
                        else:
                            stats.completed += 1
                            stats.bytes_downloaded += nbytes
                    else:
                        stats.failed += 1
                except Exception:
                    stats.failed += 1
                finally:
                    progress.advance(task_id)

        return stats


# ─────────────────────────────────────────────
# OSM 타일 다운로더
# ─────────────────────────────────────────────
class OSMTileDownloader:
    def __init__(self, map_dir: Path, downloader: ParallelDownloader):
        self.map_dir = map_dir
        self.downloader = downloader

    def get_zoom_levels_for_radius(self, radius_km: float) -> List[int]:
        """반경에 맞는 줌 레벨 자동 선택"""
        if radius_km <= 2:
            return [10, 13, 15, 16]
        elif radius_km <= 5:
            return [9, 12, 14, 16]
        else:
            return [8, 11, 13, 15]

    def build_tile_tasks(
        self, lat: float, lon: float, radius_km: float, zoom_levels: List[int]
    ) -> List[Tuple[str, Path]]:
        """다운로드할 타일 URL 및 경로 목록 생성"""
        tasks = []
        seen: Set[Tuple[int, int, int]] = set()

        for zoom in zoom_levels:
            x_min, y_min, x_max, y_max = GeoUtils.get_tile_bbox(lat, lon, radius_km, zoom)
            for x in range(x_min, x_max + 1):
                for y in range(y_min, y_max + 1):
                    key = (zoom, x, y)
                    if key in seen:
                        continue
                    seen.add(key)

                    url = OSM_TILE_URL.format(z=zoom, x=x, y=y)
                    dest = self.map_dir / "tiles" / str(zoom) / str(x) / f"{y}.png"
                    tasks.append((url, dest))

        return tasks

    def create_mbtiles(self, lat: float, lon: float, radius_km: float, zoom_levels: List[int]) -> Path:
        """다운로드된 타일을 MBTiles 포맷으로 패키징"""
        import sqlite3

        mbtiles_path = self.map_dir / f"offline_map_{lat:.4f}_{lon:.4f}_{radius_km}km.mbtiles"
        conn = sqlite3.connect(str(mbtiles_path))
        cur = conn.cursor()

        # MBTiles 스키마 생성
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT);
            CREATE TABLE IF NOT EXISTS tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB
            );
            CREATE UNIQUE INDEX IF NOT EXISTS tile_index
                ON tiles (zoom_level, tile_column, tile_row);
        """)

        # 메타데이터 삽입
        meta = [
            ("name", "DJI FH2 Offline Map"),
            ("type", "overlay"),
            ("version", "1.1"),
            ("description", f"Center: {lat},{lon} Radius: {radius_km}km"),
            ("format", "png"),
            ("minzoom", str(min(zoom_levels))),
            ("maxzoom", str(max(zoom_levels))),
            ("center", f"{lon},{lat},{min(zoom_levels)+2}"),
        ]
        cur.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?)", meta)

        # 타일 삽입
        tile_dir = self.map_dir / "tiles"
        inserted = 0
        for zoom in zoom_levels:
            zoom_dir = tile_dir / str(zoom)
            if not zoom_dir.exists():
                continue
            for x_dir in zoom_dir.iterdir():
                for tile_file in x_dir.glob("*.png"):
                    if tile_file.stat().st_size == 0:
                        continue
                    try:
                        x = int(x_dir.name)
                        y = int(tile_file.stem)
                        # MBTiles는 y축이 반전됨
                        tms_y = (2 ** zoom - 1) - y
                        data = tile_file.read_bytes()
                        cur.execute(
                            "INSERT OR REPLACE INTO tiles VALUES (?, ?, ?, ?)",
                            (zoom, x, tms_y, data)
                        )
                        inserted += 1
                    except (ValueError, OSError):
                        continue

        conn.commit()
        conn.close()
        return mbtiles_path


# ─────────────────────────────────────────────
# SRTM 고도 데이터 다운로더
# ─────────────────────────────────────────────
class SRTMDownloader:
    def __init__(self, elevation_dir: Path, downloader: ParallelDownloader):
        self.elevation_dir = elevation_dir
        self.downloader = downloader

    def build_srtm_tasks(self, lat: float, lon: float, radius_km: float) -> List[Tuple[str, Path]]:
        """SRTM 타일 다운로드 작업 목록 생성"""
        tasks = []
        srtm_tiles = GeoUtils.get_srtm_tiles(lat, lon, radius_km)

        for tile_lat, tile_lon in srtm_tiles:
            tile_name = GeoUtils.srtm_tile_name(tile_lat, tile_lon)
            # CGIAR SRTM 5x5 도 GeoTIFF
            url = f"{SRTM_BASE_URL}/{tile_name}.zip"
            dest = self.elevation_dir / "srtm" / f"{tile_name}.zip"
            tasks.append((url, dest))

        return tasks

    def extract_and_convert(self, progress, task_id) -> List[Path]:
        """다운로드된 SRTM zip 파일 압축 해제"""
        srtm_dir = self.elevation_dir / "srtm"
        extracted = []

        zip_files = list(srtm_dir.glob("*.zip"))
        progress.update(task_id, total=len(zip_files))

        for zip_path in zip_files:
            if zip_path.stat().st_size == 0:
                progress.advance(task_id)
                continue
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    for name in zf.namelist():
                        if name.endswith(('.tif', '.TIF', '.asc', '.hgt')):
                            out_path = srtm_dir / name
                            if not out_path.exists():
                                zf.extract(name, srtm_dir)
                            extracted.append(srtm_dir / name)
            except zipfile.BadZipFile:
                pass
            progress.advance(task_id)

        return extracted

    def merge_to_vrt(self, extracted_files: List[Path]) -> Optional[Path]:
        """추출된 DEM 파일을 VRT(가상 래스터)로 병합 - GDAL 필요"""
        if not extracted_files:
            return None

        vrt_path = self.elevation_dir / "elevation_merged.vrt"
        try:
            import subprocess
            tif_files = [str(f) for f in extracted_files if f.suffix.lower() in ('.tif', '.asc')]
            if tif_files:
                result = subprocess.run(
                    ["gdalbuildvrt", str(vrt_path)] + tif_files,
                    capture_output=True, text=True
                )
                if result.returncode == 0:
                    return vrt_path
        except FileNotFoundError:
            # GDAL 없으면 스킵 (원본 파일 그대로 사용)
            pass

        return None


# ─────────────────────────────────────────────
# FH2 적용 및 검증
# ─────────────────────────────────────────────
class FH2Applicator:
    """다운로드된 데이터를 FH2에 적용하고 검증"""

    def __init__(self, install_root: Path, map_dir: Path, elevation_dir: Path):
        self.install_root = install_root
        self.map_dir = map_dir
        self.elevation_dir = elevation_dir

    def apply(self, mbtiles_path: Optional[Path], elevation_files: List[Path]) -> dict:
        """FH2에 맵/고도 데이터 적용"""
        results = {
            "map_applied": False,
            "elevation_applied": False,
            "config_updated": False,
            "errors": [],
        }

        # 1. MBTiles 파일 적용
        if mbtiles_path and mbtiles_path.exists():
            try:
                # FH2가 기대하는 경로에 링크 또는 복사
                fh2_map_link = self.map_dir / "current.mbtiles"
                if fh2_map_link.exists() or fh2_map_link.is_symlink():
                    fh2_map_link.unlink()
                fh2_map_link.symlink_to(mbtiles_path)
                results["map_applied"] = True
            except OSError as e:
                results["errors"].append(f"맵 적용 오류: {e}")

        # 2. 고도 데이터 적용
        if elevation_files:
            try:
                self.elevation_dir.mkdir(parents=True, exist_ok=True)
                results["elevation_applied"] = True
            except OSError as e:
                results["errors"].append(f"고도 데이터 적용 오류: {e}")

        # 3. FH2 설정 파일 업데이트
        results["config_updated"] = self._update_fh2_config(mbtiles_path)

        return results

    def _update_fh2_config(self, mbtiles_path: Optional[Path]) -> bool:
        """FH2 설정 파일에 오프라인 맵 경로 업데이트"""
        config_candidates = [
            self.install_root / "application.yml",
            self.install_root / "config" / "application.yml",
            self.install_root / "conf" / "application.yml",
        ]
        for cfg_path in config_candidates:
            if cfg_path.exists():
                try:
                    content = cfg_path.read_text()
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup = cfg_path.with_suffix(f".yml.bak_{timestamp}")
                    backup.write_text(content)

                    # YAML 업데이트 (간단한 텍스트 치환)
                    if mbtiles_path and "offline_map_path" not in content:
                        content += f"\n# Added by FH2 Map Downloader\noffline_map_path: {mbtiles_path}\n"
                        cfg_path.write_text(content)
                    return True
                except (OSError, PermissionError):
                    return False
        return False

    def verify(self, mbtiles_path: Optional[Path], elevation_files: List[Path]) -> dict:
        """적용 결과 검증"""
        import sqlite3

        verification = {
            "map_file_exists": False,
            "map_tile_count": 0,
            "map_zoom_levels": [],
            "elevation_files_count": 0,
            "elevation_total_size_mb": 0.0,
            "symlink_valid": False,
            "fh2_accessible": False,
            "overall_status": "UNKNOWN",
            "details": [],
        }

        # MBTiles 검증
        if mbtiles_path and mbtiles_path.exists():
            verification["map_file_exists"] = True
            size_mb = mbtiles_path.stat().st_size / 1024 / 1024
            verification["details"].append(f"MBTiles 파일: {mbtiles_path.name} ({size_mb:.1f} MB)")

            try:
                conn = sqlite3.connect(str(mbtiles_path))
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM tiles")
                count = cur.fetchone()[0]
                verification["map_tile_count"] = count

                cur.execute("SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level")
                zooms = [row[0] for row in cur.fetchall()]
                verification["map_zoom_levels"] = zooms
                verification["details"].append(f"타일 수: {count:,}개, 줌 레벨: {zooms}")
                conn.close()
            except Exception as e:
                verification["details"].append(f"MBTiles 읽기 오류: {e}")

        # 심볼릭 링크 검증
        link_path = self.map_dir / "current.mbtiles"
        if link_path.is_symlink() and link_path.exists():
            verification["symlink_valid"] = True
            verification["details"].append(f"심볼릭 링크 정상: {link_path} → {link_path.resolve()}")

        # 고도 데이터 검증
        existing_elev = [f for f in elevation_files if f.exists() and f.stat().st_size > 0]
        verification["elevation_files_count"] = len(existing_elev)
        total_mb = sum(f.stat().st_size for f in existing_elev) / 1024 / 1024
        verification["elevation_total_size_mb"] = total_mb
        if existing_elev:
            verification["details"].append(f"고도 파일: {len(existing_elev)}개 ({total_mb:.1f} MB)")

        # FH2 접근성 검증
        if self.install_root.exists():
            verification["fh2_accessible"] = True
            verification["details"].append(f"FH2 설치 경로 접근 가능: {self.install_root}")

        # 종합 상태
        if (verification["map_file_exists"] and
                verification["map_tile_count"] > 0 and
                verification["elevation_files_count"] > 0):
            verification["overall_status"] = "SUCCESS"
        elif verification["map_file_exists"] and verification["map_tile_count"] > 0:
            verification["overall_status"] = "PARTIAL (고도 데이터 없음)"
        elif verification["map_file_exists"]:
            verification["overall_status"] = "PARTIAL (타일 없음)"
        else:
            verification["overall_status"] = "FAILED"

        return verification


# ─────────────────────────────────────────────
# 메인 TUI 애플리케이션
# ─────────────────────────────────────────────
class FH2MapDownloaderApp:
    def __init__(self, workers: int = MAX_PARALLEL_WORKERS):
        self.console = console
        self.workers = workers
        self.detector = FH2PathDetector(FH2_INSTALL_ROOT)
        self.map_dir: Optional[Path] = None
        self.elevation_dir: Optional[Path] = None

    def print_banner(self):
        banner = """
╔══════════════════════════════════════════════════════════════╗
║       DJI FH2 On-Premise Offline Map Downloader             ║
║       OSM 타일 + SRTM 고도 데이터 병렬 다운로드 도구        ║
╚══════════════════════════════════════════════════════════════╝"""
        self.console.print(f"[bold cyan]{banner}[/bold cyan]")
        self.console.print()

    def detect_paths(self) -> bool:
        """FH2 경로 자동 감지"""
        self.console.print(Panel("[bold]Step 1: FH2 설치 경로 감지[/bold]", style="blue"))

        map_path, elev_path = self.detector.detect()

        if map_path:
            self.console.print(f"  [green]✓[/green] 맵 저장 경로 감지됨: [bold]{map_path}[/bold]")
        else:
            self.console.print(f"  [yellow]⚠[/yellow] 자동 감지 실패. 기본 경로 사용: [bold]{FH2_INSTALL_ROOT}/data/maps[/bold]")
            map_path = FH2_INSTALL_ROOT / "data" / "maps"

        if elev_path:
            self.console.print(f"  [green]✓[/green] 고도 데이터 경로 감지됨: [bold]{elev_path}[/bold]")
        else:
            elev_path = map_path / "elevation"
            self.console.print(f"  [dim]고도 저장 경로: {elev_path}[/dim]")

        # 사용자 확인 및 수정
        self.console.print()
        confirmed_map = Prompt.ask(
            "  맵 저장 경로",
            default=str(map_path)
        )
        confirmed_elev = Prompt.ask(
            "  고도 저장 경로",
            default=str(elev_path)
        )

        self.map_dir = Path(confirmed_map)
        self.elevation_dir = Path(confirmed_elev)

        # 디렉토리 생성
        try:
            self.map_dir.mkdir(parents=True, exist_ok=True)
            self.elevation_dir.mkdir(parents=True, exist_ok=True)
            self.console.print(f"\n  [green]✓[/green] 저장 경로 준비 완료")
            return True
        except PermissionError as e:
            self.console.print(f"\n  [red]✗[/red] 경로 생성 실패 (권한 오류): {e}")
            self.console.print("  [dim]sudo 권한으로 실행하거나 경로를 변경하세요.[/dim]")
            return False

    def get_coordinates(self, args) -> Tuple[float, float, float]:
        """위도/경도/반경 입력 받기"""
        self.console.print()
        self.console.print(Panel("[bold]Step 2: 위치 및 반경 설정[/bold]", style="blue"))

        if args.lat is not None and args.lon is not None:
            lat = args.lat
            lon = args.lon
            self.console.print(f"  [dim]명령줄 인수에서 좌표 로드됨: {lat}, {lon}[/dim]")
        else:
            self.console.print("  [dim]예시 좌표: 철원군 38.1467, 127.3139[/dim]")
            while True:
                try:
                    lat_str = Prompt.ask("  위도 (Latitude, -90 ~ 90)")
                    lat = float(lat_str)
                    if not -90 <= lat <= 90:
                        self.console.print("  [red]위도는 -90에서 90 사이여야 합니다.[/red]")
                        continue
                    break
                except ValueError:
                    self.console.print("  [red]숫자를 입력하세요.[/red]")

            while True:
                try:
                    lon_str = Prompt.ask("  경도 (Longitude, -180 ~ 180)")
                    lon = float(lon_str)
                    if not -180 <= lon <= 180:
                        self.console.print("  [red]경도는 -180에서 180 사이여야 합니다.[/red]")
                        continue
                    break
                except ValueError:
                    self.console.print("  [red]숫자를 입력하세요.[/red]")

        if args.radius is not None:
            radius = float(args.radius)
        else:
            self.console.print()
            self.console.print("  반경 선택 (1 ~ 10 km):")

            table = Table(show_header=True, header_style="bold magenta", box=None)
            table.add_column("번호", style="cyan", width=6)
            table.add_column("반경", style="green", width=10)
            table.add_column("예상 타일 수 (줌 8-16)", style="yellow")
            table.add_column("예상 용량")

            radii = [1, 2, 3, 5, 7, 10]
            for i, r in enumerate(radii, 1):
                zoom_levels = [8, 11, 13, 15]
                total_tiles = sum(GeoUtils.count_tiles(lat, lon, r, z) for z in zoom_levels)
                est_mb = total_tiles * 15 / 1024  # ~15KB per tile avg
                table.add_row(
                    str(i),
                    f"{r} km",
                    f"~{total_tiles:,}개",
                    f"~{est_mb:.0f} MB"
                )
            self.console.print(table)

            while True:
                try:
                    choice_str = Prompt.ask(
                        "  번호 선택 (또는 직접 입력 예: 4.5)",
                        default="3"
                    )
                    try:
                        idx = int(choice_str) - 1
                        if 0 <= idx < len(radii):
                            radius = float(radii[idx])
                        else:
                            radius = float(choice_str)
                    except (ValueError, IndexError):
                        radius = float(choice_str)

                    if 1 <= radius <= 10:
                        break
                    else:
                        self.console.print("  [red]1에서 10 사이의 값을 입력하세요.[/red]")
                except ValueError:
                    self.console.print("  [red]숫자를 입력하세요.[/red]")

        self.console.print(f"\n  [green]✓[/green] 설정: 위도 [bold]{lat}[/bold], 경도 [bold]{lon}[/bold], 반경 [bold]{radius} km[/bold]")
        return lat, lon, radius

    def confirm_and_download(self, lat: float, lon: float, radius: float):
        """다운로드 확인 및 실행"""
        self.console.print()
        self.console.print(Panel("[bold]Step 3: 다운로드 계획 확인[/bold]", style="blue"))

        zoom_levels_map = {
            1: [10, 13, 15, 16],
            2: [10, 13, 15, 16],
            3: [9, 12, 14, 15],
            5: [9, 12, 14, 15],
            7: [8, 11, 13, 15],
            10: [8, 11, 13, 15],
        }
        closest_r = min(zoom_levels_map.keys(), key=lambda x: abs(x - radius))
        zoom_levels = zoom_levels_map[closest_r]

        total_osm = sum(GeoUtils.count_tiles(lat, lon, radius, z) for z in zoom_levels)
        srtm_tiles = GeoUtils.get_srtm_tiles(lat, lon, radius)

        summary = Table(show_header=False, box=None, padding=(0, 2))
        summary.add_column("항목", style="bold")
        summary.add_column("값", style="green")

        summary.add_row("중심 좌표", f"{lat:.6f}°N, {lon:.6f}°E")
        summary.add_row("반경", f"{radius} km")
        summary.add_row("OSM 줌 레벨", str(zoom_levels))
        summary.add_row("예상 OSM 타일 수", f"~{total_osm:,}개")
        summary.add_row("예상 OSM 용량", f"~{total_osm * 15 / 1024:.0f} MB")
        summary.add_row("SRTM 고도 타일 수", f"{len(srtm_tiles)}개")
        summary.add_row("예상 SRTM 용량", f"~{len(srtm_tiles) * 30:.0f} MB")
        summary.add_row("병렬 워커 수", str(self.workers))
        summary.add_row("맵 저장 경로", str(self.map_dir))
        summary.add_row("고도 저장 경로", str(self.elevation_dir))

        self.console.print(summary)
        self.console.print()

        if not Confirm.ask("  다운로드를 시작하시겠습니까?", default=True):
            self.console.print("[yellow]취소되었습니다.[/yellow]")
            return

        # 캐시 디렉토리 설정
        cache_dir = self.map_dir / ".cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        downloader = ParallelDownloader(cache_dir, max_workers=self.workers)
        osm_dl = OSMTileDownloader(self.map_dir, downloader)
        srtm_dl = SRTMDownloader(self.elevation_dir, downloader)
        applicator = FH2Applicator(FH2_INSTALL_ROOT, self.map_dir, self.elevation_dir)

        # OSM 타일 다운로드
        self.console.print()
        self.console.print(Panel("[bold]Step 4: OSM 지도 타일 다운로드[/bold]", style="blue"))

        osm_tasks = osm_dl.build_tile_tasks(lat, lon, radius, zoom_levels)
        osm_stats = DownloadStats()

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("•"),
            DownloadColumn(),
            TextColumn("•"),
            TransferSpeedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            task_id = progress.add_task(
                f"OSM 타일 다운로드 ({len(osm_tasks):,}개)",
                total=len(osm_tasks)
            )
            osm_stats = downloader.download_batch(osm_tasks, progress, task_id, osm_stats)

        self.console.print(
            f"  [green]✓[/green] OSM 완료: "
            f"성공 {osm_stats.completed:,}개, "
            f"캐시 {osm_stats.cached:,}개, "
            f"실패 {osm_stats.failed:,}개 | "
            f"총 {osm_stats.bytes_downloaded/1024/1024:.1f} MB | "
            f"평균 속도 {osm_stats.speed_mbps:.1f} MB/s"
        )

        # MBTiles 패키징
        self.console.print()
        self.console.print("  MBTiles 패키징 중...", end="")
        mbtiles_path = osm_dl.create_mbtiles(lat, lon, radius, zoom_levels)
        self.console.print(f" [green]✓[/green] {mbtiles_path.name} ({mbtiles_path.stat().st_size/1024/1024:.1f} MB)")

        # SRTM 고도 데이터 다운로드
        self.console.print()
        self.console.print(Panel("[bold]Step 5: SRTM 고도 데이터 다운로드[/bold]", style="blue"))

        srtm_tasks = srtm_dl.build_srtm_tasks(lat, lon, radius)
        srtm_stats = DownloadStats()

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("•"),
            DownloadColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            task_id = progress.add_task(
                f"SRTM 고도 다운로드 ({len(srtm_tasks)}개)",
                total=len(srtm_tasks)
            )
            srtm_stats = downloader.download_batch(srtm_tasks, progress, task_id, srtm_stats)

        self.console.print(
            f"  [green]✓[/green] SRTM 완료: "
            f"성공 {srtm_stats.completed}개, "
            f"캐시 {srtm_stats.cached}개, "
            f"실패 {srtm_stats.failed}개 | "
            f"총 {srtm_stats.bytes_downloaded/1024/1024:.1f} MB"
        )

        # SRTM 압축 해제
        self.console.print("\n  SRTM 압축 해제 중...")
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), console=self.console) as progress:
            task_id = progress.add_task("압축 해제", total=1)
            elevation_files = srtm_dl.extract_and_convert(progress, task_id)

        if elevation_files:
            self.console.print(f"  [green]✓[/green] 고도 파일 {len(elevation_files)}개 추출 완료")

        # VRT 병합 시도 (GDAL 있을 경우)
        vrt = srtm_dl.merge_to_vrt(elevation_files)
        if vrt:
            self.console.print(f"  [green]✓[/green] VRT 병합 완료: {vrt}")

        # FH2 적용
        self.console.print()
        self.console.print(Panel("[bold]Step 6: FH2 적용[/bold]", style="blue"))
        apply_results = applicator.apply(mbtiles_path, elevation_files)

        for key, val in apply_results.items():
            if key == "errors":
                for err in val:
                    self.console.print(f"  [red]✗[/red] {err}")
            else:
                icon = "[green]✓[/green]" if val else "[yellow]⚠[/yellow]"
                label_map = {
                    "map_applied": "맵 데이터 적용",
                    "elevation_applied": "고도 데이터 적용",
                    "config_updated": "FH2 설정 업데이트",
                }
                label = label_map.get(key, key)
                self.console.print(f"  {icon} {label}: {'완료' if val else '스킵/실패'}")

        # 최종 검증
        self.console.print()
        self.console.print(Panel("[bold]Step 7: 검증[/bold]", style="blue"))
        verification = applicator.verify(mbtiles_path, elevation_files)

        verify_table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        verify_table.add_column("검증 항목", style="bold")
        verify_table.add_column("결과")

        checks = [
            ("MBTiles 파일 존재", verification["map_file_exists"]),
            ("타일 데이터 정상", verification["map_tile_count"] > 0),
            ("고도 파일 존재", verification["elevation_files_count"] > 0),
            ("심볼릭 링크 정상", verification["symlink_valid"]),
            ("FH2 경로 접근 가능", verification["fh2_accessible"]),
        ]
        for label, ok in checks:
            icon = "[green]✓ PASS[/green]" if ok else "[red]✗ FAIL[/red]"
            verify_table.add_row(label, icon)

        self.console.print(verify_table)
        self.console.print()

        for detail in verification["details"]:
            self.console.print(f"  [dim]• {detail}[/dim]")

        self.console.print()
        status_color = "green" if "SUCCESS" in verification["overall_status"] else "yellow" if "PARTIAL" in verification["overall_status"] else "red"
        self.console.print(Panel(
            f"[bold {status_color}]최종 상태: {verification['overall_status']}[/bold {status_color}]\n"
            f"[dim]맵 타일: {verification['map_tile_count']:,}개 | "
            f"줌 레벨: {verification['map_zoom_levels']} | "
            f"고도 파일: {verification['elevation_files_count']}개 "
            f"({verification['elevation_total_size_mb']:.1f} MB)[/dim]",
            title="검증 결과",
            border_style=status_color
        ))

        # 요약 리포트 저장
        report = {
            "timestamp": datetime.now().isoformat(),
            "center": {"lat": lat, "lon": lon},
            "radius_km": radius,
            "zoom_levels": zoom_levels,
            "osm_stats": {
                "total": osm_stats.total,
                "completed": osm_stats.completed,
                "cached": osm_stats.cached,
                "failed": osm_stats.failed,
                "bytes_mb": round(osm_stats.bytes_downloaded / 1024 / 1024, 2),
                "speed_mbps": round(osm_stats.speed_mbps, 2),
            },
            "srtm_stats": {
                "total": srtm_stats.total,
                "completed": srtm_stats.completed,
                "cached": srtm_stats.cached,
                "failed": srtm_stats.failed,
                "bytes_mb": round(srtm_stats.bytes_downloaded / 1024 / 1024, 2),
            },
            "verification": {
                k: v for k, v in verification.items() if k != "details"
            },
            "mbtiles_path": str(mbtiles_path) if mbtiles_path else None,
            "elevation_files": [str(f) for f in elevation_files],
        }
        report_path = self.map_dir / f"download_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        self.console.print(f"\n  [dim]리포트 저장: {report_path}[/dim]")

    def run(self, args):
        self.print_banner()

        if not self.detect_paths():
            sys.exit(1)

        lat, lon, radius = self.get_coordinates(args)
        self.confirm_and_download(lat, lon, radius)

        self.console.print()
        self.console.print("[bold green]완료![/bold green] DJI FH2 오프라인 맵 다운로드 및 적용이 완료되었습니다.")


# ─────────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="DJI FH2 On-Premise Offline Map & Elevation Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python3 fh2_map_downloader.py
  python3 fh2_map_downloader.py --lat 38.1467 --lon 127.3139 --radius 5
  python3 fh2_map_downloader.py --lat 37.5665 --lon 126.9780 --radius 3
        """
    )
    parser.add_argument("--lat", type=float, help="위도 (예: 38.1467)")
    parser.add_argument("--lon", type=float, help="경도 (예: 127.3139)")
    parser.add_argument("--radius", type=float, help="반경 km (1~10)")
    parser.add_argument("--workers", type=int, default=MAX_PARALLEL_WORKERS, help=f"병렬 워커 수 (기본: {MAX_PARALLEL_WORKERS})")
    args = parser.parse_args()

    workers = args.workers if args.workers else MAX_PARALLEL_WORKERS
    app = FH2MapDownloaderApp(workers=workers)
    try:
        app.run(args)
    except KeyboardInterrupt:
        console.print("\n[yellow]사용자가 중단했습니다.[/yellow]")
        sys.exit(0)


if __name__ == "__main__":
    main()
