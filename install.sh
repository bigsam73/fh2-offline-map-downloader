#!/usr/bin/env bash
# ============================================================
# DJI FH2 On-Premise Offline Map Downloader - 설치 스크립트
# Ubuntu 20.04 / 22.04 / 24.04 LTS 지원
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/opt/fh2-map-downloader"
VENV_DIR="$INSTALL_DIR/venv"
LOG_FILE="/tmp/fh2_map_install.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[⚠]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*" >&2; }
info()  { echo -e "${BLUE}[i]${NC} $*"; }

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  DJI FH2 Offline Map Downloader - 설치 스크립트         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─────────────────────────────────────────────
# 시스템 요구사항 확인
# ─────────────────────────────────────────────
info "시스템 요구사항 확인 중..."

# Ubuntu 확인
if ! grep -qi "ubuntu" /etc/os-release 2>/dev/null; then
    warn "Ubuntu가 아닌 시스템입니다. 호환성이 보장되지 않을 수 있습니다."
fi

# Python 3.8+ 확인
if ! command -v python3 &>/dev/null; then
    error "Python3가 설치되어 있지 않습니다."
    echo "  설치: sudo apt-get install -y python3 python3-pip python3-venv"
    exit 1
fi

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]); then
    error "Python 3.8 이상이 필요합니다. 현재: $PYTHON_VERSION"
    exit 1
fi
log "Python $PYTHON_VERSION 확인됨"

# pip 확인
if ! python3 -m pip --version &>/dev/null; then
    warn "pip 없음. 설치 중..."
    sudo apt-get install -y python3-pip 2>/dev/null || {
        error "pip 설치 실패. 수동으로 설치하세요: sudo apt-get install python3-pip"
        exit 1
    }
fi
log "pip 확인됨"

# venv 확인
if ! python3 -m venv --help &>/dev/null; then
    warn "python3-venv 없음. 설치 중..."
    sudo apt-get install -y python3-venv 2>/dev/null || true
fi

# ─────────────────────────────────────────────
# 시스템 패키지 설치 (선택적)
# ─────────────────────────────────────────────
info "시스템 패키지 확인 중..."

MISSING_PKGS=()

# GDAL (선택적 - VRT 병합용)
if ! command -v gdalbuildvrt &>/dev/null; then
    MISSING_PKGS+=("gdal-bin")
    warn "GDAL 없음 (선택사항). 고도 데이터 VRT 병합 기능이 비활성화됩니다."
fi

# curl (다운로드 테스트용)
if ! command -v curl &>/dev/null; then
    MISSING_PKGS+=("curl")
fi

if [ ${#MISSING_PKGS[@]} -gt 0 ]; then
    echo ""
    echo "  선택적 패키지 설치 여부 (sudo 필요):"
    echo "  패키지: ${MISSING_PKGS[*]}"
    read -rp "  설치하시겠습니까? [y/N] " -n 1
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        sudo apt-get update -qq
        sudo apt-get install -y "${MISSING_PKGS[@]}"
        log "시스템 패키지 설치 완료"
    fi
fi

# ─────────────────────────────────────────────
# 설치 디렉토리 준비
# ─────────────────────────────────────────────
info "설치 디렉토리 준비 중: $INSTALL_DIR"

if [ ! -d "$INSTALL_DIR" ]; then
    sudo mkdir -p "$INSTALL_DIR"
    sudo chown "$USER:$USER" "$INSTALL_DIR"
fi
log "설치 디렉토리 준비됨"

# ─────────────────────────────────────────────
# 가상환경 생성 및 의존성 설치
# ─────────────────────────────────────────────
info "Python 가상환경 생성 중: $VENV_DIR"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
log "가상환경 생성됨"

info "Python 패키지 설치 중..."
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet -r "$SCRIPT_DIR/requirements.txt"
log "Python 패키지 설치 완료 (requests, rich, pyyaml)"

# ─────────────────────────────────────────────
# 스크립트 설치
# ─────────────────────────────────────────────
info "스크립트 설치 중..."

cp "$SCRIPT_DIR/fh2_map_downloader.py" "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/fh2_map_downloader.py"

# 실행 래퍼 스크립트 생성
cat > "$INSTALL_DIR/fh2-map" << WRAPPER
#!/usr/bin/env bash
# DJI FH2 Map Downloader 실행 래퍼
exec "$VENV_DIR/bin/python3" "$INSTALL_DIR/fh2_map_downloader.py" "\$@"
WRAPPER
chmod +x "$INSTALL_DIR/fh2-map"

# 시스템 PATH에 추가 (선택적)
SYMLINK_PATH="/usr/local/bin/fh2-map"
if [ -L "$SYMLINK_PATH" ] || [ -f "$SYMLINK_PATH" ]; then
    sudo rm -f "$SYMLINK_PATH"
fi

sudo ln -sf "$INSTALL_DIR/fh2-map" "$SYMLINK_PATH"
log "실행 파일 설치됨: $SYMLINK_PATH"

# ─────────────────────────────────────────────
# FH2 데이터 디렉토리 확인
# ─────────────────────────────────────────────
info "FH2 데이터 디렉토리 확인 중..."

FH2_ROOT="/fh2"
if [ ! -d "$FH2_ROOT" ]; then
    warn "FH2 설치 경로($FH2_ROOT)가 존재하지 않습니다."
    warn "스크립트 실행 시 경로를 수동으로 입력하거나, /fh2 경로를 생성하세요."
else
    log "FH2 설치 경로 확인됨: $FH2_ROOT"
fi

# ─────────────────────────────────────────────
# 네트워크 연결 테스트
# ─────────────────────────────────────────────
info "네트워크 연결 테스트 중..."

OSM_TEST_URL="https://tile.openstreetmap.org/0/0/0.png"
SRTM_TEST_URL="https://srtm.csi.cgiar.org"

if curl -sf --connect-timeout 10 "$OSM_TEST_URL" -o /dev/null 2>/dev/null; then
    log "OpenStreetMap 서버 연결 확인됨"
else
    warn "OpenStreetMap 서버 연결 실패. 네트워크 또는 방화벽을 확인하세요."
fi

if curl -sf --connect-timeout 10 "$SRTM_TEST_URL" -o /dev/null 2>/dev/null; then
    log "SRTM 서버 연결 확인됨"
else
    warn "SRTM 서버 연결 실패. SRTM 고도 데이터 다운로드가 제한될 수 있습니다."
fi

# ─────────────────────────────────────────────
# 설치 완료
# ─────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                   설치 완료!                            ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  실행 방법:                                             ║"
echo "║    fh2-map                          (대화형 실행)        ║"
echo "║    fh2-map --lat 38.14 --lon 127.31 --radius 5          ║"
echo "║                                                          ║"
echo "║  설치 경로: $INSTALL_DIR         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
