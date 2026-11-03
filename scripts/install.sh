
#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# BMAS Installer
# Broadcast Monitoring & Archiving Service
# Supported OS: Ubuntu 22.04 / 24.04
# ============================================================

APP="bmas"
APP_NAME="Broadcast Monitoring & Archiving Service"

INSTALL_DIR="/opt/bmas"
CONFIG_DIR="/etc/bmas"
DATA_DIR="/var/lib/bmas"

SERVICE_FILE="/etc/systemd/system/bmas.service"
SERVICE_NAME="bmas.service"

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

OLAF_REPO="https://github.com/JorenSix/Olaf.git"
OLAF_REF="${OLAF_REF:-master}"

ZIG_VERSION="${ZIG_VERSION:-0.14.1}"
MIN_CARGO_VERSION="1.85.0"

RUST_HOME="/root/.rustup"
CARGO_HOME="/root/.cargo"

export RUSTUP_HOME="$RUST_HOME"
export CARGO_HOME="$CARGO_HOME"
export PATH="$CARGO_HOME/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

log() {
    echo
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

fail() {
    echo
    echo "ERROR: $*" >&2
    exit 1
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

version_ge() {
    # Returns success if version $1 >= version $2.
    [[ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" == "$2" ]]
}

cleanup() {
    if [[ -n "${TMP:-}" && -d "$TMP" ]]; then
        rm -rf "$TMP"
    fi
}

# ------------------------------------------------------------
# Header
# ------------------------------------------------------------

echo "======================================"
echo " BMAS Installer"
echo " $APP_NAME"
echo "======================================"

# ------------------------------------------------------------
# Root check
# ------------------------------------------------------------

if [[ "$EUID" -ne 0 ]]; then
    echo "Run this installer using sudo:"
    echo "sudo bash ./scripts/install.sh"
    exit 1
fi

# ------------------------------------------------------------
# Check existing installation FIRST
# ------------------------------------------------------------

if systemctl list-unit-files --type=service --no-legend 2>/dev/null \
    | awk '{print $1}' \
    | grep -qx "$SERVICE_NAME"; then

    echo
    echo "BMAS is already installed."
    echo "Installation skipped."
    exit 0
fi

if [[ -f "$SERVICE_FILE" ]]; then
    echo
    echo "BMAS service file already exists."
    echo "Installation skipped."
    exit 0
fi

# ------------------------------------------------------------
# Validate operating system
# ------------------------------------------------------------

if [[ ! -f /etc/os-release ]]; then
    fail "Cannot identify operating system."
fi

. /etc/os-release

if [[ "$ID" != "ubuntu" ]] ||
   [[ "$VERSION_ID" != "22.04" && "$VERSION_ID" != "24.04" ]]; then
    fail "Only Ubuntu 22.04 and 24.04 are supported."
fi

ARCH="$(uname -m)"

case "$ARCH" in
    x86_64)
        ZIG_ARCH="x86_64"
        ;;
    aarch64|arm64)
        ZIG_ARCH="aarch64"
        ;;
    *)
        fail "Unsupported architecture: $ARCH"
        ;;
esac

# ------------------------------------------------------------
# Temporary directory
# ------------------------------------------------------------

TMP="$(mktemp -d)"
trap cleanup EXIT

# ============================================================
# 1. System dependencies
# ============================================================

log "[1/8] Installing system dependencies..."

export DEBIAN_FRONTEND=noninteractive

apt-get update

apt-get install -y \
    build-essential \
    git \
    make \
    curl \
    wget \
    xz-utils \
    ca-certificates \
    pkg-config \
    ffmpeg \
    libsqlite3-dev \
    openssl \
    libssl-dev

command_exists ffmpeg || fail "FFmpeg installation failed."
command_exists ffprobe || fail "FFprobe installation failed."

# ============================================================
# 2. Zig
# ============================================================

log "[2/8] Installing Zig ${ZIG_VERSION}..."

CURRENT_ZIG_VERSION=""

if command_exists zig; then
    CURRENT_ZIG_VERSION="$(zig version 2>/dev/null || true)"
fi

if [[ "$CURRENT_ZIG_VERSION" != "$ZIG_VERSION" ]]; then

    log "Installing Zig ${ZIG_VERSION} (current: ${CURRENT_ZIG_VERSION:-not installed})..."

    ZIG_ARCHIVE="zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz"
    ZIG_URL="https://ziglang.org/download/${ZIG_VERSION}/${ZIG_ARCHIVE}"

    curl -fL "$ZIG_URL" -o "$TMP/zig.tar.xz"

    tar -xf "$TMP/zig.tar.xz" -C "$TMP"

    ZIG_EXTRACTED="$TMP/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}"

    [[ -x "$ZIG_EXTRACTED/zig" ]] \
        || fail "Zig binary was not found in downloaded archive."

    install -m 0755 \
        "$ZIG_EXTRACTED/zig" \
        /usr/local/bin/zig

else
    log "Required Zig version already installed."
fi

ACTUAL_ZIG_VERSION="$(zig version)"

[[ "$ACTUAL_ZIG_VERSION" == "$ZIG_VERSION" ]] \
    || fail "Expected Zig ${ZIG_VERSION}, found ${ACTUAL_ZIG_VERSION}."

echo "Zig: $(zig version)"

# ============================================================
# 3. OLAF
# ============================================================

log "[3/8] Installing OLAF..."

if command_exists olaf; then

    echo "OLAF already installed. Skipping."

else

    git clone \
        --depth 1 \
        --branch "$OLAF_REF" \
        "$OLAF_REPO" \
        "$TMP/Olaf"

    make -C "$TMP/Olaf"

    make -C "$TMP/Olaf" install

fi

command_exists olaf || fail "OLAF installation failed."

echo "OLAF: $(command -v olaf)"
echo "FFmpeg: $(command -v ffmpeg)"
echo "FFprobe: $(command -v ffprobe)"

# ============================================================
# 4. Rust and Cargo
# ============================================================

log "[4/8] Installing/updating Rust..."

if ! command_exists rustup; then

    log "Installing Rustup..."

    curl --proto '=https' \
        --tlsv1.2 \
        -sSf https://sh.rustup.rs \
        -o "$TMP/rustup-init.sh"

    sh "$TMP/rustup-init.sh" -y \
        --default-toolchain stable \
        --profile minimal

fi

export PATH="$CARGO_HOME/bin:$PATH"

command_exists rustup || fail "Rustup installation failed."

log "Installing/updating stable Rust toolchain..."

rustup toolchain install stable --profile minimal

rustup default stable

rustup update stable

command_exists cargo || fail "Cargo installation failed."

RUST_VERSION="$(rustc --version)"
CARGO_VERSION="$(cargo --version)"

echo "Rust:  $RUST_VERSION"
echo "Cargo: $CARGO_VERSION"

CARGO_VERSION_NUMBER="$(cargo --version | awk '{print $2}')"

if ! version_ge "$CARGO_VERSION_NUMBER" "$MIN_CARGO_VERSION"; then
    fail "Cargo ${MIN_CARGO_VERSION} or newer is required. Found ${CARGO_VERSION_NUMBER}."
fi

# ============================================================
# 5. Build BMAS
# ============================================================

log "[5/8] Building BMAS..."

cd "$SOURCE_DIR"

if [[ ! -f Cargo.toml ]]; then
    fail "Cargo.toml not found in ${SOURCE_DIR}."
fi

cargo build --release

BINARY="$SOURCE_DIR/target/release/bmas"

[[ -x "$BINARY" ]] || fail "BMAS binary was not generated."

# ============================================================
# 6. Create service account and directories
# ============================================================

log "[6/8] Creating BMAS service account..."

if ! id "$APP" >/dev/null 2>&1; then

    useradd \
        --system \
        --home-dir "$DATA_DIR" \
        --shell /usr/sbin/nologin \
        "$APP"

fi

install -d -o "$APP" -g "$APP" -m 0750 "$DATA_DIR"

install -d -o root -g "$APP" -m 0750 "$CONFIG_DIR"

install -d -o root -g root -m 0755 "$INSTALL_DIR"

install -m 0755 \
    "$BINARY" \
    "$INSTALL_DIR/bmas"

# ============================================================
# 7. Install configuration
# ============================================================

log "[7/8] Installing BMAS configuration..."

if [[ ! -f "$CONFIG_DIR/config.toml" ]]; then

    [[ -f "$SOURCE_DIR/config.toml.example" ]] \
        || fail "Source config.toml.example not found."

    install -m 0640 \
        "$SOURCE_DIR/config.toml.example" \
        "$CONFIG_DIR/config.toml"

    # R2 remains disabled until the operator provisions credentials.
    sed -i '/^\[r2\]/,/^\[/ s/^enabled = true/enabled = false/' \
        "$CONFIG_DIR/config.toml"

    TOKEN="$(openssl rand -hex 32)"
    NODE_ID="$(cat /proc/sys/kernel/random/uuid)"

    cat > "$CONFIG_DIR/bmas.env" <<EOF
BMAS_API_TOKEN=${TOKEN}
BMAS_NODE_ID=${NODE_ID}
BMAS_R2_ACCOUNT_ID=
BMAS_R2_ACCESS_KEY_ID=
BMAS_R2_SECRET_ACCESS_KEY=
BMAS_R2_BUCKET=
BMAS_R2_ENDPOINT_URL=
BMAS_CENTRAL_NODE_TOKEN=
EOF

    chown root:root "$CONFIG_DIR/bmas.env"
    chmod 0600 "$CONFIG_DIR/bmas.env"

    echo "Generated secure API token in $CONFIG_DIR/bmas.env."
    echo "Generated node UUID $NODE_ID in $CONFIG_DIR/bmas.env."
    echo "When provisioning this node in the central API, replace BMAS_NODE_ID with the UUID returned by central."
    echo "Provision R2 credentials in $CONFIG_DIR/bmas.env before enabling [r2]."

else

    echo "Existing configuration found. Preserving it."

fi

chown root:"$APP" "$CONFIG_DIR/config.toml"
chmod 0640 "$CONFIG_DIR/config.toml"

# ============================================================
# 8. Install systemd service
# ============================================================

log "[8/8] Installing systemd service..."

cat > "$SERVICE_FILE" <<'UNIT'
[Unit]
Description=Broadcast Monitoring & Archiving Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple

User=bmas
Group=bmas

EnvironmentFile=-/etc/bmas/bmas.env
ExecStart=/opt/bmas/bmas --config /etc/bmas/config.toml

Restart=always
RestartSec=5

NoNewPrivileges=true
PrivateTmp=true

ProtectSystem=strict
ProtectHome=true

ReadWritePaths=/var/lib/bmas

LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT

chmod 0644 "$SERVICE_FILE"

systemctl daemon-reload

systemctl enable bmas

systemctl start bmas

# ============================================================
# Completion
# ============================================================

log "BMAS installation completed successfully."

echo
echo "======================================"
echo " BMAS Installation Summary"
echo "======================================"

echo "Binary:        $INSTALL_DIR/bmas"
echo "Configuration: $CONFIG_DIR/config.toml"
echo "Environment:   $CONFIG_DIR/bmas.env"
echo "Data:          $DATA_DIR"
echo "Service:       $SERVICE_NAME"
echo "Swagger:       http://127.0.0.1:8787/docs"

echo
echo "Installed versions:"
echo "Rust:          $(rustc --version)"
echo "Cargo:         $(cargo --version)"
echo "Zig:           $(zig version)"
echo "OLAF:          $(command -v olaf)"
echo "FFmpeg:        $(ffmpeg -version | head -n 1)"

echo
echo "Service status:"

systemctl --no-pager status bmas || true

echo
echo "Useful commands:"
echo "  sudo systemctl status bmas"
echo "  sudo journalctl -u bmas -f"
echo "  sudo systemctl restart bmas"
echo "  sudo systemctl stop bmas"
echo
