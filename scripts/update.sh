
#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# BMAS Updater
# Broadcast Monitoring & Archiving Service
# Supported OS: Ubuntu 22.04 / 24.04
#
# Builds the current source, synchronizes configuration,
# replaces the installed BMAS binary, and restarts the service.
#
# Automatically rolls back the binary and configuration
# if the updated service fails to start.
# ============================================================

APP="bmas"
APP_NAME="Broadcast Monitoring & Archiving Service"

INSTALL_DIR="/opt/bmas"
CONFIG_DIR="/etc/bmas"
DATA_DIR="/var/lib/bmas"

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SOURCE_CONFIG="$SOURCE_DIR/config.toml"
INSTALLED_CONFIG="$CONFIG_DIR/config.toml"

SERVICE_FILE="/etc/systemd/system/bmas.service"
SERVICE_NAME="bmas.service"
ENV_FILE="$CONFIG_DIR/bmas.env"

RUST_HOME="/root/.rustup"
CARGO_HOME="/root/.cargo"

export RUSTUP_HOME="$RUST_HOME"
export CARGO_HOME="$CARGO_HOME"
export PATH="$CARGO_HOME/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

TMP=""

CURRENT_BINARY="$INSTALL_DIR/bmas"
BACKUP_BINARY="$INSTALL_DIR/bmas.rollback"
BACKUP_CONFIG="$CONFIG_DIR/config.toml.backup"

NEW_BINARY=""
PREVIOUS_BINARY=""
PREVIOUS_CONFIG=""

CONFIG_UPDATED=false
BINARY_UPDATED=false
SYNC_CONFIG=false

for arg in "$@"; do
    case "$arg" in
        --sync-config) SYNC_CONFIG=true ;;
        *) echo "ERROR: Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# ------------------------------------------------------------
# Logging and error handling
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

cleanup() {
    if [[ -n "${TMP:-}" && -d "$TMP" ]]; then
        rm -rf "$TMP"
    fi
}

config_value() {
    local section="$1" key="$2" file="$3"

    [[ -f "$file" ]] || return 0

    awk -v section="$section" -v key="$key" '
        /^[[:space:]]*\[/ {
            header=$0
            gsub(/^[[:space:]]*\[[[:space:]]*/, "", header)
            gsub(/[[:space:]]*\][[:space:]]*$/, "", header)
            in_section = (header == section)
            next
        }
        in_section {
            line=$0
            sub(/^[[:space:]]*/, "", line)
            if (index(line, key) != 1) next
            rest=substr(line, length(key) + 1)
            if (rest !~ /^[[:space:]]*=/) next
            value=substr(rest, index(rest, "=") + 1)
            sub(/^[[:space:]]*/, "", value)
            sub(/[[:space:]]+$/, "", value)
            if (value ~ /^".*"$/) {
                value=substr(value, 2, length(value) - 2)
            } else if (value ~ /^'\''.*'\''$/) {
                value=substr(value, 2, length(value) - 2)
            } else {
                sub(/[[:space:]]*#.*$/, "", value)
            }
            print value
            exit
        }
    ' "$file"
}

seed_environment_from_config() {
    local dest="$1"
    local mapping env_name section key value

    for mapping in \
        "BMAS_NODE_ID:node:id" \
        "BMAS_API_TOKEN:node:api_token" \
        "BMAS_R2_ACCOUNT_ID:r2:account_id" \
        "BMAS_R2_ACCESS_KEY_ID:r2:access_key_id" \
        "BMAS_R2_SECRET_ACCESS_KEY:r2:secret_access_key" \
        "BMAS_R2_BUCKET:r2:bucket" \
        "BMAS_R2_ENDPOINT_URL:r2:endpoint_url" \
        "BMAS_CENTRAL_NODE_TOKEN:central:node_token"; do

        env_name="${mapping%%:*}"
        mapping="${mapping#*:}"
        section="${mapping%%:*}"
        key="${mapping#*:}"

        value="$(config_value "$section" "$key" "$INSTALLED_CONFIG")"

        case "$value" in
            ""|'${'*'}') continue ;;
        esac

        printf '%s=%s\n' "$env_name" "$value" >> "$dest"
    done
}

ensure_environment_file() {
    if [[ ! -f "$ENV_FILE" ]]; then
        install -m 0600 -o root -g root /dev/null "$ENV_FILE"
        seed_environment_from_config "$ENV_FILE"
    fi

    chown root:root "$ENV_FILE"
    chmod 0600 "$ENV_FILE"

    local token node_id
    token="$(openssl rand -hex 32)"
    node_id="$(cat /proc/sys/kernel/random/uuid)"

    grep -q '^BMAS_API_TOKEN=' "$ENV_FILE" \
        || echo "BMAS_API_TOKEN=$token" >> "$ENV_FILE"
    grep -q '^BMAS_NODE_ID=' "$ENV_FILE" \
        || echo "BMAS_NODE_ID=$node_id" >> "$ENV_FILE"

    local key
    for key in \
        BMAS_R2_ACCOUNT_ID \
        BMAS_R2_ACCESS_KEY_ID \
        BMAS_R2_SECRET_ACCESS_KEY \
        BMAS_R2_BUCKET \
        BMAS_R2_ENDPOINT_URL \
        BMAS_CENTRAL_NODE_TOKEN; do
        grep -q "^${key}=" "$ENV_FILE" || echo "${key}=" >> "$ENV_FILE"
    done
}

ensure_service_environment() {
    if ! grep -q '^EnvironmentFile=-/etc/bmas/bmas.env$' "$SERVICE_FILE"; then
        sed -i '/^\[Service\]/a EnvironmentFile=-/etc/bmas/bmas.env' "$SERVICE_FILE"
        systemctl daemon-reload
    fi
}

migrate_legacy_r2_token() {
    [[ -f "$INSTALLED_CONFIG" ]] || return 0

    local has_token
    has_token="$(awk '
        /^[[:space:]]*\[/ {
            header=$0
            gsub(/^[[:space:]]*\[[[:space:]]*/, "", header)
            gsub(/[[:space:]]*\][[:space:]]*$/, "", header)
            in_r2 = (header == "r2")
            next
        }
        in_r2 {
            line=$0
            sub(/^[[:space:]]*/, "", line)
            if (line ~ /^token[[:space:]]*=/) {
                print "yes"
                exit
            }
        }
    ' "$INSTALLED_CONFIG")"

    [[ "$has_token" == "yes" ]] || return 0

    local backup="$CONFIG_DIR/config.toml.legacy-token.bak"
    local tmp
    tmp="$(mktemp)"

    cp -p "$INSTALLED_CONFIG" "$backup"

    awk '
        /^[[:space:]]*\[/ {
            header=$0
            gsub(/^[[:space:]]*\[[[:space:]]*/, "", header)
            gsub(/[[:space:]]*\][[:space:]]*$/, "", header)
            in_r2 = (header == "r2")
        }
        in_r2 {
            line=$0
            sub(/^[[:space:]]*/, "", line)
            if (line ~ /^token[[:space:]]*=/) next
        }
        { print }
    ' "$INSTALLED_CONFIG" > "$tmp"

    install -o root -g bmas -m 0640 "$tmp" "$INSTALLED_CONFIG"
    rm -f "$tmp"

    log "Removed legacy [r2] token from $INSTALLED_CONFIG"
    echo "Backup: $backup"
    echo "Rotate the removed token if it was a live credential."

    if [[ -z "$(config_value r2 secret_access_key "$INSTALLED_CONFIG")" ]]; then
        echo "WARNING: [r2].secret_access_key is missing."
        echo "Set BMAS_R2_SECRET_ACCESS_KEY in $ENV_FILE before enabling R2."
    fi
}

trap cleanup EXIT

echo "======================================"
echo " BMAS Updater"
echo " $APP_NAME"
echo "======================================"

# ------------------------------------------------------------
# Root and installation checks
# ------------------------------------------------------------

if [[ "$EUID" -ne 0 ]]; then
    echo "Run the updater using sudo:"
    echo "sudo bash ./scripts/update.sh"
    exit 1
fi

[[ -f "$SERVICE_FILE" ]] \
    || fail "BMAS service file not found: $SERVICE_FILE"

systemctl daemon-reload

systemctl cat "$SERVICE_NAME" >/dev/null 2>&1 \
    || fail "BMAS service is not registered with systemd."

[[ -f "$INSTALLED_CONFIG" ]] \
    || fail "Installed configuration not found: $INSTALLED_CONFIG"

[[ -f "$SOURCE_CONFIG" ]] \
    || fail "Project configuration not found: $SOURCE_CONFIG"

[[ -d "$SOURCE_DIR" ]] \
    || fail "Source directory not found: $SOURCE_DIR"

[[ -f "$SOURCE_DIR/Cargo.toml" ]] \
    || fail "Cargo.toml not found in $SOURCE_DIR"

[[ -x "$CURRENT_BINARY" ]] \
    || fail "Installed BMAS binary not found: $CURRENT_BINARY"

command -v cargo >/dev/null 2>&1 \
    || fail "Cargo is not installed or not in PATH."

getent group bmas >/dev/null 2>&1 \
    || fail "The bmas group does not exist."

TMP="$(mktemp -d)"

log "Ensuring BMAS environment and systemd configuration..."
ensure_environment_file
ensure_service_environment

NEW_BINARY="$TMP/bmas.new"
PREVIOUS_BINARY="$TMP/bmas.previous"
PREVIOUS_CONFIG="$TMP/config.previous"

# ------------------------------------------------------------
# Build
# ------------------------------------------------------------

log "[1/5] Building BMAS release binary..."

cd "$SOURCE_DIR"

cargo build --release

BUILD_BINARY="$SOURCE_DIR/target/release/bmas"

[[ -x "$BUILD_BINARY" ]] \
    || fail "Release binary was not generated."

install -o root -g root -m 0755 "$BUILD_BINARY" "$NEW_BINARY"

# ------------------------------------------------------------
# Record current version and prepare backups
# ------------------------------------------------------------

log "[2/5] Preparing update and backups..."

OLD_VERSION="$("$CURRENT_BINARY" --version 2>/dev/null || echo "version unavailable")"
NEW_VERSION="$("$NEW_BINARY" --version 2>/dev/null || echo "version unavailable")"

echo "Installed: $OLD_VERSION"
echo "Building:  $NEW_VERSION"

# Temporary rollback copies.
cp -p "$CURRENT_BINARY" "$PREVIOUS_BINARY"
cp -p "$INSTALLED_CONFIG" "$PREVIOUS_CONFIG"

# ------------------------------------------------------------
# Synchronize configuration
# ------------------------------------------------------------

if [[ "$SYNC_CONFIG" == true ]]; then

    log "[3/5] Synchronizing BMAS configuration..."

    # Preserve the currently installed configuration as a
    # persistent backup before replacing it.
    cp -p "$INSTALLED_CONFIG" "$BACKUP_CONFIG"

    # Copy the project configuration into the production location.
    # The bmas group must be able to read the configuration because
    # the systemd service runs under the bmas account.
    if ! install -o root -g bmas -m 0640 \
        "$SOURCE_CONFIG" "$INSTALLED_CONFIG"; then

        log "Configuration synchronization failed."

        install -o root -g bmas -m 0640 \
            "$PREVIOUS_CONFIG" "$INSTALLED_CONFIG"

        fail "Update aborted. Previous configuration restored."
    fi

    CONFIG_UPDATED=true

    log "Configuration synchronized successfully."
    echo "Source:      $SOURCE_CONFIG"
    echo "Destination: $INSTALLED_CONFIG"

else

    log "[3/5] Preserving installed BMAS configuration."
    migrate_legacy_r2_token
    echo "Use --sync-config to replace $INSTALLED_CONFIG from the source tree."

fi

# ------------------------------------------------------------
# Replace binary and restart service
# ------------------------------------------------------------

log "[4/5] Installing updated BMAS binary..."

if ! systemctl stop "$SERVICE_NAME"; then
    log "Could not stop BMAS. Restoring configuration..."

    install -o root -g bmas -m 0640 \
        "$PREVIOUS_CONFIG" "$INSTALLED_CONFIG"

    fail "Update aborted. Previous configuration restored."
fi

# Install the new binary to a temporary path in INSTALL_DIR
# and atomically replace the existing binary.
install -o root -g root -m 0755 \
    "$NEW_BINARY" "$INSTALL_DIR/bmas.new"

mv -f "$INSTALL_DIR/bmas.new" "$CURRENT_BINARY"

BINARY_UPDATED=true

log "[5/5] Starting updated BMAS service..."

# ------------------------------------------------------------
# Start and verify
# ------------------------------------------------------------

START_SUCCESS=false

if systemctl start "$SERVICE_NAME"; then

    for _ in {1..15}; do
        if systemctl is-active --quiet "$SERVICE_NAME"; then
            START_SUCCESS=true
            break
        fi

        sleep 1
    done

fi

# ------------------------------------------------------------
# Rollback on failure
# ------------------------------------------------------------

if [[ "$START_SUCCESS" != true ]]; then

    log "Updated service failed to start. Rolling back..."

    systemctl stop "$SERVICE_NAME" || true

    if [[ "$BINARY_UPDATED" == true ]]; then
        install -o root -g root -m 0755 \
            "$PREVIOUS_BINARY" "$CURRENT_BINARY"
    fi

    if [[ "$CONFIG_UPDATED" == true ]]; then
        install -o root -g bmas -m 0640 \
            "$PREVIOUS_CONFIG" "$INSTALLED_CONFIG"
    fi

    systemctl reset-failed "$SERVICE_NAME" || true

    if ! systemctl start "$SERVICE_NAME"; then
        log "WARNING: Previous BMAS version also failed to start."
        log "Check logs using: sudo journalctl -u bmas -n 100"
    fi

    fail "Update failed. Previous binary and configuration restored."
fi

# ------------------------------------------------------------
# Retain previous binary for manual rollback
# ------------------------------------------------------------

install -o root -g root -m 0755 \
    "$PREVIOUS_BINARY" "$BACKUP_BINARY"

# ------------------------------------------------------------
# Success
# ------------------------------------------------------------

echo
echo "======================================"
echo " BMAS Update Completed Successfully"
echo "======================================"

echo "Binary:              $CURRENT_BINARY"
echo "Previous binary:     $BACKUP_BINARY"
echo "Configuration:       $INSTALLED_CONFIG"
echo "Configuration backup: $BACKUP_CONFIG"
echo "Data:                $DATA_DIR"
echo "Service:             $SERVICE_NAME"

echo
echo "Useful commands:"
echo "  sudo systemctl status bmas"
echo "  sudo journalctl -u bmas -f"
echo "  sudo systemctl restart bmas"

echo
echo "To manually roll back the binary:"
echo "  sudo systemctl stop bmas"
echo "  sudo cp '$BACKUP_BINARY' '$CURRENT_BINARY'"
echo "  sudo chmod 0755 '$CURRENT_BINARY'"
echo "  sudo systemctl start bmas"

echo
echo "To manually restore the previous configuration:"
echo "  sudo cp '$BACKUP_CONFIG' '$INSTALLED_CONFIG'"
echo "  sudo chown root:bmas '$INSTALLED_CONFIG'"
echo "  sudo chmod 0640 '$INSTALLED_CONFIG'"
echo "  sudo systemctl restart bmas"

echo

systemctl --no-pager --full status "$SERVICE_NAME" || true
