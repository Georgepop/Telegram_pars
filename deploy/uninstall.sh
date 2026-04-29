#!/usr/bin/env bash
# Remove the websocet systemd service installed by deploy/install.sh.
#
# Usage: sudo ./deploy/uninstall.sh
# Env:   INSTALL_DIR (default /opt/websocet)
#        SERVICE_NAME (default websocet)
#        PURGE=1 to also delete INSTALL_DIR

set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root (use sudo)." >&2
  exit 1
fi

INSTALL_DIR="${INSTALL_DIR:-/opt/websocet}"
SERVICE_NAME="${SERVICE_NAME:-websocet}"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}.service"

if systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
  systemctl disable --now "${SERVICE_NAME}.service" || true
fi

rm -f "${UNIT_DST}"
systemctl daemon-reload

if [[ "${PURGE:-0}" == "1" ]]; then
  echo "==> Removing ${INSTALL_DIR}..."
  rm -rf "${INSTALL_DIR}"
fi

echo "==> Uninstalled ${SERVICE_NAME}.service"
