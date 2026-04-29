#!/usr/bin/env bash
# Install websocet.py as a systemd service on Ubuntu so it starts on boot
# and restarts automatically on failure.
#
# Usage:   sudo ./deploy/install.sh
# Env:     INSTALL_DIR (default /opt/websocet)
#          SERVICE_USER (default current invoking user, or "websocet" when run via sudo from root)
#          SERVICE_NAME (default websocet)
#
# After install:
#   sudo systemctl status websocet
#   journalctl -u websocet -f

set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root (use sudo)." >&2
  exit 1
fi

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${INSTALL_DIR:-/opt/websocet}"
SERVICE_NAME="${SERVICE_NAME:-websocet}"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-websocet}}"

echo "==> Repo dir:    ${REPO_DIR}"
echo "==> Install dir: ${INSTALL_DIR}"
echo "==> Service:     ${SERVICE_NAME}.service"
echo "==> Run as user: ${SERVICE_USER}"

# 1. System packages
echo "==> Installing system packages (python3, venv, pip)..."
apt-get update -y
apt-get install -y python3 python3-venv python3-pip

# 2. Service user
if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  echo "==> Creating system user '${SERVICE_USER}'..."
  useradd --system --create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

# 3. Install files
echo "==> Copying application files to ${INSTALL_DIR}..."
mkdir -p "${INSTALL_DIR}"
install -m 0644 "${REPO_DIR}/websocet.py" "${INSTALL_DIR}/websocet.py"
if [[ -f "${REPO_DIR}/requirements.txt" ]]; then
  install -m 0644 "${REPO_DIR}/requirements.txt" "${INSTALL_DIR}/requirements.txt"
fi

# 4. Python venv + dependencies
echo "==> Creating virtualenv and installing dependencies..."
python3 -m venv "${INSTALL_DIR}/.venv"
"${INSTALL_DIR}/.venv/bin/pip" install --upgrade pip
if [[ -f "${INSTALL_DIR}/requirements.txt" ]]; then
  "${INSTALL_DIR}/.venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt"
else
  "${INSTALL_DIR}/.venv/bin/pip" install websockets requests pymongo
fi

# 5. Optional .env (placeholder if missing)
if [[ ! -f "${INSTALL_DIR}/.env" ]]; then
  cat >"${INSTALL_DIR}/.env" <<'ENV'
# Environment variables for websocet.service.
# Add MongoDB connection settings or other secrets here, e.g.:
# MONGO_URI=mongodb://localhost:27017
ENV
  chmod 0640 "${INSTALL_DIR}/.env"
fi

chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}"

# 6. Render and install systemd unit
UNIT_SRC="${REPO_DIR}/deploy/websocet.service"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}.service"
echo "==> Writing ${UNIT_DST}..."
sed \
  -e "s|\${SERVICE_USER}|${SERVICE_USER}|g" \
  -e "s|\${INSTALL_DIR}|${INSTALL_DIR}|g" \
  "${UNIT_SRC}" >"${UNIT_DST}"
chmod 0644 "${UNIT_DST}"

# 7. Enable and start
echo "==> Reloading systemd and starting service..."
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
systemctl restart "${SERVICE_NAME}.service"

echo
echo "==> Done. Useful commands:"
echo "    sudo systemctl status ${SERVICE_NAME}"
echo "    sudo journalctl -u ${SERVICE_NAME} -f"
echo "    sudo systemctl restart ${SERVICE_NAME}"
echo "    sudo systemctl stop ${SERVICE_NAME}"
