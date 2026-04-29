# Running `websocet.py` as a systemd service on Ubuntu

This directory contains everything needed to run `websocet.py` as a
[systemd](https://www.freedesktop.org/wiki/Software/systemd/) service on
Ubuntu so it starts automatically on boot and restarts on failure.

Files:

- `websocet.service` — systemd unit template (`${SERVICE_USER}` and
  `${INSTALL_DIR}` are substituted by the installer).
- `install.sh` — one-shot installer: creates a system user, copies files
  to `/opt/websocet`, builds a Python virtualenv, installs dependencies,
  writes the unit file to `/etc/systemd/system/`, then enables and
  starts the service.
- `uninstall.sh` — disables and removes the service (keeps files unless
  `PURGE=1`).

## Quick start

```bash
git clone https://github.com/Georgepop/Telegram_pars.git
cd Telegram_pars
sudo ./deploy/install.sh
```

That's it — the service is now running and will start on every boot.

## Verify it is running

```bash
sudo systemctl status websocet
sudo journalctl -u websocet -f      # follow live logs
```

## Day‑to‑day commands

```bash
sudo systemctl restart websocet     # restart
sudo systemctl stop websocet        # stop
sudo systemctl start websocet       # start
sudo systemctl disable websocet     # don't start on boot
sudo systemctl enable websocet      # start on boot
```

## Configuration

`install.sh` honours these environment variables:

| Variable        | Default                              | Purpose                              |
|-----------------|--------------------------------------|--------------------------------------|
| `INSTALL_DIR`   | `/opt/websocet`                      | Where files and the venv live        |
| `SERVICE_USER`  | `$SUDO_USER` or `websocet`           | Unix user the service runs as        |
| `SERVICE_NAME`  | `websocet`                           | Name of the systemd unit             |

Example:

```bash
sudo INSTALL_DIR=/srv/websocet SERVICE_USER=botuser ./deploy/install.sh
```

Secrets and runtime configuration (e.g. MongoDB URI) can be placed in
`/opt/websocet/.env` — the unit loads it via `EnvironmentFile=`. The
installer creates a placeholder file on first install.

## Manual installation (without `install.sh`)

```bash
# 1. System packages
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip

# 2. Application files + venv
sudo mkdir -p /opt/websocet
sudo cp websocet.py requirements.txt /opt/websocet/
sudo python3 -m venv /opt/websocet/.venv
sudo /opt/websocet/.venv/bin/pip install -r /opt/websocet/requirements.txt

# 3. Service user (optional — you can also reuse an existing user)
sudo useradd --system --create-home --shell /usr/sbin/nologin websocet
sudo chown -R websocet:websocet /opt/websocet

# 4. Install the unit file (substitute placeholders)
sudo sed \
  -e 's|${SERVICE_USER}|websocet|g' \
  -e 's|${INSTALL_DIR}|/opt/websocet|g' \
  deploy/websocet.service | sudo tee /etc/systemd/system/websocet.service

# 5. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now websocet
```

## Uninstall

```bash
sudo ./deploy/uninstall.sh           # removes the service unit
sudo PURGE=1 ./deploy/uninstall.sh   # also deletes /opt/websocet
```

## Troubleshooting

- **Service is `failed`** — inspect logs:
  `journalctl -u websocet -n 200 --no-pager`.
- **`ModuleNotFoundError`** — re-run `install.sh`, or:
  `sudo /opt/websocet/.venv/bin/pip install -r /opt/websocet/requirements.txt`.
- **`mongopy` import error** — `websocet.py` imports `from mongopy import *`,
  which is a project-local module. Place `mongopy.py` (or the package)
  next to `websocet.py` inside `${INSTALL_DIR}` before starting the
  service.
- **Port / network errors on first start** — the unit waits for
  `network-online.target`, but on slow networks you may need to
  `sudo systemctl restart websocet` once the network is up.
