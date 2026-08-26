#!/usr/bin/env bash
#
# Bootstrap an Oracle Cloud "Always Free" VM to run StellarGate.
# Idempotent — safe to re-run.
#
#   curl -fsSL https://raw.githubusercontent.com/StellarGateLabs/StellarGate/main/deploy/setup-oracle.sh | bash
#   # or, from a clone:  bash deploy/setup-oracle.sh
#
# Installs Docker, opens the host firewall, and installs a systemd unit so the
# stack comes back after a reboot. It does NOT start the app — you still need
# to write deploy/stellargate.env and point DNS at this host first.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/StellarGateLabs/StellarGate.git}"
APP_DIR="${APP_DIR:-$HOME/StellarGate}"

log() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m warning:\033[0m %s\n' "$*"; }

[[ $EUID -eq 0 ]] && {
	echo "Run as a normal user with sudo, not as root." >&2
	exit 1
}

log "Architecture: $(uname -m)  Kernel: $(uname -r)"

# ── Docker ────────────────────────────────────────────────────────────────
if command -v docker >/dev/null 2>&1; then
	log "Docker already installed: $(docker --version)"
else
	log "Installing Docker"
	curl -fsSL https://get.docker.com | sudo sh
fi

if ! groups "$USER" | grep -qw docker; then
	sudo usermod -aG docker "$USER"
	warn "Added $USER to the docker group. Log out and back in (or run 'newgrp docker') before using docker without sudo."
fi

sudo systemctl enable --now docker

# ── Host firewall ─────────────────────────────────────────────────────────
#
# This is the step people miss on Oracle Cloud. Opening 80/443 in the VCN
# security list is necessary but NOT sufficient: Oracle's stock images also
# ship a restrictive local firewall that silently drops the traffic. Symptom
# is a connection that hangs rather than refuses, and a Caddy certificate that
# never issues because the ACME challenge cannot reach you.
log "Opening ports 80 and 443 on the host firewall"

if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
	# Oracle Linux / RHEL family
	sudo firewall-cmd --permanent --add-service=http
	sudo firewall-cmd --permanent --add-service=https
	sudo firewall-cmd --reload
	log "firewalld configured"
elif command -v iptables >/dev/null 2>&1; then
	# Ubuntu on Oracle: rules live in iptables with a REJECT catch-all, so new
	# ACCEPT rules must be INSERTed above it, not appended after.
	for port in 80 443; do
		if ! sudo iptables -C INPUT -p tcp --dport "$port" -j ACCEPT 2>/dev/null; then
			sudo iptables -I INPUT 6 -p tcp --dport "$port" -j ACCEPT
		fi
	done
	if command -v netfilter-persistent >/dev/null 2>&1; then
		sudo netfilter-persistent save
	else
		sudo DEBIAN_FRONTEND=noninteractive apt-get install -y iptables-persistent >/dev/null 2>&1 || true
		sudo netfilter-persistent save 2>/dev/null || warn "Could not persist iptables rules; they will be lost on reboot."
	fi
	log "iptables configured"
else
	warn "No recognised firewall tool. Open 80/443 manually."
fi

cat <<'EOF'

  ┌──────────────────────────────────────────────────────────────────┐
  │ ALSO required, in the Oracle Cloud console (the host firewall    │
  │ above is only half of it):                                       │
  │                                                                  │
  │   Networking → Virtual Cloud Networks → your VCN → Security      │
  │   Lists → Default → Add Ingress Rules                            │
  │                                                                  │
  │     Source 0.0.0.0/0   TCP   dest port 80                        │
  │     Source 0.0.0.0/0   TCP   dest port 443                       │
  │                                                                  │
  │ Without these, connections hang instead of being refused.        │
  └──────────────────────────────────────────────────────────────────┘

EOF

# ── Source ────────────────────────────────────────────────────────────────
if [[ -d $APP_DIR/.git ]]; then
	log "Updating existing checkout at $APP_DIR"
	git -C "$APP_DIR" pull --ff-only
else
	log "Cloning into $APP_DIR"
	command -v git >/dev/null 2>&1 || sudo apt-get install -y git || sudo dnf install -y git
	git clone "$REPO_URL" "$APP_DIR"
fi

# ── systemd unit ──────────────────────────────────────────────────────────
log "Installing systemd unit"
sudo tee /etc/systemd/system/stellargate.service >/dev/null <<UNIT
[Unit]
Description=StellarGate payment gateway
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${APP_DIR}
ExecStart=/usr/bin/docker compose --env-file deploy/stellargate.env -f deploy/docker-compose.prod.yml up -d --pull always
ExecStop=/usr/bin/docker compose --env-file deploy/stellargate.env -f deploy/docker-compose.prod.yml down
TimeoutStartSec=0
User=${USER}
Group=docker

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable stellargate.service >/dev/null
log "systemd unit installed and enabled (not started)"

cat <<EOF

  Next:

    1. cp $APP_DIR/deploy/stellargate.env.example $APP_DIR/deploy/stellargate.env
       chmod 600 $APP_DIR/deploy/stellargate.env
       \$EDITOR $APP_DIR/deploy/stellargate.env

       Generate both secrets with:  openssl rand -hex 32
       Set STELLARGATE_VERSION to a released tag from:
         https://github.com/StellarGateLabs/StellarGate/releases

    2. Point your domain's A record at this host:
         $(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || echo "<this VM's public IP>")
       Caddy cannot issue a certificate until DNS resolves here.

    3. sudo systemctl start stellargate

    4. curl https://<your-domain>/health

  Logs:    docker compose --env-file deploy/stellargate.env -f deploy/docker-compose.prod.yml logs -f
  Status:  systemctl status stellargate

EOF
