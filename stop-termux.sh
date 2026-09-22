#!/data/data/com.termux/files/usr/bin/bash
# Beendet Watchdog + Tunnel sauber

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .watchdog.pid ]; then
  WPID=$(cat .watchdog.pid)
  echo "🛑 Beende Watchdog (PID $WPID)..."
  kill -TERM "$WPID" 2>/dev/null || true
  rm -f .watchdog.pid
fi

if [ -f .tunnel.pid ]; then
  TPID=$(cat .tunnel.pid)
  echo "🛑 Beende Tunnel (PID $TPID)..."
  kill -TERM "$TPID" 2>/dev/null || true
  rm -f .tunnel.pid
fi

rm -f .tunnel-url.txt

echo "🔓 Löse Wakelock..."
termux-wake-unlock 2>/dev/null || true

echo "✅ Gestoppt."
