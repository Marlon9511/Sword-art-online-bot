#!/data/data/com.termux/files/usr/bin/bash
# Beendet Watchdog + Bot + Tunnel sauber — inklusive verwaister Prozesse,
# die nicht (mehr) über .watchdog.pid / .tunnel.pid bekannt sind
# (z.B. weil start-termux.sh mehrfach ohne vorheriges Stoppen lief).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🛑 Beende bekannte Prozesse (per PID-Datei)..."

if [ -f .watchdog.pid ]; then
  WPID=$(cat .watchdog.pid)
  kill -TERM "$WPID" 2>/dev/null || true
  rm -f .watchdog.pid
fi

if [ -f .tunnel.pid ]; then
  TPID=$(cat .tunnel.pid)
  kill -TERM "$TPID" 2>/dev/null || true
  rm -f .tunnel.pid
fi

sleep 1

echo "🧹 Räume verwaiste Prozesse auf (falls mehrfach gestartet wurde)..."

# pkill -f matcht den vollen Kommandozeilen-String, findet also auch
# Prozesse, deren PID nirgendwo mehr gespeichert war.
pkill -9 -f "node.*watchdog\.js" 2>/dev/null || true
pkill -9 -f "node.*index\.js" 2>/dev/null || true
pkill -9 -f "lt --port" 2>/dev/null || true
pkill -9 -f "localtunnel" 2>/dev/null || true

sleep 1

rm -f .tunnel-url.txt

echo "🔓 Löse Wakelock..."
termux-wake-unlock 2>/dev/null || true

REMAINING=$(pgrep -f "node.*(watchdog|index)\.js" 2>/dev/null || true)
if [ -n "$REMAINING" ]; then
  echo "⚠️ Es laufen noch Prozesse (PIDs: $REMAINING) — versuche erneut mit SIGKILL..."
  kill -9 $REMAINING 2>/dev/null || true
fi

echo "✅ Gestoppt. Prüfe zur Sicherheit mit: ps aux | grep node"
