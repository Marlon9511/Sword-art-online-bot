#!/data/data/com.termux/files/usr/bin/bash
# ─────────────────────────────────────────────────────────
# Startskript für Termux: Bot + Watchdog + Tunnel (Port 5000)
# ─────────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT=5000
LOG_DIR="$SCRIPT_DIR/logs"
TUNNEL_LOG="$LOG_DIR/tunnel.log"
WATCHDOG_LOG="$LOG_DIR/watchdog-stdout.log"

mkdir -p "$LOG_DIR"

echo "🔒 Aktiviere Termux Wakelock (verhindert Sleep-Kill durch Android)..."
termux-wake-lock || echo "⚠️ termux-wake-lock nicht verfügbar (termux-api installiert?)"

echo "📦 Prüfe benötigte Pakete..."
if ! command -v node >/dev/null 2>&1; then
  echo "❌ Node.js nicht gefunden. Installiere mit: pkg install nodejs"
  exit 1
fi

if ! command -v lt >/dev/null 2>&1; then
  echo "📥 Installiere localtunnel global via npm..."
  npm install -g localtunnel
fi

# ── Alte Prozesse auf Port 5000 beenden (falls vorhanden) ──
OLD_PID=$(lsof -ti tcp:$PORT 2>/dev/null || true)
if [ -n "$OLD_PID" ]; then
  echo "🛑 Beende alten Prozess auf Port $PORT (PID $OLD_PID)..."
  kill -9 $OLD_PID 2>/dev/null || true
  sleep 1
fi

# ── Tunnel starten (localtunnel, Port 5000) ──
echo "🌐 Starte Tunnel auf Port $PORT..."
nohup lt --port $PORT > "$TUNNEL_LOG" 2>&1 &
TUNNEL_PID=$!
echo "$TUNNEL_PID" > "$SCRIPT_DIR/.tunnel.pid"

sleep 3

TUNNEL_URL=$(grep -o 'https://[a-zA-Z0-9.-]*\.loca\.lt' "$TUNNEL_LOG" | tail -n1)
if [ -n "$TUNNEL_URL" ]; then
  echo "✅ Tunnel läuft: $TUNNEL_URL"
  echo "   Health-Check: $TUNNEL_URL/health"
  echo "   Restart:      curl -X POST $TUNNEL_URL/restart?token=DEIN_TOKEN"
  termux-notification --title "Bot-Tunnel aktiv" --content "$TUNNEL_URL" 2>/dev/null || true
  echo "$TUNNEL_URL" > "$SCRIPT_DIR/.tunnel-url.txt"
else
  echo "⚠️ Tunnel-URL konnte nicht ermittelt werden, prüfe $TUNNEL_LOG"
fi

# ── Optional: eigenes Restart-Token setzen ──
# export RESTART_TOKEN="dein-geheimes-token"

# ── Watchdog starten (übernimmt Start/Restart von index.js) ──
echo "🐕 Starte Watchdog..."
nohup node "$SCRIPT_DIR/watchdog.js" > "$WATCHDOG_LOG" 2>&1 &
WATCHDOG_PID=$!
echo "$WATCHDOG_PID" > "$SCRIPT_DIR/.watchdog.pid"

echo ""
echo "✅ Alles gestartet."
echo "   Watchdog PID: $WATCHDOG_PID"
echo "   Tunnel PID:   $TUNNEL_PID"
echo "   Logs: $LOG_DIR/"
echo ""
echo "Beenden mit: ./stop-termux.sh"
