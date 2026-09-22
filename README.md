# Termux Restart-Watchdog mit Tunnel (Port 5000)

## Was das macht
- `watchdog.js` startet deinen Bot (`index.js`) als Kindprozess.
- Stürzt der Bot ab, startet der Watchdog ihn automatisch neu — mit
  exponentiellem Backoff (Werte kommen aus `restart-config.json`).
- Ein kleiner HTTP-Server läuft dabei auf **Port 5000** mit zwei Routen:
  - `GET /health` — Status des Bots (läuft er gerade?)
  - `POST /restart` — löst manuell einen Neustart aus
- `start-termux.sh` öffnet zusätzlich einen **Tunnel** (via `localtunnel`)
  auf Port 5000, sodass du `/health` und `/restart` auch von außerhalb
  deines Handynetzes erreichen kannst (z.B. um den Bot von unterwegs
  neu zu starten).

## Dateien in diesen Ordner legen
Kopiere `watchdog.js`, `restart-config.json`, `start-termux.sh`,
`stop-termux.sh` in denselben Ordner wie deine `index.js`.

## Einmalige Einrichtung in Termux

```bash
pkg update && pkg upgrade
pkg install nodejs termux-api
npm install -g localtunnel

chmod +x start-termux.sh stop-termux.sh
```

`termux-api` wird für Benachrichtigungen und den Wakelock gebraucht.
Installiere zusätzlich die **Termux:API-App** aus F-Droid, sonst
funktionieren `termux-wake-lock` / `termux-notification` nicht.

## Starten

```bash
./start-termux.sh
```

Das Skript zeigt dir danach die Tunnel-URL an, z.B.:
```
✅ Tunnel läuft: https://xyz123.loca.lt
   Health-Check: https://xyz123.loca.lt/health
   Restart:      curl -X POST https://xyz123.loca.lt/restart?token=DEIN_TOKEN
```

## Beenden

```bash
./stop-termux.sh
```

## Absichern des `/restart`-Endpunkts (empfohlen!)
Ohne Token kann **jeder**, der die Tunnel-URL kennt, deinen Bot neu
starten. Setz dir vor dem Start ein eigenes Token:

```bash
export RESTART_TOKEN="mein-geheimes-token-123"
./start-termux.sh
```

Neustart dann nur noch mit:
```bash
curl -X POST "https://xyz123.loca.lt/restart?token=mein-geheimes-token-123"
```

## Beim Handy-Neustart automatisch starten (optional)
Installiere die **Termux:Boot-App** (F-Droid), lege dann an:

`~/.termux/boot/start-bot.sh`:
```bash
#!/data/data/com.termux/files/usr/bin/bash
cd /pfad/zu/deinem/bot-ordner
./start-termux.sh
```

```bash
chmod +x ~/.termux/boot/start-bot.sh
```

## Wichtige Konfigurationswerte (`restart-config.json`)
| Feld | Bedeutung |
|---|---|
| `restart.maxRetries` | Wie oft nacheinander automatisch neu gestartet wird |
| `restart.backoff.multiplier` | Wartezeit verdoppelt sich pro Versuch |
| `restart.backoff.maxDelay` | Obergrenze der Wartezeit (30s) |
| `watchdog.port` | Port für Health-/Restart-Server (5000) |
| `session.clearOnMaxRetries` | Ob die WhatsApp-Session gelöscht wird, wenn alle Neustarts fehlschlagen |
| `schedule.dailyRestart` | Optionaler täglicher Neustart zu fester Uhrzeit |

## Hinweis zu localtunnel
`localtunnel` ist kostenlos und braucht keinen Account, ist aber nicht
für Produktionslasten gedacht (URL ändert sich bei jedem Neustart,
gelegentliche Downtimes). Für stabilere URLs kannst du stattdessen
`cloudflared` (`pkg install tur-repo && pkg install cloudflared`,
dann `cloudflared tunnel --url http://localhost:5000`) verwenden —
das Prinzip bleibt identisch.
