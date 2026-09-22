'use strict';

const { spawn } = require('child_process');
const http = require('http');
const fs = require('fs');
const path = require('path');

const CONFIG_PATH = path.join(__dirname, 'restart-config.json');
let config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));

const LOG_DIR = path.dirname(path.join(__dirname, config.logging.file));
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const LOG_FILE = path.join(__dirname, config.logging.file);

// ── Logging mit einfacher Größen-Rotation ──
function parseSize(sizeStr) {
  const m = String(sizeStr).match(/^(\d+)([kmg])?$/i);
  if (!m) return 10 * 1024 * 1024;
  const num = parseInt(m[1], 10);
  const unit = (m[2] || '').toLowerCase();
  if (unit === 'k') return num * 1024;
  if (unit === 'm') return num * 1024 * 1024;
  if (unit === 'g') return num * 1024 * 1024 * 1024;
  return num;
}

function rotateLogsIfNeeded() {
  try {
    if (!fs.existsSync(LOG_FILE)) return;
    const maxBytes = parseSize(config.logging.maxFileSize);
    const stats = fs.statSync(LOG_FILE);
    if (stats.size < maxBytes) return;

    const maxFiles = config.logging.maxFiles || 5;
    for (let i = maxFiles - 1; i >= 1; i--) {
      const src = `${LOG_FILE}.${i}`;
      const dst = `${LOG_FILE}.${i + 1}`;
      if (fs.existsSync(src)) {
        if (i + 1 > maxFiles) fs.unlinkSync(src);
        else fs.renameSync(src, dst);
      }
    }
    fs.renameSync(LOG_FILE, `${LOG_FILE}.1`);
  } catch (e) {
    console.error('[watchdog] Log-Rotation fehlgeschlagen:', e.message);
  }
}

function log(level, msg) {
  if (!config.logging.enabled) return;
  const ts = config.logging.timestamps ? `[${new Date().toISOString()}] ` : '';
  const line = `${ts}[${level.toUpperCase()}] ${msg}`;

  if (config.notifications.channels.console) {
    console.log(line);
  }
  if (config.notifications.channels.logFile) {
    rotateLogsIfNeeded();
    fs.appendFileSync(LOG_FILE, line + '\n');
  }
}

function notifyTermux(title, content) {
  if (!config.notifications.channels.termux) return;
  try {
    const { execSync } = require('child_process');
    const safeTitle = title.replace(/"/g, '\\"');
    const safeContent = content.replace(/"/g, '\\"');
    execSync(`termux-notification --title "${safeTitle}" --content "${safeContent}"`, { stdio: 'ignore' });
  } catch (e) {
    // termux-notification evtl. nicht installiert (termux-api Paket) — kein Fehler werfen
  }
}

// ── Prozess-Management ──
let child = null;
let restartCount = 0;
let lastRestartTime = 0;
let manualStop = false;
let currentDelay = config.restart.retryDelay;

function startProcess() {
  const scriptPath = path.join(__dirname, config.process.script);
  log('info', `Starte Prozess: ${config.process.interpreter} ${scriptPath}`);

  child = spawn(config.process.interpreter, [scriptPath], {
    cwd: path.join(__dirname, config.process.cwd || './'),
    env: { ...process.env, NODE_ENV: config.process.env || 'production' },
    stdio: 'inherit'
  });

  child.on('exit', (code, signal) => {
    log('info', `Prozess beendet (Code: ${code}, Signal: ${signal})`);
    child = null;

    if (manualStop) {
      manualStop = false;
      return;
    }

    if (!config.restart.enabled) return;

    // "on-failure": nur bei Fehlercode neu starten; bei code 0 nicht.
    if (config.restart.strategy === 'on-failure' && code === 0) {
      log('info', 'Sauberer Exit (Code 0) — kein Neustart laut Strategie "on-failure".');
      return;
    }

    handleRestart();
  });

  child.on('error', (err) => {
    log('error', `Fehler beim Starten des Prozesses: ${err.message}`);
  });
}

function handleRestart() {
  if (restartCount >= config.restart.maxRetries) {
    log('error', `Maximale Anzahl an Neustarts (${config.restart.maxRetries}) erreicht. Gebe auf.`);
    if (config.notifications.onMaxRetriesReached) {
      notifyTermux('Bot-Watchdog', `Maximale Neustarts erreicht (${config.restart.maxRetries}). Bot bleibt offline.`);
    }

    if (config.session.clearOnMaxRetries) {
      const sessionPath = path.join(__dirname, config.session.sessionPath);
      log('warn', `clearOnMaxRetries aktiv — lösche Session unter ${sessionPath}`);
      try {
        fs.rmSync(sessionPath, { recursive: true, force: true });
      } catch (e) {
        log('error', `Konnte Session nicht löschen: ${e.message}`);
      }
    }
    return;
  }

  restartCount++;
  lastRestartTime = Date.now();

  let delay = config.restart.retryDelay;
  if (config.restart.backoff && config.restart.backoff.type === 'exponential') {
    delay = Math.min(
      config.restart.retryDelay * Math.pow(config.restart.backoff.multiplier, restartCount - 1),
      config.restart.backoff.maxDelay
    );
  }
  currentDelay = delay;

  log('warn', `Neustart ${restartCount}/${config.restart.maxRetries} in ${delay}ms...`);
  if (config.notifications.onRestart) {
    notifyTermux('Bot-Watchdog', `Neustart ${restartCount}/${config.restart.maxRetries} in ${Math.round(delay / 1000)}s`);
  }

  setTimeout(() => {
    startProcess();
  }, delay);
}

function forceRestart(reason) {
  log('warn', `Manueller/erzwungener Neustart ausgelöst${reason ? ` (${reason})` : ''}.`);
  restartCount = 0; // manueller Neustart setzt Backoff-Zähler zurück
  if (child) {
    manualStop = false; // soll trotzdem den normalen Restart-Pfad durchlaufen
    child.kill('SIGTERM');
    setTimeout(() => {
      if (child) child.kill('SIGKILL');
    }, 5000);
  } else {
    startProcess();
  }
}

// ── Health-Check (intern, gegen den Kindprozess) ──
function isChildAlive() {
  if (!child) return false;
  try {
    process.kill(child.pid, 0);
    return true;
  } catch (e) {
    return false;
  }
}

// Periodischer Watchdog-Tick: prüft ob der Kindprozess noch lebt.
if (config.watchdog.enabled) {
  setInterval(() => {
    if (!child && !manualStop) {
      log('warn', 'Watchdog-Tick: Kein aktiver Prozess gefunden, versuche Neustart...');
      handleRestart();
    }
  }, config.watchdog.interval);
}

// ── Täglicher geplanter Neustart ──
if (config.schedule.dailyRestart.enabled) {
  setInterval(() => {
    const now = new Date();
    const localTime = now.toLocaleTimeString('de-DE', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: config.schedule.dailyRestart.timezone
    });
    if (localTime === config.schedule.dailyRestart.time) {
      forceRestart('geplanter täglicher Neustart');
    }
  }, 60 * 1000);
}

// ── HTTP-Server auf Port 5000: /health + /restart ──
// Dieser Port wird per Tunnel (localtunnel) nach außen freigegeben,
// damit sowohl externe Monitore als auch du selbst von unterwegs
// den Bot-Status prüfen bzw. einen Neustart auslösen könnt.
const RESTART_TOKEN = process.env.RESTART_TOKEN || null;

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    const alive = isChildAlive();
    res.writeHead(alive ? 200 : 503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: alive ? 'ok' : 'down',
      restartCount,
      lastRestartTime,
      currentDelay,
      pid: child ? child.pid : null
    }));
    return;
  }

  if (req.method === 'POST' && req.url.startsWith('/restart')) {
    if (RESTART_TOKEN) {
      const url = new URL(req.url, 'http://localhost');
      const token = url.searchParams.get('token');
      if (token !== RESTART_TOKEN) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Ungültiges Token' }));
        return;
      }
    }
    forceRestart('extern via /restart ausgelöst');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'restart-eingeleitet' }));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found', routes: ['GET /health', 'POST /restart[?token=...]'] }));
});

server.listen(config.watchdog.port, () => {
  log('info', `Watchdog-HTTP-Server läuft auf Port ${config.watchdog.port}`);
  if (RESTART_TOKEN) {
    log('info', 'RESTART_TOKEN ist gesetzt — /restart erfordert ?token=...');
  } else {
    log('warn', 'Kein RESTART_TOKEN gesetzt — /restart ist ungeschützt erreichbar sobald der Tunnel läuft!');
  }
});

// ── Sauberes Beenden ──
process.on('SIGINT', () => {
  log('info', 'Watchdog wird beendet (SIGINT)...');
  manualStop = true;
  if (child) child.kill('SIGTERM');
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  log('info', 'Watchdog wird beendet (SIGTERM)...');
  manualStop = true;
  if (child) child.kill('SIGTERM');
  server.close(() => process.exit(0));
});

// ── Start ──
log('info', '=== Watchdog gestartet ===');
startProcess();
