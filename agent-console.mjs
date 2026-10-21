// agent-console.mjs
// Owner-Konsole für deinen WhatsApp-Agenten "Kirito" (Meta WhatsApp Agent Platform).
// Läuft PARALLEL zu Baileys. Nur der Ersteller (du) wird bedient, alle anderen werden ignoriert.
//
// .env:
//   WHATSAPP_AGENT_API_KEY=...            (Chat mit Kirito -> Chat-Info -> API key)
//   WHATSAPP_AGENT_OWNER_ID=              (optional: feste ID des Erstellers, sonst wird die
//                                          erste Absender-ID gespeichert = "Trust on first use")
//
// ⚠️ ANNAHMEN: Die Datei basiert auf dem, was über die API öffentlich bekannt ist
// (https://api.whatsapp.com/agent/v1, GET /updates per Long-Poll, POST /messages).
// Auth-Header, Feldnamen und Sende-Body habe ich NICHT aus dem offiziellen Handbuch.
// Alles, was ggf. angepasst werden muss, steht im Block "ANPASSEN" direkt unten.

import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';

// ───────────────────────── ANPASSEN (falls das Handbuch anders sagt) ─────────────────────────
// 👉 HIER deinen API-Key eintragen (zwischen die Anführungszeichen):
const HARDCODED_API_KEY = 'WAAViU6pxGSMmtoN6ZAZAPZBxC6lRjxcDRsBiPOmZBPw4gfTQwBZB7diZCfGfwTWiWr3MsTb4GCAftOu5BVK86bOzKEBGnQDrJDRZCoEIlA4ktZAwe2BOlxjSHHcZADRQ5LYMrKipgSYZAt8fw7ydY7dUTc9ZCBooir7SIF5d4VOdHPv1xAXSCDCnXEfq';

const BASE_URL = process.env.WHATSAPP_AGENT_BASE_URL || 'https://api.whatsapp.com/agent/v1';

const authHeaders = (key) => ({
  Authorization: `Bearer ${key}`,
  'Content-Type': 'application/json'
});

// Eine Update-Antwort -> { updates: [...], cursor }
function parseUpdatesResponse(data) {
  const list = data?.updates || data?.messages || data?.data || (Array.isArray(data) ? data : []);
  const cursor = data?.cursor ?? data?.next_cursor ?? data?.offset ?? null;
  return { updates: Array.isArray(list) ? list : [], cursor };
}

// Ein einzelnes Update -> { id, from, text } oder null (z.B. Medien, Reaktionen)
function normalizeUpdate(u) {
  const msg = u?.message || u;
  const text = msg?.text?.body ?? (typeof msg?.text === 'string' ? msg.text : null) ?? msg?.body ?? null;
  const from = msg?.from ?? msg?.user_id ?? msg?.sender ?? u?.from ?? null;
  const id = msg?.id ?? u?.id ?? null;
  if (!text || !from) return null;
  return { id: id ? String(id) : null, from: String(from), text: String(text) };
}

// Body für POST /messages
function buildSendBody(to, text) {
  return { to, type: 'text', text: { body: text } };
}
// ───────────────────────────────────────────────────────────────────────────────────────────

const MAX_CHARS = 4000;          // laut Meta werden längere Antworten gesplittet
const SEND_PER_MIN = 12;         // Meta-Limit Nachrichten
const POLL_MIN_INTERVAL = 4500;  // max ~13 Polls/Minute (Limit: 15)
const POLL_TIMEOUT_S = 25;       // Long-Poll-Dauer

export function startAgentConsole(ctx) {
  const {
    DATA_PATH, BASE_DIR, LOG_FILE,
    getSock,            // () => Baileys-Socket der 'default'-Session
    getStats,           // () => { users, registered, sessions }
    groupChatLog,       // Map aus index.js
    normalizeGroupId,   // Funktion aus index.js
    setOffline,         // (bool) => void
    getOffline,         // () => bool
    restart             // () => void
  } = ctx;

  const API_KEY = HARDCODED_API_KEY || process.env.WHATSAPP_AGENT_API_KEY;
  if (!API_KEY) {
    console.log('[agent-console] Kein API-Key: trage ihn in agent-console.mjs (HARDCODED_API_KEY) ein.');
    return { stop() {} };
  }

  const STATE_FILE = path.join(DATA_PATH, 'agent-console.json');
  let state = { cursor: null, owner: process.env.WHATSAPP_AGENT_OWNER_ID || null };
  try {
    if (fs.existsSync(STATE_FILE)) {
      const saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      state = { ...saved, owner: process.env.WHATSAPP_AGENT_OWNER_ID || saved.owner || null };
    }
  } catch {}
  const saveState = () => {
    try { fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2)); } catch {}
  };

  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  let running = true;
  const seen = new Set();

  // ── Senden mit Rate-Limit (12/Minute) ──
  const sentAt = [];
  async function waitForSendSlot() {
    for (;;) {
      const now = Date.now();
      while (sentAt.length && now - sentAt[0] > 60_000) sentAt.shift();
      if (sentAt.length < SEND_PER_MIN) { sentAt.push(now); return; }
      await sleep(60_000 - (now - sentAt[0]) + 100);
    }
  }

  async function sendText(to, text) {
    const parts = [];
    let rest = String(text);
    while (rest.length > MAX_CHARS) {
      let cut = rest.lastIndexOf('\n', MAX_CHARS);
      if (cut < MAX_CHARS / 2) cut = MAX_CHARS;
      parts.push(rest.slice(0, cut));
      rest = rest.slice(cut);
    }
    parts.push(rest);

    for (const part of parts) {
      await waitForSendSlot();
      try {
        const res = await fetch(`${BASE_URL}/messages`, {
          method: 'POST',
          headers: authHeaders(API_KEY),
          body: JSON.stringify(buildSendBody(to, part))
        });
        if (res.status === 429) {
          console.log('[agent-console] 429 beim Senden – warte 15s.');
          await sleep(15_000);
        } else if (!res.ok) {
          console.error(`[agent-console] Senden fehlgeschlagen: HTTP ${res.status}`);
        }
      } catch (e) {
        console.error('[agent-console] Senden Fehler:', e?.message || e);
      }
    }
  }

  // ── Befehle (nur Owner) ──
  const tailErrors = (n) => {
    const targets = [
      { label: 'logs.txt', p: LOG_FILE },
      { label: 'watchdog-stdout.log', p: path.join(BASE_DIR, 'logs', 'watchdog-stdout.log') },
      { label: 'tunnel.log', p: path.join(BASE_DIR, 'logs', 'tunnel.log') }
    ];
    let out = '';
    for (const t of targets) {
      if (!fs.existsSync(t.p)) continue;
      let lines;
      try {
        lines = fs.readFileSync(t.p, 'utf8').split('\n')
          .filter(l => /error|fehler|exception|throw|uncaught/i.test(l))
          .slice(-n);
      } catch { continue; }
      if (!lines.length) continue;
      // gleiche Zeilen zusammenfassen (z.B. 15x derselbe 429)
      const counted = [];
      for (const l of lines) {
        const last = counted[counted.length - 1];
        if (last && last.l === l) last.c++; else counted.push({ l, c: 1 });
      }
      out += `\n*${t.label}:*\n` + counted.map(x => x.c > 1 ? `${x.l}  (x${x.c})` : x.l).join('\n') + '\n';
    }
    return out || '✅ Keine Fehler in den letzten Log-Einträgen gefunden.';
  };

  const COMMANDS = {
    help: () =>
      '🗡️ *Kirito-Konsole*\n' +
      'help – diese Liste\n' +
      'ping – Lebenszeichen\n' +
      'status – Bot-Status\n' +
      'stats – Nutzer/Sessions\n' +
      'errors [n] – letzte Fehler (Standard 10)\n' +
      'gsend <gruppenid> <text> – in Gruppe schreiben\n' +
      'gread <gruppenid> [n] – Gruppenchat lesen\n' +
      'offline on|off – Bot-Offline-Modus\n' +
      'restart – Bot neu starten',

    ping: async () => '🏓 Pong!',

    status: async () => {
      const sock = getSock?.();
      return `🤖 Bot-Socket: ${sock?.user ? '✅ verbunden' : '❌ nicht verbunden'}\n` +
             `🔌 Offline-Modus: ${getOffline?.() ? 'AN' : 'AUS'}`;
    },

    stats: async () => {
      const s = getStats?.() || {};
      return `📊 Nutzer gesamt: ${s.users ?? '?'}\n📝 Registriert: ${s.registered ?? '?'}\n🖥️ Sessions: ${s.sessions ?? '?'}`;
    },

    errors: async (args) => tailErrors(Math.min(Math.max(parseInt(args[0]) || 10, 1), 50)),

    gsend: async (args) => {
      const sock = getSock?.();
      if (!sock) return '❌ Bot nicht verbunden.';
      const gid = normalizeGroupId(args[0]);
      const text = args.slice(1).join(' ').trim();
      if (!gid || !text) return '❌ Nutzung: gsend <gruppenid> <text>';
      try {
        await sock.sendMessage(gid, { text });
        return `✅ Gesendet an ${gid}`;
      } catch (e) {
        return '❌ Fehlgeschlagen: ' + (e?.message || e);
      }
    },

    gread: async (args) => {
      const gid = normalizeGroupId(args[0]);
      if (!gid) return '❌ Nutzung: gread <gruppenid> [anzahl]';
      const n = Math.min(Math.max(parseInt(args[1]) || 15, 1), 50);
      const entries = (groupChatLog.get(gid) || []).slice(-n);
      if (!entries.length) return 'ℹ️ Keine Nachrichten gespeichert (nur seit dem letzten Start).';
      return entries.map(e => {
        const t = new Date(e.at).toLocaleString('de-DE', { dateStyle: 'short', timeStyle: 'short' });
        return `[${t}] ${e.fromMe ? 'Bot' : String(e.sender).split('@')[0]}: ${e.text}`;
      }).join('\n');
    },

    offline: async (args) => {
      const a = (args[0] || '').toLowerCase();
      if (['on', 'an'].includes(a)) { setOffline(true); return '✅ Offline-Modus AN.'; }
      if (['off', 'aus'].includes(a)) { setOffline(false); return '✅ Bot wieder online.'; }
      return '❌ Nutzung: offline on|off';
    },

    restart: async (_args, from) => {
      await sendText(from, '🔄 Bot startet neu...');
      setTimeout(() => restart(), 1500);
      return null;
    }
  };

  async function handle(update) {
    const msg = normalizeUpdate(update);
    if (!msg) return;
    if (msg.id) {
      if (seen.has(msg.id)) return;
      seen.add(msg.id);
      if (seen.size > 500) seen.delete(seen.values().next().value);
    }

    // Owner-Pinning (Trust on first use, oder fest per .env)
    if (!state.owner) {
      state.owner = msg.from;
      saveState();
      console.log('[agent-console] Owner-ID gespeichert (erste Nachricht).');
    }
    if (msg.from !== state.owner) {
      console.log('[agent-console] Nachricht von Fremden ignoriert.');
      return;
    }

    const text = msg.text.trim().replace(/^[?\/!#$.]/, '');
    const [cmdRaw, ...args] = text.split(/\s+/);
    const cmd = (cmdRaw || '').toLowerCase();
    const fn = COMMANDS[cmd];

    if (!fn) {
      return sendText(msg.from, '❓ Unbekannt. Schreib "help".');
    }
    try {
      const out = typeof fn === 'function' ? await fn(args, msg.from) : fn;
      if (out) await sendText(msg.from, out);
    } catch (e) {
      console.error('[agent-console] Befehlsfehler:', e?.message || e);
      await sendText(msg.from, '❌ Fehler: ' + (e?.message || 'unbekannt'));
    }
  }

  // ── Long-Poll-Schleife ──
  (async () => {
    let conflicts = 0;
    console.log('[agent-console] gestartet (Long-Polling).');
    while (running) {
      const started = Date.now();
      try {
        const url = new URL(`${BASE_URL}/updates`);
        if (state.cursor != null) url.searchParams.set('cursor', String(state.cursor));
        url.searchParams.set('timeout', String(POLL_TIMEOUT_S));

        const res = await fetch(url.toString(), { headers: authHeaders(API_KEY) });

        if (res.status === 401 || res.status === 403) {
          console.error('[agent-console] API-Key ungültig (evtl. neu generiert). Konsole gestoppt.');
          running = false;
          break;
        }
        if (res.status === 409) {
          conflicts++;
          console.error('[agent-console] 409: Ein anderer Poller nutzt denselben Key. Pause 60s.');
          if (conflicts >= 3) { running = false; break; }
          await sleep(60_000);
          continue;
        }
        if (res.status === 429) { await sleep(15_000); continue; }
        if (!res.ok) {
          console.error(`[agent-console] Poll HTTP ${res.status}`);
          await sleep(10_000);
          continue;
        }
        conflicts = 0;

        const data = await res.json().catch(() => ({}));
        const { updates, cursor } = parseUpdatesResponse(data);
        for (const u of updates) await handle(u);
        if (cursor != null && cursor !== state.cursor) { state.cursor = cursor; saveState(); }
      } catch (e) {
        console.error('[agent-console] Poll-Fehler:', e?.message || e);
        await sleep(10_000);
      }

      const elapsed = Date.now() - started;
      if (elapsed < POLL_MIN_INTERVAL) await sleep(POLL_MIN_INTERVAL - elapsed);
    }
  })();

  return { stop() { running = false; } };
}
