// apply-bancheck.mjs (v5) — baut ?bancheck, ?einspruch und ?einspruchstatus komplett in deine Bot-Datei ein.
// Nutzung:  node apply-bancheck.mjs ./index.js [API-KEY]
// Der API-Key ist optional: Steht er schon in der .env, brauchst du ihn nicht anzugeben.
// Funktioniert für frische Dateien und überschreibt frühere Versionen (auch apply-appeal.mjs) sauber.
// Legt vorher ein Backup (<datei>.bak) an.
// v5: Bei API-Fehlern wird die komplette Antwort geloggt und im Chat der echte Grund angezeigt.

import fs from 'fs';
import path from 'path';

const FUNCS = String.raw`const BANCHECK_API_BASE = 'https://baron0.com';

function bancheckErrorText(status, body) {
  if (body && typeof body === 'object') {
    const msg = body.detail || body.title || body.error || body.message;
    if (msg) return typeof msg === 'string' ? msg : JSON.stringify(msg);
    return 'HTTP ' + status + ' — ' + JSON.stringify(body).slice(0, 300);
  }
  return 'HTTP ' + status + (body ? ' — ' + String(body).slice(0, 300) : '');
}

async function bancheckParse(res) {
  const text = await res.text();
  try { return JSON.parse(text); } catch { return text; }
}

async function bancheckRequest(endpoint, payload) {
  const apiKey = process.env.BANCHECK_API_KEY;
  if (!apiKey) throw new Error('BANCHECK_API_KEY fehlt in der .env');
  const res = await fetch(BANCHECK_API_BASE + endpoint, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const body = await bancheckParse(res);
  if (!res.ok) {
    console.error('[bancheck] HTTP', res.status, endpoint, JSON.stringify(body));
    throw new Error(bancheckErrorText(res.status, body));
  }
  console.log('[bancheck] API-Antwort:', JSON.stringify(body));
  return body;
}

async function bancheckGet(endpoint, params) {
  const apiKey = process.env.BANCHECK_API_KEY;
  if (!apiKey) throw new Error('BANCHECK_API_KEY fehlt in der .env');
  const url = new URL(BANCHECK_API_BASE + endpoint);
  for (const [k, v] of Object.entries(params || {})) url.searchParams.set(k, v);
  const res = await fetch(url.toString(), { method: 'GET', headers: { Authorization: 'Bearer ' + apiKey } });
  const body = await bancheckParse(res);
  if (!res.ok) {
    console.error('[appeal] HTTP', res.status, endpoint, JSON.stringify(body));
    throw new Error(bancheckErrorText(res.status, body));
  }
  console.log('[appeal] API-Antwort:', JSON.stringify(body));
  return body;
}

async function apiCheckNumber(number) {
  return bancheckRequest('/api/v2/check', { number });
}

async function apiBulkCheck(numbers) {
  const body = await bancheckRequest('/api/v2/bulk-check', { numbers });
  return body.results;
}

const toApiNumber = (raw) => {
  const digits = String(raw || '').replace(/[^0-9]/g, '');
  return digits.length >= 6 ? '+' + digits : null;
};

function describeBan(r) {
  if (!r.banned) return '✅ Nicht gebannt';

  // Alle Textwerte der Antwort durchsuchen (nicht nur "reason")
  const values = Object.values(r).filter(v => typeof v === 'string').join(' ').toLowerCase();
  const flagPerm = Object.entries(r).some(([k, v]) => /perm/i.test(k) && v === true);
  const flagSpam = Object.entries(r).some(([k, v]) => /spam/i.test(k) && v === true);
  const flagTemp = Object.entries(r).some(([k, v]) => /temp/i.test(k) && v === true);

  if (flagPerm || /perm|dauer|forever|lifetime/.test(values)) return '⛔ Perma-Ban';
  if (flagSpam || /spam/.test(values)) return '📛 Spam-Ban';
  if (/mod|moderat|review|violation|tos|terms/.test(values)) return '🛡️ Mod-Ban';
  if (flagTemp || /temp|vor.?übergehend/.test(values)) return '⏳ Temporärer Ban';

  // Unbekannte Art: Wert aus einem Typ-Feld (oder reason) anzeigen, sonst nur "Gebannt"
  const entry = Object.entries(r).find(([k, v]) => /type|kind|categor|class/i.test(k) && typeof v === 'string' && v.trim());
  const label = String(entry ? entry[1] : (r.reason || '')).trim().slice(0, 40);
  return label ? '🚫 Gebannt: ' + label : '🚫 Gebannt';
}

function describeAppeal(r) {
  const keys = ['status', 'state', 'result', 'message', 'detail', 'info'];
  const parts = keys
    .filter(k => r && r[k] !== undefined && r[k] !== null && typeof r[k] !== 'object')
    .map(k => k + ': ' + r[k]);
  return parts.length ? parts.join('\n') : JSON.stringify(r).slice(0, 300);
}

`;

const COMMAND = String.raw`if (cmd === 'einspruch' || cmd === 'appeal') {
  if (!isOwner) return send('❌ Nur Owner/CoOwner dürfen Einsprüche einreichen.');
  const number = toApiNumber(args[0]);
  if (!number) {
    return send('❌ Nutzung: ' + activePrefix + 'einspruch <nummer>\nBeispiel: ' + activePrefix + 'einspruch 491701234567');
  }

  await send('📨 Reiche Einspruch ein für ' + number + '...');

  try {
    const r = await bancheckRequest('/api/v2/appeal-submit', { number });
    return send(
      '📨 *Einspruch eingereicht*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
      '📞 ' + number + '\n' +
      describeAppeal(r)
    );
  } catch (e) {
    console.error('[appeal] Fehler:', e?.message || e);
    return send('❌ Einspruch fehlgeschlagen: ' + (e?.message || 'Unbekannter Fehler'));
  }
}

if (cmd === 'einspruchstatus' || cmd === 'appealstatus') {
  if (!isTeamMember) return send('❌ Nur für das Team.');
  const number = toApiNumber(args[0]);
  if (!number) {
    return send('❌ Nutzung: ' + activePrefix + 'einspruchstatus <nummer>\nBeispiel: ' + activePrefix + 'einspruchstatus 491701234567');
  }

  try {
    const r = await bancheckGet('/api/v2/appeal-status', { number });
    return send(
      '📋 *Einspruchsstatus*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
      '📞 ' + number + '\n' +
      describeAppeal(r)
    );
  } catch (e) {
    console.error('[appeal] Status-Fehler:', e?.message || e);
    return send('❌ Status-Abfrage fehlgeschlagen: ' + (e?.message || 'Unbekannter Fehler'));
  }
}

if (cmd === 'bancheck' || cmd === 'checkban') {
  const numbers = [...new Set(args.map(toApiNumber).filter(Boolean))].slice(0, isTeamMember ? 20 : 1);
  if (!numbers.length) {
    return send('❌ Nutzung: ' + activePrefix + 'bancheck <nummer> [nummer2 ...]\nBeispiel: ' + activePrefix + 'bancheck 491701234567');
  }

  const bancheckCd = checkCooldown(sender, 'bancheck');
  if (bancheckCd && !isOwner) return send(bancheckCd);

  await send('🔎 Prüfe ' + numbers.length + ' Nummer(n)...');

  try {
    if (numbers.length === 1) {
      const r = await apiCheckNumber(numbers[0]);
      return send(
        '📋 *Ban-Check*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
        '📞 ' + numbers[0] + '\n' +
        describeBan(r)
      );
    }

    const results = await apiBulkCheck(numbers);
    const lines = results.map(r =>
      r.number + ' — ' + describeBan(r)
    );
    return send('📋 *Ban-Check (' + results.length + ')*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' + lines.join('\n'));
  } catch (e) {
    console.error('[bancheck] Fehler:', e?.message || e);
    return send('❌ Ban-Check fehlgeschlagen: ' + (e?.message || 'Unbekannter Fehler'));
  }
}
`;

const FUNCS_ANCHOR = 'const randInt = (min, max)';
const SLEEP_ANCHOR = 'const sleep = ms =>';
const FUNCS_START = 'const BANCHECK_API_BASE';
const OLD_APPEAL_FUNCS = 'async function bancheckGet';
const CMD_ANCHOR = 'const lidHandled = await lidSystem.handle(';
const CMD_STARTS = [
  "if (cmd === 'einspruch' || cmd === 'appeal') {",
  "if (cmd === 'bancheck' || cmd === 'checkban') {"
];
const LIST_ANCHOR = "'bitchkick', 'givecommand',";

const target = process.argv[2];
const argKey = process.argv[3];
if (!target || !fs.existsSync(target)) {
  console.error('Nutzung: node apply-bancheck.mjs <pfad-zur-bot-datei> [API-KEY]');
  process.exit(1);
}

let src = fs.readFileSync(target, 'utf8');

for (const [name, a] of [['randInt', FUNCS_ANCHOR], ['sleep', SLEEP_ANCHOR], ['lidHandled', CMD_ANCHOR], ['ALL_COMMANDS', LIST_ANCHOR]]) {
  if (!src.includes(a)) {
    console.error('Abbruch: Einfüge-Stelle "' + name + '" nicht gefunden (' + a + ').');
    console.error('Bist du sicher, dass das die Datei ist, die ./start-termux.sh startet?');
    process.exit(1);
  }
}

fs.copyFileSync(target, target + '.bak');

// 0) Alte, separat eingebaute Einspruch-Funktionen (apply-appeal.mjs) entfernen,
//    damit nichts doppelt deklariert wird.
{
  const rI = src.indexOf(FUNCS_ANCHOR);
  const oldG = src.indexOf(OLD_APPEAL_FUNCS);
  const sI = src.indexOf(SLEEP_ANCHOR);
  if (oldG !== -1 && oldG > rI && sI > oldG) {
    src = src.slice(0, oldG) + src.slice(sI);
    console.log('Alte Einspruch-Funktionen (apply-appeal) entfernt.');
  }
}

// 1) Hilfsfunktionen: alte Version ersetzen oder neu einfügen
const fStart = src.indexOf(FUNCS_START);
const fAnchor = src.indexOf(FUNCS_ANCHOR);
if (fStart !== -1 && fStart < fAnchor) {
  src = src.slice(0, fStart) + FUNCS + src.slice(fAnchor);
  console.log('Hilfsfunktionen aktualisiert.');
} else if (fStart === -1) {
  src = src.replace(FUNCS_ANCHOR, () => FUNCS + FUNCS_ANCHOR);
  console.log('Hilfsfunktionen eingefügt.');
} else {
  console.error('Abbruch: Alte Hilfsfunktionen stehen an unerwarteter Stelle.');
  process.exit(1);
}

// 2) Befehle: alte Version(en) ersetzen oder neu einfügen
const cAnchor = src.indexOf(CMD_ANCHOR);
const cStarts = CMD_STARTS.map(s => src.indexOf(s)).filter(i => i !== -1);
const cStart = cStarts.length ? Math.min(...cStarts) : -1;
if (cStart !== -1 && cStart < cAnchor) {
  src = src.slice(0, cStart) + COMMAND + src.slice(cAnchor);
  console.log('Befehle aktualisiert.');
} else if (cStart === -1) {
  src = src.replace(CMD_ANCHOR, () => COMMAND + CMD_ANCHOR);
  console.log('Befehle eingefügt.');
} else {
  console.error('Abbruch: Alte Befehle stehen an unerwarteter Stelle.');
  process.exit(1);
}

// 3) Befehlsliste (für den Tippfehler-Vorschlag)
if (!src.includes("'bancheck', 'checkban'")) {
  src = src.replace(LIST_ANCHOR, () => LIST_ANCHOR + " 'bancheck', 'checkban',");
}
if (!src.includes("'einspruch', 'appeal'")) {
  src = src.replace("'bancheck', 'checkban',", () => "'bancheck', 'checkban', 'einspruch', 'appeal', 'einspruchstatus', 'appealstatus',");
}

fs.writeFileSync(target, src);

// 4) API-Key in die .env (nur wenn angegeben oder noch nicht vorhanden)
const envPath = path.join(path.dirname(path.resolve(target)), '.env');
let env = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
const hasKey = /^BANCHECK_API_KEY=/m.test(env);
if (argKey) {
  env = env.replace(/^BANCHECK_API_KEY=.*\r?\n?/m, '');
  if (env && !env.endsWith('\n')) env += '\n';
  fs.writeFileSync(envPath, env + 'BANCHECK_API_KEY=' + argKey + '\n');
  console.log('API-Key in ' + envPath + ' eingetragen.');
} else if (hasKey) {
  console.log('.env enthält schon BANCHECK_API_KEY — nicht angefasst.');
} else {
  console.log('⚠️ In der .env fehlt BANCHECK_API_KEY. Führe das Skript nochmal aus mit: node apply-bancheck.mjs ' + target + ' DEIN_KEY');
}

console.log('Fertig. Backup: ' + target + '.bak — Bot neu starten, dann: ?bancheck 491701234567');