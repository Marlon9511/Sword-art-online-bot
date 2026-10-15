// apply-bancheck.mjs (v3) — baut den ?bancheck-Befehl komplett in deine Bot-Datei ein.
// Nutzung:  node apply-bancheck.mjs ./index.js
// Funktioniert für frische Dateien und überschreibt eine frühere Version sauber.
// Legt vorher ein Backup (<datei>.bak) an und trägt den API-Key in die .env ein.

import fs from 'fs';
import path from 'path';

const API_KEY = 'bk_v1_F9gm0Zxhm8fYrGj4P7yOUKRkUsxs926rzHfaDV7xQarWP5C982Gv1JMwbVQKMADx98h53ldK8hZo6NdTO-31B7bJcShJ1FtvGfz14Ujp7eFzNfvA5Gd4giJlWCI1svlyU4E-We0CQX0XwOe2TotT3yP9toQyRXpzIHp5u_lnVWogc7bYueACSVy7no-gNBPy7A52GWv3yynF2Zl_ig_9EYKW6CdAFxgGDkicD1tVbghCP2FssOnC0x_FJtz_diUqjyOJJ8uUMhFes6EsTG9EllEKDggqBBLcXn95xAGaK2QfxTi-I_Mj79-IQSFzwm_HWn4gsjYkcET9WECxB0Ddx9ZqqyOAoWlzI95yDd6eVTFXDXQ1onb65k-Oq8XdcgTbXzpT8m2KVuAoVyNdkYOT74h3JvIYxRF21Z3-R5N11ZeB_KxTNHq-NemSiO8GDYHcYQbyCr7yBP8R04d0pANHJyg0iAQVEXdY1tVSjznBXrnbAVcBEMVDKTWKM5k7cscsgPPS2FUp0nK96xH4tra5n_H6ZYrIdCcaRZhlFgMlNJKF7TnKgH3CcuArT6sQNhC4og5WuLkSES21etQI';

const FUNCS = String.raw`const BANCHECK_API_BASE = 'https://baron0.com';

async function bancheckRequest(endpoint, payload) {
  const apiKey = process.env.BANCHECK_API_KEY;
  if (!apiKey) throw new Error('BANCHECK_API_KEY fehlt in der .env');
  const res = await fetch(BANCHECK_API_BASE + endpoint, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const body = await res.json();
  if (!res.ok) throw new Error(body.detail || body.title || ('HTTP ' + res.status));
  console.log('[bancheck] API-Antwort:', JSON.stringify(body));
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

`;

const COMMAND = String.raw`if (cmd === 'bancheck' || cmd === 'checkban') {
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
const FUNCS_START = 'const BANCHECK_API_BASE';
const CMD_ANCHOR = 'const lidHandled = await lidSystem.handle(';
const CMD_START = "if (cmd === 'bancheck' || cmd === 'checkban') {";
const LIST_ANCHOR = "'bitchkick', 'givecommand',";

const target = process.argv[2];
if (!target || !fs.existsSync(target)) {
  console.error('Nutzung: node apply-bancheck.mjs <pfad-zur-bot-datei>');
  process.exit(1);
}

let src = fs.readFileSync(target, 'utf8');

for (const [name, a] of [['randInt', FUNCS_ANCHOR], ['lidHandled', CMD_ANCHOR], ['ALL_COMMANDS', LIST_ANCHOR]]) {
  if (!src.includes(a)) {
    console.error('Abbruch: Einfüge-Stelle "' + name + '" nicht gefunden (' + a + ').');
    console.error('Bist du sicher, dass das die Datei ist, die ./start-termux.sh startet?');
    process.exit(1);
  }
}

fs.copyFileSync(target, target + '.bak');

// 1) Hilfsfunktionen: alte Version ersetzen oder neu einfügen
const fStart = src.indexOf(FUNCS_START);
const fAnchor = src.indexOf(FUNCS_ANCHOR);
if (fStart !== -1 && fStart < fAnchor) {
  src = src.slice(0, fStart) + FUNCS + src.slice(fAnchor);
  console.log('Hilfsfunktionen aktualisiert.');
} else if (fStart === -1) {
  src = src.replace(FUNCS_ANCHOR, FUNCS + FUNCS_ANCHOR);
  console.log('Hilfsfunktionen eingefügt.');
} else {
  console.error('Abbruch: Alte Hilfsfunktionen stehen an unerwarteter Stelle.');
  process.exit(1);
}

// 2) Befehl: alte Version ersetzen oder neu einfügen
const cStart = src.indexOf(CMD_START);
const cAnchor = src.indexOf(CMD_ANCHOR);
if (cStart !== -1 && cStart < cAnchor) {
  src = src.slice(0, cStart) + COMMAND + src.slice(cAnchor);
  console.log('Befehl aktualisiert.');
} else if (cStart === -1) {
  src = src.replace(CMD_ANCHOR, COMMAND + CMD_ANCHOR);
  console.log('Befehl eingefügt.');
} else {
  console.error('Abbruch: Alter Befehl steht an unerwarteter Stelle.');
  process.exit(1);
}

// 3) Befehlsliste (für den Tippfehler-Vorschlag)
if (!src.includes("'bancheck', 'checkban'")) {
  src = src.replace(LIST_ANCHOR, LIST_ANCHOR + " 'bancheck', 'checkban',");
}

fs.writeFileSync(target, src);

// 4) API-Key in die .env
const envPath = path.join(path.dirname(path.resolve(target)), '.env');
let env = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
if (!/^BANCHECK_API_KEY=/m.test(env)) {
  if (env && !env.endsWith('\n')) env += '\n';
  fs.writeFileSync(envPath, env + 'BANCHECK_API_KEY=' + API_KEY + '\n');
  console.log('API-Key in ' + envPath + ' eingetragen.');
} else {
  console.log('.env enthält schon BANCHECK_API_KEY — nicht überschrieben.');
}

console.log('Fertig. Backup: ' + target + '.bak — Bot neu starten, dann: ?bancheck 491701234567');
