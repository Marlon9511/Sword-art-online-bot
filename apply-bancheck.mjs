// apply-bancheck.mjs — baut den ?bancheck-Befehl in deine Bot-Datei ein.
// Nutzung:  node apply-bancheck.mjs ./index.js
// Legt vorher ein Backup (<datei>.bak) an und trägt den API-Key in die .env ein.

import fs from 'fs';
import path from 'path';

const API_KEY = 'bk_v1_Nrr2NnM3cs0vMBl3rVabq4dx1fl6wDpzPTSJX1WNJQkrK-sK1clk9GK8ZACgU2tYenHT8ByfZsPR2wGPaqNHpLMlk5op1T0ohql28veCD4BeSZ5l214TII9TWC7ZrX8tOK8K3BEl68b3K0RUaWyilOmYmkN_WMHukwoAHBiEJKi6D5i8jUHI12SoolfDNLcKH_1Uzh0IKrt49r7HRWr3CgPUfjl0zRrZFJnRt68zaYdntriZ6tCYECQM9wa0sFKID25f1L0LsoRsEwQjJhGRkvVWty8_jG_QJBAjdfl8tDgPu1bX0UYsv7zPilHy5hPI6nJqOGS88kVJEMbVK2sih9toLoEoHgyKmJ4VwhbPDx67C-pOLGdn8K96KgagWOMBwoln89PVcRK2jt2M25N54hVG_eCycz0pGhXEqtWNi1bEnWGYqYgGMiH-p_SoV4vxJsiAAgTPIBrvX9ZWp1CT_VrgMFSJ9BnFIiVmiJrKf_oiJnT28DM-u6pOBQdzVEntBL9dVGTn-rNj5D0igYkIGU9kBq7Q8Lr8rvK8BxCvw6Q7HZuB0wg';

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

`;

const COMMAND = String.raw`if (cmd === 'bancheck' || cmd === 'checkban') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('❌ Kein Zugriff.');

  const numbers = [...new Set(args.map(toApiNumber).filter(Boolean))];
  if (!numbers.length) {
    return send('❌ Nutzung: ' + activePrefix + 'bancheck <nummer> [nummer2 ...]\nBeispiel: ' + activePrefix + 'bancheck 491701234567');
  }

  await send('🔎 Prüfe ' + numbers.length + ' Nummer(n)...');

  try {
    if (numbers.length === 1) {
      const r = await apiCheckNumber(numbers[0]);
      return send(
        '📋 *Ban-Check*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
        '📞 ' + numbers[0] + '\n' +
        (r.banned ? '🚫 Gebannt' + (r.reason ? ': ' + r.reason : '') : '✅ Nicht gebannt')
      );
    }

    const results = await apiBulkCheck(numbers);
    const lines = results.map(r =>
      (r.banned ? '🚫 ' : '✅ ') + r.number + (r.banned && r.reason ? ' — ' + r.reason : '')
    );
    return send('📋 *Ban-Check (' + results.length + ')*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' + lines.join('\n'));
  } catch (e) {
    console.error('[bancheck] Fehler:', e?.message || e);
    return send('❌ Ban-Check fehlgeschlagen: ' + (e?.message || 'Unbekannter Fehler'));
  }
}
`;

const target = process.argv[2];
if (!target || !fs.existsSync(target)) {
  console.error('Nutzung: node apply-bancheck.mjs <pfad-zur-bot-datei>');
  process.exit(1);
}

let src = fs.readFileSync(target, 'utf8');

if (src.includes('apiCheckNumber')) {
  console.error('Abbruch: Der Ban-Check ist in dieser Datei bereits eingebaut.');
  process.exit(1);
}

const anchors = {
  funcs: 'const randInt = (min, max)',
  command: 'const lidHandled = await lidSystem.handle(',
  list: "'bitchkick', 'givecommand',"
};
for (const [name, a] of Object.entries(anchors)) {
  if (!src.includes(a)) {
    console.error('Abbruch: Einfüge-Stelle "' + name + '" nicht gefunden (' + a + ').');
    process.exit(1);
  }
}

fs.copyFileSync(target, target + '.bak');

src = src.replace(anchors.funcs, FUNCS + anchors.funcs);
src = src.replace(anchors.command, COMMAND + anchors.command);
src = src.replace(anchors.list, anchors.list + " 'bancheck', 'checkban',");
fs.writeFileSync(target, src);

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
