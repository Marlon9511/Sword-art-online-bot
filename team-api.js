// team-api.js
// Kleines Zusatzmodul für deinen SAO-Bot. Läuft im selben Prozess wie
// index.js und liest ranks/users direkt aus dem Speicher — dadurch ist
// die Webseite SOFORT aktuell, sobald du z.B. ?setrole ausführst.
//
// Keine neuen npm-Pakete nötig (nur Node-eigene Module).

import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Reihenfolge + Anzeige-Rollen für die Team-Seite.
// USER und VIP werden absichtlich weggelassen (kein "Team").
const ROLE_ORDER = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'];

export function startTeamApi({ getRanks, getUsers, port = 3000, htmlPath }) {
  const resolvedHtmlPath = htmlPath || path.join(__dirname, 'public', 'index.html');

  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');

    // ---- API: aktuelle Team-Rollen ----
    if (req.url === '/api/team' && req.method === 'GET') {
      try {
        const ranks = getRanks() || {};
        const users = getUsers() || {};

        const grouped = {};
        for (const role of ROLE_ORDER) grouped[role] = [];

        for (const [jid, role] of Object.entries(ranks)) {
          if (!ROLE_ORDER.includes(role)) continue;
          const u = users[jid];
          const name = u?.name || u?.registrationName || jid.split('@')[0];
          if (!grouped[role].includes(name)) grouped[role].push(name);
        }

        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(grouped));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: e.message }));
      }
      return;
    }

    // ---- Webseite selbst ausliefern ----
    if ((req.url === '/' || req.url === '/index.html') && req.method === 'GET') {
      fs.readFile(resolvedHtmlPath, (err, data) => {
        if (err) {
          res.writeHead(404);
          res.end('index.html nicht gefunden unter: ' + resolvedHtmlPath);
          return;
        }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(data);
      });
      return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  });

  server.listen(port, () => {
    console.log(`✅ Player-Menu läuft auf Port ${port}`);
    console.log(`   Webseite: http://localhost:${port}/`);
    console.log(`   API:      http://localhost:${port}/api/team`);
  });

  return server;
}
