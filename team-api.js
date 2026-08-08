// team-api.js
// Kleines Zusatzmodul für deinen SAO-Bot. Läuft im selben Prozess wie
// index.js und liest ranks/users direkt aus dem Speicher — dadurch ist
// die Webseite SOFORT aktuell, sobald du z.B. ?setrole ausführst.
//
// Diese Version liest die Webseite aus public/index.html (statt sie
// einzubetten) — bequemer zum Bearbeiten der Seite direkt in Termux.
//
// Startet zusätzlich automatisch einen localtunnel, damit die Seite
// öffentlich erreichbar ist. Der öffentliche Link ändert sich bei jedem
// Neustart und wird beim Start in der Konsole ausgegeben + in
// current-link.txt gespeichert.
//
// Installation (im Bot-Ordner, einmalig):
//   npm install localtunnel

import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import localtunnel from 'localtunnel';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LINK_FILE = path.join(__dirname, 'current-link.txt');

// Reihenfolge + Anzeige-Rollen für die Team-Seite.
// USER und VIP werden absichtlich weggelassen (kein "Team").
const ROLE_ORDER = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'];

export function startTeamApi({ getRanks, getUsers, port = 3000, htmlPath, onLink } = {}) {
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

    // ---- Webseite aus public/index.html ausliefern ----
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

  server.listen(port, async () => {
    console.log('✅ Player-Menu läuft lokal auf Port ' + port);
    console.log('   Webseite-Datei: ' + resolvedHtmlPath);

    // ---- Automatisch öffentlichen Tunnel-Link erzeugen ----
    try {
      const tunnel = await localtunnel({ port });
      console.log('🔗 Öffentlicher Link: ' + tunnel.url);

      try {
        fs.writeFileSync(LINK_FILE, tunnel.url + '\n');
      } catch (e) {
        console.error('[team-api] Konnte current-link.txt nicht schreiben:', e.message);
      }

      if (typeof onLink === 'function') {
        try { onLink(tunnel.url); } catch (e) {}
      }

      tunnel.on('close', () => {
        console.log('❌ Tunnel wurde geschlossen (z.B. wegen Inaktivität). Bot-Neustart erzeugt einen neuen Link.');
      });

      tunnel.on('error', (err) => {
        console.error('[team-api] Tunnel-Fehler:', err.message);
      });
    } catch (err) {
      console.error('❌ Konnte keinen öffentlichen Tunnel-Link erzeugen:', err.message);
      console.error('   Die Seite ist trotzdem lokal unter http://localhost:' + port + '/ erreichbar.');
    }
  });

  return server;
}
