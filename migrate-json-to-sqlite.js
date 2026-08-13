// migrate-json-to-sqlite.js
// EINMALIG ausführen, bevor du db.js in index.js einbindest.
// Übernimmt alle bestehenden Daten aus /data/*.json in bot.db.
// Die ursprünglichen .json-Dateien werden NICHT gelöscht (dienen als Backup).
//
// Aufruf: node migrate-json-to-sqlite.js

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createStore } from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_PATH = path.join(__dirname, 'data');

// Muss zu deiner FILES-Konstante im Bot passen
const FILES = [
  'users.json', 'bans.json', 'joinreq.json', 'pets.json', 'tickets.json',
  'ranks.json', 'command-bans.json', 'broadcast-settings.json', 'deleted.json',
  'owner.json', 'team-todos.json', 'user-todos.json', 'group-invites.json',
  'group-settings.json', 'credits.json', 'guilds.json', 'marriages.json',
  'command-allow.json', 'official-group.json', 'partners.json',
  'group-lock-schedule.json', 'bitchkick.json'
];

if (!fs.existsSync(DATA_PATH)) {
  console.error(`❌ Datenordner nicht gefunden: ${DATA_PATH}`);
  process.exit(1);
}

const store = createStore(DATA_PATH);

let migrated = 0, skipped = 0, failed = 0;

for (const file of FILES) {
  const filePath = path.join(DATA_PATH, file);
  if (!fs.existsSync(filePath)) {
    console.log(`⏭️  ${file} — nicht vorhanden, übersprungen.`);
    skipped++;
    continue;
  }
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    const data = raw.trim() ? JSON.parse(raw) : {};
    store.save(file, data);
    const count = Array.isArray(data) ? data.length : Object.keys(data).length;
    console.log(`✅ ${file} übernommen (${count} Einträge)`);
    migrated++;
  } catch (e) {
    console.error(`❌ ${file} konnte NICHT migriert werden: ${e.message}`);
    failed++;
  }
}

store.close();

console.log('\n──────────────────────────────');
console.log(`Fertig: ${migrated} migriert, ${skipped} übersprungen, ${failed} fehlgeschlagen.`);
console.log(`Neue Datenbank: ${path.join(DATA_PATH, 'bot.db')}`);
console.log('Die alten .json-Dateien liegen weiterhin in /data (Backup, kannst du später löschen).');
