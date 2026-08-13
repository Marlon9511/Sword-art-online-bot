// db.js
// Ersetzt die bisherigen JSON-Datei-Reads/Writes durch SQLite.
// Jede bisherige "Datei" (users.json, ranks.json, ...) wird als ein
// Datensatz (Blob) in der Tabelle kv_store gespeichert, unter genau dem
// Dateinamen als Key, den du bisher auch verwendet hast (FILES.x.file).
//
// Vorteil gegenüber fs.writeFileSync:
// - Atomare Writes (SQLite-Transaktion), kein "halb geschriebenes" JSON mehr
// - WAL-Modus: übersteht einen harten Prozess-Kill (z.B. Android killt Termux)
//   ohne die Datenbank zu zerstören
// - Ein einziges bot.db statt 20+ Einzeldateien

import Database from 'better-sqlite3';
import path from 'path';

export function createStore(dataDir) {
  const dbPath = path.join(dataDir, 'bot.db');
  const db = new Database(dbPath);

  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL'); // guter Kompromiss: sicher gegen App-Kills,
                                      // nur bei echtem Stromausfall theoretisch riskant

  db.exec(`
    CREATE TABLE IF NOT EXISTS kv_store (
      file TEXT PRIMARY KEY,
      data TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);

  const getStmt = db.prepare('SELECT data FROM kv_store WHERE file = ?');
  const upsertStmt = db.prepare(`
    INSERT INTO kv_store (file, data, updated_at)
    VALUES (@file, @data, @updated_at)
    ON CONFLICT(file) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at
  `);

  // Gibt das gespeicherte Objekt zurück, oder {} wenn (noch) nicht vorhanden
  // bzw. bei defekten Daten (kann durch SQLite eigentlich nicht mehr passieren,
  // Absicherung bleibt trotzdem drin).
  function load(f) {
    let file = f;
    if (typeof file !== 'string') {
      if (file?.file) file = file.file;
      else { console.error('❌ INVALID FILE (load):', f); return {}; }
    }
    const row = getStmt.get(file);
    if (!row) return {};
    try {
      return JSON.parse(row.data);
    } catch (e) {
      console.error(`[db] Korrupte Daten für "${file}", gebe leeres Objekt zurück:`, e.message);
      return {};
    }
  }

  // Signatur bewusst identisch zu deiner bisherigen save()-Funktion,
  // inkl. der Abfederung falls mal ein FILES.x-Objekt statt String übergeben wird.
  function save(f, d) {
    let file = f;
    if (typeof file !== 'string') {
      if (file?.file) {
        file = file.file;
      } else {
        console.error('❌ INVALID FILE (save):', f);
        console.trace();
        return;
      }
    }
    try {
      upsertStmt.run({ file, data: JSON.stringify(d), updated_at: Date.now() });
    } catch (e) {
      console.error(`[db] Fehler beim Speichern von "${file}":`, e.message);
    }
  }

  function close() {
    db.close();
  }

  return { db, load, save, close };
}
