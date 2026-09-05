// yugioh/storage.mjs
import fs from 'fs';
import path from 'path';

/**
 * Simpler JSON-Datei-Speicher, analog zu deinem bestehenden load()/save()-Muster,
 * aber in sich geschlossen, damit dieses System unabhängig von db.js funktioniert.
 * Legt seine Datei(en) direkt in deinem vorhandenen DATA_PATH-Ordner ab.
 */
export function createJsonStore(dataPath, fileName) {
  const filePath = path.join(dataPath, fileName);

  const ensure = () => {
    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, JSON.stringify({}, null, 2), 'utf-8');
    }
  };
  ensure();

  const readAll = () => {
    try {
      return JSON.parse(fs.readFileSync(filePath, 'utf-8') || '{}');
    } catch (e) {
      console.error(`[yugioh-storage] Fehler beim Lesen von ${filePath}:`, e.message);
      return {};
    }
  };

  const writeAll = (data) => {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8');
  };

  return {
    get: (key) => readAll()[key],
    set: (key, value) => {
      const data = readAll();
      data[key] = value;
      writeAll(data);
    },
    delete: (key) => {
      const data = readAll();
      delete data[key];
      writeAll(data);
    },
    readAll,
  };
}
