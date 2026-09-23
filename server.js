import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();

// eigener Ordner, damit nichts mit deinem bestehenden "public"-Ordner
// (den deine Bot-API auf Port 3001 nutzt) kollidiert
app.use(express.static(path.join(__dirname, 'dashboard-public')));

const PORT = process.env.PORT || 5050;
app.listen(PORT, () => {
  console.log(`✅ Aincrad-Terminal läuft auf http://localhost:${PORT}`);
});
