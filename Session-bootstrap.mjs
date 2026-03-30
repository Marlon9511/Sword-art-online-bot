import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
import fs from 'fs';
import path from 'path';
import P from 'pino';
import QRCodeTerminal from 'qrcode-terminal';

// Session name wird als Argument übergeben (z.B. von PM2: -- test)
const sessionName = process.argv[2];

if (!sessionName) {
  console.error('❌ Kein Session-Name angegeben! Nutzung: node session-bootstrap.mjs <sessionName>');
  process.exit(1);
}

const BASE_DIR = path.resolve('./');
const SESSIONS_DIR = path.join(BASE_DIR, 'sessions');
const SESSION_PATH = path.join(SESSIONS_DIR, sessionName);

// Session-Ordner anlegen falls nicht vorhanden
if (!fs.existsSync(SESSION_PATH)) {
  fs.mkdirSync(SESSION_PATH, { recursive: true });
}

console.log(`🚀 Starte Session: ${sessionName}`);
console.log(`📁 Session-Pfad: ${SESSION_PATH}`);

async function startSession() {
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_PATH);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    logger: P({ level: 'silent' }),
    printQRInTerminal: false,
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    // QR-Code im Terminal anzeigen
    if (qr) {
      console.log(`\n📱 QR-Code für Session "${sessionName}" – bitte scannen:\n`);
      QRCodeTerminal.generate(qr, { small: true });
    }

    if (connection === 'open') {
      const id = sock.user?.id ?? '(unbekannt)';
      console.log(`✅ Session "${sessionName}" verbunden! JID: ${id}`);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      console.log(`🔌 Session "${sessionName}" getrennt. Code: ${code}`);

      // Automatisch neu verbinden außer bei Logout (401)
      if (code !== 401) {
        console.log('🔄 Versuche Neuverbindung...');
        setTimeout(startSession, 5000);
      } else {
        console.log('🚫 Session ausgeloggt. Bitte neu scannen.');
        // Auth-Dateien löschen damit beim nächsten Start ein neuer QR kommt
        try {
          fs.rmSync(SESSION_PATH, { recursive: true, force: true });
          fs.mkdirSync(SESSION_PATH, { recursive: true });
        } catch (e) {
          console.error('Fehler beim Löschen der Auth-Dateien:', e);
        }
        setTimeout(startSession, 3000);
      }
    }
  });

  // Nachrichten empfangen und einfach loggen (eigene Logik hier einfügbar)
  sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const msg of messages) {
      if (!msg.message || msg.key.fromMe) continue;
      const from = msg.key.remoteJid;
      const text =
        msg.message?.conversation ||
        msg.message?.extendedTextMessage?.text ||
        '';
      if (text) {
        console.log(`[${sessionName}] Nachricht von ${from}: ${text}`);
      }
    }
  });
}

startSession().catch((err) => {
  console.error('❌ Fehler beim Starten der Session:', err);
  process.exit(1);
});