import TelegramBot from 'node-telegram-bot-api';

// ============================================================
// telegram-connect.js
// Steuert NUR die WhatsApp-Verbindung (Pairing-Code / QR-Code)
// über Telegram — kein eigenes Befehlssystem, keine eigene
// Economy. Nur du (Owner) kannst damit interagieren.
//
// SETUP:
// 1. npm install node-telegram-bot-api
// 2. Umgebungsvariablen setzen (NICHT hart in den Code schreiben):
//      TELEGRAM_BOT_TOKEN=dein_bot_token
//      OWNER_TELEGRAM_ID=deine_eigene_telegram_user_id
//    -> Deine Telegram-User-ID bekommst du z.B. von @userinfobot
// 3. Start z.B. mit: node --env-file=.env deinbot.js
// ============================================================

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '8614468465:AAHP7693iiKX56Sp-9TRNa3q2gGMBXOQ-ms';
const OWNER_TELEGRAM_ID = process.env.OWNER_TELEGRAM_ID ? Number(process.env.OWNER_TELEGRAM_ID) : 8598584607;

let telegramBot = null;
let currentSock = null;

function isOwnerChat(msg) {
  return OWNER_TELEGRAM_ID && msg.from && msg.from.id === OWNER_TELEGRAM_ID;
}

/**
 * Muss beim Start deines Haupt-Bots einmal aufgerufen werden.
 */
export function initTelegramConnect() {
  if (!TELEGRAM_BOT_TOKEN) {
    console.log('ℹ️ TELEGRAM_BOT_TOKEN nicht gesetzt — Telegram-Verbindung deaktiviert.');
    return null;
  }
  if (!OWNER_TELEGRAM_ID) {
    console.log('⚠️ OWNER_TELEGRAM_ID nicht gesetzt — Telegram-Bot startet nicht, damit niemand sonst deine WhatsApp-Session pairen kann.');
    return null;
  }

  telegramBot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
  telegramBot.on('polling_error', (e) => console.error('[telegram polling_error]', e.message));

  telegramBot.onText(/\/start|\/help/, (msg) => {
    if (!isOwnerChat(msg)) return;
    telegramBot.sendMessage(msg.chat.id,
      '🤖 *WhatsApp-Verbindung*\n\n' +
      '/pair <nummer> - Pairing-Code anfordern (z.B. /pair 49123456789)\n' +
      '/status - Verbindungsstatus prüfen\n' +
      '/unpair - WhatsApp-Verbindung trennen\n\n' +
      'Sobald ein QR-Code generiert wird, schicke ich ihn dir automatisch hier als Bild.',
      { parse_mode: 'Markdown' });
  });

  telegramBot.onText(/\/pair\s+(\d+)/, async (msg, match) => {
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff.');
    if (!currentSock) return telegramBot.sendMessage(msg.chat.id, '⚠️ WhatsApp-Bot ist noch nicht bereit. Versuch es in ein paar Sekunden erneut.');
    if (currentSock.authState?.creds?.registered) {
      return telegramBot.sendMessage(msg.chat.id, '✅ WhatsApp ist bereits verbunden.');
    }
    try {
      let code = await currentSock.requestPairingCode(match[1]);
      code = code?.match(/.{1,4}/g)?.join('-') || code;
      telegramBot.sendMessage(msg.chat.id,
        `🔑 Pairing-Code: *${code}*\n\nIn WhatsApp: Einstellungen → Verknüpfte Geräte → Gerät verknüpfen → "Stattdessen mit Telefonnummer verknüpfen" → Code eingeben.`,
        { parse_mode: 'Markdown' });
    } catch (e) {
      telegramBot.sendMessage(msg.chat.id, '❌ Fehler beim Generieren des Codes: ' + e.message);
    }
  });

  telegramBot.onText(/\/status/, (msg) => {
    if (!isOwnerChat(msg)) return;
    const connected = !!(currentSock && currentSock.user);
    telegramBot.sendMessage(msg.chat.id, connected
      ? `✅ Verbunden als ${currentSock.user.id}`
      : '⚠️ WhatsApp ist aktuell nicht verbunden.');
  });

  telegramBot.onText(/\/unpair/, async (msg) => {
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff.');
    if (!currentSock) return telegramBot.sendMessage(msg.chat.id, '⚠️ WhatsApp-Bot ist noch nicht bereit.');
    
    const connected = !!(currentSock && currentSock.user);
    if (!connected) {
      return telegramBot.sendMessage(msg.chat.id, '⚠️ WhatsApp ist nicht verbunden. Es gibt nichts zu trennen.');
    }

    try {
      await currentSock.logout();
      telegramBot.sendMessage(msg.chat.id, '✅ WhatsApp-Verbindung erfolgreich beendet. Du kannst dich jetzt neu pairen mit /pair');
    } catch (e) {
      telegramBot.sendMessage(msg.chat.id, '❌ Fehler beim Trennen: ' + e.message);
    }
  });

  console.log('✅ Telegram-Verbindungs-Bot gestartet. Schreib /start an deinen Bot.');
  return telegramBot;
}

/**
 * Muss aufgerufen werden, sobald du dein `sock`-Objekt (aus makeWASocket)
 * erstellt hast, damit /pair und /status funktionieren.
 */
export function setActiveSock(sock) {
  currentSock = sock;
}

/**
 * Optional: ruf das im QR-Handler deines Bots auf, um den QR-Code
 * zusätzlich per Telegram zu bekommen (praktisch, wenn du kein Terminal
 * zur Hand hast).
 */
export async function sendQrToTelegram(qrBuffer, caption = '📱 WhatsApp QR-Code zum Scannen') {
  if (!telegramBot || !OWNER_TELEGRAM_ID) return;
  try {
    await telegramBot.sendPhoto(OWNER_TELEGRAM_ID, qrBuffer, { caption });
  } catch (e) {
    console.error('Telegram QR send failed:', e.message);
  }
}
