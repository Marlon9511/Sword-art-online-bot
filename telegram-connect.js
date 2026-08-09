import TelegramBot from 'node-telegram-bot-api';

// ============================================================
// telegram-connect.js
// Steuert BELIEBIG VIELE WhatsApp-Sessions (Pairing-Code / QR-Code)
// über Telegram — kein eigenes Befehlssystem, keine eigene
// Economy. Nur du (Owner) kannst damit interagieren.
//
// SETUP:
// 1. npm install node-telegram-bot-api
// 2. Umgebungsvariablen setzen (NICHT hart in den Code schreiben):
//      TELEGRAM_BOT_TOKEN=dein_bot_token
//      OWNER_TELEGRAM_ID=deine_eigene_telegram_user_id
//    -> Deine Telegram-User-ID bekommst du z.B. von @userinfobot
// 3. Start z.B. mit: node --env-file=.env index.js
//
// WICHTIG: index.js muss beim Start initTelegramConnect(sessionManager)
// mit einem sessionManager-Objekt aufrufen (siehe unten im Kommentar
// am Ende der Datei für ein Beispiel, wie das aussehen muss).
// ============================================================

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '8614468465:AAHP7693iiKX56Sp-9TRNa3q2gGMBXOQ-ms';
const OWNER_TELEGRAM_ID = process.env.OWNER_TELEGRAM_ID ? Number(process.env.OWNER_TELEGRAM_ID) : 8598584607;

let telegramBot = null;
let sessionManager = null;

function isOwnerChat(msg) {
  return OWNER_TELEGRAM_ID && msg.from && msg.from.id === OWNER_TELEGRAM_ID;
}

function requireManager(chatId) {
  if (!sessionManager) {
    telegramBot.sendMessage(chatId, '⚠️ Session-Manager ist nicht angebunden. Prüfe die initTelegramConnect(...)-Aufruf in index.js.');
    return false;
  }
  return true;
}

/**
 * @param {object} manager - Muss folgende Methoden bereitstellen:
 *   - startSession(name, hooks) -> Promise<sock>   // hooks: { onQr(qrBuffer), onOpen(jid) }
 *   - getSession(name) -> sock | undefined
 *   - listSessions() -> Array<{ name, connected, jid }>
 *   - stopSession(name) -> Promise<void>
 *   - deleteSession(name) -> Promise<void>          // stoppt + löscht Login-Daten
 */
export function initTelegramConnect(manager) {
  sessionManager = manager || null;

  if (!TELEGRAM_BOT_TOKEN) {
    console.log('ℹ️ TELEGRAM_BOT_TOKEN nicht gesetzt — Telegram-Verbindung deaktiviert.');
    return null;
  }
  if (!OWNER_TELEGRAM_ID) {
    console.log('⚠️ OWNER_TELEGRAM_ID nicht gesetzt — Telegram-Bot startet nicht, damit niemand sonst deine WhatsApp-Sessions pairen kann.');
    return null;
  }

  telegramBot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
  telegramBot.on('polling_error', (e) => console.error('[telegram polling_error]', e.message));

  telegramBot.onText(/\/start|\/help/, (msg) => {
    if (!isOwnerChat(msg)) return;
    telegramBot.sendMessage(msg.chat.id,
      '🤖 *WhatsApp-Session-Verwaltung*\n\n' +
      '/newsession <name> - neue Session starten (QR-Code kommt automatisch)\n' +
      '/pair <name> <nummer> - Pairing-Code statt QR anfordern (z.B. /pair bot2 49123456789)\n' +
      '/sessions - alle aktiven Sessions auflisten\n' +
      '/status <name> - Status einer bestimmten Session\n' +
      '/unpair <name> - Session trennen (ausloggen, Login-Daten bleiben)\n' +
      '/deletesession <name> - Session stoppen UND Login-Daten löschen\n\n' +
      'Du kannst so viele Sessions starten, wie du willst — jede unter einem eigenen Namen.',
      { parse_mode: 'Markdown' });
  });

  // ---- Neue Session starten (QR-Code-Flow) ----
  telegramBot.onText(/\/newsession\s+(\S+)/, async (msg, match) => {
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff.');
    if (!requireManager(msg.chat.id)) return;

    const name = match[1];
    if (sessionManager.getSession(name)) {
      return telegramBot.sendMessage(msg.chat.id, `⚠️ Session "${name}" läuft bereits. Nutze /status ${name}.`);
    }

    telegramBot.sendMessage(msg.chat.id, `⏳ Starte Session "${name}"...`);

    try {
      await sessionManager.startSession(name, {
        onQr: async (qrBuffer) => {
          try {
            await telegramBot.sendPhoto(OWNER_TELEGRAM_ID, qrBuffer, {
              caption: `📱 QR-Code für Session "${name}"\nScanne mit WhatsApp: Verknüpfte Geräte → Gerät verknüpfen`
            });
          } catch (e) {
            console.error('[telegram] QR senden fehlgeschlagen:', e.message);
          }
        },
        onOpen: async (jid) => {
          try {
            await telegramBot.sendMessage(OWNER_TELEGRAM_ID, `✅ Session "${name}" verbunden!\nJID: ${jid || '(unbekannt)'}`);
          } catch (e) {}
        }
      });
    } catch (e) {
      console.error('[telegram] Session-Start fehlgeschlagen:', e);
      telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Starten von "${name}": ${e.message}`);
    }
  });

  // ---- Pairing-Code statt QR anfordern ----
  telegramBot.onText(/\/pair\s+(\S+)\s+(\d+)/, async (msg, match) => {
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff.');
    if (!requireManager(msg.chat.id)) return;

    const [, name, number] = match;
    let sock = sessionManager.getSession(name);

    // Falls die Session noch gar nicht existiert, gleich mit erstellen
    if (!sock) {
      telegramBot.sendMessage(msg.chat.id, `⏳ Session "${name}" existiert noch nicht — erstelle sie...`);
      try {
        sock = await sessionManager.startSession(name, {
          onOpen: async (jid) => {
            try { await telegramBot.sendMessage(OWNER_TELEGRAM_ID, `✅ Session "${name}" verbunden!\nJID: ${jid || '(unbekannt)'}`); } catch (e) {}
          }
        });
      } catch (e) {
        return telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Erstellen von "${name}": ${e.message}`);
      }
    }

    if (sock.authState?.creds?.registered) {
      return telegramBot.sendMessage(msg.chat.id, `✅ Session "${name}" ist bereits verbunden.`);
    }

    try {
      let code = await sock.requestPairingCode(number);
      code = code?.match(/.{1,4}/g)?.join('-') || code;
      telegramBot.sendMessage(msg.chat.id,
        `🔑 Pairing-Code für "${name}": *${code}*\n\n` +
        'In WhatsApp: Einstellungen → Verknüpfte Geräte → Gerät verknüpfen → "Stattdessen mit Telefonnummer verknüpfen" → Code eingeben.',
        { parse_mode: 'Markdown' });
    } catch (e) {
      telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Generieren des Codes für "${name}": ${e.message}`);
    }
  });

  // ---- Alle Sessions auflisten ----
  telegramBot.onText(/\/sessions/, (msg) => {
    if (!isOwnerChat(msg)) return;
    if (!requireManager(msg.chat.id)) return;

    const list = sessionManager.listSessions();
    if (!list.length) {
      return telegramBot.sendMessage(msg.chat.id, 'ℹ️ Keine aktiven Sessions. Starte eine mit /newsession <name>.');
    }
    const text = list.map(s => `${s.connected ? '✅' : '⏳'} *${s.name}* — ${s.jid || '(verbindet...)'}`).join('\n');
    telegramBot.sendMessage(msg.chat.id, `📋 *Aktive Sessions* (${list.length}):\n\n${text}`, { parse_mode: 'Markdown' });
  });

  // ---- Status einer einzelnen Session ----
  telegramBot.onText(/\/status(?:\s+(\S+))?/, (msg, match) => {
    if (!isOwnerChat(msg)) return;
    if (!requireManager(msg.chat.id)) return;

    const name = match[1];
    if (!name) {
      const list = sessionManager.listSessions();
      const connectedCount = list.filter(s => s.connected).length;
      return telegramBot.sendMessage(msg.chat.id, `📊 ${connectedCount}/${list.length} Sessions verbunden.\nNutze /status <name> für Details oder /sessions für die volle Liste.`);
    }

    const sock = sessionManager.getSession(name);
    if (!sock) return telegramBot.sendMessage(msg.chat.id, `⚠️ Session "${name}" existiert nicht.`);
    const connected = !!sock.user;
    telegramBot.sendMessage(msg.chat.id, connected
      ? `✅ "${name}" verbunden als ${sock.user.id}`
      : `⚠️ "${name}" ist aktuell nicht verbunden.`);
  });

  // ---- Session trennen (Login-Daten bleiben erhalten) ----
  telegramBot.onText(/\/unpair\s+(\S+)/, async (msg, match) => {
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff.');
    if (!requireManager(msg.chat.id)) return;

    const name = match[1];
    const sock = sessionManager.getSession(name);
    if (!sock) return telegramBot.sendMessage(msg.chat.id, `⚠️ Session "${name}" existiert nicht.`);

    try {
      await sessionManager.stopSession(name);
      telegramBot.sendMessage(msg.chat.id, `✅ Session "${name}" getrennt. Login-Daten bleiben erhalten — mit /newsession ${name} neu verbinden.`);
    } catch (e) {
      telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Trennen von "${name}": ${e.message}`);
    }
  });

  // ---- Session komplett löschen (inkl. Login-Daten) ----
  telegramBot.onText(/\/deletesession\s+(\S+)/, async (msg, match) => {
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff.');
    if (!requireManager(msg.chat.id)) return;

    const name = match[1];
    if (name === 'default') {
      return telegramBot.sendMessage(msg.chat.id, '❌ Die Standard-Session kann nicht über Telegram gelöscht werden.');
    }

    try {
      await sessionManager.deleteSession(name);
      telegramBot.sendMessage(msg.chat.id, `🗑️ Session "${name}" wurde gestoppt und komplett gelöscht (inkl. Login-Daten).`);
    } catch (e) {
      telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Löschen von "${name}": ${e.message}`);
    }
  });

  console.log('✅ Telegram-Verbindungs-Bot gestartet (Multi-Session). Schreib /start an deinen Bot.');
  return telegramBot;
}

/**
 * Optional: Für Sonderfälle, in denen du außerhalb des normalen
 * startSession-Hooks einen QR-Code an Telegram schicken willst.
 */
export async function sendQrToTelegram(qrBuffer, caption = '📱 WhatsApp QR-Code zum Scannen') {
  if (!telegramBot || !OWNER_TELEGRAM_ID) return;
  try {
    await telegramBot.sendPhoto(OWNER_TELEGRAM_ID, qrBuffer, { caption });
  } catch (e) {
    console.error('Telegram QR send failed:', e.message);
  }
}
