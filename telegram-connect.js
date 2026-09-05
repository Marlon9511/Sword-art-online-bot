import TelegramBot from 'node-telegram-bot-api';

// Token fest eingebaut (kein .env nötig)
const TELEGRAM_BOT_TOKEN = '8614468465:AAHP7693iiKX56Sp-9TRNa3q2gGMBXOQ-ms';

let telegramBot = null;
let sessionManager = null;
let activeSock = null;

function requireManager(chatId) {
  if (!sessionManager) {
    telegramBot.sendMessage(chatId, '⚠️ Session-Manager ist nicht angebunden. Prüfe die initTelegramConnect(...)-Aufruf in index.js.');
    return false;
  }
  return true;
}

export function initTelegramConnect(manager) {
  sessionManager = manager || null;

  if (!TELEGRAM_BOT_TOKEN) {
    console.log('ℹ️ TELEGRAM_BOT_TOKEN nicht gesetzt — Telegram-Verbindung deaktiviert.');
    return null;
  }

  telegramBot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
  telegramBot.on('polling_error', (e) => console.error('[telegram polling_error]', e.message));

  telegramBot.onText(/\/start|\/help/, (msg) => {
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

  telegramBot.onText(/\/newsession\s+(\S+)/, async (msg, match) => {
    if (!requireManager(msg.chat.id)) return;

    const name = match[1];
    if (sessionManager.getSession(name)) {
      return telegramBot.sendMessage(msg.chat.id, `⚠️ Session "${name}" läuft bereits. Nutze /status ${name}.`);
    }

    telegramBot.sendMessage(msg.chat.id, `⏳ Starte Session "${name}"...`);

    try {
      const sock = await sessionManager.startSession(name, {
        onQr: async (qrBuffer) => {
          try {
            await telegramBot.sendPhoto(msg.chat.id, qrBuffer, {
              caption: `📱 QR-Code für Session "${name}"\nScanne mit WhatsApp: Verknüpfte Geräte → Gerät verknüpfen`
            });
          } catch (e) {
            console.error('[telegram] QR senden fehlgeschlagen:', e.message);
          }
        },
        onOpen: async (jid) => {
          try {
            await telegramBot.sendMessage(msg.chat.id, `✅ Session "${name}" verbunden!\nJID: ${jid || '(unbekannt)'}`);
          } catch (e) {}
        }
      });
      setActiveSock(sock);
    } catch (e) {
      console.error('[telegram] Session-Start fehlgeschlagen:', e);
      telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Starten von "${name}": ${e.message}`);
    }
  });

  telegramBot.onText(/\/pair\s+(\S+)\s+(\d+)/, async (msg, match) => {
    if (!requireManager(msg.chat.id)) return;

    const [, name, number] = match;
    let sock = sessionManager.getSession(name);
    let justCreated = false;

    if (!sock) {
      telegramBot.sendMessage(msg.chat.id, `⏳ Session "${name}" existiert noch nicht — erstelle sie...`);
      try {
        sock = await sessionManager.startSession(name, {
          onOpen: async (jid) => {
            try { await telegramBot.sendMessage(msg.chat.id, `✅ Session "${name}" verbunden!\nJID: ${jid || '(unbekannt)'}`); } catch (e) {}
          }
        });
        setActiveSock(sock);
        justCreated = true;
      } catch (e) {
        return telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Erstellen von "${name}": ${e.message}`);
      }
    }

    if (sock.authState?.creds?.registered) {
      return telegramBot.sendMessage(msg.chat.id, `✅ Session "${name}" ist bereits verbunden.`);
    }

    if (justCreated) {
      await new Promise(resolve => setTimeout(resolve, 3000));
    }

    const maxAttempts = 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        let code = await sock.requestPairingCode(number);
        code = code?.match(/.{1,4}/g)?.join('-') || code;
        return telegramBot.sendMessage(msg.chat.id,
          `🔑 Pairing-Code für "${name}": *${code}*\n\n` +
          'In WhatsApp: Einstellungen → Verknüpfte Geräte → Gerät verknüpfen → "Stattdessen mit Telefonnummer verknüpfen" → Code eingeben.',
          { parse_mode: 'Markdown' });
      } catch (e) {
        const isConnectionIssue = /connection closed/i.test(e.message || '');
        if (isConnectionIssue && attempt < maxAttempts) {
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }
        return telegramBot.sendMessage(msg.chat.id, `❌ Fehler beim Generieren des Codes für "${name}": ${e.message}`);
      }
    }
  });

  telegramBot.onText(/\/sessions/, (msg) => {
    if (!requireManager(msg.chat.id)) return;

    const list = sessionManager.listSessions();
    if (!list.length) {
      return telegramBot.sendMessage(msg.chat.id, 'ℹ️ Keine aktiven Sessions. Starte eine mit /newsession <name>.');
    }
    const text = list.map(s => `${s.connected ? '✅' : '⏳'} *${s.name}* — ${s.jid || '(verbindet...)'}`).join('\n');
    telegramBot.sendMessage(msg.chat.id, `📋 *Aktive Sessions* (${list.length}):\n\n${text}`, { parse_mode: 'Markdown' });
  });

  telegramBot.onText(/\/status(?:\s+(\S+))?/, (msg, match) => {
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

  telegramBot.onText(/\/unpair\s+(\S+)/, async (msg, match) => {
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

  telegramBot.onText(/\/deletesession\s+(\S+)/, async (msg, match) => {
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

  console.log('✅ Telegram-Verbindungs-Bot gestartet (Multi-Session, für alle Nutzer offen). Schreib /start an deinen Bot.');
  return telegramBot;
}

export function setActiveSock(sock) {
  activeSock = sock;
}

export function getActiveSock() {
  return activeSock;
}

export async function sendQrToTelegram(qrBuffer, caption = '📱 WhatsApp QR-Code zum Scannen') {
  if (!telegramBot) return;
  console.warn('[telegram] sendQrToTelegram ohne festen Owner: es gibt keinen Ziel-Chat mehr, da OWNER_TELEGRAM_ID entfernt wurde.');
}
