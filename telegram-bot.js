import TelegramBot from 'node-telegram-bot-api';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

// ============================================================================
// EIN Bot, EIN Token für ALLES (Session-Manager + Aincrad-Game).
// Grund: Telegram erlaubt nur eine aktive Polling-Verbindung pro Token —
// zwei separate Bot-Instanzen mit demselben Token haben sich bisher
// gegenseitig mit "409 Conflict" rausgeworfen.
// ============================================================================
const TELEGRAM_BOT_TOKEN = '8614468465:AAHP7693iiKX56Sp-9TRNa3q2gGMBXOQ-ms';

// Nur dieser Telegram-Account darf /deletesession benutzen.
const OWNER_TELEGRAM_ID = 8598584607;

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
}

const SHOP = {
  potion: { price: 100, desc: 'Heilt / gibt +10 XP' },
  box: { price: 500, desc: 'Zufälliger Coins-Betrag' },
  vip: { price: 2000, desc: '7 Tage VIP (halbierte Cooldowns)' }
};

let telegramBot = null;
let sessionManager = null;
let activeSock = null;

function isOwnerChat(msg) {
  return OWNER_TELEGRAM_ID && msg.from && msg.from.id === OWNER_TELEGRAM_ID;
}

function requireManager(chatId) {
  if (!sessionManager) {
    telegramBot.sendMessage(chatId, '⚠️ Session-Manager ist nicht angebunden. Prüfe die initTelegramBot(...)-Aufruf in index.js.');
    return false;
  }
  return true;
}

/**
 * Startet den EINEN Telegram-Bot mit allen Befehlen.
 *
 * @param {object} manager - der WhatsApp-Session-Manager (newsession/pair/sessions/status/unpair/deletesession)
 * @param {object} aincradDeps - Abhängigkeiten für die Aincrad-Game-Befehle (whoami/balance/shop/...):
 *   { users, ranks, bans, save, FILES, ensureUser, isAuthorized, randInt?, DATA_PATH? }
 */
export function initTelegramBot(manager, aincradDeps = {}) {
  sessionManager = manager || null;

  if (!TELEGRAM_BOT_TOKEN) {
    console.log('ℹ️ TELEGRAM_BOT_TOKEN nicht gesetzt — Telegram-Bot deaktiviert.');
    return null;
  }

  const {
    users,
    ranks,
    bans = {},
    save,
    FILES,
    ensureUser,
    isAuthorized,
    randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min,
    DATA_PATH = path.resolve('./data')
  } = aincradDeps;

  const aincradReady = !!(users && ranks && save && FILES && ensureUser && isAuthorized);
  if (!aincradReady) {
    console.log('ℹ️ Aincrad-Abhängigkeiten unvollständig — nur Session-Manager-Befehle sind aktiv.');
  }

  telegramBot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
  telegramBot.on('polling_error', (e) => console.error('[telegram polling_error]', e.message));

  // ==========================================================================
  // GEMEINSAMER /start & /help
  // ==========================================================================
  telegramBot.onText(/^\/start$/, (msg) => {
    let text = '🤖 *WhatsApp-Session-Verwaltung*\n\n' +
      '/newsession <n> - neue Session starten (QR-Code kommt automatisch)\n' +
      '/pair <n> <nummer> - Pairing-Code statt QR anfordern\n' +
      '/sessions - alle aktiven Sessions auflisten\n' +
      '/status <n> - Status einer bestimmten Session\n' +
      '/unpair <n> - Session trennen (Login-Daten bleiben)\n' +
      '/deletesession <n> - Session stoppen UND Login-Daten löschen (nur Owner)\n';
    if (aincradReady) {
      text += '\n⚔️ *Willkommen bei Aincrad!*\n' +
        'Nutze /help für die Aincrad-Game-Befehle.\n' +
        'Verknüpfe deinen WhatsApp-Account mit `/login <ID> <Passwort>`.';
    }
    telegramBot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
  });

  telegramBot.onText(/^\/help$/, (msg) => {
    let text = '🤖 *Session-Verwaltung*\n\n' +
      '/newsession <n> - neue Session starten\n' +
      '/pair <n> <nummer> - Pairing-Code anfordern\n' +
      '/sessions - alle aktiven Sessions auflisten\n' +
      '/status <n> - Status einer Session\n' +
      '/unpair <n> - Session trennen\n' +
      '/deletesession <n> - Session löschen (nur Owner)\n';
    if (aincradReady) {
      text += '\n⚔️ *— AINCRAD-BEFEHLE —* ⚔️\n\n' +
        '👤 *Account*\n' +
        '/whoami — Profil anzeigen\n' +
        '/login <ID> <Passwort> — mit WhatsApp-Account verknüpfen\n' +
        '/logout — Verknüpfung aufheben\n\n' +
        '💰 *Wirtschaft*\n' +
        '/balance — Coins/Level/XP\n' +
        '/daily — täglicher Bonus\n' +
        '/work — Coins verdienen\n' +
        '/shop — Shop anzeigen\n' +
        '/buy <item> — kaufen\n' +
        '/inventory — Inventar\n' +
        '/use <item> — Item benutzen\n\n' +
        '🎲 *Spiele*\n' +
        '/slot <einsatz> — Spielautomat\n' +
        '/rps <stein|papier|schere>\n\n' +
        '🏆 *Sonstiges*\n' +
        '/rangliste [xp|level|coins]\n\n' +
        '🛡️ *Admin* (nur Owner/CoOwner/Admin, als Antwort auf eine Nachricht)\n' +
        '/addcash <betrag>\n/addxp <betrag>\n/ban [grund]\n/unban';
    }
    telegramBot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
  });

  // ==========================================================================
  // SESSION-MANAGER-BEFEHLE
  // ==========================================================================

  telegramBot.onText(/\/newsession\s+(\S+)/, async (msg, match) => {
    // Owner darf auch dann durch, wenn der Session-Manager (noch) nicht angebunden ist.
    if (!isOwnerChat(msg) && !requireManager(msg.chat.id)) return;

    const name = match[1];

    try {
      if (sessionManager && sessionManager.getSession(name)) {
        return telegramBot.sendMessage(msg.chat.id, `⚠️ Session "${name}" läuft bereits. Nutze /status ${name}.`);
      }

      telegramBot.sendMessage(msg.chat.id, `⏳ Starte Session "${name}"...`);

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
      return telegramBot.sendMessage(msg.chat.id, 'ℹ️ Keine aktiven Sessions. Starte eine mit /newsession <n>.');
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
      return telegramBot.sendMessage(msg.chat.id, `📊 ${connectedCount}/${list.length} Sessions verbunden.\nNutze /status <n> für Details oder /sessions für die volle Liste.`);
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
    if (!isOwnerChat(msg)) return telegramBot.sendMessage(msg.chat.id, '❌ Kein Zugriff. Nur der Owner darf Sessions löschen.');
    // Kein requireManager-Check: der Owner darf es versuchen, ein Fehler landet im catch unten.

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

  // ==========================================================================
  // AINCRAD-GAME-BEFEHLE (nur aktiv, wenn aincradDeps vollständig übergeben wurden)
  // ==========================================================================
  if (aincradReady) {
    const LINKS_FILE = path.join(DATA_PATH, 'telegram-links.json');

    function loadLinks() {
      try {
        if (!fs.existsSync(LINKS_FILE)) return {};
        const raw = fs.readFileSync(LINKS_FILE, 'utf8');
        return raw.trim() ? JSON.parse(raw) : {};
      } catch (e) {
        console.error('[telegram] Konnte telegram-links.json nicht laden:', e?.message || e);
        return {};
      }
    }

    function saveLinksFile() {
      try {
        fs.mkdirSync(DATA_PATH, { recursive: true });
        fs.writeFileSync(LINKS_FILE, JSON.stringify(tgLinks, null, 2));
      } catch (e) {
        console.error('[telegram] Konnte telegram-links.json nicht speichern:', e?.message || e);
      }
    }

    let tgLinks = loadLinks();

    function jidForTelegramUser(tgUserId) {
      const linked = tgLinks[String(tgUserId)];
      if (linked) return linked;
      return `tg${tgUserId}@telegram`;
    }

    function isNativeTelegramJid(jid) {
      return typeof jid === 'string' && jid.endsWith('@telegram');
    }

    function displayName(user) {
      return user.name || user.registrationName || 'Unbekannt';
    }

    function saveUsers() { save(FILES.users, users); }
    function saveRanks() { save(FILES.ranks, ranks); }
    function saveBansIfPresent() {
      if (FILES.bans) save(FILES.bans, bans);
    }

    function resolveSender(msg) {
      const jid = jidForTelegramUser(msg.from.id);
      ensureUser(jid);
      if (!users[jid].name && isNativeTelegramJid(jid)) {
        users[jid].name = msg.from.username
          ? `@${msg.from.username}`
          : (msg.from.first_name || 'Telegram-Nutzer');
        saveUsers();
      }
      return jid;
    }

    function resolveReplyTarget(msg) {
      if (!msg.reply_to_message || !msg.reply_to_message.from) return null;
      const jid = jidForTelegramUser(msg.reply_to_message.from.id);
      ensureUser(jid);
      return jid;
    }

    function reply(chatId, text, opts = {}) {
      return telegramBot.sendMessage(chatId, text, { parse_mode: 'Markdown', ...opts }).catch(e => {
        console.error('[telegram] sendMessage Fehler:', e?.message || e);
      });
    }

    // ---- Kontoverknüpfung -----------------------------------------------------

    telegramBot.onText(/^\/login\s+(\S+)\s+(.+)$/, (msg, match) => {
      const chatId = msg.chat.id;
      const wantedId = match[1].toUpperCase();
      const password = match[2];

      const entry = Object.entries(users).find(([, u]) => u.webId === wantedId);
      if (!entry) {
        return reply(chatId, '❌ Diese ID wurde nicht gefunden. Prüfe sie mit `?myid` auf WhatsApp.');
      }
      const [targetJid, targetUser] = entry;
      if (!targetUser.webPasswordHash || !targetUser.webPasswordSalt) {
        return reply(chatId, '❌ Für diese ID wurde noch kein Passwort gesetzt (nutze `?setpasswort <passwort>` auf WhatsApp).');
      }
      const hash = hashPassword(password, targetUser.webPasswordSalt);
      if (hash !== targetUser.webPasswordHash) {
        return reply(chatId, '❌ Falsches Passwort.');
      }

      tgLinks[String(msg.from.id)] = targetJid;
      saveLinksFile();

      reply(chatId,
        `✅ Erfolgreich verknüpft!\n` +
        `Du nutzt jetzt den Account *${displayName(targetUser)}* ` +
        `(Level ${targetUser.level || 1}, ${targetUser.coins || 0} Coins) – ` +
        `identisch mit deinem WhatsApp-Account.`
      );
    });

    telegramBot.onText(/^\/login$/, (msg) => {
      reply(msg.chat.id, '❌ Nutzung: `/login <ID> <Passwort>`\nDie ID setzt du auf WhatsApp mit `?setpasswort <passwort>`.');
    });

    telegramBot.onText(/^\/logout$/, (msg) => {
      const tgId = String(msg.from.id);
      if (!tgLinks[tgId]) {
        return reply(msg.chat.id, 'ℹ️ Du bist mit keinem WhatsApp-Account verknüpft.');
      }
      delete tgLinks[tgId];
      saveLinksFile();
      reply(msg.chat.id, '✅ Verknüpfung aufgehoben. Du nutzt jetzt wieder deinen eigenständigen Telegram-Account.');
    });

    // ---- Profil / Wirtschaft ----------------------------------------------------

    telegramBot.onText(/^\/whoami$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const rank = ranks[jid] || u.rank || 'USER';
      const linkedNote = isNativeTelegramJid(jid) ? '' : '\n🔗 _(mit WhatsApp verknüpft)_';
      reply(msg.chat.id,
        `👤 *${displayName(u)}*\n` +
        `🏅 Rang: ${rank}\n` +
        `⭐ Level: ${u.level || 1}\n` +
        `✨ XP: ${u.xp || 0}\n` +
        `💰 Coins: ${u.coins || 0}${linkedNote}`
      );
    });

    telegramBot.onText(/^\/balance$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      reply(msg.chat.id, `💰 Coins: ${u.coins || 0}\n⭐ Level: ${u.level || 1}\nXP: ${u.xp || 0}`);
    });

    telegramBot.onText(/^\/daily$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const now = Date.now();
      const last = u.lastDaily || 0;
      if (now - last < 24 * 3600 * 1000) {
        const hours = Math.floor((24 * 3600 * 1000 - (now - last)) / 3600000);
        return reply(msg.chat.id, `🕒 Wieder in ca. ${hours} Stunden verfügbar.`);
      }
      const amount = randInt(1, 1000);
      u.coins = (u.coins || 0) + amount;
      u.lastDaily = now;
      saveUsers();
      reply(msg.chat.id, `🎁 Daily: +${amount} Coins!`);
    });

    telegramBot.onText(/^\/work$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const earn = randInt(50, 200);
      u.coins = (u.coins || 0) + earn;
      u.xp = (u.xp || 0) + 20;
      saveUsers();
      reply(msg.chat.id, `🛠 Du hast ${earn} Coins verdient!`);
    });

    telegramBot.onText(/^\/shop$/, (msg) => {
      let out = '🛒 *Shop*\n\n';
      for (const [k, v] of Object.entries(SHOP)) {
        out += `• \`${k}\` — ${v.price} 💰 | ${v.desc}\n`;
      }
      out += '\nKaufen mit: `/buy <item>`';
      reply(msg.chat.id, out);
    });

    telegramBot.onText(/^\/buy\s+(\S+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const item = match[1].toLowerCase();
      if (!SHOP[item]) return reply(msg.chat.id, '❌ Unbekanntes Item. Siehe `/shop`.');
      if ((u.coins || 0) < SHOP[item].price) return reply(msg.chat.id, '💸 Zu wenig Coins.');
      u.coins -= SHOP[item].price;
      if (!u.items) u.items = {};
      u.items[item] = (u.items[item] || 0) + 1;
      saveUsers();
      reply(msg.chat.id, `✅ ${item} gekauft.`);
    });

    telegramBot.onText(/^\/inventory$/, (msg) => {
      const jid = resolveSender(msg);
      const inv = users[jid].items || {};
      const out = Object.keys(inv).length
        ? Object.entries(inv).map(([k, v]) => `${k}: ${v}`).join('\n')
        : '(leer)';
      reply(msg.chat.id, `🎒 *Inventar*\n${out}`);
    });

    telegramBot.onText(/^\/use\s+(\S+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const item = match[1].toLowerCase();
      if (!u.items || !u.items[item]) return reply(msg.chat.id, '❌ Item nicht vorhanden.');

      if (item === 'potion') {
        u.items[item] -= 1;
        u.xp = (u.xp || 0) + 10;
        saveUsers();
        return reply(msg.chat.id, '💊 Trank verwendet: +10 XP');
      }
      if (item === 'box') {
        u.items[item] -= 1;
        const coins = randInt(50, 300);
        u.coins = (u.coins || 0) + coins;
        saveUsers();
        return reply(msg.chat.id, `🎁 Box geöffnet: +${coins} Coins`);
      }
      if (item === 'vip') {
        u.items[item] -= 1;
        u.vipUntil = Date.now() + 7 * 24 * 3600 * 1000;
        const prevRank = ranks[jid] || u.rank || 'USER';
        if (prevRank === 'USER') { ranks[jid] = 'VIP'; u.rank = 'VIP'; saveRanks(); }
        saveUsers();
        return reply(msg.chat.id, '💎 VIP aktiviert! Gültig für 7 Tage.');
      }
      reply(msg.chat.id, 'Item verwendet.');
    });

    // ---- Spiele -----------------------------------------------------------------

    telegramBot.onText(/^\/slot(?:\s+(\d+))?$/, (msg, match) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const bet = match[1] ? parseInt(match[1]) : 50;
      if ((u.coins || 0) < bet) return reply(msg.chat.id, 'Zu wenig Coins.');
      const symbols = ['🍒', '🍋', '🍇', '🍉', '⭐', '💎'];
      const spin = [0, 0, 0].map(() => symbols[randInt(0, symbols.length - 1)]);
      const win = spin[0] === spin[1] && spin[1] === spin[2];
      if (win) {
        u.coins += bet * 3;
        u.xp = (u.xp || 0) + 50;
        saveUsers();
        reply(msg.chat.id, `🎰 | ${spin.join(' | ')} |\n🎉 Jackpot! +${bet * 3} Coins, +50 XP`);
      } else {
        u.coins -= bet;
        saveUsers();
        reply(msg.chat.id, `🎰 | ${spin.join(' | ')} |\n😢 Verloren -${bet} Coins`);
      }
    });

    telegramBot.onText(/^\/rps\s+(stein|papier|schere|rock|paper|scissors)$/i, (msg, match) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const map = { stein: 'rock', papier: 'paper', schere: 'scissors' };
      const norm = map[match[1].toLowerCase()] || match[1].toLowerCase();
      const opts = ['rock', 'paper', 'scissors'];
      const botOpt = opts[randInt(0, 2)];
      const draw = norm === botOpt;
      const win = (norm === 'rock' && botOpt === 'scissors') ||
                  (norm === 'paper' && botOpt === 'rock') ||
                  (norm === 'scissors' && botOpt === 'paper');
      let res = `🤖 Ich: ${botOpt}\nDu: ${norm}\n`;
      if (draw) {
        res += 'Unentschieden 😐';
      } else if (win) {
        u.coins = (u.coins || 0) + 50;
        u.xp = (u.xp || 0) + 10;
        saveUsers();
        res += 'Du gewinnst! +50 Coins +10 XP 🎉';
      } else {
        u.coins = Math.max(0, (u.coins || 0) - 20);
        saveUsers();
        res += 'Du verlierst -20 Coins 😢';
      }
      reply(msg.chat.id, res);
    });

    // ---- Rangliste ----------------------------------------------------------------

    telegramBot.onText(/^\/rangliste(?:\s+(xp|level|coins))?$/, (msg, match) => {
      const sortBy = match[1] || 'xp';
      const entries = Object.entries(users).filter(([, u]) => u && typeof u === 'object');
      const sorted = sortBy === 'coins'
        ? entries.sort((a, b) => (b[1].coins || 0) - (a[1].coins || 0))
        : entries.sort((a, b) => ((b[1].level || 1) * 1000 + (b[1].xp || 0)) - ((a[1].level || 1) * 1000 + (a[1].xp || 0)));

      const top = sorted.slice(0, 10);
      if (!top.length) return reply(msg.chat.id, '📊 Noch keine Spieler vorhanden.');

      const medals = ['🥇', '🥈', '🥉'];
      const lines = top.map(([, u], i) => {
        const icon = medals[i] || `${i + 1}.`;
        const name = displayName(u) !== 'Unbekannt' ? displayName(u) : '(anonym)';
        return sortBy === 'coins'
          ? `${icon} ${name} — 💰 ${u.coins || 0} Coins`
          : `${icon} ${name} — ⭐ Lv.${u.level || 1} (${u.xp || 0} XP)`;
      });
      reply(msg.chat.id, `🏆 *Rangliste (${sortBy})*\n\n${lines.join('\n')}`);
    });

    // ---- Admin-Befehle (nur Owner/CoOwner/Admin, per Reply auf eine Nachricht) --

    telegramBot.onText(/^\/addcash\s+(\d+)$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN'])) {
        return reply(msg.chat.id, '❌ Kein Zugriff.');
      }
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const amount = parseInt(match[1]);
      users[targetJid].coins = (users[targetJid].coins || 0) + amount;
      saveUsers();
      reply(msg.chat.id, `✅ ${amount} Coins vergeben an ${displayName(users[targetJid])}.`);
    });

    telegramBot.onText(/^\/addxp\s+(\d+)$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN'])) {
        return reply(msg.chat.id, '❌ Kein Zugriff.');
      }
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const amount = parseInt(match[1]);
      users[targetJid].xp = (users[targetJid].xp || 0) + amount;
      saveUsers();
      reply(msg.chat.id, `✅ ${amount} XP vergeben an ${displayName(users[targetJid])}.`);
    });

    telegramBot.onText(/^\/ban(?:\s+(.+))?$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) {
        return reply(msg.chat.id, '❌ Kein Zugriff.');
      }
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const reason = match[1] || 'Kein Grund';
      bans[targetJid] = { by: senderJid, at: new Date().toISOString(), reason };
      saveBansIfPresent();
      reply(msg.chat.id, `🚫 ${displayName(users[targetJid])} gebannt. Grund: ${reason}`);
    });

    telegramBot.onText(/^\/unban$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) {
        return reply(msg.chat.id, '❌ Kein Zugriff.');
      }
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      delete bans[targetJid];
      saveBansIfPresent();
      reply(msg.chat.id, `✅ ${displayName(users[targetJid])} entbannt.`);
    });

    // ---- Ban-Sperre: gebannte Nutzer werden ignoriert ----------------------------

    telegramBot.on('message', (msg) => {
      if (!msg.text || !msg.text.startsWith('/')) return;
      const jid = jidForTelegramUser(msg.from.id);
      if (bans[jid]) {
        reply(msg.chat.id, '🚫 Du bist gebannt.');
      }
    });
  }

  console.log(`✅ Telegram-Bot gestartet (EIN Token, EINE Polling-Verbindung). Session-Manager: ${sessionManager ? 'ja' : 'nein'}, Aincrad-Game: ${aincradReady ? 'ja' : 'nein'}.`);
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
  console.warn('[telegram] sendQrToTelegram ohne festen Owner: es gibt keinen Ziel-Chat mehr.');
}
