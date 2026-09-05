// ============================================================================
// telegram-module.js
// ----------------------------------------------------------------------------
// Eigenständiges Telegram-Modul für den Sword-Art-Online-Bot.
//
// ZIEL
// Nutzer sollen den Bot auch über Telegram nutzen können, ohne dass ein
// komplett getrenntes zweites Bot-System entsteht. Coins, XP, Level, Rang
// etc. werden über dieselben Objekte (users, ranks, bans) verwaltet, die
// auch der WhatsApp-Teil benutzt – dieses Modul bekommt sie einfach als
// Referenz übergeben ("dependency injection"), verändert sie direkt und
// ruft danach ganz normal save(FILES.xxx, xxx) auf, genau wie der
// WhatsApp-Code das auch tut. Beide Plattformen lesen/schreiben also die
// gleichen JSON-Dateien in /data.
//
// KONTOVERKNÜPFUNG (WhatsApp <-> Telegram)
// Ein Telegram-Nutzer hat erstmal eine eigene, "native" Telegram-Identität:
//   jid = "tg<telegramUserId>@telegram"
// Hat er sich auf WhatsApp bereits per "?setpasswort <passwort>" eine
// ID + Passwort gesetzt (siehe web-auth.js / users[jid].webId), kann er
// sich in Telegram mit:
//   /login <ID> <Passwort>
// an genau diesen WhatsApp-Account anmelden. Ab dann zeigen/ändern alle
// Telegram-Befehle die Daten DIESES verknüpften Accounts (gleiche Coins,
// XP, Rang wie auf WhatsApp). /logout löst die Verknüpfung wieder (der
// Telegram-Nutzer fällt zurück auf seinen eigenen "tg...@telegram"-Account).
//
// INSTALLATION
//   npm install node-telegram-bot-api
//
// EINBINDUNG IN DEN HAUPT-BOT
// Am Ende deiner index.js/bot.js, NACHDEM users/ranks/bans/save/FILES/
// ensureUser/isAuthorized bereits geladen sind:
//
//   import { createTelegramModule } from './telegram-module.js';
//
//   const telegramModule = createTelegramModule({
//     users, ranks, bans, save, FILES, ensureUser, isAuthorized,
//     randInt, sleep, DATA_PATH
//   });
//   telegramModule.start();
//
// In deiner .env-Datei:
//   TELEGRAM_BOT_TOKEN=123456:ABC-dein-bot-token-von-BotFather
//
// Fehlt TELEGRAM_BOT_TOKEN, startet das Modul einfach nicht (kein Crash) –
// der WhatsApp-Teil läuft unabhängig davon normal weiter.
// ============================================================================

import TelegramBot from 'node-telegram-bot-api';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

// Muss exakt dieselben Parameter benutzen wie web-auth.js im Haupt-Bot,
// damit ein Passwort, das über WhatsApp (?setpasswort) gesetzt wurde, auch
// hier korrekt geprüft werden kann.
function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
}

const SHOP = {
  potion: { price: 100, desc: 'Heilt / gibt +10 XP' },
  box: { price: 500, desc: 'Zufälliger Coins-Betrag' },
  vip: { price: 2000, desc: '7 Tage VIP (halbierte Cooldowns)' }
};

const RARITY_LABEL = {
  common: '⚪ Gewöhnlich',
  uncommon: '🟢 Ungewöhnlich',
  rare: '🔵 Selten',
  epic: '🟣 Episch',
  legendary: '🟡 Legendär'
};

export function createTelegramModule(deps) {
  const {
    users,
    ranks,
    bans = {},
    save,
    FILES,
    ensureUser,
    isAuthorized,
    randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min,
    sleep = ms => new Promise(r => setTimeout(r, ms)),
    DATA_PATH = path.resolve('./data')
  } = deps;

  const token = process.env.TELEGRAM_BOT_TOKEN;

  if (!token) {
    console.log('⚠️  TELEGRAM_BOT_TOKEN nicht gesetzt — Telegram-Modul wird NICHT gestartet.');
    return { start: () => {}, stop: () => {}, bot: null };
  }
  if (!users || !ranks || !save || !FILES || !ensureUser || !isAuthorized) {
    console.error('[telegram] createTelegramModule: fehlende Abhängigkeiten (users/ranks/save/FILES/ensureUser/isAuthorized). Modul wird nicht gestartet.');
    return { start: () => {}, stop: () => {}, bot: null };
  }

  // ---- Verknüpfungs-Datei (Telegram-User-ID -> jid) ------------------------
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

  function saveLinks() {
    try {
      fs.mkdirSync(DATA_PATH, { recursive: true });
      fs.writeFileSync(LINKS_FILE, JSON.stringify(tgLinks, null, 2));
    } catch (e) {
      console.error('[telegram] Konnte telegram-links.json nicht speichern:', e?.message || e);
    }
  }

  let tgLinks = loadLinks(); // { "<telegramUserId>": "<jid>" }

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

  // ---- Telegram-Bot-Client --------------------------------------------------
  const bot = new TelegramBot(token, { polling: false });

  bot.on('polling_error', (err) => {
    console.error('[telegram] polling_error:', err?.message || err);
  });

  // Hilfsfunktion: löst aus einer Nachricht den jid des Absenders auf und
  // stellt sicher, dass ein Account existiert.
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

  // Hilfsfunktion: löst aus einer beantworteten Nachricht (reply) den jid
  // des ursprünglichen Absenders auf. Gibt null zurück, wenn kein Reply.
  function resolveReplyTarget(msg) {
    if (!msg.reply_to_message || !msg.reply_to_message.from) return null;
    const jid = jidForTelegramUser(msg.reply_to_message.from.id);
    ensureUser(jid);
    return jid;
  }

  function reply(chatId, text, opts = {}) {
    return bot.sendMessage(chatId, text, { parse_mode: 'Markdown', ...opts }).catch(e => {
      console.error('[telegram] sendMessage Fehler:', e?.message || e);
    });
  }

  // ==========================================================================
  // BEFEHLE
  // ==========================================================================

  bot.onText(/^\/start$/, (msg) => {
    const jid = resolveSender(msg);
    reply(msg.chat.id,
      `⚔️ *Willkommen bei Aincrad!*\n\n` +
      `Dein Account ist bereit. Nutze /help für alle Befehle.\n\n` +
      `Hast du schon einen WhatsApp-Account bei diesem Bot? Verknüpfe ihn mit:\n` +
      `\`/login <ID> <Passwort>\`\n` +
      `(Die ID bekommst du auf WhatsApp mit \`?setpasswort <passwort>\`)`
    );
  });

  bot.onText(/^\/help$/, (msg) => {
    reply(msg.chat.id,
      `⚔️ *— BEFEHLE —* ⚔️\n\n` +
      `👤 *Account*\n` +
      `/whoami — Profil anzeigen\n` +
      `/login <ID> <Passwort> — mit WhatsApp-Account verknüpfen\n` +
      `/logout — Verknüpfung aufheben\n\n` +
      `💰 *Wirtschaft*\n` +
      `/balance — Coins/Level/XP\n` +
      `/daily — täglicher Bonus\n` +
      `/work — Coins verdienen\n` +
      `/shop — Shop anzeigen\n` +
      `/buy <item> — kaufen\n` +
      `/inventory — Inventar\n` +
      `/use <item> — Item benutzen\n\n` +
      `🎲 *Spiele*\n` +
      `/slot <einsatz> — Spielautomat\n` +
      `/rps <stein|papier|schere>\n\n` +
      `🏆 *Sonstiges*\n` +
      `/rangliste [xp|level|coins]\n\n` +
      `🛡️ *Admin* (nur Owner/CoOwner/Admin)\n` +
      `Als Antwort auf eine Nachricht:\n` +
      `/addcash <betrag>\n/addxp <betrag>\n/ban [grund]\n/unban`
    );
  });

  // ---- Kontoverknüpfung -----------------------------------------------------

  bot.onText(/^\/login\s+(\S+)\s+(.+)$/, (msg, match) => {
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
    saveLinks();

    reply(chatId,
      `✅ Erfolgreich verknüpft!\n` +
      `Du nutzt jetzt den Account *${displayName(targetUser)}* ` +
      `(Level ${targetUser.level || 1}, ${targetUser.coins || 0} Coins) – ` +
      `identisch mit deinem WhatsApp-Account.`
    );
  });

  bot.onText(/^\/login$/, (msg) => {
    reply(msg.chat.id, '❌ Nutzung: `/login <ID> <Passwort>`\nDie ID setzt du auf WhatsApp mit `?setpasswort <passwort>`.');
  });

  bot.onText(/^\/logout$/, (msg) => {
    const tgId = String(msg.from.id);
    if (!tgLinks[tgId]) {
      return reply(msg.chat.id, 'ℹ️ Du bist mit keinem WhatsApp-Account verknüpft.');
    }
    delete tgLinks[tgId];
    saveLinks();
    reply(msg.chat.id, '✅ Verknüpfung aufgehoben. Du nutzt jetzt wieder deinen eigenständigen Telegram-Account.');
  });

  // ---- Profil / Wirtschaft ----------------------------------------------------

  bot.onText(/^\/whoami$/, (msg) => {
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

  bot.onText(/^\/balance$/, (msg) => {
    const jid = resolveSender(msg);
    const u = users[jid];
    reply(msg.chat.id, `💰 Coins: ${u.coins || 0}\n⭐ Level: ${u.level || 1}\nXP: ${u.xp || 0}`);
  });

  bot.onText(/^\/daily$/, (msg) => {
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

  bot.onText(/^\/work$/, (msg) => {
    const jid = resolveSender(msg);
    const u = users[jid];
    const earn = randInt(50, 200);
    u.coins = (u.coins || 0) + earn;
    u.xp = (u.xp || 0) + 20;
    saveUsers();
    reply(msg.chat.id, `🛠 Du hast ${earn} Coins verdient!`);
  });

  bot.onText(/^\/shop$/, (msg) => {
    let out = '🛒 *Shop*\n\n';
    for (const [k, v] of Object.entries(SHOP)) {
      out += `• \`${k}\` — ${v.price} 💰 | ${v.desc}\n`;
    }
    out += '\nKaufen mit: `/buy <item>`';
    reply(msg.chat.id, out);
  });

  bot.onText(/^\/buy\s+(\S+)$/, (msg, match) => {
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

  bot.onText(/^\/inventory$/, (msg) => {
    const jid = resolveSender(msg);
    const inv = users[jid].items || {};
    const out = Object.keys(inv).length
      ? Object.entries(inv).map(([k, v]) => `${k}: ${v}`).join('\n')
      : '(leer)';
    reply(msg.chat.id, `🎒 *Inventar*\n${out}`);
  });

  bot.onText(/^\/use\s+(\S+)$/, (msg, match) => {
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

  bot.onText(/^\/slot(?:\s+(\d+))?$/, (msg, match) => {
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

  bot.onText(/^\/rps\s+(stein|papier|schere|rock|paper|scissors)$/i, (msg, match) => {
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

  bot.onText(/^\/rangliste(?:\s+(xp|level|coins))?$/, (msg, match) => {
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

  bot.onText(/^\/addcash\s+(\d+)$/, (msg, match) => {
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

  bot.onText(/^\/addxp\s+(\d+)$/, (msg, match) => {
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

  bot.onText(/^\/ban(?:\s+(.+))?$/, (msg, match) => {
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

  bot.onText(/^\/unban$/, (msg) => {
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

  bot.on('message', (msg) => {
    if (!msg.text || !msg.text.startsWith('/')) return;
    const jid = jidForTelegramUser(msg.from.id);
    if (bans[jid]) {
      reply(msg.chat.id, '🚫 Du bist gebannt.');
      return;
    }
  });

  // ==========================================================================
  // Start / Stop
  // ==========================================================================

  function start() {
    bot.startPolling();
    console.log('✅ Telegram-Modul gestartet (Polling aktiv).');
  }

  function stop() {
    bot.stopPolling();
    console.log('🛑 Telegram-Modul gestoppt.');
  }

  return { start, stop, bot };
}
