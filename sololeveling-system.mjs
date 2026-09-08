import TelegramBot from 'node-telegram-bot-api';
import fs from 'fs';
import path from 'path';

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

// ── Angel-Events (1:1 aus dem WhatsApp-Bot übernommen) ──────────────────────
const FISH_EVENTS = [
  { chance: 20, rarity: 'common', text: '🐟 *Flusswels* aus dem Fluss der Stadt der Anfänge gefangen! (+8 Coins)', coins: 8 },
  { chance: 16, rarity: 'common', text: '🐠 *Blauschuppen-Barsch* gefangen! (+15 Coins)', coins: 15, xp: 3 },
  { chance: 12, rarity: 'uncommon', text: '🐡 *Kugelfisch der ersten Ebene* gefangen! (+25 Coins)', coins: 25, xp: 5 },
  { chance: 10, rarity: 'uncommon', text: '💎 *Kristallforelle* aus dem Kristallwald-See schimmert in der Sonne! (+30 Coins)', coins: 30, xp: 8 },
  { chance: 7, rarity: 'rare', text: '🌫️ *Nebelaal* (Floor 35) aus dem Sumpf gezogen! (+45 Coins)', coins: 45, xp: 10 },
  { chance: 8, rarity: 'common', text: '🦀 *Kobold-Krebs* hat sich in der Angel verheddert. (+5 Coins)', coins: 5 },
  { chance: 5, rarity: 'rare', text: '🐍 *Riesenaal* zerrt dich fast von der Plattform, gibt aber auf! (+40 Coins)', coins: 40, xp: 8 },
  { chance: 4, rarity: 'epic', text: '🦈 *Sturmhai* (Floor 50) durchbricht die Wasseroberfläche! (+90 Coins)', coins: 90, xp: 20 },
  { chance: 3, rarity: 'epic', text: '🎐 *Leuchtqualle der Tiefen* (Floor 75) taucht schimmernd auf! (+70 Coins)', coins: 70, xp: 15 },
  { chance: 1.5, rarity: 'legendary', text: '🐉 *Drachenkarpfen* (Floor 90) — ein legendärer Feldboss-Fisch! (+180 Coins)', coins: 180, xp: 35 },
  { chance: 0.4, rarity: 'legendary', text: '🐋 *Der Weiße Wal von Aincrad* — eine Systemlegende wird wahr! JACKPOT! (+600 Coins)', coins: 600, xp: 120 },
  { chance: 3, rarity: 'rare', text: '📦 *Schatztruhe* am Grund des Sees entdeckt! (+200 Coins)', coins: 200, xp: 10 },
  { chance: 2, rarity: 'uncommon', text: '📜 *Flaschenpost eines gefallenen Spielers* gefunden. (+50 Coins)', coins: 50, xp: 20 },
  { chance: 3, rarity: 'uncommon', text: '💊 Eine *Heiltrank-Flasche* trieb vorbei und wurde eingesammelt.', item: 'potion', itemQty: 1 },
  { chance: 3, rarity: 'uncommon', text: '🎁 Eine *mysteriöse Kiste* hing im Schilf fest.', item: 'box', itemQty: 1 },
  { chance: 10, rarity: 'common', text: '🌿 Nur Algen gefangen... Aincrad ist manchmal enttäuschend.' },
  { chance: 8, rarity: 'common', text: '💨 Ein Fisch hat deinen Köder gestohlen und ist geflüchtet!' },
  { chance: 4, rarity: 'common', text: '💦 Du bist ausgerutscht und ins eiskalte Wasser gefallen!' },
  { chance: 3, rarity: 'common', text: '🦶 Ein *Kobold* hat dein Boot umgestoßen! (-30 Coins)', coins: -30 },
  { chance: 2, rarity: 'common', text: '🪝 Deine Angel ist an einem Stein zerbrochen! (-10 Coins)', coins: -10 }
];

// ── Blackjack-Helfer (1:1 aus dem WhatsApp-Bot übernommen) ──────────────────
const BJ_SUITS = ['♠', '♥', '♦', '♣'];
const BJ_VALUES = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
function bjDraw() {
  return {
    value: BJ_VALUES[Math.floor(Math.random() * BJ_VALUES.length)],
    suit: BJ_SUITS[Math.floor(Math.random() * BJ_SUITS.length)]
  };
}
function bjVal(card) {
  if (['J', 'Q', 'K'].includes(card.value)) return 10;
  if (card.value === 'A') return 11;
  return parseInt(card.value);
}
function bjScore(hand) {
  let s = 0, ac = 0;
  for (const c of hand) {
    if (c.value === 'A') { ac++; s += 11; } else s += bjVal(c);
  }
  while (s > 21 && ac > 0) { s -= 10; ac--; }
  return s;
}

function parseDuration(str) {
  const match = String(str || '').match(/^(\d+)([dhm])$/);
  if (!match) return null;
  const [, amount, unit] = match;
  const num = parseInt(amount);
  switch (unit) {
    case 'd': return num * 24 * 60 * 60 * 1000;
    case 'h': return num * 60 * 60 * 1000;
    case 'm': return num * 60 * 1000;
    default: return null;
  }
}

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
 * @param {object} aincradDeps - Abhängigkeiten für die Aincrad-Game-Befehle:
 *   {
 *     users, ranks, bans, save, FILES, ensureUser, isAuthorized, randInt?, DATA_PATH?,
 *     pets?, tickets?, teamTodos?, userTodos?, credits?, partners?, marriages?, commandBans?
 *   }
 *   Die zusätzlichen Objekte (pets, tickets, teamTodos, ...) sind optional — wird
 *   keins übergeben, bleiben die zugehörigen Befehle einfach inaktiv/lokal-leer.
 *   Wichtig: wenn dieselben Objekt-Referenzen wie in index.js übergeben werden
 *   (nicht Kopien!), teilen sich WhatsApp und Telegram automatisch dieselben
 *   Daten (z.B. ein Team-Todo, das auf WhatsApp erstellt wurde, ist auch über
 *   /todo list auf Telegram sichtbar, und umgekehrt).
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
    pets = {},
    tickets = {},
    teamTodos = {},
    userTodos = {},
    credits = { list: [] },
    partners = { list: [] },
    marriages = {},
    commandBans = {},
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
        '/profile — Kurzprofil\n' +
        '/userinfo — als Reply: Profil einer anderen Person\n' +
        '/setinfo <feld> <wert> — name/alter/hobbys/sexualitaet setzen\n' +
        '/login <ID> <Passwort> — mit WhatsApp-Account verknüpfen\n' +
        '/logout — Verknüpfung aufheben\n' +
        '/afk [grund] — Abwesenheit setzen (wird bei nächster Nachricht aufgehoben)\n\n' +
        '💰 *Wirtschaft*\n' +
        '/balance — Coins/Level/XP\n' +
        '/daily — täglicher Bonus\n' +
        '/work — Coins verdienen\n' +
        '/fish — angeln gehen\n' +
        '/shop — Shop anzeigen\n' +
        '/buy <item> — kaufen\n' +
        '/inventory — Inventar\n' +
        '/use <item> — Item benutzen\n' +
        '/give <betrag> — als Reply: Coins verschenken\n\n' +
        '🎲 *Spiele*\n' +
        '/slot <einsatz> — Spielautomat\n' +
        '/rps <stein|papier|schere>\n' +
        '/blackjack (oder /bj) — Blackjack starten\n' +
        '/hit — Karte ziehen\n' +
        '/stand — Halten\n\n' +
        '🐾 *Haustiere*\n' +
        '/adopt <dog|cat|bird> [name]\n' +
        '/pet — Haustier-Infos\n' +
        '/feed — füttern\n' +
        '/play — spielen\n\n' +
        '💍 *Beziehungen*\n' +
        '/marry — als Reply: Antrag machen\n' +
        '/marry accept / deny / cancel\n' +
        '/divorce — scheiden lassen\n\n' +
        '📝 *Todos*\n' +
        '/todo add|list|done|remove <text/id>\n' +
        '/usertodo add <text> — Vorschlag einreichen\n' +
        '/usertodo list|done|remove <id> — nur Owner\n\n' +
        '✨ *Sonstiges*\n' +
        '/credits — Mitwirkende anzeigen\n' +
        '/partner — Gilden-Bündnisse anzeigen\n' +
        '/rangliste [xp|level|coins]\n\n' +
        '⚡ *Hunter-System (Solo Leveling)*\n' +
        '/awaken — als Hunter erwachen\n' +
        '/hunterinfo · /gate · /extract · /shadows\n' +
        '/huntershop · /buyweapon <code> · /hunterequip <code>\n' +
        '/dailyquest · /hunterrank\n' +
        '/sololevelinghelp — vollständige Hunter-Befehlsliste\n\n' +
        '🛡️ *Admin* (jeweils als Reply auf eine Nachricht)\n' +
        '/addcash <betrag> · /addxp <betrag> · /addvip <1d|12h|30m>\n' +
        '/ban [grund] · /unban · /warn <grund> · /warns · /clearwarns\n' +
        '/setrank <OWNER|COOWNER|ADMIN|MOD|VIP|USER> (nur Owner)\n' +
        '/resetcoins · /resetlevel (nur Owner)\n' +
        '/bancmd <befehl> · /unbancmd <befehl> · /bancmds\n' +
        '/addcredit Name | Rolle · /delcredit <nr> (nur Owner)\n' +
        '/addpartner Name | Link · /delpartner <nr>';
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
    function saveBansIfPresent() { if (FILES.bans) save(FILES.bans, bans); }
    function savePets() { if (FILES.pets) save(FILES.pets, pets); }
    function saveTickets() { if (FILES.tickets) save(FILES.tickets, tickets); }
    function saveTeamTodos() { if (FILES.teamTodos) save(FILES.teamTodos, teamTodos); }
    function saveUserTodos() { if (FILES.userTodos) save(FILES.userTodos, userTodos); }
    function saveCredits() { if (FILES.credits) save(FILES.credits, credits); }
    function savePartners() { if (FILES.partners) save(FILES.partners, partners); }
    function saveMarriages() { if (FILES.marriages) save(FILES.marriages, marriages); }
    function saveCommandBans() { if (FILES.commandBans) save(FILES.commandBans, commandBans); }

    function isCmdBanned(name) {
      return !!(commandBans && commandBans[name]);
    }

    let todoCounter = Object.keys(teamTodos).length;
    let userTodoCounter = Object.keys(userTodos).length;

    // Ausstehende Heiratsanträge: targetJid -> { from: jid, at }
    const pendingMarriageProposals = new Map();

    // ── Solo-Leveling / Hunter-System ────────────────────────────────────────
    // Wiederverwendung desselben Moduls wie auf WhatsApp: identische Logik,
    // identische Datei (sololeveling.json) im DATA_PATH → Hunter-Fortschritt
    // ist zwischen WhatsApp und Telegram geteilt, sofern der Account über
    // /login verknüpft ist (gleiche JID). Nicht verknüpfte Telegram-Nutzer
    // bekommen automatisch ihre eigene "tg<id>@telegram"-Hunter-Akte.
    const soloLeveling = createSoloLevelingSystem(DATA_PATH);

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
      if (!users[jid].name && isNativeTelegramJid(jid) && msg.reply_to_message.from) {
        const f = msg.reply_to_message.from;
        users[jid].name = f.username ? `@${f.username}` : (f.first_name || 'Telegram-Nutzer');
        saveUsers();
      }
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

    // ---- AFK --------------------------------------------------------------------

    telegramBot.onText(/^\/afk(?:\s+(.+))?$/, (msg, match) => {
      const jid = resolveSender(msg);
      users[jid].afk = { reason: (match[1] || 'Abwesend').trim(), at: new Date().toISOString() };
      saveUsers();
      reply(msg.chat.id, `🔕 Du bist jetzt AFK: ${users[jid].afk.reason}`);
    });

    // ---- Profil / Wirtschaft ----------------------------------------------------

    telegramBot.onText(/^\/whoami$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      const rank = ranks[jid] || u.rank || 'USER';
      const linkedNote = isNativeTelegramJid(jid) ? '' : '\n🔗 _(mit WhatsApp verknüpft)_';
      const marriage = marriages[jid];
      const marriageLine = marriage
        ? `\n💍 Verheiratet mit: ${displayName(users[marriage.partner] || {})}`
        : '\n💍 Status: Single';
      reply(msg.chat.id,
        `👤 *${displayName(u)}*\n` +
        `🏅 Rang: ${rank}\n` +
        `⭐ Level: ${u.level || 1}\n` +
        `✨ XP: ${u.xp || 0}\n` +
        `💰 Coins: ${u.coins || 0}${marriageLine}${linkedNote}`
      );
    });

    telegramBot.onText(/^\/profile$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      reply(msg.chat.id, `📊 Profil:\nLevel: ${u.level || 1}\nXP: ${u.xp || 0}\nCoins: ${u.coins || 0}\nNachrichten: ${u.msgCount || 0}`);
    });

    telegramBot.onText(/^\/userinfo$/, (msg) => {
      const jid = resolveReplyTarget(msg) || resolveSender(msg);
      ensureUser(jid);
      const u = users[jid];
      reply(msg.chat.id,
        `👤 *${displayName(u)}*\n` +
        `Level: ${u.level || 1}\nXP: ${u.xp || 0}\nCoins: ${u.coins || 0}\nRang: ${ranks[jid] || u.rank || 'USER'}`
      );
    });

    telegramBot.onText(/^\/setinfo\s+(\S+)\s+(.+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      const feld = match[1].toLowerCase();
      const wert = match[2].trim();
      const erlaubteFelder = {
        name: 'name', alter: 'alter', hobbys: 'hobbys', hobby: 'hobbys',
        sexualitaet: 'sexualitaet', 'sexualität': 'sexualitaet'
      };
      if (!erlaubteFelder[feld]) {
        return reply(msg.chat.id, '❌ Verfügbare Felder: name, alter, hobbys, sexualitaet\nBeispiel: `/setinfo alter 22`');
      }
      const key = erlaubteFelder[feld];
      if (key === 'alter') {
        const num = parseInt(wert);
        if (isNaN(num) || num < 1 || num > 120) return reply(msg.chat.id, '❌ Bitte gib ein gültiges Alter zwischen 1 und 120 an.');
        users[jid].alter = num;
      } else {
        users[jid][key] = wert;
      }
      saveUsers();
      reply(msg.chat.id, `✅ ${feld} wurde gespeichert. Nutze /whoami, um dein Profil anzuzeigen.`);
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
      if (isCmdBanned('work')) return reply(msg.chat.id, '⛔ Dieser Befehl wurde vom Owner gesperrt.');
      const jid = resolveSender(msg);
      const u = users[jid];
      const earn = randInt(50, 200);
      u.coins = (u.coins || 0) + earn;
      u.xp = (u.xp || 0) + 20;
      saveUsers();
      reply(msg.chat.id, `🛠 Du hast ${earn} Coins verdient!`);
    });

    telegramBot.onText(/^\/fish$/, (msg) => {
      if (isCmdBanned('fish')) return reply(msg.chat.id, '⛔ Dieser Befehl wurde vom Owner gesperrt.');
      const jid = resolveSender(msg);
      const u = users[jid];

      const totalWeight = FISH_EVENTS.reduce((sum, e) => sum + e.chance, 0);
      let random = Math.random() * totalWeight;
      let selected = FISH_EVENTS[FISH_EVENTS.length - 1];
      for (const event of FISH_EVENTS) {
        random -= event.chance;
        if (random <= 0) { selected = event; break; }
      }

      if (selected.coins) u.coins = Math.max(0, (u.coins || 0) + selected.coins);
      if (selected.xp) u.xp = (u.xp || 0) + selected.xp;
      if (selected.item) {
        if (!u.items) u.items = {};
        u.items[selected.item] = (u.items[selected.item] || 0) + (selected.itemQty || 1);
      }
      saveUsers();

      reply(msg.chat.id, `🎣 *— ANGELN IN AINCRAD —* 🎣\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${selected.text}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`);
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

    telegramBot.onText(/^\/give\s+(\d+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte als Antwort auf die Nachricht des Empfängers senden.\nNutzung: `/give <betrag>` (als Reply)');
      if (targetJid === jid) return reply(msg.chat.id, '❌ Du kannst dir nicht selbst Coins geben!');
      const amount = parseInt(match[1]);
      if (isNaN(amount) || amount <= 0) return reply(msg.chat.id, '❌ Ungültiger Betrag.');
      if ((users[jid].coins || 0) < amount) return reply(msg.chat.id, '❌ Nicht genug Coins!');
      users[jid].coins -= amount;
      users[targetJid].coins = (users[targetJid].coins || 0) + amount;
      saveUsers();
      reply(msg.chat.id, `✅ ${amount} Coins an ${displayName(users[targetJid])} gesendet!`);
    });

    // ---- Spiele -----------------------------------------------------------------

    telegramBot.onText(/^\/slot(?:\s+(\d+))?$/, (msg, match) => {
      if (isCmdBanned('slot')) return reply(msg.chat.id, '⛔ Dieser Befehl wurde vom Owner gesperrt.');
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
      if (isCmdBanned('rps')) return reply(msg.chat.id, '⛔ Dieser Befehl wurde vom Owner gesperrt.');
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

    telegramBot.onText(/^\/(blackjack|bj)$/, (msg) => {
      if (isCmdBanned('blackjack')) return reply(msg.chat.id, '⛔ Dieser Befehl wurde vom Owner gesperrt.');
      const jid = resolveSender(msg);
      const u = users[jid];
      if (u.bj?.active) return reply(msg.chat.id, 'Du hast bereits ein aktives Spiel. Nutze /hit oder /stand.');
      const player = [bjDraw(), bjDraw()];
      const dealer = [bjDraw(), bjDraw()];
      u.bj = { player, dealer, active: true };
      saveUsers();
      reply(msg.chat.id,
        `🃏 *Blackjack!*\nDeine Karten: ${player.map(c => c.value + c.suit).join(', ')}\nDealer zeigt: ${dealer[0].value + dealer[0].suit}\nNutze /hit oder /stand`
      );
    });

    telegramBot.onText(/^\/hit$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      if (!u.bj?.active) return reply(msg.chat.id, 'Kein aktives Spiel. Starte mit /blackjack');
      const bj = u.bj;
      bj.player.push(bjDraw());
      const p = bjScore(bj.player);
      let out = `Deine Karten: ${bj.player.map(c => c.value + c.suit).join(', ')}\nPunkte: ${p}`;
      if (p > 21) {
        out += '\n😢 Bust! Du verlierst.';
        delete u.bj;
      } else if (p === 21) {
        u.coins = (u.coins || 0) + 75;
        u.xp = (u.xp || 0) + 40;
        out += '\n🎉 Blackjack! Du gewinnst +75 Coins +40 XP';
        delete u.bj;
      } else {
        out += '\nNutze /hit oder /stand';
      }
      saveUsers();
      reply(msg.chat.id, out);
    });

    telegramBot.onText(/^\/stand$/, (msg) => {
      const jid = resolveSender(msg);
      const u = users[jid];
      if (!u.bj?.active) return reply(msg.chat.id, 'Kein aktives Spiel. Starte mit /blackjack');
      const bj = u.bj;
      const p = bjScore(bj.player);
      let d = bjScore(bj.dealer);
      while (d < 17) {
        bj.dealer.push(bjDraw());
        d = bjScore(bj.dealer);
      }
      let out = `Dealer-Karten: ${bj.dealer.map(c => c.value + c.suit).join(', ')}\nDealer: ${d}\nDu: ${p}`;
      if (p > 21) {
        out += '\n😢 Bust! Du verlierst.';
      } else if (d > 21 || p > d) {
        u.coins = (u.coins || 0) + 75;
        u.xp = (u.xp || 0) + 40;
        out += '\n🎉 Du gewinnst! +75 Coins +40 XP';
      } else if (p === d) {
        out += '\nUnentschieden';
      } else {
        out += '\nDealer gewinnt';
      }
      delete u.bj;
      saveUsers();
      reply(msg.chat.id, out);
    });

    // ---- Haustiere ----------------------------------------------------------------

    telegramBot.onText(/^\/adopt\s+(dog|cat|bird)(?:\s+(.+))?$/i, (msg, match) => {
      const jid = resolveSender(msg);
      const type = match[1].toLowerCase();
      const name = match[2] ? match[2].trim() : null;
      pets[jid] = { type, name, xp: 0, hunger: 100, happiness: 100, lastFed: Date.now() };
      savePets();
      reply(msg.chat.id, `🐾 ${type} ${name ? 'mit Namen ' + name : ''} adoptiert!`);
    });

    telegramBot.onText(/^\/(pet|petinfo)$/, (msg) => {
      const jid = resolveSender(msg);
      const p = pets[jid];
      if (!p) return reply(msg.chat.id, 'Du hast kein Haustier. Nutze `/adopt <dog|cat|bird> [name]`');
      reply(msg.chat.id, `🐶 ${p.type} ${p.name ? '- ' + p.name : ''}\nHunger: ${p.hunger}%\nGlück: ${p.happiness}%\nXP: ${p.xp}`);
    });

    telegramBot.onText(/^\/feed$/, (msg) => {
      const jid = resolveSender(msg);
      const p = pets[jid];
      if (!p) return reply(msg.chat.id, 'Du hast kein Haustier.');
      p.hunger = Math.min(100, (p.hunger || 0) + 20);
      p.happiness = Math.min(100, (p.happiness || 0) + 10);
      p.lastFed = Date.now();
      savePets();
      reply(msg.chat.id, `🍖 ${p.type} gefüttert. Hunger: ${p.hunger}% Glück: ${p.happiness}%`);
    });

    telegramBot.onText(/^\/play$/, (msg) => {
      const jid = resolveSender(msg);
      const p = pets[jid];
      if (!p) return reply(msg.chat.id, 'Du hast kein Haustier.');
      p.happiness = Math.min(100, (p.happiness || 0) + 20);
      p.xp = (p.xp || 0) + 5;
      savePets();
      reply(msg.chat.id, `🎾 Mit ${p.type} gespielt. Glück: ${p.happiness}% XP: ${p.xp}`);
    });

    // ---- Beziehungen ----------------------------------------------------------------

    telegramBot.onText(/^\/marry(?:\s+(accept|deny|decline|cancel))?$/i, (msg, match) => {
      const jid = resolveSender(msg);
      const sub = (match[1] || '').toLowerCase();

      if (sub === 'accept') {
        const proposal = pendingMarriageProposals.get(jid);
        if (!proposal) return reply(msg.chat.id, '❌ Du hast keinen offenen Heiratsantrag.');
        if (marriages[jid] || marriages[proposal.from]) {
          pendingMarriageProposals.delete(jid);
          return reply(msg.chat.id, '❌ Einer von euch ist inzwischen bereits verheiratet.');
        }
        marriages[jid] = { partner: proposal.from, since: Date.now() };
        marriages[proposal.from] = { partner: jid, since: Date.now() };
        saveMarriages();
        pendingMarriageProposals.delete(jid);
        return reply(msg.chat.id, `💍 Herzlichen Glückwunsch! Du und ${displayName(users[proposal.from] || {})} sind jetzt verheiratet! 🎉`);
      }

      if (sub === 'deny' || sub === 'decline') {
        if (!pendingMarriageProposals.has(jid)) return reply(msg.chat.id, '❌ Du hast keinen offenen Heiratsantrag.');
        pendingMarriageProposals.delete(jid);
        return reply(msg.chat.id, '💔 Der Heiratsantrag wurde abgelehnt.');
      }

      if (sub === 'cancel') {
        let found = null;
        for (const [targetJid, v] of pendingMarriageProposals.entries()) {
          if (v.from === jid) { found = targetJid; break; }
        }
        if (!found) return reply(msg.chat.id, '❌ Du hast keinen offenen Antrag zum Zurückziehen.');
        pendingMarriageProposals.delete(found);
        return reply(msg.chat.id, '✅ Dein Heiratsantrag wurde zurückgezogen.');
      }

      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Nutzung: als Antwort auf die Nachricht der gewünschten Person `/marry` senden.\n/marry accept / deny / cancel');
      if (targetJid === jid) return reply(msg.chat.id, '❌ Du kannst dich nicht selbst heiraten! 😅');
      if (marriages[jid]) return reply(msg.chat.id, `❌ Du bist bereits mit ${displayName(users[marriages[jid].partner] || {})} verheiratet. Nutze zuerst /divorce.`);
      if (marriages[targetJid]) return reply(msg.chat.id, `❌ ${displayName(users[targetJid])} ist bereits verheiratet.`);

      const existing = pendingMarriageProposals.get(targetJid);
      if (existing && existing.from === jid) return reply(msg.chat.id, '❌ Du hast bereits einen offenen Antrag an diese Person.');

      pendingMarriageProposals.set(targetJid, { from: jid, at: Date.now() });
      reply(msg.chat.id, `💍 Antrag an ${displayName(users[targetJid])} gesendet! Die Person kann mit /marry accept annehmen oder /marry deny ablehnen.`);
    });

    telegramBot.onText(/^\/divorce$/, (msg) => {
      const jid = resolveSender(msg);
      const marriage = marriages[jid];
      if (!marriage) return reply(msg.chat.id, '❌ Du bist nicht verheiratet.');
      const partnerJid = marriage.partner;
      delete marriages[jid];
      delete marriages[partnerJid];
      saveMarriages();
      reply(msg.chat.id, `💔 Du hast dich von ${displayName(users[partnerJid] || {})} scheiden lassen.`);
    });

    // ---- Todos ----------------------------------------------------------------------

    telegramBot.onText(/^\/todo(?:\s+(\S+))?(?:\s+(.+))?$/, (msg, match) => {
      const jid = resolveSender(msg);
      const sub = (match[1] || 'list').toLowerCase();
      const rest = match[2] || '';

      if (sub === 'list') {
        const all = Object.values(teamTodos);
        if (!all.length) return reply(msg.chat.id, '📝 Keine Team-Todos.');
        const lines = all.map(t => `${t.id} [${t.status}] - ${t.text}` + (t.status === 'done' ? ' ✅' : ''));
        return reply(msg.chat.id, `📝 *Team-Todos:*\n${lines.join('\n')}`);
      }
      if (sub === 'add') {
        if (!rest.trim()) return reply(msg.chat.id, 'Nutzung: `/todo add <text>`');
        todoCounter++;
        const tdId = `TD${String(todoCounter).padStart(3, '0')}`;
        teamTodos[tdId] = { id: tdId, text: rest.trim(), creator: jid, status: 'open', created: Date.now() };
        saveTeamTodos();
        return reply(msg.chat.id, `✅ Todo ${tdId} erstellt.`);
      }
      if (sub === 'done' || sub === 'complete') {
        const id = rest.trim();
        if (!id || !teamTodos[id]) return reply(msg.chat.id, 'Nutzung: `/todo done <id>`');
        teamTodos[id].status = 'done';
        teamTodos[id].doneBy = jid;
        saveTeamTodos();
        return reply(msg.chat.id, `✅ Todo ${id} erledigt.`);
      }
      if (sub === 'remove' || sub === 'rm') {
        if (!isAuthorized(jid, ['OWNER', 'COOWNER', 'ADMIN'])) return reply(msg.chat.id, 'Kein Zugriff.');
        const id = rest.trim();
        if (!id || !teamTodos[id]) return reply(msg.chat.id, 'Nutzung: `/todo remove <id>`');
        delete teamTodos[id];
        saveTeamTodos();
        return reply(msg.chat.id, `🗑️ Todo ${id} entfernt.`);
      }
      reply(msg.chat.id, 'Nutzung: `/todo add <text>` | `list` | `done <id>` | `remove <id>`');
    });

    telegramBot.onText(/^\/usertodo(?:\s+(\S+))?(?:\s+(.+))?$/, (msg, match) => {
      const jid = resolveSender(msg);
      const sub = (match[1] || 'list').toLowerCase();
      const rest = match[2] || '';
      const isOwner = isAuthorized(jid, ['OWNER']);

      if (sub === 'add') {
        const text = rest.trim();
        if (!text) return reply(msg.chat.id, 'Nutzung: `/usertodo add <befehlsvorschlag>`');
        userTodoCounter++;
        const utId = `UT${String(userTodoCounter).padStart(3, '0')}`;
        userTodos[utId] = { id: utId, text, sender: jid, status: 'open', created: Date.now() };
        saveUserTodos();
        return reply(msg.chat.id, `✅ Dein Vorschlag wurde gespeichert (${utId})! Der Owner schaut sich das an.`);
      }
      if (sub === 'done' || sub === 'complete') {
        if (!isOwner) return reply(msg.chat.id, '❌ Nur der Owner kann Vorschläge als erledigt markieren.');
        const id = rest.trim();
        if (!id || !userTodos[id]) return reply(msg.chat.id, 'Nutzung: `/usertodo done <id>`');
        userTodos[id].status = 'done';
        userTodos[id].doneBy = jid;
        saveUserTodos();
        return reply(msg.chat.id, `✅ Vorschlag ${id} als erledigt markiert.`);
      }
      if (sub === 'remove' || sub === 'rm' || sub === 'delete') {
        if (!isOwner) return reply(msg.chat.id, '❌ Nur der Owner kann Vorschläge entfernen.');
        const id = rest.trim();
        if (!id || !userTodos[id]) return reply(msg.chat.id, 'Nutzung: `/usertodo remove <id>`');
        delete userTodos[id];
        saveUserTodos();
        return reply(msg.chat.id, `🗑️ Vorschlag ${id} entfernt.`);
      }
      if (sub === 'list') {
        if (!isOwner) return reply(msg.chat.id, '❌ Nur der Owner kann sich die Vorschlagsliste ansehen. Nutze `/usertodo add <text>`.');
        const all = Object.values(userTodos);
        if (!all.length) return reply(msg.chat.id, '📋 Es liegen noch keine User-Vorschläge vor.');
        const lines = all.map(t => `${t.status === 'done' ? '✅' : '🕓'} ${t.id} — ${t.text}\n   von: ${displayName(users[t.sender] || {})}`);
        return reply(msg.chat.id, `📋 *Von Usern vorgeschlagene Befehle*\n\n${lines.join('\n\n')}`);
      }
      reply(msg.chat.id, `Nutzung: \`/usertodo add <text>\`${isOwner ? ' | list | done <id> | remove <id>' : ''}`);
    });

    // ---- Credits & Partner ------------------------------------------------------

    telegramBot.onText(/^\/credits$/, (msg) => {
      if (!credits.list || credits.list.length === 0) return reply(msg.chat.id, '📋 Noch keine Credits eingetragen.');
      let out = '✨ *Credits* ✨\n\n';
      credits.list.forEach((c, i) => { out += `${i + 1}. *${c.name}* — ${c.role}\n`; });
      out += '\n❤️ Danke euch allen!';
      reply(msg.chat.id, out);
    });

    telegramBot.onText(/^\/addcredit\s+(.+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      if (!isAuthorized(jid, ['OWNER'])) return reply(msg.chat.id, '❌ Nur der Inhaber darf Credits hinzufügen.');
      const [name, role] = match[1].split('|').map(s => s?.trim());
      if (!name || !role) return reply(msg.chat.id, '❌ Nutzung: `/addcredit Name | Rolle`');
      credits.list.push({ name, role });
      saveCredits();
      reply(msg.chat.id, `✅ *${name}* wurde zu den Credits hinzugefügt.`);
    });

    telegramBot.onText(/^\/delcredit\s+(\d+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      if (!isAuthorized(jid, ['OWNER'])) return reply(msg.chat.id, '❌ Nur der Inhaber darf Credits entfernen.');
      const index = parseInt(match[1]) - 1;
      if (isNaN(index) || index < 0 || index >= credits.list.length) return reply(msg.chat.id, '❌ Ungültige Nummer. Nutze /credits für die Liste.');
      const removed = credits.list.splice(index, 1)[0];
      saveCredits();
      reply(msg.chat.id, `🗑️ *${removed.name}* wurde aus den Credits entfernt.`);
    });

    telegramBot.onText(/^\/partner(?:s)?$/, (msg) => {
      if (!partners.list || partners.list.length === 0) return reply(msg.chat.id, '⚔️ Aktuell bestehen keine Bündnisse mit anderen Gilden.');
      let out = '⚔️ *— GILDEN-BÜNDNISSE —* ⚔️\n\n';
      partners.list.forEach(p => { out += `🛡️ *${p.name}*\n🔗 ${p.link}\n\n`; });
      reply(msg.chat.id, out);
    });

    telegramBot.onText(/^\/addpartner\s+(.+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      if (!isAuthorized(jid, ['OWNER', 'COOWNER'])) return reply(msg.chat.id, '❌ Nur der Gildenmeister darf neue Bündnisse eingehen.');
      const [name, link] = match[1].split('|').map(s => s?.trim());
      if (!name || !link || !/^https?:\/\//i.test(link)) return reply(msg.chat.id, '❌ Nutzung: `/addpartner Name | https://link`');
      partners.list.push({ name, link, addedBy: jid, at: Date.now() });
      savePartners();
      reply(msg.chat.id, `✅ Bündnis mit *${name}* wurde geschlossen! ⚔️`);
    });

    telegramBot.onText(/^\/delpartner\s+(\d+)$/, (msg, match) => {
      const jid = resolveSender(msg);
      if (!isAuthorized(jid, ['OWNER', 'COOWNER'])) return reply(msg.chat.id, '❌ Nur der Gildenmeister darf Bündnisse auflösen.');
      const index = parseInt(match[1]) - 1;
      if (isNaN(index) || index < 0 || index >= partners.list.length) return reply(msg.chat.id, '❌ Ungültige Nummer. Nutze /partner für die Liste.');
      const removed = partners.list.splice(index, 1)[0];
      savePartners();
      reply(msg.chat.id, `💔 Bündnis mit *${removed.name}* wurde aufgelöst.`);
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
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const amount = parseInt(match[1]);
      users[targetJid].coins = (users[targetJid].coins || 0) + amount;
      saveUsers();
      reply(msg.chat.id, `✅ ${amount} Coins vergeben an ${displayName(users[targetJid])}.`);
    });

    telegramBot.onText(/^\/addxp\s+(\d+)$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const amount = parseInt(match[1]);
      users[targetJid].xp = (users[targetJid].xp || 0) + amount;
      saveUsers();
      reply(msg.chat.id, `✅ ${amount} XP vergeben an ${displayName(users[targetJid])}.`);
    });

    telegramBot.onText(/^\/addvip\s+(\d+[dhm])$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const duration = parseDuration(match[1]);
      if (!duration) return reply(msg.chat.id, '❌ Ungültiges Zeitformat. Beispiel: `/addvip 1d` (als Reply)');
      users[targetJid].vipUntil = Date.now() + duration;
      const prevRank = ranks[targetJid] || users[targetJid].rank || 'USER';
      if (prevRank === 'USER') { ranks[targetJid] = 'VIP'; users[targetJid].rank = 'VIP'; saveRanks(); }
      saveUsers();
      reply(msg.chat.id, `✅ VIP für ${displayName(users[targetJid])} bis ${new Date(users[targetJid].vipUntil).toLocaleString('de-DE')}.`);
    });

    telegramBot.onText(/^\/ban(?:\s+(.+))?$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const reason = match[1] || 'Kein Grund';
      bans[targetJid] = { by: senderJid, at: new Date().toISOString(), reason };
      saveBansIfPresent();
      reply(msg.chat.id, `🚫 ${displayName(users[targetJid])} gebannt. Grund: ${reason}`);
    });

    telegramBot.onText(/^\/unban$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      delete bans[targetJid];
      saveBansIfPresent();
      reply(msg.chat.id, `✅ ${displayName(users[targetJid])} entbannt.`);
    });

    telegramBot.onText(/^\/warn(?:\s+(.+))?$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const reason = match[1] || 'Kein Grund';
      users[targetJid].warns = users[targetJid].warns || [];
      users[targetJid].warns.push({ by: senderJid, reason, at: new Date().toISOString() });
      saveUsers();
      reply(msg.chat.id, `⚠ ${displayName(users[targetJid])} verwarnt. Grund: ${reason}`);
    });

    telegramBot.onText(/^\/warns$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const warnList = users[targetJid]?.warns || [];
      if (!warnList.length) return reply(msg.chat.id, `✅ ${displayName(users[targetJid])} hat keine Verwarnungen.`);
      const lines = warnList.map((w, i) => `${i + 1}. ${w.reason} — von ${displayName(users[w.by] || {})} (${new Date(w.at).toLocaleString('de-DE')})`);
      reply(msg.chat.id, `⚠️ *Verwarnungen von ${displayName(users[targetJid])}* (${warnList.length}):\n\n${lines.join('\n')}`);
    });

    telegramBot.onText(/^\/clearwarns$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return reply(msg.chat.id, '❌ Kein Zugriff.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      if (users[targetJid]) users[targetJid].warns = [];
      saveUsers();
      reply(msg.chat.id, `✅ Warns entfernt für ${displayName(users[targetJid])}.`);
    });

    telegramBot.onText(/^\/setrank\s+(OWNER|COOWNER|ADMIN|MOD|VIP|USER)$/i, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER'])) return reply(msg.chat.id, '❌ Nur der Inhaber.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const r = match[1].toUpperCase();
      ranks[targetJid] = r;
      if (users[targetJid]) users[targetJid].rank = r;
      saveRanks();
      saveUsers();
      reply(msg.chat.id, `✅ Rang von ${displayName(users[targetJid])} auf ${r} gesetzt.`);
    });

    telegramBot.onText(/^\/resetcoins$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER'])) return reply(msg.chat.id, '❌ Nur der Inhaber darf diesen Befehl nutzen.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const old = users[targetJid].coins || 0;
      users[targetJid].coins = 0;
      saveUsers();
      reply(msg.chat.id, `✅ Coins von ${displayName(users[targetJid])} zurückgesetzt (vorher: ${old} → jetzt: 0).`);
    });

    telegramBot.onText(/^\/resetlevel$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER'])) return reply(msg.chat.id, '❌ Nur der Inhaber darf diesen Befehl nutzen.');
      const targetJid = resolveReplyTarget(msg);
      if (!targetJid) return reply(msg.chat.id, '❌ Bitte auf die Nachricht des Ziel-Nutzers antworten.');
      const oldLevel = users[targetJid].level || 1;
      const oldXp = users[targetJid].xp || 0;
      users[targetJid].level = 1;
      users[targetJid].xp = 0;
      saveUsers();
      reply(msg.chat.id, `✅ Level von ${displayName(users[targetJid])} zurückgesetzt (vorher: Lv.${oldLevel}, ${oldXp} XP → jetzt: Lv.1, 0 XP).`);
    });

    telegramBot.onText(/^\/bancmd\s+(\S+)$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER'])) return reply(msg.chat.id, '❌ Nur Owner/CoOwner.');
      const cmdName = match[1].toLowerCase().replace(/^\//, '');
      if (['bancmd', 'unbancmd', 'help', 'start'].includes(cmdName)) return reply(msg.chat.id, `❌ Der Befehl "${cmdName}" kann nicht gesperrt werden.`);
      commandBans[cmdName] = { by: senderJid, at: new Date().toISOString() };
      saveCommandBans();
      reply(msg.chat.id, `⛔ Befehl ${cmdName} wurde gesperrt (gilt für WhatsApp & Telegram, sofern gemeinsam genutzt).`);
    });

    telegramBot.onText(/^\/unbancmd\s+(\S+)$/, (msg, match) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER'])) return reply(msg.chat.id, '❌ Nur Owner/CoOwner.');
      const cmdName = match[1].toLowerCase().replace(/^\//, '');
      if (!commandBans[cmdName]) return reply(msg.chat.id, `ℹ️ Befehl ${cmdName} war nicht gesperrt.`);
      delete commandBans[cmdName];
      saveCommandBans();
      reply(msg.chat.id, `✅ Befehl ${cmdName} wurde entsperrt.`);
    });

    telegramBot.onText(/^\/bancmds$/, (msg) => {
      const senderJid = resolveSender(msg);
      if (!isAuthorized(senderJid, ['OWNER', 'COOWNER'])) return reply(msg.chat.id, '❌ Nur Owner/CoOwner.');
      const entries = Object.entries(commandBans || {});
      if (!entries.length) return reply(msg.chat.id, '✅ Aktuell sind keine Befehle gesperrt.');
      const lines = entries.map(([cmdName, info]) =>
        `⛔ /${cmdName} — gesperrt von ${displayName(users[info.by] || {})} am ${info.at ? new Date(info.at).toLocaleString('de-DE') : '(unbekannt)'}`
      );
      reply(msg.chat.id, `📋 *Gesperrte Befehle* (${entries.length}):\n\n${lines.join('\n')}`);
    });

    // ---- Hunter-System (Solo Leveling) -------------------------------------------
    // Ein generischer Router: fängt jede Nachricht ab, die wie ein Befehl aussieht,
    // und leitet sie nur dann an das Hunter-System weiter, wenn der Befehlsname
    // zu SL_COMMANDS gehört. Alle anderen Befehle laufen unangetastet weiter
    // durch ihre eigenen, spezifischen onText-Handler oben.
    const slCmdRegex = new RegExp(`\\?(${soloLeveling.SL_COMMANDS.join('|')})\\b`, 'g');

    telegramBot.onText(/^\/(\S+)(?:\s+([\s\S]+))?$/, async (msg, match) => {
      const cmdName = match[1].toLowerCase();
      if (!soloLeveling.SL_COMMANDS.includes(cmdName)) return;

      const jid = resolveSender(msg);
      const chatId = msg.chat.id;
      const isGroupChat = msg.chat.type !== 'private';
      const argsStr = (match[2] || '').trim();
      const args = argsStr ? argsStr.split(/\s+/) : [];

      // Die Hilfetexte des Moduls nutzen "?befehl" (WhatsApp-Präfix) —
      // für Telegram hier auf "/befehl" umschreiben, ohne das Modul selbst
      // anzufassen.
      const sendAdapted = (text, opts) => reply(chatId, String(text).replace(slCmdRegex, '/$1'), opts);

      // Schlanker Ersatz für das Baileys-`sock`-Objekt: das Modul nutzt es nur,
      // um bei einer erfolgreichen Schatten-Extraktion eine Sprachnachricht
      // ("Arise") zu senden — das funktioniert per sendVoice 1:1 genauso.
      const slSock = {
        sendMessage: async (targetChatId, opts) => {
          if (opts?.audio) {
            try { await telegramBot.sendVoice(targetChatId, opts.audio); }
            catch (e) { console.error('[sololeveling] Telegram-Voice-Fehler:', e?.message || e); }
          }
        }
      };

      try {
        const handled = await soloLeveling.handle({
          cmd: cmdName,
          args,
          sender: jid,
          from: chatId,
          isGroup: isGroupChat,
          send: sendAdapted,
          sock: slSock,
          users,
          ensureUser,
          normalizeJid: (j) => j, // JIDs sind hier bereits kanonisch (tg<id>@telegram oder verknüpfte WA-JID)
          getNumberMention: async (j) => displayName(users[j] || {}),
          randInt,
          isPrimaryOwner: (j) => isAuthorized(j, ['OWNER'])
        });
        if (!handled) return; // sollte wegen des SL_COMMANDS-Filters oben nicht vorkommen
      } catch (e) {
        console.error('[sololeveling] Fehler:', e?.message || e);
        reply(chatId, '❌ Ein Fehler ist im Hunter-System aufgetreten.');
      }
    });

    // ---- Ban-Sperre & AFK-Auflösung ----------------------------------------------

    telegramBot.on('message', (msg) => {
      if (!msg.text) return;
      const jid = jidForTelegramUser(msg.from.id);

      if (bans[jid]) {
        if (msg.text.startsWith('/')) reply(msg.chat.id, '🚫 Du bist gebannt.');
        return;
      }

      if (users[jid]?.afk && !msg.text.startsWith('/afk')) {
        const reason = users[jid].afk.reason || 'Abwesend';
        delete users[jid].afk;
        saveUsers();
        reply(msg.chat.id, `✅ Du bist nicht mehr AFK (Grund: ${reason}).`);
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
