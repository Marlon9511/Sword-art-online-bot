import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason } from '@whiskeysockets/baileys';
import fs from 'fs';
import path from 'path';
import P from 'pino';
import fetch from 'node-fetch';
import QRCodeImg from 'qrcode';
import { exec } from 'child_process';
import archiver from 'archiver';
import QRCode from 'qrcode-terminal';
import chalk from 'chalk';
import readline from "readline";
import gradient from "gradient-string";

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const question = (q) => new Promise(resolve => rl.question(q, resolve));

const ensureDir = (dir) => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
};

const ensureFile = (filePath, defaultData = {}) => {
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(
      filePath,
      typeof defaultData === "string" ? defaultData : JSON.stringify(defaultData, null, 2)
    );
  } else {
    try {
      const data = fs.readFileSync(filePath, 'utf-8');
      if (data.trim() === "") throw new Error("Empty file");
      JSON.parse(data);
    } catch {
      fs.writeFileSync(
        filePath,
        typeof defaultData === "string" ? defaultData : JSON.stringify(defaultData, null, 2)
      );
    }
  }
};

const BASE_DIR = path.resolve('./');
const SESSIONS_DIR = path.join(BASE_DIR, 'sessions');
const DATA_PATH = path.join(BASE_DIR, 'data');

ensureDir(SESSIONS_DIR);
ensureDir(DATA_PATH);

const USERS_FILE = path.join(DATA_PATH, 'users.json');
const RESTART_FILE = path.join(DATA_PATH, 'restart.json');
const LOG_FILE = path.join(BASE_DIR, 'logs.txt');

ensureFile(USERS_FILE, { registeredUsers: {} });
ensureFile(RESTART_FILE, {});
ensureFile(LOG_FILE, "");

const FILES = {
  users: { file: 'users.json', default: {} },
  bans: { file: 'bans.json', default: {} },
  joinreq: { file: 'joinreq.json', default: {} },
  pets: { file: 'pets.json', default: {} },
  tickets: { file: 'tickets.json', default: {} },
  ranks: { file: 'ranks.json', default: {} },
  broadcastSettings: { file: 'broadcast-settings.json', default: {} },
  deleted: { file: 'deleted.json', default: {} },
  owner: { file: 'owner.json', default: {} },
  teamTodos: { file: 'team-todos.json', default: {} },
  groupInvites: { file: 'group-invites.json', default: {} },
  groupSettings: { file: 'group-settings.json', default: {} }
};

Object.values(FILES).forEach(({ file, default: def }) => {
  ensureFile(path.join(DATA_PATH, file), def);
});

const activeSessions = new Map();
const registeredUsers = JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8')).registeredUsers;

const DSGVO_TEXT = `
Datenschutzerklärung und Einwilligung:

1. Ich stimme der Verarbeitung meiner Nachrichten und Daten durch den Bot zu.
2. Meine Daten werden nur zur Bereitstellung des Dienstes verwendet.
3. Ich kann meine Einwillung jederzeit widerrufen (Befehl: $unregister).
4. Meine Daten werden bei Widerruf gelöscht.

Zum Registrieren antworten Sie bitte mit "$register confirm".
`;

const saveRegisteredUsers = () => {
  fs.writeFileSync(USERS_FILE, JSON.stringify({ registeredUsers }, null, 2));
};

//=========================//
// Connect Bot + Pairing-Code
//=========================//
async function connectBot() {
  const { state, saveCreds } = await useMultiFileAuthState("./auth");

  const sock = makeWASocket({
    auth: state,
    logger: P({ level: 'silent' }),
    printQRInTerminal: false // false, weil wir Pairing-Code nutzen
  });

  if (!sock.authState.creds.registered) {
    let phoneNumber = await question(gradient("#ff0000", "#C00000")("📲 Deine Nummer (inkl. Ländervorwahl, z.B. 49123456789): "));
    phoneNumber = phoneNumber.replace(/[^0-9]/g, "");

    if (!phoneNumber) {
      console.log(chalk.red("❌ Ungültige Telefonnummer!"));
      return;
    }

    console.log(chalk.yellow("⏳ Generiere Pairing-Code... Bitte warten..."));
    setTimeout(async () => {
      try {
        let code = await sock.requestPairingCode(phoneNumber);
        code = code?.match(/.{1,4}/g)?.join("-") || code;
        console.log(gradient("#00ffcc", "#0099ff")("\n🔑 DEIN PAIRING CODE: " + code + "\n"));
      } catch (error) {
        console.log(chalk.red("❌ Fehler beim Generieren des Pairing-Codes: "), error);
      }
    }, 3000);
  }

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === "close") {
      const shouldReconnect = (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut);
      console.log(chalk.red("❌ Verbindung geschlossen."));
      if (shouldReconnect) {
        console.log(chalk.yellow("🔄 Reconnecte in 5 Sekunden..."));
        setTimeout(connectBot, 5000);
      }
    } else if (connection === "open") {
      console.log(chalk.green("✅ Erfolgreich mit WhatsApp verbunden!"));
      console.log(chalk.green("-----------------------------------------"));
    }
  });

  sock.ev.on("creds.update", saveCreds);
}

// ========== CONFIG ==========

const ROLES = {
  OWNER: [],
  COOWNER: [],
  ADMIN: [],
  MOD: [],
  VIP: [],
  USER: [],
  SUPPORTER: [],
  TEST_SUPPORTER: []
};

const SUPPORT_CONFIG = {
  TICKET_GROUP: '120363425785044232@g.us',
  SUPPORT_GROUP: '120363426001183575@g.us',
};

const owner = '27088878862400@lid';
let OWNER_LID = '27088878862400@lid';
let OWNER_PRIV = '4915111254435@s.whatsapp.net';
let COOWNER_LID = '85865774756093@lid';

ROLES.OWNER.push(OWNER_LID, OWNER_PRIV);
ROLES.COOWNER.push(COOWNER_LID);

const PREFIX = '$';

const BOT_STATE_FILE = path.join(DATA_PATH, 'bot-state.json');
let BOT_OFFLINE = false;
try {
  if (fs.existsSync(BOT_STATE_FILE)) {
    const st = JSON.parse(fs.readFileSync(BOT_STATE_FILE, 'utf8') || '{}');
    BOT_OFFLINE = !!st.offline;
  }
} catch (e) { console.error('Failed to load bot state:', e); }

const saveBotState = () => {
  try {
    fs.writeFileSync(BOT_STATE_FILE, JSON.stringify({ offline: !!BOT_OFFLINE }, null, 2));
  } catch (e) { console.error('Failed to save bot state:', e); }
};

const _teamTodosPath = path.join(DATA_PATH, FILES.teamTodos.file);
if (!fs.existsSync(_teamTodosPath)) fs.writeFileSync(_teamTodosPath, '{}');

const _groupInvitesPath = path.join(DATA_PATH, FILES.groupInvites.file);
if (!fs.existsSync(_groupInvitesPath)) fs.writeFileSync(_groupInvitesPath, '{}');

const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function createBackup() {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupPath = `./backup_${timestamp}.zip`;

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(backupPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', () => resolve(backupPath));
    archive.on('error', err => reject(err));

    archive.pipe(output);

    Object.values(FILES).forEach(fileObj => {
      const filePath = path.join(DATA_PATH, fileObj.file);
      if (fs.existsSync(filePath)) {
        archive.file(filePath, { name: fileObj.file });
      }
    });

    if (fs.existsSync(SESSIONS_DIR)) {
      archive.directory(SESSIONS_DIR, 'sessions');
    }

    archive.finalize();
  });
}

const vipExpiry = new Map();

function addVip(jid, durationStr) {
  const duration = parseDuration(durationStr);
  if (!duration) return false;

  const expiry = Date.now() + duration;
  vipExpiry.set(jid, expiry);

  if (!ROLES.VIP.includes(jid)) {
    ROLES.VIP.push(jid);
  }

  setTimeout(() => {
    ROLES.VIP = ROLES.VIP.filter(id => id !== jid);
    vipExpiry.delete(jid);
  }, duration);

  return true;
}

function parseDuration(str) {
  const match = str.match(/^(\d+)([dhm])$/);
  if (!match) return null;

  const [_, amount, unit] = match;
  const num = parseInt(amount);

  switch (unit) {
    case 'd': return num * 24 * 60 * 60 * 1000;
    case 'h': return num * 60 * 60 * 1000;
    case 'm': return num * 60 * 1000;
    default: return null;
  }
}

function isVip(jid) {
  if (!ROLES.VIP.includes(jid)) return false;
  const expiry = vipExpiry.get(jid);
  if (!expiry) return false;
  return Date.now() < expiry;
}

const COOLDOWN_TIME = 10 * 60 * 1000;
const commandCooldowns = new Map();

function checkCooldown(userId, command) {
  if (!commandCooldowns.has(userId)) {
    commandCooldowns.set(userId, new Map());
  }

  const userCooldowns = commandCooldowns.get(userId);
  const lastUsage = userCooldowns.get(command);
  const now = Date.now();

  const effectiveCooldown = isVip(userId) ? COOLDOWN_TIME / 2 : COOLDOWN_TIME;

  if (lastUsage && now - lastUsage < effectiveCooldown) {
    const remainingTime = Math.ceil((effectiveCooldown - (now - lastUsage)) / 1000);
    const minutes = Math.floor(remainingTime / 60);
    const seconds = remainingTime % 60;
    return `⏰ Bitte warte noch ${minutes}:${seconds.toString().padStart(2, '0')} Minuten.`;
  }

  userCooldowns.set(command, now);
  return null;
}

const load = f => {
  try { return JSON.parse(fs.readFileSync(path.join(DATA_PATH, f), 'utf8') || '{}'); } catch { return {}; }
};

const save = (f, d) => {
  if (typeof f !== 'string') {
    if (f?.file) {
      f = f.file;
    } else {
      console.error('❌ INVALID FILE:', f);
      console.trace();
      return;
    }
  }

  const filePath = path.join(DATA_PATH, f);
  fs.writeFileSync(filePath, JSON.stringify(d, null, 2));
};

const log = s => fs.appendFileSync(LOG_FILE, `[${new Date().toISOString()}] ${s}\n`);

const prettyRank = r => ({
  OWNER: '👑 Inhaber',
  COOWNER: '👑 Co-Inhaber',
  ADMIN: '🛡 Admin',
  MOD: '⚔ Moderator',
  VIP: '💎 VIP',
  SUPPORTER: '🌟 Supporter',
  TEST_SUPPORTER: '🔰 Test-Supporter',
  USER: '👤 Nutzer'
}[r] || '👤 Nutzer');

function normalizeJid(jid) {
  if (!jid) return jid;
  jid = String(jid);
  if (jid.startsWith('@')) jid = jid.substring(1);
  if (/^\d+$/.test(jid)) return `${jid}@s.whatsapp.net`;
  if (jid.includes('@')) return jid;
  const num = jid.replace(/\D+/g, '');
  return num ? `${num}@s.whatsapp.net` : jid;
}

function toParticipantJid(jid) {
  if (!jid) return jid;
  const n = normalizeJid(jid);
  if (!n) return n;
  if (n.endsWith('@s.whatsapp.net')) return n;
  if (n.endsWith('@lid')) {
    const num = n.replace(/\D+/g, '');
    return num ? `${num}@s.whatsapp.net` : n;
  }
  return n;
}

function isSameJid(a, b) {
  if (!a || !b) return false;
  return normalizeJid(a) === normalizeJid(b);
}

function normalizeDataKeys(obj) {
  const out = {};
  for (const k of Object.keys(obj || {})) {
    out[normalizeJid(k)] = obj[k];
  }
  return out;
}

function hasAdminPerms(jid) {
  return isAuthorized(jid, ['OWNER', 'COOWNER', 'ADMIN']);
}

function isAuthorized(jid, allowedRoles) {
  const normalizedJid = normalizeJid(jid);
  const role = ranks[normalizedJid] || users[normalizedJid]?.rank || 'USER';
  if (Array.isArray(allowedRoles) && allowedRoles.includes(role)) return true;
  return allowedRoles.some(role => ROLES[role]?.some(roleJid => isSameJid(normalizedJid, roleJid)));
}

let users = normalizeDataKeys(load(FILES.users.file));
let bans = normalizeDataKeys(load(FILES.bans.file));
let joinreqs = normalizeDataKeys(load(FILES.joinreq.file));
let pets = normalizeDataKeys(load(FILES.pets.file));
let tickets = normalizeDataKeys(load(FILES.tickets.file));
let ranks = normalizeDataKeys(load(FILES.ranks.file));

console.log('Loaded ranks:', ranks);

if (Object.keys(ranks).length === 0) {
  console.log('No ranks found, creating defaults...');
  ranks = {
    [normalizeJid(OWNER_LID)]: 'OWNER',
    [normalizeJid(COOWNER_LID)]: 'COOWNER'
  };
  save(FILES.ranks.file, ranks);
  console.log('Created default ranks:', ranks);
}

let groupSettings = normalizeDataKeys(load(FILES.groupSettings.file));
let ticketCounter = Object.keys(tickets).length;
let teamTodos = load(FILES.teamTodos.file) || {};
let todoCounter = Object.keys(teamTodos).length;
let groupInvites = load(FILES.groupInvites.file) || {};
let broadcastSettings = load(FILES.broadcastSettings.file);
let deletedUsers = normalizeDataKeys(load(FILES.deleted.file));
let ownerCfg = {};
try { ownerCfg = load(FILES.owner.file) || {}; } catch (e) { ownerCfg = {}; }
if (ownerCfg.ownerLid) OWNER_LID = ownerCfg.ownerLid;
if (ownerCfg.ownerPriv) OWNER_PRIV = ownerCfg.ownerPriv;
if (ownerCfg.coownerLid) COOWNER_LID = ownerCfg.coownerLid;

if (ownerCfg.roles) {
  Object.assign(ROLES, ownerCfg.roles);
} else {
  ROLES.OWNER = [OWNER_LID, OWNER_PRIV];
  ROLES.COOWNER = [COOWNER_LID];
}

function ensureUser(rawJid) {
  const jid = normalizeJid(rawJid);
  if (deletedUsers[jid]) return;
  if (!users[jid]) users[jid] = {
    xp: 0,
    level: 1,
    coins: 100,
    rank: 'USER',
    msgCount: 0,
    lastDaily: 0,
    items: {},
    registered: false,
    registrationDate: null,
    name: null
  };
  const normalizedJid = normalizeJid(jid);
  if (!ranks[normalizedJid]) {
    ranks[normalizedJid] = (isSameJid(jid, OWNER_LID) || isSameJid(jid, OWNER_PRIV)) ? 'OWNER' : (isSameJid(jid, COOWNER_LID) ? 'COOWNER' : 'USER');
  }
  save(FILES.users, users);
  save(FILES.ranks, ranks);
}

function isUserRegistered(jid) {
  const normalizedJid = normalizeJid(jid);
  return users[normalizedJid]?.registered === true;
}

function registerUser(jid, name) {
  const normalizedJid = normalizeJid(jid);
  if (!users[normalizedJid]) ensureUser(normalizedJid);
  users[normalizedJid].registered = true;
  users[normalizedJid].registrationDate = new Date().toISOString();
  users[normalizedJid].name = name;
  save(FILES.users, users);
}

function unregisterUser(jid) {
  const normalizedJid = normalizeJid(jid);
  if (users[normalizedJid]) {
    users[normalizedJid].registered = false;
    users[normalizedJid].registrationDate = null;
    save(FILES.users, users);
  }
}

function persistAll() {
  save(FILES.users, users);
  save(FILES.bans, bans);
  save(FILES.joinreq, joinreqs);
  save(FILES.pets, pets);
  save(FILES.tickets, tickets);
  save(FILES.ranks, ranks);
  save(FILES.broadcastSettings, broadcastSettings);
  save(FILES.deleted, deletedUsers);
  try {
    save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
  } catch (e) { console.error('Failed to save owner config:', e); }
}
setInterval(persistAll, 60_000);

// ========== GAME HELPERS ==========
const SLOT_SYMBOLS = ['🍒', '🍋', '🍇', '🍉', '⭐', '💎'];
function spinSlots() { return [SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)], SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)], SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)]]; }

const BJ_SUITS = ['♠', '♥', '♦', '♣'];
const BJ_VALUES = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
function bjDraw() { return { value: BJ_VALUES[randInt(0, BJ_VALUES.length - 1)], suit: BJ_SUITS[randInt(0, BJ_SUITS.length - 1)] }; }
function bjVal(card) { if (['J', 'Q', 'K'].includes(card.value)) return 10; if (card.value === 'A') return 11; return parseInt(card.value); }
function bjScore(hand) { let s = 0, ac = 0; for (const c of hand) { if (c.value === 'A') { ac++; s += 11; } else s += bjVal(c); } while (s > 21 && ac > 0) { s -= 10; ac--; } return s; }

// ========== START BOT ==========
async function startBot() {
  const sessionSockets = new Map();

  const AUTH_DIR = process.env.AUTH_DIR || path.join(SESSIONS_DIR, process.env.SESSION_NAME || 'default');

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();

  // FIX: printQRInTerminal korrekt geschrieben
  const sock = makeWASocket({
    version,
    logger: P({ level: 'silent' }),
    printQRInTerminal: true,
    auth: state
  });

  const groupMetaCache = new Map();
  const lastProcessed = new Map();
  const pendingActions = new Map();

  async function getGroupMetaSafe(jid) {
    if (!jid) {
      console.error('[groupMeta] Called with null/undefined jid');
      return null;
    }

    if (groupMetaCache.has(jid)) {
      return groupMetaCache.get(jid);
    }

    let attempt = 0;
    const maxAttempts = 3;

    while (attempt < maxAttempts) {
      try {
        let meta = await sock.groupMetadata(jid).catch(() => null);

        if (!meta) {
          const groups = await sock.groupFetchAllParticipating().catch(() => ({}));
          meta = groups[jid];
        }

        if (meta && typeof meta === 'object') {
          groupMetaCache.set(jid, meta);
          return meta;
        }

        attempt++;
        const wait = Math.min(500 * Math.pow(2, attempt), 5000);
        await sleep(wait);

      } catch (e) {
        const msg = String(e && e.message || '');
        console.error(`[groupMeta] Error:`, msg);
        attempt++;
        await sleep(1000);
      }
    }

    return null;
  }

  async function updateBotProfile() {
    try {
      await sock.updateProfileName('Sword art online bot');
      console.log('✅ Bot-Name wurde zu Sword art online bot geändert');

      const profilePath = './profil.jpg';
      if (fs.existsSync(profilePath)) {
        const profileImage = fs.readFileSync(profilePath);
        await sock.updateProfilePicture(sock.user.id, profileImage);
        console.log('✅ Profilbild wurde aktualisiert');
      }
    } catch (error) {
      console.error('❌ Fehler beim Aktualisieren des Profils:', error);
    }
  }

  // FIX: connection.update korrekt strukturiert mit richtigen Klammern
  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;

    // FIX: QR-Code an OWNER_PRIV senden (nicht `from` welches hier undefined wäre)
    if (qr) {
      console.log('📱 QR-Code wird generiert...');
      // Im Terminal anzeigen (Fallback immer verfügbar)
      QRCode.generate(qr, { small: true }); // FIX: `true` statt `info`

      // Als Bild an Owner senden
      try {
        const dataUrl = await QRCodeImg.toDataURL(qr, { type: 'image/png', scale: 4 });
        const base64 = dataUrl.split(',')[1];
        const qrBuffer = Buffer.from(base64, 'base64');
        await sock.sendMessage(OWNER_PRIV, { // FIX: OWNER_PRIV statt `from`
          image: qrBuffer,
          caption: '🤖 QR-Code zum Scannen mit WhatsApp'
        });
      } catch (err) {
        console.error('QR Bild-Senden fehlgeschlagen:', err);
        console.log('QR-Code wurde im Terminal angezeigt.');
      }
    }

    // FIX: Diese Blöcke sind jetzt AUSSERHALB des if(qr)-Blocks
    if (connection === 'open') {
      console.log('✅ Verbunden mit WhatsApp!');
    }

    if (connection === 'close') {
      console.log('⚠ Verbindung geschlossen — neu verbinden in 3s');
      setTimeout(() => startBot(), 3000);
    }
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('group-participants.update', async (update) => {
    try {
      const { id: groupId, participants, action } = update;

      if (!groupSettings[groupId]) {
        groupSettings[groupId] = {
          welcome: {
            enabled: false,
            message: 'Willkommen in der Gruppe {user}! 👋'
          }
        };
      }
      const settings = groupSettings[groupId];

      if (action === 'add' && settings.welcome.enabled) {
        const welcomeMsg = settings.welcome.message || 'Willkommen in der Gruppe! 👋';
        for (const participant of participants) {
          const formattedMsg = welcomeMsg.replace('{user}', '@' + participant.split('@')[0]);
          await sock.sendMessage(groupId, {
            text: formattedMsg,
            mentions: [participant]
          });
        }
      }
    } catch (err) {
      console.error('[Welcome] Error:', err);
    }
  });

  process.on('SIGINT', () => { persistAll(); process.exit(); });

  sock.ev.on('messages.upsert', async ({ messages }) => {
    try {
      if (!messages || !Array.isArray(messages) || messages.length === 0) return;
      const m = messages[0];
      if (!m || !m.message) return;

      const rawFrom = m.key.remoteJid;
      const rawParticipant = m.key.participant || m.key.remoteJid;
      const from = normalizeJid(rawFrom);
      const sender = normalizeJid(rawParticipant);
      const isGroup = typeof from === 'string' && from.endsWith('@g.us');

      const body = (m.message.conversation)
        || (m.message.extendedTextMessage && m.message.extendedTextMessage.text)
        || (m.message.imageMessage && m.message.imageMessage.caption)
        || '';

      const isCmd = !!(body && body.startsWith(PREFIX));

      if (isGroup && isCmd) {
        try {
          const meta = await getGroupMetaSafe(from);

          if (!meta) {
            try { await sock.sendMessage(from, { text: '⚠️ Ich kann meine Administrator-Rechte in dieser Gruppe nicht prüfen. Bitte mache mich zum Administrator.' }); } catch (e) {}
            return;
          }

          const BOT_LID = '32174136897540@lid';
          const rawNumber = (sock.user?.id || '').split(':')[0].split('@')[0].replace(/[^0-9]/g, '');
          const lidNumber = BOT_LID.split('@')[0];

          const possibleBotIds = [
            BOT_LID,
            sock.user?.id,
            sock.user?.jid,
            sock.user?.id?.split(':')[0],
            rawNumber ? `${rawNumber}@s.whatsapp.net` : null,
            rawNumber ? `${rawNumber}@c.us` : null,
            `${lidNumber}@s.whatsapp.net`,
          ].filter(Boolean);

          const allBotIds = [...new Set(possibleBotIds.flatMap(id => [
            id,
            id.split('@')[0],
            `${id.split('@')[0]}@s.whatsapp.net`,
            `${id.split('@')[0]}@c.us`
          ]))].map(String);

          const botPart = (meta.participants || []).find(p => {
            const pids = [
              p.id,
              p.id?.split('@')[0],
              `${p.id?.split('@')[0]}@s.whatsapp.net`,
            ].filter(Boolean).map(String);
            return pids.some(pid => allBotIds.includes(pid));
          });

          if (!botPart) {
            try { await sock.sendMessage(from, { text: '⚠️ Ich konnte meine Teilnahme in dieser Gruppe nicht verifizieren.' }); } catch (e) {}
            return;
          }

          const botIsAdmin = !!(
            botPart.admin === 'admin' ||
            botPart.admin === 'superadmin' ||
            botPart.admin === true ||
            botPart.isAdmin === true
          );

          if (!botIsAdmin) {
            try { await sock.sendMessage(from, { text: '⚠️ Ich bin kein Administrator in dieser Gruppe und kann keine Befehle ausführen.' }); } catch (e) {}
            return;
          }
        } catch (e) {
          console.error('[permissions] Error:', e);
          try { await sock.sendMessage(from, { text: '⚠️ Ich konnte meine Administrator-Rechte nicht prüfen.' }); } catch (err) {}
          return;
        }
      }

      if (body.toLowerCase().startsWith(PREFIX + 'register')) {
        const args = body.slice(PREFIX.length).trim().split(/\s+/);

        if (args.length === 1) {
          if (!isUserRegistered(sender)) {
            await sock.sendMessage(from, { text: DSGVO_TEXT + `\n\nUm sich zu registrieren:\n${PREFIX}register confirm IhrName` });
            return;
          } else {
            await sock.sendMessage(from, { text: 'Sie sind bereits registriert.' });
            return;
          }
        }

        if (args.length >= 3 && args[1].toLowerCase() === 'confirm') {
          if (!isUserRegistered(sender)) {
            const name = args.slice(2).join(' ');
            if (name.length < 2) {
              await sock.sendMessage(from, { text: 'Bitte geben Sie einen gültigen Namen ein.' });
              return;
            }
            registerUser(sender, name);
            await sock.sendMessage(from, { text: `Vielen Dank für Ihre Registrierung, ${name}! Sie können den Bot nun nutzen.` });
            return;
          } else {
            await sock.sendMessage(from, { text: 'Sie sind bereits registriert.' });
            return;
          }
        }
      }

      if (body.toLowerCase() === PREFIX + 'unregister') {
        if (isUserRegistered(sender)) {
          const userName = users[sender]?.name || 'Unbekannt';
          unregisterUser(sender);
          await sock.sendMessage(from, { text: `Auf Wiedersehen, ${userName}! Ihre Registrierung wurde erfolgreich gelöscht.` });
          return;
        } else {
          await sock.sendMessage(from, { text: 'Sie sind nicht registriert.' });
          return;
        }
      }

      if (body.toLowerCase() === PREFIX + 'backup') {
        if (ROLES.OWNER.includes(sender)) {
          try {
            await sock.sendMessage(sender, { text: 'Backup wird erstellt, bitte warten...' });
            const backupPath = await createBackup();
            await sock.sendMessage(sender, {
              document: fs.readFileSync(backupPath),
              mimetype: 'application/zip',
              fileName: path.basename(backupPath)
            });
            fs.unlinkSync(backupPath);
            await sock.sendMessage(sender, { text: 'Backup wurde erfolgreich erstellt und gesendet.' });
          } catch (error) {
            console.error('Backup error:', error);
            await sock.sendMessage(sender, { text: 'Fehler beim Erstellen des Backups: ' + error.message });
          }
          return;
        } else {
          await sock.sendMessage(from, { text: 'Dieser Befehl ist nur für den Bot-Inhaber verfügbar.' });
          return;
        }
      }

      if (!isUserRegistered(sender)) {
        if (isCmd) {
          await sock.sendMessage(from, { text: 'Bitte registrieren Sie sich zuerst mit dem Befehl ' + PREFIX + 'register.' });
          return;
        }
        return;
      }

      const msgTs = m.messageTimestamp || Date.now();
      try {
        const lastTs = lastProcessed.get(from) || 0;
        if (msgTs <= lastTs) return;
        lastProcessed.set(from, msgTs);
      } catch (e) {}

      if (deletedUsers[sender]) {
        try { await sock.sendMessage(from, { text: '🚫 Dein Account wurde vom Inhaber gelöscht und ist gesperrt.' }); } catch {}
        return;
      }

      if (bans[sender]) {
        try { await sock.sendMessage(from, { text: '🚫 Du bist gebannt.' }); } catch {}
        return;
      }

      ensureUser(sender);

      if (!m.key.fromMe) {
        users[sender].xp = (users[sender].xp || 0) + 5;
        users[sender].msgCount = (users[sender].msgCount || 0) + 1;
        const needed = 100 + (users[sender].level * 50);
        if (users[sender].xp >= needed) {
          users[sender].level = (users[sender].level || 1) + 1;
          users[sender].xp -= needed;
          try {
            await sock.sendMessage(from, {
              text: `🎉 Level-Up! @${sender.split('@')[0]} ist jetzt Level ${users[sender].level}`,
              mentions: [sender]
            });
          } catch (e) {}
        }
      }

      if (!body || !body.startsWith(PREFIX)) return;

      await sleep(150);

      const [cmdRaw, ...args] = body.trim().split(/\s+/);
      const rawCmd = cmdRaw.toLowerCase();
      const cmd = rawCmd.startsWith(PREFIX) ? rawCmd.slice(PREFIX.length) : rawCmd;

      const userRank = ranks[sender] || users[sender]?.rank || 'USER';
      const isOwner = isAuthorized(sender, ['OWNER', 'COOWNER']);
      const isCoOwner = isAuthorized(sender, ['COOWNER']);
      const isAdmin = isAuthorized(sender, ['ADMIN']);

      if (BOT_OFFLINE && !isOwner) {
        try { await sock.sendMessage(from, { text: '⚠️ Der Bot ist derzeit im Offline-Modus.' }); } catch (e) {}
        return;
      }

      const send = async (text, opts = {}) => {
        try { await sock.sendMessage(from, { text, ...opts }); } catch (e) { console.error('send failed', e); }
      };
      log(`${sender} -> ${body}`);

      // GETLID
      if (cmd === 'getlid') {
        let target = args[0];
        if (!target) {
          await sock.sendMessage(from, { text: 'Nutzung: #getlid <Nummer|@nutzer>' });
          return;
        }
        const ctx = m.message?.extendedTextMessage?.contextInfo;
        if (ctx && Array.isArray(ctx.mentionedJid) && ctx.mentionedJid.length) {
          target = ctx.mentionedJid[0];
        }
        const num = String(target).replace(/[^0-9]/g, '');
        const lid = num ? `${num}@lid` : 'Unbekannt';
        await sock.sendMessage(from, { text: `Die LID ist ${lid}` });
        return;
      }

      // HELP
      if (cmd === 'help') {
        let helpText = `🤖 *Bot Command Übersicht*\n\n`;

        helpText += `*📱 Basis-Befehle:*
${PREFIX}help - Zeigt diese Hilfe an
${PREFIX}ping - Prüft ob der Bot online ist
${PREFIX}whoami - Zeigt deine Nutzerinfo
${PREFIX}me - Zeigt deine Statistiken
${PREFIX}pet - Zeigt dein Haustier-Status
${PREFIX}daily - Tägliche Belohnung abholen
${PREFIX}blackjack - Starte ein Blackjack-Spiel
${PREFIX}adopt <name> - Adoptiere ein Haustier
${PREFIX}feed - Füttere dein Haustier
${PREFIX}fish - Gehe angeln

*💬 Chat & Gruppen:*
${PREFIX}gi - Gruppeneinstellungen anzeigen
${PREFIX}welcome-an - Welcome aktivieren
${PREFIX}welcome-aus - Welcome deaktivieren
${PREFIX}welcome-set <text> - Welcome-Text setzen
${PREFIX}hidetag - Nachricht mit verstecktem Tag\n\n`;

        if (isAuthorized(sender, ['OWNER', 'SUPPORTER', 'TEST_SUPPORTER'])) {
          helpText += `*🎫 Support-System:*
${PREFIX}answer <ticket-id> <text> - Ticket beantworten
${PREFIX}support <nachricht> - Support-Ticket erstellen\n\n`;
        }

        if (isAdmin) {
          helpText += `*⚔️ Admin-Befehle:*
${PREFIX}warn @user - Nutzer verwarnen
${PREFIX}kick @user - Nutzer entfernen
${PREFIX}promote @user - Zum Admin machen
${PREFIX}demote @user - Admin-Rechte entziehen
${PREFIX}addxp <@nutzer> <menge> - XP schenken
${PREFIX}addcash <@nutzer> <menge> - Coins schenken
${PREFIX}addvip <@nutzer> <zeit> - VIP-Status geben\n\n`;
        }

        if (hasAdminPerms(sender)) {
          helpText += `*👑 Owner Befehle:*
${PREFIX}broadcast <text> - Nachricht an alle Gruppen
${PREFIX}restart - Bot neu starten
${PREFIX}updateprofile - Profil aktualisieren
${PREFIX}setrole @user <rolle> - Nutzerrolle setzen
${PREFIX}listroles - Alle Rollen anzeigen\n\n`;
        }

        helpText += `_Tipp: Nutze die Befehle ohne Parameter für mehr Info_`;
        return send(helpText);
      }

      // Cooldown
      if (!isOwner && cmd !== 'help' && cmd !== 'menu') {
        const cooldownCommands = [
          'work', 'fish', 'slot', 'hunt', 'dig', 'crime', 'rob', 'daily', 'weekly', 'monthly',
          'collect', 'open', 'mine', 'farm', 'adventure', 'explore', 'quest', 'raid', 'train',
          'duel', 'gamble', 'casino', 'blackjack', 'rps', 'lottery', 'spin', 'loot'
        ];
        if (cooldownCommands.includes(cmd)) {
          const cooldownMessage = checkCooldown(sender, cmd);
          if (cooldownMessage) return send(cooldownMessage);
        }
      }

      // Group Settings (gi)
      if (cmd === 'gi' && isGroup) {
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = groupMetadata?.participants?.find(p => p.id === sender)?.admin;

        if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Du musst Admin in dieser Gruppe sein.');
        }

        if (!groupSettings[from]) {
          groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' } };
        }

        const settings = groupSettings[from];
        return send(
          `📋 *Gruppeneinstellungen*\n\n*Welcome:* ${settings.welcome.enabled ? '✅ An' : '❌ Aus'}\n*Text:*\n${settings.welcome.message}\n\n*Befehle:*\n$welcome-an / $welcome-aus / $welcome-set <text>`
        );
      }

      // Welcome Controls
      if ((cmd === 'welcome-an' || cmd === 'welcome-aus' || cmd === 'welcome-set') && isGroup) {
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = groupMetadata?.participants?.find(p => p.id === sender)?.admin;

        if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Du musst Admin in dieser Gruppe sein.');
        }

        if (!groupSettings[from]) {
          groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' } };
        }

        if (cmd === 'welcome-an') {
          groupSettings[from].welcome.enabled = true;
          save(FILES.groupSettings, groupSettings);
          return send('✅ Welcome-Nachricht aktiviert.');
        }

        if (cmd === 'welcome-aus') {
          groupSettings[from].welcome.enabled = false;
          save(FILES.groupSettings, groupSettings);
          return send('✅ Welcome-Nachricht deaktiviert.');
        }

        if (cmd === 'welcome-set') {
          if (!args.length) return send('❌ Beispiel: $welcome-set Willkommen {user}!');
          groupSettings[from].welcome.message = args.join(' ');
          save(FILES.groupSettings, groupSettings);
          return send(`✅ Welcome-Text gesetzt auf:\n${args.join(' ')}`);
        }
      }

      // Ticket answer
      if (cmd === 'answer' && isAuthorized(sender, ['OWNER', 'SUPPORTER', 'TEST_SUPPORTER']) && args.length >= 2) {
        const ticketId = args[0];
        const answer = args.slice(1).join(' ');

        if (from === SUPPORT_CONFIG.SUPPORT_GROUP) {
          if (!tickets[ticketId]) return send(`❌ Ticket #${ticketId} nicht gefunden.`);

          await sock.sendMessage(SUPPORT_CONFIG.SUPPORT_GROUP, {
            text: `📝 Antwort auf Ticket #${ticketId}:\n\n${answer}\n\nSupporter: @${sender.split('@')[0]}`,
            mentions: [sender]
          });

          tickets[ticketId].status = 'answered';
          tickets[ticketId].answeredBy = sender;
          tickets[ticketId].answer = answer;
          save(FILES.tickets, tickets);
          return send(`✅ Antwort für Ticket #${ticketId} gesendet.`);
        } else {
          return send('❌ Dieser Befehl kann nur in der Support-Gruppe verwendet werden.');
        }
      }

      // SETROLE
      if (cmd === 'setrole') {
        if (!isAuthorized(sender, ['OWNER'])) return send('❌ Nur für Owner.');

        const [role, ...jids] = args;
        if (!role || jids.length === 0) return send(`❌ Nutzung: $setrole <ROLLE> <jid1,jid2,...>`);

        const roleUpper = role.toUpperCase();
        if (!ROLES.hasOwnProperty(roleUpper)) return send(`❌ Ungültige Rolle. Verfügbar: ${Object.keys(ROLES).join(', ')}`);

        const jidList = jids.join(' ').split(',').map(j => j.trim());
        const validJids = jidList.filter(j => {
          const normalized = normalizeJid(j);
          return normalized && (normalized.endsWith('@s.whatsapp.net') || normalized.endsWith('@lid'));
        });

        if (!validJids.length) return send('❌ Keine gültigen JIDs gefunden.');

        ROLES[roleUpper] = validJids;
        try {
          save(FILES.owner, { ...ownerCfg, roles: ROLES, ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
        } catch (e) {
          return send('❌ Fehler beim Speichern.');
        }
        return send(`✅ ${validJids.length} JIDs der Rolle ${roleUpper} zugewiesen.`);
      }

      // LISTROLES
      if (cmd === 'listroles') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        let message = '📋 Rollen:\n\n';
        for (const [role, jids] of Object.entries(ROLES)) {
          message += `${role}: ${jids.length ? '\n' + jids.join('\n') : '(keine)'}\n\n`;
        }
        return send(message.trim());
      }

      // UPDATEPROFILE
      if (cmd === 'updateprofile') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        await send('🔄 Aktualisiere Bot-Profil...');
        await updateBotProfile();
        return send('✅ Profilaktualisierung abgeschlossen.');
      }

      // RESTART
      if (cmd === 'restart') {
        if (!hasAdminPerms(sender)) return send('❌ Kein Zugriff.');
        await send('🔄 Bot wird neugestartet...');
        try {
          fs.writeFileSync(RESTART_FILE, JSON.stringify({ timestamp: Date.now(), chatId: from, initiator: sender }));
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🔄 Bot-Neustart durch ${sender}` });
        } catch {}
        process.exit(0);
      }

      // WHOAMI / ME
      if (cmd === 'whoami' || cmd === 'me') {
        const normalizedSender = normalizeJid(sender);
        const r = ranks[normalizedSender] || users[normalizedSender]?.rank || '(none)';
        return send(`You: ${sender}\nNormalized: ${normalizedSender}\nRank: ${r}`);
      }

      // PING
      if (cmd === 'ping') {
        const startTime = Date.now();
        await send('🏓 Pong!');
        return send(`Antwortzeit: ${Date.now() - startTime}ms`);
      }

      // CODE
      if (cmd === 'code') {
        if (!isOwner) return send('❌ Nur der Inhaber darf diesen Befehl verwenden.');
        const target = args[0];
        if (!target) return send('❌ Nutzung: $code <pfad> [start-end]');

        const norm = path.normalize(target);
        if (norm.startsWith('..')) return send('❌ Zugriff verweigert.');

        const filePath = path.join(process.cwd(), norm);
        if (!fs.existsSync(filePath)) return send(`❌ Datei nicht gefunden: ${norm}`);

        let start = 1, end = Infinity;
        if (args[1]) {
          const lineMatch = String(args[1]).match(/(\d+)-(\d+)/);
          if (lineMatch) {
            start = Math.max(1, parseInt(lineMatch[1]));
            end = Math.max(start, parseInt(lineMatch[2]));
          }
        }

        try {
          const all = fs.readFileSync(filePath, 'utf8').split('\n');
          end = Math.min(end === Infinity ? all.length : end, all.length);
          const snippet = all.slice(start - 1, end).join('\n') || '(leer)';

          if (snippet.length > 1500) {
            const buf = Buffer.from(snippet, 'utf8');
            await sock.sendMessage(from, { document: buf, mimetype: 'text/plain', fileName: `${path.basename(filePath)}.txt` });
            return send(`✅ Code als Datei gesendet.`);
          }
          return send('```' + snippet + '```');
        } catch (e) {
          return send('❌ Fehler beim Lesen der Datei.');
        }
      }

      // BOTOFFLINE
      if (cmd === 'botoffline') {
        if (!isOwner) return send('❌ Nur der Inhaber.');
        const action = (args[0] || '').toLowerCase();
        if (!action || action === 'status') return send(`🔌 Offline-Modus: ${BOT_OFFLINE ? 'AN' : 'AUS'}`);
        if (['on', 'enable', 'true'].includes(action)) { BOT_OFFLINE = true; saveBotState(); return send('✅ Offline-Modus AN.'); }
        if (['off', 'disable', 'false'].includes(action)) { BOT_OFFLINE = false; saveBotState(); return send('✅ Bot wieder online.'); }
        if (action === 'toggle') { BOT_OFFLINE = !BOT_OFFLINE; saveBotState(); return send(`🔁 Offline-Modus: ${BOT_OFFLINE ? 'AN' : 'AUS'}`); }
        return send('❌ Nutzung: $botoffline on|off|toggle|status');
      }

      // NEWSESSION
      if (cmd === 'newsession') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('❌ Kein Zugriff.');
        const sessionName = args[0];
        if (!sessionName) return send('❌ Beispiel: $newsession meinbot');

        const authPath = path.join(SESSIONS_DIR, sessionName);
        if (!fs.existsSync(authPath)) fs.mkdirSync(authPath, { recursive: true });

        try {
          const { state: newState, saveCreds: newSaveCreds } = await useMultiFileAuthState(authPath);
          const { version: newVersion } = await fetchLatestBaileysVersion();
          const newSock = makeWASocket({
            version: newVersion,
            logger: P({ level: 'silent' }),
            printQRInTerminal: true, // FIX: korrekter Name
            auth: newState,
            browser: ['Sword-art-online-bot MultiSession', 'Chrome', '4.0.0'],
          });

          activeSessions.set(sessionName, newSock);

          newSock.ev.on('creds.update', newSaveCreds);

          newSock.ev.on('connection.update', async (upd) => {
            const { qr: newQr, connection: newConn } = upd;

            if (newQr) {
              try {
                const dataUrl = await QRCodeImg.toDataURL(newQr, { type: 'image/png', scale: 6 });
                const base64 = dataUrl.split(',')[1];
                const qrBuffer = Buffer.from(base64, 'base64');
                await sock.sendMessage(from, {
                  image: qrBuffer,
                  mimetype: 'image/png',
                  caption: `🤖 Neue Bot Session: ${sessionName}\nScanne den QR-Code.`
                });
              } catch (err) {
                console.error('QR send error:', err);
                try { await sock.sendMessage(from, { text: `QR für ${sessionName}:\n${newQr}` }); } catch (e) {}
              }
            }

            if (newConn === 'open') {
              const id = newSock.user?.id || '(unknown)';
              try { await sock.sendMessage(from, { text: `✅ Session "${sessionName}" angemeldet! JID: ${id}` }); } catch (e) {}
            }
          });
        } catch (err) {
          console.error('Session creation error:', err);
          return send('❌ Fehler beim Erstellen der Session.');
        }
        return;
      }

      // HIDETAG
      if (cmd === 'hidetag') {
        if (!isGroup) return send('❌ Nur in Gruppen.');
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('❌ Kein Zugriff.');
        const message = args.join(' ');
        if (!message) return send('❌ Beispiel: $hidetag Wichtige Ankündigung!');
        try {
          const groupMembers = await getGroupMetaSafe(from);
          const mentions = groupMembers.participants.map(p => p.id);
          await sock.sendMessage(from, { text: message, mentions }, { quoted: m });
        } catch (err) {
          return send('❌ Fehler beim Ausführen.');
        }
      }

      // BALANCE
      if (cmd === 'balance') {
        ensureUser(sender);
        const u = users[sender];
        return send(`💰 Coins: ${u.coins}\n⭐ Level: ${u.level}\nXP: ${u.xp}`);
      }

      // ADD XP
      if (cmd === 'addxp' && hasAdminPerms(sender)) {
        const target = args[0];
        const amount = parseInt(args[1]);
        if (!target || isNaN(amount) || amount < 0) return send('❌ Nutzung: $addxp <@nutzer> <menge>');
        const targetJid = normalizeJid(target);
        ensureUser(targetJid);
        users[targetJid].xp = (users[targetJid].xp || 0) + amount;
        save(FILES.users, users);
        return send(`✅ ${amount} XP an @${targetJid.split('@')[0]} geschenkt.`, { mentions: [targetJid] });
      }

      // ADD CASH
      if (cmd === 'addcash' && hasAdminPerms(sender)) {
        const target = args[0];
        const amount = parseInt(args[1]);
        if (!target || isNaN(amount) || amount < 0) return send('❌ Nutzung: $addcash <@nutzer> <menge>');
        const targetJid = normalizeJid(target);
        ensureUser(targetJid);
        users[targetJid].coins = (users[targetJid].coins || 0) + amount;
        save(FILES.users, users);
        return send(`✅ ${amount} Coins an @${targetJid.split('@')[0]} geschenkt.`, { mentions: [targetJid] });
      }

      // ADD VIP
      if (cmd === 'addvip' && hasAdminPerms(sender)) {
        const target = args[0];
        const duration = args[1];
        if (!target || !duration) return send('❌ Nutzung: $addvip <@nutzer> <1d|12h|30m>');
        const targetJid = normalizeJid(target);
        if (!addVip(targetJid, duration)) return send('❌ Ungültiges Zeitformat.');
        ensureUser(targetJid);
        const expiry = new Date(vipExpiry.get(targetJid)).toLocaleString();
        return send(`✅ VIP für @${targetJid.split('@')[0]} bis ${expiry}.`, { mentions: [targetJid] });
      }

      // FISH
      if (cmd === 'fish') {
        const events = [
          { chance: 30, text: '🐟 Kleinen Fisch gefangen! (+10 Coins)', coins: 10 },
          { chance: 20, text: '🐠 Tropenfisch gefangen! (+20 Coins)', coins: 20 },
          { chance: 10, text: '🐡 Kugelfisch gefangen! (+30 Coins)', coins: 30 },
          { chance: 5, text: '🦈 Kleinen Hai gefangen! (+100 Coins)', coins: 100 },
          { chance: 3, text: '📦 Schatztruhe gefunden! (+200 Coins)', coins: 200 },
          { chance: 2, text: '📜 Flaschenpost gefunden! (+50 Coins)', coins: 50, xp: 20 },
          { chance: 15, text: '💨 Köder gestohlen...' },
          { chance: 10, text: '😅 Nur Seegras gefangen.' },
          { chance: 4, text: '💦 Du bist ins Wasser gefallen!' },
          { chance: 1, text: '🌊 Welle hat Boot umgeworfen! (-50 Coins)', coins: -50 }
        ];

        const totalWeight = events.reduce((sum, e) => sum + e.chance, 0);
        let random = Math.random() * totalWeight;
        let selectedEvent = events[events.length - 1];
        for (const event of events) {
          random -= event.chance;
          if (random <= 0) { selectedEvent = event; break; }
        }

        ensureUser(sender);
        if (selectedEvent.coins) users[sender].coins = (users[sender].coins || 0) + selectedEvent.coins;
        if (selectedEvent.xp) users[sender].xp = (users[sender].xp || 0) + selectedEvent.xp;
        save(FILES.users, users);
        return send(selectedEvent.text);
      }

      // GIVE
      if (cmd === 'give') {
        const target = args[0];
        const amount = parseInt(args[1]);
        if (!target || isNaN(amount) || amount <= 0) return send('❌ Nutzung: $give <nummer|@mention> <betrag>');
        const targetJid = normalizeJid(target);
        if ((users[sender]?.coins || 0) < amount) return send('❌ Nicht genug Coins!');
        ensureUser(sender);
        ensureUser(targetJid);
        if (isSameJid(sender, targetJid)) return send('❌ Du kannst dir nicht selbst Coins geben!');
        users[sender].coins -= amount;
        users[targetJid].coins = (users[targetJid].coins || 0) + amount;
        save(FILES.users, users);
        try { await sock.sendMessage(targetJid, { text: `💰 Du hast ${amount} Coins von @${sender.split('@')[0]} erhalten!`, mentions: [sender] }); } catch (e) {}
        return send(`✅ ${amount} Coins an @${targetJid.split('@')[0]} gesendet!`, { mentions: [targetJid] });
      }

      // WORK
      if (cmd === 'work') {
        const earn = randInt(50, 200);
        users[sender].coins = (users[sender].coins || 0) + earn;
        users[sender].xp = (users[sender].xp || 0) + 20;
        save(FILES.users, users);
        return send(`🛠 Du hast ${earn} Coins verdient!`);
      }

      // DAILY
      if (cmd === 'daily') {
        const now = Date.now();
        const last = users[sender].lastDaily || 0;
        if (now - last < 24 * 3600 * 1000) {
          const hours = Math.floor((24 * 3600 * 1000 - (now - last)) / 3600000);
          return send(`🕒 Wieder in ca. ${hours} Stunden verfügbar.`);
        }
        const amount = randInt(1, 1000);
        users[sender].coins = (users[sender].coins || 0) + amount;
        users[sender].lastDaily = now;
        save(FILES.users, users);
        return send(`🎁 Daily: +${amount} Coins!`);
      }

      // SHOP
      const SHOP = { potion: { price: 100, desc: 'Heilt 10 HP' }, box: { price: 500, desc: 'Zufälliger Gegenstand' }, vip: { price: 2000, desc: 'VIP-Rang' } };
      if (cmd === 'shop') {
        let out = '🛒 Shop:\n';
        for (const [k, v] of Object.entries(SHOP)) out += `• ${k} — ${v.price} 💰 | ${v.desc}\n`;
        return send(out);
      }
      if (cmd === 'buy') {
        const item = args[0];
        if (!item || !SHOP[item]) return send('Nutzung: $buy <item>');
        if ((users[sender].coins || 0) < SHOP[item].price) return send('💸 Zu wenig Coins');
        users[sender].coins -= SHOP[item].price;
        users[sender].items[item] = (users[sender].items[item] || 0) + 1;
        save(FILES.users, users);
        return send(`✅ ${item} gekauft.`);
      }
      if (cmd === 'inventory') {
        const inv = users[sender].items || {};
        const out = Object.keys(inv).length ? Object.entries(inv).map(([k, v]) => `${k}: ${v}`).join('\n') : '(leer)';
        return send(`🎒 Inventar:\n${out}`);
      }
      if (cmd === 'use') {
        const it = args[0];
        if (!it) return send('Nutzung: $use <item>');
        if (!users[sender].items || !users[sender].items[it]) return send('Item nicht vorhanden');
        if (it === 'potion') {
          users[sender].items[it] -= 1;
          users[sender].xp = (users[sender].xp || 0) + 10;
          save(FILES.users, users);
          return send('💊 Trank verwendet: +10 XP');
        }
        if (it === 'box') {
          users[sender].items[it] -= 1;
          const coins = randInt(50, 300);
          users[sender].coins = (users[sender].coins || 0) + coins;
          save(FILES.users, users);
          return send(`🎁 Box geöffnet: +${coins} Coins`);
        }
        return send('Item verwendet.');
      }

      // SLOTS
      if (cmd === 'slot') {
        const bet = parseInt(args[0]) || 50;
        if ((users[sender].coins || 0) < bet) return send('Zu wenig Coins.');
        const spin = spinSlots();
        const win = spin[0] === spin[1] && spin[1] === spin[2];
        if (win) {
          users[sender].coins += bet * 3;
          users[sender].xp = (users[sender].xp || 0) + 50;
          save(FILES.users, users);
          return send(`🎰 | ${spin.join(' | ')} |\n🎉 Jackpot! +${bet * 3} Coins, +50 XP`);
        } else {
          users[sender].coins -= bet;
          save(FILES.users, users);
          return send(`🎰 | ${spin.join(' | ')} |\n😢 Verloren -${bet} Coins`);
        }
      }

      // RPS
      if (cmd === 'rps') {
        const choice = (args[0] || '').toLowerCase();
        const valid = ['rock', 'paper', 'scissors', 'stein', 'papier', 'schere'];
        if (!valid.includes(choice)) return send('Usage: $rps <rock|paper|scissors>');
        const norm = (choice === 'stein') ? 'rock' : (choice === 'papier') ? 'paper' : (choice === 'schere') ? 'scissors' : choice;
        const botOpt = ['rock', 'paper', 'scissors'][randInt(0, 2)];
        const draw = norm === botOpt;
        const win = (norm === 'rock' && botOpt === 'scissors') || (norm === 'paper' && botOpt === 'rock') || (norm === 'scissors' && botOpt === 'paper');
        let res = `🤖 Ich: ${botOpt}\nDu: ${norm}\n`;
        if (draw) res += 'Unentschieden 😐';
        else if (win) { users[sender].coins = (users[sender].coins || 0) + 50; users[sender].xp = (users[sender].xp || 0) + 10; save(FILES.users, users); res += 'Du gewinnst! +50 Coins +10 XP 🎉'; }
        else { users[sender].coins = Math.max(0, (users[sender].coins || 0) - 20); save(FILES.users, users); res += 'Du verlierst -20 Coins 😢'; }
        return send(res);
      }

      // BLACKJACK
      if (cmd === 'bjstart') {
        const player = [bjDraw(), bjDraw()];
        const dealer = [bjDraw(), bjDraw()];
        users[sender].bj = { player, dealer, active: true };
        save(FILES.users, users);
        return send(`🃏 Blackjack!\nDeine Karten: ${player.map(c => c.value + c.suit).join(', ')}\nDealer zeigt: ${dealer[0].value + dealer[0].suit}\nNutze $hit oder $stand`);
      }
      if (cmd === 'hit') {
        if (!users[sender].bj?.active) return send('Kein aktives Spiel. Starte mit $bjstart');
        const bj = users[sender].bj;
        bj.player.push(bjDraw());
        const p = bjScore(bj.player);
        const d = bjScore(bj.dealer);
        let out = `Deine Karten: ${bj.player.map(c => c.value + c.suit).join(', ')}\nPunkte: ${p}`;
        if (p > 21) {
          out += '\n😢 Bust! Du verloren.';
          delete users[sender].bj;
          save(FILES.users, users);
        }
        return send(out);
      }
      if (cmd === 'stand') {
        if (!users[sender].bj?.active) return send('Kein aktives Spiel. Starte mit $bjstart');
        const bj = users[sender].bj;
        const p = bjScore(bj.player);
        const d = bjScore(bj.dealer);
        let out = `Dealer: ${d}, Du: ${p}`;
        if (d > 21 || p > d) { users[sender].coins = (users[sender].coins || 0) + 75; users[sender].xp = (users[sender].xp || 0) + 40; out += '\n🎉 Du gewinnst! +75 Coins +40 XP'; }
        else if (p === d) out += '\nUnentschieden';
        else out += '\nDealer gewinnt';
        delete users[sender].bj;
        save(FILES.users, users);
        return send(out);
      }

      // PET SYSTEM
      if (cmd === 'adopt') {
        const type = (args[0] || '').toLowerCase();
        if (!['dog', 'cat', 'bird'].includes(type)) return send('Usage: $adopt <dog|cat|bird>');
        const name = args.slice(1).join(' ') || null;
        pets[sender] = { type, name, xp: 0, hunger: 100, happiness: 100, lastFed: Date.now() };
        save(FILES.pets, pets);
        return send(`🐾 ${type} ${name ? 'mit Namen ' + name : ''} adoptiert!`);
      }
      if (cmd === 'petinfo' || cmd === 'pet') {
        const p = pets[sender];
        if (!p) return send('Du hast kein Haustier. $adopt <dog|cat|bird>');
        return send(`🐶 ${p.type} ${p.name ? '- ' + p.name : ''}\nHunger: ${p.hunger}%\nGlück: ${p.happiness}%\nXP: ${p.xp}`);
      }
      if (cmd === 'feed') {
        const p = pets[sender];
        if (!p) return send('Du hast kein Haustier.');
        p.hunger = Math.min(100, (p.hunger || 0) + 20);
        p.happiness = Math.min(100, (p.happiness || 0) + 10);
        p.lastFed = Date.now();
        save(FILES.pets, pets);
        return send(`🍖 ${p.type} gefüttert. Hunger: ${p.hunger}% Glück: ${p.happiness}%`);
      }
      if (cmd === 'play') {
        const p = pets[sender];
        if (!p) return send('Du hast kein Haustier.');
        p.happiness = Math.min(100, (p.happiness || 0) + 20);
        p.xp = (p.xp || 0) + 5;
        save(FILES.pets, pets);
        return send(`🎾 Mit ${p.type} gespielt. Glück: ${p.happiness}% XP: ${p.xp}`);
      }

      // SUPPORT / TICKETS
      if (cmd === 'support' || cmd === 'ticket') {
        const text = args.join(' ') || 'Kein Text';
        ticketCounter++;
        const ticketId = ticketCounter.toString().padStart(4, '0');
        tickets[ticketId] = { id: ticketId, sender, message: text, status: 'open', timestamp: Date.now() };
        save(FILES.tickets, tickets);
        try {
          await sock.sendMessage(SUPPORT_CONFIG.TICKET_GROUP, {
            text: `🎫 Neues Ticket #${ticketId}\nVon: @${sender.split('@')[0]}\n\nNachricht:\n${text}`,
            mentions: [sender]
          });
          return send(`✅ Ticket #${ticketId} erstellt.`);
        } catch (e) {
          return send('❌ Fehler beim Erstellen des Tickets.');
        }
      }
      if (cmd === 'tickets') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('Kein Zugriff.');
        const list = Object.values(tickets).map(t => `${t.id} - ${t.status}`).join('\n') || '(keine)';
        return send(`🎫 Tickets:\n${list}`);
      }
      if (cmd === 'closeticket') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('Kein Zugriff.');
        const id = args[0];
        if (!id || !tickets[id]) return send('Usage: $closeticket <id>');
        tickets[id].status = 'closed';
        save(FILES.tickets, tickets);
        return send(`✅ Ticket ${id} geschlossen.`);
      }

      // TEAM TODOS
      if (cmd === 'todo' || cmd === 'todos') {
        const sub = (args[0] || '').toLowerCase();
        if (!sub || sub === 'list') {
          const all = Object.values(teamTodos);
          if (!all.length) return send('📝 Keine Team-Todos.');
          const now = Date.now();
          const lines = all.map(t => {
            let line = `${t.id} [${t.status}] - ${t.text}`;
            if (t.assignee) line += ` (→ @${t.assignee.split('@')[0]})`;
            if (t.deadline) {
              const days = Math.ceil((t.deadline - now) / (1000 * 60 * 60 * 24));
              line += ` [Fällig: ${new Date(t.deadline).toLocaleDateString('de-DE')} (${days > 0 ? `in ${days} Tagen` : days === 0 ? 'heute' : 'überfällig'})]`;
            }
            return line + (t.status === 'done' ? ' ✅' : '');
          });
          return send(`📝 Team-Todos:\n${lines.join('\n')}`);
        }
        if (sub === 'add') {
          const text = args.slice(1).join(' ');
          if (!text) return send('Usage: $todo add <text>');
          todoCounter++;
          const tdId = `TD${String(todoCounter).padStart(3, '0')}`;
          teamTodos[tdId] = { id: tdId, text, creator: sender, status: 'open', created: Date.now() };
          save(FILES.teamTodos, teamTodos);
          return send(`✅ Todo ${tdId} erstellt.`);
        }
        if (sub === 'done' || sub === 'complete') {
          const id = args[1] || args[0];
          if (!id || !teamTodos[id]) return send('Usage: $todo done <id>');
          teamTodos[id].status = 'done';
          teamTodos[id].doneBy = sender;
          save(FILES.teamTodos, teamTodos);
          return send(`✅ Todo ${id} erledigt.`);
        }
        if (sub === 'remove' || sub === 'rm') {
          if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('Kein Zugriff.');
          const id = args[1] || args[0];
          if (!id || !teamTodos[id]) return send('Usage: $todo remove <id>');
          delete teamTodos[id];
          save(FILES.teamTodos, teamTodos);
          return send(`🗑️ Todo ${id} entfernt.`);
        }
        return send('Usage: $todo add <text> | list | done <id> | remove <id>');
      }

      // MODERATION
      if (cmd === 'ban') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $ban <num|jid> [kick]');
        const jid = normalizeJid(t);
        const reason = args.slice(1).filter(a => a !== 'kick' && a !== 'remove').join(' ') || 'Kein Grund';
        bans[jid] = { by: sender, at: new Date().toISOString(), reason };
        save(FILES.bans, bans);
        if (args.includes('kick') || args.includes('remove')) {
          try {
            const groups = await sock.groupFetchAllParticipating();
            for (const gid of Object.keys(groups)) {
              try { await sock.groupParticipantsUpdate(gid, [jid], 'remove'); await sleep(200); } catch {}
            }
          } catch (e) {}
        }
        try { await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🚫 Gebannt: ${jid}\nDurch: ${sender}\nGrund: ${reason}` }); } catch {}
        return send(`🚫 ${jid} gebannt.`);
      }
      if (cmd === 'banlist') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('Kein Zugriff.');
        const list = Object.entries(bans).map(([j, b]) => `${j} — ${b.reason}`).join('\n') || '(keine)';
        return send(`🚫 Banliste:\n${list}`);
      }
      if (cmd === 'unban') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $unban <num|jid>');
        delete bans[normalizeJid(t)];
        save(FILES.bans, bans);
        return send(`✅ ${t} entbannt.`);
      }

      if (cmd === 'kick') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $kick <num|jid>');
        const participantJid = toParticipantJid(normalizeJid(t));
        if (!participantJid) return send('❌ Ungültige JID.');
        try { await sock.groupParticipantsUpdate(from, [participantJid], 'remove'); return send(`✅ ${participantJid} entfernt.`); } catch (e) { return send('❌ Kicken fehlgeschlagen.'); }
      }

      if (cmd === 'warn') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('Kein Zugriff.');
        const t = args[0]; const reason = args.slice(1).join(' ') || 'Kein Grund';
        if (!t) return send('Usage: $warn <num|jid> <grund>');
        const jid = normalizeJid(t);
        ensureUser(jid);
        users[jid].warns = users[jid].warns || [];
        users[jid].warns.push({ by: sender, reason, at: new Date().toISOString() });
        save(FILES.users, users);
        return send(`⚠ ${jid} verwarnt.`);
      }
      if (cmd === 'clearwarns') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $clearwarns <num|jid>');
        const jid = normalizeJid(t);
        if (users[jid]) users[jid].warns = [];
        save(FILES.users, users);
        return send(`✅ Warns entfernt für ${jid}`);
      }

      if (cmd === 'promote') {
        if (!isOwner) return send('Nur Owner/Co-Owner darf promoten.');
        const t = args[0]; if (!t) return send('Usage: $promote <num|jid>');
        const jid = normalizeJid(t);
        ranks[jid] = 'ADMIN'; save(FILES.ranks, ranks);
        return send(`✅ ${jid} zum ADMIN befördert.`);
      }
      if (cmd === 'demote') {
        if (!isOwner) return send('Nur Owner/Co-Owner darf demoten.');
        const t = args[0]; if (!t) return send('Usage: $demote <num|jid>');
        const jid = normalizeJid(t);
        ranks[jid] = 'USER'; save(FILES.ranks, ranks);
        return send(`✅ ${jid} demoted.`);
      }

 if (cmd === 'setrank') {
  if (!isOwner) return send('❌ Nur der Inhaber.');
  const r = (args[args.length - 1] || '').toUpperCase();
  if (!r) return send('Usage: $setrank <@mention|num|jid> <OWNER|COOWNER|ADMIN|MOD|VIP|USER>');

  const allowed = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'VIP', 'USER'];
  if (!allowed.includes(r)) return send('Ungültiger Rang.');

  // JID aus @-Markierung, Nummer oder direkt
  let jid;
  const mentioned = msg.message?.extendedTextMessage?.contextInfo?.mentionedJid;
  if (mentioned && mentioned.length > 0) {
    jid = mentioned[0]; // erste @-Markierung verwenden
  } else {
    jid = normalizeJid(args[0]);
  }

  if (!jid) return send('Usage: $setrank <@mention|num|jid> <OWNER|COOWNER|ADMIN|MOD|VIP|USER>');

  if (r === 'OWNER') {
    for (const k of Object.keys(ranks)) { if (ranks[k] === 'OWNER') ranks[k] = 'USER'; }
    ranks[jid] = 'OWNER'; OWNER_LID = jid;
    if (jid.endsWith('@s.whatsapp.net')) OWNER_PRIV = jid;
  } else if (r === 'COOWNER') {
    for (const k of Object.keys(ranks)) { if (ranks[k] === 'COOWNER') ranks[k] = 'USER'; }
    ranks[jid] = 'COOWNER'; COOWNER_LID = jid;
  } else {
    ranks[jid] = r;
  }

  try {
    save(FILES.ranks, ranks);
    save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
  } catch (e) {}

  return send(`✅ Rang von ${jid} auf ${r} gesetzt.`);
}

      if (cmd === 'datadelete') {
        if (!isOwner) return send('❌ Nur der Inhaber.');
        const t = args[0]; if (!t) return send('Usage: $datadelete <num|jid>');
        const jid = normalizeJid(t);
        delete users[jid]; delete pets[jid]; delete ranks[jid]; delete joinreqs[jid];
        for (const id of Object.keys(tickets)) {
          if (tickets[id]?.user && isSameJid(tickets[id].user, jid)) delete tickets[id];
        }
        deletedUsers[jid] = { by: sender, at: new Date().toISOString() };
        bans[jid] = { by: sender, at: new Date().toISOString(), reason: 'Data deleted by owner' };
        save(FILES.users, users); save(FILES.pets, pets); save(FILES.ranks, ranks);
        save(FILES.joinreq, joinreqs); save(FILES.tickets, tickets);
        save(FILES.deleted, deletedUsers); save(FILES.bans, bans);
        try { await sock.sendMessage(jid, { text: '🚫 Dein Account wurde gelöscht.' }); } catch (e) {}
        return send(`✅ Daten von ${jid} gelöscht.`);
      }

   if (cmd === 'selfpromote' || cmd === 'sp') {
  try {
    if (!from?.endsWith('@g.us')) return send('⚠ Nur in Gruppen.');
    if (sender !== ownerNumber) return send('⛔ Nur der Owner kann diesen Befehl nutzen.');
    await sock.groupParticipantsUpdate(from, [sender], 'promote');
    return send('🔰 Selfpromote ausgeführt.');
  } catch (e) {
    return send('❌ Selfpromote fehlgeschlagen.');
  }
}

// selfdemote
if (cmd === 'selfdemote' || cmd === 'sd') {
  try {
    if (!from?.endsWith('@g.us')) return send('⚠ Nur in Gruppen.');
    if (sender !== OWNER_LID) return send('⛔ Nur der Owner kann diesen Befehl nutzen.');
    await sock.groupParticipantsUpdate(from, [sender], 'demote');
    return send('🔱 Selfdemote ausgeführt.');
  } catch (e) {
    return send('❌ Selfdemote fehlgeschlagen.');
  }
}

      if (cmd === 'joinreq') {
        const link = args[0];
        if (!link) return send('Usage: $joinreq <link>');
        joinreqs[sender] = { link, at: new Date().toISOString() };
        save(FILES.joinreq, joinreqs);
        try { await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `📩 Joinrequest von ${sender}: ${link}` }); } catch {}
        return send('✅ Anfrage gesendet.');
      }
      if (cmd === 'join') {
        if (!(isOwner || isCoOwner)) return send('Kein Zugriff.');
        const link = args[0] || Object.values(joinreqs)[0]?.link;
        if (!link) return send('Kein Link gefunden.');
        const code = (link.match(/chat\.whatsapp\.com\/([A-Za-z0-9_-]+)/)?.[1]) || link;
        try { await sock.groupAcceptInvite(code); return send('✅ Erfolgreich beigetreten'); } catch (e) { return send('❌ Beitritt fehlgeschlagen'); }
      }
      if (cmd === 'leave') {
        if (!from?.endsWith('@g.us')) return send('Nur in Gruppen.');
        if (!(isOwner || isCoOwner)) return send('Kein Zugriff.');
        try {
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `Bot verlässt Gruppe: ${from}` });
          await sock.groupLeave(from);
        } catch (e) { return send('❌ Konnte Gruppe nicht verlassen.'); }
      }

      if (cmd === 'grouplist' || cmd === 'gl') {
        if (!isOwner) return send('Kein Zugriff.');
        try {
          const groups = await sock.groupFetchAllParticipating();
          let list = '📋 *Gruppenliste*\n\n';
          for (const [id, group] of Object.entries(groups)) {
            list += `*${group.subject || 'Unbekannt'}*\nID: ${id}\nMitglieder: ${group.participants?.length || 0}\n\n`;
          }
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: list });
          return send('📨 Gruppenliste privat zugeschickt.');
        } catch (error) {
          return send('❌ Fehler beim Abrufen der Gruppenliste.');
        }
      }

      if (cmd === 'broadcast') {
        if (!(isOwner || isCoOwner)) return send('Kein Zugriff.');
        const textMsg = args.join(' ');
        if (!textMsg) return send('Usage: $broadcast <text>');
        const chats = await sock.groupFetchAllParticipating();
        const gids = Object.keys(chats).filter(gid => broadcastSettings[gid] !== false);
        send(`📣 Broadcast an ${gids.length} Gruppen...`);
        for (const g of gids) { try { await sock.sendMessage(g, { text: `📣 Broadcast:\n${textMsg}` }); await sleep(300); } catch {} }
        return send('✅ Broadcast abgeschlossen.');
      }

      if (cmd === 'stats' || cmd === 'profile') {
        const u = users[sender];
        return send(`📊 Profil:\nLevel: ${u.level}\nXP: ${u.xp}\nCoins: ${u.coins}\nNachrichten: ${u.msgCount}`);
      }
      if (cmd === 'userinfo') {
        const t = args[0] ? normalizeJid(args[0]) : sender;
        ensureUser(t);
        const u = users[t];
        return send(`👤 ${t}\nLevel: ${u.level}\nXP: ${u.xp}\nCoins: ${u.coins}\nRank: ${ranks[t] || u.rank}`);
      }
      if (cmd === 'top') {
        const top = Object.entries(users).sort((a, b) => (b[1].level * 1000 + (b[1].xp || 0)) - (a[1].level * 1000 + (a[1].xp || 0))).slice(0, 10);
        let out = '🏆 Top Spieler\n';
        top.forEach(([jid, u], i) => out += `${i + 1}. ${jid.split('@')[0]} - Lv.${u.level} (${u.xp} XP)\n`);
        return send(out);
      }

      // YEETBAN
      if (cmd === 'yeetban') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('Kein Zugriff.');
        let target = args[0];
        try {
          const ctx = m.message?.extendedTextMessage?.contextInfo;
          if (!target && ctx?.participant) target = ctx.participant;
          if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
        } catch (e) {}
        if (!target) return send('Usage: $yeetban <num|jid>');
        const jid = normalizeJid(target);
        const reason = args.slice(1).join(' ') || 'Kein Grund';
        bans[jid] = { by: sender, at: new Date().toISOString(), reason };
        save(FILES.bans, bans);
        try {
          const groups = await sock.groupFetchAllParticipating();
          let removed = 0, failed = 0;
          for (const gid of Object.keys(groups)) {
            try {
              const meta = await getGroupMetaSafe(gid);
              if (!meta?.participants) { failed++; continue; }
              const rawJid = jid.split('@')[0];
              const targetParticipant = meta.participants.find(p => (p.id || '').split('@')[0] === rawJid);
              if (!targetParticipant) { failed++; continue; }
              await sock.groupParticipantsUpdate(gid, [targetParticipant.id], 'remove');
              await sleep(500);
              removed++;
            } catch (e) { failed++; }
          }
          return send(`✅ Yeetban: ${jid} — entfernt aus ${removed} Gruppen, fehlgeschlagen: ${failed}`);
        } catch (e) {
          return send('❌ Yeetban fehlgeschlagen.');
        }
      }

      // Unbekannter Befehl
      return send('❓ Unbekannter Befehl — $help für eine Liste der Befehle.');

    } catch (err) {
      console.error('messages.upsert error:', err);
      log(`ERROR: ${err?.message || String(err)}`);
    }
  });

  console.log('✅ Sword-art-online-bot gestartet.');
}

// ========== MAIN ==========
connectBot();
startBot();
