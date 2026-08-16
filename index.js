import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason, downloadMediaMessage } from '@whiskeysockets/baileys';
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
import { initTelegramConnect, setActiveSock, sendQrToTelegram } from './telegram-connect.js';
import { fileURLToPath } from 'url';
import webp from 'node-webpmux';
import 'dotenv/config';
import { createArenaSystem, ARENA_SHOP_ITEM, ARENA_HELP_TEXT, ARENA_COMMANDS, ITEM_DB as ARENA_ITEM_DB } from './arena-system.mjs';
import { createGuildSystem, GUILD_COMMANDS, GUILD_HELP_TEXT } from './guild-system.mjs';
import { createTitleSystem, TITLE_COMMANDS, TITLE_HELP_TEXT, checkProgress, TITLES } from './titel-achievments.js';
import { createPokemonSystem, POKEMON_COMMANDS, POKEMON_HELP_TEXT } from './pokemon-system.mjs';
import { createMenuSystem, MENU_COMMANDS } from './menu-system.mjs';
import crypto from 'crypto';
import { createAuthTools } from './web-auth.js';
import { createGameRoutes } from './web-games.js';
import { createGuildBossSystem } from './guildboss-event.mjs';
import { createGuildWarSystem, GUILDWAR_COMMANDS, GUILDWAR_HELP_TEXT } from './guildwars.mjs';
import { createDemonSlayerSystem } from './demonslayer-system.mjs';
import { createSoloLevelingSystem } from './sololeveling-system.mjs';

process.on('unhandledRejection', (reason) => {
  console.error('⚠️ Unhandled Rejection:', reason?.message || reason);
});

process.on('uncaughtException', (err) => {
  console.error('⚠️ Uncaught Exception:', err?.message || err);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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
  commandBans: { file: 'command-bans.json', default: {} },
  broadcastSettings: { file: 'broadcast-settings.json', default: {} },
  deleted: { file: 'deleted.json', default: {} },
  owner: { file: 'owner.json', default: {} },
  teamTodos: { file: 'team-todos.json', default: {} },
  userTodos: { file: 'user-todos.json', default: {} },
  groupInvites: { file: 'group-invites.json', default: {} },
  groupSettings: { file: 'group-settings.json', default: {} },
  credits: { file: 'credits.json', default: { list: [] } },
guilds: { file: 'guilds.json', default: {} },
marriages: { file: 'marriages.json', default: {} },
commandAllow: { file: 'command-allow.json', default: {} },
  officialGroup: { file: 'official-group.json', default: { link: 'https://chat.whatsapp.com/DBiDcF2s16FEWiGKyZA7Nl' } },
partners: { file: 'partners.json', default: { list: [] } },
groupLockSchedule: { file: 'group-lock-schedule.json', default: {} },
bitchkick: { file: 'bitchkick.json', default: {} }
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
3. Ich kann meine Einwillung jederzeit widerrufen (Befehl: ?unregister).
4. Meine Daten werden bei Widerruf gelöscht.

Zum Registrieren antworten Sie bitte mit "?register confirm".
`;

const saveRegisteredUsers = () => {
  fs.writeFileSync(USERS_FILE, JSON.stringify({ registeredUsers }, null, 2));
};

async function connectBot() {
  const { state, saveCreds } = await useMultiFileAuthState("./auth");

  const sock = makeWASocket({
    auth: state,
    logger: P({ level: 'silent' }),
    printQRInTerminal: false
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
const APPLICATION_STEPS = [
  { key: 'name', question: '⚔️ *— BEWERBUNG FÜR DIE GILDE —* ⚔️\n\nWillkommen, Schwertkämpfer! Beantworte der Reihe nach die folgenden Fragen, um dich zu bewerben.\n(Schreibe jederzeit "abbrechen", um die Bewerbung abzubrechen.)\n\n1️⃣ Wie lautet dein *Name*?' },
  { key: 'alter', question: '2️⃣ Wie *alt* bist du?' },
  { key: 'warum_bewerben', question: '3️⃣ *Warum* willst du dich bewerben?' },
  { key: 'warum_nehmen', question: '4️⃣ *Warum sollten wir dich nehmen?*' },
  { key: 'rang', question: '5️⃣ Welchen *Rang* strebst du an? (z.B. Supporter, Moderator, Admin...)' },
  { key: 'erfahrung', question: '6️⃣ Hast du bereits *Erfahrung als Teammitglied*? Wenn ja, wo?\n(z.B. "Ja, bei XY-Server" oder "Nein")' }
];

const SUPPORT_CONFIG = {
  TICKET_GROUP: '120363425785044232@g.us',
  SUPPORT_GROUP: '120363426001183575@g.us',
};

const JOIN_REQUEST_GROUP = '120363427405874233@g.us';

const owner = '27088878862400@lid';
let OWNER_LID = '27088878862400@lid';
let OWNER_LID2 = '256212616671466@lid';
let OWNER_PRIV = '4915111254435@s.whatsapp.net';
let OWNER_PRIV2 = '491793923329@s.whatsapp.net';
let COOWNER_LID = '126496081477863@lid';

ROLES.OWNER.push(OWNER_LID, OWNER_PRIV);
ROLES.COOWNER.push(COOWNER_LID);

const BOT_STATE_FILE = path.join(DATA_PATH, 'bot-state.json');
let BOT_OFFLINE = false;
let OWNER_MODE = false;
let PREFIX = '?';
try {
  if (fs.existsSync(BOT_STATE_FILE)) {
    const st = JSON.parse(fs.readFileSync(BOT_STATE_FILE, 'utf8') || '{}');
    BOT_OFFLINE = !!st.offline;
    OWNER_MODE = !!st.ownerMode;
    if (st.prefix && typeof st.prefix === 'string' && st.prefix.trim().length) {
      PREFIX = st.prefix.trim().slice(0, 1);
    }
  }
} catch (e) { console.error('Failed to load bot state:', e); }

const saveBotState = () => {
  try {
    fs.writeFileSync(BOT_STATE_FILE, JSON.stringify({ offline: !!BOT_OFFLINE, ownerMode: !!OWNER_MODE, prefix: PREFIX }, null, 2));
  } catch (e) { console.error('Failed to save bot state:', e); }
};
const _teamTodosPath = path.join(DATA_PATH, FILES.teamTodos.file);
if (!fs.existsSync(_teamTodosPath)) fs.writeFileSync(_teamTodosPath, '{}');

const _userTodosPath = path.join(DATA_PATH, FILES.userTodos.file);
if (!fs.existsSync(_userTodosPath)) fs.writeFileSync(_userTodosPath, '{}');

const _groupInvitesPath = path.join(DATA_PATH, FILES.groupInvites.file);
if (!fs.existsSync(_groupInvitesPath)) fs.writeFileSync(_groupInvitesPath, '{}');

const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleep = ms => new Promise(r => setTimeout(r, ms));

const arena = createArenaSystem();
const guildSystem = createGuildSystem();
const titleSystem = createTitleSystem();
const pokemonSystem = createPokemonSystem();
const menuSystem = createMenuSystem();
const guildBoss = createGuildBossSystem(DATA_PATH);
const guildWars = createGuildWarSystem(DATA_PATH);
const demonSlayer = createDemonSlayerSystem(DATA_PATH);
const soloLeveling = createSoloLevelingSystem(DATA_PATH);

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

  const normalizedJid = normalizeJid(jid);
  const expiry = Date.now() + duration;
  vipExpiry.set(normalizedJid, expiry);

  if (!ROLES.VIP.includes(normalizedJid)) {
    ROLES.VIP.push(normalizedJid);
  }

  const currentRank = ranks[normalizedJid] || users[normalizedJid]?.rank || 'USER';
  if (currentRank === 'USER') {
    ranks[normalizedJid] = 'VIP';
    if (users[normalizedJid]) users[normalizedJid].rank = 'VIP';
    save(FILES.ranks, ranks);
    save(FILES.users, users);
  }

  setTimeout(() => {
    ROLES.VIP = ROLES.VIP.filter(id => id !== normalizedJid);
    vipExpiry.delete(normalizedJid);

    const stillVip = (ranks[normalizedJid] || users[normalizedJid]?.rank) === 'VIP';
    if (stillVip) {
      ranks[normalizedJid] = 'USER';
      if (users[normalizedJid]) users[normalizedJid].rank = 'USER';
      save(FILES.ranks, ranks);
      save(FILES.users, users);
    }
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
const COOLDOWN_EXCLUDED = ['pokeshop'];
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

import { createStore } from './db.js';
const { load, save } = createStore(DATA_PATH);

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

function normalizeNumber(input) {
  const num = String(input || '').replace(/[^0-9]/g, '');
  if (!num) return null;
  return `${num}@s.whatsapp.net`;
}

function normalizeTicketId(id) {
  if (!id) return id;
  const ticketId = String(id).trim();
  if (/^\d{1,4}$/.test(ticketId)) return ticketId.padStart(4, '0');
  return ticketId;
}

function getTicketById(id) {
  const normalized = normalizeTicketId(id);
  return tickets[normalized] || tickets[id];
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

function toLidJid(jid) {
  if (!jid) return jid;
  const n = normalizeJid(jid);
  if (!n) return n;
  if (n.endsWith('@lid')) return n;
  const num = n.replace(/\D+/g, '');
  return num ? `${num}@lid` : n;
}

function isSameJid(a, b) {
  if (!a || !b) return false;
  return normalizeJid(a) === normalizeJid(b);
}
function isSenderGroupAdmin(groupMetadata, senderJid) {
  const rawSender = senderJid.split('@')[0];
  const p = groupMetadata?.participants?.find(p =>
    isSameJid(p.id, senderJid) || (p.id || '').split('@')[0] === rawSender
  );
  return p?.admin === 'admin' || p?.admin === 'superadmin';
}
function extractRawNumber(jid) {
  if (!jid) return null;
  return String(jid).split(':')[0].split('@')[0].replace(/[^0-9]/g, '') || null;
}

function isGroupAdminJid(groupMeta, jid) {
  if (!groupMeta?.participants || !jid) return false;
  const rawNum = extractRawNumber(jid);
  if (!rawNum) return false;
  const participant = groupMeta.participants.find(p => extractRawNumber(p.id) === rawNum);
  if (!participant) return false;
  return !!(
    participant.admin === 'admin' ||
    participant.admin === 'superadmin' ||
    participant.admin === true ||
    participant.isAdmin === true
  );
}

function isBotAdminInGroup(groupMeta, sock) {
  if (!groupMeta?.participants) return false;
  const allBotIds = [...getBotSelfIds(sock)];
  const botPart = groupMeta.participants.find(p => {
    const pids = [
      p.id,
      p.id?.split('@')[0],
      `${p.id?.split('@')[0]}@s.whatsapp.net`,
    ].filter(Boolean).map(String);
    return pids.some(pid => allBotIds.includes(pid));
  });
  return !!(
    botPart?.admin === 'admin' ||
    botPart?.admin === 'superadmin' ||
    botPart?.admin === true ||
    botPart?.isAdmin === true
  );
}

function getBotSelfIds(sock) {
  const ids = new Set();

  const add = (value) => {
    if (!value) return;
    const s = String(value);
    ids.add(s);

    const num = s.split(':')[0].split('@')[0].replace(/[^0-9]/g, '');
    if (num) {
      ids.add(num);
      ids.add(`${num}@s.whatsapp.net`);
      ids.add(`${num}@lid`);
      ids.add(`${num}@c.us`);
    }
  };

  add(sock?.user?.id);
  add(sock?.user?.lid);
  add(sock?.user?.jid);
  add(sock?.authState?.creds?.me?.id);
  add(sock?.authState?.creds?.me?.lid);

  return ids;
}

function findAfkKey(rawJid) {
  const candidates = new Set(
    [normalizeJid(rawJid), toParticipantJid(rawJid), toLidJid(rawJid)].filter(Boolean)
  );
  for (const candidate of candidates) {
    if (users[candidate]?.afk) return candidate;
  }
  return null;
}

function normalizeDataKeys(obj) {
  const out = {};
  for (const k of Object.keys(obj || {})) {
    out[normalizeJid(k)] = obj[k];
  }
  return out;
}

function getGroupPrefix(groupJid) {
  const normalized = normalizeJid(groupJid);
  return groupSettings[normalized]?.prefix || PREFIX;
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
let joinreqs = load(FILES.joinreq.file) || {};
let groupLockSchedules = load(FILES.groupLockSchedule.file) || {};
let partners = load(FILES.partners.file) || { list: [] };
let pets = normalizeDataKeys(load(FILES.pets.file));
let tickets = normalizeDataKeys(load(FILES.tickets.file));
let ranks = normalizeDataKeys(load(FILES.ranks.file));
let guilds = normalizeDataKeys(load(FILES.guilds.file));
let marriages = normalizeDataKeys(load(FILES.marriages.file));
let commandBans = load(FILES.commandBans.file) || {};
let commandAllow = load(FILES.commandAllow.file) || {};
let credits = load(FILES.credits.file) || { list: [] };
let officialGroup = load(FILES.officialGroup.file) || { link: 'https://chat.whatsapp.com/DBiDcF2s16FEWiGKyZA7Nl' };
if (!officialGroup.link) officialGroup.link = 'https://chat.whatsapp.com/DBiDcF2s16FEWiGKyZA7Nl';

let bitchkickData = load(FILES.bitchkick.file) || {};

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
let joinReqCounter = Object.keys(joinreqs).length;
let teamTodos = load(FILES.teamTodos.file) || {};
let todoCounter = Object.keys(teamTodos).length;
let userTodos = load(FILES.userTodos.file) || {};
let userTodoCounter = Object.keys(userTodos).length;
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

const PRIMARY_OWNER_IDS = new Set(
  [OWNER_LID, OWNER_PRIV].filter(Boolean).map(normalizeJid)
);

function isPrimaryOwner(jid) {
  const n = normalizeJid(jid);
  return n ? PRIMARY_OWNER_IDS.has(n) : false;
}

function protectPrimaryOwner() {
  for (const jid of PRIMARY_OWNER_IDS) {
    ranks[jid] = 'OWNER';
    if (users[jid]) users[jid].rank = 'OWNER';
    if (!ROLES.OWNER.some(id => isSameJid(id, jid))) ROLES.OWNER.push(jid);
  }
}
protectPrimaryOwner();
function generateWebId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let id;
  do {
    id = '';
    for (let i = 0; i < 6; i++) {
      id += chars[Math.floor(Math.random() * chars.length)];
    }
  } while (Object.values(users).some(u => u.webId === id));
  return id;
}

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
}

function setUserWebPassword(jid, password) {
  const normalizedJid = normalizeJid(jid);
  ensureUser(normalizedJid);
  const u = users[normalizedJid];
  if (!u.webId) {
    u.webId = generateWebId();
  }
  const salt = crypto.randomBytes(16).toString('hex');
  u.webPasswordSalt = salt;
  u.webPasswordHash = hashPassword(password, salt);
  u.webPasswordSetAt = new Date().toISOString();
  save(FILES.users, users);
  return u.webId;
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
    name: null,
    alter: null,
    hobbys: null,
    sexualitaet: null
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

function getMentionDisplay(jid, contacts = {}) {
  const normalizedJid = normalizeJid(jid);
  const user = users[normalizedJid];
  let display = user?.name || user?.registrationName;
  const contact = contacts[normalizedJid];
  if (!display && contact) {
    display = contact.notify || contact.name || contact.vname || contact.short || contact.formattedName;
  }
  return `@${String(display || normalizedJid.split('@')[0]).replace(/\n/g, ' ').trim()}`;
}

async function resolvePhoneJid(jid, sock) {
  const n = normalizeJid(jid);
  if (!n) return null;
  if (n.endsWith('@s.whatsapp.net')) return n;
  if (n.endsWith('@lid')) {
    try {
      const pn = await sock?.signalRepository?.lidMapping?.getPNForLID(n);
      if (pn) {
        const num = String(pn).split('@')[0].split(':')[0].replace(/[^0-9]/g, '');
        if (num) return `${num}@s.whatsapp.net`;
      }
    } catch (e) {}
  }
  return null;
}

async function getNumberMention(jid, sock) {
  const resolved = await resolvePhoneJid(jid, sock);
  if (resolved) return `@${resolved.split('@')[0]}`;
  return '@Nutzer';
}

function findUserJidByName(name) {
  const target = String(name || '').trim().toLowerCase();
  if (!target) return null;
  for (const [jid, u] of Object.entries(users)) {
    const uname = u?.name || u?.registrationName;
    if (uname && String(uname).trim().toLowerCase() === target) return jid;
  }
  return null;
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
  save(FILES.partners, partners);
  save(FILES.broadcastSettings, broadcastSettings);
  save(FILES.guilds, guilds);
save(FILES.deleted, deletedUsers);
  save(FILES.credits, credits);
save(FILES.marriages, marriages);
  save(FILES.groupLockSchedule, groupLockSchedules);
save(FILES.officialGroup, officialGroup);
save(FILES.bitchkick, bitchkickData);
  try {
    save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
  } catch (e) { console.error('Failed to save owner config:', e); }
}
setInterval(persistAll, 60_000);
const groupMessageHistory = new Map();
const MAX_TRACKED_PER_GROUP = 2000;

function trackGroupMessage(groupJid, msgId, participant) {
  if (!groupMessageHistory.has(groupJid)) {
    groupMessageHistory.set(groupJid, []);
  }
  const arr = groupMessageHistory.get(groupJid);
  arr.push({ id: msgId, participant });
  if (arr.length > MAX_TRACKED_PER_GROUP) {
    arr.splice(0, arr.length - MAX_TRACKED_PER_GROUP);
  }
}
function loadBannedCommands() {
  try {
    if (!fs.existsSync(BANNED_CMDS_FILE)) return [];
    return JSON.parse(fs.readFileSync(BANNED_CMDS_FILE, 'utf8'));
  } catch (e) {
    console.error('[bancmd] Fehler beim Laden:', e.message);
    return [];
  }
}

function saveBannedCommands(list) {
  fs.mkdirSync(path.dirname(BANNED_CMDS_FILE), { recursive: true });
  fs.writeFileSync(BANNED_CMDS_FILE, JSON.stringify(list, null, 2));
}

function isCommandBanned(cmdName) {
  const banned = loadBannedCommands();
  return banned.includes(cmdName.toLowerCase());
}

const SLOT_SYMBOLS = ['🍒', '🍋', '🍇', '🍉', '⭐', '💎'];
function spinSlots() { return [SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)], SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)], SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)]]; }

const BJ_SUITS = ['♠', '♥', '♦', '♣'];
const BJ_VALUES = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
function bjDraw() { return { value: BJ_VALUES[randInt(0, BJ_VALUES.length - 1)], suit: BJ_SUITS[randInt(0, BJ_SUITS.length - 1)] }; }
function bjVal(card) { if (['J', 'Q', 'K'].includes(card.value)) return 10; if (card.value === 'A') return 11; return parseInt(card.value); }
function bjScore(hand) { let s = 0, ac = 0; for (const c of hand) { if (c.value === 'A') { ac++; s += 11; } else s += bjVal(c); } while (s > 21 && ac > 0) { s -= 10; ac--; } return s; }

const RARITY_INFO = {
  common:    { label: 'Gewöhnlich', emoji: '⚪', weight: 45 },
  uncommon:  { label: 'Ungewöhnlich', emoji: '🟢', weight: 30 },
  rare:      { label: 'Selten', emoji: '🔵', weight: 15 },
  epic:      { label: 'Episch', emoji: '🟣', weight: 8 },
  legendary: { label: 'Legendär', emoji: '🟡', weight: 2 },
  secret:    { label: 'Geheim', emoji: '⚫', weight: 0 }
};
const ITEM_DB = {
  w_common_1:    { name: 'Rostiges Schwert',        type: 'weapon', rarity: 'common',    power: 8 },
  w_common_2:    { name: 'Holzstab',                type: 'weapon', rarity: 'common',    power: 6 },
  w_common_3:    { name: 'Alter Dolch',             type: 'weapon', rarity: 'common',    power: 7 },
  w_uncommon_1:  { name: 'Stahlschwert',            type: 'weapon', rarity: 'uncommon',  power: 16 },
  w_uncommon_2:  { name: 'Kampfaxt',                type: 'weapon', rarity: 'uncommon',  power: 18 },
  w_uncommon_3:  { name: 'Kurzbogen',               type: 'weapon', rarity: 'uncommon',  power: 15 },
  w_rare_1:      { name: 'Silberklinge',            type: 'weapon', rarity: 'rare',      power: 28 },
  w_rare_2:      { name: 'Kristalldolch',           type: 'weapon', rarity: 'rare',      power: 26 },
  w_rare_3:      { name: 'Kriegshammer',            type: 'weapon', rarity: 'rare',      power: 30 },
  w_epic_1:      { name: 'Nachtschattenklinge',     type: 'weapon', rarity: 'epic',      power: 45 },
  w_epic_2:      { name: 'Flammenschwert',          type: 'weapon', rarity: 'epic',      power: 48 },
  w_epic_3:      { name: 'Sturmspeer',              type: 'weapon', rarity: 'epic',      power: 46 },
  w_legendary_1: { name: 'Elucidator',              type: 'weapon', rarity: 'legendary', power: 70 },
  w_legendary_2: { name: 'Dark Repulser',           type: 'weapon', rarity: 'legendary', power: 68 },
  w_legendary_3: { name: 'Lambent Light',           type: 'weapon', rarity: 'legendary', power: 66 },

  w_excalibur: {
    name: 'Excalibur',
    type: 'weapon',
    rarity: 'secret',
    power: 10000,
    secret: true,
    ownerOnly: true
  },
  w_ragnarok: {
    name: 'Ragnarok',
    trueName: 'Die Klinge der Götterdämmerung',
    type: 'weapon',
    rarity: 'legendary',
    power: 170,
    secret: true,
    bossBonus: 0.30
  },

  w_secret_dualblades: {
    name: 'Holzstab',
    trueName: 'Kiritos Doppelklingen (Dual Blades)',
    type: 'weapon',
    rarity: 'legendary',
    power: 150,
    secret: true
  },
  w_secret_liberator: {
    name: 'Rostiger schwarzer Rapier',
    trueName: 'Liberator — Asunas Rapier aus Neu-Aincrad',
    type: 'weapon',
    rarity: 'legendary',
    power: 130,
    secret: true
  },
  w_secret_lightflash: {
    name: 'Abgenutztes Übungsrapier',
    trueName: 'Lightning Flash — Asunas erstes Rapier',
    type: 'weapon',
    rarity: 'epic',
    power: 145,
    secret: true
  },
  w_secret_bluerose: {
    name: 'Zersplitterte blaue Klinge',
    trueName: 'Blue Rose Sword',
    type: 'weapon',
    rarity: 'legendary',
    power: 135,
    secret: true
  },
  w_secret_nightsky: {
    name: 'Schwarzes Schattenschwert',
    trueName: 'Night Sky Sword — Klinge des Dunklen Ritters',
    type: 'weapon',
    rarity: 'legendary',
    power: 140,
    secret: true
  },
  w_secret_holyblade: {
    name: 'Verzierte gesegnete Klinge',
    trueName: 'Heathcliffs Heilige Klinge — Rache des Systemadministrators',
    type: 'weapon',
    rarity: 'legendary',
    power: 155,
    secret: true
  },

  a_common_1:    { name: 'Lederrüstung',            type: 'armor',  rarity: 'common',    power: 8 },
  a_common_2:    { name: 'Stoffmantel',             type: 'armor',  rarity: 'common',    power: 6 },
  a_common_3:    { name: 'Einfacher Schild',        type: 'armor',  rarity: 'common',    power: 7 },
  a_uncommon_1:  { name: 'Kettenhemd',              type: 'armor',  rarity: 'uncommon',  power: 16 },
  a_uncommon_2:  { name: 'Verstärkte Weste',        type: 'armor',  rarity: 'uncommon',  power: 18 },
  a_uncommon_3:  { name: 'Eisenschild',             type: 'armor',  rarity: 'uncommon',  power: 15 },
  a_rare_1:      { name: 'Silberharnisch',          type: 'armor',  rarity: 'rare',      power: 28 },
  a_rare_2:      { name: 'Drachenschuppen-Umhang',  type: 'armor',  rarity: 'rare',      power: 30 },
  a_rare_3:      { name: 'Kristallschild',          type: 'armor',  rarity: 'rare',      power: 26 },
  a_epic_1:      { name: 'Nachtschatten-Rüstung',   type: 'armor',  rarity: 'epic',      power: 46 },
  a_epic_2:      { name: 'Phönixmantel',            type: 'armor',  rarity: 'epic',      power: 45 },
  a_epic_3:      { name: 'Titanplatte',             type: 'armor',  rarity: 'epic',      power: 48 },
  a_legendary_1: { name: 'Coat of Midnight',        type: 'armor',  rarity: 'legendary', power: 70 },
  a_legendary_2: { name: 'Rune des Kobold-Königs',  type: 'armor',  rarity: 'legendary', power: 68 },
  a_legendary_3: { name: 'Himmlischer Panzer',      type: 'armor',  rarity: 'legendary', power: 66 },

  a_aegis: {
    name: 'Aegis des Systemadministrators',
    type: 'armor',
    rarity: 'legendary',
    power: 92,
    secret: true,
    ownerOnly: true
  },

  a_secret_bwcoat: {
    name: 'Zerschlissener schwarzer Mantel',
    trueName: 'Blackwyrm Coat',
    type: 'armor',
    rarity: 'legendary',
    power: 120,
    secret: true
  },
  a_secret_negacloak: {
    name: 'Nachtschwarzer Umhang',
    trueName: 'Umhang der Laughing Coffin',
    type: 'armor',
    rarity: 'legendary',
    power: 125,
    secret: true
  },
  a_secret_flashcoat: {
    name: 'Zerrissene rote Weste',
    trueName: 'Flash-Panzerung — Asunas Kommandantinnen-Rüstung',
    type: 'armor',
    rarity: 'legendary',
    power: 130,
    secret: true
  },
  a_secret_bloodoath: {
    name: 'Rostiger Plattenpanzer',
    trueName: 'Rüstung der Blutschwur-Ritter (Knights of the Blood Oath)',
    type: 'armor',
    rarity: 'legendary',
    power: 128,
    secret: true
  }
};
import express from 'express';
import cors from 'cors';

const webApi = express();
webApi.use(cors());
webApi.use(express.json());

webApi.use(express.static(path.join(__dirname, 'public')));

const { signToken, authenticateToken } = createAuthTools(DATA_PATH);

webApi.post('/api/login', (req, res) => {
  const { id, password } = req.body || {};
  if (!id || !password) {
    return res.status(400).json({ success: false, error: 'ID und Passwort erforderlich.' });
  }

  const entry = Object.entries(users).find(([, u]) => u.webId === String(id).toUpperCase());
  if (!entry) {
    return res.status(401).json({ success: false, error: 'ID nicht gefunden.' });
  }

  const [, user] = entry;
  if (!user.webPasswordHash || !user.webPasswordSalt) {
    return res.status(401).json({ success: false, error: 'Für diese ID wurde noch kein Passwort gesetzt.' });
  }

  const hash = hashPassword(password, user.webPasswordSalt);
  if (hash !== user.webPasswordHash) {
    return res.status(401).json({ success: false, error: 'Falsches Passwort.' });
  }

  const token = signToken(user.webId);
  return res.json({ success: true, token, id: user.webId, name: user.name || null });
});

webApi.use('/api/games', authenticateToken, createGameRoutes({ users, save, FILES }));
webApi.get('/api/me', authenticateToken, (req, res) => {
  const entry = Object.entries(users).find(([, u]) => u.webId === req.webId);
  if (!entry) return res.status(404).json({ success: false, error: 'Nutzer nicht gefunden.' });
  const u = entry[1];
  res.json({ success: true, coins: u.coins || 0, level: u.level || 1, xp: u.xp || 0, name: u.name || null });
});

const WEB_API_PORT = process.env.WEB_API_PORT || 3001;
webApi.listen(WEB_API_PORT, () => {
  console.log(`✅ Web-Login-API läuft auf Port ${WEB_API_PORT}`);
});

async function startBot(sessionName = 'default', hooks = {}) {
  if (activeSessions.has(sessionName)) {
    console.log(chalk.yellow(`⚠ Session "${sessionName}" läuft bereits.`));
    return activeSessions.get(sessionName);
  }

  const AUTH_DIR = path.join(SESSIONS_DIR, sessionName);
  ensureDir(AUTH_DIR);

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    logger: P({ level: 'silent' }),
    printQRInTerminal: true,
    auth: state
  });

  activeSessions.set(sessionName, sock);

  if (sessionName === 'default') {
    setActiveSock(sock);
  }
sock.ev.on('creds.update', saveCreds);

const pendingMarriageProposals = new Map();
const pendingApplications = new Map();
  const groupMetaCache = new Map();
  const lastProcessed = new Map();
  const pendingActions = new Map();

  async function getGroupMetaSafe(jid, forceRefresh = false) {
    if (!jid) {
      console.error('[groupMeta] Called with null/undefined jid');
      return null;
    }

    if (!forceRefresh && groupMetaCache.has(jid)) {
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

setInterval(async () => {
  try {
    await guildBoss.checkExpiry({
      send: async (text, opts) => { try { await sock.sendMessage(OWNER_PRIV, { text, ...opts }); } catch (e) {} },
      sock, users, guilds, save, FILES, getNumberMention,
      ITEM_DB,
      ensureArenaFields: arena.ensureArenaFields,
      randInt
    });
  } catch (e) { console.error('[guildboss] Expiry-Check Fehler:', e); }
}, 60 * 1000);
setInterval(async () => {
  try {
    await guildWars.checkExpiry({
      send: async (text, opts) => { try { await sock.sendMessage(OWNER_PRIV, { text, ...opts }); } catch (e) {} },
      sock, users, guilds, save, FILES, getNumberMention
    });
  } catch (e) { console.error('[guildwars] Expiry-Check Fehler:', e); }
}, 60 * 1000);
async function updateBotProfile() {
    try {
      const profileName = `Sword-art-online-bot (${sessionName})`;
      await sock.updateProfileName(profileName);
      console.log(`✅ Bot-Name wurde zu ${profileName} geändert`);

      const profilePath = path.join(__dirname, '1b40b580eca7976d582b9afe0cd7bec5.jpg');
      if (fs.existsSync(profilePath)) {
        await sock.updateProfilePicture(sock.user.id, { url: profilePath });
        console.log('✅ Profilbild wurde aktualisiert');
      } else {
        console.error('❌ Profilbild-Datei nicht gefunden unter:', profilePath);
      }
    } catch (error) {
      console.error('❌ Fehler beim Aktualisieren des Profils:', error);
    }
  }
   sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;

    if (qr) {
      console.log(`📱 QR-Code für Session "${sessionName}" wird generiert...`);
      QRCode.generate(qr, { small: true });

      try {
        const dataUrl = await QRCodeImg.toDataURL(qr, { type: 'image/png', scale: 4 });
        const base64 = dataUrl.split(',')[1];
        const qrBuffer = Buffer.from(base64, 'base64');

        if (hooks.onQr) {
          await hooks.onQr(qrBuffer, sessionName);
        } else {
          await sendQrToTelegram(qrBuffer);
        }
      } catch (err) {
        console.error('QR Bild-Senden fehlgeschlagen:', err);
        console.log('QR-Code wurde im Terminal angezeigt.');
      }
    }

    if (connection === 'open') {
      console.log(`✅ Session "${sessionName}" verbunden mit WhatsApp!`);
      if (hooks.onOpen) {
        try { await hooks.onOpen(sock.user?.id, sessionName); } catch (e) {}
      }
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const errorMessage = lastDisconnect?.error?.message || '(keine Fehlermeldung)';
      console.log(`⚠ Session "${sessionName}" getrennt — Code: ${statusCode || '(unbekannt)'} | Grund: ${errorMessage}`);

      const isLoggedOut = statusCode === DisconnectReason.loggedOut;
      const isConflict = statusCode === DisconnectReason.connectionReplaced;
      const isRestartRequired = statusCode === DisconnectReason.restartRequired;

      if (isRestartRequired) {
        console.log(`ℹ️ Session "${sessionName}": Code 515 (restartRequired) — das ist NORMAL direkt nach dem ersten QR-Scan/Pairing. Verbinde automatisch neu...`);
      }

      if (isLoggedOut) {
        console.log(`❌ Session "${sessionName}" wurde ausgeloggt (Code 401). Login-Daten sind ungültig — Account wurde vermutlich getrennt/gesperrt. Neu pairen nötig, kein automatischer Reconnect.`);
        activeSessions.delete(sessionName);
        return;
      }

      if (isConflict) {
        console.log(`⚠ Session "${sessionName}": Verbindung ersetzt (Code 440) — läuft evtl. ein zweiter Bot-Prozess mit denselben Login-Daten, oder WhatsApp Web ist parallel auf einem anderen Gerät offen?`);
      }

      activeSessions.delete(sessionName);
      setTimeout(() => startBot(sessionName, hooks), 3000);
    }
  });

  sock.ev.on('group-participants.update', async (update) => {
    try {
      const { id: groupId, participants, action } = update;
      function isSenderGroupAdmin(groupMetadata, senderJid) {
        const rawSender = senderJid.split('@')[0];
        const p = groupMetadata?.participants?.find(p =>
          isSameJid(p.id, senderJid) || (p.id || '').split('@')[0] === rawSender
        );
        return p?.admin === 'admin' || p?.admin === 'superadmin';
      }
      groupMetaCache.delete(groupId);

      if (!groupSettings[groupId]) {
        groupSettings[groupId] = {
          welcome: {
            enabled: false,
            message: 'Willkommen in der Gruppe {user}! 👋'
          }
        };
      }
      const settings = groupSettings[groupId];

      if (action === 'add') {
        const groupKickList = bitchkickData[groupId] || [];
        if (groupKickList.length) {
          const freshMeta = await getGroupMetaSafe(groupId, true);
          const botCanKick = isBotAdminInGroup(freshMeta, sock);

          if (botCanKick) {
            for (const rawParticipant of participants) {
              const participantJid = typeof rawParticipant === 'string'
                ? rawParticipant
                : (rawParticipant?.id || rawParticipant?.jid || null);
              if (!participantJid) continue;

              const normalizedParticipant = normalizeJid(participantJid);
              const rawNum = extractRawNumber(normalizedParticipant);
              const isListed = groupKickList.some(j => extractRawNumber(j) === rawNum);

              if (isListed) {
                try {
                  await sock.groupParticipantsUpdate(groupId, [participantJid], 'remove');
                  console.log(`[bitchkick] ${participantJid} aus ${groupId} entfernt.`);
                } catch (e) {
                  console.error(`[bitchkick] Kick fehlgeschlagen für ${participantJid}:`, e?.message || e);
                }
                await sleep(400);
              }
            }
          } else {
            console.log(`[bitchkick] Bot ist kein Admin in ${groupId}, automatischer Kick übersprungen.`);
          }
        }
      }

      if (action === 'add' && settings.welcome.enabled) {
        const welcomeMsg = settings.welcome.message || 'Willkommen in der Gruppe! 👋';

        for (const rawParticipant of participants) {
          const participantJid = typeof rawParticipant === 'string'
            ? rawParticipant
            : (rawParticipant?.id || rawParticipant?.jid || null);

          if (!participantJid || typeof participantJid !== 'string') {
            console.error('[Welcome] Konnte keine gültige JID aus participant extrahieren:', rawParticipant);
            continue;
          }

          const formattedMsg = welcomeMsg.replace('{user}', '@' + participantJid.split('@')[0]);
          try {
            await sock.sendMessage(groupId, {
              text: formattedMsg,
              mentions: [participantJid]
            });
          } catch (sendErr) {
            console.error('[Welcome] Senden fehlgeschlagen für', participantJid, ':', sendErr?.message || sendErr);
          }
        }
      }
    } catch (err) {
      console.error('[Welcome] Error:', err);
    }
  });

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

      if (OWNER_MODE && !isAuthorized(sender, ['OWNER', 'COOWNER']) && !m.key.fromMe) {
        return;
      }

 const body = (m.message.conversation)
        || (m.message.extendedTextMessage && m.message.extendedTextMessage.text)
        || (m.message.imageMessage && m.message.imageMessage.caption)
        || '';

     if (body) {
        log(`[MSG] ${sender} in ${isGroup ? from : 'PM'}: ${body}`);
      }
if (!m.key.fromMe && pendingApplications.has(sender)) {
  const appState = pendingApplications.get(sender);
  const answerText = (body || '').trim();

  if (/^(abbrechen|cancel)$/i.test(answerText)) {
    pendingApplications.delete(sender);
    await sock.sendMessage(from, { text: '❌ Deine Bewerbung wurde abgebrochen, Schwertkämpfer.' });
    return;
  }

  if (!answerText) {
    await sock.sendMessage(from, { text: '⚔️ Bitte gib eine Antwort ein, oder schreibe "abbrechen".' });
    return;
  }

  const currentField = APPLICATION_STEPS[appState.step];
  appState.answers[currentField.key] = answerText;
  appState.step++;

  if (appState.step < APPLICATION_STEPS.length) {
    const nextField = APPLICATION_STEPS[appState.step];
    await sock.sendMessage(from, { text: nextField.question });
    return;
  }

  pendingApplications.delete(sender);
  const a = appState.answers;

  const summary =
    `⚔️ *— NEUE GILDEN-BEWERBUNG —* ⚔️\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `👤 *Name:* ${a.name}\n` +
    `🎂 *Alter:* ${a.alter}\n\n` +
    `📜 *Warum willst du dich bewerben?*\n${a.warum_bewerben}\n\n` +
    `💠 *Warum sollten wir dich nehmen?*\n${a.warum_nehmen}\n\n` +
    `🏅 *Gewünschter Rang:* ${a.rang}\n\n` +
    `🛡️ *Erfahrung als Teammitglied:*\n${a.erfahrung}\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `📇 Bewerber: @${sender.split('@')[0]}\n` +
    `🕓 Eingereicht: ${new Date().toLocaleString('de-DE')}`;

 try {
    await sock.sendMessage('120363429401880501@g.us', {
      text: summary,
      mentions: [sender]
    });
    await sock.sendMessage(from, { text: '✅ Deine Bewerbung wurde erfolgreich an den Anführer der Gilde übermittelt! Er wird sich bei dir melden, Schwertkämpfer. ⚔️' });
  } catch (e) {
    console.error('[bewerbung] Fehler beim Senden an Owner:', e);
    await sock.sendMessage(from, { text: '❌ Deine Bewerbung konnte nicht gesendet werden. Bitte kontaktiere den Owner direkt.' });
  }
  return;
}
function levenshtein(a, b) {
  const m = a.length, n = b.length;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (a[i - 1] === b[j - 1]) dp[i][j] = dp[i - 1][j - 1];
      else dp[i][j] = 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return dp[m][n];
}

const ALL_COMMANDS = [
  'help', 'menu', 'sao', 'ping', 'owner', 'com', 'whoami', 'me', 'afk',
  'register', 'unregister', 'backup', 'balance', 'stats', 'profile', 'userinfo',
  'daily', 'work', 'fish', 'give', 'shop', 'buy', 'inventory', 'use',
  'slot', 'rps', 'blackjack', 'bj', 'bjstart', 'hit', 'stand',
  'adopt', 'pet', 'petinfo', 'feed', 'play',
  'support', 'ticket', 'tickets', 'answer', 'closeticket', 'cleartickets',
  'todo', 'todos', 'usertodo', 'usertodos',
  'gi', 'welcome-an', 'welcome-aus', 'welcome-set', 'games-an', 'games-aus',
  'antilink-an', 'antilink-aus', 'setprefix', 'resetprefix', 'hidetag', 'delete', 'del', 'purge', 'clearchat',
  'ban', 'unban', 'banlist', 'kick', 'warn', 'clearwarns', 'promote', 'demote',
  'setrole', 'setrank', 'listroles', 'applyroles', 'addxp', 'addcash', 'addvip', 'resetcoins',
  'bancmd', 'unbancmd', 'broadcast', 'restart', 'updateprofile',
  'newsession', 'sessions', 'stopsession', 'deletesession', 'delsession',
  'grouplist', 'gl', 'join', 'leave', 'getlid', 'groupid', 'gruppenid',
  'credits', 'addcredit', 'delcredit', 'partner', 'partners', 'buendnisse', 'addpartner', 'delpartner',
  'marry', 'divorce', 'bewerbung', 'bewerben', 'apply',
  'dsgvo', 'ytmp3', 'sticker', 's', 'stiker', 'add', 'code', 'yeetban', 'datadelete',
  'rangliste', 'leaderboard', 'rank', 'setinfo', 'allowcmd', 'say', 'md',
  'selfpromote', 'sp', 'selfdemote', 'sd',
  'slap', 'hug', 'kiss', 'pat', 'poke', 'cuddle', 'bite', 'punch', 'throw',
  'love', 'blush', 'handhold', 'lick', 'nervous',
  'bitchkick',
  ...ARENA_COMMANDS
];

function findClosestCommand(input) {
  let best = null;
  let bestDist = Infinity;
  for (const c of ALL_COMMANDS) {
    const dist = levenshtein(input, c);
    if (dist < bestDist) {
      bestDist = dist;
      best = c;
    }
  }

  const maxLen = Math.max(input.length, best ? best.length : 1);
  const similarity = Math.round((1 - bestDist / maxLen) * 100);

  const threshold = Math.max(1, Math.floor(input.length * 0.4));
  if (bestDist <= threshold) {
    return { command: best, similarity };
  }
  return null;
}
const rawBody = (body || '').trim();
const noPrefixMatch = rawBody.match(/^[^\w]*(\w+)/);
const cmdNoPrefix = noPrefixMatch ? noPrefixMatch[1].toLowerCase() : '';

if (cmdNoPrefix === 'resetprefix' && isGroup) {
  const groupMetadata = await getGroupMetaSafe(from);
  const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
  if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER'])) {
    await sock.sendMessage(from, { text: '❌ Du musst Gruppenadmin sein, um das Gruppenpräfix zurückzusetzen.' });
    return;
  }

  if (!groupSettings[from]) {
    groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' }, prefix: PREFIX };
  }
  groupSettings[from].prefix = '?';
  save(FILES.groupSettings, groupSettings);
  await sock.sendMessage(from, { text: '✅ Gruppenpräfix zurückgesetzt auf: ?' });
  return;
}

if (isGroup && m.key.id) {
  trackGroupMessage(from, m.key.id, m.key.fromMe ? null : (m.key.participant || sender));
}

      if (body && !m.key.fromMe) {
        const afkKey = findAfkKey(sender);
        if (afkKey) {
          const oldReason = users[afkKey].afk.reason || 'Abwesend';
          delete users[afkKey].afk;
          try { save(FILES.users, users); } catch (e) {}
          try {
            if (isGroup) {
              await sock.sendMessage(from, {
                text: `✅ @${sender.split('@')[0]} ist zurück (war AFK: ${oldReason}).`,
                mentions: [sender]
              });
            } else {
              await sock.sendMessage(from, { text: `✅ Du bist nicht mehr AFK (Grund: ${oldReason}).` });
            }
          } catch (e) {}
        }
      }

const whatsappLinkRegex = /(https?:\/\/)?(chat\.whatsapp\.com|whatsapp\.com\/channel)\/[a-zA-Z0-9]+/i;

      if (isGroup && body && !m.key.fromMe && whatsappLinkRegex.test(body)) {
        try {
          const antilinkSettings = groupSettings[from]?.antilink;
          if (antilinkSettings?.enabled) {
            const meta = await getGroupMetaSafe(from, true);

            const senderIsGroupAdmin = isGroupAdminJid(meta, sender);
            const senderIsTeam = isAuthorized(sender, ['OWNER', 'COOWNER', 'GROUPADMIN', 'MOD']);

            if (!senderIsGroupAdmin && !senderIsTeam) {
              const allBotIds = [...getBotSelfIds(sock)];
              const botPart = (meta?.participants || []).find(p => {
                const pids = [
                  p.id,
                  p.id?.split('@')[0],
                  `${p.id?.split('@')[0]}@s.whatsapp.net`,
                ].filter(Boolean).map(String);
                return pids.some(pid => allBotIds.includes(pid));
              });
              const botIsAdmin = !!(
                botPart?.admin === 'admin' ||
                botPart?.admin === 'superadmin' ||
                botPart?.admin === true ||
                botPart?.isAdmin === true
              );

              if (botIsAdmin) {
                try {
                  await sock.sendMessage(from, {
                    delete: { remoteJid: from, id: m.key.id, fromMe: false, participant: sender }
                  });
                } catch (e) { console.error('[antilink] Löschen fehlgeschlagen:', e?.message || e); }

                try {
                  await sock.sendMessage(from, {
                    text: `🚫 @${sender.split('@')[0]} wurde wegen eines WhatsApp-Links entfernt.`,
mentions: [sender]
                  });
                } catch (e) {}

                try {
                  await sock.groupParticipantsUpdate(from, [sender], 'remove');
                } catch (e) { console.error('[antilink] Kick fehlgeschlagen:', e?.message || e); }
              } else {
                console.log('[antilink] Bot ist kein Admin, kann Link-Poster nicht entfernen.');
              }

              return;
            } else {
              console.log(`[antilink] ${sender} ist Admin/Team — wird nicht entfernt.`);
            }
          }
        } catch (e) { console.error('[antilink] Fehler:', e); }
      }

      const activePrefix = isGroup ? getGroupPrefix(from) : PREFIX;
      if (!body || !body.startsWith(activePrefix)) return;

      const afterPrefix = body.slice(activePrefix.length).trim();
      if (!afterPrefix) return;

      const isCmd = true;

      if (isGroup) {
        try {
          const meta = await getGroupMetaSafe(from);

          if (!meta) {
            try { await sock.sendMessage(from, { text: '⚠️ Ich kann meine Administrator-Rechte in dieser Gruppe nicht prüfen. Bitte mache mich zum Administrator.' }); } catch (e) {}
            return;
          }

          const allBotIds = [...getBotSelfIds(sock)];

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
          if (groupSettings[from]?.adminMode?.enabled) {
            const senderIsGroupAdmin = isGroupAdminJid(meta, sender);
            const senderIsPrivileged = isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER']);
            if (!senderIsGroupAdmin && !senderIsPrivileged) {
              return;
            }
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

      if (!body || !body.startsWith(activePrefix)) return;

      await sleep(150);

      const [cmdRaw, ...args] = body.trim().split(/\s+/);
      const rawCmd = cmdRaw.toLowerCase();
      const cmd = rawCmd.startsWith(activePrefix) ? rawCmd.slice(activePrefix.length) : rawCmd;

      const userRank = ranks[sender] || users[sender]?.rank || 'USER';
      const isOwner = isAuthorized(sender, ['OWNER', 'COOWNER']);
      const isCoOwner = isAuthorized(sender, ['COOWNER']);
      const isAdmin = isAuthorized(sender, ['ADMIN']);
      const isTeamMember = isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER']);

      if (BOT_OFFLINE && !isOwner) {
        try { await sock.sendMessage(from, { text: '⚠️ Der Bot ist derzeit im Offline-Modus.' }); } catch (e) {}
        return;
      }

      const send = async (text, opts = {}) => {
        try {
          if (!isTeamMember) {
            try { await sock.sendPresenceUpdate('composing', from); } catch (e) {}
            await sleep(3000);
            try { await sock.sendPresenceUpdate('paused', from); } catch (e) {}
          }
          const sendOpts = { ...opts };
          if (sendOpts.mentions) {
            const mentionArray = Array.isArray(sendOpts.mentions) ? sendOpts.mentions : [sendOpts.mentions];
            sendOpts.contextInfo = sendOpts.contextInfo || {};
            sendOpts.contextInfo.mentionedJid = mentionArray;
          }
          await sock.sendMessage(from, { text, ...sendOpts });
        } catch (e) { console.error('send failed', e); }
      };
      log(`${sender} -> ${body}`);

      if (commandBans && commandBans[cmd] && !isAuthorized(sender, ['OWNER', 'COOWNER'])) {
        try { await sock.sendMessage(from, { text: '⛔ Dieser Befehl wurde vom Owner gesperrt und ist nur für Owner/CoOwner verfügbar.' }); } catch (e) {}
        return;
      }

      const mentionCtx = m.message?.extendedTextMessage?.contextInfo;
      if (mentionCtx && Array.isArray(mentionCtx.mentionedJid) && mentionCtx.mentionedJid.length) {
        for (const mid of mentionCtx.mentionedJid) {
          const afkKey = findAfkKey(mid);
          const afk = afkKey ? users[afkKey].afk : null;
          if (afk) {
            const reason = afk.reason || 'Abwesend';
            try {
              await sock.sendMessage(from, { text: `ℹ️ Dieser User ist nicht erreichbar aufgrund von ${reason}. Versuch es später nochmal.`, mentions: [mid] });
            } catch (e) {}
          }
        }
      }

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
      if (cmd === 'groupid' || cmd === 'gruppenid') {
        if (!isGroup) return send('❌ Dieser Befehl funktioniert nur in Gruppen.');
        return send(`📋 Diese Gruppen-ID ist:\n${from}`);
      }

      if (cmd === 'bitchkick') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        if (!isGroup) return send('❌ Dieser Befehl funktioniert nur innerhalb einer Gruppe.');

        const sub = (args[0] || '').toLowerCase();

        if (sub === 'add') {
          const raw = args[1];
          if (!raw) return send(`❌ Nutzung: ${activePrefix}bitchkick add <nummer>`);
          const jid = normalizeNumber(raw);
          if (!jid) return send('❌ Ungültige Nummer.');

          if (!bitchkickData[from]) bitchkickData[from] = [];
          if (bitchkickData[from].includes(jid)) {
            return send(`ℹ️ ${raw} steht bereits auf der Kick-Liste dieser Gruppe.`);
          }
          bitchkickData[from].push(jid);
          save(FILES.bitchkick, bitchkickData);
          return send(`✅ ${raw} wurde zur Kick-Liste *dieser Gruppe* hinzugefügt.\nWird beim Beitritt automatisch entfernt.`);
        }

        if (sub === 'remove' || sub === 'del') {
          const raw = args[1];
          if (!raw) return send(`❌ Nutzung: ${activePrefix}bitchkick remove <nummer>`);
          const jid = normalizeNumber(raw);
          if (!jid) return send('❌ Ungültige Nummer.');

          if (!bitchkickData[from] || !bitchkickData[from].includes(jid)) {
            return send(`ℹ️ ${raw} steht nicht auf der Liste dieser Gruppe.`);
          }
          bitchkickData[from] = bitchkickData[from].filter(j => j !== jid);
          save(FILES.bitchkick, bitchkickData);
          return send(`✅ ${raw} wurde von der Liste entfernt.`);
        }

        if (sub === 'list') {
          const list = bitchkickData[from] || [];
          if (!list.length) return send('ℹ️ Die Kick-Liste dieser Gruppe ist leer.');
          const formatted = list.map((jid, i) => `${i + 1}. ${jid.split('@')[0]}`).join('\n');
          return send(`📋 *Bitchkick-Liste (diese Gruppe)* (${list.length}):\n${formatted}`);
        }

        if (sub === 'clear') {
          const count = (bitchkickData[from] || []).length;
          bitchkickData[from] = [];
          save(FILES.bitchkick, bitchkickData);
          return send(count ? `✅ ${count} Einträge aus der Liste gelöscht.` : 'ℹ️ Liste war bereits leer.');
        }

        if (sub === 'status') {
          const meta = await getGroupMetaSafe(from, true);
          const botIsAdmin = isBotAdminInGroup(meta, sock);
          return send(
            botIsAdmin
              ? '✅ Bot ist Admin – Bitchkick funktioniert in dieser Gruppe.'
              : '⚠️ Bot ist KEIN Admin – Bitchkick kann hier niemanden entfernen! Bitte Bot zum Admin machen.'
          );
        }

        return send(
          `❌ Nutzung:\n` +
          `${activePrefix}bitchkick add <nummer>\n` +
          `${activePrefix}bitchkick remove <nummer>\n` +
          `${activePrefix}bitchkick list\n` +
          `${activePrefix}bitchkick clear\n` +
          `${activePrefix}bitchkick status`
        );
      }

      if (cmd === 'afk') {
        const reason = args.length ? args.join(' ') : 'Abwesend';
        if (!users[sender]) ensureUser(sender);
        users[sender].afk = { reason, at: new Date().toISOString() };
        try { save(FILES.users, users); } catch (e) {}
        return send(`🔕 Du bist jetzt AFK: ${reason}`);
      }

const SHORT_URL = 'https://youtube.com/shorts/FBMAN-SeeBQ?si=WfMtoSNb1ZD95Dk9';
const CACHE_PATH = path.join(__dirname, 'cache', 'menu-edit.mp4');
const YTMP3_CACHE_DIR = path.join(__dirname, 'cache', 'ytmp3');

function downloadYoutubeMp3(url) {
  return new Promise((resolve, reject) => {
    fs.mkdirSync(YTMP3_CACHE_DIR, { recursive: true });

    const outTemplate = path.join(YTMP3_CACHE_DIR, `${Date.now()}-%(title).60s.%(ext)s`);

    const cmd = `yt-dlp -x --audio-format mp3 --audio-quality 0 --no-playlist -o "${outTemplate}" "${url}"`;

    exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err, stdout, stderr) => {
      if (err) return reject(err);

      const files = fs.readdirSync(YTMP3_CACHE_DIR)
        .filter(f => f.endsWith('.mp3'))
        .map(f => ({ f, t: fs.statSync(path.join(YTMP3_CACHE_DIR, f)).mtimeMs }))
        .sort((a, b) => b.t - a.t);

      if (!files.length) return reject(new Error('Keine MP3-Datei erzeugt.'));
      resolve(path.join(YTMP3_CACHE_DIR, files[0].f));
    });
  });
}

function getYoutubeTitle(url) {
  return new Promise((resolve) => {
    exec(`yt-dlp --get-title --no-playlist "${url}"`, (err, stdout) => {
      resolve(err ? null : stdout.trim());
    });
  });
}

function downloadShortIfNeeded() {
  return new Promise((resolve, reject) => {
    if (fs.existsSync(CACHE_PATH)) return resolve(CACHE_PATH);

    fs.mkdirSync(path.dirname(CACHE_PATH), { recursive: true });

    const cmd = `yt-dlp -f "mp4" -o "${CACHE_PATH}" "${SHORT_URL}"`;

    exec(cmd, (err) => {
      if (err) return reject(err);
      resolve(CACHE_PATH);
    });
  });
}

if (cmd === 'help' || cmd === 'menu') {
  const helpText = menuSystem.buildMenuText({
  args, sender, activePrefix, PREFIX,
  isAuthorized, hasAdminPerms,
  ARENA_HELP_TEXT, GUILD_HELP_TEXT, TITLE_HELP_TEXT, POKEMON_HELP_TEXT,
  GUILDWAR_HELP_TEXT,
  DS_HELP_TEXT: demonSlayer.DS_HELP_TEXT,
  SL_HELP_TEXT: soloLeveling.SL_HELP_TEXT
});
  try {
    const videoPath = await downloadShortIfNeeded();
    await sock.sendMessage(from, {
      video: fs.readFileSync(videoPath),
      caption: helpText,
      mimetype: 'video/mp4'
    }, { quoted: m });
  } catch (e) {
    console.error('Video send failed, fallback to text:', e);
    await sock.sendMessage(from, { text: helpText }, { quoted: m });
  }
  return;
}

 const GAME_COMMANDS = [
  'daily', 'work', 'blackjack', 'bj', 'bjstart', 'hit', 'stand',
  'slot', 'rps', 'fish', 'adopt', 'pet', 'petinfo', 'feed', 'play',

  'openkiste', 'kisteoeffnen', 'openbox', 'gear', 'ausruestung', 'equipment',
  'equip', 'unequip', 'sell', 'verkaufen', 'duell', 'arena', 'duelleaderboard',
  'kampfrangliste', 'arenaitems', 'itemliste', 'floor', 'etage',

  'gilde', 'guild', 'gildenrang', 'guildrank',

  'pokestarter', 'wild', 'catch', 'pokemon', 'p', 'pokeinfo',
  'pokeactive', 'pokename', 'pokerelease', 'pokedex', 'pokeshop',
  'pokebuy', 'poketrain', 'pokevolve', 'pokebattle', 'pokehelp'
];
if (isGroup && GAME_COMMANDS.includes(cmd)) {
  const gamesEnabled = groupSettings[from]?.games?.enabled !== false;
  if (!gamesEnabled) {
    return send(`🎮 Spiele sind in dieser Gruppe deaktiviert. Ein Admin kann sie mit ${activePrefix}games-an wieder aktivieren.`);
  }
}
      if (!isOwner && cmd !== 'help' && cmd !== 'menu') {
        const cooldownCommands = [
          'work', 'fish', 'slot', 'hunt', 'dig', 'crime', 'rob', 'daily', 'weekly', 'monthly',
          'collect', 'open', 'mine', 'farm', 'adventure', 'explore', 'quest', 'raid', 'train',
          'duel', 'gamble', 'casino', 'blackjack', 'rps', 'lottery', 'spin', 'loot',

  'duell', 'arena', 'duelleaderboard',
          'kampfrangliste',

          'gilde', 'guild', 'gildenrang', 'guildrank',

          'pokestarter', 'wild',  'pokemon', 'p', 'pokeinfo',
          'pokeactive', 'pokename', 'pokerelease', 'pokedex', 'pokeshop',
          'pokebuy', 'poketrain', 'pokevolve', 'pokebattle', 'pokehelp'
        ];
        if (cooldownCommands.includes(cmd) && !COOLDOWN_EXCLUDED.includes(cmd)) {
          const cooldownMessage = checkCooldown(sender, cmd);
          if (cooldownMessage) return send(cooldownMessage);
        }
      }
      if (cmd === 'gi' && isGroup) {
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);

        if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Du musst Admin in dieser Gruppe sein.');
        }

        if (!groupSettings[from]) {
          groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' }, prefix: PREFIX };
        }

        const settings = groupSettings[from];
        const groupPrefix = settings.prefix || PREFIX;
        return send(
          `📋 *Gruppeneinstellungen*\n\n*Welcome:* ${settings.welcome.enabled ? '✅ An' : '❌ Aus'}\n*Text:*\n${settings.welcome.message}\n*Prefix:* ${groupPrefix}\n\n*Befehle:*\n${groupPrefix}welcome-an / ${groupPrefix}welcome-aus / ${groupPrefix}welcome-set <text> / ${groupPrefix}setprefix <symbol> / ${groupPrefix}resetprefix`
        );
      }

      if ((cmd === 'welcome-an' || cmd === 'welcome-aus' || cmd === 'welcome-set') && isGroup) {
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);

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
if ((cmd === 'games-an' || cmd === 'games-aus') && isGroup) {
  const groupMetadata = await getGroupMetaSafe(from);
  const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
  if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
    return send('❌ Du musst Admin in dieser Gruppe sein.');
  }

  if (!groupSettings[from]) {
    groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' } };
  }
  if (!groupSettings[from].games) groupSettings[from].games = { enabled: true };

  if (cmd === 'games-an') {
    groupSettings[from].games.enabled = true;
    save(FILES.groupSettings, groupSettings);
    return send('✅ Spiele-Befehle wurden in dieser Gruppe aktiviert.');
  }

  groupSettings[from].games.enabled = false;
  save(FILES.groupSettings, groupSettings);
  return send('✅ Spiele-Befehle wurden in dieser Gruppe deaktiviert.');
}
if ((cmd === 'adminmodus-an' || cmd === 'adminmodus-aus') && isGroup) {
  const groupMetadata = await getGroupMetaSafe(from);
  const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
  if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER'])) {
    return send('❌ Du musst Admin in dieser Gruppe sein.');
  }

  if (!groupSettings[from]) {
    groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' } };
  }
  if (!groupSettings[from].adminMode) groupSettings[from].adminMode = { enabled: false };

  if (cmd === 'adminmodus-an') {
    groupSettings[from].adminMode.enabled = true;
    save(FILES.groupSettings, groupSettings);
    return send('🔒 Admin-Modus aktiviert: Nur noch Gruppenadmins, Supporter, Moderatoren, Co-Owner und Owner können den Bot in dieser Gruppe nutzen.');
  }

  groupSettings[from].adminMode.enabled = false;
  save(FILES.groupSettings, groupSettings);
  return send('🔓 Admin-Modus deaktiviert. Der Bot reagiert wieder auf alle Mitglieder.');
}
      if (cmd === 'answer') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) return send('❌ Kein Zugriff.');
        if (args.length < 2) return send(`❌ Nutzung: ${PREFIX}answer <ticket-id> <antwort-text>`);

        const rawTicketId = args[0];
        const ticketId = normalizeTicketId(rawTicketId);
        const answerText = args.slice(1).join(' ');

        const ticket = getTicketById(rawTicketId);
        if (!ticket) return send(`❌ Ticket #${rawTicketId} nicht gefunden.`);

        const normalizedAnswerer = normalizeJid(sender);
        const answererRank = ranks[normalizedAnswerer] || users[normalizedAnswerer]?.rank || 'USER';
        const rankLabel = prettyRank(answererRank);

        try {
          await sock.sendMessage(SUPPORT_CONFIG.SUPPORT_GROUP, {
            text: `📝 Antwort auf Ticket #${ticket.id}:\n\n${answerText}\n\n${rankLabel}: ${await getNumberMention(sender, sock)}`,
            mentions: [sender]
          });
        } catch (e) {
          console.error('[answer] Konnte Support-Gruppe nicht benachrichtigen (falsche Gruppen-ID oder Bot nicht mehr Mitglied?):', e);
        }

        try {
          await sock.sendMessage(ticket.sender, {
            text: `📩 Antwort auf dein Support-Ticket #${ticket.id}:\n\n${answerText}\n\nBeantwortet von: ${rankLabel} ${await getNumberMention(sender, sock)}`,
            mentions: [sender]
          });
        } catch (e) {
          console.error('[answer] Konnte Antwort nicht direkt an den Nutzer senden:', e);
        }

        delete tickets[ticket.id];
        save(FILES.tickets, tickets);
        ticketCounter = Object.keys(tickets).length;

        return send(`✅ Antwort für Ticket #${ticket.id} gesendet und Ticket gelöscht.`);
      }

      if (cmd === 'setrole') {
        if (!isAuthorized(sender, ['OWNER'])) return send('❌ Nur für Owner.');

        const ctx = m.message?.extendedTextMessage?.contextInfo;
        const isReply = !!ctx?.participant;

        if (args.length < 1 || (args.length < 2 && !isReply)) {
          return send(`❌ Nutzung:\n${PREFIX}setrole @user <ROLLE>\noder: ${PREFIX}setrole <ROLLE> @user\noder: als Antwort auf eine Nachricht einfach ${PREFIX}setrole <ROLLE>\nVerfügbare Rollen: ${Object.keys(ROLES).join(', ')}`);
        }

        const firstUpper = args[0].toUpperCase();
        const lastUpper = args[args.length - 1].toUpperCase();

        let roleUpper, targetArgs;
        if (args.length === 1 && ROLES.hasOwnProperty(firstUpper)) {
          roleUpper = firstUpper;
          targetArgs = [];
        } else if (ROLES.hasOwnProperty(lastUpper)) {
          roleUpper = lastUpper;
          targetArgs = args.slice(0, -1);
        } else if (ROLES.hasOwnProperty(firstUpper)) {
          roleUpper = firstUpper;
          targetArgs = args.slice(1);
        } else {
          return send(`❌ Ungültige Rolle. Verfügbar: ${Object.keys(ROLES).join(', ')}\nNutzung: ${PREFIX}setrole @user <ROLLE> oder ${PREFIX}setrole <ROLLE> @user`);
        }

        const mentioned = ctx?.mentionedJid || [];
        let jidList = [];
        if (ctx?.participant) jidList.push(ctx.participant);
        jidList.push(...mentioned);

        const textTargets = targetArgs.join(' ').split(',').map(s => s.trim()).filter(Boolean);
        for (const raw of textTargets) {
          const cleaned = raw.startsWith('@') ? raw.slice(1) : raw;
          if (/^\d{5,}(@[\w.]+)?$/.test(cleaned)) {
            jidList.push(raw);
          } else {
            const byName = findUserJidByName(cleaned);
            if (byName) jidList.push(byName);
          }
        }

        if (jidList.length === 0) {
          jidList = [sender];
        }

        const validJids = jidList.map(normalizeJid).filter(j => j && (j.endsWith('@s.whatsapp.net') || j.endsWith('@lid')));
        if (!validJids.length) return send('❌ Keine gültigen Nutzer gefunden (weder Mention, Nummer noch registrierter Name).');

        const normalizedJids = Array.from(new Set(validJids));

        if (roleUpper !== 'OWNER' && normalizedJids.some(isPrimaryOwner)) {
          return send('❌ Der Haupt-Owner ist geschützt und kann nicht heruntergestuft werden.');
        }

normalizedJids.forEach(jid => {
  ranks[jid] = roleUpper;

  if (!users[jid]) {
    users[jid] = {
      xp: 0,
      level: 1,
      coins: 100,
      rank: roleUpper,
      msgCount: 0,
      lastDaily: 0,
      items: {},
      registered: false,
      registrationDate: null,
      name: null,
      alter: null,
      hobbys: null,
      sexualitaet: null
    };
  } else {
    users[jid].rank = roleUpper;
  }
});

        for (const otherRole of Object.keys(ROLES)) {
          if (otherRole === roleUpper) continue;
          ROLES[otherRole] = (ROLES[otherRole] || []).filter(
            id => !normalizedJids.some(jid => isSameJid(id, jid))
          );
        }

        if (!ROLES[roleUpper]) ROLES[roleUpper] = [];
        for (const jid of normalizedJids) {
          if (!ROLES[roleUpper].some(id => isSameJid(id, jid))) {
            ROLES[roleUpper].push(jid);
          }
        }

        protectPrimaryOwner();

        try {
          save(FILES.ranks, ranks);
          save(FILES.users, users);
          save(FILES.owner, {
            ...ownerCfg,
            roles: ROLES,
            ownerLid: OWNER_LID,
            ownerPriv: OWNER_PRIV,
            coownerLid: COOWNER_LID
          });
        } catch (e) {
          return send('❌ Fehler beim Speichern: ' + e.message);
        }

        const displayMentions = (await Promise.all(normalizedJids.map(j => getNumberMention(j, sock)))).join(', ');
        return send(
          `✅ Rolle *${roleUpper}* (${prettyRank(roleUpper)}) erfolgreich gesetzt für:\n${displayMentions}\n\n` +
          `💾 Gespeichert in: ranks.json, users.json, owner.json`,
          { mentions: normalizedJids }
        );
      }

      if (cmd === 'setprefix') {
        if (isGroup) {
          const groupMetadata = await getGroupMetaSafe(from);
          const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
          if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER'])) {
            return send('❌ Du musst Gruppenadmin sein, um das Gruppenpräfix zu ändern.');
          }
          if (!groupSettings[from]) {
            groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' }, prefix: PREFIX };
          }
          const desired = args.join(' ').trim();
          if (!desired) return send(`❌ Nutzung: ${groupSettings[from].prefix || PREFIX}setprefix <symbol>`);
          groupSettings[from].prefix = desired[0];
          save(FILES.groupSettings, groupSettings);
          return send(`✅ Gruppenpräfix gesetzt auf: ${groupSettings[from].prefix}`);
        }
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        const desired = args.join(' ').trim();
        if (!desired) return send(`❌ Nutzung: ${PREFIX}setprefix <symbol>`);
        PREFIX = desired[0];
        saveBotState();
        return send(`✅ Präfix gesetzt auf: ${PREFIX}`);
      }

      if (cmd === 'resetcoins') {
        if (!isAuthorized(sender, ['OWNER'])) return send('❌ Nur der Inhaber darf diesen Befehl nutzen.');

        const ctx = m.message?.extendedTextMessage?.contextInfo;
        let target = args[0];
        if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
        if (!target && ctx?.participant) target = ctx.participant;

        if (!target) return send(`❌ Nutzung: ${PREFIX}resetcoins <@user|nummer>`);

        const targetJid = normalizeJid(target);
        ensureUser(targetJid);
        const oldCoins = users[targetJid].coins || 0;
        users[targetJid].coins = 0;
        save(FILES.users, users);

        return send(`✅ Coins von @${targetJid.split('@')[0]} wurden zurückgesetzt (vorher: ${oldCoins} → jetzt: 0).`, { mentions: [targetJid] });
      }
if (cmd === 'listroles') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');

  const roleOrder = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'VIP', 'SUPPORTER', 'TEST_SUPPORTER'];

  const rawEntries = {};
  for (const r of roleOrder) rawEntries[r] = new Set();

  const collect = (role, jid) => {
    if (role === 'USER') return;
    const n = normalizeJid(jid);
    if (!n) return;
    if (!rawEntries[role]) rawEntries[role] = new Set();
    rawEntries[role].add(n);
  };

  for (const [jid, role] of Object.entries(ranks)) collect(role, jid);
  for (const [role, jids] of Object.entries(ROLES)) {
    if (!Array.isArray(jids)) continue;
    for (const id of jids) collect(role, id);
  }

  const grouped = {};
  for (const role of Object.keys(rawEntries)) {
    grouped[role] = new Map();
    for (const n of rawEntries[role]) {
      let canonicalRaw = extractRawNumber(n);
      let displayJid = n;

      if (n.endsWith('@lid')) {
        const resolved = await resolvePhoneJid(n, sock);
        if (resolved) {
          canonicalRaw = extractRawNumber(resolved);
          displayJid = resolved;
        }
      }

      const existing = grouped[role].get(canonicalRaw);
      if (!existing) {
        grouped[role].set(canonicalRaw, { displayJid, sourceJids: new Set([n]) });
      } else {
        existing.sourceJids.add(n);
        if (existing.displayJid.endsWith('@lid') && displayJid.endsWith('@s.whatsapp.net')) {
          existing.displayJid = displayJid;
        }
      }
    }
  }

  const allRoleKeys = [...new Set([...roleOrder, ...Object.keys(grouped)])];

  let message = `📋 *Rollen* (${new Date().toLocaleString('de-DE')})\n\n`;
  const allMentions = [];

  for (const role of allRoleKeys) {
    const map = grouped[role];
    if (!map || map.size === 0) {
      message += `*${prettyRank(role)}* (0):\n(keine)\n\n`;
      continue;
    }

    message += `*${prettyRank(role)}* (${map.size}):\n`;
    for (const [raw, entry] of map) {
      let name = '(kein name)';
      for (const src of entry.sourceJids) {
        const u = users[src];
        if (u && (u.name || u.registrationName)) { name = u.name || u.registrationName; break; }
      }
      message += `• @${raw} — ${name}\n`;
      allMentions.push(entry.displayJid);
    }
    message += '\n';
  }

  return send(message.trim(), { mentions: allMentions });
}
      if (cmd === 'applyroles') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        const target = args[0] ? args[0].trim() : from;
        const groupJid = normalizeJid(target);
        if (!groupJid || !groupJid.endsWith('@g.us')) return send('❌ Gib eine Gruppen-JID an oder nutze den Befehl innerhalb einer Gruppe.');

        const adminList = (ROLES.ADMIN || []).map(j => toParticipantJid(j)).filter(Boolean);
        if (!adminList.length) return send('ℹ Keine Admin-JIDs in den Rollen gefunden.');

        try {
          await sock.groupParticipantsUpdate(groupJid, adminList, 'promote');
          return send(`✅ ${adminList.length} Teilnehmer in ${groupJid} zu Admins befördert.`);
        } catch (e) {
          console.error('applyroles error', e);
          return send('❌ Fehler beim Anwenden der Rollen in der Gruppe.');
        }
      }

      if (cmd === 'unbancmd') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Nur Owner/CoOwner.');
        const target = args[0];
        if (!target) {
          return send(`Nutzung: ${PREFIX}unbancmd <befehl>`);
        }
        const tcmd = String(target).toLowerCase().replace(new RegExp(`^\\${PREFIX}`), '').trim();
        if (!tcmd) return send('❌ Ungültiger Befehl.');
        if (commandBans[tcmd]) {
          delete commandBans[tcmd];
          try { save(FILES.commandBans, commandBans); } catch (e) {}
          return send(`✅ Befehl ${tcmd} wurde entsperrt.`);
        } else {
          return send(`ℹ️ Befehl ${tcmd} war nicht gesperrt.`);
        }
      }

      if (cmd === 'updateprofile') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        await send('🔄 Aktualisiere Bot-Profil...');
        await updateBotProfile();
        return send('✅ Profilaktualisierung abgeschlossen.');
      }

      if (cmd === 'restart') {
        if (!hasAdminPerms(sender)) return send('❌ Kein Zugriff.');
        await send('🔄 Bot wird neugestartet...');
        try {
          fs.writeFileSync(RESTART_FILE, JSON.stringify({ timestamp: Date.now(), chatId: from, initiator: sender }));
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🔄 Bot-Neustart durch ${sender}` });
        } catch {}
        process.exit(0);
      }

if (cmd === 'whoami' || cmd === 'me') {
  const normalizedSender = normalizeJid(sender);
  const user = users[normalizedSender] || {};
  const username = user.name || user.registrationName || sender.split('@')[0];
  const coins = user.coins || 0;
  const r = ranks[normalizedSender] || user.rank || '(none)';
  const level = user.level || 1;
  const xp = user.xp || 0;
  const neededXp = 100 + (level * 50);
  const remainingXp = Math.max(0, neededXp - xp);

  const marriage = marriages[normalizedSender];
  let marriageLine = '💍 Status: Single';
  const marriageMentions = [];
  if (marriage) {
    const partnerUser = users[marriage.partner] || {};
    const partnerName = partnerUser.name || partnerUser.registrationName || marriage.partner.split('@')[0];
    marriageLine = `💍 Verheiratet mit: ${partnerName} (seit ${new Date(marriage.since).toLocaleDateString('de-DE')})`;
    marriageMentions.push(marriage.partner);
  }

  const infoLines = [];
  if (user.alter) infoLines.push('Alter: ' + user.alter);
  if (user.hobbys) infoLines.push('Hobbys: ' + user.hobbys);
  if (user.sexualitaet) infoLines.push('Sexualitaet: ' + user.sexualitaet);
  const infoBlock = infoLines.length ? '\n' + infoLines.join('\n') : '';

  const activeTitleObj = TITLES.find(t => t.id === user.activeTitle);
  const titleLine = activeTitleObj
    ? `🎖️ Titel: ${activeTitleObj.icon} "${activeTitleObj.name}"`
    : '🎖️ Titel: Keiner';

  arena.ensureArenaFields(users, normalizedSender);
  const weaponId = user.equipped?.weapon;
  const armorId = user.equipped?.armor;

  const formatGearName = (itemId) => {
    if (!itemId) return '— (nichts ausgerüstet)';
    const item = ITEM_DB[itemId];
    if (!item) return '— (unbekannt)';
    const rarityIcon = RARITY_INFO[item.rarity]?.emoji || '';
    return `${rarityIcon} ${item.name}`;
  };

  const weaponLine = formatGearName(weaponId);
  const armorLine = formatGearName(armorId);

  const caption = `User: ${username}\n${titleLine}\nCoins: ${coins}\nRank: ${r}\nLevel: ${level}\nXP: ${xp} / ${neededXp}\nNoch ${remainingXp} XP bis Level ${level + 1}\n🗡️ Waffe: ${weaponLine}\n🛡️ Rüstung: ${armorLine}\n${marriageLine}${infoBlock}`;

  const FALLBACK_PP_URL = 'https://raw.githubusercontent.com/Marlon9511/Sword-art-online-bot/main/5d553cd8911378163e989839dff229f3.webp.jpg';

  try {
    const candidates = [];

    const resolved = await resolvePhoneJid(normalizedSender, sock);
    if (resolved) candidates.push(resolved);

    candidates.push(normalizedSender);

    if (isSameJid(normalizedSender, OWNER_LID) || isSameJid(normalizedSender, OWNER_PRIV)) {
      if (OWNER_PRIV) candidates.push(OWNER_PRIV);
      if (OWNER_LID) candidates.push(OWNER_LID);
    }

    const getPPUrl = async (jid) => {
      const types = ['image', 'preview'];
      for (const type of types) {
        try {
          const result = await Promise.race([
            sock.profilePictureUrl(jid, type),
            new Promise((_, reject) => setTimeout(() => reject(new Error('pp timeout (15s)')), 15000))
          ]);
          if (result) return result;
        } catch (e) {
          console.error(`[whoami] PP-Fehler für ${jid} (${type}):`, e);
        }
      }
      return null;
    };

    let ppUrl = null;
    const tried = new Set();
    for (const c of candidates) {
      if (!c || tried.has(c)) continue;
      tried.add(c);
      ppUrl = await getPPUrl(c);
      if (ppUrl) break;
    }
    console.log('[whoami] Socket-Status:', sock.ws?.readyState, '| User:', !!sock.user);

    if (!ppUrl) {
      console.error('[whoami] Kein Profilbild gefunden für Kandidaten:', [...tried]);
      ppUrl = FALLBACK_PP_URL;
    }

    if (ppUrl) {
      if (!isTeamMember) {
        try { await sock.sendPresenceUpdate('composing', from); } catch (e) {}
        await sleep(3000);
        try { await sock.sendPresenceUpdate('paused', from); } catch (e) {}
      }
      await sock.sendMessage(from, { image: { url: ppUrl }, caption });
      return;
    }
  } catch (e) {
    console.error('[whoami] Allgemeiner Fehler:', e?.message || e);
  }

  return send(caption);
}

      if (cmd === 'ping') {
        const startTime = Date.now();
        await send('🏓 Pong!');
        return send(`Antwortzeit: ${Date.now() - startTime}ms`);
      }

      if (cmd === 'owner') {
        return send('👑 Kontaktiere den Owner:\nhttps://wa.me/4915111254435');
      }

      if (cmd === 'com') {
        if (!args.length) {
          return send(`📢 *Offizielle Gruppe*\n${officialGroup.link}`);
        }

        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
          return send('❌ Nur Owner/CoOwner dürfen den Link ändern.');
        }

        const newLink = args[0].trim();
        if (!/^https?:\/\/chat\.whatsapp\.com\/[A-Za-z0-9]+$/.test(newLink)) {
          return send(`❌ Ungültiger WhatsApp-Gruppenlink.\nBeispiel: ${PREFIX}com https://chat.whatsapp.com/XXXXXXXXXXXXXXXXXXXXXX`);
        }

        officialGroup.link = newLink;
        save(FILES.officialGroup, officialGroup);
        return send(`✅ Offizieller Gruppenlink aktualisiert:\n${newLink}`);
      }

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
if (cmd === 'bewerbung' || cmd === 'bewerben' || cmd === 'apply') {
  if (pendingApplications.has(sender)) {
    return send('⚔️ Du hast bereits eine offene Bewerbung. Beantworte die letzte Frage oder schreibe "abbrechen".');
  }
  pendingApplications.set(sender, { step: 0, answers: {} });
  await sock.sendMessage(from, { text: APPLICATION_STEPS[0].question });
  return;
}
      if (cmd === 'botoffline') {
        if (!isOwner) return send('❌ Nur der Inhaber.');
        const action = (args[0] || '').toLowerCase();
        if (!action || action === 'status') return send(`🔌 Offline-Modus: ${BOT_OFFLINE ? 'AN' : 'AUS'}`);
        if (['on', 'enable', 'true'].includes(action)) { BOT_OFFLINE = true; saveBotState(); return send('✅ Offline-Modus AN.'); }
        if (['off', 'disable', 'false'].includes(action)) { BOT_OFFLINE = false; saveBotState(); return send('✅ Bot wieder online.'); }
        if (action === 'toggle') { BOT_OFFLINE = !BOT_OFFLINE; saveBotState(); return send(`🔁 Offline-Modus: ${BOT_OFFLINE ? 'AN' : 'AUS'}`); }
        return send('❌ Nutzung: $botoffline on|off|toggle|status');
      }

      if (cmd === 'newsession') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('❌ Kein Zugriff.');
        const newSessionName = args[0];
        if (!newSessionName) return send('❌ Beispiel: $newsession meinbot');
        if (activeSessions.has(newSessionName)) return send(`❌ Session "${newSessionName}" läuft bereits.`);

        await send(`⏳ Starte neue Session "${newSessionName}"...`);

        startBot(newSessionName, {
          onQr: async (qrBuffer) => {
            try {
              await sock.sendMessage(from, {
                image: qrBuffer,
                mimetype: 'image/png',
                caption: `🤖 Neue Bot Session: ${newSessionName}\nScanne den QR-Code.`
              });
            } catch (err) {
              console.error('QR send error:', err);
            }
          },
          onOpen: async (id) => {
            try { await sock.sendMessage(from, { text: `✅ Session "${newSessionName}" angemeldet! JID: ${id || '(unbekannt)'}` }); } catch (e) {}
          }
        }).catch(async (err) => {
          console.error('Session creation error:', err);
          try { await sock.sendMessage(from, { text: `❌ Fehler beim Erstellen der Session "${newSessionName}".` }); } catch (e) {}
        });

        return;
      }

      if (cmd === 'sessions') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('❌ Kein Zugriff.');
        const list = [...activeSessions.keys()].map(name => {
          const s = activeSessions.get(name);
          const id = s?.user?.id || '(verbindet...)';
          return `• ${name} — ${id}`;
        }).join('\n') || '(keine aktiven Sessions)';
        return send(`📱 Aktive Sessions:\n${list}`);
      }

      if (cmd === 'stopsession') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        const target = args[0];
        if (!target) return send(`❌ Nutzung: ${PREFIX}stopsession <name>`);
        if (target === 'default') return send('❌ Die Standard-Session kann nicht über den Befehl gestoppt werden.');
        const targetSock = activeSessions.get(target);
        if (!targetSock) return send(`❌ Session "${target}" ist nicht aktiv.`);
        try {
          activeSessions.delete(target);
          targetSock.ev.removeAllListeners();
          try { await targetSock.logout(); } catch (e) {}
          try { targetSock.end(new Error('stopped by command')); } catch (e) {}
          return send(`✅ Session "${target}" gestoppt.`);
        } catch (e) {
          return send(`❌ Fehler beim Stoppen von "${target}": ` + e.message);
        }
      }

      if (cmd === 'deletesession' || cmd === 'delsession') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        const target = args[0];
        if (!target) return send(`❌ Nutzung: ${PREFIX}deletesession <name>`);

        const targetPath = path.join(SESSIONS_DIR, target);
        const normalizedTargetPath = path.normalize(targetPath);
        if (!normalizedTargetPath.startsWith(SESSIONS_DIR)) return send('❌ Ungültiger Session-Name.');

        const isCurrentSession = target === sessionName;
        if (isCurrentSession) {
          return send('❌ Diese Session läuft gerade und beantwortet deinen Befehl — lösche sie über eine andere Session oder stoppe sie zuerst manuell auf dem Server.');
        }

        const targetSock = activeSessions.get(target);
        if (targetSock) {
          try {
            activeSessions.delete(target);
            targetSock.ev.removeAllListeners();
            try { await targetSock.logout(); } catch (e) {}
            try { targetSock.end(new Error('deleted by command')); } catch (e) {}
          } catch (e) {}
        }

        if (!fs.existsSync(targetPath)) {
          return send(`❌ Session "${target}" existiert nicht (weder aktiv noch als gespeicherte Login-Daten).`);
        }

        try {
          fs.rmSync(targetPath, { recursive: true, force: true });
        } catch (e) {
          return send(`❌ Fehler beim Löschen der Session-Dateien von "${target}": ` + e.message);
        }

        return send(`✅ Session "${target}" wurde gestoppt und vollständig gelöscht (inkl. Login-Daten). Beim nächsten "${PREFIX}newsession ${target}" muss neu per QR-Code gescannt werden.`);
      }
if (cmd === 'marry') {
  const sub = (args[0] || '').toLowerCase();

  if (sub === 'accept') {
  const proposal = pendingMarriageProposals.get(sender);
  if (!proposal) return send('❌ Du hast keinen offenen Heiratsantrag.');
  if (marriages[sender] || marriages[proposal.from]) {
    pendingMarriageProposals.delete(sender);
    return send('❌ Einer von euch ist inzwischen bereits verheiratet.');
  }
  marriages[sender] = { partner: proposal.from, since: Date.now() };
  marriages[proposal.from] = { partner: sender, since: Date.now() };
  save(FILES.marriages, marriages);
  pendingMarriageProposals.delete(sender);

  ensureUser(sender);
  ensureUser(proposal.from);
  users[sender].__isMarried = true;
  users[proposal.from].__isMarried = true;
  save(FILES.users, users);

  await checkProgress({
    users, save, FILES, send, activePrefix,
    guilds, ownerJids: [OWNER_LID, OWNER_LID2, OWNER_PRIV, OWNER_PRIV2]
  }, sender);
  await checkProgress({
    users, save, FILES, send: async (text, opts) => {
      try { await sock.sendMessage(proposal.from, { text, ...opts }); } catch (e) {}
    }, activePrefix,
    guilds, ownerJids: [OWNER_LID, OWNER_LID2, OWNER_PRIV, OWNER_PRIV2]
  }, proposal.from);

  return send(
    `💍 Herzlichen Glückwunsch! @${proposal.from.split('@')[0]} und @${sender.split('@')[0]} sind jetzt verheiratet! 🎉`,
    { mentions: [sender, proposal.from] }
  );
}
  if (sub === 'deny' || sub === 'decline') {
    const proposal = pendingMarriageProposals.get(sender);
    if (!proposal) return send('❌ Du hast keinen offenen Heiratsantrag.');
     pendingMarriageProposals.delete(sender);
    return send(`💔 @${sender.split('@')[0]} hat den Heiratsantrag abgelehnt.`, { mentions: [sender] });
  }

  if (sub === 'cancel') {
    let found = null;
    for (const [targetJid, v] of pendingMarriageProposals.entries()) {
      if (v.from === sender) { found = targetJid; break; }
    }
    if (!found) return send('❌ Du hast keinen offenen Antrag zum Zurückziehen.');
    pendingMarriageProposals.delete(found);
    return send('✅ Dein Heiratsantrag wurde zurückgezogen.');
  }

  const ctx = m.message?.extendedTextMessage?.contextInfo;
  let target = args[0];
  if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
  if (!target && ctx?.participant) target = ctx.participant;
  if (!target) return send(`❌ Nutzung: ${activePrefix}marry @user\n${activePrefix}marry accept / deny / cancel`);

  const targetJid = normalizeJid(target);
  ensureUser(sender);
  ensureUser(targetJid);

  if (isSameJid(sender, targetJid)) return send('❌ Du kannst dich nicht selbst heiraten! 😅');
  if (marriages[sender]) return send(`❌ Du bist bereits mit @${marriages[sender].partner.split('@')[0]} verheiratet. Nutze zuerst ${activePrefix}divorce.`, { mentions: [marriages[sender].partner] });
  if (marriages[targetJid]) return send(`❌ @${targetJid.split('@')[0]} ist bereits verheiratet.`, { mentions: [targetJid] });

  const existing = pendingMarriageProposals.get(targetJid);
  if (existing && existing.from === sender) return send('❌ Du hast bereits einen offenen Antrag an diese Person.');

  pendingMarriageProposals.set(targetJid, { from: sender, at: Date.now() });

  return send(
    `💍 @${sender.split('@')[0]} möchte @${targetJid.split('@')[0]} heiraten!\n\n@${targetJid.split('@')[0]}, antworte mit:\n${activePrefix}marry accept — annehmen\n${activePrefix}marry deny — ablehnen`,
    { mentions: [sender, targetJid] }
  );
}

if (cmd === 'divorce') {
  ensureUser(sender);
  const marriage = marriages[sender];
  if (!marriage) return send('❌ Du bist nicht verheiratet.');

  const partnerJid = marriage.partner;
  delete marriages[sender];
  delete marriages[partnerJid];
  save(FILES.marriages, marriages);

  try {
    await sock.sendMessage(partnerJid, {
      text: `💔 @${sender.split('@')[0]} hat sich von dir scheiden lassen.`,
      mentions: [sender]
    });
  } catch (e) {}

  return send(`💔 Du hast dich von @${partnerJid.split('@')[0]} scheiden lassen.`, { mentions: [partnerJid] });
}
      if (cmd === 'hidetag') {
        if (!isGroup) return send('❌ Nur in Gruppen.');
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
        if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('❌ Kein Zugriff.');
        const message = args.join(' ');
        if (!message) return send('❌ Beispiel: $hidetag Wichtige Ankündigung!');
        try {
          const groupMembers = await getGroupMetaSafe(from);
          const mentions = (groupMembers?.participants || [])
            .map(p => toLidJid(p.id))
            .filter(jid => jid && jid.endsWith('@lid'));
          if (!mentions.length) return send('❌ Keine Gruppenmitglieder gefunden.');
          return send(message, { mentions, quoted: m });
        } catch (err) {
          return send('❌ Fehler beim Ausführen.');
        }
      }

      if (cmd === 'delete' || cmd === 'del') {
        const ctx = m.message?.extendedTextMessage?.contextInfo;
        if (!ctx || !ctx.stanzaId) {
          return send(`❌ Antworte mit ${activePrefix}delete auf die Nachricht, die gelöscht werden soll.`);
        }

        const quotedParticipant = normalizeJid(ctx.participant || from);
        const botSelfIds = getBotSelfIds(sock);
        const quotedRaw = String(quotedParticipant);
        const isOwnMessage = botSelfIds.has(quotedRaw) || botSelfIds.has(quotedRaw.split('@')[0]);

        if (!isOwnMessage) {
          if (!isGroup) {
            return send('❌ In privaten Chats kann ich nur meine eigenen Nachrichten löschen.');