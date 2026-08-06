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
import { createArenaSystem, ARENA_SHOP_ITEM, ARENA_HELP_TEXT, ARENA_COMMANDS } from './arena-system.mjs';
import { createGuildSystem, GUILD_COMMANDS, GUILD_HELP_TEXT } from './guild-system.mjs';
import { createTitleSystem, TITLE_COMMANDS, TITLE_HELP_TEXT, checkProgress, TITLES } from './titel-achievments.js';

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
groupLockSchedule: { file: 'group-lock-schedule.json', default: {} }
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


//=========================//
// Connect Bot + Pairing-Code
//=========================//
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
let COOWNER_LID = '272696835330300@lid';

ROLES.OWNER.push(OWNER_LID, OWNER_PRIV);
ROLES.COOWNER.push(COOWNER_LID);

const BOT_STATE_FILE = path.join(DATA_PATH, 'bot-state.json');
let BOT_OFFLINE = false;
let PREFIX = '?';
try {
  if (fs.existsSync(BOT_STATE_FILE)) {
    const st = JSON.parse(fs.readFileSync(BOT_STATE_FILE, 'utf8') || '{}');
    BOT_OFFLINE = !!st.offline;
    if (st.prefix && typeof st.prefix === 'string' && st.prefix.trim().length) {
      PREFIX = st.prefix.trim().slice(0, 1);
    }
  }
} catch (e) { console.error('Failed to load bot state:', e); }

const saveBotState = () => {
  try {
    fs.writeFileSync(BOT_STATE_FILE, JSON.stringify({ offline: !!BOT_OFFLINE, prefix: PREFIX }, null, 2));
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

// ⚔️ Arena-System (Ausrüstung, Kisten, PVP-Duelle, Leaderboard)
const arena = createArenaSystem();
const guildSystem = createGuildSystem();
const titleSystem = createTitleSystem();

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
// ---- FIX: robuster Admin-Check über die reine Rufnummer ----
// Baileys liefert Gruppenteilnehmer je nach Situation als @lid oder
// @s.whatsapp.net (teils mit :device-Suffix). isSameJid() vergleicht nur
// exakt normalisierte Strings und erkennt @lid <-> @s.whatsapp.net NICHT
// als gleich. Dadurch wurden echte Gruppenadmins nicht erkannt, wenn ihr
// JID-Typ nicht exakt zum gespeicherten sender-JID passte.
// extractRawNumber() zieht in jedem Fall nur die reine Ziffernfolge raus,
// damit der Vergleich unabhängig vom JID-Typ funktioniert.
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
  try {
    save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
  } catch (e) { console.error('Failed to save owner config:', e); }
}
setInterval(persistAll, 60_000);
const groupMessageHistory = new Map(); // groupJid -> Array<{ id, participant }>
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
// ========== GAME HELPERS ==========
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
  legendary: { label: 'Legendär', emoji: '🟡', weight: 2 }
};

// type: 'weapon' | 'armor'
const ITEM_DB = {
  // ---- WAFFEN ----
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

  // ---- RÜSTUNGEN ----
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
  a_legendary_3: { name: 'Himmlischer Panzer',      type: 'armor',  rarity: 'legendary', power: 66 }
};


// ========== START BOT ==========

// hooks: optionale { onQr(qrBuffer, sessionName), onOpen(botId, sessionName) },
//        werden z.B. vom $newsession-Befehl genutzt, um QR/Status in den
//        anfragenden Chat statt an OWNER_PRIV zu schicken.
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

const pendingMarriageProposals = new Map(); // targetJid -> { from, at }
const pendingApplications = new Map(); // sender -> { step, answers }
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
// Nachtsperre-Scheduler: prüft jede Minute, ob Gruppen gesperrt/entsperrt werden müssen
  const lockStateCache = new Map(); // groupJid -> 'locked' | 'unlocked'

  function timeToMinutes(t) {
    const parts = t.split(':');
    return parseInt(parts[0]) * 60 + parseInt(parts[1]);
  }

  function isWithinLockWindow(startStr, endStr, nowMinutes) {
    const start = timeToMinutes(startStr);
    const end = timeToMinutes(endStr);
    if (start === end) return false;
    if (start < end) {
      // z.B. 08:00 - 18:00 (gleicher Tag)
      return nowMinutes >= start && nowMinutes < end;
    }
    // z.B. 22:00 - 07:00 (über Mitternacht)
    return nowMinutes >= start || nowMinutes < end;
  }

 setInterval(async () => {
    try {
      const now = new Date();
      const nowMinutes = now.getHours() * 60 + now.getMinutes();

      const scheduleCount = Object.keys(groupLockSchedules).length;
      console.log('[nachtsperre] Check läuft:', now.toLocaleTimeString('de-DE'), '| Session:', sessionName, '| Zeitzone:', Intl.DateTimeFormat().resolvedOptions().timeZone, '| Gruppen mit Zeitplan:', scheduleCount);

      for (const [groupJid, schedule] of Object.entries(groupLockSchedules)) {
        const shouldBeLocked = isWithinLockWindow(schedule.start, schedule.end, nowMinutes);
        const currentState = lockStateCache.get(groupJid);
        const desiredState = shouldBeLocked ? 'locked' : 'unlocked';

        console.log('[nachtsperre]', groupJid, '| soll:', desiredState, '| ist aktuell:', currentState || '(unbekannt)');

        if (currentState === desiredState) continue;

        try {
          const meta = await getGroupMetaSafe(groupJid, true);
          if (!meta) {
            console.error('[nachtsperre] ❌ Konnte Gruppen-Metadaten nicht laden für', groupJid, '(Bot noch Mitglied?)');
            continue;
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

          const botIsAdmin = !!(
            botPart?.admin === 'admin' ||
            botPart?.admin === 'superadmin' ||
            botPart?.admin === true ||
            botPart?.isAdmin === true
          );

          if (!botIsAdmin) {
            console.error('[nachtsperre] ❌ Bot ist kein Admin in', groupJid, '— kann nicht sperren/entsperren. Bitte Bot zum Admin machen.');
            continue;
          }
        } catch (metaErr) {
          console.error('[nachtsperre] ❌ Fehler beim Prüfen der Admin-Rechte für', groupJid, ':', metaErr?.message || metaErr);
          continue;
        }

        try {
          await sock.groupSettingUpdate(groupJid, shouldBeLocked ? 'announcement' : 'not_announcement');
          lockStateCache.set(groupJid, desiredState);
          console.log('[nachtsperre] ✅ Gruppe', groupJid, shouldBeLocked ? 'gesperrt' : 'entsperrt');

          if (shouldBeLocked) {
            await sock.sendMessage(groupJid, { text: '🌙 Nachtsperre aktiv. Die Gruppe wurde bis ' + schedule.end + ' Uhr gesperrt.' });
          } else {
            await sock.sendMessage(groupJid, { text: '☀️ Nachtsperre beendet. Die Gruppe ist wieder offen.' });
          }
        } catch (e) {
          console.error('[nachtsperre] ❌ Fehler beim Sperren/Entsperren von', groupJid, ':', e?.message || e);
        }
      }
    } catch (e) {
      console.error('[nachtsperre] Scheduler-Fehler:', e);
    }
  }, 60 * 1000); // jede Minute prüfen
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
          await sock.sendMessage(OWNER_PRIV, {
            image: qrBuffer,
            caption: `🤖 QR-Code zum Scannen mit WhatsApp (Session: ${sessionName})`
          });
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
      console.log(`⚠ Session "${sessionName}" getrennt — neu verbinden in 3s`);
      activeSessions.delete(sessionName);
      setTimeout(() => startBot(sessionName, hooks), 3000);
    }
  });

  sock.ev.on('creds.update', saveCreds);

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
      // Cache invalidieren: Admin-Status/Mitgliederliste hat sich geändert
      // (u.a. wichtig für promote/demote und den Antilink-Admin-Check)
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

  // Alle Fragen beantwortet -> an Owner senden
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
// Levenshtein-Distanz: misst, wie "ähnlich" zwei Strings sind
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

// Alle bekannten Befehle — bei neuen Commands hier ergänzen!
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

  // Nur vorschlagen, wenn der Tippfehler "klein genug" ist (max. 40% der Wortlänge abweichend)
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
            // Frische (nicht gecachte) Metadaten holen, damit ein kürzlich
            // beförderter/degradierter Admin-Status korrekt erkannt wird.
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

      // Wenn nach dem Prefix nichts (oder nur Leerzeichen) kommt, nicht reagieren
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

          // Eigene Identität wird komplett dynamisch aus dem Socket ermittelt —
          // keine hartkodierte LID mehr nötig.
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
      // Team-Mitglieder = alle Ränge außer VIP, USER und ADMIN
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

      // AFK-Mentions: Wenn ein AFK-User erwähnt wird, Sender informieren
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
// GROUPID
      if (cmd === 'groupid' || cmd === 'gruppenid') {
        if (!isGroup) return send('❌ Dieser Befehl funktioniert nur in Gruppen.');
        return send(`📋 Diese Gruppen-ID ist:\n${from}`);
      }
      // AFK
      if (cmd === 'afk') {
        const reason = args.length ? args.join(' ') : 'Abwesend';
        if (!users[sender]) ensureUser(sender);
        users[sender].afk = { reason, at: new Date().toISOString() };
        try { save(FILES.users, users); } catch (e) {}
        return send(`🔕 Du bist jetzt AFK: ${reason}`);
      }
const SHORT_URL = 'https://youtube.com/shorts/Tnj-yTpHpoY?si=nZXYlSHtpdT42Awi';
const CACHE_PATH = path.join(__dirname, 'cache', 'menu-edit.mp4');
const YTMP3_CACHE_DIR = path.join(__dirname, 'cache', 'ytmp3');

function downloadYoutubeMp3(url) {
  return new Promise((resolve, reject) => {
    fs.mkdirSync(YTMP3_CACHE_DIR, { recursive: true });

    const outTemplate = path.join(YTMP3_CACHE_DIR, `${Date.now()}-%(title).60s.%(ext)s`);

    // -x = nur Audio extrahieren, --audio-format mp3 = zu mp3 konvertieren (braucht ffmpeg)
    const cmd = `yt-dlp -x --audio-format mp3 --audio-quality 0 --no-playlist -o "${outTemplate}" "${url}"`;

    exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err, stdout, stderr) => {
      if (err) return reject(err);

      // Neueste .mp3-Datei im Cache-Ordner finden
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

    // -f mp4 wählt ein Format, das WhatsApp gut abspielen kann
    const cmd = `yt-dlp -f "mp4" -o "${CACHE_PATH}" "${SHORT_URL}"`;

    exec(cmd, (err) => {
      if (err) return reject(err);
      resolve(CACHE_PATH);
    });
  });
}
// HELP / MENU
      if (cmd === 'help' || cmd === 'menu') {
        const divider = '⚔️┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈⚔️';
        let helpText = `┏━━━━━━━━━━━━━━━┓\n┃  ▄▄▄▄▄▄▄▄▄▄▄▄▄  ┃\n┃  █ AINCRAD █  ┃\n┃  ▀▀▀▀▀▀▀▀▀▀▀▀▀  ┃\n┗━━━━━━━━━━━━━━━┛\n     🗡️ System Command Window 🗡️\n     ⌈ Floor: Main Menu ⌋\n\n`;

        helpText += `🔷 *SYSTEM MENU*\n${divider}\n`;
        helpText += `▸ ${PREFIX}help — Dieses Command-Window öffnen\n`;
        helpText += `▸ ${PREFIX}ping — Verbindung zum Server prüfen\n`;
        helpText += `▸ ${PREFIX}owner — Game Master kontaktieren\n`;
        helpText += `▸ ${PREFIX}com — Link zur Gilden-Halle\n`;
        helpText += `▸ ${PREFIX}whoami / ${PREFIX}me — Charakterbogen anzeigen\n`;
        helpText += `▸ ${PREFIX}afk [grund] — Logout-Status setzen\n`;
        helpText += `▸ ${PREFIX}usertodo add <text> — Skill vorschlagen\n`;
        helpText += `▸ ${PREFIX}credits — Alle Beta-Tester des Systems\n\n`;
helpText += `▸ ${PREFIX}marry @user — Verlobungsring überreichen\n`;
helpText += `▸ ${PREFIX}divorce — Ring zurückgeben\n`;
helpText += `▸ ${PREFIX}sticker — Bild/GIF antworten → Sticker craften\n`;
helpText += `▸ ${PREFIX}bewerbung — Gildenbeitritt beantragen\n`;
helpText += `▸ ${PREFIX}setinfo <feld> <wert> — Profilinfos setzen (name/alter/hobbys/sexualitaet)\n`;
helpText += `▸ ${PREFIX}sao — Zufälligen Sword Art Online Edit abspielen\n`;
        helpText += `⚔️ *ARENA & WIRTSCHAFT* (Cor & Kämpfe)\n${divider}\n`;
        helpText += `▸ ${PREFIX}daily — Tägliche Quest-Belohnung\n`;
        helpText += `▸ ${PREFIX}blackjack — Glücksspiel im Coliseum\n`;
        helpText += `▸ ${PREFIX}slot — Spielautomat in der Taverne\n`;
        helpText += `▸ ${PREFIX}fish — Angeln am Floor-See\n`;
        helpText += `▸ ${PREFIX}pet — Begleiter-Status prüfen\n`;
        helpText += `▸ ${PREFIX}adopt <name> — Begleiter zähmen\n`;
        helpText += `▸ ${PREFIX}feed — Begleiter füttern\n\n`;
        helpText += `⚔️ *ARENA-SYSTEM* (Ausrüstung & PVP)\n${divider}\n`;
        helpText += ARENA_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n') + '\n\n';
helpText += `🏰 *GILDEN-SYSTEM* (Verbünde)\n${divider}\n`;
        helpText += GUILD_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n') + '\n\n';
helpText += `🎖️ *TITEL & ERFOLGE*\n${divider}\n`;
helpText += TITLE_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n') + '\n\n';
helpText += `\n💞 *SOCIAL SKILLS* (Interaktion)\n${divider}\n`;
helpText += `▸ ${PREFIX}slap @user — Ohrfeige verpassen\n`;
helpText += `▸ ${PREFIX}hug @user — Umarmen\n`;
helpText += `▸ ${PREFIX}kiss @user — Küssen\n`;
helpText += `▸ ${PREFIX}pat @user — Tätscheln\n`;
helpText += `▸ ${PREFIX}poke @user — Anpiksen\n`;
helpText += `▸ ${PREFIX}cuddle @user — Kuscheln\n`;
helpText += `▸ ${PREFIX}bite @user — Beißen\n`;
helpText += `▸ ${PREFIX}punch @user — Schlagen\n`;
helpText += `▸ ${PREFIX}love @user — Lieben\n`;
helpText += `▸ ${PREFIX}blush @user — Erröten wegen jemandem\n`;
helpText += `▸ ${PREFIX}handhold @user — Hand halten\n`;
helpText += `▸ ${PREFIX}lick @user — Ablecken\n`;
helpText += `▸ ${PREFIX}nervous @user — Nervös wegen jemandem sein\n`;
helpText += `▸ ${PREFIX}throw @user — Werfen\n`;
helpText += `▸ ${PREFIX}sleep @user — Einschlafen neben\n`;
helpText += `▸ ${PREFIX}angrystare @user — Wütend anstarren\n`;
helpText += `▸ ${PREFIX}bleh @user — Zunge rausstrecken\n`;
helpText += `▸ ${PREFIX}confused @user — Verwirrt sein wegen\n`;
helpText += `▸ ${PREFIX}cry @user — Weinen wegen\n`;
helpText += `▸ ${PREFIX}evillaugh @user — Böse lachen\n`;
helpText += `▸ ${PREFIX}facepalm @user — Facepalm machen\n`;
helpText += `▸ ${PREFIX}happy @user — Glücklich sein wegen\n`;
helpText += `▸ ${PREFIX}laugh @user — Lachen\n`;
helpText += `▸ ${PREFIX}mad @user — Sauer sein wegen\n`;
helpText += `▸ ${PREFIX}nuzzle @user — Anschmiegen\n`;
helpText += `▸ ${PREFIX}no @user — Nein sagen zu\n`;
helpText += `▸ ${PREFIX}nosebleed @user — Nasenbluten wegen\n`;
helpText += `▸ ${PREFIX}sad @user — Traurig sein wegen\n`;
helpText += `▸ ${PREFIX}scared @user — Angst haben wegen\n`;
helpText += `▸ ${PREFIX}shout @user — Anschreien\n`;
helpText += `▸ ${PREFIX}shy @user — Schüchtern sein wegen\n`;
helpText += `▸ ${PREFIX}sneeze @user — Niesen\n`;
helpText += `▸ ${PREFIX}surprised @user — Überrascht sein wegen\n`;
helpText += `▸ ${PREFIX}tired @user — Müde sein wegen\n`;
helpText += `▸ ${PREFIX}yes @user — Ja sagen zu\n\n`;

helpText += `💀 *FUN & ACTION SKILLS* (Giphy-Reactions)\n${divider}\n`;
helpText += `▸ ${PREFIX}kill @user — Erledigen\n`;
helpText += `▸ ${PREFIX}yeet @user — Yeeten\n`;
helpText += `▸ ${PREFIX}nuke @user — Nuken\n`;
helpText += `▸ ${PREFIX}banish @user — Verbannen\n`;
helpText += `▸ ${PREFIX}stab @user — Durchbohren\n`;
helpText += `▸ ${PREFIX}smash @user — Zerschmettern\n`;
helpText += `▸ ${PREFIX}vaporize @user — Pulverisieren\n`;
helpText += `▸ ${PREFIX}choke @user — Würgen\n`;
helpText += `▸ ${PREFIX}kick @user — Treten\n`;
helpText += `▸ ${PREFIX}spin @user — Herumwirbeln\n`;
helpText += `▸ ${PREFIX}glare @user — Böse anstarren\n`;
helpText += `▸ ${PREFIX}smirk @user — Süffisant grinsen\n`;
helpText += `▸ ${PREFIX}highfive @user — High Five geben\n`;
helpText += `▸ ${PREFIX}dance @user — Zusammen tanzen\n\n`;
        helpText += `💬 *GILDEN-CHAT* (Chat & Gruppen)\n${divider}\n`;
        helpText += `▸ ${PREFIX}gi — Gildeneinstellungen anzeigen\n`;
        helpText += `▸ ${PREFIX}welcome-an / -aus — Willkommens-Portal an/aus\n`;
        helpText += `▸ ${PREFIX}welcome-set <text> — Willkommenstext setzen\n`;
        helpText += `▸ ${PREFIX}antilink-an / -aus — Anti-Fremdportal-Bann an/aus\n`;
        helpText += `▸ ${PREFIX}hidetag <text> — Nachricht mit verstecktem Tag\n`;
        helpText += `▸ ${PREFIX}delete — Als Reply: Nachricht löschen\n`;
        helpText += `▸ ${PREFIX}ytmp3 <link> — YouTube als MP3\n\n`;
helpText += `▸ ${PREFIX}nachtsperre an <HH:MM> <HH:MM> — Zeitgesteuerte Gildensperre\n`;
helpText += `▸ ${PREFIX}nachtsperre aus / status — Sperre verwalten\n`;

        helpText += `⚙️ *Aktueller System-Befehl:* ${PREFIX}\n`;

        if (isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) {
          helpText += `\n🎫 *KNIGHTS OF THE BLOOD SUPPORT* (Ticket-System)\n${divider}\n`;
          helpText += `▸ ${PREFIX}support <nachricht> — Notfall-Ticket erstellen\n`;
          helpText += `▸ ${PREFIX}answer <id> <text> — Ticket beantworten\n`;
          helpText += `▸ ${PREFIX}tickets [id|status] — Tickets anzeigen\n`;
          helpText += `▸ ${PREFIX}cleartickets — Alle Tickets löschen\n`;
        }

        helpText += `\n🛡️ *GILDENMEISTER* (Admin)\n${divider}\n`;
        helpText += `▸ ${PREFIX}warn @user — Verwarnen\n`;
        helpText += `▸ ${PREFIX}kick @user — Aus der Gilde werfen\n`;
        helpText += `▸ ${PREFIX}promote / ${PREFIX}demote @user — Gildenadmin-Rechte\n`;
        helpText += `▸ ${PREFIX}addxp <@user> <menge> — EXP schenken\n`;
        helpText += `▸ ${PREFIX}addcash <@user> <menge> — Cor schenken\n`;
        helpText += `▸ ${PREFIX}addvip <@user> <zeit> — VIP-Rang geben\n`;
        helpText += `▸ ${PREFIX}purge [anzahl] — Nachrichten löschen (alle oder letzte Nachrichten)\n`;

        if (hasAdminPerms(sender)) {
          helpText += `\n👑 *SYSTEM ADMINISTRATOR* (Kayaba-Rechte)\n${divider}\n`;
          helpText += `▸ ${PREFIX}broadcast <text> — Serverweite Ansage an alle Gilden\n`;
          helpText += `▸ ${PREFIX}restart — System neu starten\n`;
          helpText += `▸ ${PREFIX}updateprofile — Avatar aktualisieren\n`;
          helpText += `▸ ${PREFIX}bancmd <befehl> [ban|unban] — Skill sperren\n`;
          helpText += `▸ ${PREFIX}setrole @user <rolle> — Rang setzen\n`;
          helpText += `▸ ${PREFIX}listroles — Alle Ränge anzeigen\n`;
          helpText += `▸ ${PREFIX}newsession <name> — Neuen Server starten\n`;
          helpText += `▸ ${PREFIX}sessions — Aktive Server anzeigen\n`;
          helpText += `▸ ${PREFIX}stopsession <name> — Server stoppen\n`;
          helpText += `▸ ${PREFIX}deletesession <name> — Server löschen\n`;
          helpText += `▸ ${PREFIX}addcredit Name | Rolle — Beta-Tester hinzufügen\n`;
          helpText += `▸ ${PREFIX}delcredit <nummer> — Beta-Tester entfernen\n`;
          helpText += `▸ ${PREFIX}com <link> — Gilden-Link ändern\n`;
          helpText += `▸ ${PREFIX}usertodo — Von Spielern vorgeschlagene Skills ansehen\n`;
        }

        helpText += `\n${divider}\n_⚔️ "The days of my life... I'll cut through them all." — Nutze Befehle ohne Parameter für mehr Info_`;
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
  'slot', 'rps', 'fish', 'adopt', 'pet', 'petinfo', 'feed', 'play'
];
// GAMES an/aus Check 
if (isGroup && GAME_COMMANDS.includes(cmd)) {
  const gamesEnabled = groupSettings[from]?.games?.enabled !== false; // Default: an
  if (!gamesEnabled) {
    return send(`🎮 Spiele sind in dieser Gruppe deaktiviert. Ein Admin kann sie mit ${activePrefix}games-an wieder aktivieren.`);
  }
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

      // Welcome Controls
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
      // Ticket answer
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

 // Alle JIDs in ranks.json und users.json speichern
normalizedJids.forEach(jid => {
  // ranks.json aktualisieren
  ranks[jid] = roleUpper;

  // users.json aktualisieren (User anlegen falls nötig)
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

        // Aus anderen ROLES-Arrays entfernen (keine veralteten Einträge)
        for (const otherRole of Object.keys(ROLES)) {
          if (otherRole === roleUpper) continue;
          ROLES[otherRole] = (ROLES[otherRole] || []).filter(
            id => !normalizedJids.some(jid => isSameJid(id, jid))
          );
        }

        // Zur Ziel-Rolle hinzufügen (Duplikate vermeiden)
        if (!ROLES[roleUpper]) ROLES[roleUpper] = [];
        for (const jid of normalizedJids) {
          if (!ROLES[roleUpper].some(id => isSameJid(id, jid))) {
            ROLES[roleUpper].push(jid);
          }
        }

        protectPrimaryOwner();

        // Alles persistieren
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

// RESET COINS
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
      // LISTROLES
if (cmd === 'listroles') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');

  const roleOrder = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'VIP', 'SUPPORTER', 'TEST_SUPPORTER'];

  // Schritt 1: alle rohen JIDs pro Rolle sammeln (noch ohne Dedupe)
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

  // Schritt 2: @lid-Einträge über die ECHTE LID->Telefonnummer-Zuordnung auflösen
  // (nicht per Suffix-Tausch, sondern über die offizielle Mapping-Funktion),
  // damit dieselbe Person aus @lid- und @s.whatsapp.net-Quellen zusammengeführt wird.
  const grouped = {};
  for (const role of Object.keys(rawEntries)) {
    grouped[role] = new Map(); // canonicalRaw -> { displayJid, sourceJids: Set }
    for (const n of rawEntries[role]) {
      let canonicalRaw = extractRawNumber(n);
      let displayJid = n;

      if (n.endsWith('@lid')) {
        const resolved = await resolvePhoneJid(n, sock);
        if (resolved) {
          canonicalRaw = extractRawNumber(resolved);
          displayJid = resolved;
        }
        // Falls keine Auflösung möglich ist (z.B. noch kein Signal-Session mit
        // dieser Person aufgebaut), bleibt es bei der rohen LID — das ist dann
        // technisch bedingt und kein Bug.
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
      // Name über jede bekannte Quell-JID suchen (falls users.json unter der
      // anderen JID-Form gespeichert ist)
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
      // APPLYROLES
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

      // UNBANCMD
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

  // 🎖️ Titel
  const activeTitleObj = TITLES.find(t => t.id === user.activeTitle);
  const titleLine = activeTitleObj
    ? `🎖️ Titel: ${activeTitleObj.icon} "${activeTitleObj.name}"`
    : '🎖️ Titel: Keiner';

  // ⚔️ Ausrüstung — nur Name + Seltenheits-Icon, KEINE ID, KEINE Stärke
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

  // Fallback-Bild, falls kein echtes Profilbild gefunden wird
  const FALLBACK_PP_URL = 'https://raw.githubusercontent.com/Marlon9511/Sword-art-online-bot/main/5d553cd8911378163e989839dff229f3.webp.jpg';

  try {
    const candidates = [];

    // Echte Nummer über die offizielle LID->PN-Zuordnung auflösen (falls sender eine @lid ist)
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

    // Kein echtes Profilbild gefunden -> Fallback nutzen
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

      // PING
      if (cmd === 'ping') {
        const startTime = Date.now();
        await send('🏓 Pong!');
        return send(`Antwortzeit: ${Date.now() - startTime}ms`);
      }

      // OWNER
      if (cmd === 'owner') {
        return send('👑 Kontaktiere den Owner:\nhttps://wa.me/4915111254435');
      }

      // COM — Offizielle Gruppe anzeigen / Link ändern
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
// BEWERBUNG
if (cmd === 'bewerbung' || cmd === 'bewerben' || cmd === 'apply') {
  if (pendingApplications.has(sender)) {
    return send('⚔️ Du hast bereits eine offene Bewerbung. Beantworte die letzte Frage oder schreibe "abbrechen".');
  }
  pendingApplications.set(sender, { step: 0, answers: {} });
  await sock.sendMessage(from, { text: APPLICATION_STEPS[0].question });
  return;
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

      // SESSIONS
      if (cmd === 'sessions') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) return send('❌ Kein Zugriff.');
        const list = [...activeSessions.keys()].map(name => {
          const s = activeSessions.get(name);
          const id = s?.user?.id || '(verbindet...)';
          return `• ${name} — ${id}`;
        }).join('\n') || '(keine aktiven Sessions)';
        return send(`📱 Aktive Sessions:\n${list}`);
      }

      // STOPSESSION
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

      // DELETESESSION — Session stoppen UND komplett von der Platte löschen
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
// MARRY
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

// DIVORCE
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
      // HIDETAG
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

      // DELETE / DEL — Nachricht löschen (per Reply auf die Nachricht)
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
          }
          let permitted = isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD']);
          if (!permitted) {
            const groupMetadata = await getGroupMetaSafe(from);
            const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
            permitted = !!isGroupAdmin;
          }
          if (!permitted) return send('❌ Du darfst nur eigene Nachrichten oder (als Admin) fremde Nachrichten löschen.');
        }

        const key = {
          remoteJid: from,
          id: ctx.stanzaId,
          fromMe: isOwnMessage,
          ...(isGroup ? { participant: quotedParticipant } : {})
        };

        try {
          await sock.sendMessage(from, { delete: key });
          return;
        } catch (e) {
          console.error('[delete] Fehler:', e);
          return send('❌ Löschen fehlgeschlagen (ich brauche dafür ggf. Admin-Rechte in der Gruppe, wenn es nicht meine eigene Nachricht ist).');
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

      // FISH — Angeln in den Gewässern von Aincrad
      if (cmd === 'fish') {
        const FISH_EVENTS = [
          // ---- Floor 1: Stadt der Anfänge — Fluss ----
          { chance: 20, rarity: 'common',    text: '🐟 *Flusswels* aus dem Fluss der Stadt der Anfänge gefangen! (+8 Coins)', coins: 8 },
          { chance: 16, rarity: 'common',    text: '🐠 *Blauschuppen-Barsch* gefangen! (+15 Coins)', coins: 15, xp: 3 },
          { chance: 12, rarity: 'uncommon',  text: '🐡 *Kugelfisch der ersten Ebene* gefangen! (+25 Coins)', coins: 25, xp: 5 },

          // ---- Floor 22: Kristallwald-See ----
          { chance: 10, rarity: 'uncommon',  text: '💎 *Kristallforelle* aus dem Kristallwald-See schimmert in der Sonne! (+30 Coins)', coins: 30, xp: 8 },
          { chance: 7,  rarity: 'rare',      text: '🌫️ *Nebelaal* (Floor 35) aus dem Sumpf gezogen! (+45 Coins)', coins: 45, xp: 10 },

          // ---- Comedic / kleine Mob-Drops ----
          { chance: 8,  rarity: 'common',    text: '🦀 *Kobold-Krebs* hat sich in der Angel verheddert. (+5 Coins)', coins: 5 },
          { chance: 5,  rarity: 'rare',      text: '🐍 *Riesenaal* zerrt dich fast von der Plattform, gibt aber auf! (+40 Coins)', coins: 40, xp: 8 },

          // ---- Höhere Floors ----
          { chance: 4,  rarity: 'epic',      text: '🦈 *Sturmhai* (Floor 50) durchbricht die Wasseroberfläche! (+90 Coins)', coins: 90, xp: 20 },
          { chance: 3,  rarity: 'epic',      text: '🎐 *Leuchtqualle der Tiefen* (Floor 75) taucht schimmernd auf! (+70 Coins)', coins: 70, xp: 15 },
          { chance: 1.5, rarity: 'legendary', text: '🐉 *Drachenkarpfen* (Floor 90) — ein legendärer Feldboss-Fisch! (+180 Coins)', coins: 180, xp: 35 },
          { chance: 0.4, rarity: 'legendary', text: '🐋 *Der Weiße Wal von Aincrad* — eine Systemlegende wird wahr! JACKPOT! (+600 Coins)', coins: 600, xp: 120 },

          // ---- Schätze & Botschaften ----
          { chance: 3,  rarity: 'rare',      text: '📦 *Schatztruhe* am Grund des Sees entdeckt! (+200 Coins)', coins: 200, xp: 10 },
          { chance: 2,  rarity: 'uncommon',  text: '📜 *Flaschenpost eines gefallenen Spielers* gefunden. (+50 Coins)', coins: 50, xp: 20 },

          // ---- Item-Drops ----
          { chance: 2,  rarity: 'epic',      text: '⚔️ Im Netz verfangen: eine *SAO-Ausrüstungskiste*! Nutze {P}openkiste, um sie zu öffnen.', item: 'kiste', itemQty: 1, xp: 15 },
          { chance: 3,  rarity: 'uncommon',  text: '💊 Eine *Heiltrank-Flasche* trieb vorbei und wurde eingesammelt.', item: 'potion', itemQty: 1 },
          { chance: 3,  rarity: 'uncommon',  text: '🎁 Eine *mysteriöse Kiste* hing im Schilf fest.', item: 'box', itemQty: 1 },

          // ---- Nieten / Pech ----
          { chance: 10, rarity: 'common',    text: '🌿 Nur Algen gefangen... Aincrad ist manchmal enttäuschend.' },
          { chance: 8,  rarity: 'common',    text: '💨 Ein Fisch hat deinen Köder gestohlen und ist geflüchtet!' },
          { chance: 4,  rarity: 'common',    text: '💦 Du bist ausgerutscht und ins eiskalte Wasser gefallen!' },
          { chance: 3,  rarity: 'common',    text: '🦶 Ein *Kobold* hat dein Boot umgestoßen! (-30 Coins)', coins: -30 },
          { chance: 2,  rarity: 'common',    text: '🪝 Deine Angel ist an einem Stein zerbrochen! (-10 Coins)', coins: -10 }
        ];

        const totalWeight = FISH_EVENTS.reduce((sum, e) => sum + e.chance, 0);
        let random = Math.random() * totalWeight;
        let selectedEvent = FISH_EVENTS[FISH_EVENTS.length - 1];
        for (const event of FISH_EVENTS) {
          random -= event.chance;
          if (random <= 0) { selectedEvent = event; break; }
        }

        ensureUser(sender);

        if (selectedEvent.coins) {
          users[sender].coins = Math.max(0, (users[sender].coins || 0) + selectedEvent.coins);
        }
        if (selectedEvent.xp) {
          users[sender].xp = (users[sender].xp || 0) + selectedEvent.xp;
        }
        if (selectedEvent.item) {
          if (!users[sender].items) users[sender].items = {};
          users[sender].items[selectedEvent.item] = (users[sender].items[selectedEvent.item] || 0) + (selectedEvent.itemQty || 1);
        }
        save(FILES.users, users);

        const rarityTag = selectedEvent.rarity ? (RARITY_INFO[selectedEvent.rarity]?.emoji || '') : '';
        const header = `🎣 *— ANGELN IN AINCRAD —* 🎣\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
        const footer = `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`;
        const bodyText = selectedEvent.text.replace(/\{P\}/g, activePrefix);

        return send(`${header}${rarityTag ? rarityTag + ' ' : ''}${bodyText}${footer}`);
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
// PURGE / CLEARCHAT — löscht alle (oder die letzten N) bekannten Nachrichten der Gruppe
if (cmd === 'purge' || cmd === 'clearchat') {
  if (!isGroup) return send('❌ Nur in Gruppen.');
  if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
    const groupMetadata = await getGroupMetaSafe(from);
    const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
    if (!isGroupAdmin) return send('❌ Du musst Admin sein, um diesen Befehl zu nutzen.');
  }

  const fullHistory = groupMessageHistory.get(from) || [];
  if (!fullHistory.length) {
    return send('ℹ️ Keine gespeicherten Nachrichten zum Löschen vorhanden (ich kann nur Nachrichten löschen, die ich seit meinem Start gesehen habe).');
  }

  // Optionales Limit: $purge 50 löscht nur die letzten 50 Nachrichten
  let limit = null;
  if (args[0]) {
    const parsed = parseInt(args[0]);
    if (isNaN(parsed) || parsed <= 0) {
      return send(`❌ Ungültige Zahl. Nutzung: ${PREFIX}purge [anzahl]\nBeispiel: ${PREFIX}purge 50`);
    }
    limit = parsed;
  }

  // Bei Limit: nur die letzten N Einträge (neueste zuerst gelöscht, wie gewünscht)
  const toDelete = limit ? fullHistory.slice(-limit) : fullHistory;
  const remaining = limit ? fullHistory.slice(0, -limit) : [];

  await send(`🧹 Lösche ${toDelete.length} Nachricht(en), bitte warten...`);

  let deleted = 0, failed = 0;
  for (const entry of toDelete) {
    try {
      const isOwnMsg = !entry.participant;
      await sock.sendMessage(from, {
        delete: {
          remoteJid: from,
          id: entry.id,
          fromMe: isOwnMsg,
          ...(isOwnMsg ? {} : { participant: entry.participant })
        }
      });
      deleted++;
      await sleep(200); // kleine Pause, um Rate-Limits zu vermeiden
    } catch (e) {
      failed++;
    }
  }

  groupMessageHistory.set(from, remaining);
  return send(`✅ Fertig: ${deleted} Nachricht(en) gelöscht, ${failed} fehlgeschlagen (z.B. schon gelöscht oder zu alt).`);
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
      const SHOP = {
        potion: { price: 100, desc: 'Heilt 10 HP' },
        box: { price: 500, desc: 'Zufälliger Gegenstand' },
        vip: { price: 2000, desc: 'VIP-Rang' },
        [ARENA_SHOP_ITEM.id]: { price: ARENA_SHOP_ITEM.price, desc: ARENA_SHOP_ITEM.desc }
      };
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
      if (cmd === 'blackjack' || cmd === 'bjstart' || cmd === 'bj') {
        if (!users[sender]) ensureUser(sender);
        if (users[sender].bj?.active) return send('Du hast bereits ein aktives Spiel. Nutze $hit oder $stand.');
        const player = [bjDraw(), bjDraw()];
        const dealer = [bjDraw(), bjDraw()];
        users[sender].bj = { player, dealer, active: true };
        save(FILES.users, users);
        return send(`🃏 Blackjack!\nDeine Karten: ${player.map(c => c.value + c.suit).join(', ')}\nDealer zeigt: ${dealer[0].value + dealer[0].suit}\nNutze ${PREFIX}hit oder ${PREFIX}stand`);
      }
      if (cmd === 'hit') {
        if (!users[sender]) ensureUser(sender);
        if (!users[sender].bj?.active) return send(`Kein aktives Spiel. Starte mit ${PREFIX}blackjack`);
        const bj = users[sender].bj;
        bj.player.push(bjDraw());
        const p = bjScore(bj.player);
        let out = `Deine Karten: ${bj.player.map(c => c.value + c.suit).join(', ')}\nPunkte: ${p}`;
        if (p > 21) {
          out += '\n😢 Bust! Du verlierst.';
          delete users[sender].bj;
        } else if (p === 21) {
          users[sender].coins = (users[sender].coins || 0) + 75;
          users[sender].xp = (users[sender].xp || 0) + 40;
          out += '\n🎉 Blackjack! Du gewinnst +75 Coins +40 XP';
          delete users[sender].bj;
        } else {
          out += '\nNutze $hit oder $stand';
        }
        save(FILES.users, users);
        return send(out);
      }
      if (cmd === 'stand') {
        if (!users[sender]) ensureUser(sender);
        if (!users[sender].bj?.active) return send(`Kein aktives Spiel. Starte mit ${PREFIX}blackjack`);
        const bj = users[sender].bj;
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
          users[sender].coins = (users[sender].coins || 0) + 75;
          users[sender].xp = (users[sender].xp || 0) + 40;
          out += '\n🎉 Du gewinnst! +75 Coins +40 XP';
        } else if (p === d) {
          out += '\nUnentschieden';
        } else {
          out += '\nDealer gewinnt';
        }
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
            text: `🎫 Neues Ticket #${ticketId}\nVon: ${await getNumberMention(sender, sock)}\n\nNachricht:\n${text}`,
            mentions: [sender]
          });
          return send(`✅ Ticket #${ticketId} erstellt.`);
        } catch (e) {
          return send('❌ Fehler beim Erstellen des Tickets.');
        }
      }
      if (cmd === 'tickets') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) return send('Kein Zugriff.');
        const filter = (args[0] || '').toLowerCase();
        let ticket = null;

        if (filter) {
          ticket = tickets[filter] || tickets[filter.padStart(4, '0')];
        }

        if (ticket) {
          const messageText = ticket.message || ticket.text || 'Keine Nachricht';
          return send(
            `🎫 Ticket #${ticket.id}\n` +
            `Status: ${ticket.status}\n` +
            `Von: ${await getNumberMention(ticket.sender, sock)}\n` +
            `Nachricht: ${messageText}\n` +
            `Antwort: ${ticket.answer || 'Keine'}\n` +
            `Erstellt: ${new Date(ticket.timestamp).toLocaleString()}`,
            { mentions: [ticket.sender] }
          );
        }

        const ticketValues = Object.values(tickets);
        const visibleTickets = ticketValues.filter(t => !filter || filter === 'all' || t.status.toLowerCase() === filter);
        const mentions = [...new Set(visibleTickets.map(t => t.sender))];
        const listLines = await Promise.all(visibleTickets.map(async t => {
          const msg = String(t.message || t.text || '').slice(0, 50);
          return `${t.id} | ${t.status} | ${await getNumberMention(t.sender, sock)} | ${msg}${msg.length >= 50 ? '…' : ''}`;
        }));
        const list = listLines.join('\n') || '(keine)';
        const subtitle = filter ? ` (${filter === 'all' ? 'alle' : filter})` : '';
        return send(`🎫 Tickets${subtitle}:\n${list}`, { mentions });
      }
      if (cmd === 'closeticket') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) return send('Kein Zugriff.');
        const id = args[0];
        if (!id || !tickets[id]) return send('Usage: $closeticket <id>');
        tickets[id].status = 'closed';
        save(FILES.tickets, tickets);
        return send(`✅ Ticket ${id} geschlossen.`);
      }

      // CLEARTICKETS — alle Tickets auf einmal löschen, Zähler zurücksetzen
      if (cmd === 'cleartickets' || cmd === 'clearalltickets' || cmd === 'ticketsclear') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Kein Zugriff.');
        const count = Object.keys(tickets).length;
        if (!count) return send('ℹ️ Es sind bereits keine Tickets vorhanden.');

        for (const key of Object.keys(tickets)) delete tickets[key];
        save(FILES.tickets, tickets);
        ticketCounter = 0;

        return send(`✅ ${count} Ticket(s) wurden gelöscht. Zähler zurückgesetzt — das nächste Ticket beginnt wieder bei #0001.`);
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

      // USER TODOS — Befehlsvorschläge von Usern, nur Owner/CoOwner können die Liste einsehen
      if (cmd === 'usertodo' || cmd === 'usertodos') {
        const sub = (args[0] || '').toLowerCase();

        // usertodo add <text> — jeder registrierte User darf vorschlagen
        if (sub === 'add') {
          const text = args.slice(1).join(' ').trim();
          if (!text) return send(`❌ Nutzung: ${PREFIX}usertodo add <befehlsvorschlag>`);
          userTodoCounter++;
          const utId = `UT${String(userTodoCounter).padStart(3, '0')}`;
          userTodos[utId] = {
            id: utId,
            text,
            sender,
            status: 'open',
            created: Date.now()
          };
          save(FILES.userTodos, userTodos);
          return send(`✅ Dein Vorschlag wurde gespeichert (${utId})! Der Owner schaut sich das an.`);
        }

        // ?usertodo done <id> / ?usertodo remove <id> — nur Owner/CoOwner
        if (sub === 'done' || sub === 'complete') {
          if (!isOwner) return send('❌ Nur der Owner kann Vorschläge als erledigt markieren.');
          const id = args[1];
          if (!id || !userTodos[id]) return send(`Usage: ${PREFIX}usertodo done <id>`);
          userTodos[id].status = 'done';
          userTodos[id].doneBy = sender;
          save(FILES.userTodos, userTodos);
          return send(`✅ Vorschlag ${id} als erledigt markiert.`);
        }
        if (sub === 'remove' || sub === 'rm' || sub === 'delete') {
          if (!isOwner) return send('❌ Nur der Owner kann Vorschläge entfernen.');
          const id = args[1];
          if (!id || !userTodos[id]) return send(`Usage: ${PREFIX}usertodo remove <id>`);
          delete userTodos[id];
          save(FILES.userTodos, userTodos);
          return send(`🗑️ Vorschlag ${id} entfernt.`);
        }

        // ?usertodo (ohne Argument) oder ?usertodo list — nur Owner/CoOwner dürfen die Liste öffnen
        if (!sub || sub === 'list') {
          if (!isOwner) return send('❌ Nur der Owner kann sich die Vorschlagsliste ansehen. Nutze stattdessen: ' + PREFIX + 'usertodo add <text>');
          const all = Object.values(userTodos);
          if (!all.length) return send('📋 Es liegen noch keine User-Vorschläge vor.');
          const lines = await Promise.all(all.map(async t => {
            const who = await getNumberMention(t.sender, sock);
            const status = t.status === 'done' ? '✅' : '🕓';
            return `${status} ${t.id} — ${t.text}\n   von: ${who}`;
          }));
          const mentions = all.map(t => t.sender);
          return send(`📋 *Von Usern vorgeschlagene Befehle*\n\n${lines.join('\n\n')}\n\n_${PREFIX}usertodo done <id> / ${PREFIX}usertodo remove <id>_`, { mentions });
        }

        return send(`Usage: ${PREFIX}usertodo add <text>${isOwner ? ` | list | done <id> | remove <id>` : ''}`);
      }    

      // MODERATION
if (cmd === 'ban') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('Kein Zugriff.');

        const ctx = m.message?.extendedTextMessage?.contextInfo;
        let t = args[0];
        if ((!t || t === 'kick' || t === 'remove') && ctx?.mentionedJid?.length) t = ctx.mentionedJid[0];
        if ((!t || t === 'kick' || t === 'remove') && ctx?.participant) t = ctx.participant;

        if (!t) return send('Usage: $ban <@user|num|jid> [kick]');
        const jid = normalizeJid(t);
        if (isPrimaryOwner(jid)) return send('❌ Der Haupt-Owner ist geschützt und kann nicht gebannt werden.');
        const reason = args.slice(1).filter(a => a !== 'kick' && a !== 'remove' && !a.startsWith('@')).join(' ') || 'Kein Grund';
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
        return send(`🚫 @${jid.split('@')[0]} gebannt.`, { mentions: [jid] });
      }
      if (cmd === 'banlist') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('Kein Zugriff.');
        const list = Object.entries(bans).map(([j, b]) => `${j} — ${b.reason}`).join('\n') || '(keine)';
        return send(`🚫 Banliste:\n${list}`);
      }
      if (cmd === 'unban') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) return send('Kein Zugriff.');

        const ctx = m.message?.extendedTextMessage?.contextInfo;
        let t = args[0];
        if (!t && ctx?.mentionedJid?.length) t = ctx.mentionedJid[0];
        if (!t && ctx?.participant) t = ctx.participant;

        if (!t) return send('Usage: $unban <@user|num|jid>');
        const jid = normalizeJid(t);
        delete bans[jid];
        save(FILES.bans, bans);
        return send(`✅ @${jid.split('@')[0]} entbannt.`, { mentions: [jid] });
      }

      if (cmd === 'kick') {
        const ctx = m.message?.extendedTextMessage?.contextInfo;
        let target = args[0];
        if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
        if (!target) return send('Usage: $kick <num|jid|@user>');
        if (isPrimaryOwner(target)) return send('❌ Der Haupt-Owner ist geschützt und kann nicht gekickt werden.');

        let permitted = isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD']);
        let groupMetadata;
        if (!permitted && isGroup) {
          groupMetadata = await getGroupMetaSafe(from);
          const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
          permitted = !!isGroupAdmin;
        }
        if (!permitted) return send('Kein Zugriff.');

        if (!isGroup) return send('❌ Nur in Gruppen.');

        groupMetadata = groupMetadata || await getGroupMetaSafe(from);
        const normalizedTarget = normalizeJid(target);
        const rawId = target.replace(/^@/, '').split('@')[0];

        const targetParticipant = groupMetadata?.participants?.find(p => 
          isSameJid(p.id, normalizedTarget) || 
          (p.id || '').split('@')[0] === rawId
        );

        if (!targetParticipant) return send('❌ Benutzer nicht gefunden.');

        console.log('[kick-debug] Ziel:', { id: targetParticipant.id, jid: normalizedTarget, raw: rawId });

        try {
          await sock.groupParticipantsUpdate(from, [targetParticipant.id], 'remove');
          return send(`✅ @${targetParticipant.id.split('@')[0]} entfernt.`, { mentions: [targetParticipant.id] });
        } catch (e) {
          console.error('[kick] Fehler:', e?.message, e?.response?.status);
          if (e?.message?.includes('not admin') || e?.message?.includes('admin')) {
            return send('❌ Ich bin kein Gruppenadmin und kann niemanden kicken.');
          }
          return send('❌ Kicken fehlgeschlagen: ' + (e?.message || 'Unbekannter Fehler'));
        }
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
  if (!isGroup) return send('❌ Nur in Gruppen.');

  const groupMetadata = await getGroupMetaSafe(from, true);
  const senderJid = m.key.participant || m.key.remoteJid; // Absender-JID
  const senderIsGroupAdmin = isSenderGroupAdmin(groupMetadata, senderJid);

  if (!isOwner && !senderIsGroupAdmin) {
    return send('Nur Owner/Co-Owner oder Gruppenadmins dürfen promoten.');
  }

  const ctx = m.message?.extendedTextMessage?.contextInfo;
  let target = args[0];
  if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
  if (!target && ctx?.participant) target = ctx.participant;
  if (!target) return send('Usage: $promote <num|jid|@user>');

  const jid = normalizeJid(target);
  const rawId = jid.split('@')[0];
  const targetParticipant = groupMetadata?.participants?.find(p =>
    isSameJid(p.id, jid) || (p.id || '').split('@')[0] === rawId
  );
  if (!targetParticipant) return send('❌ Benutzer nicht in dieser Gruppe gefunden.');

  try {
    await sock.groupParticipantsUpdate(from, [targetParticipant.id], 'promote');
    groupMetaCache.delete(from);
    return send(`✅ @${targetParticipant.id.split('@')[0]} wurde zum Gruppenadmin befördert.`, { mentions: [targetParticipant.id] });
  } catch (e) {
    console.error('[promote] Fehler:', e?.message || e);
    return send('❌ Beförderung fehlgeschlagen (bin ich Gruppenadmin?).');
  }
}
// demote
if (cmd === 'demote') {
  if (!isGroup) return send('❌ Nur in Gruppen.');

  const groupMetadata = await getGroupMetaSafe(from, true);
  const senderJid = m.key.participant || m.key.remoteJid;
  const senderIsGroupAdmin = isSenderGroupAdmin(groupMetadata, senderJid);

  if (!isOwner && !senderIsGroupAdmin) {
    return send('Nur Owner/Co-Owner oder Gruppenadmins dürfen demoten.');
  }

  const ctx = m.message?.extendedTextMessage?.contextInfo;
  let target = args[0];
  if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
  if (!target && ctx?.participant) target = ctx.participant;
  if (!target) return send('Usage: $demote <num|jid|@user>');

  const jid = normalizeJid(target);
  const rawId = jid.split('@')[0];
  const targetParticipant = groupMetadata?.participants?.find(p =>
    isSameJid(p.id, jid) || (p.id || '').split('@')[0] === rawId
  );
  if (!targetParticipant) return send('❌ Benutzer nicht in dieser Gruppe gefunden.');

  try {
    await sock.groupParticipantsUpdate(from, [targetParticipant.id], 'demote');
    groupMetaCache.delete(from);

    // Internen ADMIN-Rang ebenfalls entfernen, falls gesetzt
    const normJid = normalizeJid(targetParticipant.id);
    if (ranks[normJid] === 'ADMIN') {
      ranks[normJid] = 'USER';
      save(FILES.ranks, ranks);
    }
    if (users[normJid] && users[normJid].rank === 'ADMIN') {
      users[normJid].rank = 'USER';
      save(FILES.users, users);
    }
    ROLES.ADMIN = (ROLES.ADMIN || []).filter(id => !isSameJid(id, normJid));

    return send(`✅ @${targetParticipant.id.split('@')[0]} wurde als Gruppenadmin entfernt.`, { mentions: [targetParticipant.id] });
  } catch (e) {
    console.error('[demote] Fehler:', e?.message || e);
    return send('❌ Herabstufung fehlgeschlagen (bin ich Gruppenadmin?).');
  }
}

      if (cmd === 'setrank') {
        if (!isOwner) return send('❌ Nur der Inhaber.');
        const r = (args[args.length - 1] || '').toUpperCase();
        if (!r) return send('Usage: $setrank <@mention|num|jid> <OWNER|COOWNER|ADMIN|MOD|VIP|USER>');

        const allowed = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'VIP', 'USER'];
        if (!allowed.includes(r)) return send('Ungültiger Rang.');

        let jid;
        const mentioned = m.message?.extendedTextMessage?.contextInfo?.mentionedJid;
        if (mentioned && mentioned.length > 0) {
          jid = mentioned[0];
        } else {
          jid = normalizeJid(args[0]);
        }

        if (!jid) return send('Usage: $setrank <@mention|num|jid> <OWNER|COOWNER|ADMIN|MOD|VIP|USER>');

        // Der Haupt-Owner ist geschützt: sein Rang kann NIE weggenommen werden.
        if (isPrimaryOwner(jid) && r !== 'OWNER') {
          return send('❌ Der Haupt-Owner ist geschützt und kann nicht heruntergestuft werden.');
        }

        if (r === 'OWNER') {
          // Bestehende Owner werden NICHT mehr entmachtet — der/die Haupt-Owner
          // bleibt in jedem Fall Owner. Diese Person wird zusätzlich Owner.
          ranks[jid] = 'OWNER';
          if (!ROLES.OWNER.some(id => isSameJid(id, jid))) ROLES.OWNER.push(jid);
        } else if (r === 'COOWNER') {
          for (const k of Object.keys(ranks)) { if (ranks[k] === 'COOWNER' && !isPrimaryOwner(k)) ranks[k] = 'USER'; }
          ranks[jid] = 'COOWNER'; COOWNER_LID = jid;
        } else {
          ranks[jid] = r;
        }

        protectPrimaryOwner();

        try {
          save(FILES.ranks, ranks);
          save(FILES.users, users);
          save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID, roles: ROLES });
        } catch (e) {}

        return send(`✅ Rang von ${await getNumberMention(jid, sock)} auf ${r} gesetzt.`, { mentions: [jid] });
      }
if (cmd === 'datadelete') {
        if (!isOwner) return send('❌ Nur der Inhaber.');
        const t = args[0]; if (!t) return send('Usage: $datadelete <num|jid>');
        const jid = normalizeJid(t);
        if (isPrimaryOwner(jid)) return send('❌ Der Haupt-Owner ist geschützt und kann nicht gelöscht/gebannt werden.');
        delete users[jid]; delete pets[jid]; delete ranks[jid];
        for (const rid of Object.keys(joinreqs)) {
          if (joinreqs[rid]?.sender && isSameJid(joinreqs[rid].sender, jid)) delete joinreqs[rid];
        }
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
    if (
      sender !== OWNER_LID &&
      sender !== OWNER_LID2 &&
      sender !== OWNER_PRIV &&
      sender !== OWNER_PRIV2 &&
      sender !== COOWNER_LID
    ) return send('⛔ Nur der Owner kann diesen Befehl nutzen.');
    await sock.groupParticipantsUpdate(from, [sender], 'promote');
    return send('🔰 Selfpromote ausgeführt.');
  } catch (e) {
    return send('❌ Selfpromote fehlgeschlagen.');
  }
}

if (cmd === 'selfdemote' || cmd === 'sd') {
  try {
    if (!from?.endsWith('@g.us')) return send('⚠ Nur in Gruppen.');
    if (
      sender !== OWNER_LID &&
      sender !== OWNER_LID2 &&
      sender !== OWNER_PRIV &&
      sender !== OWNER_PRIV2 &&
      sender !== COOWNER_LID
    ) return send('⛔ Nur der Owner kann diesen Befehl nutzen.');
    await sock.groupParticipantsUpdate(from, [sender], 'demote');
    return send('🔱 Selfdemote ausgeführt.');
  } catch (e) {
    return send('❌ Selfdemote fehlgeschlagen.');
  }
}

      // JOIN — Beitrittsanfragen: einreichen, annehmen (Team) und ablehnen (Team)
      if (cmd === 'join') {
        const sub = (args[0] || '').toLowerCase();

        // ---- join accept <session> [id] — Team nimmt eine gespeicherte Anfrage an ----
        if (sub === 'accept') {
          if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD'])) return send('❌ Nur Team (Owner/CoOwner/Mod) darf Beitrittsanfragen annehmen.');

          const sessionArg = args[1];
          if (!sessionArg) return send(`❌ Nutzung: ${activePrefix}join accept <session> [id]\nBeispiel: ${activePrefix}join accept default`);

          const targetSock = activeSessions.get(sessionArg);
          if (!targetSock) return send(`❌ Session "${sessionArg}" ist nicht aktiv. Aktive Sessions: ${[...activeSessions.keys()].join(', ') || '(keine)'}`);

          const idArg = args[2];
          let reqId, reqEntry;
          if (idArg) {
            reqEntry = joinreqs[idArg];
            reqId = idArg;
          } else {
            const sorted = Object.entries(joinreqs).sort((a, b) => (a[1].at || 0) - (b[1].at || 0));
            if (!sorted.length) return send('ℹ️ Keine offenen Beitrittsanfragen vorhanden.');
            [reqId, reqEntry] = sorted[0];
          }

          if (!reqEntry) return send(`❌ Anfrage "${idArg}" nicht gefunden.`);

          const reqLink = reqEntry.link;
          const match = reqLink.match(/chat\.whatsapp\.com\/(?:invite\/)?([A-Za-z0-9_-]+)/i);
          const code = match ? match[1] : reqLink.trim();
          if (!code) return send('❌ Konnte keinen gültigen Einladungscode aus dem gespeicherten Link extrahieren.');

          try {
            try {
              await targetSock.groupGetInviteInfo(code);
            } catch (infoErr) {
              const msg = infoErr?.message || String(infoErr);
              if (msg.includes('410') || msg.toLowerCase().includes('gone')) {
                return send('❌ Dieser Einladungslink ist ungültig oder abgelaufen.');
              }
              if (msg.includes('401') || msg.toLowerCase().includes('unauthorized')) {
                return send('❌ Kein Zugriff auf diesen Link (evtl. wurde der Bot bereits entfernt/blockiert).');
              }
            }

            const result = await targetSock.groupAcceptInvite(code);
            delete joinreqs[reqId];
            save(FILES.joinreq, joinreqs);

            try {
              await sock.sendMessage(JOIN_REQUEST_GROUP, {
                text: `✅ Anfrage #${reqId} angenommen — Session "${sessionArg}" ist der Gruppe beigetreten${result ? ` (${result})` : ''}.\nAngenommen von: @${sender.split('@')[0]}`,
                mentions: [sender]
              });
            } catch (e) {}

            try {
              await sock.sendMessage(reqEntry.sender, { text: `✅ Deine Beitrittsanfrage wurde angenommen! Der Bot (Session: ${sessionArg}) ist deiner Gruppe beigetreten.` });
            } catch (e) {}

            return send(`✅ Erfolgreich beigetreten${result ? `: ${result}` : ''} (Session: ${sessionArg})`);
          } catch (e) {
            console.error('[join accept] Fehler beim Beitritt:', e);
            const msg = e?.message || String(e);
            let explanation = msg;
            if (msg.includes('conflict') || msg.includes('409')) explanation = 'Bot ist bereits Mitglied dieser Gruppe.';
            else if (msg.includes('410') || msg.toLowerCase().includes('gone')) explanation = 'Der Link ist ungültig oder abgelaufen.';
            else if (msg.includes('401')) explanation = 'Kein Zugriff — evtl. wurde der Bot aus der Gruppe entfernt oder blockiert.';
            else if (msg.includes('429')) explanation = 'Zu viele Beitrittsversuche — bitte kurz warten und erneut versuchen.';
            return send(`❌ Beitritt fehlgeschlagen: ${explanation}`);
          }
        }

        // ---- join deny <id> — Team lehnt eine gespeicherte Anfrage ab ----
        if (sub === 'deny' || sub === 'decline' || sub === 'reject') {
          if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD'])) return send('❌ Nur Team (Owner/CoOwner/Mod) darf Beitrittsanfragen ablehnen.');
          const idArg = args[1];
          if (!idArg || !joinreqs[idArg]) return send(`❌ Nutzung: ${activePrefix}join deny <id>`);
          const entry = joinreqs[idArg];
          delete joinreqs[idArg];
          save(FILES.joinreq, joinreqs);
          try { await sock.sendMessage(entry.sender, { text: '❌ Deine Beitrittsanfrage wurde abgelehnt.' }); } catch (e) {}
          return send(`✅ Anfrage #${idArg} abgelehnt.`);
        }

        // ---- join list — offene Anfragen anzeigen (Team) ----
        if (sub === 'list') {
          if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD'])) return send('❌ Nur Team (Owner/CoOwner/Mod) darf die Liste einsehen.');
          const entries = Object.entries(joinreqs);
          if (!entries.length) return send('ℹ️ Keine offenen Beitrittsanfragen.');
          const lines = entries.map(([id, e]) => `${id} | von @${e.sender.split('@')[0]} | ${e.link}`);
          return send(`📋 Offene Beitrittsanfragen:\n${lines.join('\n')}`, { mentions: entries.map(([, e]) => e.sender) });
        }

        // ---- join <link> — Anfrage einreichen (an Team-Gruppe senden) ----
        const link = args[0];
        if (!link) {
          return send(
            `❌ Nutzung:\n${activePrefix}join <link> — Beitrittsanfrage einreichen\n${activePrefix}join accept <session> [id] — Anfrage annehmen (Team)\n${activePrefix}join deny <id> — Anfrage ablehnen (Team)\n${activePrefix}join list — Offene Anfragen anzeigen (Team)`
          );
        }
        if (!/^https?:\/\/chat\.whatsapp\.com\//i.test(link)) {
          return send('❌ Bitte gib einen gültigen WhatsApp-Gruppenlink an (https://chat.whatsapp.com/...).');
        }

        joinReqCounter++;
        const reqId = 'JR' + String(joinReqCounter).padStart(3, '0');
        joinreqs[reqId] = { id: reqId, sender, link, at: Date.now() };
        save(FILES.joinreq, joinreqs);

        try {
          await sock.sendMessage(JOIN_REQUEST_GROUP, {
            text: `📩 *Neue Beitrittsanfrage* #${reqId}\nVon: @${sender.split('@')[0]}\nLink: ${link}\n\nTeam kann annehmen mit:\n${activePrefix}join accept <session> ${reqId}`,
            mentions: [sender]
          });
        } catch (e) {
          console.error('[join] Konnte Anfrage nicht an Team-Gruppe senden:', e);
        }

        return send('✅ Deine Beitrittsanfrage wurde an das Team gesendet.');
      }
// leave
if (cmd === 'leave') {
  const isGroupChat = from?.endsWith('@g.us');

  // Fall 1: Privatchat -> JID als Argument nötig
  if (!isGroupChat) {
    if (!isOwner) return send('Kein Zugriff.'); // im Privatchat nur Owner, kein CoOwner

    const targetJid = args?.[0]?.trim();
    if (!targetJid || !targetJid.endsWith('@g.us')) {
      return send('Nutze: ?leave <gruppen-jid>\nBeispiel: ?leave 120363437195661019@g.us');
    }

    try {
      await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `Bot verlässt Gruppe: ${targetJid}` });
      await sock.groupLeave(targetJid);
      return send(`✅ Gruppe ${targetJid} verlassen.`);
    } catch (e) {
      return send('❌ Konnte Gruppe nicht verlassen (falsche JID oder Bot nicht Mitglied).');
    }
  }

  // Fall 2: Innerhalb einer Gruppe -> bisheriges Verhalten
  if (!(isOwner || isCoOwner)) return send('Kein Zugriff.');
  try {
    await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `Bot verlässt Gruppe: ${from}` });
    await sock.groupLeave(from);
    return;
  } catch (e) {
    return send('❌ Konnte Gruppe nicht verlassen.');
  }
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

      // PROFILE — persönliche Werte (unverändert wie bisher)
if (cmd === 'profile') {
  const u = users[sender];
  return send(`📊 Profil:\nLevel: ${u.level}\nXP: ${u.xp}\nCoins: ${u.coins}\nNachrichten: ${u.msgCount}`);
}

// STATS — System-weite Statistik über ALLE Sessions
if (cmd === 'stats') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
    return send(`❌ Kein Zugriff auf System-Statistiken. Nutze ${activePrefix}profile für deine persönlichen Werte.`);
  }

  await send('📊 Sammle System-Statistiken über alle Sessions, bitte warten...');

  const totalCommands = ALL_COMMANDS.length;
  const registeredUsersCount = Object.values(users).filter(u => u?.registered === true).length;
  const totalUsersCount = Object.keys(users).length;

  let totalGroups = 0;
  const perSessionLines = [];
  for (const [sName, sSock] of activeSessions.entries()) {
    try {
      const groups = await sSock.groupFetchAllParticipating();
      const count = Object.keys(groups || {}).length;
      totalGroups += count;
      perSessionLines.push(`  • ${sName}: ${count} Gruppen`);
    } catch (e) {
      perSessionLines.push(`  • ${sName}: ⚠️ Fehler beim Abrufen`);
    }
  }

  const out =
    `📊 *— SYSTEM-STATISTIKEN (AINCRAD) —* 📊\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `⚔️ Verfügbare Befehle: ${totalCommands}\n` +
    `📝 Registrierte Nutzer: ${registeredUsersCount}\n` +
    `👥 Bekannte Nutzer (gesamt): ${totalUsersCount}\n` +
    `🖥️ Aktive Sessions: ${activeSessions.size}\n` +
    `🏯 Gruppen (über alle Sessions): ${totalGroups}\n` +
    (perSessionLines.length ? `\n*Aufschlüsselung pro Session:*\n${perSessionLines.join('\n')}\n` : '') +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `_Datenbank: lokale JSON-Dateien in /data (kein SQL/NoSQL-Server)_`;

  return send(out);
}
      if (cmd === 'userinfo') {
        const t = args[0] ? normalizeJid(args[0]) : sender;
        ensureUser(t);
        const u = users[t];
        return send(`👤 ${t}\nLevel: ${u.level}\nXP: ${u.xp}\nCoins: ${u.coins}\nRank: ${ranks[t] || u.rank}`);
      }
// RANGLISTE (rein lesend, verändert niemals XP/Coins)
      if (cmd === 'rangliste' || cmd === 'leaderboard' || cmd === 'rank') {
        const sortBy = (args[0] || 'xp').toLowerCase();

        const validSort = ['xp', 'level', 'coins'];
        if (!validSort.includes(sortBy)) {
          return send(`❌ Nutzung: ${activePrefix}rangliste <xp|level|coins>\nBeispiel: ${activePrefix}rangliste coins`);
        }

        const entries = Object.entries(users).filter(([jid, u]) => u && typeof u === 'object');

        let sorted;
        if (sortBy === 'coins') {
          sorted = entries.sort((a, b) => (b[1].coins || 0) - (a[1].coins || 0));
        } else {
          // xp und level zusammen gewichten (wie bisheriger ?top)
          sorted = entries.sort((a, b) => ((b[1].level || 1) * 1000 + (b[1].xp || 0)) - ((a[1].level || 1) * 1000 + (a[1].xp || 0)));
        }

        const topList = sorted.slice(0, 10);
        if (!topList.length) return send('📊 Noch keine Spieler vorhanden.');

        const medals = ['🥇', '🥈', '🥉'];
        const lines = await Promise.all(topList.map(async ([jid, u], i) => {
          const displayName = u.name || u.registrationName || await getNumberMention(jid, sock);
          const rankIcon = medals[i] || `${i + 1}.`;
          if (sortBy === 'coins') {
            return `${rankIcon} ${displayName} — 💰 ${u.coins || 0} Coins`;
          }
          return `${rankIcon} ${displayName} — ⭐ Lv.${u.level || 1} (${u.xp || 0} XP)`;
        }));

        const mentions = topList.map(([jid]) => jid);
        const titleMap = { xp: '⚔️ XP-Rangliste', level: '⚔️ Level-Rangliste', coins: '💰 Coins-Rangliste' };

  return send(
          `${titleMap[sortBy]}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n🔥 Ihr seid die Besten! 🔥`,
          { mentions }
        );
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
        if (isPrimaryOwner(jid)) return send('❌ Der Haupt-Owner ist geschützt und kann nicht gebannt/entfernt werden.');
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

      // CREDITS
      if (cmd === 'credits') {
        if (!credits.list || credits.list.length === 0) {
          return send('📋 Noch keine Credits eingetragen.');
        }
        let out = '✨ *Credits* ✨\n\n';
        credits.list.forEach((c, i) => {
          out += `${i + 1}. *${c.name}* — ${c.role}\n`;
        });
        out += '\n❤️ Danke euch allen!';
        return send(out);
      }

      // ADDCREDIT
      if (cmd === 'addcredit') {
        if (!isAuthorized(sender, ['OWNER'])) return send('❌ Nur der Inhaber darf Credits hinzufügen.');
        const input = args.join(' ');
        const [name, role] = input.split('|').map(s => s?.trim());
        if (!name || !role) {
          return send(`❌ Nutzung: ${PREFIX}addcredit Name | Rolle\nBeispiel: ${PREFIX}addcredit Max | Coding Hilfe`);
        }
        credits.list.push({ name, role });
        save(FILES.credits, credits);
        return send(`✅ *${name}* wurde zu den Credits hinzugefügt.`);
      }
      // DELCREDIT
      if (cmd === 'delcredit') {
        if (!isAuthorized(sender, ['OWNER'])) return send('❌ Nur der Inhaber darf Credits entfernen.');
        const index = parseInt(args[0]) - 1;
        if (isNaN(index) || index < 0 || index >= credits.list.length) {
          return send(`❌ Ungültige Nummer. Nutze ${PREFIX}credits um die Liste mit Nummern zu sehen.`);
        }
        const removed = credits.list.splice(index, 1)[0];
        save(FILES.credits, credits);
        return send(`🗑️ *${removed.name}* wurde aus den Credits entfernt.`);
      }
 //dsgvo
if (cmd === 'dsgvo') {
  return send(DSGVO_TEXT.trim());
}


// Anti-Link Controls
      if ((cmd === 'antilink-an' || cmd === 'antilink-aus') && isGroup) {
        const groupMetadata = await getGroupMetaSafe(from);
        const senderIsGroupAdmin = isGroupAdminJid(groupMetadata, sender);

        if (!senderIsGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'GROUPADMIN'])) {
          return send('❌ Du musst Admin in dieser Gruppe sein.');
        }

        if (!groupSettings[from]) {
          groupSettings[from] = { welcome: { enabled: false, message: 'Willkommen in der Gruppe {user}! 👋' }, antilink: { enabled: false } };
        }
        if (!groupSettings[from].antilink) groupSettings[from].antilink = { enabled: false };

        if (cmd === 'antilink-an') {
          groupSettings[from].antilink.enabled = true;
          save(FILES.groupSettings, groupSettings);
          return send('✅ Anti-Link aktiviert: Wer einen WhatsApp-Link postet, wird gekickt (Admins ausgenommen).');
        }

        if (cmd === 'antilink-aus') {
          groupSettings[from].antilink.enabled = false;
          save(FILES.groupSettings, groupSettings);
          return send('✅ Anti-Link deaktiviert.');
        }
      }
// YTMP3
if (cmd === 'ytmp3') {
  const fullText = args.join(' ').trim();
  const urlMatch = fullText.match(/(https?:\/\/)?(www\.|music\.|m\.)?(youtube\.com\/(watch\?v=|shorts\/)|youtu\.be\/)[\w-]+(\S*)?/i);

  if (!urlMatch) {
    return send(`❌ Nutzung: ${PREFIX}ytmp3 <youtube-link>\n\nBeispiel:\n${PREFIX}ytmp3 https://youtu.be/dQw4w9WgXcQ`);
  }

  let url = urlMatch[0];
  if (!/^https?:\/\//i.test(url)) url = 'https://' + url;

  const cooldownMsg = checkCooldown(sender, 'ytmp3');
  if (cooldownMsg && !isOwner) return send(cooldownMsg);

  await send('⏳ Lade Audio herunter, bitte warten...');

  try {
    const title = await getYoutubeTitle(url);
    const filePath = await downloadYoutubeMp3(url);
    const stats = fs.statSync(filePath);

    if (stats.size > 95 * 1024 * 1024) {
      fs.unlinkSync(filePath);
      return send('❌ Die Datei ist zu groß für WhatsApp (>95MB).');
    }

    await sock.sendMessage(from, {
      audio: fs.readFileSync(filePath),
      mimetype: 'audio/mpeg',
      fileName: `${title || 'audio'}.mp3`
    }, { quoted: m });

    fs.unlinkSync(filePath);
  } catch (e) {
    console.error('[ytmp3] Fehler:', e?.message || e);
    return send('❌ Download fehlgeschlagen. Prüfe den Link oder versuche es später erneut.');
  }
  return;
}
const REACTION_COMMANDS = {
  throw:  { emoji: "🤾", verb: "wirft",                 apiReaction: "throw" },
  slap:   { emoji: "👋", verb: "verpasst eine Ohrfeige",  apiReaction: "slap" },
  hug:    { emoji: "🤗", verb: "umarmt",                 apiReaction: "hug" },
  kiss:   { emoji: "😘", verb: "küsst",                  apiReaction: "kiss" },
  pat:    { emoji: "🤚", verb: "tätschelt",              apiReaction: "pat" },
  poke:   { emoji: "👉", verb: "pikst",                  apiReaction: "poke" },
  cuddle: { emoji: "🥰", verb: "kuschelt mit",           apiReaction: "cuddle" },
  bite:   { emoji: "😬", verb: "beißt",                  apiReaction: "bite" },
  punch:  { emoji: "🥊", verb: "verpasst einen Schlag",   apiReaction: "punch" },
  sleep:  { emoji: "😴", verb: "schläft ein neben",       apiReaction: "sleep" },
  angrystare: { emoji: "😠", verb: "starrt wütend" },
  bleh: { emoji: "😝", verb: "streckt die Zunge raus" },
  confused: { emoji: "😕", verb: "ist verwirrt" },
  cry: { emoji: "😭", verb: "weint wegen " },
  evillaugh: { emoji: "😈", verb: "lacht böse" },
  facepalm: { emoji: "🤦", verb: "macht einen Facepalm" },
  happy: { emoji: "😊", verb: "ist glücklich" },
  laugh: { emoji: "😂", verb: "lacht" },
  mad: { emoji: "😡", verb: "ist sauer" },
  nuzzle: { emoji: "🥺", verb: "schmiegt sich an" },
  no: { emoji: "🙅", verb: "sagt Nein" },
  nosebleed: { emoji: "🩸", verb: "hat Nasenbluten" },
  sad: { emoji: "😢", verb: "ist traurig" },
  scared: { emoji: "😱", verb: "hat Angst" },
  shout: { emoji: "📢", verb: "schreit" },
  shy: { emoji: "🙈", verb: "ist schüchtern" },
  sneeze: { emoji: "🤧", verb: "niest" },
  surprised: { emoji: "😲", verb: "ist überrascht" },
  tired: { emoji: "😴", verb: "ist müde" },
  yes: { emoji: "🙆", verb: "sagt Ja" },
  love: { emoji: "❤️", verb: "liebt", apiReaction: "love" },
  blush: { emoji: "😳", verb: "wird rot wegen", apiReaction: "blush" },
  handhold: { emoji: "🤝", verb: "hält die Hand von", apiReaction: "handhold" },
  lick: { emoji: "👅", verb: "leckt", apiReaction: "lick" },
  nervous: { emoji: "😅", verb: "ist nervös wegen", apiReaction: "nervous" },

  // Zusätzliche Reactions über die Neko API (nekos.best) — kein API-Key nötig
  kill:      { emoji: "☠️", verb: "erledigt",              source: "neko", nekoCategory: "punch" },
  yeet:      { emoji: "🚀", verb: "yeetet",                 source: "neko", nekoCategory: "yeet" },
  nuke:      { emoji: "☢️", verb: "nukt",                   source: "neko", nekoCategory: "punch" },
  banish:    { emoji: "🌀", verb: "verbannt",               source: "neko", nekoCategory: "wave" },
  stab:      { emoji: "🗡️", verb: "durchbohrt",             source: "neko", nekoCategory: "punch" },
  smash:     { emoji: "🔨", verb: "zerschmettert",          source: "neko", nekoCategory: "punch" },
  vaporize:  { emoji: "💥", verb: "pulverisiert",           source: "neko", nekoCategory: "punch" },
  choke:     { emoji: "🫳", verb: "würgt",                  source: "neko", nekoCategory: "bite" },
  kick:      { emoji: "🦵", verb: "verpasst einen Tritt",   source: "neko", nekoCategory: "kick" },
  spin:      { emoji: "🌪️", verb: "wirbelt herum",          source: "neko", nekoCategory: "dance" },
  facepalm2: { emoji: "🤦", verb: "macht einen Facepalm",   source: "neko", nekoCategory: "facepalm" },
  glare:     { emoji: "👀", verb: "starrt böse an",         source: "neko", nekoCategory: "stare" },
  smirk:     { emoji: "😏", verb: "grinst süffisant",       source: "neko", nekoCategory: "smug" },
  cry2:      { emoji: "😭", verb: "heult wegen",            source: "neko", nekoCategory: "cry" },
  highfive:  { emoji: "🙌", verb: "gibt ein High Five",     source: "neko", nekoCategory: "highfive" },
  dance:     { emoji: "💃", verb: "tanzt mit",              source: "neko", nekoCategory: "dance" },
};

// ---- otakugifs (bestehend) ----
async function getReactionGifUrl(reaction) {
  const res = await fetch(`https://api.otakugifs.xyz/gif?reaction=${reaction}`);
  if (!res.ok) throw new Error(`otakugifs API-Fehler: ${res.status}`);
  const data = await res.json();
  return data.url;
}

// ---- Neko API (nekos.best) — kein API-Key nötig ----
async function getNekoGifUrl(category) {
  const url = `https://nekos.best/api/v2/${category}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Neko-API-Fehler: ${res.status}`);
  const data = await res.json();

  if (!data.results?.length) throw new Error("Keine Neko-API-Treffer gefunden");

  const pick = data.results[Math.floor(Math.random() * data.results.length)];
  return pick.url;
}

// ---- Router, wählt automatisch die richtige API je nach config.source ----
async function getGifUrl(cmd, config) {
  if (config.source === "neko") {
    return await getNekoGifUrl(config.nekoCategory || cmd);
  }
  // Default: otakugifs
  return await getReactionGifUrl(config.apiReaction || cmd);
}

// NEU: Cache-Ordner für die Gif->mp4 Konvertierung (analog zu YTMP3_CACHE_DIR)
const REACTION_GIF_CACHE_DIR = path.join(__dirname, 'cache', 'reaction-gifs');

// NEU: lädt das Gif runter und wandelt es mit ffmpeg in ein echtes,
// WhatsApp-kompatibles mp4 um. Das ist der eigentliche Fix -
// rohe .gif-Bytes als "video" zu schicken spielt bei WhatsApp nicht ab.
function fetchAndConvertGifToMp4(gifUrl) {
  return new Promise(async (resolve, reject) => {
    try {
      fs.mkdirSync(REACTION_GIF_CACHE_DIR, { recursive: true });

      const stamp = Date.now();
      const gifPath = path.join(REACTION_GIF_CACHE_DIR, `${stamp}.gif`);
      const mp4Path = path.join(REACTION_GIF_CACHE_DIR, `${stamp}.mp4`);

      const res = await fetch(gifUrl);
      if (!res.ok) throw new Error(`Gif-Download fehlgeschlagen: ${res.status}`);
      const arrBuf = await res.arrayBuffer();
      fs.writeFileSync(gifPath, Buffer.from(arrBuf));

      // scale auf max. 480px Breite = schneller/leichter auf Termux
      const cmd = `ffmpeg -y -i "${gifPath}" -movflags faststart -pix_fmt yuv420p -vf "scale='trunc(min(480,iw)/2)*2':'-2'" "${mp4Path}"`;

      exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
        try { fs.unlinkSync(gifPath); } catch (e) {}

        if (err) {
          console.error('[reaction-gif] ffmpeg-Fehler:', err.message);
          return reject(err);
        }

        try {
          const mp4Buffer = fs.readFileSync(mp4Path);
          fs.unlinkSync(mp4Path);
          resolve(mp4Buffer);
        } catch (readErr) {
          reject(readErr);
        }
      });
    } catch (e) {
      reject(e);
    }
  });
}

/* -----------------------------------------------------------
 * Command-Handler für alle Reaction-Commands
 * ---------------------------------------------------------*/

if (REACTION_COMMANDS[cmd]) {
  const config = REACTION_COMMANDS[cmd];

  const ctx = m.message?.extendedTextMessage?.contextInfo;
  const mentioned = ctx?.mentionedJid || [];
  const repliedTo = ctx?.participant;
  const target = mentioned[0] || repliedTo;

  if (!target) {
    return send(`❓ Wen soll ich ${cmd}en? Erwähne jemanden mit @user oder antworte auf seine Nachricht mit ${activePrefix}${cmd}`);
  }

  const targetJid = normalizeJid(target);
  ensureUser(sender);
  ensureUser(targetJid);

  try {
    const gifUrl = await getGifUrl(cmd, config);
    const mp4Buffer = await fetchAndConvertGifToMp4(gifUrl);

    if (!isTeamMember) {
      try { await sock.sendPresenceUpdate('composing', from); } catch (e) {}
      await sleep(1500);
      try { await sock.sendPresenceUpdate('paused', from); } catch (e) {}
    }

    await sock.sendMessage(from, {
      video: mp4Buffer,
      gifPlayback: true,
      mimetype: 'video/mp4',
      caption: `${config.emoji} @${sender.split('@')[0]} ${config.verb} @${targetJid.split('@')[0]}!`,
      mentions: [sender, targetJid],
    }, { quoted: m });
  } catch (err) {
    console.error(`[${cmd}] Fehler:`, err);
    return send('⚠️ Konnte gerade kein Gif holen, versuch\'s gleich nochmal.');
  }
  return;
}
// ---- NEU: Neko-Bilder (nekos.best) — png, nur für den Haupt-Owner ----
const NEKO_IMAGE_CATEGORIES = ['neko', 'waifu', 'husbando', 'kitsune'];
const NEKO_IMAGE_OWNER_JID = '27088878862400@lid';

async function getNekoImageUrl(category) {
  const url = `https://nekos.best/api/v2/${category}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Neko-API-Fehler: ${res.status}`);
  const data = await res.json();

  if (!data.results?.length) throw new Error("Keine Neko-API-Treffer gefunden");

  return data.results[0];
}

if (NEKO_IMAGE_CATEGORIES.includes(cmd)) {
  if (!isSameJid(sender, NEKO_IMAGE_OWNER_JID)) {
    return send('❌ Dieser Befehl ist nur für den Haupt-Owner verfügbar.');
  }

  try {
    const result = await getNekoImageUrl(cmd);
    let caption = `🖼️ *${cmd}*`;
    if (result.artist_name) caption += `\n🎨 Künstler: ${result.artist_name}`;
    if (result.source_url) caption += `\n🔗 Quelle: ${result.source_url}`;

    await sock.sendMessage(from, {
      image: { url: result.url },
      caption
    }, { quoted: m });
  } catch (err) {
    console.error(`[${cmd}] Fehler:`, err);
    return send('⚠️ Konnte gerade kein Bild holen, versuch\'s gleich nochmal.');
  }
  return;
}
// ---- Sticker-Teil unverändert ----
const STICKER_CACHE_DIR = path.join(__dirname, 'cache', 'stickers');

function bufferToSticker(inputBuffer, ext, isAnimated) {
  return new Promise((resolve, reject) => {
    fs.mkdirSync(STICKER_CACHE_DIR, { recursive: true });
    const stamp = Date.now();
    const inPath = path.join(STICKER_CACHE_DIR, `${stamp}_in.${ext}`);
    const outPath = path.join(STICKER_CACHE_DIR, `${stamp}_out.webp`);
    fs.writeFileSync(inPath, inputBuffer);

    const filter = "scale=512:512:force_original_aspect_ratio=increase,crop=512:512,fps=15";

    const cmd = isAnimated
      ? `ffmpeg -y -i "${inPath}" -vf "${filter}" -t 6 -loop 0 -an -vsync 0 -c:v libwebp -lossless 0 -qscale 60 -preset default "${outPath}"`
      : `ffmpeg -y -i "${inPath}" -vf "${filter}" -vframes 1 -c:v libwebp -lossless 1 -qscale 75 "${outPath}"`;

    exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
      try { fs.unlinkSync(inPath); } catch (e) {}
      if (err) return reject(err);
      try {
        const buf = fs.readFileSync(outPath);
        fs.unlinkSync(outPath);
        resolve(buf);
      } catch (e) { reject(e); }
    });
  });
}
async function addStickerExif(webpBuffer, packName, authorName) {
  const img = new webp.Image();
  await img.load(webpBuffer);

  const json = {
    'sticker-pack-id': 'sao-bot-' + Date.now(),
    'sticker-pack-name': packName,
    'sticker-pack-publisher': authorName,
    'emojis': ['⚔️']
  };

  const exifAttr = Buffer.from([
    0x49, 0x49, 0x2A, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x41, 0x57,
    0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00
  ]);
  const jsonBuffer = Buffer.from(JSON.stringify(json), 'utf-8');
  const exif = Buffer.concat([exifAttr, jsonBuffer]);
  exif.writeUIntLE(jsonBuffer.length, 14, 4);

  img.exif = exif;
  return await img.save(null);
}
if (cmd === 'add') {
  if (!isGroup) return send('❌ Nur in Gruppen.');
  if (!hasAdminPerms(sender)) return send('❌ Kein Zugriff.');

  const rawTarget = args[0];
  if (!rawTarget) return send(`❌ Nutzung: ${activePrefix}add <nummer>\nBeispiel: ${activePrefix}add 4915123456789`);

  const numberToAdd = rawTarget.replace(/[^0-9]/g, '');
  if (!numberToAdd || numberToAdd.length < 6) {
    return send('❌ Ungültige Nummer. Bitte mit Ländervorwahl angeben, z.B. 4915123456789 (ohne "+" oder Leerzeichen).');
  }
  const jid = `${numberToAdd}@s.whatsapp.net`;

  // Frische Metadaten holen, um sicherzugehen, dass der Admin-Status aktuell ist
  const meta = await getGroupMetaSafe(from, true);
  const allBotIds = [...getBotSelfIds(sock)];
  const botPart = (meta?.participants || []).find(p => {
    const pids = [p.id, p.id?.split('@')[0], `${p.id?.split('@')[0]}@s.whatsapp.net`].filter(Boolean).map(String);
    return pids.some(pid => allBotIds.includes(pid));
  });
  const botIsAdmin = !!(botPart?.admin === 'admin' || botPart?.admin === 'superadmin' || botPart?.admin === true || botPart?.isAdmin === true);

  if (!botIsAdmin) {
    return send('❌ Ich bin kein Administrator in dieser Gruppe. Bitte mache mich zuerst zum Admin.');
  }

  // Bereits Mitglied?
  const alreadyMember = (meta?.participants || []).some(p => {
    const raw = (p.id || '').split('@')[0];
    return raw === numberToAdd;
  });
  if (alreadyMember) return send(`ℹ️ ${numberToAdd} ist bereits Mitglied dieser Gruppe.`);

  try {
    const result = await sock.groupParticipantsUpdate(from, [jid], 'add');
    const entry = Array.isArray(result) ? result[0] : result;
    const status = String(entry?.status ?? '');

    if (status === '200') {
      return send(`✅ @${numberToAdd} wurde zur Gruppe hinzugefügt.`, { mentions: [jid] });
    }

    // Status 403 / 401 -> Nummer erlaubt kein direktes Hinzufügen (z.B. wegen Datenschutzeinstellungen).
    // In diesem Fall bietet Baileys oft einen invite_code im content-Feld an.
    let inviteCode = null;
    try {
      const content = entry?.content;
      if (Array.isArray(content)) {
        const inviteNode = content.find(c => c?.tag === 'add_request');
        inviteCode = inviteNode?.attrs?.code || null;
      }
    } catch (e) {}

    if (inviteCode) {
      try {
        const inviteLink = `https://chat.whatsapp.com/${inviteCode}`;
        await sock.sendMessage(jid, {
          text: `👋 Du wurdest eingeladen, der Gruppe "${meta?.subject || ''}" beizutreten:\n${inviteLink}`
        });
        return send(`ℹ️ ${numberToAdd} konnte nicht direkt hinzugefügt werden (Datenschutzeinstellung), aber ich habe eine private Einladung per Nachricht geschickt.`);
      } catch (e) {
        return send(`⚠️ ${numberToAdd} konnte nicht direkt hinzugefügt werden und die private Einladung ist fehlgeschlagen: ${e?.message || e}`);
      }
    }

    const statusMessages = {
      '403': 'Die Nummer erlaubt kein direktes Hinzufügen (Datenschutzeinstellungen) und akzeptiert auch keine automatische Einladung.',
      '408': 'Zeitüberschreitung — die Nummer ist eventuell nicht (mehr) auf WhatsApp registriert.',
      '409': 'Die Nummer ist bereits Mitglied.',
      '401': 'Die Nummer/der Account lässt sich grundsätzlich nicht kontaktieren (z.B. gesperrte oder spezielle System-Accounts).',
    };

    return send(`⚠️ Hinzufügen von ${numberToAdd} fehlgeschlagen.\nGrund: ${statusMessages[status] || `Status-Code ${status}`}`);
  } catch (e) {
    console.error('[add] Fehler:', e);
    const msg = e?.data === 463 || String(e?.message).includes('account_reachout_restricted')
      ? 'Diese Nummer/dieser Account lässt sich grundsätzlich nicht per automatischem Hinzufügen kontaktieren (z.B. spezielle System-/Business-Accounts wie MetaAI).'
      : (e?.message || 'Unbekannter Fehler');
    return send(`❌ Hinzufügen fehlgeschlagen: ${msg}`);
  }
}
// STICKER
if (cmd === 'sticker' || cmd === 's' || cmd === 'stiker') {
  const ctx = m.message?.extendedTextMessage?.contextInfo;

  let targetMsg = null;
  let mediaType = null;

  // Fall 1: Antwort auf Bild/GIF/Video/Sticker
  if (ctx?.quotedMessage) {
    const q = ctx.quotedMessage;
    const quotedKey = {
      remoteJid: from,
      id: ctx.stanzaId,
      fromMe: false,
      participant: ctx.participant
    };
    if (q.imageMessage) {
      targetMsg = { key: quotedKey, message: q };
      mediaType = 'image';
    } else if (q.videoMessage) {
      targetMsg = { key: quotedKey, message: q };
      mediaType = 'video';
    } else if (q.stickerMessage) {
      targetMsg = { key: quotedKey, message: q };
      mediaType = q.stickerMessage.isAnimated ? 'animatedSticker' : 'sticker';
    }
  }

  // Fall 2: Bild/Video direkt mit Befehl als Bildunterschrift
  if (!targetMsg && m.message.imageMessage) { targetMsg = m; mediaType = 'image'; }
  if (!targetMsg && m.message.videoMessage) { targetMsg = m; mediaType = 'video'; }

  if (!targetMsg) {
    return send('❓ Schick ein Bild/GIF direkt mit "' + activePrefix + cmd + '" als Bildunterschrift, oder antworte mit "' + activePrefix + cmd + '" auf ein Bild/Video/GIF.');
  }

  await send('⏳ Erstelle Sticker...');

  try {
    const buffer = await downloadMediaMessage(
      targetMsg,
      'buffer',
      {},
      { logger: P({ level: 'silent' }), reuploadRequest: sock.updateMediaMessage }
    );

    let webpBuffer;

    if (mediaType === 'sticker' || mediaType === 'animatedSticker') {
      // Ist bereits ein gültiger WhatsApp-Sticker (webp) -> keine ffmpeg-Konvertierung nötig,
      // die scheitert bei animierten Stickern am ffmpeg-WebP-Demuxer.
      webpBuffer = buffer;
    } else {
      const isAnimated = mediaType === 'video';
      const ext = (mediaType === 'video') ? 'mp4' : 'jpg';
      webpBuffer = await bufferToSticker(buffer, ext, isAnimated);
    }

    const customName = args.join(' ').trim();
    const packName = 'Sword Art Online Bot';
    const authorName = customName ? packName + ' | ' + customName : packName;

    webpBuffer = await addStickerExif(webpBuffer, packName, authorName);

    await sock.sendMessage(from, { sticker: webpBuffer }, { quoted: m });
  } catch (e) {
    console.error('[sticker] Fehler:', e);
    return send('❌ Sticker-Erstellung fehlgeschlagen. (ffmpeg/node-webpmux installiert?)');
  }
  return;
}
// SHOWUSER — Profil eines Users anzeigen (inkl. Registrierungsdatum & Ausrüstung)
if (cmd === 'showuser') {
  // 🔒 Berechtigungsprüfung: Team-Ränge + VIP dürfen diesen Command nutzen
  if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'VIP', 'SUPPORTER', 'TEST_SUPPORTER'])) {
    return send('🚫 Dieser Befehl ist nur für Team-Mitglieder und VIPs verfügbar.');
  }

  const ctx = m.message?.extendedTextMessage?.contextInfo;
  let target = args[0];
  if (!target && ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
  if (!target && ctx?.participant) target = ctx.participant;
  if (!target) target = sender; // ohne Angabe -> eigenes Profil

  const targetJid = normalizeJid(target);
  ensureUser(targetJid);
  arena.ensureArenaFields(users, targetJid); // stellt sicher, dass .equipped existiert
  const u = users[targetJid];

  const displayName = u.name || u.registrationName || targetJid.split('@')[0];
  const rank = ranks[targetJid] || u.rank || 'USER';
  const registriert = u.registered ? '✅ Ja' : '❌ Nein';
  const regDatum = u.registrationDate
    ? new Date(u.registrationDate).toLocaleString('de-DE', { dateStyle: 'long', timeStyle: 'short' })
    : '—';

  const infoLines = [];
  if (u.alter) infoLines.push('🎂 Alter: ' + u.alter);
  if (u.hobbys) infoLines.push('🎯 Hobbys: ' + u.hobbys);
  if (u.sexualitaet) infoLines.push('💫 Sexualität: ' + u.sexualitaet);
  const infoBlock = infoLines.length ? '\n' + infoLines.join('\n') : '';

  const marriage = marriages[targetJid];
  let marriageLine = '💍 Status: Single';
  if (marriage) {
    const partnerUser = users[marriage.partner] || {};
    const partnerName = partnerUser.name || partnerUser.registrationName || marriage.partner.split('@')[0];
    marriageLine = `💍 Verheiratet mit: ${partnerName}`;
  }

  // 🎖️ Titel
  const activeTitleObj = TITLES.find(t => t.id === u.activeTitle);
  const titleLine = activeTitleObj
    ? `🎖️ Titel: ${activeTitleObj.icon} "${activeTitleObj.name}"`
    : '🎖️ Titel: Keiner';

  // ⚔️ Ausrüstung aus dem Arena-System
  // Owner-exklusive / geheime Items (z.B. Excalibur) werden für alle
  // außer dem Haupt-Owner selbst als "Unbekannt" angezeigt, egal wer
  // das Profil ansieht oder wessen Profil es ist.
  const viewerIsPrimaryOwner = isPrimaryOwner(sender);

  const formatGearLineForShowuser = (itemId) => {
    if (!itemId) return '— (keine Ausrüstung)';
    const it = ITEM_DB[itemId];
    if (!it) return '— (unbekannt)';
    if ((it.ownerOnly || it.secret) && !viewerIsPrimaryOwner) {
      return '❓ Unbekannt';
    }
    return arena.formatItemLine(itemId, 1);
  };

  const weaponId = u.equipped?.weapon;
  const armorId = u.equipped?.armor;
  const weaponLine = formatGearLineForShowuser(weaponId);
  const armorLine = formatGearLineForShowuser(armorId);
  const gearBlock = `\n⚔️ *Ausrüstung*\n🗡️ Waffe: ${weaponLine}\n🛡️ Rüstung: ${armorLine}`;

  const caption =
    `👤 *Profil von ${displayName}*\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `🏅 Rang: ${prettyRank(rank)}\n` +
    `${titleLine}\n` +
    `⭐ Level: ${u.level || 1}\n` +
    `✨ XP: ${u.xp || 0}\n` +
    `💰 Coins: ${u.coins || 0}\n` +
    `📨 Nachrichten: ${u.msgCount || 0}\n` +
    `📝 Registriert: ${registriert}\n` +
    `📅 Registrierungsdatum: ${regDatum}\n` +
    `${marriageLine}${infoBlock}\n` +
    `${gearBlock}`;

  return send(caption, { mentions: [targetJid] });
}
// PARTNER (Gilden-Bündnisse anzeigen)
if (cmd === 'partner' || cmd === 'partners' || cmd === 'buendnisse') {
  if (!partners.list || partners.list.length === 0) {
    return send('⚔️ *— GILDEN-BÜNDNISSE —* ⚔️\n\nAktuell bestehen keine Bündnisse mit anderen Gilden.');
  }

  const divider = '⚔️┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈⚔️';
  let out = '⚔️ *— GILDEN-BÜNDNISSE —* ⚔️\n' + divider + '\n\n';
  partners.list.forEach((p, i) => {
    out += '🛡️ *' + p.name + '*\n🔗 ' + p.link + '\n\n';
  });
  out += divider + '\n_"Gemeinsam sind wir stärker." — Verbündete Gilden von AINCRAD_';
  return send(out);
}
// ADDPARTNER
if (cmd === 'addpartner') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
    return send('❌ Nur der Gildenmeister darf neue Bündnisse eingehen.');
  }

  const input = args.join(' ');
  const parts = input.split('|').map(s => s ? s.trim() : s);
  const name = parts[0];
  const link = parts[1];

  if (!name || !link) {
    return send(
      '❌ Nutzung: ' + activePrefix + 'addpartner Bot-Name | Link\n' +
      'Beispiel: ' + activePrefix + 'addpartner Elucidator-Bot | https://chat.whatsapp.com/XXXXXXXX'
    );
  }

  if (!/^https?:\/\//i.test(link)) {
    return send('❌ Bitte gib einen gültigen Link an (muss mit http:// oder https:// beginnen).');
  }

  partners.list.push({ name: name, link: link, addedBy: sender, at: Date.now() });
  save(FILES.partners, partners);

  return send('✅ Bündnis mit *' + name + '* wurde geschlossen und in die Gildenchronik eingetragen! ⚔️');
}

// DELPARTNER
if (cmd === 'delpartner') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
    return send('❌ Nur der Gildenmeister darf Bündnisse auflösen.');
  }

  const index = parseInt(args[0]) - 1;
  if (isNaN(index) || index < 0 || index >= partners.list.length) {
    return send('❌ Ungültige Nummer. Nutze ' + activePrefix + 'partner um die Liste mit Nummern zu sehen.');
  }

  const removed = partners.list.splice(index, 1)[0];
  save(FILES.partners, partners);
  return send('💔 Bündnis mit *' + removed.name + '* wurde aufgelöst.');
}
// NACHTSPERRE
if (cmd === 'nachtsperre' || cmd === 'quiethours') {
  if (!isGroup) return send('❌ Nur in Gruppen.');

  const groupMetadata = await getGroupMetaSafe(from);
  const isGroupAdmin = isGroupAdminJid(groupMetadata, sender);
  if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
    return send('❌ Du musst Admin in dieser Gruppe sein.');
  }

  const sub = (args[0] || '').toLowerCase();

  if (sub === 'aus' || sub === 'off') {
    delete groupLockSchedules[from];
    save(FILES.groupLockSchedule, groupLockSchedules);
    // Zur Sicherheit sofort wieder entsperren, falls sie gerade gesperrt ist
    try { await sock.groupSettingUpdate(from, 'not_announcement'); } catch (e) {}
    return send('✅ Nachtsperre deaktiviert. Die Gruppe ist dauerhaft offen.');
  }

  if (sub === 'status' || !sub) {
    const entry = groupLockSchedules[from];
    if (!entry) return send('ℹ️ Für diese Gruppe ist keine Nachtsperre aktiv.\n\nNutzung: ' + activePrefix + 'nachtsperre an 22:00 07:00');
    return send('🌙 Nachtsperre aktiv:\nSperrt um ' + entry.start + ' Uhr\nÖffnet um ' + entry.end + ' Uhr');
  }

  if (sub === 'an' || sub === 'on') {
    const start = args[1];
    const end = args[2];
    const timeRegex = /^([01]\d|2[0-3]):([0-5]\d)$/;

    if (!start || !end || !timeRegex.test(start) || !timeRegex.test(end)) {
      return send('❌ Nutzung: ' + activePrefix + 'nachtsperre an <start HH:MM> <ende HH:MM>\nBeispiel: ' + activePrefix + 'nachtsperre an 22:00 07:00');
    }

    groupLockSchedules[from] = { start: start, end: end, setBy: sender };
    save(FILES.groupLockSchedule, groupLockSchedules);

    return send('✅ Nachtsperre aktiviert.\nSperrt täglich um ' + start + ' Uhr\nÖffnet täglich um ' + end + ' Uhr\n\nNur Admins können während der Sperrzeit schreiben.');
  }

  return send('❌ Nutzung: ' + activePrefix + 'nachtsperre an/aus/status');
}
// ===== MURDER DRONES EDITS (automatische Suche) =====
const MD_SEARCH_QUERIES = [
  'murder drones edit', 'murder drones amv', 'murder drones edit shorts',
  'uzi doorman edit', 'murder drones tiktok edit'
];
const MD_CACHE_DIR = path.join(__dirname, 'cache', 'murderdrones');

function searchMdEditUrl() {
  return new Promise((resolve, reject) => {
    const query = MD_SEARCH_QUERIES[randInt(0, MD_SEARCH_QUERIES.length - 1)];
    const cmd = `yt-dlp "ytsearch10:${query}" --get-id --no-playlist --match-filter "duration < 180"`;
    exec(cmd, { maxBuffer: 1024 * 1024 * 10 }, (err, stdout) => {
      if (err) return reject(err);
      const ids = stdout.split('\n').map(s => s.trim()).filter(Boolean);
      if (!ids.length) return reject(new Error('Keine Ergebnisse gefunden'));
      resolve(`https://www.youtube.com/watch?v=${ids[randInt(0, ids.length - 1)]}`);
    });
  });
}

function downloadMdEdit(url) {
  return new Promise((resolve, reject) => {
    fs.mkdirSync(MD_CACHE_DIR, { recursive: true });
    const outPath = path.join(MD_CACHE_DIR, `${Date.now()}.mp4`);
    exec(`yt-dlp -f "mp4" --no-playlist -o "${outPath}" "${url}"`, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
      if (err) return reject(err);
      resolve(outPath);
    });
  });
}

// ===== SWORD ART ONLINE EDITS (automatische Suche) =====
const SAO_SEARCH_QUERIES = [
  'sword art online edit', 'sword art online amv', 'sao edit shorts',
  'kirito asuna edit', 'sword art online tiktok edit'
];
const SAO_CACHE_DIR = path.join(__dirname, 'cache', 'saoedits');

function searchSaoEditUrl() {
  return new Promise((resolve, reject) => {
    const query = SAO_SEARCH_QUERIES[randInt(0, SAO_SEARCH_QUERIES.length - 1)];
    const cmd = `yt-dlp "ytsearch10:${query}" --get-id --no-playlist --match-filter "duration < 180"`;
    exec(cmd, { maxBuffer: 1024 * 1024 * 10 }, (err, stdout) => {
      if (err) return reject(err);
      const ids = stdout.split('\n').map(s => s.trim()).filter(Boolean);
      if (!ids.length) return reject(new Error('Keine Ergebnisse gefunden'));
      resolve(`https://www.youtube.com/watch?v=${ids[randInt(0, ids.length - 1)]}`);
    });
  });
}

function downloadSaoEdit(url) {
  return new Promise((resolve, reject) => {
    fs.mkdirSync(SAO_CACHE_DIR, { recursive: true });
    const outPath = path.join(SAO_CACHE_DIR, `${Date.now()}.mp4`);
    exec(`yt-dlp -f "mp4" --no-playlist -o "${outPath}" "${url}"`, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
      if (err) return reject(err);
      resolve(outPath);
    });
  });
}
// MD
if (cmd === 'md') {
  await send('🔍 Suche einen Edit...');
  try {
    const url = await searchMdEditUrl();
    await send('⏳ Lade Edit, bitte warten...');
    const videoPath = await downloadMdEdit(url);
    const stats = fs.statSync(videoPath);
    if (stats.size > 95 * 1024 * 1024) {
      fs.unlinkSync(videoPath);
      return send('❌ Das gefundene Video ist zu groß für WhatsApp. Versuch es nochmal.');
    }
    await sock.sendMessage(from, {
      video: fs.readFileSync(videoPath), caption: '🤖 Murder Drones Edit', mimetype: 'video/mp4'
    }, { quoted: m });
    fs.unlinkSync(videoPath);
  } catch (e) {
    console.error('[md] Fehler:', e?.message || e);
    return send('❌ Konnte keinen passenden Edit finden oder herunterladen. Versuch es später erneut.');
  }
  return;
}

// SAO
if (cmd === 'sao') {
  await send('⚔️ Durchsuche die Aincrad-Archive nach einem Edit...');
  try {
    const url = await searchSaoEditUrl();
    await send('⏳ Lade Edit, bitte warten...');
    const videoPath = await downloadSaoEdit(url);
    const stats = fs.statSync(videoPath);
    if (stats.size > 95 * 1024 * 1024) {
      fs.unlinkSync(videoPath);
      return send('❌ Das gefundene Video ist zu groß für WhatsApp. Versuch es nochmal.');
    }
    await sock.sendMessage(from, {
      video: fs.readFileSync(videoPath), caption: '⚔️ Sword Art Online Edit', mimetype: 'video/mp4'
    }, { quoted: m });
    fs.unlinkSync(videoPath);
  } catch (e) {
    console.error('[sao] Fehler:', e?.message || e);
    return send('❌ Konnte keinen passenden Edit finden oder herunterladen. Versuch es später erneut.');
  }
  return;
}

// SAY (Hinweis: Berechtigungsprüfung habe ich beibehalten, dein altes Script hatte
// keine — bei einem offenen "Bot sagt was ich will"-Befehl rate ich davon ab)
if (cmd === 'say') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN', 'MOD'])) {
    return send('❌ Kein Zugriff.');
  }
  const text = args.join(' ').trim();
  if (!text) return send(`❌ Nutzung: ${activePrefix}say <nachricht>`);

  if (isGroup) {
    try {
      await sock.sendMessage(from, {
        delete: { remoteJid: from, id: m.key.id, fromMe: false, participant: sender }
      });
    } catch (e) {}
  }
  await sock.sendMessage(from, { text });
  return;
}
// SETINFO
if (cmd === 'setinfo') {
  const feld = (args[0] || '').toLowerCase();
  const wert = args.slice(1).join(' ').trim();
  const erlaubteFelder = {
    name: 'name', alter: 'alter', hobbys: 'hobbys', hobby: 'hobbys',
    sexualitaet: 'sexualitaet', 'sexualität': 'sexualitaet'
  };

  if (!feld || !erlaubteFelder[feld] || !wert) {
    return send(
      `❌ Nutzung: ${activePrefix}setinfo <feld> <wert>\n\n` +
      `Verfügbare Felder: name, alter, hobbys, sexualitaet\n\n` +
      `Beispiele:\n${activePrefix}setinfo name Kirito\n${activePrefix}setinfo alter 22\n` +
      `${activePrefix}setinfo hobbys Lesen, Gaming\n${activePrefix}setinfo sexualitaet Hetero`
    );
  }

  const key = erlaubteFelder[feld];
  if (key === 'alter') {
    const num = parseInt(wert);
    if (isNaN(num) || num < 1 || num > 120) return send('❌ Bitte gib ein gültiges Alter zwischen 1 und 120 an.');
    users[sender].alter = num;
  } else {
    users[sender][key] = wert;
  }
  save(FILES.users, users);
  return send(`✅ ${feld.charAt(0).toUpperCase() + feld.slice(1)} wurde gespeichert. Nutze ${activePrefix}me, um dein Profil anzuzeigen.`);
}
// BANCMD
if (cmd === 'bancmd') {
  if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) return send('❌ Nur Owner/CoOwner.');
  const target = args[0];
  const action = (args[1] || 'ban').toLowerCase();
  if (!target) return send(`Nutzung: ${PREFIX}bancmd <befehl> [ban|unban]`);

  const tcmd = String(target).toLowerCase().replace(new RegExp(`^\\${PREFIX}`), '').trim();
  if (!tcmd) return send('❌ Ungültiger Befehl.');

  const protectedCmds = ['bancmd', 'unbancmd', 'help', 'menu'];
  if (protectedCmds.includes(tcmd)) return send(`❌ Der Befehl "${tcmd}" kann nicht gesperrt werden.`);

  if (action === 'unban') {
    if (commandBans[tcmd]) {
      delete commandBans[tcmd];
      save(FILES.commandBans, commandBans);
      return send(`✅ Befehl ${tcmd} wurde entsperrt.`);
    }
    return send(`ℹ️ Befehl ${tcmd} war nicht gesperrt.`);
  }

  commandBans[tcmd] = { by: sender, at: new Date().toISOString() };
  save(FILES.commandBans, commandBans);
  return send(`⛔ Befehl ${tcmd} wurde gesperrt und ist nur noch für Owner/CoOwner verfügbar.`);
}
// ⚔️ Arena-System: Kisten, Ausrüstung, Duelle, Leaderboard
const arenaHandled = await arena.handle({
  cmd, args, sender, from, m, isGroup, activePrefix, send, sock,
  users, save, FILES, ensureUser, normalizeJid, isSameJid,
  getNumberMention, randInt, sleep, isPrimaryOwner
});
if (arenaHandled) {
  // Nach jedem Arena-Command (inkl. Duellen) automatisch prüfen,
  // ob der aufrufende Spieler neue Titel/Achievements freigeschaltet hat.
  await checkProgress({
    users, save, FILES, send, activePrefix,
    guilds, ownerJids: [OWNER_LID, OWNER_LID2, OWNER_PRIV, OWNER_PRIV2]
  }, sender);
  return;
}

const guildHandled = await guildSystem.handle({
  cmd, args, sender, send, sock,
  users, guilds, save, FILES, ensureUser, normalizeJid, isSameJid,
  getNumberMention, activePrefix, m
});
if (guildHandled) {
  // Auch nach Gilden-Aktionen prüfen (z.B. Gilde gegründet -> Gildenmeister-Titel)
  await checkProgress({
    users, save, FILES, send, activePrefix,
    guilds, ownerJids: [OWNER_LID, OWNER_LID2, OWNER_PRIV, OWNER_PRIV2]
  }, sender);
  return;
}

const titleHandled = await titleSystem.handle({
  cmd, args, sender, from, m, isGroup, activePrefix, send, sock,
  users, guilds, save, FILES, ensureUser, normalizeJid, isSameJid,
  ownerJids: [OWNER_LID, OWNER_LID2, OWNER_PRIV, OWNER_PRIV2]
});
if (titleHandled) return;
// Unbekannter Befehl
const suggestion = findClosestCommand(cmd);
if (suggestion) {
  return send(
    '⚠️ *SYSTEM-FEHLER* ⚠️\n' +
    '┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
    'Der Befehl "' + cmd + '" existiert nicht im Aincrad-System.\n\n' +
    '🔍 *Ähnlichste Erkenntnis:*\n' +
    '⌈ ' + activePrefix + suggestion.command + ' ⌋ — Übereinstimmung: ' + suggestion.similarity + '%\n\n' +
    'Meintest du das, Schwertkämpfer?\n' +
    '┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
    '_Nutze ' + activePrefix + 'help für das vollständige Skill-Menü._'
  );
}
return send(
  '❓ *UNBEKANNTER BEFEHL* ❓\n' +
  '┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n' +
  'Dieser Skill wurde noch nicht erlernt.\n' +
  'Nutze ' + activePrefix + 'help oder ' + activePrefix + 'menu für das Command-Window.\n\n' +
  'Falls du glaubst, dieser Skill sollte existieren, wende dich an Daddy Kirito unter ' + activePrefix + 'owner.'
);
    } catch (err) {
      console.error('messages.upsert error:', err);
      log('ERROR: ' + (err?.message || String(err)));
    }
  });

  console.log('✅ Sword-art-online-bot Session "' + sessionName + '" gestartet.');
  return sock;
}
// ========== MAIN ==========
initTelegramConnect();

(async () => {
  let existingSessions = [];
  try {
    existingSessions = fs.readdirSync(SESSIONS_DIR, { withFileTypes: true })
      .filter(d => d.isDirectory())
      .map(d => d.name);
  } catch (e) {
    existingSessions = [];
  }

  if (existingSessions.length === 0) {
    
    await startBot('default');
  } else {
    
    for (const sessionName of existingSessions) {
      await startBot(sessionName);
      await sleep(1000);
    }
  }
})();