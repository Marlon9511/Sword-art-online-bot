import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion } from '@717Development/Baileys';
import fs from 'fs';
import path from 'path';
import P from 'pino';
import fetch from 'node-fetch';
import QRCode from 'qrcode';
import { exec } from 'child_process';
import archiver from 'archiver';

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
  TICKET_GROUP: '120363404391054839@g.us',
  SUPPORT_GROUP: '120363404110964816@g.us',
};

let OWNER_LID = process.env.OWNER_LID || process.env.OWNER_JID || '27088878862400@lid';
let OWNER_PRIV = process.env.OWNER_PRIV || '4915111254435@s.whatsapp.net';
let COOWNER_LID = process.env.COOWNER_LID || process.env.COOWNER_JID || '147274562842774@lid';

ROLES.OWNER.push(OWNER_LID, OWNER_PRIV);
ROLES.COOWNER.push(COOWNER_LID);

// Prefix
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
  
  switch(unit) {
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
const save = (f, d) => fs.writeFileSync(path.join(DATA_PATH, f), JSON.stringify(d, null, 2));
const log = s => fs.appendFileSync(LOG_FILE, `[${new Date().toISOString()}] ${s}\n`);

const prettyRank = r => ({ 
  OWNER:'👑 Inhaber', 
  COOWNER:'👑 Co-Inhaber', 
  ADMIN:'🛡 Admin', 
  MOD:'⚔ Moderator', 
  VIP:'💎 VIP',
  SUPPORTER: '🌟 Supporter',
  TEST_SUPPORTER: '🔰 Test-Supporter',
  USER:'👤 Nutzer' 
}[r]||'👤 Nutzer');

function normalizeJid(jid){
  if(!jid) return jid;
  jid = String(jid);
  if (jid.startsWith('@')) jid = jid.substring(1);
  if (/^\d+$/.test(jid)) return `${jid}@s.whatsapp.net`;
  if (jid.includes('@')) return jid;
  const num = jid.replace(/\D+/g, '');
  return num ? `${num}@s.whatsapp.net` : jid;
}

function toParticipantJid(jid){
  if(!jid) return jid;
  const n = normalizeJid(jid);
  if (!n) return n;
  if (n.endsWith('@s.whatsapp.net')) return n;
  if (n.endsWith('@lid')) {
    const num = n.replace(/\D+/g, '');
    return num ? `${num}@s.whatsapp.net` : n;
  }
  return n;
}

function isSameJid(a,b){
  if(!a || !b) return false;
  return normalizeJid(a) === normalizeJid(b);
}

function normalizeDataKeys(obj){
  const out = {};
  for(const k of Object.keys(obj||{})){
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


// ensure structures
function ensureUser(rawJid) {
  const jid = normalizeJid(rawJid);
  // If the user was permanently deleted, do not recreate their data
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

// Check if user is registered
function isUserRegistered(jid) {
  const normalizedJid = normalizeJid(jid);
  return users[normalizedJid]?.registered === true;
}

// Register a user
function registerUser(jid, name) {
  const normalizedJid = normalizeJid(jid);
  if (!users[normalizedJid]) ensureUser(normalizedJid);
  users[normalizedJid].registered = true;
  users[normalizedJid].registrationDate = new Date().toISOString();
  users[normalizedJid].name = name;
  save(FILES.users, users);
}

// Unregister a user
function unregisterUser(jid) {
  const normalizedJid = normalizeJid(jid);
  if (users[normalizedJid]) {
    users[normalizedJid].registered = false;
    users[normalizedJid].registrationDate = null;
    save(FILES.users, users);
  }
}

// persist everything periodically
function persistAll() {
  save(FILES.users, users);
  save(FILES.bans, bans);
  save(FILES.joinreq, joinreqs);
  save(FILES.pets, pets);
  save(FILES.tickets, tickets);
  save(FILES.ranks, ranks);
  save(FILES.broadcastSettings, broadcastSettings);
  save(FILES.deleted, deletedUsers);
  // persist owner/coowner configuration
  try {
    save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
  } catch (e) { console.error('Failed to save owner config:', e); }
}
setInterval(persistAll, 60_000);

// ========== GAME HELPERS ==========
const SLOT_SYMBOLS = ['🍒','🍋','🍇','🍉','⭐','💎'];
function spinSlots() { return [SLOT_SYMBOLS[randInt(0,SLOT_SYMBOLS.length-1)], SLOT_SYMBOLS[randInt(0,SLOT_SYMBOLS.length-1)], SLOT_SYMBOLS[randInt(0,SLOT_SYMBOLS.length-1)]]; }

// Blackjack helpers
const BJ_SUITS = ['♠','♥','♦','♣'];
const BJ_VALUES = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
function bjDraw() { return { value: BJ_VALUES[randInt(0,BJ_VALUES.length-1)], suit: BJ_SUITS[randInt(0,BJ_SUITS.length-1)] }; }
function bjVal(card) { if (['J','Q','K'].includes(card.value)) return 10; if (card.value === 'A') return 11; return parseInt(card.value); }
function bjScore(hand) { let s=0, ac=0; for(const c of hand){ if(c.value==='A'){ ac++; s+=11; } else s+=bjVal(c);} while(s>21 && ac>0){ s-=10; ac--; } return s; }

// ========== START BOT ==========
async function startBot() {
  // Keep created session sockets referenced so they are not GC'd and we can inspect them later
  const sessionSockets = new Map();

  // Allow overriding the auth directory from env (used by session-bootstrap/pm2)
  const AUTH_DIR = process.env.AUTH_DIR || path.join(SESSIONS_DIR, process.env.SESSION_NAME || 'default');
  const SESSION_NAME_ENV = process.env.SESSION_NAME || null;

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();
  const sock = makeWASocket({ version, logger: P({ level:'silent' }), printQRInTerminal: true, auth: state });

  // Cache and rate-limit-safe wrapper for group metadata calls
  const groupMetaCache = new Map();
  const lastProcessed = new Map(); // per-chat last processed message timestamp to skip old messages
  // pending actions that require confirmation (owner-confirmed flows)
  const pendingActions = new Map();

  async function getGroupMetaSafe(jid, attempts = 3) {
    if (!jid) {
      console.error('[groupMeta] Called with null/undefined jid');
      return null;
    }

    // Debug log
    console.log(`[groupMeta] Fetching metadata for group ${jid}`);

    // Check cache first
    if (groupMetaCache.has(jid)) {
      console.log(`[groupMeta] Cache hit for ${jid}`);
      return groupMetaCache.get(jid);
    }

    let attempt = 0;
    const maxAttempts = 3; // Increase max attempts

    while (attempt < maxAttempts) {
      try {
        console.log(`[groupMeta] Attempt ${attempt + 1}/${maxAttempts} for ${jid}`);
        
        // Try direct API call first
        let meta = await sock.groupMetadata(jid).catch(e => {
          console.log(`[groupMeta] Direct call failed:`, e.message);
          return null;
        });

        // If direct call failed, try alternative method
        if (!meta) {
          console.log(`[groupMeta] Trying alternative fetch method for ${jid}`);
          // Try to fetch participating groups and find this one
          const groups = await sock.groupFetchAllParticipating().catch(e => {
            console.log(`[groupMeta] Participating groups fetch failed:`, e.message);
            return {};
          });
          meta = groups[jid];
        }

        // Validate metadata
        if (meta && typeof meta === 'object') {
          console.log(`[groupMeta] Successfully got metadata for ${jid}:`, {
            subject: meta.subject,
            id: meta.id,
            size: meta.size,
            participants: (meta.participants || []).length
          });
          
          // Update cache
          groupMetaCache.set(jid, meta);
          return meta;
        }

        // If we get here, increase attempt counter and maybe retry
        attempt++;
        
        // On failure, wait with exponential backoff
        const wait = Math.min(500 * Math.pow(2, attempt), 5000); // max 5 second wait
        console.log(`[groupMeta] Attempt ${attempt} failed for ${jid}, waiting ${wait}ms before retry`);
        await sleep(wait);

      } catch (e) {
        const msg = String(e && e.message || '');
        console.error(`[groupMeta] Error fetching metadata for ${jid}:`, msg);
        
        // Handle rate limits
        if ((e && e.data === 429) || /rate-overlimit/i.test(msg) || /429/.test(msg)) {
          attempt++;
          const wait = Math.min(500 * Math.pow(2, attempt), 5000);
          console.warn(`[groupMeta] Rate limit for ${jid}, retry ${attempt} in ${wait}ms`);
          await sleep(wait);
          continue;
        }

        // For other errors, increment attempt and retry
        attempt++;
        await sleep(1000); // 1s delay for non-rate-limit errors
      }
    }

    console.error(`[groupMeta] Failed to get metadata for ${jid} after ${maxAttempts} attempts`);
    return null;
  }

  // Funktion zum Aktualisieren des Bot-Profils
  async function updateBotProfile() {
    try {
      // Bot-Namen auf "Sword art online bot" setzen
      await sock.updateProfileName('Sword art online bot');
      console.log('✅ Bot-Name wurde zu Sword art online bot geändert');

      // Profilbild setzen wenn profil.png existiert
      const profilePath = './profil.jpg';
      if (fs.existsSync(profilePath)) {
        const profileImage = fs.readFileSync(profilePath);
        await sock.updateProfilePicture(sock.user.id, profileImage);
        console.log('✅ Profilbild wurde aktualisiert');
      } else {
        console.log('⚠️ profil.png nicht gefunden');
      }
    } catch (error) {
      console.error('❌ Fehler beim Aktualisieren des Profils:', error);
    }
  }

  sock.ev.on('connection.update', ({ connection }) => {
    if (connection === 'open') {
      // Profil aktualisieren nach erfolgreicher Verbindung
      updateBotProfile();
      console.log('✅ Verbunden');
      // Prüfe auf Neustart und sende Benachrichtigung
      try {
        const restartInfo = JSON.parse(fs.readFileSync(RESTART_FILE, 'utf8'));
        if (restartInfo.timestamp && restartInfo.chatId) {
          const timeSinceRestart = Date.now() - restartInfo.timestamp;
          // Nur wenn der Neustart weniger als 30 Sekunden her ist
          if (timeSinceRestart < 30000) {
            sock.sendMessage(restartInfo.chatId, {
              text: `✅ Bot wurde erfolgreich neugestartet!`,
              mentions: restartInfo.initiator ? [restartInfo.initiator] : undefined
            });
          }
        }
        // Lösche die Restart-Info nach dem Senden
        fs.writeFileSync(RESTART_FILE, JSON.stringify({}));
      } catch (e) {
        console.error('Fehler beim Senden der Neustart-Nachricht:', e);
      }
    }
    if (connection === 'close') {
      console.log('⚠ Verbindung geschlossen — neu verbinden in 3s');
      setTimeout(()=>startBot(), 3000);
    }
  });
  sock.ev.on('creds.update', saveCreds);

  // --- Reliable Welcome Message Handler ---
  sock.ev.on('group-participants.update', async (update) => {
    try {
      const { id: groupId, participants, action } = update;
      // Debug log for event
      console.log(`[Welcome] Event:`, { groupId, participants, action });

      // Ensure group settings are initialized
      if (!groupSettings[groupId]) {
        groupSettings[groupId] = {
          welcome: {
            enabled: false,
            message: 'Willkommen in der Gruppe {user}! 👋'
          }
        };
      }
      const settings = groupSettings[groupId];
      // Debug log for settings
      console.log(`[Welcome] Settings:`, settings);

      // Only handle new joins if welcome is enabled
      if (action === 'add' && settings.welcome.enabled) {
        const welcomeMsg = settings.welcome.message || 'Willkommen in der Gruppe! 👋';
        for (const participant of participants) {
          const formattedMsg = welcomeMsg.replace('{user}', '@' + participant.split('@')[0]);
          await sock.sendMessage(groupId, {
            text: formattedMsg,
            mentions: [participant]
          });
          // Debug log for sent message
          console.log(`[Welcome] Sent to ${groupId}:`, formattedMsg);
        }
      }
    } catch (err) {
      console.error('[Welcome] Error handling group participant update:', err);
    }
  });

  // periodic save also when graceful exit
  process.on('SIGINT', () => { persistAll(); process.exit(); });

  // messages handler — safe checks
  sock.ev.on('messages.upsert', async ({ messages }) => {
    const event = { messages };
    try {
      if (!event || !event.messages || !Array.isArray(event.messages) || event.messages.length === 0) return;
      const m = event.messages[0];
      if (!m || !m.message) return;

      // normalize sender/from early and use normalized keys everywhere
  const rawFrom = m.key.remoteJid;
  const rawParticipant = m.key.participant || m.key.remoteJid;
  const from = normalizeJid(rawFrom);
  const sender = normalizeJid(rawParticipant);
  // whether this message is from a group chat (group JIDs end with @g.us)
  const isGroup = typeof from === 'string' && from.endsWith('@g.us');

      // get textual body (handles simple text, extendedText, image captions)
      const body = (m.message.conversation)
        || (m.message.extendedTextMessage && m.message.extendedTextMessage.text)
        || (m.message.imageMessage && m.message.imageMessage.caption)
        || '';

      // whether this message is a command (starts with PREFIX)
      const isCmd = !!(body && body.startsWith(PREFIX));

      // If message comes from a group and is a command, ensure the bot is an admin in that group.
      // If the bot is not admin (or group metadata can't be fetched), ignore the command entirely.
      if (isGroup && isCmd) {
        try {
          // Log everything about the bot's identity first
          console.log('[permissions] Bot identity:', {
            user: sock.user,
            id: sock.user?.id,
            jid: sock.user?.jid,
            number: sock.user?.id?.split(':')[0]?.split('@')[0],
            me: sock.user?.me,
            name: sock.user?.name
          });

          const meta = await getGroupMetaSafe(from);
          console.log(`[permissions] Group metadata for ${from}:`, JSON.stringify(meta, null, 2));
          
          if (!meta) {
            console.log(`[permissions] No group metadata for ${from} — informing group that bot cannot execute commands.`);
            try { await sock.sendMessage(from, { text: '⚠️ Ich kann deine Anfrage nicht bearbeiten — ich kann meine Administrator-Rechte in dieser Gruppe nicht prüfen. Bitte mache mich zum Administrator, damit ich Befehle ausführen kann.' }); } catch (e) { console.error('Failed to send admin-missing notice (no meta):', e); }
            return;
          }

          // Get ALL possible bot ID variants, including the known LID
          const BOT_LID = '32174136897540@lid';
          const rawNumber = (sock.user?.id || '')?.split(':')[0]?.split('@')[0]?.replace(/[^0-9]/g, '');
          const lidNumber = BOT_LID.split('@')[0];
          
          const possibleBotIds = [
            BOT_LID, // Known LID
            sock.user?.id,
            sock.user?.jid,
            sock.user?.id?.split(':')[0],
            sock.user?.jid?.split(':')[0],
            rawNumber ? `${rawNumber}@s.whatsapp.net` : null,
            rawNumber ? `${rawNumber}@c.us` : null,
            sock.user?.me?.id,
            sock.user?.name,
            `${rawNumber}@g.us`,
            `${lidNumber}@s.whatsapp.net`,
            `${lidNumber}@c.us`,
            lidNumber
          ].filter(Boolean);

          // Add more variants
          const moreBotIds = possibleBotIds.flatMap(id => [
            id,
            id.split('@')[0],
            `${id.split('@')[0]}@s.whatsapp.net`,
            `${id.split('@')[0]}@c.us`
          ]);

          const allBotIds = [...new Set([...possibleBotIds, ...moreBotIds])].map(id => String(id));
          
          // Debug log EVERYTHING
          console.log('[permissions] All possible bot IDs:', allBotIds);
          console.log('[permissions] Raw participants:', meta.participants);
          console.log('[permissions] Participant IDs:', meta.participants?.map(p => p.id));
          
          // Try different matching strategies with explicit LID support
          const botPart = (meta.participants || []).find(p => {
            // Get all possible forms of this participant's ID
            const pids = [
              p.id,
              p.jid,
              p.id?.split(':')[0],
              p.id?.split('@')[0],
              `${p.id?.split('@')[0]}@s.whatsapp.net`,
              String(p.id).replace(/[^0-9]/g, ''),
              // Extra check: if this participant's number matches our LID number
              p.id?.split('@')[0] === lidNumber ? BOT_LID : null,
              // Try the raw number comparison
              p.id?.replace(/[^0-9]/g, '') === lidNumber ? BOT_LID : null
            ].filter(Boolean).map(String);
            
            console.log('[permissions] Checking participant:', {
              original: p.id,
              variants: pids,
              matches: pids.some(pid => allBotIds.includes(pid))
            });
            
            return pids.some(pid => allBotIds.includes(pid));
          });

          if (!botPart) {
            console.log(`[permissions] Could not find bot in participants list`);
            try { await sock.sendMessage(from, { text: '⚠️ Ich konnte meine Teilnahme in dieser Gruppe nicht verifizieren. Bitte entferne und füge mich neu hinzu.' }); } catch (e) { console.error('Failed to send not-found notice:', e); }
            return;
          }

          console.log(`[permissions] Found bot participant:`, botPart);
          
          // Check all possible admin indicators
          const botIsAdmin = !!(
            botPart.admin === 'admin' ||
            botPart.admin === 'superadmin' ||
            botPart.admin === true ||
            botPart.isAdmin === true ||
            String(botPart.admin).toLowerCase() === 'true'
          );

          console.log(`[permissions] Bot admin stattus:`, {
            found: !!botPart,
            admin: botPart.admin,
            isAdmin: botPart.isAdmin,
            finalStatus: botIsAdmin
          });

          if (!botIsAdmin) {
            console.log(`[permissions] Command in ${from} ignored — bot is not admin. Notifying group.`);
            try { await sock.sendMessage(from, { text: '⚠️ Ich bin kein Administrator in dieser Gruppe und kann keine Befehle ausführen. Bitte mache mich zum Admin, damit ich Befehle beantworten kann.' }); } catch (e) { console.error('Failed to send admin-missing notice:', e); }
            return;
          }
        } catch (e) {
          console.error('[permissions] Error checking bot admin status:', e);
          try { await sock.sendMessage(from, { text: '⚠️ Ich konnte meine Administrator-Rechte nicht prüfen. Bitte überprüfe die Gruppen-Einstellungen oder mache mich zum Administrator.' }); } catch (err) { console.error('Failed to send admin-check-error notice:', err); }
          return;
        }
      }

      // Check for registration commands
      if (body.toLowerCase().startsWith(PREFIX + 'register')) {
        const args = body.slice(PREFIX.length).trim().split(/\s+/);
        
        // Handle basic register command
        if (args.length === 1) {
          if (!isUserRegistered(sender)) {
            await sock.sendMessage(from, { 
              text: DSGVO_TEXT + `\n\nUm sich zu registrieren, nutzen Sie bitte den Befehl:\n${PREFIX}register confirm IhrName` 
            });
            return;
          } else {
            await sock.sendMessage(from, { text: 'Sie sind bereits registriert.' });
            return;
          }
        }
        
        // Handle register confirm command
        if (args.length >= 3 && args[1].toLowerCase() === 'confirm') {
          if (!isUserRegistered(sender)) {
            const name = args.slice(2).join(' '); // Zusammenfügen des Namens, falls er Leerzeichen enthält
            if (name.length < 2) {
              await sock.sendMessage(from, { text: 'Bitte geben Sie einen gültigen Namen ein.' });
              return;
            }
            registerUser(sender, name);
            await sock.sendMessage(from, { 
              text: `Vielen Dank für Ihre Registrierung, ${name}! Sie können den Bot nun nutzen.\n\nSie können Ihre Registrierung jederzeit mit ${PREFIX}unregister widerrufen.` 
            });
            return;
          } else {
            await sock.sendMessage(from, { text: 'Sie sind bereits registriert.' });
            return;
          }
        }
      }

      // Check for unregister command
      if (body.toLowerCase() === PREFIX + 'unregister') {
        if (isUserRegistered(sender)) {
          const userName = users[sender]?.name || 'Unbekannt';
          unregisterUser(sender);
          await sock.sendMessage(from, { 
            text: `Auf Wiedersehen, ${userName}! Ihre Registrierung wurde erfolgreich gelöscht. Ihre Daten wurden entfernt.` 
          });
          return;
        } else {
          await sock.sendMessage(from, { text: 'Sie sind nicht registriert.' });
          return;
        }
      }

      // Handle backup command (owner only)
      if (body.toLowerCase() === PREFIX + 'backup') {
        // Check if sender is owner
        if (ROLES.OWNER.includes(sender)) {
          try {
            await sock.sendMessage(sender, { 
              text: 'Backup wird erstellt, bitte warten...' 
            });
            
            const backupPath = await createBackup();
            
            // Send the backup file
            await sock.sendMessage(sender, {
              document: fs.readFileSync(backupPath),
              mimetype: 'application/zip',
              fileName: path.basename(backupPath)
            });

            // Delete the backup file after sending
            fs.unlinkSync(backupPath);

            await sock.sendMessage(sender, { 
              text: 'Backup wurde erfolgreich erstellt und gesendet.' 
            });
          } catch (error) {
            console.error('Backup error:', error);
            await sock.sendMessage(sender, { 
              text: 'Fehler beim Erstellen des Backups: ' + error.message 
            });
          }
          return;
        } else {
          await sock.sendMessage(from, { 
            text: 'Dieser Befehl ist nur für den Bot-Inhaber verfügbar.' 
          });
          return;
        }
      }

      // Check if user is registered before processing any other commands
      if (!isUserRegistered(sender)) {
        if (isCmd) {
          await sock.sendMessage(from, { 
            text: 'Bitte registrieren Sie sich zuerst mit dem Befehl /register oder ' + PREFIX + 'register, um den Bot nutzen zu können.' 
          });
          return;
        }
        return; // Ignore non-command messages from unregistered users
      }

      // message timestamp (some events include messageTimestamp)
      const msgTs = m.messageTimestamp || (m.message && m.message.messageTimestamp) || Date.now();
      // skip processing messages older or equal to the last processed for this chat
      try {
        const lastTs = lastProcessed.get(from) || 0;
        if (msgTs <= lastTs) {
          console.log(`[msg] skipping old message from ${from} ts=${msgTs} last=${lastTs}`);
          return;
        }
        lastProcessed.set(from, msgTs);
      } catch (e) { /* ignore caching errors */ }
      // check if the user was permanently deleted
      if (deletedUsers[sender]) {
        try { await sock.sendMessage(from, { text: '🚫 Dein Account wurde vom Inhaber gelöscht und ist gesperrt.' }); } catch {}
        return;
      }

      // ignore banned users
      if (bans[sender]) {
        try { await sock.sendMessage(from, { text: '🚫 Du bist gebannt.' }); } catch {}
        return;
      }

      // ensure user (will not recreate deleted users)
      ensureUser(sender);

      // XP on each message (not from bot itself)
      if (!m.key.fromMe) {
        users[sender].xp = (users[sender].xp || 0) + 5;
        users[sender].msgCount = (users[sender].msgCount || 0) + 1;
        // level up
        const needed = 100 + (users[sender].level * 50);
        if (users[sender].xp >= needed) {
          users[sender].level = (users[sender].level || 1) + 1;
          users[sender].xp -= needed;
          try { 
            await sock.sendMessage(from, { 
              text: `🎉 Level-Up! @${sender.split('@')[0]} ist jetzt Level ${users[sender].level}`,
              mentions: [sender]
            }); 
          } catch (e) { /* ignore send errors */ }
          try {
            const meta = await getGroupMetaSafe(from);
            if (!meta) {
              try { await sock.sendMessage(from, { text: '⚠️ Ich kann meine Administrator-Rechte in dieser Gruppe nicht prüfen. Bitte mache mich zum Administrator.' }); } catch (e) {}
              return;
            }
            // Suche Bot anhand verschiedener Formen (raw id und @lid-Format)
            const botRawId = (sock.user?.id || sock.user?.jid || sock.user || '').split(':')[0];
            const botLidFormat = botRawId ? `${botRawId.replace(/\D+/g, '')}@lid` : null;
            const botPart = (meta.participants || []).find(p => 
              p.id === botLidFormat || 
              p.jid === botLidFormat ||
              p.id === botRawId ||
              p.jid === botRawId
            );
            const botIsAdmin = botPart && (
              botPart.admin === 'admin' || 
              botPart.admin === 'superadmin' || 
              botPart.isAdmin ||
              botPart.admin === true
            );
            if (!botIsAdmin) {
              try { await sock.sendMessage(from, { text: '⚠️ Ich bin kein Administrator in dieser Gruppe und kann keine Befehle ausführen.' }); } catch (e) {}
              return;
            }
          } catch (e) {
            try { await sock.sendMessage(from, { text: '⚠️ Fehler beim Admin-Check. Bitte prüfe die Gruppen-Einstellungen.' }); } catch (err) {}
            return;
          }
        }
      }

      // non-command messages ignored here
      if (!body || !body.startsWith(PREFIX)) return;

      // small delay to reduce race
      await sleep(150);

      const [cmdRaw, ...args] = body.trim().split(/\s+/);
      const rawCmd = cmdRaw.toLowerCase();
      const cmd = rawCmd.startsWith(PREFIX) ? rawCmd.slice(PREFIX.length) : rawCmd;

      // GETLID: Gibt die aktuelle LID des Bots aus
      if (cmd === 'getlid') {
        // Ermittle Ziel aus Argument oder Mention
        let target = args[0];
        // Falls keine Argumente, gib Hilfe aus
        if (!target) {
          await sock.sendMessage(from, { text: 'Nutzung: #getlid <Nummer|@nutzer>\nBeispiel: #getlid 49123456789 oder #getlid @nutzer' });
          return;
        }

        // Falls Mention, hole aus contextInfo
        const ctx = m.message && m.message.extendedTextMessage && m.message.extendedTextMessage.contextInfo;
        if (ctx && Array.isArray(ctx.mentionedJid) && ctx.mentionedJid.length) {
          target = ctx.mentionedJid[0];
        }

        // Zielnummer normalisieren und LID bauen
        const num = String(target).replace(/[^0-9]/g, '');
        let lid = 'Unbekannt';
        if (num) {
          lid = `${num}@lid`;
        }
        await sock.sendMessage(from, { text: `Die LID ist ${lid}` });
        return;
      }
      const userRank = ranks[sender] || users[sender]?.rank || 'USER';
      // Verbesserte Owner-Erkennung (Co-Owner hat gleiche Rechte wie Owner)
      const isOwner = isAuthorized(sender, ['OWNER', 'COOWNER']);
      const isCoOwner = isAuthorized(sender, ['COOWNER']);
      const isAdmin = isAuthorized(sender, ['ADMIN']);
      // If bot is set to offline mode, ignore commands from non-owners
      if (BOT_OFFLINE && !isOwner) {
        // Optionally, silently ignore by returning; here we send a polite notice
        try { await sock.sendMessage(from, { text: '⚠️ Der Bot ist derzeit im Offline-Modus. Nur der Inhaber kann Befehle ausführen.' }); } catch (e) {}
        return;
      }
      const send = async (text, opts={}) => { try { await sock.sendMessage(from, { text, ...opts }); } catch (e) { console.error('send failed', e); } };
      log(`${sender} -> ${body}`);

      // KI-Befehle
      if (cmd === 'ai' && hasAdminPerms(sender)) {
        if (!args.length) {
          return send(
`🤖 *KI-Einstellungen*

*Aktuelles Modell:* ${currentModel}
*Status:* ${AI_CONFIG.settings.enabled ? '✅ Aktiv' : '❌ Deaktiviert'}
*Gruppen:* ${AI_CONFIG.settings.groupsEnabled ? '✅ Erlaubt' : '❌ Gesperrt'}
*Owner-Only:* ${AI_CONFIG.settings.ownerOnly ? '✅ Ja' : '❌ Nein'}
*Temperature:* ${TEMPERATURE}

*Verfügbare Modelle:*
${Object.values(AI_CONFIG.models).map(m => `- ${m.name}: ${m.description}`).join('\n')}

*Befehle:*
${PREFIX}ai model <name> - Modell wechseln
${PREFIX}ai temp <0.1-1.0> - Temperature einstellen
${PREFIX}ai toggle - KI an/aus schalten
${PREFIX}ai groups - Gruppen-Zugriff umschalten
${PREFIX}ai owneronly - Nur-Owner Modus umschalten`);
        }

        const [subCmd, ...subArgs] = args;
        
        switch(subCmd) {
          case 'model':
            const newModel = subArgs[0]?.toLowerCase();
            if (!AI_CONFIG.models[newModel]) {
              return send('❌ Unbekanntes Modell. Verfügbar: ' + Object.keys(AI_CONFIG.models).join(', '));
            }

            // Prüfe ob das Modell installiert ist
            if (!await isModelInstalled(newModel)) {
              send(`🔄 Modell "${newModel}" ist noch nicht installiert. Starte Download...`);
              const success = await installModel(newModel, send);
              if (!success) return;
            }

            currentModel = newModel;
            return send(`✅ Modell gewechselt zu: ${newModel}`);
            
          case 'temp':
            const temp = parseFloat(subArgs[0]);
            if (isNaN(temp) || temp < 0.1 || temp > 1.0) {
              return send('❌ Temperature muss zwischen 0.1 und 1.0 liegen');
            }
            AI_CONFIG.settings.temperature = temp;
            return send(`✅ Temperature auf ${temp} gesetzt`);
            
          case 'toggle':
            AI_CONFIG.settings.enabled = !AI_CONFIG.settings.enabled;
            return send(`${AI_CONFIG.settings.enabled ? '✅ KI aktiviert' : '❌ KI deaktiviert'}`);
            
          case 'groups':
            AI_CONFIG.settings.groupsEnabled = !AI_CONFIG.settings.groupsEnabled;
            return send(`${AI_CONFIG.settings.groupsEnabled ? '✅ Gruppen-Zugriff aktiviert' : '❌ Gruppen-Zugriff deaktiviert'}`);
            
          case 'owneronly':
            AI_CONFIG.settings.ownerOnly = !AI_CONFIG.settings.ownerOnly;
            return send(`${AI_CONFIG.settings.ownerOnly ? '✅ Nur-Owner Modus aktiviert' : '❌ Nur-Owner Modus deaktiviert'}`);
            
          default:
            return send(`❌ Unbekannter AI-Befehl. Nutze ${PREFIX}ai ohne Parameter für Hilfe.`);
        }
      }

      // Ticket command handler
      if (cmd === 'ticket') {
        ticketCounter++; // Erhöhe Counter für neue Ticket-ID
        const ticketId = ticketCounter;
        tickets[ticketId] = {
          from: sender,
          message: body,
          created: new Date().toISOString(),
          status: 'open'
        };
        save(FILES.tickets, tickets);

        // Sende Ticket an Ticket-Gruppe (nur Ticket ohne Antwort-Option)
        await sock.sendMessage(SUPPORT_CONFIG.TICKET_GROUP, {
          text: `🎫 Ticket #${ticketId}\nVon: @${sender.split('@')[0]}\n\nNachricht:\n${body}`,
          mentions: [sender]
        });
        
        // Sende Ticket mit Antwort-Option an Support-Gruppe
        await sock.sendMessage(SUPPORT_CONFIG.SUPPORT_GROUP, {
          text: `✅ Dein Ticket wurde erstellt (#${ticketId}). Ein Supporter wird sich bald bei dir melden.`,
          mentions: [sender]
        });
        
        return;
      }

      // Help command with AI settings for owner
      if (cmd === 'help') {
        let helpText = `🤖 *Bot Command Übersicht*\n\n`;
        
  // Grundlegende Befehle für alle
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
${PREFIX}fish - Gehe angeln (mit Glück und Pech)

*💬 Chat & Gruppen:*
${PREFIX}gi - Gruppen-Einstellungen anzeigen
${PREFIX}welcome-an - Welcome-Nachricht aktivieren
${PREFIX}welcome-aus - Welcome-Nachricht deaktivieren
${PREFIX}welcome-set <text> - Welcome-Text setzen
${PREFIX}hidetag - Nachricht mit verstecktem Tag

*🤖 KI-Befehle:*
${PREFIX}ask <frage> - Stelle eine Frage an die KI
${PREFIX}ai - KI-Einstellungen anzeigen/ändern
${PREFIX}models - Verfügbare KI-Modelle anzeigen\n\n`;

        // Support-Befehle für Supporter
  if (isAuthorized(sender, ['OWNER', 'SUPPORTER', 'TEST_SUPPORTER'])) {
    helpText += `*🎫 Support-System:*
${PREFIX}answer <ticket-id> <text> - Ticket beantworten
${PREFIX}support <nachricht> - Support-Ticket erstellen\n\n`;
  }

        // Admin-Befehle
        if (isAdmin) {
          helpText += `*⚔️ Admin-Befehle:*
${PREFIX}warn @user - Nutzer verwarnen
${PREFIX}kick @user - Nutzer entfernen
${PREFIX}add @user - Nutzer hinzufügen
${PREFIX}promote @user - Zum Admin machen
${PREFIX}demote @user - Admin-Rechte entziehen
${PREFIX}addxp <@nutzer/LID> <menge> - Schenke XP an einen Nutzer
${PREFIX}addcash <@nutzer/LID> <menge> - Schenke Coins an einen Nutzer
${PREFIX}addvip <@nutzer/LID> <zeit> - Gebe temporären VIP-Status (1d/12h/30m)
$addcash <@nutzer/LID> <menge> - Schenke Coins an einen Nutzer\n\n`;
        }

        if (hasAdminPerms(sender)) {
          helpText += `*👑 Admin & Owner Befehle:*
#broadcast <text> - Nachricht an alle Gruppen
#restart - Bot neu starten
#updateprofile - Profilinfo aktualisieren
#setrole @user <rolle> -jid Nutzerrolle setzen
#listroles - Alle Rollen anzeigen

*⚙️ KI-Einstellungen:*
#ai - KI-Status und Konfiguration
#ai model <name> - KI-Modell wechseln
#ai temp <0.1-1.0> - Temperature anpassen
#ai toggle - KI aktivieren/deaktivieren
#ai groups - Gruppen-KI-Zugriff
#ai owneronly - Nur-Owner Modus
#models - Verfügbare KI-Modelle anzeigen
#addmodel <name> - Neues Modell hinzufügen
#setmodel <name> - Standard-Modell setzen\n\n`;
        }

        helpText += `\n_Tipp: Nutze die Befehle ohne Parameter für mehr Info_`;
        
        return send(helpText);
        
        return send(helpText);
      }

      // Cooldown check für alle außer Owner
      if (!isOwner && cmd !== 'help' && cmd !== 'menu') {
        // Nur für bestimmte Befehle Cooldown aktivieren
        const cooldownCommands = [
          'work', 'fish', 'slot', 'hunt', 'dig', 'crime', 'rob', 'daily', 'weekly', 'monthly',
          'collect', 'open', 'mine', 'farm', 'adventure', 'explore', 'quest', 'raid', 'train',
          'duel', 'gamble', 'casino', 'blackjack', 'rps', 'lottery', 'spin', 'loot'
        ];
        if (cooldownCommands.includes(cmd)) {
          const cooldownMessage = checkCooldown(sender, cmd);
          if (cooldownMessage) {
            return send(cooldownMessage);
          }
        }
      }

      // ---------- COMMANDS ----------

      // Group Settings Command (gi)
      if (cmd === 'gi' && isGroup) {
        // Check if user is admin in the group
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = groupMetadata?.participants?.find(p => p.id === sender)?.admin;
        
        if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Du musst Admin in dieser Gruppe sein, um die Gruppeneinstellungen zu ändern.');
        }

        // Initialize group settings if not exist
        if (!groupSettings[from]) {
          groupSettings[from] = {
            welcome: {
              enabled: false,
              message: 'Willkommen in der Gruppe {user}! 👋'
            }
          };
        }

        const settings = groupSettings[from];
        const welcomeStatus = settings.welcome.enabled ? '✅ An' : '❌ Aus';
        const welcomeMsg = settings.welcome.message;

        return send(
`📋 *Gruppeneinstellungen*

*Welcome Nachricht:* ${welcomeStatus}
*Welcome Text:*
${welcomeMsg}

*Befehle:*
$welcome-an - Aktiviert die Welcome Nachricht
$welcome-aus - Deaktiviert die Welcome Nachricht
$welcome-set <text> - Setzt den Welcome Text
  {user} wird durch den Namen des neuen Mitglieds ersetzt`
        );
      }

      // Welcome Message Controls
      if ((cmd === 'welcome-an' || cmd === 'welcome-aus' || cmd === 'welcome-set') && isGroup) {
        const groupMetadata = await getGroupMetaSafe(from);
        const isGroupAdmin = groupMetadata?.participants?.find(p => p.id === sender)?.admin;
        
        if (!isGroupAdmin && !isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Du musst Admin in dieser Gruppe sein, um die Welcome-Nachricht zu ändern.');
        }

        // Initialize group settings if not exist
        if (!groupSettings[from]) {
          groupSettings[from] = {
            welcome: {
              enabled: false,
              message: 'Willkommen in der Gruppe {user}! 👋'
            }
          };
        }

        if (cmd === 'welcome-an') {
          groupSettings[from].welcome.enabled = true;
          save(FILES.groupSettings, groupSettings);
          return send('✅ Welcome-Nachricht wurde aktiviert.');
        }
        
        if (cmd === 'welcome-aus') {
          groupSettings[from].welcome.enabled = false;
          save(FILES.groupSettings, groupSettings);
          return send('✅ Welcome-Nachricht wurde deaktiviert.');
        }
        
        if (cmd === 'welcome-set') {
          if (!args.length) {
            return send('❌ Bitte gib einen Welcome-Text an.\nBeispiel: #welcome-set Willkommen {user} in unserer Gruppe!');
          }
          const newMessage = args.join(' ');
          groupSettings[from].welcome.message = newMessage;
          save(FILES.groupSettings, groupSettings);
          return send(`✅ Welcome-Nachricht wurde gesetzt auf:\n${newMessage}`);
        }
      }
      
      // Ticket Antwort Command
      if (cmd === 'answer' && 
          (isAuthorized(sender, ['OWNER', 'SUPPORTER', 'TEST_SUPPORTER'])) &&
          args.length >= 2) {
        
        const ticketId = args[0];
        const answer = args.slice(1).join(' ');
        
        // Prüfe ob in Support-Gruppe
        if (from === SUPPORT_CONFIG.SUPPORT_GROUP) {
          // Prüfe ob Ticket existiert
          if (!tickets[ticketId]) {
            return send(`❌ Ticket #${ticketId} wurde nicht gefunden.`);
          }
          
          const ticket = tickets[ticketId];
          
          // Sende Antwort nur an Support-Gruppe
          await sock.sendMessage(SUPPORT_CONFIG.SUPPORT_GROUP, {
            text: `📝 Antwort auf Ticket #${ticketId}:\n\n${answer}\n\nSupporter: @${sender.split('@')[0]}`,
            mentions: [sender]
          });
          
          // Update Ticket Status
          ticket.status = 'answered';
          ticket.answeredBy = sender;
          ticket.answer = answer;
          ticket.answerTimestamp = Date.now();
          save(FILES.tickets, tickets);
          
          // Bestätige dem Supporter
          return send(`✅ Deine Antwort wurde für Ticket #${ticketId} gesendet.`);
        } else {
          return send(`❌ Dieser Befehl kann nur in der Support-Gruppe verwendet werden.`);
        }
      }

      // HELP / MENU
      if (cmd === 'help') 


      // SETROLE - Verwaltet JIDs für Rollen
      if (cmd === 'setrole') {
        if (!isAuthorized(sender, ['OWNER'])) {
          return send('❌ Dieser Befehl ist nur für Owner verfügbar.');
        }

        const [role, ...jids] = args;
        if (!role || jids.length === 0) {
          return send(`❌ Nutzung: #setrole <ROLLE> <jid1,jid2,...>
Verfügbare Rollen: ${Object.keys(ROLES).join(', ')}

Beispiele:
#setrole ADMIN 1234@s.whatsapp.net
#setrole MOD 1234@s.whatsapp.net,5678@s.whatsapp.net`);
        }

        const roleUpper = role.toUpperCase();
        if (!ROLES.hasOwnProperty(roleUpper)) {
          return send(`❌ Ungültige Rolle. Verfügbare Rollen: ${Object.keys(ROLES).join(', ')}`);
        }

        // Parse comma-separated JIDs
        const jidList = jids.join(' ').split(',').map(j => j.trim());
        
        // Validate and normalize JIDs
        const validJids = jidList.filter(j => {
          const normalized = normalizeJid(j);
          return normalized && (normalized.endsWith('@s.whatsapp.net') || normalized.endsWith('@lid'));
        });

        if (validJids.length === 0) {
          return send('❌ Keine gültigen JIDs gefunden.');
        }

        // Speichere die JIDs für die Rolle
        ROLES[roleUpper] = validJids;

        // Speichere die Änderungen in der owner.json
        try {
          save(FILES.owner, {
            ...ownerCfg,
            roles: ROLES,
            ownerLid: OWNER_LID,
            ownerPriv: OWNER_PRIV,
            coownerLid: COOWNER_LID
          });
        } catch (e) {
          console.error('Failed to save role config:', e);
          return send('❌ Fehler beim Speichern der Rollen-Konfiguration.');
        }

        return send(`✅ ${validJids.length} JIDs wurden der Rolle ${roleUpper} zugewiesen.`);
      }

      // LISTROLES - Zeigt alle JIDs pro Rolle
      if (cmd === 'listroles') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
          return send('❌ Dieser Befehl ist nur für Owner/Co-Owner verfügbar.');
        }

        let message = '📋 Rollen und zugewiesene JIDs:\n\n';
        for (const [role, jids] of Object.entries(ROLES)) {
          message += `${role}: ${jids.length ? '\n' + jids.join('\n') : '(keine)'}\n\n`;
        }

        return send(message.trim());
      }

      // UPDATE PROFILE
      if (cmd === 'updateprofile') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
          return send('❌ Dieser Befehl ist nur für Owner/Co-Owner verfügbar.');
        }
        await send('🔄 Aktualisiere Bot-Profil...');
        await updateBotProfile();
        return send('✅ Profilaktualisierung abgeschlossen.');
      }

      // RESTART
      if (cmd === 'restart') {
        if (!hasAdminPerms(sender)) return send('❌ Dieser Befehl ist nur für Admins und Owner verfügbar.');
        await send('🔄 Bot wird neugestartet...');
        try {
          // Speichere Infos für die Neustart-Nachricht
          const restartInfo = {
            timestamp: Date.now(),
            chatId: from,
            initiator: sender
          };
          fs.writeFileSync(RESTART_FILE, JSON.stringify(restartInfo));
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🔄 Bot-Neustart durch ${sender}` });
        } catch {}
        process.exit(0); // Der Bot wird durch den Process Manager automatisch neugestartet
      }

      // debug: show caller jid and rank
      if (cmd === 'whoami' || cmd === 'me') {
        const normalizedSender = normalizeJid(sender);
        const r = ranks[normalizedSender] || users[normalizedSender]?.rank || '(none)';
        return send(`You: ${sender}\nNormalized: ${normalizedSender}\nRank: ${r}`);
      }
      

      // PING with response time
      if (cmd === 'ping') {
        const startTime = Date.now();
        await send('🏓 Pong!');
        const endTime = Date.now();
        const responseTime = endTime - startTime;
        return send(`Antwortzeit: ${responseTime}ms`);
      }

      // CODE: send a code snippet or file contents
      // Usage: #code relative/path/to/file [start-end]
      if (cmd === 'code') {
        // only owner or co-owner may use this command
        if (!isOwner) return send('❌ Nur der Inhaber darf den #code Befehl verwenden.');
        const target = args[0];
        if (!target) return send('❌ Nutzung: #code <pfad> [start-end]\nBeispiel: #code index.mjs 1-120');

        // prevent simple path traversal outside cwd
        const norm = path.normalize(target);
        if (norm.startsWith('..')) return send('❌ Zugriff auf Pfade außerhalb des Arbeitsverzeichnisses ist verboten.');

        const filePath = path.join(process.cwd(), norm);
        if (!fs.existsSync(filePath)) return send(`❌ Datei nicht gefunden: ${norm}`);

        let start = 1, end = Infinity;
        if (args[1]) {
          const m = String(args[1]).match(/(\d+)-(\d+)/);
          if (m) {
            start = Math.max(1, parseInt(m[1]));
            end = Math.max(start, parseInt(m[2]));
          }
        }

        try {
          const all = fs.readFileSync(filePath, 'utf8').split('\n');
          end = Math.min(end === Infinity ? all.length : end, all.length);
          const snippet = all.slice(start - 1, end).join('\n') || '(leer)';

          // If too long for a chat message, send as a document
          const MAX_TEXT = 1500;
          if (snippet.length > MAX_TEXT) {
            const buf = Buffer.from(snippet, 'utf8');
            await sock.sendMessage(from, {
              document: buf,
              mimetype: 'text/plain',
              fileName: `${path.basename(filePath)}.lines${start}-${end}.txt`
            });
            return send(`✅ Code als Datei gesendet: ${path.basename(filePath)} (Zeilen ${start}-${end})`);
          }

          // send as formatted code block
          const payload = '```' + snippet + '```';
          return send(payload);
        } catch (e) {
          console.error('code command error', e);
          return send('❌ Fehler beim Lesen oder Senden der Datei.');
        }
      }

      // BOTOFFLINE - only owner can toggle offline mode
      if (cmd === 'botoffline') {
        if (!isOwner) return send('❌ Nur der Inhaber kann den Offline-Modus setzen.');
        const action = (args[0] || '').toLowerCase();
        if (!action || action === 'status') {
          return send(`🔌 Offline-Modus: ${BOT_OFFLINE ? 'AN' : 'AUS'}`);
        }

        if (action === 'on' || action === 'enable' || action === 'true') {
          BOT_OFFLINE = true;
          saveBotState();
          return send('✅ Bot ist jetzt im Offline-Modus. Nur der Inhaber kann Befehle ausführen.');
        }

        if (action === 'off' || action === 'disable' || action === 'false') {
          BOT_OFFLINE = false;
          saveBotState();
          return send('✅ Bot ist jetzt wieder online für alle Nutzer.');
        }

        if (action === 'toggle') {
          BOT_OFFLINE = !BOT_OFFLINE;
          saveBotState();
          return send(`🔁 Offline-Modus umgeschaltet. Jetzt: ${BOT_OFFLINE ? 'AN' : 'AUS'}`);
        }

        return send('❌ Ungültige Nutzung. Nutze: #botoffline on|off|toggle|status');
      }

      // OLLAMA COMMANDS
      if (cmd === 'models' || cmd === 'ai') {
        if (!hasAdminPerms(sender)) {
          return send('❌ Dieser Befehl ist nur für Admins verfügbar.');
        }
        
        try {
          const installedModels = await listModels();
          const modelStatus = Object.entries(AI_CONFIG.models).map(([name, config]) => {
            const isInstalled = installedModels.some(m => m.toLowerCase() === name.toLowerCase());
            return `- ${name}: ${config.description} ${isInstalled ? '✅' : '❌'}`;
          }).join('\n');
          
          return send(
`📚 *KI-Modelle Übersicht*
Aktuelles Modell: ${currentModel}

*Verfügbare Modelle:*
${modelStatus}

✅ = Installiert
❌ = Nicht installiert`);
        } catch (error) {
          return send('❌ Fehler beim Abrufen der Modelle. Läuft Ollama?');
        }
      }

      if (cmd === 'addmodel') {
        if (!hasAdminPerms(sender)) {
          return send('❌ Dieser Befehl ist nur für Admins und Owner verfügbar.');
        }
        const modelName = args[0];
        if (!modelName) {
          return send('❌ Bitte gib einen Modellnamen an.\nBeispiel: #addmodel llama2\nOder: #addmodel all (um alle konfigurierten Modelle zu installieren)');
        }

        // Support pulling all configured models
        if (modelName.toLowerCase() === 'all') {
          const models = Object.keys(AI_CONFIG.models || {});
          if (!models.length) return send('⚠️ Keine Modelle in der Konfiguration gefunden.');

          await send(`⏳ Starte Installation von ${models.length} Modell(en)...`);
          for (const m of models) {
            try {
              await send(`⏳ Pulling model ${m}...`);
              const ok = await pullModel(m);
              await sleep(500);
              if (ok) await send(`✅ Modell "${m}" erfolgreich installiert.`);
              else await send(`❌ Modell "${m}" konnte nicht installiert werden.`);
            } catch (e) {
              await send(`❌ Fehler beim Installieren von "${m}": ${e && e.message ? e.message : e}`);
            }
            // small delay between pulls to avoid resource spikes
            await sleep(1500);
          }

          return send('✅ Alle Pull-Vorgänge abgeschlossen (siehe oben für Details).');
        }

        // Single model pull
        try {
          await send(`⏳ Lade Modell "${modelName}" herunter...`);
          const success = await pullModel(modelName);
          if (success) {
            return send(`✅ Modell "${modelName}" erfolgreich installiert!`);
          } else {
            return send(`❌ Fehler beim Herunterladen von "${modelName}".`);
          }
        } catch (error) {
          return send(`❌ Fehler: ${error.message}`);
        }
      }

      if (cmd === 'setmodel') {
        if (!hasAdminPerms(sender)) {
          return send('❌ Dieser Befehl ist nur für Admins und Owner verfügbar.');
        }

        const modelName = args[0]?.toLowerCase();
        if (!modelName) {
          return send(`❌ Bitte gib einen Modellnamen an.\nAktuelles Modell: ${currentModel}\nBeispiel: #setmodel llama2`);
        }

        try {
          // If model not known in config, attempt to install and add
          if (!AI_CONFIG.models[modelName]) {
            await send(`ℹ️ Modell "${modelName}" ist nicht in der Konfiguration, versuche Installation und Hinzufügen...`);
            const ok = await installModel(modelName, send);
            if (!ok) return send('❌ Konnte das Modell nicht installieren.');
            AI_CONFIG.models[modelName] = { name: modelName, description: '🆕 Neu hinzugefügt', maxTokens: 150 };
          }

          // Ensure it's installed
          if (!await isModelInstalled(modelName)) {
            await send(`🔄 Modell "${modelName}" ist noch nicht installiert. Starte Download...`);
            const success = await installModel(modelName, send);
            if (!success) return send('❌ Installation fehlgeschlagen.');
          }

          currentModel = modelName;
          process.env.OLLAMA_MODEL = modelName;
          return send(`✅ Standard-Modell wurde auf "${modelName}" gesetzt.`);
        } catch (error) {
          console.error('setmodel error:', error);
          return send(`❌ Fehler beim Setzen des Modells: ${error?.message || error}`);
        }
      }

      // ASK / AI QUERY
      if (cmd === 'ask' || cmd === 'aiask') {
        const prompt = args.join(' ');
        if (!prompt) return send('❌ Nutzung: #ask <Frage oder Prompt>');
        
        // Check if AI is enabled and user has permission
        if (!AI_CONFIG.settings.enabled) {
          return send('❌ KI ist derzeit deaktiviert.');
        }
        
        if (AI_CONFIG.settings.ownerOnly && !isOwner) {
          return send('❌ KI ist derzeit nur für Owner verfügbar.');
        }
        
        if (!AI_CONFIG.settings.groupsEnabled && isGroup) {
          return send('❌ KI ist in Gruppen derzeit deaktiviert.');
        }
        
        try {
          await send('🤖 Denke nach...');
          const reply = await generateCompletion({
            model: currentModel,
            prompt: prompt,
            temperature: AI_CONFIG.settings.temperature,
            stream: false
          });
          return send(reply);
        } catch (e) {
          console.error('AI generate error:', e);
          return send('❌ Fehler beim Abrufen der Antwort von Ollama. Läuft der Ollama-Dienst?');
        }
      }

      // NEWSESSION
      if (cmd === 'newsession') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Dieser Befehl ist nur für Admins verfügbar.');
        }

        const sessionName = args[0];
        if (!sessionName) {
          return send('❌ Bitte gib einen Namen für die neue Session an.\nBeispiel: #newsession meinbot');
        }

        // Create new session directory
        const sessionPath = path.join(SESSIONS_DIR, sessionName);
        const authPath = sessionPath;  // auth files go directly in session dir

        if (!fs.existsSync(authPath)) {
          fs.mkdirSync(authPath, { recursive: true });
        }

        try {
          const { state, saveCreds } = await useMultiFileAuthState(authPath);
          const { version } = await fetchLatestBaileysVersion();
          const newSock = makeWASocket({ 
            version,
            logger: P({ level: 'silent' }),
            printQRInTerminal: false,
            auth: state,
            browser: ['RaptorBot MultiSession', 'Chrome', '4.0.0'],
            markOnlineOnConnect: true
          });

          // keep reference to avoid GC
          activeSessions.set(sessionName, newSock);

          // Wrap saveCreds to log when credentials are updated and list auth files
          newSock.ev.on('creds.update', async (creds) => {
            try {
              console.log(`newsession creds.update for ${sessionName}`);
              // call the provided saveCreds to persist
              await saveCreds(creds);
              // list files in authPath for debugging (do not log file contents)
              try {
                const files = fs.readdirSync(authPath).map(f => f);
                console.log(`Auth files for ${sessionName}:`, files);
                // also send a minimal notification (filenames only) to the command sender
                await sock.sendMessage(from, { text: `🔐 Credentials updated for session "${sessionName}". Saved files: ${files.join(', ')}` });
              } catch (e) {
                console.error(`Failed to list auth files for ${sessionName}:`, e);
              }
            } catch (e) {
              console.error('Error in newsession creds.update wrapper:', e);
            }
          });

          // Start a PM2-managed process for this session using the bootstrapper
          try {
            const pm2Name = `raptor-${sessionName}`;
            // Start the session via PM2; pass the sessionName as an arg to the bootstrap script
            exec(`pm2 start session-bootstrap.mjs --name ${pm2Name} -- ${sessionName}`, { cwd: process.cwd() }, (err, stdout, stderr) => {
              if (err) {
                console.error(`Failed to start pm2 process for ${sessionName}:`, err, stderr);
                return;
              }
              console.log(`pm2 start output for ${pm2Name}:`, stdout);
              sock.sendMessage(from, { text: `🟢 PM2: started process ${pm2Name}` }).catch(()=>{});
            });
          } catch (e) {
            console.error('Failed to exec pm2 start for newsession:', e);
          }

          // Handle QR code generation
          newSock.ev.on('connection.update', async (update) => {
            // Log the entire update for debugging (helps see connection state changes)
            console.log('newsession connection.update:', update);

            const { qr, connection } = update;

            // If QR string provided, render and send image; fallback to sending the QR text as message
            if (qr) {
              try {
                const dataUrl = await QRCode.toDataURL(qr, { type: 'image/png', scale: 6 });
                const base64 = dataUrl.split(',')[1];
                const qrBuffer = Buffer.from(base64, 'base64');
                await sock.sendMessage(from, {
                  image: qrBuffer,
                  mimetype: 'image/png',
                  caption: `🤖 Neue Bot Session: ${sessionName}\nScanne den QR-Code um die Session zu starten.`
                });
              } catch (err) {
                console.error('QR send error (image):', err);
                // Fallback: send the QR payload as text so it can be scanned by a phone that supports text-to-QR
                try {
                  await sock.sendMessage(from, { text: `🤖 Neue Bot Session: ${sessionName}\nQR-String:\n${qr}` });
                } catch (e) {
                  console.error('QR send error (text fallback):', e);
                }
              }
            }

            // Notify when the new session actually opens/authenticates
            if (connection === 'open') {
              console.log(`new session ${sessionName} opened`);
              try {
                const id = newSock.user && newSock.user.id ? newSock.user.id : '(unknown)';
                await sock.sendMessage(from, { text: `✅ Neue Session "${sessionName}" erfolgreich angemeldet! JID: ${id}` });
              } catch (err) {
                console.error('Failed to send newsession open confirmation:', err);
              }
            }

            // If the socket closed, print a deep inspection of lastDisconnect to see server output/data
            if (update.lastDisconnect) {
              try {
                console.error(`newsession ${sessionName} lastDisconnect:`, util.inspect(update.lastDisconnect, { depth: 5 }));
              } catch (e) {
                console.error('Failed to inspect lastDisconnect:', e, update.lastDisconnect);
              }
            }
            // If the stream errored with code 515 (restart required), ask PM2 to restart online raptor-* processes
            try {
              const ld = update.lastDisconnect;
              if (ld && ld.output && ld.output.statusCode === 515) {
                console.log('Detected 515 stream error: asking PM2 to restart online raptor-* processes');
                // get pm2 process list in json
                exec('pm2 jlist', (err, stdout, stderr) => {
                  if (err) {
                    console.error('pm2 jlist failed:', err, stderr);
                    return;
                  }
                  try {
                    const list = JSON.parse(stdout);
                    for (const proc of list) {
                      const pmName = proc.name;
                      const status = proc.pm2_env && proc.pm2_env.status;
                      if (pmName && pmName.startsWith('raptor-') && status === 'online') {
                        console.log(`Restarting pm2 process ${pmName}`);
                        exec(`pm2 restart ${pmName}`, (rerr, rout, rerrout) => {
                          if (rerr) console.error(`Failed to restart ${pmName}:`, rerr, rerrout);
                          else console.log(`Restarted ${pmName}:`, rout);
                        });
                      }
                    }
                  } catch (e) {
                    console.error('Failed to parse pm2 jlist output:', e);
                  }
                });
              }
            } catch (e) {
              console.error('Error while handling 515/pm2 restart logic:', e);
            }
          });

          // Save credentials when authenticated
          newSock.ev.on('creds.update', saveCreds);
        } catch (err) {
          console.error('Session creation error:', err);
          return send('❌ Fehler beim Erstellen der neuen Session.');
        }
        
        return;
      }

      // HIDETAG
      if (cmd === 'hidetag') {
        // Check if it's a group
        if (!isGroup) {
          return send('❌ Dieser Befehl funktioniert nur in Gruppen.');
        }

        // Check permissions
        if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'ADMIN'])) {
          return send('❌ Dieser Befehl ist nur für Admins verfügbar.');
        }

        const message = args.join(' ');
        if (!message) {
          return send('❌ Bitte gib eine Nachricht an.\nBeispiel: #hidetag Wichtige Ankündigung!');
        }

        try {
          const groupMembers = await getGroupMetaSafe(from);
          const mentions = groupMembers.participants.map(p => p.id);
          
          await sock.sendMessage(from, {
            text: message,
            mentions: mentions
          }, {
            quoted: m
          });
        } catch (err) {
          console.error('Hidetag error:', err);
          return send('❌ Fehler beim Ausführen des Befehls.');
        }
      }

      // BALANCE
      if (cmd === 'balance') {
        ensureUser(sender);
        const u = users[sender];
        return send(`💰 Coins: ${u.coins}\n⭐ Level: ${u.level}\nXP: ${u.xp}`);
      }

      // ADD XP (Admin only)
      if (cmd === 'addxp' && hasAdminPerms(sender)) {
        const target = args[0];
        const amount = parseInt(args[1]);

        if (!target || isNaN(amount) || amount < 0) {
          return send('❌ Verwendung: $addxp <@nutzer/LID> <menge>');
        }

        const targetJid = await findJid(target, from);
        if (!targetJid) {
          return send('❌ Nutzer nicht gefunden.');
        }

        ensureUser(targetJid);
        users[targetJid].xp = (users[targetJid].xp || 0) + amount;
        save(FILES.users, users);

        return send(`✅ ${amount} XP an @${targetJid.split('@')[0]} geschenkt.`, {
          mentions: [targetJid]
        });
      }

      // ADD CASH (Admin only)
      if (cmd === 'addcash' && hasAdminPerms(sender)) {
        const target = args[0];
        const amount = parseInt(args[1]);

        if (!target || isNaN(amount) || amount < 0) {
          return send('❌ Verwendung: $addcash <@nutzer/LID> <menge>');
        }

        const targetJid = await findJid(target, from);
        if (!targetJid) {
          return send('❌ Nutzer nicht gefunden.');
        }

        ensureUser(targetJid);
        users[targetJid].coins = (users[targetJid].coins || 0) + amount;
        save(FILES.users, users);

        return send(`✅ ${amount} Coins an @${targetJid.split('@')[0]} geschenkt.`, {
          mentions: [targetJid]
        });
      }

      // ADD VIP (Admin only)
      if (cmd === 'addvip' && hasAdminPerms(sender)) {
        const target = args[0];
        const duration = args[1];

        if (!target || !duration) {
          return send('❌ Verwendung: $addvip <@nutzer/LID> <zeit>\nBeispiel: 1d (1 Tag), 12h (12 Stunden), 30m (30 Minuten)');
        }

        const targetJid = await findJid(target, from);
        if (!targetJid) {
          return send('❌ Nutzer nicht gefunden.');
        }

        if (!addVip(targetJid, duration)) {
          return send('❌ Ungültiges Zeitformat. Nutze z.B. 1d, 12h, oder 30m');
        }

        ensureUser(targetJid);
        const expiry = new Date(vipExpiry.get(targetJid)).toLocaleString();
        return send(`✅ VIP-Status für @${targetJid.split('@')[0]} bis ${expiry} aktiviert.`, {
          mentions: [targetJid]
        });
      }

      // FISH command
      if (cmd === 'fish') {
        const cooldown = checkCooldown(sender, 'fish');
        if (cooldown) return send(cooldown);

        // Zufällige Fisch-Events
        const events = [
          // Erfolge mit Fischen
          { chance: 30, type: 'fish', text: '🐟 Du hast einen kleinen Fisch gefangen! (+10 Coins)', coins: 10 },
          { chance: 20, type: 'fish', text: '🐠 Du hast einen bunten Tropenfisch gefangen! (+20 Coins)', coins: 20 },
          { chance: 10, type: 'fish', text: '🐡 Du hast einen Kugelfisch gefangen! (+30 Coins)', coins: 30 },
          { chance: 5, type: 'fish', text: '🦈 Wow! Du hast einen kleinen Hai gefangen! (+100 Coins)', coins: 100 },
          
          // Spezielle Objekte
          { chance: 3, type: 'treasure', text: '📦 Du hast eine alte Schatztruhe gefunden! (+200 Coins)', coins: 200 },
          { chance: 2, type: 'bottle', text: '📜 Du hast eine Flaschenpost gefunden! (+50 Coins, +20 XP)', coins: 50, xp: 20 },
          
          // Fails & Pech
          { chance: 15, type: 'fail', text: '💨 Dein Köder wurde gestohlen...' },
          { chance: 10, type: 'fail', text: '😅 Du hast nur Seegras gefangen.' },
          { chance: 4, type: 'fail', text: '💦 Du bist ins Wasser gefallen!' },
          { chance: 1, type: 'disaster', text: '🌊 Eine riesige Welle hat dein Boot umgeworfen! (-50 Coins)', coins: -50 }
        ];

        // Gewichte für Zufallsauswahl berechnen
        const totalWeight = events.reduce((sum, event) => sum + event.chance, 0);
        let random = Math.random() * totalWeight;
        
        // Event auswählen
        let selectedEvent;
        for (const event of events) {
          random -= event.chance;
          if (random <= 0) {
            selectedEvent = event;
            break;
          }
        }

        // Belohnungen & Strafen anwenden
        ensureUser(sender);
        if (selectedEvent.coins) {
          users[sender].coins = (users[sender].coins || 0) + selectedEvent.coins;
        }
        if (selectedEvent.xp) {
          users[sender].xp = (users[sender].xp || 0) + selectedEvent.xp;
        }
        save(FILES.users, users);

        return send(selectedEvent.text);
      }

      // GIVE COINS
      if (cmd === 'give') {
        const target = args[0];
        const amount = parseInt(args[1]);

        if (!target || isNaN(amount) || amount <= 0) {
          return send('❌ Nutzung: #give <nummer|@mention> <betrag>');
        }

        const targetJid = normalizeJid(target);
        if (!targetJid) {
          return send('❌ Ungültiger Empfänger.');
        }

        // Prüfe ob der Sender genug Coins hat
        if ((users[sender]?.coins || 0) < amount) {
          return send('❌ Du hast nicht genug Coins!');
        }

        // Stelle sicher dass beide Benutzer existieren
        ensureUser(sender);
        ensureUser(targetJid);

        // Verhindere Selbst-Überweisung
        if (isSameJid(sender, targetJid)) {
          return send('❌ Du kannst dir nicht selbst Coins geben!');
        }

        // Führe die Überweisung durch
        users[sender].coins -= amount;
        users[targetJid].coins = (users[targetJid].coins || 0) + amount;
        save(FILES.users, users);

        // Sende Bestätigungen
        try {
          // Benachrichtige den Empfänger
          await sock.sendMessage(targetJid, {
            text: `💰 Du hast ${amount} Coins von @${sender.split('@')[0]} erhalten!`,
            mentions: [sender]
          });
        } catch (e) {
          console.error('Failed to notify receiver:', e);
        }

        return send(`✅ Du hast ${amount} Coins an @${targetJid.split('@')[0]} gesendet!`, {
          mentions: [targetJid]
        });
      }

      // WORK
      if (cmd === 'work') {
        const earn = randInt(50, 200);
        users[sender].coins = (users[sender].coins||0) + earn;
        users[sender].xp = (users[sender].xp||0) + 20;
        save(FILES.users, users);
        return send(`🛠 Du hast ${earn} Coins verdient!`);
      }

      // DAILY (1..1000 random, 24h cooldown)
      if (cmd === 'daily') {
        const now = Date.now();
        const last = users[sender].lastDaily || 0;
        if (now - last < 24*3600*1000) {
          const msLeft = 24*3600*1000 - (now - last);
          const hours = Math.floor(msLeft / 3600000);
          return send(`🕒 Du kannst $daily erst wieder in ca. ${hours} Stunden nutzen.`);
        }
        const amount = randInt(1, 1000);
        users[sender].coins = (users[sender].coins||0) + amount;
        users[sender].lastDaily = now;
        save(FILES.users, users);
        return send(`🎁 Daily: Du hast ${amount} Coins erhalten!`);
      }

      // SHOP / BUY / INVENTORY / USE
      const SHOP = { potion:{price:100,desc:'Heilt 10 HP'}, box:{price:500,desc:'Zufälliger Gegenstand'}, vip:{price:2000,desc:'VIP-Rang'} };
      if (cmd === 'shop') {
        let out = '🛒 Shop:\n';
        for (const [k,v] of Object.entries(SHOP)) out += `• ${k} — ${v.price} 💰 | ${v.desc}\n`;
        return send(out);
      }
      if (cmd === 'buy') {
        const item = args[0];
        if (!item || !SHOP[item]) return send('Nutzung: $buy <item>');
        if ((users[sender].coins||0) < SHOP[item].price) return send('💸 Zu wenig Coins');
        users[sender].coins -= SHOP[item].price;
        users[sender].items[item] = (users[sender].items[item]||0) + 1;
        save(FILES.users, users);
        return send(`✅ ${item} gekauft.`);
      }
      if (cmd === 'inventory') {
        const inv = users[sender].items || {};
        const out = Object.keys(inv).length ? Object.entries(inv).map(([k,v])=>`${k}: ${v}`).join('\n') : '(leer)';
        return send(`🎒 Inventar:\n${out}`);
      }
      if (cmd === 'use') {
        const it = args[0];
        if (!it) return send('Nutzung: $use <item>');
        if (!users[sender].items || !users[sender].items[it]) return send('Item nicht vorhanden');
        if (it === 'potion') {
          users[sender].items[it] -= 1;
          users[sender].xp = (users[sender].xp||0) + 10;
          save(FILES.users, users);
          return send('💊 Trank verwendet: +10 XP');
        }
        if (it === 'box') {
          users[sender].items[it] -= 1;
          const coins = randInt(50,300);
          users[sender].coins = (users[sender].coins||0) + coins;
          save(FILES.users, users);
          return send(`🎁 Box geöffnet: +${coins} Coins`);
        }
        return send('Item verwendet.');
      }

      // SLOTS
      if (cmd === 'slot') {
        const bet = parseInt(args[0]) || 50;
        if ((users[sender].coins||0) < bet) return send('Zu wenig Coins.');
        const spin = spinSlots();
        const win = spin[0] === spin[1] && spin[1] === spin[2];
        if (win) {
          users[sender].coins += bet * 3;
          users[sender].xp = (users[sender].xp||0) + 50;
          save(FILES.users, users);
          return send(`🎰 | ${spin.join(' | ')} |\n🎉 Jackpot! +${bet*3} Coins, +50 XP`);
        } else {
          users[sender].coins -= bet;
          save(FILES.users, users);
          return send(`🎰 | ${spin.join(' | ')} |\n😢 Verloren -${bet} Coins`);
        }

        
        }

        // Rock-Paper-Scissors (RPS)
        if (cmd === 'rps') {
          const choice = (args[0] || '').toLowerCase();
          const valid = ['rock','paper','scissors','stein','papier','schere'];
          if (!valid.includes(choice)) return send('Usage: $rps <rock|paper|scissors> (or stein|papier|schere)');
          const norm = (choice === 'stein') ? 'rock' : (choice === 'papier') ? 'paper' : (choice === 'schere') ? 'scissors' : choice;
          const botOpt = ['rock','paper','scissors'][randInt(0,2)];
          const draw = norm === botOpt;
          const win = (norm === 'rock' && botOpt === 'scissors') || (norm === 'paper' && botOpt === 'rock') || (norm === 'scissors' && botOpt === 'paper');
          let res = `🤖 Ich: ${botOpt}\nDu: ${norm}\n`;
          if (draw) res += 'Unentschieden 😐';
          else if (win) { users[sender].coins = (users[sender].coins||0) + 50; users[sender].xp = (users[sender].xp||0) + 10; save(FILES.users, users); res += 'Du gewinnst! +50 Coins +10 XP 🎉'; }
          else { users[sender].coins = Math.max(0,(users[sender].coins||0) - 20); save(FILES.users, users); res += 'Du verlierst -20 Coins 😢'; }
          return send(res);
        }

      // BLACKJACK (simple persistent per-user)
      if (cmd === 'bjstart') {
        const player = [bjDraw(), bjDraw()];
        const dealer = [bjDraw(), bjDraw()];
        users[sender].bj = { player, dealer, active: true };
        save(FILES.users, users);
        return send(`🃏 Blackjack gestartet!\nDeine Karten: ${player.map(c=>c.value+c.suit).join(', ')}\nDealer zeigt: ${dealer[0].value+dealer[0].suit}\nBenutze $hit oder $stand`);
      }
      if (cmd === 'hit') {
        let out = `Dealer: ${d}, Du: ${p}`;
        if (p > 21) out += '\nDu verloren.';
        else if (d > 21 || p > d) { users[sender].coins = (users[sender].coins||0) + 75; users[sender].xp = (users[sender].xp||0) + 40; out += '\n🎉 Du gewinnst! +75 Coins +40 XP'; }
        else if (p === d) out += '\nUnentschieden';
        else out += '\nDealer gewinnt';
        delete users[sender].bj;
        save(FILES.users, users);
        return send(out);
      }

      // ========== PET SYSTEM ==========
      // adopt: $adopt dog|cat|bird <name?>
      if (cmd === 'adopt') {
        const type = (args[0] || '').toLowerCase();
        if (!['dog','cat','bird'].includes(type)) return send('Usage: $adopt <dog|cat|bird>');
        const name = args.slice(1).join(' ') || null;
        pets[sender] = { type, name, xp:0, hunger:100, happiness:100, lastFed: Date.now() };
        save(FILES.pets, pets);
        return send(`🐾 Du hast ein ${type} ${name?('mit Namen ' + name):''} adoptiert!`);
      }
      if (cmd === 'petinfo' || cmd === 'pet') {
        const p = pets[sender];
        if (!p) return send('Du hast kein Haustier. $adopt <dog|cat|bird>');
        return send(`🐶 Haustier: ${p.type} ${p.name ? ('- ' + p.name) : ''} \nHunger: ${p.hunger}%\nGlück: ${p.happiness}%\nXP: ${p.xp}`);
      }
      if (cmd === 'feed') {
        const p = pets[sender];
        if (!p) return send('Du hast kein Haustier.');
        p.hunger = Math.min(100, (p.hunger||0) + 20);
        p.happiness = Math.min(100, (p.happiness||0) + 10);
        p.lastFed = Date.now();
        save(FILES.pets, pets);
        return send(`🍖 ${p.type} gefüttert. Hunger: ${p.hunger}% Glück: ${p.happiness}%`);
      }
      if (cmd === 'play') {
        const p = pets[sender];
        if (!p) return send('Du hast kein Haustier.');
        p.happiness = Math.min(100, (p.happiness||0) + 20);
        p.xp = (p.xp||0) + 5;
        save(FILES.pets, pets);
        return send(`🎾 Du spielst mit ${p.type}. Glück: ${p.happiness}% XP: ${p.xp}`);
      }

      // ========== SUPPORT / TICKETS ==========
      // $support <text>  (creates ticket)
      if (cmd === 'support' || cmd === 'ticket') {
        console.log('CMD support called by', sender, 'rank=', ranks[sender], 'args=', args);
        const text = args.join(' ') || 'Kein Text';
        
          // Generiere Ticket ID mit Zähler
          ticketCounter++;
          const ticketId = ticketCounter.toString().padStart(4, '0');
        
          // Speichere Ticket
          tickets[ticketId] = {
            id: ticketId,
            sender: sender,
            message: text,
            status: 'open',
            timestamp: Date.now()
          };
          save(FILES.tickets, tickets);
        
          // Sende Ticket an Ticket-Gruppe (Anfragen sollen in die TICKET_GROUP)
          try {
            await sock.sendMessage(SUPPORT_CONFIG.TICKET_GROUP, {
              text: `🎫 Neues Ticket #${ticketId}\nVon: @${sender.split('@')[0]}\n\nNachricht:\n${text}\n\nSupporter können mit ${PREFIX}answer ${ticketId} <antwort> antworten.`,
              mentions: [sender]
            });
          
            // Also post a short todo for the team in the support group so team members see it
            todoCounter++;
            const tdId = `TD${String(todoCounter).padStart(3,'0')}`;
            teamTodos[tdId] = { id: tdId, text, creator: sender, status: 'open', created: Date.now() };
            save(FILES.teamTodos, teamTodos);
            // Note: No group notification per user request — todos are private until $todo list is run

            // Bestätige Ticket-Erstellung
            return send(`✅ Dein Ticket wurde erstellt (#${ticketId}). Ein Supporter wird sich bald bei dir melden.`);
          } catch (e) {
            console.error('Fehler beim Senden des Tickets:', e);
            return send('❌ Fehler beim Erstellen des Tickets. Bitte versuche es später erneut.');
          }
      }
      if (cmd === 'tickets') {
        if (!isAuthorized(sender, ['OWNER','COOWNER'])) return send('Kein Zugriff.');
        const list = Object.values(tickets).map(t => `${t.id} - ${t.user} - ${t.status}`).join('\n') || '(keine)';
        return send(`🎫 Tickets:\n${list}`);
      }
      if (cmd === 'closeticket') {
        if (!isAuthorized(sender, ['OWNER','COOWNER'])) return send('Kein Zugriff.');
        const id = args[0];
        if (!id || !tickets[id]) return send('Usage: $closeticket <id>');
        tickets[id].status = 'closed';
        save(FILES.tickets, tickets);
        return send(`✅ Ticket ${id} geschlossen.`);
      }

      // ======= TEAM TODOS =======
      // $todo add <text> -> creates a team todo and notifies SUPPORT_GROUP
      // $todo list -> lists todos
      // $todo done <id> -> mark todo as done
      if (cmd === 'todo' || cmd === 'todos') {
        const sub = (args[0]||'').toLowerCase();
        if (!sub || sub === 'list') {
          const all = Object.values(teamTodos);
          if (!all.length) return send('📝 Es gibt derzeit keine Team-Todos.');
          const now = Date.now();
          const lines = all.map(t => {
            let line = `${t.id} [${t.status}] - ${t.text}`;
            if (t.assignee) line += ` (zugewiesen an @${t.assignee.split('@')[0]})`;
            if (t.deadline) {
              const days = Math.ceil((t.deadline - now) / (1000 * 60 * 60 * 24));
              line += ` [Fällig: ${new Date(t.deadline).toLocaleDateString('de-DE')}${days > 0 ? ` (in ${days} Tagen)` : days === 0 ? ' (heute)' : ' (überfällig)'}]`;
            }
            line += t.status === 'done' ? ' ✅' : '';
            return line;
          });
          return send(`📝 Team-Todos:\n${lines.join('\n')}`);
        }

        if (sub === 'add') {
          const text = args.slice(1).join(' ');
          if (!text) return send('Usage: $todo add <text>');
          todoCounter++;
          const tdId = `TD${String(todoCounter).padStart(3,'0')}`;
          teamTodos[tdId] = { id: tdId, text, creator: sender, status: 'open', created: Date.now() };
          save(FILES.teamTodos, teamTodos);
          // No group notification — todos remain private until someone runs $todo list
          return send(`✅ Team-Todo ${tdId} erstellt.`);
        }

        if (sub === 'assign') {
          // Usage: $todo assign <id> <jid|number|reply|me>
          const id = args[1];
          if (!id || !teamTodos[id]) return send('Usage: $todo assign <id> <jid|reply|me>');
          // try to resolve target
          let target = args[2];
          const ctx = m.message && m.message.extendedTextMessage && m.message.extendedTextMessage.contextInfo;
          if (!target) {
            if (ctx && Array.isArray(ctx.mentionedJid) && ctx.mentionedJid.length) target = ctx.mentionedJid[0];
            else if (ctx && ctx.participant) target = ctx.participant;
            else return send('Bitte gib eine Ziel-JID an oder antworte auf eine Nachricht der Person oder nutze "me".');
          }
          if (target === 'me') target = sender;
          // normalize if looks like a raw number
          const assignee = normalizeJid(target);
          teamTodos[id].assignee = assignee;
          teamTodos[id].assignedBy = sender;
          teamTodos[id].assignedAt = Date.now();
          save(FILES.teamTodos, teamTodos);
          // confirm to assigner
          try {
            await sock.sendMessage(from, { text: `✅ ${id} wurde zugewiesen an @${assignee.split('@')[0]}`, mentions: [assignee] });
          } catch (e) {
            // fallback plain text
            await send(`✅ ${id} wurde zugewiesen an ${assignee}`);
          }
          // notify assignee privately (best-effort, no group notifications)
          try {
            await sock.sendMessage(assignee, { text: `📝 Du wurdest dem Team-Todo ${id} zugewiesen von @${sender.split('@')[0]}:\n${teamTodos[id].text}` });
          } catch (e) { /* ignore */ }
          return;
        }

        if (sub === 'unassign' || sub === 'unassign') {
          const id = args[1] || args[0];
          if (!id || !teamTodos[id]) return send('Usage: $todo unassign <id>');
          delete teamTodos[id].assignee;
          delete teamTodos[id].assignedBy;
          delete teamTodos[id].assignedAt;
          save(FILES.teamTodos, teamTodos);
          return send(`✅ ${id} wurde die Zuweisung entfernt.`);
        }

        if (sub === 'done' || sub === 'complete') {
          const id = args[1] || args[0];
          if (!id || !teamTodos[id]) return send('Usage: $todo done <id>');
          teamTodos[id].status = 'done';
          teamTodos[id].doneBy = sender;
          teamTodos[id].doneAt = Date.now();
          save(FILES.teamTodos, teamTodos);
          // No group notification for done — only confirm to the user who ran the command
          return send(`✅ Team-Todo ${id} als erledigt markiert.`);
        }

        if (sub === 'remove' || sub === 'rm') {
          if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN'])) return send('Kein Zugriff.');
          const id = args[1] || args[0];
          if (!id || !teamTodos[id]) return send('Usage: $todo remove <id>');
          delete teamTodos[id];
          save(FILES.teamTodos, teamTodos);
          return send(`🗑️ Team-Todo ${id} entfernt.`);
        }

        if (sub === 'deadline' || sub === 'due') {
          const id = args[1];
          if (!id || !teamTodos[id]) return send('Usage: $todo deadline <id> <date|remove>\nBeispiele für Datum:\n- 25.12\n- +7 (in 7 Tagen)\n- remove (Deadline entfernen)');
          
          const dateStr = args.slice(2).join(' ').toLowerCase();
          if (!dateStr) return send('Bitte gib ein Datum an oder "remove" um die Deadline zu entfernen.');
          
          if (dateStr === 'remove' || dateStr === 'clear' || dateStr === 'none') {
            delete teamTodos[id].deadline;
            save(FILES.teamTodos, teamTodos);
            return send(`✅ Deadline für ${id} wurde entfernt.`);
          }
          
          let deadline;
          if (dateStr.startsWith('+')) {
            // +N format for N days from now
            const days = parseInt(dateStr.slice(1));
            if (isNaN(days)) return send('Ungültiges Format. Nutze +N für N Tage ab heute.');
            deadline = Date.now() + (days * 24 * 60 * 60 * 1000);
          } else {
            // Try DD.MM or DD.MM.YYYY format
            const parts = dateStr.split('.');
            if (parts.length < 2) return send('Ungültiges Datumsformat. Nutze TT.MM oder TT.MM.JJJJ');
            const day = parseInt(parts[0]);
            const month = parseInt(parts[1]) - 1; // JS months are 0-based
            const year = parts.length > 2 ? parseInt(parts[2]) : new Date().getFullYear();
            const date = new Date(year, month, day);
            if (isNaN(date.getTime())) return send('Ungültiges Datum.');
            deadline = date.getTime();
          }
          
          teamTodos[id].deadline = deadline;
          teamTodos[id].deadlineSetBy = sender;
          teamTodos[id].deadlineSetAt = Date.now();
          save(FILES.teamTodos, teamTodos);
          
          const daysUntil = Math.ceil((deadline - Date.now()) / (1000 * 60 * 60 * 24));
          return send(`✅ Deadline für ${id} gesetzt auf ${new Date(deadline).toLocaleDateString('de-DE')} (in ${daysUntil} Tagen).`);
        }

        return send('Usage: $todo add <text> | $todo list | $todo done <id> | $todo deadline <id> <date> | $todo remove <id> (owner/admin)');
      }

      // ======= MODERATION =======
      if (cmd === 'ban') {
        console.log('CMD ban called by', sender, 'rank=', ranks[sender], 'args=', args);
  if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $ban <num|jid> [kick]');
        const jid = normalizeJid(t);
        const reason = args.slice(1).filter(a=>a!=='kick' && a!=='remove').join(' ') || 'Kein Grund';
        bans[jid] = { by: sender, at: new Date().toISOString(), reason };
        save(FILES.bans, bans);

        // optional 'kick' flag: remove user from all groups
        if (args.includes('kick') || args.includes('remove')) {
          try {
            const groups = await sock.groupFetchAllParticipating();
            for (const gid of Object.keys(groups)) {
              try { await sock.groupParticipantsUpdate(gid, [jid], 'remove'); await sleep(200); } catch {} 
            }
          } catch (e) { console.error('ban kick error', e); }
        }

        try { await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🚫 Gebannt: ${jid}\nDurch: ${sender}\nGrund: ${reason}` }); } catch {}
        return send(`🚫 ${jid} gebannt.${(args.includes('kick')||args.includes('remove')) ? ' (entfernt aus Gruppen)' : ''}`);
      }
      if (cmd === 'banlist') {
        if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN'])) return send('Kein Zugriff.');
        const list = Object.entries(bans).map(([j,b])=>`${j} — by ${b.by} at ${b.at} — ${b.reason}`).join('\n') || '(keine)';
        return send(`🚫 Gebannte Nutzer:\n${list}`);
      }
      if (cmd === 'unban') {
        if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $unban <num|jid>');
        const jid = normalizeJid(t);
        delete bans[jid]; save(FILES.bans, bans);
        return send(`✅ ${jid} entbannt.`);
      }

      // YEETBAN: remove from all groups where bot is in — bot must be admin to remove participants
      if (cmd === 'yeetban') {
        console.log('CMD yeetban called by', sender, 'rank=', ranks[sender], 'args=', args);
  if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN'])) return send('Kein Zugriff.');
        // allow: $yeetban <num|jid> <grund>
        // or reply to a user's message with $yeetban <grund>
        let target = args[0];
        // if no explicit arg, try to get participant from a quoted message (reply)
        try {
          const ctx = m.message && m.message.extendedTextMessage && m.message.extendedTextMessage.contextInfo;
          if (!target && ctx && ctx.participant) target = ctx.participant;
          // also support mentionedJid in case user mentioned someone
          if (!target && ctx && Array.isArray(ctx.mentionedJid) && ctx.mentionedJid.length) target = ctx.mentionedJid[0];
          // log quick debug info to console (don't spam the chat)
          try { console.log(`DEBUG: args=${args.join(' ')} target=${target||'(none)'} mentions=${(ctx && ctx.mentionedJid) ? ctx.mentionedJid.join(',') : '(none)'} auth=${isAuthorized(sender,['OWNER','COOWNER','ADMIN'])}`); } catch(e){}
        } catch (e) { /* ignore */ }

        if (!target) return send('Usage: $yeetban <num|jid> (oder reply mit $yeetban <grund>)');
        const jid = normalizeJid(target);
        const reason = args.slice(1).join(' ') || 'Kein Grund';

        // immediately record ban locally so the user gets blocked from using commands
        bans[jid] = { by: sender, at: new Date().toISOString(), reason };
        save(FILES.bans, bans);

        try {
          // Hole alle Gruppen, in denen der Bot ist
          const groups = await sock.groupFetchAllParticipating();
          let removed = 0, failed = 0;

          // Konvertiere Target-JID in verschiedene Formate für besseres Matching
          const rawJid = jid.split('@')[0];
          const possibleJids = [
            jid,  // Original
            `${rawJid}@s.whatsapp.net`,  // WhatsApp Format
            `${rawJid}@c.us`,  // Alte WhatsApp Format
            rawJid  // Nur Nummer
          ].filter(Boolean);

          console.log(`[yeetban] Checking possible target JIDs:`, possibleJids);

          // Iteriere durch alle Gruppen
          for (const gid of Object.keys(groups)) {
            try {
              // Hole Gruppen-Metadaten mit Retry-Logik
              const meta = await getGroupMetaSafe(gid);
              if (!meta || !meta.participants) {
                console.log(`[yeetban] Keine Metadaten für Gruppe ${gid}`);
                failed++;
                continue;
              }

              console.log(`[yeetban] Checking group ${meta.subject} (${gid})`);
              
              // Prüfe Bot-Admin-Status
              const botParticipant = meta.participants.find(p => {
                // Normalisiere participant ID
                const pid = (p.id || p.jid || '').split(':')[0];
                const botId = (sock.user?.id || '').split(':')[0];
                return pid === botId;
              });

              if (!botParticipant || !botParticipant.admin) {
                console.log(`[yeetban] Bot ist kein Admin in ${meta.subject}`);
                failed++;
                continue;
              }

              // Finde Ziel in Gruppe (prüfe alle möglichen JID-Formate)
              const targetParticipant = meta.participants.find(p => {
                const pid = (p.id || p.jid || '').split(':')[0];
                return possibleJids.some(testJid => {
                  const testId = testJid.split('@')[0];
                  return pid === testId;
                });
              });

              if (!targetParticipant) {
                console.log(`[yeetban] Ziel nicht in Gruppe ${meta.subject}`);
                failed++;
                continue;
              }

              // Versuche Ziel zu entfernen
              console.log(`[yeetban] Entferne ${targetParticipant.id} aus ${meta.subject}`);
              await sock.groupParticipantsUpdate(gid, [targetParticipant.id], 'remove');
              
              // Warte kurz um Rate-Limits zu vermeiden
              await sleep(500);
              removed++;
            } catch (e) {
              console.error(`[yeetban] Fehler in Gruppe ${gid}:`, e.message || e);
              failed++;
            }
          }

          console.log(`yeetban: ${jid} attempted removals=${removed} failed=${failed}`);
          try { await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🚫 Yeetban: ${jid}\nGrund: ${reason}\nDurch: ${sender}\nRemoved from: ${removed} groups\nFailed to remove from: ${failed} groups` }); } catch (e) {}
          return send(`✅ Yeetban ausgeführt: ${jid} — entfernt aus ${removed} Gruppen, fehlgeschlagen: ${failed}`);
        } catch (e) {
          console.error('yeetban error', e);
          return send('❌ Yeetban fehlgeschlagen (Fehler beim Abrufen der Gruppen).');
        }
      }

      if (cmd === 'kick') {
        console.log('CMD kick called by', sender, 'rank=', ranks[sender], 'args=', args);
  if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN','MOD'])) return send('Kein Zugriff.');
  const t = args[0]; if (!t) return send('Usage: $kick <num|jid>');
  const jid = normalizeJid(t);
  const participantJid = toParticipantJid(jid);
  if (!participantJid) return send('❌ Ungültige Ziel-JID.');
  try { await sock.groupParticipantsUpdate(from, [participantJid], 'remove'); return send(`✅ ${participantJid} entfernt.`); } catch (e) { return send('❌ Kicken fehlgeschlagen.'); }
      }

      if (cmd === 'warn') {
  if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN','MOD'])) return send('Kein Zugriff.');
        const t = args[0]; const reason = args.slice(1).join(' ') || 'Kein Grund';
        if (!t) return send('Usage: $warn <num|jid> <grund>');
        const jid = normalizeJid(t);
        ensureUser(jid);
        users[jid].warns = users[jid].warns || [];
        users[jid].warns.push({ by: sender, reason, at: new Date().toISOString() });
        save(FILES.users, users);
        return send(`⚠ ${jid} verwarned.`);
      }
      if (cmd === 'clearwarns') {
        if (!isAuthorized(sender, ['OWNER','COOWNER','ADMIN','MOD'])) return send('Kein Zugriff.');
        const t = args[0]; if (!t) return send('Usage: $clearwarns <num|jid>');
        const jid = normalizeJid(t);
        if (users[jid]) users[jid].warns = [];
        save(FILES.users, users);
        return send(`✅ Warns entfernt für ${jid}`);
      }

      // PROMOTE / DEMOTE
      if (cmd === 'promote') {
  if (!(isSameJid(sender, OWNER_LID) || isSameJid(sender, OWNER_PRIV) || isSameJid(sender, COOWNER_LID))) return send('Nur Owner/Co-Owner darf promoten.');
        const t = args[0]; if (!t) return send('Usage: $promote <num|jid>');
        const jid = normalizeJid(t);
        ranks[jid] = 'ADMIN'; save(FILES.ranks, ranks);
        return send(`✅ ${jid} zum ADMIN befördert.`);
      }
      if (cmd === 'demote') {
  if (!(isSameJid(sender, OWNER_LID) || isSameJid(sender, OWNER_PRIV) || isSameJid(sender, COOWNER_LID))) return send('Nur Owner/Co-Owner darf demoten.');
        const t = args[0]; if (!t) return send('Usage: $demote <num|jid>');
        const jid = normalizeJid(t);
        ranks[jid] = 'USER'; save(FILES.ranks, ranks);
        return send(`✅ ${jid} demoted.`);
      }

      // SETRANK - owner can set ranks and change owner/coowner pointers
      if (cmd === 'setrank') {
        if (!isOwner) return send('❌ Nur der Inhaber darf diesen Befehl nutzen.');
        const t = args[0]; const r = (args[1] || '').toUpperCase();
        if (!t || !r) return send('Usage: $setrank <num|jid> <OWNER|COOWNER|ADMIN|MOD|VIP|USER>');
        const jid = normalizeJid(t);
        const allowed = ['OWNER','COOWNER','ADMIN','MOD','VIP','USER'];
        if (!allowed.includes(r)) return send('Ungültiger Rang. Erlaubt: OWNER, COOWNER, ADMIN, MOD, VIP, USER');

        // handle special OWNER/COOWNER assignments (ensure single owner/coowner)
        if (r === 'OWNER') {
          // demote any existing owner entries
          for (const k of Object.keys(ranks)) { if (ranks[k] === 'OWNER') ranks[k] = 'USER'; }
          ranks[jid] = 'OWNER';
          OWNER_LID = jid;
          // if the jid looks like a whatsapp jid, update OWNER_PRIV too
          if (jid.endsWith('@s.whatsapp.net')) OWNER_PRIV = jid;
        } else if (r === 'COOWNER') {
          for (const k of Object.keys(ranks)) { if (ranks[k] === 'COOWNER') ranks[k] = 'USER'; }
          ranks[jid] = 'COOWNER';
          COOWNER_LID = jid;
        } else {
          ranks[jid] = r;
        }

        // persist changes immediately
        try {
          save(FILES.ranks, ranks);
          save(FILES.owner, { ownerLid: OWNER_LID, ownerPriv: OWNER_PRIV, coownerLid: COOWNER_LID });
        } catch (e) { console.error('setrank save error', e); }

        try { await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🔧 setrank: ${jid} -> ${r} (durch ${sender})` }); } catch (e) {}
        return send(`✅ Rang von ${jid} gesetzt auf ${r}`);
      }

      // DATADELETE - permanently remove a user's data and ban them
      if (cmd === 'datadelete') {
        if (!isOwner) return send('❌ Nur der Inhaber darf diesen Befehl nutzen.');
        const t = args[0]; if (!t) return send('Usage: $datadelete <num|jid>');
        const jid = normalizeJid(t);

        // remove from users, pets, ranks, join requests
        delete users[jid];
        delete pets[jid];
        delete ranks[jid];
        delete joinreqs[jid];

        // remove tickets created by that user
        for (const id of Object.keys(tickets)) {
          if (tickets[id] && tickets[id].user && isSameJid(tickets[id].user, jid)) delete tickets[id];
        }

        // mark as permanently deleted and ban
        deletedUsers[jid] = { by: sender, at: new Date().toISOString() };
        bans[jid] = { by: sender, at: new Date().toISOString(), reason: 'Account data permanently deleted by owner' };

        // persist
        save(FILES.users, users);
        save(FILES.pets, pets);
        save(FILES.ranks, ranks);
        save(FILES.joinreq, joinreqs);
        save(FILES.tickets, tickets);
        save(FILES.deleted, deletedUsers);
        save(FILES.bans, bans);

        // notify the deleted user (best-effort) and owner
        try { await sock.sendMessage(jid, { text: '🚫 Dein Account wurde vom Inhaber gelöscht und ist gesperrt.' }); } catch (e) {}
        try { await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `🗑️ DataDelete: ${jid} — durchgeführt von ${sender}` }); } catch (e) {}

        return send(`✅ Daten von ${jid} wurden gelöscht und Nutzer gebannt.`);
      }

      // SELF PROMOTE - only if bot is admin
      if (cmd === 'selfpromote' || cmd === 'sp') {
        try {
          if (!from?.endsWith('@g.us')) return send('⚠ $selfpromote funktioniert nur in Gruppen.');
          const meta = await getGroupMetaSafe(from);
          if (!meta) return send('⚠ Gruppenmetadaten nicht verfügbar.');
          const botId = sock.user?.id || sock.user;
          const botPart = (meta.participants || []).find(p => p.id === botId || p.jid === botId);
          const botIsAdmin = botPart && (botPart.admin === 'admin' || botPart.admin === 'superadmin' || botPart.isAdmin);
          if (!botIsAdmin) return send('⚠ Der Bot ist kein Admin — Selfpromote nicht möglich.');
          await sock.groupParticipantsUpdate(from, [sender], 'promote');
          return send('🔰 Selfpromote ausgeführt (sofern Bot Admin ist).');
        } catch (e) {
          console.error('selfpromote error', e);
          return send('❌ Selfpromote fehlgeschlagen.');
        }
      }

      // JOIN / BROADCAST
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
        // Bot leaves the current group. Only Owner/CoOwner can trigger this in a group.
        if (!from?.endsWith('@g.us')) return send('Dieser Befehl funktioniert nur in Gruppen.');
        if (!(isOwner || isCoOwner)) return send('Nur Owner/Co-Owner kann den Bot aus einer Gruppe entfernen.');
        try {
          // notify owner then leave
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `Bot wird die Gruppe verlassen: ${from} (angefordert von ${sender})` });
          await sock.groupLeave(from);
          return; // after leaving we cannot send to the group
        } catch (e) {
          console.error('leave error', e);
          return send('❌ Konnte die Gruppe nicht verlassen.');
        }
      }
      // Broadcast Einstellungen verwalten
      // Gruppenliste für den Inhaber
      if (cmd === 'grouplist' || cmd === 'gl') {
        if (!isOwner) return send('Nur der Inhaber kann die Gruppenliste sehen.');
        try {
          const groups = await sock.groupFetchAllParticipating();
          let list = '📋 *Gruppenliste*\n\n';
          for (const [id, group] of Object.entries(groups)) {
            const memberCount = group.participants?.length || 0;
            list += `*Gruppe:* ${group.subject || 'Unbekannt'}\n`;
            list += `*ID:* ${id}\n`;
            list += `*Mitglieder:* ${memberCount}\n`;
            list += `*Broadcast:* ${broadcastSettings[id] === false ? '❌' : '✅'}\n\n`;
          }
          // Sende die Liste im Privatchat
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { 
            text: list,
            mentions: [] // Vermeidet ungewollte Erwähnungen
          });
          return send('📨 Gruppenliste wurde dir privat zugeschickt.');
        } catch (error) {
          console.error('Grouplist error:', error);
          return send('❌ Fehler beim Abrufen der Gruppenliste.');
        }
      }

      // OWNER ONLY: leave all groups the bot is in (requires confirmation)
      if (cmd === 'leaveall') {
        if (!isOwner) return send('Nur der Inhaber kann diesen Befehl ausführen.');
        try {
          const groups = await sock.groupFetchAllParticipating();
          const gids = Object.keys(groups || {});
          if (!gids.length) return send('Der Bot ist in keinen Gruppen.');

          if (pendingActions.has('leaveall')) {
            return send('Es gibt bereits eine ausstehende LeaveAll-Anfrage. Bitte zuerst $confirm leaveall oder $cancel leaveall.');
          }

          // try to collect invite codes where possible before leaving
          const invites = {};
          for (const gid of gids) {
            try {
              const meta = await getGroupMetaSafe(gid).catch(()=>null);
              // try known keys
              const code = meta?.inviteCode || meta?.invite?.code || meta?.invite_code || meta?.id || null;
              invites[gid] = code || null;
            } catch (e) {
              invites[gid] = null;
            }
            await sleep(20);
          }

          pendingActions.set('leaveall', { initiator: sender, groups: gids, invites, createdAt: Date.now() });
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `⚠️ LeaveAll angefragt von ${sender}. Gruppen: ${gids.length}\nBestätige mit: #confirm leaveall (oder breche ab mit #cancel leaveall)` });
          return send('✅ LeaveAll Anfrage gesendet. Bitte bestätige privat beim Inhaber mit $confirm leaveall.');
        } catch (e) {
          console.error('leaveall error', e);
          return send('❌ Fehler beim Erstellen der LeaveAll-Anfrage.');
        }
      }

      // OWNER ONLY: rejoin groups from stored invite codes (requires confirmation)
      if (cmd === 'rejoinall') {
        if (!isOwner) return send('Nur der Inhaber kann diesen Befehl ausführen.');
        const inviteEntries = Object.entries(groupInvites || {}).filter(([,code]) => code);
        if (!inviteEntries.length) return send('Keine gespeicherten Einladungen vorhanden, um wieder beizutreten.');
        if (pendingActions.has('rejoinall')) return send('Es gibt bereits eine ausstehende RejoinAll-Anfrage. Bitte $confirm rejoinall oder $cancel rejoinall.');
        const invites = Object.fromEntries(inviteEntries);
        pendingActions.set('rejoinall', { initiator: sender, invites, createdAt: Date.now() });
        await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `⚠️ RejoinAll angefragt von ${sender}. Einladungen: ${Object.keys(invites).length}\nBestätige mit: #confirm rejoinall (oder breche ab mit #cancel rejoinall)` });
        return send('✅ RejoinAll Anfrage gesendet. Bitte bestätige privat beim Inhaber mit $confirm rejoinall.');
      }

      // Confirm or cancel pending owner actions: $confirm <action> | $cancel <action>
      if (cmd === 'confirm' || cmd === 'cancel') {
        const action = args[0];
        if (!action) return send('Usage: $confirm <action> oder $cancel <action>');
        const pending = pendingActions.get(action);
        if (!pending) return send(`Keine ausstehende Aktion: ${action}`);
        if (pending.initiator !== sender && !isOwner) return send('Nur der Initiator oder Inhaber kann diese Aktion bestätigen/abbrechen.');

        if (cmd === 'cancel') {
          pendingActions.delete(action);
          return send(`❌ Aktion ${action} abgebrochen.`);
        }

        // Execute confirmed actions
        if (action === 'leaveall') {
          const { groups, invites } = pending;
          let left = 0, failed = 0;
          for (const gid of groups) {
            try {
              // store invite if available
              if (invites && invites[gid]) {
                groupInvites[gid] = invites[gid];
              }
              save(FILES.groupInvites, groupInvites);
              await sock.groupLeave(gid);
              left++;
              await sleep(300);
            } catch (e) {
              console.error('leaveall confirm error for', gid, e);
              failed++;
              await sleep(500);
            }
          }
          pendingActions.delete(action);
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `✅ LeaveAll abgeschlossen. Erfolgreich verlassen: ${left}. Fehler: ${failed}.` });
          return send('✅ LeaveAll ausgeführt. Zusammenfassung an Inhaber gesendet.');
        }

        if (action === 'rejoinall') {
          const { invites } = pending;
          let success = 0, failed = 0;
          for (const [gid, code] of Object.entries(invites || {})) {
            if (!code) { failed++; continue; }
            try {
              // accept invite code. If a full link is given, extract code
              const m = String(code).match(/([A-Za-z0-9_-]{20,})/);
              const invite = m ? m[1] : code;
              await sock.groupAcceptInvite(invite);
              success++;
              await sleep(300);
            } catch (e) {
              console.error('rejoinall error for', gid, e);
              failed++;
              await sleep(500);
            }
          }
          pendingActions.delete(action);
          await sock.sendMessage(normalizeJid(OWNER_PRIV), { text: `✅ RejoinAll abgeschlossen. Erfolgreich beigetreten: ${success}. Fehler: ${failed}.` });
          return send('✅ RejoinAll ausgeführt. Zusammenfassung an Inhaber gesendet.');
        }

        return send('Unbekannte Aktion zum Bestätigen.');
      }

      if (cmd === 'broadcast-settings' || cmd === 'bs') {
        if (!isOwner) return send('Nur der Inhaber darf Broadcast-Einstellungen ändern.');
        const subCmd = args[0]?.toLowerCase();
        const groupId = args[1];

        if (!subCmd || !['enable', 'disable', 'list'].includes(subCmd)) {
          return send('Nutzung:\n#broadcast-settings enable <group-id> - Aktiviert Broadcasts\n#broadcast-settings disable <group-id> - Deaktiviert Broadcasts\n#broadcast-settings list - Zeigt alle Einstellungen');
        }

        if (subCmd === 'list') {
          const groups = await sock.groupFetchAllParticipating();
          let out = '📢 Broadcast Einstellungen:\n';
          for (const [id, group] of Object.entries(groups)) {
            const status = broadcastSettings[id] === false ? '❌ Deaktiviert' : '✅ Aktiviert';
            out += `${group.subject || id}: ${status}\n`;
          }
          return send(out);
        }

        if (!groupId) return send('Bitte gib eine Gruppen-ID an.');
        
        if (subCmd === 'enable') {
          delete broadcastSettings[groupId]; // Standardmäßig aktiviert
          save(FILES.broadcastSettings, broadcastSettings);
          return send(`✅ Broadcasts für Gruppe ${groupId} aktiviert.`);
        }
        
        if (subCmd === 'disable') {
          broadcastSettings[groupId] = false;
          save(FILES.broadcastSettings, broadcastSettings);
          return send(`❌ Broadcasts für Gruppe ${groupId} deaktiviert.`);
        }
      }

      if (cmd === 'broadcast') {
        if (!(isOwner || isCoOwner)) return send('Kein Zugriff.');
        const textMsg = args.join(' ');
        if (!textMsg) return send('Usage: $broadcast <text>');
        const chats = await sock.groupFetchAllParticipating();
        const gids = Object.keys(chats).filter(gid => broadcastSettings[gid] !== false); // Nur an aktivierte Gruppen senden
        send(`📣 Broadcast an ${gids.length} Gruppen...`);
        for (const g of gids) { try { await sock.sendMessage(g, { text: `📣 Broadcast:\n${textMsg}` }); await sleep(300); } catch {} }
        return send('✅ Broadcast abgeschlossen.');
      }

      // STATS / USERINFO / TOP
      if (cmd === 'stats' || cmd === 'profile') {
        const u = users[sender];
        return send(`📊 Profil ${sender}:\nLevel: ${u.level}\nXP: ${u.xp}\nCoins: ${u.coins}\nNachrichten: ${u.msgCount}`);
      }
      if (cmd === 'userinfo') {
        const t = args[0] ? normalizeJid(args[0]) : sender;
        ensureUser(t);
        const u = users[t];
        return send(`👤 ${t}\nLevel:${u.level}\nXP:${u.xp}\nCoins:${u.coins}\nRank:${ranks[t]||u.rank}`);
      }
      if (cmd === 'top') {
        const top = Object.entries(users).sort((a,b)=> (b[1].level*1000 + (b[1].xp||0)) - (a[1].level*1000 + (a[1].xp||0))).slice(0,10);
        let out = '🏆 Top Spieler\n';
        top.forEach(([jid,u],i)=> out += `${i+1}. ${jid.split('@')[0]} - Lv.${u.level} (${u.xp} XP)\n`);
        return send(out);
      }

      // KI-Chat Befehl
      if (cmd === 'api') {
        const message = args.join(' ');
        if (!message) return send('Bitte gib eine Nachricht ein!');
        
        try {
          const response = await fetch('http://localhost:11434/api/generate', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({
              model: currentModel,
              prompt: message,
              stream: false,
              options: {
                temperature: AI_CONFIG.settings.temperature
              }
            })
          });
          
          if (!response.ok) throw new Error('API-Anfrage fehlgeschlagen');
          
          const data = await response.json();
          const answer = data.choices[0].message.content || 'Keine Antwort erhalten';
          return send(answer);
          
        } catch (error) {
          console.error('KI Error:', error);
          return send('Ich versuche gerade, die beste Antwort zu finden. Bitte versuche es in ein paar Sekunden noch einmal. oder die api ist akutell fehler haft bitte melde dich mein inhaber mit deinem gruppen link dann meldert er sich bei ihnen.');
        }
      }


      // unknown command
      return send('❓ Unbekannter Befehl — $help für eine Liste der Befehle.');
    } catch (err) {
      console.error('messages.upsert error:', err);
      log(`ERROR: ${(err && err.message) ? err.message : String(err)}`);
    }
  });

  console.log('RaptorBot gestartet.');
}

startBot();

