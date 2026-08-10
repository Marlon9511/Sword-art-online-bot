const fs = require('fs');
const path = require('path');

const DATA_PATH = path.join(__dirname, 'data', 'bitchkick.json');

// ---------- Storage ----------
function loadData() {
  try {
    if (!fs.existsSync(DATA_PATH)) {
      fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
      fs.writeFileSync(DATA_PATH, JSON.stringify({}, null, 2));
    }
    return JSON.parse(fs.readFileSync(DATA_PATH, 'utf-8'));
  } catch (e) {
    console.error('❌ Bitchkick: Fehler beim Laden:', e.message);
    return {};
  }
}

function saveData(data) {
  try {
    fs.writeFileSync(DATA_PATH, JSON.stringify(data, null, 2));
  } catch (e) {
    console.error('❌ Bitchkick: Fehler beim Speichern:', e.message);
  }
}

// ---------- Helpers ----------
function normalizeNumber(input) {
  const num = String(input).replace(/[^0-9]/g, '');
  if (!num) return null;
  return `${num}@s.whatsapp.net`;
}

function getGroupList(groupJid) {
  const data = loadData();
  return data[groupJid] || [];
}

function addToGroupList(groupJid, jid) {
  const data = loadData();
  if (!data[groupJid]) data[groupJid] = [];
  if (data[groupJid].includes(jid)) return false;
  data[groupJid].push(jid);
  saveData(data);
  return true;
}

function removeFromGroupList(groupJid, jid) {
  const data = loadData();
  if (!data[groupJid] || !data[groupJid].includes(jid)) return false;
  data[groupJid] = data[groupJid].filter(j => j !== jid);
  saveData(data);
  return true;
}

function clearGroupList(groupJid) {
  const data = loadData();
  if (!data[groupJid]) return 0;
  const count = data[groupJid].length;
  data[groupJid] = [];
  saveData(data);
  return count;
}

async function isBotAdmin(sock, groupJid) {
  try {
    const metadata = await sock.groupMetadata(groupJid);
    const botNumber = sock.user.id.split(':')[0];
    const botParticipant = metadata.participants.find(
      p => p.id.split(':')[0].split('@')[0] === botNumber
    );
    return botParticipant && (botParticipant.admin === 'admin' || botParticipant.admin === 'superadmin');
  } catch (e) {
    return false;
  }
}

// ---------- Command Handler ----------
async function handleBitchkickCommand({ cmd, args, sender, send, activePrefix, isAuthorized, m, sock }) {
  if (cmd !== 'bitchkick') return false;

  if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
    await send('❌ Kein Zugriff.');
    return true;
  }

  const groupJid = m.key.remoteJid;
  if (!groupJid?.endsWith('@g.us')) {
    await send('❌ Dieser Befehl funktioniert nur innerhalb einer Gruppe.');
    return true;
  }

  const sub = args[0]?.toLowerCase();

  if (sub === 'add') {
    const raw = args[1];
    if (!raw) {
      await send(`❌ Nutzung: ${activePrefix}bitchkick add <nummer>`);
      return true;
    }
    const jid = normalizeNumber(raw);
    if (!jid) {
      await send('❌ Ungültige Nummer.');
      return true;
    }
    const added = addToGroupList(groupJid, jid);
    await send(
      added
        ? `✅ ${raw} wurde zur Kick-Liste *dieser Gruppe* hinzugefügt.\nWird beim Beitritt sofort entfernt.`
        : `ℹ️ ${raw} steht bereits auf der Liste.`
    );
    return true;
  }

  if (sub === 'remove' || sub === 'del') {
    const raw = args[1];
    if (!raw) {
      await send(`❌ Nutzung: ${activePrefix}bitchkick remove <nummer>`);
      return true;
    }
    const jid = normalizeNumber(raw);
    if (!jid) {
      await send('❌ Ungültige Nummer.');
      return true;
    }
    const removed = removeFromGroupList(groupJid, jid);
    await send(removed ? `✅ ${raw} wurde von der Liste entfernt.` : `ℹ️ ${raw} stand nicht auf der Liste.`);
    return true;
  }

  if (sub === 'list') {
    const list = getGroupList(groupJid);
    if (!list.length) {
      await send('ℹ️ Die Kick-Liste dieser Gruppe ist leer.');
      return true;
    }
    const formatted = list.map((jid, i) => `${i + 1}. ${jid.split('@')[0]}`).join('\n');
    await send(`📋 *Bitchkick-Liste (diese Gruppe)* (${list.length}):\n${formatted}`);
    return true;
  }

  if (sub === 'clear') {
    const count = clearGroupList(groupJid);
    await send(count ? `✅ ${count} Einträge aus der Liste gelöscht.` : 'ℹ️ Liste war bereits leer.');
    return true;
  }

  if (sub === 'status') {
    const botIsAdmin = await isBotAdmin(sock, groupJid);
    await send(
      botIsAdmin
        ? '✅ Bot ist Admin – Bitchkick funktioniert in dieser Gruppe.'
        : '⚠️ Bot ist KEIN Admin – Bitchkick kann hier niemanden entfernen! Bitte Bot zum Admin machen.'
    );
    return true;
  }

  await send(
    `❌ Nutzung:\n` +
    `${activePrefix}bitchkick add <nummer>\n` +
    `${activePrefix}bitchkick remove <nummer>\n` +
    `${activePrefix}bitchkick list\n` +
    `${activePrefix}bitchkick clear\n` +
    `${activePrefix}bitchkick status`
  );
  return true;
}

// ---------- Join-Event Handler ----------
function registerBitchkickListener(sock) {
  sock.ev.on('group-participants-update', async (update) => {
    const { id: groupJid, participants, action } = update;
    if (action !== 'add') return;

    const list = getGroupList(groupJid);
    if (!list.length) return;

    const botIsAdmin = await isBotAdmin(sock, groupJid);
    if (!botIsAdmin) {
      console.log(`⚠️ Bitchkick: Bot ist kein Admin in ${groupJid}, Kick übersprungen.`);
      return;
    }

    for (const jid of participants) {
      if (list.includes(jid)) {
        try {
          await sock.groupParticipantsUpdate(groupJid, [jid], 'remove');
          console.log(`✅ Bitchkick: ${jid} aus ${groupJid} entfernt.`);
          await new Promise(r => setTimeout(r, 500));
        } catch (e) {
          console.log(`❌ Bitchkick-Fehler für ${jid}:`, e.message);
        }
      }
    }
  });
}

module.exports = {
  handleBitchkickCommand,
  registerBitchkickListener,
  normalizeNumber
};