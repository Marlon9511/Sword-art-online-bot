/* =====================================================================
   🏰 SAO GILDEN-SYSTEM — Modul
   =====================================================================
   Verwaltet: Gilden erstellen/beitreten/verlassen, Einladungen,
   Gilden-Info, Gilden-Rangliste (nach gewichtetem Gesamt-Level),
   sowie interne Ränge mit Rechten (Anführer, Vize, Offizier, Veteran,
   Mitglied, Rekrut).
   Speichert in eigener guilds.json: { [guildId]: { name, leader,
   members: [{ jid, rank, joinedAt }], createdAt } }
   Zusätzlich: users[jid].guildId zeigt auf die Gilde des Spielers.
   ===================================================================== */

export const GUILD_COMMANDS = [
  'gilde', 'guild', 'gildenrang', 'guildrank'
];

export const GUILD_HELP_TEXT =
  `▸ {P}gilde create <name> — Neue Gilde gründen\n` +
  `▸ {P}gilde invite @user — Spieler einladen (Anführer/Vize/Offizier)\n` +
  `▸ {P}gilde accept/deny — Einladung annehmen/ablehnen\n` +
  `▸ {P}gilde leave — Gilde verlassen\n` +
  `▸ {P}gilde kick @user — Mitglied entfernen (Anführer/Vize)\n` +
  `▸ {P}gilde promote @user — Befördern (Anführer/Vize)\n` +
  `▸ {P}gilde demote @user — Degradieren (Anführer/Vize)\n` +
  `▸ {P}gilde transfer @user — Anführerschaft übergeben (nur Anführer)\n` +
  `▸ {P}gilde ränge — Ränge & ihre Vorteile anzeigen\n` +
  `▸ {P}gilde info [name] — Gildeninfo anzeigen\n` +
  `▸ {P}gilde list — Alle Gilden anzeigen\n` +
  `▸ {P}gildenrang — Gilden-Rangliste (nach gewichtetem Gesamt-Level)\n`;

// ---------------------------------------------------------------------
// RANG-DEFINITIONEN
// ---------------------------------------------------------------------
// Reihenfolge = Hierarchie, Index 0 ist der höchste Rang.
export const RANK_ORDER = ['leader', 'vice', 'officer', 'veteran', 'member', 'recruit'];

export const RANKS = {
  leader:   { name: 'Anführer',      emoji: '👑', weight: 1.0, canInvite: true,  canKick: 'all',   canPromote: 'toVice',    desc: 'Voller Zugriff: Einladen, Entfernen, Befördern/Degradieren bis Vize, Anführerschaft übergeben. Nur der Gilden-Ersteller (oder wer sie übertragen bekommt).' },
  vice:     { name: 'Vize-Anführer', emoji: '⚔️', weight: 1.0, canInvite: true,  canKick: 'below', canPromote: 'toOfficer', desc: 'Darf einladen, Mitglieder unterhalb seines Rangs entfernen sowie bis zum Offizier befördern/degradieren.' },
  officer:  { name: 'Offizier',      emoji: '🛡️', weight: 1.0, canInvite: true,  canKick: false,   canPromote: false,       desc: 'Darf neue Spieler in die Gilde einladen. Kein Kick- oder Beförderungsrecht.' },
  veteran:  { name: 'Veteran',       emoji: '⭐', weight: 1.2, canInvite: false, canKick: false,   canPromote: false,       desc: 'Keine Sonderrechte, zählt aber mit 1.2x-Gewichtung fürs Gilden-Gesamt-Level in der Gildenrangliste — Belohnung für treue, starke Mitglieder.' },
  member:   { name: 'Mitglied',      emoji: '🔰', weight: 1.0, canInvite: false, canKick: false,   canPromote: false,       desc: 'Standardrang. Normale Gewichtung (1.0x) fürs Gilden-Gesamt-Level.' },
  recruit:  { name: 'Rekrut',        emoji: '🌱', weight: 0.8, canInvite: false, canKick: false,   canPromote: false,       desc: 'Startrang direkt nach Beitritt. Zählt nur mit 0.8x-Gewichtung fürs Gilden-Gesamt-Level, bis er sich beweist. Wird nach 3 Tagen automatisch zu Mitglied befördert.' },
};

const RECRUIT_PROMOTION_MS = 3 * 24 * 60 * 60 * 1000; // 3 Tage

function slugify(name) {
  return name.trim().toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9\-]/g, '');
}

function rankIdx(rank) {
  const i = RANK_ORDER.indexOf(rank);
  return i === -1 ? RANK_ORDER.length - 1 : i;
}

function findMember(guild, jid) {
  return guild.members.find(mm => mm.jid === jid);
}

// Rekruten, die seit 3+ Tagen dabei sind, automatisch zu Mitglied befördern.
// Gibt true zurück, wenn etwas verändert wurde (dann sollte gespeichert werden).
function autoPromoteRecruits(guild) {
  let changed = false;
  for (const mm of guild.members) {
    if (mm.rank === 'recruit' && Date.now() - (mm.joinedAt || 0) >= RECRUIT_PROMOTION_MS) {
      mm.rank = 'member';
      changed = true;
    }
  }
  return changed;
}

function weightedGuildLevel(guild, users) {
  return guild.members.reduce((sum, mm) => {
    const lvl = users[mm.jid]?.level || 1;
    const weight = RANKS[mm.rank]?.weight ?? 1.0;
    return sum + lvl * weight;
  }, 0);
}

function rankOverviewText(activePrefix) {
  const lines = RANK_ORDER.map(key => {
    const r = RANKS[key];
    return `${r.emoji} *${r.name}*\n   ${r.desc}`;
  });
  return (
    `🎖️ *— GILDEN-RÄNGE & VORTEILE —* 🎖️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    lines.join('\n\n') +
    `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `Nutze ${activePrefix}gilde promote/demote @user, um Ränge zu vergeben.`
  );
}

export function createGuildSystem() {
  const pendingInvites = new Map(); // targetJid -> { guildId, from, at }

  async function handle(ctx) {
    const {
      cmd, args, sender, send, sock,
      users, guilds, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, activePrefix, m
    } = ctx;

    if (cmd === 'gildenrang' || cmd === 'guildrank') {
      const entries = Object.entries(guilds);
      if (!entries.length) { await send('🏰 Es existieren noch keine Gilden.'); return true; }

      let anyChanged = false;
      const scored = entries.map(([id, g]) => {
        if (autoPromoteRecruits(g)) anyChanged = true;
        const totalLevel = Math.round(weightedGuildLevel(g, users));
        return { id, ...g, totalLevel };
      }).sort((a, b) => b.totalLevel - a.totalLevel);

      if (anyChanged) save(FILES.guilds, guilds);

      const medals = ['🥇', '🥈', '🥉'];
      const lines = scored.slice(0, 10).map((g, i) =>
        `${medals[i] || `${i + 1}.`} *${g.name}* — ⭐ ${g.totalLevel} Gesamt-Level | 👥 ${g.members.length} Mitglieder`
      );
      await send(`🏰 *— GILDEN-RANGLISTE —* 🏰\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`);
      return true;
    }

    if (cmd !== 'gilde' && cmd !== 'guild') return false;

    const sub = (args[0] || '').toLowerCase();
    ensureUser(sender);

    // Rekruten-Auto-Beförderung für die eigene Gilde vor jeder Aktion prüfen
    const currentGuildId = users[sender].guildId;
    if (currentGuildId && guilds[currentGuildId]) {
      if (autoPromoteRecruits(guilds[currentGuildId])) save(FILES.guilds, guilds);
    }

    // ---------- RÄNGE / VORTEILE ----------
    if (sub === 'ränge' || sub === 'raenge' || sub === 'ranks' || sub === 'rank' || sub === 'vorteile') {
      await send(rankOverviewText(activePrefix));
      return true;
    }

    // ---------- CREATE ----------
    if (sub === 'create' || sub === 'erstellen') {
      if (users[sender].guildId) { await send('❌ Du bist bereits in einer Gilde. Verlasse sie zuerst mit ' + activePrefix + 'gilde leave.'); return true; }
      const name = args.slice(1).join(' ').trim();
      if (!name || name.length < 3) { await send(`❌ Nutzung: ${activePrefix}gilde create <name> (min. 3 Zeichen)`); return true; }

      const guildId = slugify(name);
      if (!guildId) { await send('❌ Ungültiger Gildenname.'); return true; }
      if (guilds[guildId]) { await send('❌ Eine Gilde mit diesem Namen existiert bereits.'); return true; }

      guilds[guildId] = {
        name,
        leader: sender,
        members: [{ jid: sender, rank: 'leader', joinedAt: Date.now() }],
        createdAt: Date.now()
      };
      users[sender].guildId = guildId;
      save(FILES.guilds, guilds);
      save(FILES.users, users);

      await send(`🏰 Gilde *${name}* wurde gegründet! Du bist der Anführer 👑.\nLade Mitglieder ein mit ${activePrefix}gilde invite @user`);
      return true;
    }

    // ---------- INVITE ----------
    if (sub === 'invite' || sub === 'einladen') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      const guild = guilds[guildId];
      const actor = findMember(guild, sender);
      if (!actor || !RANKS[actor.rank]?.canInvite) { await send('❌ Nur Anführer, Vize-Anführer oder Offiziere dürfen einladen.'); return true; }

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = ctx2?.mentionedJid?.length ? ctx2.mentionedJid[0] : args[1];
      if (!target && ctx2?.participant) target = ctx2.participant;
      if (!target) { await send(`❌ Nutzung: ${activePrefix}gilde invite @user`); return true; }

      const targetJid = normalizeJid(target);
      ensureUser(targetJid);
      if (isSameJid(sender, targetJid)) { await send('❌ Du kannst dich nicht selbst einladen.'); return true; }
      if (users[targetJid].guildId) { await send('❌ Diese Person ist bereits in einer Gilde.'); return true; }

      pendingInvites.set(targetJid, { guildId, from: sender, at: Date.now() });

      await send(
        `📨 @${targetJid.split('@')[0]} wurde in die Gilde *${guild.name}* eingeladen!\n` +
        `Antworte mit ${activePrefix}gilde accept oder ${activePrefix}gilde deny`,
        { mentions: [targetJid] }
      );
      return true;
    }

    // ---------- ACCEPT ----------
    if (sub === 'accept' || sub === 'annehmen') {
      const invite = pendingInvites.get(sender);
      if (!invite) { await send('❌ Du hast keine offene Gildeneinladung.'); return true; }
      if (users[sender].guildId) { pendingInvites.delete(sender); await send('❌ Du bist bereits in einer Gilde.'); return true; }
      const guild = guilds[invite.guildId];
      if (!guild) { pendingInvites.delete(sender); await send('❌ Diese Gilde existiert nicht mehr.'); return true; }

      guild.members.push({ jid: sender, rank: 'recruit', joinedAt: Date.now() });
      users[sender].guildId = invite.guildId;
      pendingInvites.delete(sender);
      save(FILES.guilds, guilds);
      save(FILES.users, users);

      await send(`✅ Du bist der Gilde *${guild.name}* als 🌱 Rekrut beigetreten! Nach 3 Tagen wirst du automatisch zum Mitglied befördert.`);
      return true;
    }

    // ---------- DENY ----------
    if (sub === 'deny' || sub === 'ablehnen') {
      const invite = pendingInvites.get(sender);
      if (!invite) { await send('❌ Du hast keine offene Gildeneinladung.'); return true; }
      pendingInvites.delete(sender);
      await send('❌ Einladung abgelehnt.');
      return true;
    }

    // ---------- LEAVE ----------
    if (sub === 'leave' || sub === 'verlassen') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      const guild = guilds[guildId];

      guild.members = guild.members.filter(mm => mm.jid !== sender);
      delete users[sender].guildId;

      if (guild.leader === sender) {
        if (guild.members.length > 0) {
          // Nachfolge: Vize zuerst, sonst Offizier, sonst dienstältestes Mitglied
          guild.members.sort((a, b) => rankIdx(a.rank) - rankIdx(b.rank) || a.joinedAt - b.joinedAt);
          const successor = guild.members[0];
          successor.rank = 'leader';
          guild.leader = successor.jid;
        } else {
          delete guilds[guildId];
        }
      }

      save(FILES.guilds, guilds);
      save(FILES.users, users);
      await send(`✅ Du hast die Gilde *${guild.name}* verlassen.`);
      return true;
    }

    // ---------- KICK ----------
    if (sub === 'kick' || sub === 'entfernen') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      const guild = guilds[guildId];
      const actor = findMember(guild, sender);
      const kickPerm = actor ? RANKS[actor.rank]?.canKick : false;
      if (!kickPerm) { await send('❌ Nur Anführer oder Vize-Anführer dürfen Mitglieder entfernen.'); return true; }

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = ctx2?.mentionedJid?.length ? ctx2.mentionedJid[0] : args[1];
      if (!target) { await send(`❌ Nutzung: ${activePrefix}gilde kick @user`); return true; }
      const targetJid = normalizeJid(target);

      if (isSameJid(targetJid, sender)) { await send('❌ Nutze stattdessen ' + activePrefix + 'gilde leave.'); return true; }
      const targetMember = findMember(guild, targetJid);
      if (!targetMember) { await send('❌ Diese Person ist nicht in deiner Gilde.'); return true; }

      if (kickPerm === 'below' && rankIdx(targetMember.rank) <= rankIdx(actor.rank)) {
        await send('❌ Du kannst nur Mitglieder unterhalb deines eigenen Rangs entfernen.');
        return true;
      }

      guild.members = guild.members.filter(mm => mm.jid !== targetJid);
      if (users[targetJid]) delete users[targetJid].guildId;
      save(FILES.guilds, guilds);
      save(FILES.users, users);

      await send(`✅ @${targetJid.split('@')[0]} wurde aus der Gilde entfernt.`, { mentions: [targetJid] });
      return true;
    }

    // ---------- PROMOTE ----------
    if (sub === 'promote' || sub === 'befördern' || sub === 'befoerdern') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      const guild = guilds[guildId];
      const actor = findMember(guild, sender);
      if (!actor || !RANKS[actor.rank]?.canPromote) { await send('❌ Nur Anführer oder Vize-Anführer dürfen befördern.'); return true; }

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = ctx2?.mentionedJid?.length ? ctx2.mentionedJid[0] : args[1];
      if (!target) { await send(`❌ Nutzung: ${activePrefix}gilde promote @user`); return true; }
      const targetJid = normalizeJid(target);
      if (isSameJid(targetJid, sender)) { await send('❌ Du kannst dich nicht selbst befördern.'); return true; }

      const targetMember = findMember(guild, targetJid);
      if (!targetMember) { await send('❌ Diese Person ist nicht in deiner Gilde.'); return true; }

      const floorIdx = actor.rank === 'leader' ? rankIdx('vice') : rankIdx('officer'); // höchster erreichbarer Rang
      const currentIdx = rankIdx(targetMember.rank);
      if (currentIdx <= floorIdx) { await send(`❌ Du kannst diese Person nicht weiter befördern (dein Limit: ${RANKS[RANK_ORDER[floorIdx]].emoji} ${RANKS[RANK_ORDER[floorIdx]].name}).`); return true; }

      const newIdx = currentIdx - 1;
      targetMember.rank = RANK_ORDER[newIdx];
      save(FILES.guilds, guilds);

      await send(`✅ @${targetJid.split('@')[0]} wurde zu ${RANKS[targetMember.rank].emoji} *${RANKS[targetMember.rank].name}* befördert!`, { mentions: [targetJid] });
      return true;
    }

    // ---------- DEMOTE ----------
    if (sub === 'demote' || sub === 'degradieren') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      const guild = guilds[guildId];
      const actor = findMember(guild, sender);
      if (!actor || !RANKS[actor.rank]?.canPromote) { await send('❌ Nur Anführer oder Vize-Anführer dürfen degradieren.'); return true; }

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = ctx2?.mentionedJid?.length ? ctx2.mentionedJid[0] : args[1];
      if (!target) { await send(`❌ Nutzung: ${activePrefix}gilde demote @user`); return true; }
      const targetJid = normalizeJid(target);
      if (isSameJid(targetJid, sender)) { await send('❌ Du kannst dich nicht selbst degradieren.'); return true; }

      const targetMember = findMember(guild, targetJid);
      if (!targetMember) { await send('❌ Diese Person ist nicht in deiner Gilde.'); return true; }
      if (rankIdx(targetMember.rank) <= rankIdx(actor.rank)) { await send('❌ Du kannst niemanden auf deinem eigenen Rang oder höher degradieren.'); return true; }
      if (targetMember.rank === 'recruit') { await send('❌ Diese Person ist bereits im niedrigsten Rang.'); return true; }

      const newIdx = Math.min(rankIdx(targetMember.rank) + 1, RANK_ORDER.length - 1);
      targetMember.rank = RANK_ORDER[newIdx];
      save(FILES.guilds, guilds);

      await send(`✅ @${targetJid.split('@')[0]} wurde zu ${RANKS[targetMember.rank].emoji} *${RANKS[targetMember.rank].name}* degradiert.`, { mentions: [targetJid] });
      return true;
    }

    // ---------- TRANSFER ----------
    if (sub === 'transfer' || sub === 'übertragen' || sub === 'uebertragen') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      const guild = guilds[guildId];
      if (guild.leader !== sender) { await send('❌ Nur der Anführer darf die Anführerschaft übertragen.'); return true; }

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = ctx2?.mentionedJid?.length ? ctx2.mentionedJid[0] : args[1];
      if (!target) { await send(`❌ Nutzung: ${activePrefix}gilde transfer @user`); return true; }
      const targetJid = normalizeJid(target);
      if (isSameJid(targetJid, sender)) { await send('❌ Du bist bereits der Anführer.'); return true; }

      const targetMember = findMember(guild, targetJid);
      if (!targetMember) { await send('❌ Diese Person ist nicht in deiner Gilde.'); return true; }

      const actorMember = findMember(guild, sender);
      targetMember.rank = 'leader';
      guild.leader = targetJid;
      if (actorMember) actorMember.rank = 'vice';
      save(FILES.guilds, guilds);

      await send(`👑 @${targetJid.split('@')[0]} ist jetzt der neue Anführer der Gilde *${guild.name}*!`, { mentions: [targetJid] });
      return true;
    }

    // ---------- INFO ----------
    if (sub === 'info' || !sub) {
      const nameArg = args.slice(1).join(' ').trim();
      const guildId = nameArg ? slugify(nameArg) : users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Gilde nicht gefunden. Du bist evtl. in keiner Gilde — nutze ' + activePrefix + 'gilde info <name>.'); return true; }
      const guild = guilds[guildId];

      const sortedMembers = [...guild.members].sort((a, b) => rankIdx(a.rank) - rankIdx(b.rank));
      const memberLines = await Promise.all(sortedMembers.map(async mm => {
        const u = users[mm.jid] || {};
        const r = RANKS[mm.rank] || RANKS.member;
        const name = u.name || u.registrationName || await getNumberMention(mm.jid, sock);
        return `${r.emoji} ${name} — *${r.name}* — Lv.${u.level || 1}`;
      }));

      await send(
        `🏰 *— GILDE: ${guild.name} —* 🏰\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `👥 Mitglieder (${guild.members.length}):\n${memberLines.join('\n')}\n` +
        `📅 Gegründet: ${new Date(guild.createdAt).toLocaleDateString('de-DE')}`,
        { mentions: guild.members.map(mm => mm.jid) }
      );
      return true;
    }

    // ---------- LIST ----------
    if (sub === 'list' || sub === 'liste') {
      const all = Object.values(guilds);
      if (!all.length) { await send('🏰 Es existieren noch keine Gilden.'); return true; }
      const lines = all.map(g => `🏰 *${g.name}* — 👥 ${g.members.length} Mitglieder`);
      await send(`🏰 *— ALLE GILDEN —* 🏰\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`);
      return true;
    }

    await send(
      `❌ Nutzung:\n${activePrefix}gilde create <name>\n${activePrefix}gilde invite @user\n` +
      `${activePrefix}gilde accept / deny\n${activePrefix}gilde leave\n${activePrefix}gilde kick @user\n` +
      `${activePrefix}gilde promote @user\n${activePrefix}gilde demote @user\n${activePrefix}gilde transfer @user\n` +
      `${activePrefix}gilde ränge\n${activePrefix}gilde info [name]\n${activePrefix}gilde list`
    );
    return true;
  }

  return { handle };
}
