/* =====================================================================
   🏰 SAO GILDEN-SYSTEM — Modul
   =====================================================================
   Verwaltet: Gilden erstellen/beitreten/verlassen, Einladungen,
   Gilden-Info, Gilden-Rangliste (nach Gesamt-Level der Mitglieder).
   Speichert in eigener guilds.json: { [guildId]: { name, leader,
   members: [jid...], createdAt } }
   Zusätzlich: users[jid].guildId zeigt auf die Gilde des Spielers.
   ===================================================================== */

export const GUILD_COMMANDS = [
  'gilde', 'guild', 'gildenrang', 'guildrank'
];

export const GUILD_HELP_TEXT =
  `▸ {P}gilde create <name> — Neue Gilde gründen\n` +
  `▸ {P}gilde invite @user — Spieler einladen\n` +
  `▸ {P}gilde accept/deny — Einladung annehmen/ablehnen\n` +
  `▸ {P}gilde leave — Gilde verlassen\n` +
  `▸ {P}gilde kick @user — Mitglied entfernen (nur Anführer)\n` +
  `▸ {P}gilde info [name] — Gildeninfo anzeigen\n` +
  `▸ {P}gilde list — Alle Gilden anzeigen\n` +
  `▸ {P}gildenrang — Gilden-Rangliste (nach Gesamt-Level)\n`;

function slugify(name) {
  return name.trim().toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9\-]/g, '');
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

      const scored = entries.map(([id, g]) => {
        const totalLevel = g.members.reduce((sum, jid) => sum + (users[jid]?.level || 1), 0);
        return { id, ...g, totalLevel };
      }).sort((a, b) => b.totalLevel - a.totalLevel);

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

    // ---------- CREATE ----------
    if (sub === 'create' || sub === 'erstellen') {
      if (users[sender].guildId) { await send('❌ Du bist bereits in einer Gilde. Verlasse sie zuerst mit ' + activePrefix + 'gilde leave.'); return true; }
      const name = args.slice(1).join(' ').trim();
      if (!name || name.length < 3) { await send(`❌ Nutzung: ${activePrefix}gilde create <name> (min. 3 Zeichen)`); return true; }

      const guildId = slugify(name);
      if (!guildId) { await send('❌ Ungültiger Gildenname.'); return true; }
      if (guilds[guildId]) { await send('❌ Eine Gilde mit diesem Namen existiert bereits.'); return true; }

      guilds[guildId] = { name, leader: sender, members: [sender], createdAt: Date.now() };
      users[sender].guildId = guildId;
      save(FILES.guilds, guilds);
      save(FILES.users, users);

      await send(`🏰 Gilde *${name}* wurde gegründet! Du bist der Anführer.\nLade Mitglieder ein mit ${activePrefix}gilde invite @user`);
      return true;
    }

    // ---------- INVITE ----------
    if (sub === 'invite' || sub === 'einladen') {
      const guildId = users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }
      if (guilds[guildId].leader !== sender) { await send('❌ Nur der Gildenanführer darf einladen.'); return true; }

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
        `📨 @${targetJid.split('@')[0]} wurde in die Gilde *${guilds[guildId].name}* eingeladen!\n` +
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

      guild.members.push(sender);
      users[sender].guildId = invite.guildId;
      pendingInvites.delete(sender);
      save(FILES.guilds, guilds);
      save(FILES.users, users);

      await send(`✅ Du bist der Gilde *${guild.name}* beigetreten! 🏰`);
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

      guild.members = guild.members.filter(j => j !== sender);
      delete users[sender].guildId;

      if (guild.leader === sender) {
        if (guild.members.length > 0) {
          guild.leader = guild.members[0];
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
      if (guild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Mitglieder entfernen.'); return true; }

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = ctx2?.mentionedJid?.length ? ctx2.mentionedJid[0] : args[1];
      if (!target) { await send(`❌ Nutzung: ${activePrefix}gilde kick @user`); return true; }
      const targetJid = normalizeJid(target);

      if (isSameJid(targetJid, sender)) { await send('❌ Nutze stattdessen ' + activePrefix + 'gilde leave.'); return true; }
      if (!guild.members.includes(targetJid)) { await send('❌ Diese Person ist nicht in deiner Gilde.'); return true; }

      guild.members = guild.members.filter(j => j !== targetJid);
      if (users[targetJid]) delete users[targetJid].guildId;
      save(FILES.guilds, guilds);
      save(FILES.users, users);

      await send(`✅ @${targetJid.split('@')[0]} wurde aus der Gilde entfernt.`, { mentions: [targetJid] });
      return true;
    }

    // ---------- INFO ----------
    if (sub === 'info' || !sub) {
      const nameArg = args.slice(1).join(' ').trim();
      const guildId = nameArg ? slugify(nameArg) : users[sender].guildId;
      if (!guildId || !guilds[guildId]) { await send('❌ Gilde nicht gefunden. Du bist evtl. in keiner Gilde — nutze ' + activePrefix + 'gilde info <name>.'); return true; }
      const guild = guilds[guildId];

      const memberLines = await Promise.all(guild.members.map(async jid => {
        const u = users[jid] || {};
        const isLeader = jid === guild.leader;
        const name = u.name || u.registrationName || await getNumberMention(jid, sock);
        return `${isLeader ? '👑' : '•'} ${name} — Lv.${u.level || 1}`;
      }));

      await send(
        `🏰 *— GILDE: ${guild.name} —* 🏰\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `👥 Mitglieder (${guild.members.length}):\n${memberLines.join('\n')}\n` +
        `📅 Gegründet: ${new Date(guild.createdAt).toLocaleDateString('de-DE')}`,
        { mentions: guild.members }
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

    await send(`❌ Nutzung:\n${activePrefix}gilde create <name>\n${activePrefix}gilde invite @user\n${activePrefix}gilde accept / deny\n${activePrefix}gilde leave\n${activePrefix}gilde kick @user\n${activePrefix}gilde info [name]\n${activePrefix}gilde list`);
    return true;
  }

  return { handle };
}