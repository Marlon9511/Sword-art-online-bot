import fs from 'fs';
import path from 'path';

/* =====================================================================
   ⚔️🏰 SAO GILDENKRIEG-SYSTEM — Modul
   =====================================================================
   Erweitert das bestehende Gilden-System (guild-system.mjs) um:
   - Allianzen zwischen Gilden (gegenseitige Zustimmung nötig)
   - Gildenkriege: Zwei Gilden kämpfen gegeneinander um eine "Festungs-HP",
     Mitglieder greifen mit ihrer ausgerüsteten Waffe an (wie beim
     Clan-Boss-Event), Sieger bekommt Belohnungen + Kriegsstatistik.

   Erwartet an guilds[gid] mindestens: { name, leader, members: [jid...] }
   (exakt wie in eurem guild-system.mjs). Ergänzt bei Bedarf automatisch
   guild.allies (Array von Gilden-IDs) und guild.warStats.

   Speichert den Kriegszustand eigenständig in guildwars.json
   (wie guildboss.json beim Boss-Event) — KEIN neues FILES-Entry nötig.
   Allianzen werden direkt in guilds.json mitgespeichert (guild.allies),
   über euer bestehendes save(FILES.guilds, guilds).

   Integration in bot.js: siehe Hinweise am Ende der Datei.
   ===================================================================== */

// ---------------------------------------------------------------------
// Konstanten
// ---------------------------------------------------------------------
const RARITY_DAMAGE_BONUS = {
  common: 0,
  uncommon: 0.10,
  rare: 0.20,
  epic: 0.35,
  legendary: 0.55
};
const SECRET_WEAPON_BONUS = 0.40;
const SECRET_WEAPON_CRIT_BONUS = 10;
const RARITY_EMOJI = { common: '⚪', uncommon: '🟢', rare: '🔵', epic: '🟣', legendary: '🟡', secret: '⚫' };
const CRIT_CHANCE_BY_RARITY = { common: 5, uncommon: 8, rare: 12, epic: 16, legendary: 22, secret: 30 };

const WAR_BASE_HP = 2000;             // Grund-Festungs-HP jeder Gilde
const WAR_HP_PER_MEMBER = 300;        // + HP pro Mitglied der jeweiligen Gilde
const WAR_DEFAULT_MINUTES = 120;      // Standard-Kriegsdauer
const WAR_ATTACK_COOLDOWN_MS = 3 * 60 * 1000;
const WAR_REWARD_COINS_PER_MEMBER = 250;
const WAR_REWARD_XP_PER_MEMBER = 80;

export const GUILDWAR_COMMANDS = ['allianz', 'alliance', 'krieg', 'war', 'kriegsangriff', 'warattack'];

export const GUILDWAR_HELP_TEXT =
  `🤝 *Allianzen*\n` +
  `▸ {P}allianz propose <gildenname> — Allianz vorschlagen (nur Anführer)\n` +
  `▸ {P}allianz accept / deny — Allianzanfrage beantworten (nur Anführer)\n` +
  `▸ {P}allianz break <gildenname> — Allianz auflösen (nur Anführer)\n` +
  `▸ {P}allianz list — Eigene Verbündete anzeigen\n\n` +
  `⚔️ *Gildenkriege*\n` +
  `▸ {P}krieg declare <gildenname> [minuten] — Krieg erklären (nur Anführer)\n` +
  `▸ {P}krieg accept / deny — Kriegserklärung beantworten (nur Anführer)\n` +
  `▸ {P}krieg status — Aktuellen Krieg deiner Gilde anzeigen\n` +
  `▸ {P}krieg concede — Kapitulieren (nur Anführer, zählt als Niederlage)\n` +
  `▸ {P}kriegsangriff — Die feindliche Festung angreifen (Schaden = deine Waffe!)\n` +
  `_Verbündete Gilden können sich nicht gegenseitig den Krieg erklären._\n`;

function slugify(name) {
  return name.trim().toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9\-]/g, '');
}

function getUserGuildId(users, guilds, jid) {
  if (users[jid]?.guildId && guilds[users[jid].guildId]) return users[jid].guildId;
  for (const [gid, g] of Object.entries(guilds)) {
    if (Array.isArray(g.members) && g.members.includes(jid)) return gid;
  }
  return null;
}

function ensureGuildWarFields(guild) {
  if (!guild) return;
  if (!Array.isArray(guild.allies)) guild.allies = [];
  if (!guild.warStats) guild.warStats = { wins: 0, losses: 0, fought: 0 };
}

function calculateWarHp(guild) {
  const memberCount = Array.isArray(guild?.members) ? guild.members.length : 1;
  return WAR_BASE_HP + memberCount * WAR_HP_PER_MEMBER;
}

function hpBar(hp, maxHp, len = 20) {
  const filled = Math.max(0, Math.min(len, Math.round((hp / maxHp) * len)));
  return '🟥'.repeat(filled) + '⬜'.repeat(len - filled);
}

function formatTimeLeft(ms) {
  if (ms <= 0) return '0m';
  const totalMin = Math.ceil(ms / 60000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

// ===== Schaden basierend auf der ausgerüsteten Waffe (normale Formel,
// KEIN Boss-exklusiver Bonus für Ragnarok/Excalibur — der gilt nur im
// Clan-Boss-Event) =====
function calculateWeaponDamage({ users, ITEM_DB, ensureArenaFields, jid, randInt }) {
  ensureArenaFields(users, jid);
  const weaponId = users[jid].equipped?.weapon;
  const weapon = weaponId ? ITEM_DB[weaponId] : null;

  if (!weapon) {
    return { damage: randInt(5, 15), weaponName: 'bloße Fäuste', weaponEmoji: '👊', isCrit: false };
  }

  const basePower = weapon.power || 10;
  const isSecretWeapon = !!(weapon.secret || weapon.ownerOnly);

  let rarityBonus = RARITY_DAMAGE_BONUS[weapon.rarity] || 0;
  if (isSecretWeapon) rarityBonus += SECRET_WEAPON_BONUS;

  const variance = 0.85 + (randInt(0, 30) / 100);
  let damage = Math.round(basePower * (1 + rarityBonus) * variance);

  let critChance = CRIT_CHANCE_BY_RARITY[weapon.rarity] || 5;
  if (isSecretWeapon) critChance += SECRET_WEAPON_CRIT_BONUS;

  const isCrit = randInt(1, 100) <= critChance;
  if (isCrit) damage = Math.round(damage * 2);

  return {
    damage,
    weaponName: weapon.name,
    weaponEmoji: RARITY_EMOJI[weapon.rarity] || '',
    isCrit
  };
}

// ---------------------------------------------------------------------
// Fabrikfunktion
// ---------------------------------------------------------------------
export function createGuildWarSystem(DATA_PATH) {
  const WAR_FILE = path.join(DATA_PATH, 'guildwars.json');

  const defaultState = { wars: {} };
  let state = loadState();

  // In-Memory-Anfragen (wie pendingInvites/pendingDuels in euren anderen Modulen)
  const pendingAlliances = new Map();      // targetGuildId -> { fromGuildId, at }
  const pendingWarDeclarations = new Map(); // targetGuildId -> { fromGuildId, minutes, at }

  function loadState() {
    try {
      if (!fs.existsSync(WAR_FILE)) {
        fs.writeFileSync(WAR_FILE, JSON.stringify(defaultState, null, 2));
        return { ...defaultState, wars: {} };
      }
      const raw = JSON.parse(fs.readFileSync(WAR_FILE, 'utf8'));
      return { wars: raw.wars || {} };
    } catch (e) {
      console.error('[guildwars] Fehler beim Laden:', e);
      return { ...defaultState, wars: {} };
    }
  }

  function saveState() {
    try {
      fs.writeFileSync(WAR_FILE, JSON.stringify(state, null, 2));
    } catch (e) {
      console.error('[guildwars] Fehler beim Speichern:', e);
    }
  }

  function findActiveWarForGuild(guildId) {
    return Object.entries(state.wars).find(
      ([, w]) => w.status === 'active' && (w.guildA === guildId || w.guildB === guildId)
    );
  }

  function findPendingOutgoingWar(guildId) {
    return Object.entries(state.wars).find(
      ([, w]) => w.status === 'pending' && w.declaredBy === guildId
    );
  }

  async function endWar(warId, { send, sock, users, guilds, save, FILES, getNumberMention }, reason = 'time') {
    const war = state.wars[warId];
    if (!war || war.status !== 'active') return;

    const guildA = guilds[war.guildA];
    const guildB = guilds[war.guildB];
    ensureGuildWarFields(guildA);
    ensureGuildWarFields(guildB);

    const nameA = guildA?.name || war.guildA;
    const nameB = guildB?.name || war.guildB;

    const pctADealt = war.maxHpB > 0 ? (war.maxHpB - war.hpB) / war.maxHpB : 0; // Schaden von A an B
    const pctBDealt = war.maxHpA > 0 ? (war.maxHpA - war.hpA) / war.maxHpA : 0; // Schaden von B an A

    let winnerGid = null, loserGid = null;
    if (war.hpA <= 0 && war.hpB > 0) { winnerGid = war.guildA; loserGid = war.guildB; }
    else if (war.hpB <= 0 && war.hpA > 0) { winnerGid = war.guildB; loserGid = war.guildA; }
    else if (reason === 'concede') {
      winnerGid = war.concedeLoser === war.guildA ? war.guildB : war.guildA;
      loserGid = war.concedeLoser;
    } else if (pctADealt > pctBDealt) { winnerGid = war.guildA; loserGid = war.guildB; }
    else if (pctBDealt > pctADealt) { winnerGid = war.guildB; loserGid = war.guildA; }
    // sonst: Unentschieden (winnerGid bleibt null)

    const mentions = [];
    let rewardLine = '';

    if (winnerGid) {
      const winnerGuild = guilds[winnerGid];
      const loserGuild = guilds[loserGid];
      ensureGuildWarFields(winnerGuild);
      ensureGuildWarFields(loserGuild);

      winnerGuild.warStats.wins += 1;
      winnerGuild.warStats.fought += 1;
      if (loserGuild) { loserGuild.warStats.losses += 1; loserGuild.warStats.fought += 1; }

      const members = Array.isArray(winnerGuild?.members) ? winnerGuild.members : [];
      for (const jid of members) {
        if (!users[jid]) continue;
        users[jid].coins = (users[jid].coins || 0) + WAR_REWARD_COINS_PER_MEMBER;
        users[jid].xp = (users[jid].xp || 0) + WAR_REWARD_XP_PER_MEMBER;
        mentions.push(jid);
      }

      rewardLine = `\n🏆 Sieger: *${winnerGuild?.name || winnerGid}*\n🎁 Belohnung pro Mitglied: +${WAR_REWARD_COINS_PER_MEMBER} Coins, +${WAR_REWARD_XP_PER_MEMBER} XP`;

      save(FILES.users, users);
    } else {
      if (guildA) guildA.warStats.fought += 1;
      if (guildB) guildB.warStats.fought += 1;
      rewardLine = `\n🤝 Unentschieden — keine Gilde erhält eine Belohnung.`;
    }

    save(FILES.guilds, guilds);

    const reasonText =
      reason === 'dead' ? '💀 Die Festung einer Gilde wurde zerstört!' :
      reason === 'concede' ? '🏳️ Eine Gilde hat kapituliert!' :
      '⏳ Die Kriegszeit ist abgelaufen.';

    const summary =
      `⚔️🏰 *— GILDENKRIEG BEENDET —* 🏰⚔️\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
      `${reasonText}\n\n` +
      `*${nameA}* vs *${nameB}*\n` +
      `💥 ${nameA} → ${nameB}: ${Math.round(pctADealt * 100)}% Festungsschaden\n` +
      `💥 ${nameB} → ${nameA}: ${Math.round(pctBDealt * 100)}% Festungsschaden` +
      rewardLine +
      `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`;

    try {
      if (war.originChat) await sock.sendMessage(war.originChat, { text: summary, mentions });
      else await send(summary, { mentions });
    } catch (e) {
      await send(summary, { mentions });
    }

    delete state.wars[warId];
    saveState();
  }

  async function checkExpiry(ctx) {
    const now = Date.now();
    for (const [warId, war] of Object.entries(state.wars)) {
      if (war.status === 'active' && war.endsAt && now >= war.endsAt) {
        await endWar(warId, ctx, 'time');
      }
    }
  }

  async function handle(ctx) {
    const {
      cmd, args, sender, from, activePrefix, send, sock,
      users, guilds, save, FILES, ensureUser, normalizeJid,
      getNumberMention, randInt, ITEM_DB, ensureArenaFields
    } = ctx;

    ensureUser(sender);

    // =====================================================================
    // ALLIANZEN
    // =====================================================================
    if (cmd === 'allianz' || cmd === 'alliance') {
      const sub = (args[0] || '').toLowerCase();
      const myGuildId = getUserGuildId(users, guilds, sender);

      if (!myGuildId || !guilds[myGuildId]) {
        await send('❌ Du bist in keiner Gilde.');
        return true;
      }
      const myGuild = guilds[myGuildId];
      ensureGuildWarFields(myGuild);

      if (sub === 'propose' || sub === 'vorschlagen') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Allianzen vorschlagen.'); return true; }

        const nameArg = args.slice(1).join(' ').trim();
        const targetGuildId = slugify(nameArg);
        if (!nameArg || !guilds[targetGuildId]) { await send(`❌ Nutzung: ${activePrefix}allianz propose <gildenname>`); return true; }
        if (targetGuildId === myGuildId) { await send('❌ Du kannst keine Allianz mit deiner eigenen Gilde bilden.'); return true; }

        const targetGuild = guilds[targetGuildId];
        ensureGuildWarFields(targetGuild);

        if (myGuild.allies.includes(targetGuildId)) { await send('❌ Ihr seid bereits verbündet.'); return true; }

        pendingAlliances.set(targetGuildId, { fromGuildId: myGuildId, at: Date.now() });

        const leaderMention = await getNumberMention(targetGuild.leader, sock);
        await send(
          `🤝 Die Gilde *${myGuild.name}* schlägt der Gilde *${targetGuild.name}* eine Allianz vor!\n` +
          `${leaderMention}, antworte mit ${activePrefix}allianz accept oder ${activePrefix}allianz deny`,
          { mentions: [targetGuild.leader] }
        );
        return true;
      }

      if (sub === 'accept' || sub === 'annehmen') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Allianzanfragen beantworten.'); return true; }
        const proposal = pendingAlliances.get(myGuildId);
        if (!proposal) { await send('❌ Es liegt keine offene Allianzanfrage für deine Gilde vor.'); return true; }

        const fromGuild = guilds[proposal.fromGuildId];
        if (!fromGuild) { pendingAlliances.delete(myGuildId); await send('❌ Diese Gilde existiert nicht mehr.'); return true; }
        ensureGuildWarFields(fromGuild);

        if (!myGuild.allies.includes(proposal.fromGuildId)) myGuild.allies.push(proposal.fromGuildId);
        if (!fromGuild.allies.includes(myGuildId)) fromGuild.allies.push(myGuildId);
        pendingAlliances.delete(myGuildId);
        save(FILES.guilds, guilds);

        await send(`✅🤝 *${myGuild.name}* und *${fromGuild.name}* sind jetzt verbündet!`);
        return true;
      }

      if (sub === 'deny' || sub === 'ablehnen') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Allianzanfragen beantworten.'); return true; }
        const proposal = pendingAlliances.get(myGuildId);
        if (!proposal) { await send('❌ Es liegt keine offene Allianzanfrage für deine Gilde vor.'); return true; }
        pendingAlliances.delete(myGuildId);
        await send('❌ Allianzanfrage abgelehnt.');
        return true;
      }

      if (sub === 'break' || sub === 'auflösen' || sub === 'aufloesen') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Allianzen auflösen.'); return true; }
        const nameArg = args.slice(1).join(' ').trim();
        const targetGuildId = slugify(nameArg);
        if (!nameArg || !guilds[targetGuildId]) { await send(`❌ Nutzung: ${activePrefix}allianz break <gildenname>`); return true; }

        if (!myGuild.allies.includes(targetGuildId)) { await send('❌ Ihr seid nicht verbündet.'); return true; }

        const targetGuild = guilds[targetGuildId];
        ensureGuildWarFields(targetGuild);
        myGuild.allies = myGuild.allies.filter(g => g !== targetGuildId);
        if (targetGuild) targetGuild.allies = targetGuild.allies.filter(g => g !== myGuildId);
        save(FILES.guilds, guilds);

        await send(`💔 Die Allianz zwischen *${myGuild.name}* und *${targetGuild?.name || nameArg}* wurde aufgelöst.`);
        return true;
      }

      if (sub === 'list' || sub === 'liste' || !sub) {
        if (!myGuild.allies.length) { await send(`🤝 *${myGuild.name}* hat aktuell keine Verbündeten.`); return true; }
        const lines = myGuild.allies.map(gid => `• ${guilds[gid]?.name || gid}`);
        await send(`🤝 *— VERBÜNDETE VON ${myGuild.name} —* 🤝\n${lines.join('\n')}`);
        return true;
      }

      await send(`❌ Nutzung: ${activePrefix}allianz propose/accept/deny/break <gildenname>/list`);
      return true;
    }

    // =====================================================================
    // GILDENKRIEGE
    // =====================================================================
    if (cmd === 'krieg' || cmd === 'war') {
      const sub = (args[0] || '').toLowerCase();
      const myGuildId = getUserGuildId(users, guilds, sender);

      if (!myGuildId || !guilds[myGuildId]) {
        await send('❌ Du bist in keiner Gilde.');
        return true;
      }
      const myGuild = guilds[myGuildId];
      ensureGuildWarFields(myGuild);

      if (sub === 'declare' || sub === 'erklären' || sub === 'erklaeren') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf einen Krieg erklären.'); return true; }
        if (findActiveWarForGuild(myGuildId)) { await send('❌ Deine Gilde befindet sich bereits in einem aktiven Krieg.'); return true; }
        if (findPendingOutgoingWar(myGuildId)) { await send('❌ Deine Gilde hat bereits eine offene Kriegserklärung.'); return true; }

        // Letztes Argument könnte die Minutenzahl sein
        let minutesArg = null;
        let nameArgs = args.slice(1);
        const last = nameArgs[nameArgs.length - 1];
        if (last && /^\d+$/.test(last)) {
          minutesArg = parseInt(last);
          nameArgs = nameArgs.slice(0, -1);
        }
        const nameArg = nameArgs.join(' ').trim();
        const targetGuildId = slugify(nameArg);

        if (!nameArg || !guilds[targetGuildId]) {
          await send(`❌ Nutzung: ${activePrefix}krieg declare <gildenname> [minuten]`);
          return true;
        }
        if (targetGuildId === myGuildId) { await send('❌ Du kannst deiner eigenen Gilde nicht den Krieg erklären.'); return true; }

        const targetGuild = guilds[targetGuildId];
        ensureGuildWarFields(targetGuild);

        if (myGuild.allies.includes(targetGuildId)) {
          await send('❌ Ihr seid verbündet! Löst zuerst die Allianz mit ' + activePrefix + 'allianz break auf.');
          return true;
        }
        if (findActiveWarForGuild(targetGuildId)) { await send('❌ Diese Gilde befindet sich bereits in einem aktiven Krieg.'); return true; }

        const minutes = minutesArg && minutesArg >= 5 ? minutesArg : WAR_DEFAULT_MINUTES;
        const warId = `${myGuildId}__${targetGuildId}__${Date.now()}`;

        pendingWarDeclarations.set(targetGuildId, { fromGuildId: myGuildId, minutes, warId, at: Date.now() });

        const leaderMention = await getNumberMention(targetGuild.leader, sock);
        await send(
          `⚔️🏰 Die Gilde *${myGuild.name}* erklärt der Gilde *${targetGuild.name}* den Krieg!\n` +
          `⏳ Vorgeschlagene Dauer: ${minutes} Minuten\n\n` +
          `${leaderMention}, antworte mit ${activePrefix}krieg accept oder ${activePrefix}krieg deny`,
          { mentions: [targetGuild.leader] }
        );
        return true;
      }

      if (sub === 'accept' || sub === 'annehmen') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Kriegserklärungen beantworten.'); return true; }
        const decl = pendingWarDeclarations.get(myGuildId);
        if (!decl) { await send('❌ Es liegt keine offene Kriegserklärung für deine Gilde vor.'); return true; }

        const attackerGuild = guilds[decl.fromGuildId];
        if (!attackerGuild) { pendingWarDeclarations.delete(myGuildId); await send('❌ Diese Gilde existiert nicht mehr.'); return true; }
        ensureGuildWarFields(attackerGuild);

        if (findActiveWarForGuild(decl.fromGuildId) || findActiveWarForGuild(myGuildId)) {
          pendingWarDeclarations.delete(myGuildId);
          await send('❌ Eine der beiden Gilden befindet sich mittlerweile bereits in einem anderen Krieg. Kriegserklärung verfällt.');
          return true;
        }

        const maxHpA = calculateWarHp(attackerGuild);
        const maxHpB = calculateWarHp(myGuild);

        state.wars[decl.warId] = {
          id: decl.warId,
          guildA: decl.fromGuildId,
          guildB: myGuildId,
          maxHpA, hpA: maxHpA,
          maxHpB, hpB: maxHpB,
          status: 'active',
          declaredBy: decl.fromGuildId,
          startedAt: Date.now(),
          endsAt: Date.now() + decl.minutes * 60 * 1000,
          originChat: from,
          damageByUser: {},
          lastAttack: {},
          concedeLoser: null
        };
        saveState();
        pendingWarDeclarations.delete(myGuildId);

        await send(
          `⚔️🏰 *— GILDENKRIEG BEGINNT —* 🏰⚔️\n` +
          `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `*${attackerGuild.name}* (❤️${maxHpA}) VS *${myGuild.name}* (❤️${maxHpB})\n` +
          `⏳ Dauer: ${decl.minutes} Minuten\n\n` +
          `Mitglieder beider Gilden können mit ${activePrefix}kriegsangriff die feindliche Festung angreifen!\n` +
          `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`
        );
        return true;
      }

      if (sub === 'deny' || sub === 'ablehnen') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf Kriegserklärungen beantworten.'); return true; }
        const decl = pendingWarDeclarations.get(myGuildId);
        if (!decl) { await send('❌ Es liegt keine offene Kriegserklärung für deine Gilde vor.'); return true; }
        pendingWarDeclarations.delete(myGuildId);
        await send('🕊️ Kriegserklärung abgelehnt.');
        return true;
      }

      if (sub === 'concede' || sub === 'kapitulieren') {
        if (myGuild.leader !== sender) { await send('❌ Nur der Gildenanführer darf kapitulieren.'); return true; }
        const found = findActiveWarForGuild(myGuildId);
        if (!found) { await send('❌ Deine Gilde befindet sich in keinem aktiven Krieg.'); return true; }
        const [warId, war] = found;
        war.concedeLoser = myGuildId;
        await endWar(warId, ctx, 'concede');
        return true;
      }

      if (sub === 'status' || !sub) {
        const found = findActiveWarForGuild(myGuildId);
        const pendingOut = findPendingOutgoingWar(myGuildId);
        const pendingIn = pendingWarDeclarations.get(myGuildId);

        if (!found && !pendingOut && !pendingIn) {
          await send('ℹ️ Deine Gilde befindet sich in keinem Krieg und hat keine offenen Kriegserklärungen.');
          return true;
        }

        if (pendingIn) {
          const fromGuild = guilds[pendingIn.fromGuildId];
          await send(`⚔️ *${fromGuild?.name || pendingIn.fromGuildId}* hat deiner Gilde den Krieg erklärt!\nAntworte mit ${activePrefix}krieg accept oder ${activePrefix}krieg deny`);
          return true;
        }

        if (pendingOut) {
          await send('⏳ Deine Kriegserklärung wartet noch auf eine Antwort der Gegner-Gilde.');
          return true;
        }

        const [warId, war] = found;
        const isA = war.guildA === myGuildId;
        const myHp = isA ? war.hpA : war.hpB;
        const myMaxHp = isA ? war.maxHpA : war.maxHpB;
        const enemyHp = isA ? war.hpB : war.hpA;
        const enemyMaxHp = isA ? war.maxHpB : war.maxHpA;
        const enemyGid = isA ? war.guildB : war.guildA;
        const enemyName = guilds[enemyGid]?.name || enemyGid;
        const timeLeft = formatTimeLeft(war.endsAt - Date.now());

        await send(
          `⚔️🏰 *— GILDENKRIEG-STATUS —* 🏰⚔️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `🏠 Deine Festung: ${Math.max(0, myHp)} / ${myMaxHp}\n${hpBar(myHp, myMaxHp)}\n\n` +
          `🏴 Feindliche Festung (${enemyName}): ${Math.max(0, enemyHp)} / ${enemyMaxHp}\n${hpBar(enemyHp, enemyMaxHp)}\n\n` +
          `⏳ Verbleibend: ${timeLeft}\n\n` +
          `Nutze ${activePrefix}kriegsangriff, um die feindliche Festung anzugreifen!`
        );
        return true;
      }

      await send(`❌ Nutzung: ${activePrefix}krieg declare/accept/deny/concede/status`);
      return true;
    }

    // =====================================================================
    // KRIEGSANGRIFF
    // =====================================================================
    if (cmd === 'kriegsangriff' || cmd === 'warattack') {
      const myGuildId = getUserGuildId(users, guilds, sender);
      if (!myGuildId || !guilds[myGuildId]) { await send('❌ Du bist in keiner Gilde.'); return true; }

      const found = findActiveWarForGuild(myGuildId);
      if (!found) { await send('ℹ️ Deine Gilde befindet sich in keinem aktiven Krieg.'); return true; }
      const [warId, war] = found;

      const normalizedSender = normalizeJid(sender);
      const now = Date.now();
      const last = war.lastAttack[normalizedSender] || 0;
      if (now - last < WAR_ATTACK_COOLDOWN_MS) {
        const remaining = Math.ceil((WAR_ATTACK_COOLDOWN_MS - (now - last)) / 1000);
        const mins = Math.floor(remaining / 60);
        const secs = remaining % 60;
        await send(`⏰ Du musst noch ${mins}:${secs.toString().padStart(2, '0')} warten, bevor du erneut angreifen kannst.`);
        return true;
      }

      const isA = war.guildA === myGuildId;
      const { damage, weaponName, weaponEmoji, isCrit } = calculateWeaponDamage({
        users, ITEM_DB, ensureArenaFields, jid: normalizedSender, randInt
      });

      if (isA) war.hpB = Math.max(0, war.hpB - damage);
      else war.hpA = Math.max(0, war.hpA - damage);

      war.damageByUser[normalizedSender] = (war.damageByUser[normalizedSender] || 0) + damage;
      war.lastAttack[normalizedSender] = now;
      saveState();

      users[normalizedSender].xp = (users[normalizedSender].xp || 0) + 5;
      save(FILES.users, users);

      const enemyGid = isA ? war.guildB : war.guildA;
      const enemyName = guilds[enemyGid]?.name || enemyGid;
      const enemyHp = isA ? war.hpB : war.hpA;
      const enemyMaxHp = isA ? war.maxHpB : war.maxHpA;
      const critText = isCrit ? '💥 KRITISCHER TREFFER! ' : '';

      await send(
        `⚔️ ${critText}Mit ${weaponEmoji ? weaponEmoji + ' ' : ''}*${weaponName}* fügst du der Festung von *${enemyName}* *${damage}* Schaden zu!\n` +
        `🏴 ${enemyName}: ${Math.max(0, enemyHp)} / ${enemyMaxHp} HP\n` +
        `${hpBar(enemyHp, enemyMaxHp)}`
      );

      if (war.hpA <= 0 || war.hpB <= 0) {
        await endWar(warId, ctx, 'dead');
      }

      return true;
    }

    return false;
  }

  return { handle, checkExpiry, COMMANDS: GUILDWAR_COMMANDS, HELP_TEXT: GUILDWAR_HELP_TEXT };
}
