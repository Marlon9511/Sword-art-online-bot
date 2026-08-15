import fs from 'fs';
import path from 'path';

function findUserGuildId(users, guilds, userJid) {
  if (users[userJid]?.guildId && guilds[users[userJid].guildId]) {
    return users[userJid].guildId;
  }
  for (const [gid, g] of Object.entries(guilds)) {
    if (Array.isArray(g.members) && g.members.includes(userJid)) return gid;
  }
  return null;
}

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

const RAGNAROK_ITEM_ID = 'w_ragnarok';
const RAGNAROK_MIN_DAMAGE = 500;
const RAGNAROK_MAX_DAMAGE = 4000;

const EXCALIBUR_ITEM_ID = 'w_excalibur';
const EXCALIBUR_MIN_DAMAGE = 250;
const EXCALIBUR_MAX_DAMAGE = 1000;

const RAGNAROK_BASE_CHANCE_PERCENT = 10;
const RAGNAROK_CHANCE_PER_1000_HP = 5;
const RAGNAROK_MAX_CHANCE_PERCENT = 75;

function calculateRagnarokChance(maxHp) {
  const chance = RAGNAROK_BASE_CHANCE_PERCENT + (maxHp / 1000) * RAGNAROK_CHANCE_PER_1000_HP;
  return Math.min(RAGNAROK_MAX_CHANCE_PERCENT, Math.round(chance));
}

const RAGNAROK_RANK_CHANCE_DIVISOR = { 1: 1, 2: 2, 3: 3 };

function calculateGuildRankRagnarokChance(maxHp, rank) {
  const baseChance = calculateRagnarokChance(maxHp);
  const divisor = RAGNAROK_RANK_CHANCE_DIVISOR[rank] || 4;
  return Math.max(1, Math.round(baseChance / divisor));
}

export function createGuildBossSystem(DATA_PATH) {
  const BOSS_FILE = path.join(DATA_PATH, 'guildboss.json');

  const defaultState = {
    active: false,
    name: null,
    maxHp: 0,
    hp: 0,
    startedAt: null,
    endsAt: null,
    startedBy: null,
    originChat: null,
    damageByGuild: {},
    damageByUser: {},
    lastAttack: {}
  };

  let state = loadState();

  function loadState() {
    try {
      if (!fs.existsSync(BOSS_FILE)) {
        fs.writeFileSync(BOSS_FILE, JSON.stringify(defaultState, null, 2));
        return { ...defaultState };
      }
      const raw = JSON.parse(fs.readFileSync(BOSS_FILE, 'utf8'));
      return { ...defaultState, ...raw };
    } catch (e) {
      console.error('[guildboss] Fehler beim Laden:', e);
      return { ...defaultState };
    }
  }

  function saveState() {
    try {
      fs.writeFileSync(BOSS_FILE, JSON.stringify(state, null, 2));
    } catch (e) {
      console.error('[guildboss] Fehler beim Speichern:', e);
    }
  }

  const ATTACK_COOLDOWN_MS = 3 * 60 * 1000;
  const BOSS_NAMES = [
    'Der Skelettreaper von Floor 74', 'Heathcliffs Schatten', 'Der Sturmdrache Kayaba',
    'Der Kristallgolem von Floor 22', 'Der Verzerrte Wächter'
  ];

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

  function getTopGuilds(limit = 5) {
    return Object.entries(state.damageByGuild).sort((a, b) => b[1] - a[1]).slice(0, limit);
  }

  function calculateWeaponDamage({ users, ITEM_DB, ensureArenaFields, normalizedSender, randInt, isPrimaryOwner }) {
    ensureArenaFields(users, normalizedSender);
    const weaponId = users[normalizedSender].equipped?.weapon;
    const weapon = weaponId ? ITEM_DB[weaponId] : null;

    if (!weapon) {
      return {
        damage: randInt(5, 15),
        weaponName: 'bloße Fäuste',
        weaponEmoji: '👊',
        isCrit: false,
        isSecret: false,
        isRagnarok: false,
        isExcalibur: false
      };
    }

    const basePower = weapon.power || 10;
    const isSecretWeapon = !!(weapon.secret || weapon.ownerOnly);
    const isRagnarok = weaponId === RAGNAROK_ITEM_ID;
    const isExcalibur = weaponId === EXCALIBUR_ITEM_ID;

    if (isRagnarok) {
      let damage = randInt(RAGNAROK_MIN_DAMAGE, RAGNAROK_MAX_DAMAGE);
      const critChance = (CRIT_CHANCE_BY_RARITY[weapon.rarity] || 5) + SECRET_WEAPON_CRIT_BONUS + 15;
      const isCrit = randInt(1, 100) <= critChance;
      if (isCrit) damage = Math.round(damage * 2);

      return {
        damage,
        weaponName: weapon.name,
        weaponEmoji: RARITY_EMOJI[weapon.rarity] || '⚫',
        isCrit,
        isSecret: true,
        isRagnarok: true,
        isExcalibur: false
      };
    }

    if (isExcalibur) {
      let damage = randInt(EXCALIBUR_MIN_DAMAGE, EXCALIBUR_MAX_DAMAGE);
      const critChance = (CRIT_CHANCE_BY_RARITY[weapon.rarity] || 5) + SECRET_WEAPON_CRIT_BONUS;
      const isCrit = randInt(1, 100) <= critChance;
      if (isCrit) damage = Math.round(damage * 2);

      return {
        damage,
        weaponName: weapon.name,
        weaponEmoji: RARITY_EMOJI[weapon.rarity] || '⚫',
        isCrit,
        isSecret: true,
        isRagnarok: false,
        isExcalibur: true
      };
    }

    let rarityBonus = RARITY_DAMAGE_BONUS[weapon.rarity] || 0;
    if (isSecretWeapon) rarityBonus += SECRET_WEAPON_BONUS;

    const variance = 0.85 + (randInt(0, 30) / 100);
    let damage = Math.round(basePower * (1 + rarityBonus) * variance);

    let critChance = CRIT_CHANCE_BY_RARITY[weapon.rarity] || 5;
    if (isSecretWeapon) critChance += SECRET_WEAPON_CRIT_BONUS;

    const isCrit = randInt(1, 100) <= critChance;
    if (isCrit) damage = Math.round(damage * 2);

    const viewerIsPrimaryOwner = isPrimaryOwner ? isPrimaryOwner(normalizedSender) : false;
    let weaponName = weapon.name;
    let weaponEmoji = RARITY_EMOJI[weapon.rarity] || '';

    if (isSecretWeapon && !isRagnarok && !viewerIsPrimaryOwner) {
      weaponName = 'einer geheimnisvollen Klinge';
      weaponEmoji = '❓';
    }

    return { damage, weaponName, weaponEmoji, isCrit, isSecret: isSecretWeapon, isRagnarok, isExcalibur };
  }

  function distributeRagnarok({ users, guilds, save, FILES, ITEM_DB, ensureArenaFields, randInt, top3Guilds, maxHp }) {
    const results = [];

    top3Guilds.forEach(([guildId, guildDamage], idx) => {
      const rank = idx + 1;
      const chance = calculateGuildRankRagnarokChance(maxHp, rank);
      const roll = randInt(1, 100);
      const hit = roll <= chance;
      const recipients = [];

      if (hit) {
        const guild = guilds[guildId];
        const members = Array.isArray(guild?.members) ? guild.members : [];
        for (const jid of members) {
          if (!users[jid]) continue;
          ensureArenaFields(users, jid);
          users[jid].items[RAGNAROK_ITEM_ID] = (users[jid].items[RAGNAROK_ITEM_ID] || 0) + 1;
          recipients.push(jid);
        }
      }

      results.push({
        guildId,
        guildName: guilds[guildId]?.name || guildId,
        rank,
        hit,
        chance,
        recipients
      });
    });

    save(FILES.users, users);
    return results;
  }

  async function endEvent({ send, sock, users, guilds, save, FILES, getNumberMention, ITEM_DB, ensureArenaFields, randInt }, reason = 'time') {
    if (!state.active) return;

    const topGuilds = getTopGuilds(1);
    const totalDamage = Object.values(state.damageByGuild).reduce((a, b) => a + b, 0);

    if (!topGuilds.length || totalDamage === 0) {
      const msg = 'ℹ️ Boss-Event beendet — es wurde kein Schaden verursacht, keine Belohnung vergeben.';
      try {
        if (state.originChat) await sock.sendMessage(state.originChat, { text: msg });
        else await send(msg);
      } catch (e) {}
      state = { ...defaultState };
      saveState();
      return;
    }

    const [winningGuildId, winningDamage] = topGuilds[0];
    const winningGuild = guilds[winningGuildId];
    const guildName = winningGuild?.name || winningGuildId;
    const members = Array.isArray(winningGuild?.members) ? winningGuild.members : [];

    const REWARD_COINS_PER_MEMBER = 300;
    const REWARD_XP_PER_MEMBER = 100;
    const mentions = [];

    for (const jid of members) {
      if (!users[jid]) continue;
      users[jid].coins = (users[jid].coins || 0) + REWARD_COINS_PER_MEMBER;
      users[jid].xp = (users[jid].xp || 0) + REWARD_XP_PER_MEMBER;
      mentions.push(jid);
    }

    const topPlayerEntry = Object.entries(state.damageByUser).sort((a, b) => b[1] - a[1])[0];
    let topPlayerLine = '';
    let topPlayerJid = null;

    if (topPlayerEntry) {
      const [topJid, topDmg] = topPlayerEntry;
      topPlayerJid = topJid;
      if (users[topJid]) {
        users[topJid].coins = (users[topJid].coins || 0) + 150;
        users[topJid].xp = (users[topJid].xp || 0) + 50;
        mentions.push(topJid);
        const mention = await getNumberMention(topJid, sock);
        topPlayerLine = `\n⚔️ Höchster Einzelschaden: ${mention} (${topDmg} DMG) — Bonus: +150 Coins, +50 XP`;
      }
    }

    save(FILES.users, users);

    let ragnarokLine = '';
    if (ITEM_DB && ensureArenaFields && randInt && ITEM_DB[RAGNAROK_ITEM_ID]) {
      const top3Guilds = getTopGuilds(3);
      const results = distributeRagnarok({
        users, guilds, save, FILES, ITEM_DB, ensureArenaFields, randInt,
        top3Guilds, maxHp: state.maxHp
      });

      const ragnarok = ITEM_DB[RAGNAROK_ITEM_ID];
      const rankEmoji = { 1: '🥇', 2: '🥈', 3: '🥉' };

      const resultLines = results.map(r => {
        if (r.hit) {
          mentions.push(...r.recipients);
          return `${rankEmoji[r.rank] || `${r.rank}.`} *${r.guildName}* — Treffer! (${r.chance}% Chance) Alle Mitglieder erhalten *${ragnarok.name}*! ⚫✨`;
        }
        return `${rankEmoji[r.rank] || `${r.rank}.`} *${r.guildName}* — kein Glück (${r.chance}% Chance)`;
      });

      if (resultLines.length) {
        ragnarokLine =
          `\n\n⚫✨ *— RAGNAROK-AUSLOSUNG (TOP 3 GILDEN) —* ✨⚫\n` +
          `_${ragnarok.trueName}_\n` +
          resultLines.join('\n');
      }
    }

    const guildRanking = getTopGuilds(5)
      .map(([gid, dmg], i) => `${i + 1}. ${guilds[gid]?.name || gid} — ${dmg} DMG`)
      .join('\n');

    const reasonText = reason === 'dead' ? '💀 Der Boss wurde besiegt!' : '⏳ Die Zeit ist abgelaufen.';

    const summary =
      `⚔️ *— CLAN-BOSS EVENT BEENDET —* ⚔️\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
      `${reasonText}\n\n` +
      `🏆 *Siegreiche Gilde:* ${guildName}\n` +
      `💥 Gesamtschaden: ${winningDamage} DMG\n` +
      `🎁 Belohnung pro Mitglied: +${REWARD_COINS_PER_MEMBER} Coins, +${REWARD_XP_PER_MEMBER} XP` +
      topPlayerLine +
      ragnarokLine +
      `\n\n📊 *Schadensrangliste (Gilden):*\n${guildRanking}\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`;

    try {
      if (state.originChat) {
        await sock.sendMessage(state.originChat, { text: summary, mentions });
      } else {
        await send(summary, { mentions });
      }
    } catch (e) {
      await send(summary, { mentions });
    }

    state = { ...defaultState };
    saveState();
  }

  async function checkExpiry(ctx) {
    if (!state.active) return;
    if (state.endsAt && Date.now() >= state.endsAt) {
      await endEvent(ctx, 'time');
    }
  }

  async function handle(ctx) {
    const {
      cmd, args, sender, from, isGroup, activePrefix, send, sock,
      users, guilds, save, FILES, ensureUser, normalizeJid,
      getNumberMention, randInt, isAuthorized,
      ITEM_DB, ensureArenaFields, isPrimaryOwner
    } = ctx;

    if (cmd === 'bossevent') {
      const sub = (args[0] || '').toLowerCase();

      if (sub === 'start') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
          await send('❌ Nur Owner/CoOwner dürfen ein Boss-Event starten.');
          return true;
        }
        if (state.active) {
          await send('❌ Es läuft bereits ein Boss-Event. Beende es zuerst mit ?bossevent end.');
          return true;
        }

        const hp = parseInt(args[1]);
        const minutes = parseInt(args[2]) || 60;
        if (!hp || hp < 100) {
          await send(`❌ Nutzung: ${activePrefix}bossevent start <hp> [minuten]\nBeispiel: ${activePrefix}bossevent start 5000 60`);
          return true;
        }

        const bossName = BOSS_NAMES[randInt(0, BOSS_NAMES.length - 1)];

        state = {
          ...defaultState,
          active: true,
          name: bossName,
          maxHp: hp,
          hp,
          startedAt: Date.now(),
          endsAt: Date.now() + minutes * 60 * 1000,
          startedBy: sender,
          originChat: from,
          damageByGuild: {},
          damageByUser: {},
          lastAttack: {}
        };
        saveState();

        await send(
          `⚔️ *— CLAN-BOSS ERSCHEINT —* ⚔️\n` +
          `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `👹 *${bossName}*\n` +
          `❤️ HP: ${hp} / ${hp}\n` +
          `${hpBar(hp, hp)}\n` +
          `⏳ Dauer: ${minutes} Minuten\n\n` +
          `Kämpft mit euren Gilden gemeinsam! Nutzt:\n` +
          `➡️ ${activePrefix}bossattack — Schaden zufügen (deine ausgerüstete Waffe bestimmt den Schaden!)\n` +
          `➡️ ${activePrefix}bossevent status — Status anzeigen\n\n` +
          `💡 Rüstet eure beste Waffe mit ${activePrefix}equip aus, bevor ihr angreift!\n` +
          `🏆 Die Gilde mit dem meisten Gesamtschaden gewinnt die Belohnung!\n` +
          `⚫ Die *Top 3 Gilden* der Schadensrangliste haben je eine eigene Chance auf *Ragnarok* für alle Mitglieder — Platz 1: ${calculateGuildRankRagnarokChance(hp, 1)}%, Platz 2: ${calculateGuildRankRagnarokChance(hp, 2)}%, Platz 3: ${calculateGuildRankRagnarokChance(hp, 3)}%!\n` +
          `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`
        );
        return true;
      }

      if (sub === 'end') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
          await send('❌ Nur Owner/CoOwner dürfen das Event abbrechen.');
          return true;
        }
        if (!state.active) {
          await send('ℹ️ Es läuft aktuell kein Boss-Event.');
          return true;
        }
        await endEvent(ctx, 'manual');
        return true;
      }

      if (sub === 'resetcd' || sub === 'cdreset') {
        if (!isAuthorized(sender, ['OWNER', 'COOWNER'])) {
          await send('❌ Nur Owner/CoOwner dürfen den Cooldown zurücksetzen.');
          return true;
        }
        if (!state.active) {
          await send('ℹ️ Es läuft aktuell kein Boss-Event, es gibt keinen Cooldown zum Zurücksetzen.');
          return true;
        }

        const target = (args[1] || '').toLowerCase();

        if (target === 'all') {
          state.lastAttack = {};
          saveState();
          await send('✅ Alle Angriffs-Cooldowns wurden zurückgesetzt.');
          return true;
        }

        const normalizedSender = normalizeJid(sender);
        delete state.lastAttack[normalizedSender];
        saveState();
        await send('✅ Dein Angriffs-Cooldown wurde zurückgesetzt. Du kannst sofort wieder angreifen.');
        return true;
      }

      if (sub === 'status' || !sub) {
        if (!state.active) {
          await send('ℹ️ Aktuell erscheint kein Clan-Boss. Der Owner kann eins mit ?bossevent start <hp> [minuten] aktivieren.');
          return true;
        }
        const timeLeft = formatTimeLeft(state.endsAt - Date.now());
        const guildRanking = getTopGuilds(5)
          .map(([gid, dmg], i) => `${i + 1}. ${guilds[gid]?.name || gid} — ${dmg} DMG`)
          .join('\n') || '(noch kein Schaden)';

        const currentTopGuildEntry = getTopGuilds(1)[0];
        let topGuildPreview = '';
        if (currentTopGuildEntry) {
          const [topGid, topDmg] = currentTopGuildEntry;
          const topGuildName = guilds[topGid]?.name || topGid;
          topGuildPreview = `\n🏆 Aktuell führend: *${topGuildName}* (${topDmg} DMG)`;
        }

        await send(
          `👹 *${state.name}*\n` +
          `❤️ HP: ${Math.max(0, state.hp)} / ${state.maxHp}\n` +
          `${hpBar(state.hp, state.maxHp)}\n` +
          `⏳ Verbleibend: ${timeLeft}${topGuildPreview}\n` +
          `⚫ Ragnarok-Chance (Top 3 Gilden): 🥇 ${calculateGuildRankRagnarokChance(state.maxHp, 1)}% | 🥈 ${calculateGuildRankRagnarokChance(state.maxHp, 2)}% | 🥉 ${calculateGuildRankRagnarokChance(state.maxHp, 3)}%\n\n` +
          `📊 *Aktuelle Gilden-Rangliste:*\n${guildRanking}\n\n` +
          `Nutze ${activePrefix}bossattack, um mitzukämpfen!`
        );
        return true;
      }

      await send(`Nutzung: ${activePrefix}bossevent start <hp> [minuten] | status | end | resetcd [all]`);
      return true;
    }

    if (cmd === 'bossattack' || cmd === 'bossangriff') {
      if (!state.active) {
        await send('ℹ️ Aktuell erscheint kein Clan-Boss zum Angreifen.');
        return true;
      }

      ensureUser(sender);
      const normalizedSender = normalizeJid(sender);
      const guildId = findUserGuildId(users, guilds, normalizedSender);

      if (!guildId) {
        await send('❌ Du musst Mitglied einer Gilde sein, um am Clan-Boss-Event teilzunehmen.');
        return true;
      }

      const now = Date.now();
      const last = state.lastAttack[normalizedSender] || 0;
      if (now - last < ATTACK_COOLDOWN_MS) {
        const remaining = Math.ceil((ATTACK_COOLDOWN_MS - (now - last)) / 1000);
        const mins = Math.floor(remaining / 60);
        const secs = remaining % 60;
        await send(`⏰ Du musst noch ${mins}:${secs.toString().padStart(2, '0')} warten, bevor du erneut angreifen kannst.`);
        return true;
      }

      const { damage, weaponName, weaponEmoji, isCrit, isSecret, isRagnarok, isExcalibur } = calculateWeaponDamage({
        users, ITEM_DB, ensureArenaFields, normalizedSender, randInt, isPrimaryOwner
      });

      state.hp = Math.max(0, state.hp - damage);
      state.damageByGuild[guildId] = (state.damageByGuild[guildId] || 0) + damage;
      state.damageByUser[normalizedSender] = (state.damageByUser[normalizedSender] || 0) + damage;
      state.lastAttack[normalizedSender] = now;

      users[normalizedSender].xp = (users[normalizedSender].xp || 0) + 5;
      save(FILES.users, users);
      saveState();

      const guildName = guilds[guildId]?.name || guildId;
      const critText = isCrit ? '💥 KRITISCHER TREFFER! ' : '';
      const secretFlavor = isSecret && !isRagnarok && !isExcalibur ? '\n🌌 Eine unheimliche Macht durchströmt die Waffe...' : '';
      const ragnarokFlavor = isRagnarok ? '\n⚫ *RAGNAROK ERWACHT* — die Klinge der Götterdämmerung tobt!' : '';
      const excaliburFlavor = isExcalibur ? '\n⚫ *EXCALIBUR ERSTRAHLT* — die Klinge des Systemadministrators verwüstet den Boss!' : '';

      await send(
        `⚔️ ${critText}Mit ${weaponEmoji ? weaponEmoji + ' ' : ''}*${weaponName}* fügst du dem Boss *${damage}* Schaden zu!${secretFlavor}${ragnarokFlavor}${excaliburFlavor}\n` +
        `👹 ${state.name}: ${Math.max(0, state.hp)} / ${state.maxHp} HP\n` +
        `${hpBar(state.hp, state.maxHp)}\n` +
        `🛡️ Für Gilde: ${guildName}`
      );

      if (state.hp <= 0) {
        await endEvent(ctx, 'dead');
      }

      return true;
    }

    return false;
  }

  return {
    handle,
    checkExpiry,
    COMMANDS: ['bossevent', 'bossattack', 'bossangriff'],
    HELP_TEXT:
`⚔️ *Clan-Boss-Event*
${'{P}'}bossevent start <hp> [min] — Boss starten (Owner)
${'{P}'}bossevent status — Boss-Status ansehen
${'{P}'}bossevent end — Event abbrechen (Owner)
${'{P}'}bossevent resetcd [all] — Cooldown zurücksetzen zum Testen (Owner)
${'{P}'}bossattack — Dem Boss Schaden zufügen (Schaden = deine ausgerüstete Waffe!)
_Die Top 3 Gilden der Schadensrangliste haben je eine eigene, absteigende Chance, dass ALLE Mitglieder eine Ragnarok erhalten!_`
  };
}