import fs from 'fs';
import path from 'path';

const BREATHING_STYLES = {
  wasser:    { name: '💧 Wasseratmung',     rarity: 'common',    power: 15, weight: 26 },
  flamme:    { name: '🔥 Flammenatmung',    rarity: 'common',    power: 16, weight: 24 },
  nebel:     { name: '🌫️ Nebelatmung',      rarity: 'common',    power: 14, weight: 20 },
  donner:    { name: '⚡ Donneratmung',     rarity: 'uncommon',  power: 20, weight: 12 },
  wind:      { name: '🌪️ Windatmung',       rarity: 'uncommon',  power: 19, weight: 10 },
  stein:     { name: '🪨 Steinatmung',      rarity: 'uncommon',  power: 22, weight: 8  },
  liebe:     { name: '💗 Liebesatmung',     rarity: 'rare',      power: 25, weight: 5  },
  klang:     { name: '🔔 Klangatmung',      rarity: 'rare',      power: 24, weight: 4  },
  insekt:    { name: '🦋 Insektenatmung',   rarity: 'rare',      power: 23, weight: 4  },
  schlange:  { name: '🐍 Schlangenatmung',  rarity: 'rare',      power: 24, weight: 4  },

  sonne:     { name: '☀️ Sonnenatmung',     trueName: 'Atmung der Sonne — Hinokami Kagura', rarity: 'legendary', power: 100, weight: 0.05, ownerWeight: 3, secret: true },
  mond:      { name: '🌙 Mondatmung',       trueName: 'Atmung des Mondes', rarity: 'legendary', power: 95,  weight: 0.05, ownerWeight: 3, secret: true },

  douma:     { name: '❄️ Doumas Blutdämonenkunst',   trueName: 'Frostzirkel der Oberen Zwei — Ewiges Eis', rarity: 'epic',      power: 90,  weight: 0.07, ownerWeight: 3.5, secret: true, type: 'demonart' },
  akaza:     { name: '👊 Akazas Vernichtungskunst',  trueName: 'Kompassnadel-Zerschlagung der Oberen Drei', rarity: 'epic',      power: 88,  weight: 0.07, ownerWeight: 3.5, secret: true, type: 'demonart' },
  kokushibo: { name: '🗡️ Kokushibos Mondklingenkunst', trueName: 'Tsuki no Utsuroi — Tanz der Mondphasen', rarity: 'epic',      power: 92,  weight: 0.05, ownerWeight: 3.5, secret: true, type: 'demonart' },
  muzan:     { name: '👑 Muzan Kibutsujis Königskunst', trueName: 'Blut des Dämonenkönigs — Unendliches Verderben', rarity: 'legendary', power: 120, weight: 0.015, ownerWeight: 2, secret: true, type: 'demonart' }
};

const RARITY_LABEL = {
  common: '⚪ Gewöhnlich',
  uncommon: '🟢 Ungewöhnlich',
  rare: '🔵 Selten',
  epic: '🟣 Episch',
  legendary: '🟡 Legendär'
};

const SHOP_PRICE = {
  common: 400,
  uncommon: 700,
  rare: 1200
};

const RANKS = [
  'Mizunoto', 'Mizunoe', 'Kanoto', 'Kanoe', 'Tsuchinoto',
  'Tsuchinoe', 'Hinoto', 'Hinoe', 'Kinoto', 'Kinoe', 'Hashira'
];
const RANK_XP_REQUIRED = (idx) => idx === 0 ? 0 : idx * 150 + (idx >= RANKS.length - 1 ? 500 : 0);

const GACHA_COST = 300;
const BOSS_COOLDOWN_MS = 5 * 60 * 1000;
const BOSS_MAX_HP = 500000;

export function createDemonSlayerSystem(DATA_PATH) {
  const FILE_PATH = path.join(DATA_PATH, 'demonslayer.json');

  function ensureFile() {
    if (!fs.existsSync(FILE_PATH)) {
      fs.writeFileSync(FILE_PATH, JSON.stringify({ players: {}, boss: null }, null, 2));
    }
  }
  ensureFile();

  function load() {
    try {
      const raw = fs.readFileSync(FILE_PATH, 'utf8');
      const data = JSON.parse(raw || '{}');
      if (!data.players) data.players = {};
      if (data.boss === undefined) data.boss = null;
      return data;
    } catch {
      return { players: {}, boss: null };
    }
  }

  function save(data) {
    fs.writeFileSync(FILE_PATH, JSON.stringify(data, null, 2));
  }

  function ensurePlayer(data, jid) {
    if (!data.players[jid]) {
      data.players[jid] = {
        styles: {},
        activeStyle: null,
        rankIndex: 0,
        xp: 0,
        wins: 0,
        lastBoss: 0
      };
    }
    const p = data.players[jid];
    if (!p.styles) p.styles = {};
    if (p.style && !p.styles[p.style]) {
      p.styles[p.style] = true;
      if (!p.activeStyle) p.activeStyle = p.style;
    }
    if (p.activeStyle === undefined) p.activeStyle = null;
    return p;
  }

  function spawnBossIfNeeded(data) {
    if (!data.boss || data.boss.hp <= 0) {
      const names = [
        'Der Zerfleischer im Nebel',
        'Der Verzehrende von Aincrad',
        'Die Blutmond-Bestie',
        'Der Namenlose Dämon des Ostwalds'
      ];
      data.boss = {
        name: names[Math.floor(Math.random() * names.length)],
        hp: BOSS_MAX_HP,
        maxHp: BOSS_MAX_HP,
        spawnedAt: Date.now()
      };
    }
    return data.boss;
  }

  function rollStyle(isOwnerLike) {
    let total = 0;
    const pool = [];
    for (const [id, s] of Object.entries(BREATHING_STYLES)) {
      const w = (s.secret && isOwnerLike) ? (s.ownerWeight ?? s.weight) : s.weight;
      total += w;
      pool.push({ id, w });
    }
    let r = Math.random() * total;
    for (const entry of pool) {
      r -= entry.w;
      if (r <= 0) return entry.id;
    }
    return pool[pool.length - 1].id;
  }

  function healthBar(hp, maxHp, slots = 10) {
    const filled = Math.max(0, Math.min(slots, Math.round((hp / maxHp) * slots)));
    return '🟥'.repeat(filled) + '⬛'.repeat(slots - filled);
  }

  function styleLine(styleId) {
    const s = BREATHING_STYLES[styleId];
    if (!s) return '— (unbekannter Atemstil)';
    return `${s.name} ${RARITY_LABEL[s.rarity] || ''}`;
  }

  async function handle(ctx) {
    const {
      cmd, args, sender, from, isGroup, activePrefix, send,
      normalizeJid, isSameJid, getNumberMention, randInt, isPrimaryOwner
    } = ctx;

    const P = activePrefix;
    const senderJid = normalizeJid(sender);

    if (cmd === 'dshelp' || cmd === 'demonslayerhelp') {
      await send(
        `👹 *— DÄMONENTÖTER-SYSTEM —* 👹\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${P}atemzug — zufälligen Atemstil erlernen (${GACHA_COST} Coins)\n` +
        `${P}atemshop — Atemstile kaufen (feste Preise)\n` +
        `${P}atemkaufen <id> — einen Stil aus dem Shop kaufen\n` +
        `${P}atemschenken @user <id> — Stil an jemanden verschenken\n` +
        `${P}atemsammlung — deine besessenen Stile anzeigen\n` +
        `${P}atemausruesten <id> — aktiven Kampf-Stil wechseln\n` +
        `${P}atemstil — deinen aktuell ausgerüsteten Stil anzeigen\n` +
        `${P}atemliste — alle bekannten Atemstile anzeigen\n` +
        `${P}demonboss — den Dämon angreifen\n` +
        `${P}dsrang — deinen Korps-Rang anzeigen\n` +
        `${P}dsaufstieg — versuchen, im Rang aufzusteigen\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `🟣 Doumas, Akazas & Kokushibos Dämonenkünste sind episch selten.\n` +
        `👑 Muzans Königskunst ist legendär — alle NICHT käuflich, nur ${P}atemzug kann sie hervorbringen.`
      );
      return true;
    }

    if (cmd === 'atemliste' || cmd === 'breathinglist') {
      const lines = Object.values(BREATHING_STYLES)
        .sort((a, b) => a.power - b.power)
        .map(s => `${s.secret ? (s.type === 'demonart' ? '👹' : '❓') : '•'} ${s.secret ? '???' : s.name} ${RARITY_LABEL[s.rarity]}`);
      await send(`📜 *— BEKANNTE ATEMSTILE & DÄMONENKÜNSTE —* 📜\n\n${lines.join('\n')}\n\n_Manche Kräfte sind so selten, dass sie als Gerücht gelten..._`);
      return true;
    }

    if (cmd === 'atemshop' || cmd === 'breathingshop') {
      const lines = Object.entries(BREATHING_STYLES)
        .filter(([, s]) => !s.secret)
        .sort((a, b) => a[1].power - b[1].power)
        .map(([id, s]) => `• \`${id}\` — ${s.name} ${RARITY_LABEL[s.rarity]} | Kraft: ${s.power} | 💰 ${SHOP_PRICE[s.rarity]} Coins`);
      await send(
        `🛒 *— ATEM-SHOP —* 🛒\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${lines.join('\n')}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `Kaufen: ${P}atemkaufen <id>\n` +
        `Verschenken: ${P}atemschenken @user <id>\n\n` +
        `🟣👑 Dämonenkünste (Douma, Akaza, Kokushibo, Muzan) sowie Sonnen- & Mondatmung sind NICHT käuflich — nur durch ${P}atemzug erreichbar.`
      );
      return true;
    }

    if (cmd === 'atemkaufen' || cmd === 'buybreathing') {
      const wanted = (args[0] || '').toLowerCase();
      const style = BREATHING_STYLES[wanted];

      if (!wanted || !style) {
        await send(`❌ Nutzung: ${P}atemkaufen <id>\nSieh dir ${P}atemshop für die Liste an.`);
        return true;
      }
      if (style.secret) {
        await send(`❌ ${style.name} ist nicht käuflich — dieser Stil kann nur durch ${P}atemzug erlangt werden.`);
        return true;
      }

      const data = load();
      const player = ensurePlayer(data, senderJid);

      if (player.styles[wanted]) {
        save(data);
        await send(`ℹ️ Du beherrschst ${style.name} bereits.`);
        return true;
      }

      const price = SHOP_PRICE[style.rarity];
      const coins = ctx.users[senderJid]?.coins || 0;
      if (coins < price) {
        save(data);
        await send(`❌ Du brauchst ${price} Coins für ${style.name}. (Du hast: ${coins})`);
        return true;
      }

      ctx.users[senderJid].coins -= price;
      ctx.save(ctx.FILES.users, ctx.users);

      player.styles[wanted] = true;
      if (!player.activeStyle) player.activeStyle = wanted;
      save(data);

      await send(
        `✅ Du hast ${style.name} für ${price} Coins erworben!\n` +
        (player.activeStyle === wanted
          ? `Er ist jetzt automatisch dein aktiver Kampf-Stil.`
          : `Nutze ${P}atemausruesten ${wanted}, um ihn auszurüsten.`)
      );
      return true;
    }

    if (cmd === 'atemschenken' || cmd === 'giftbreathing') {
      const mctx = ctx.m?.message?.extendedTextMessage?.contextInfo;
      let target = args[0];
      const wanted = (args[1] || '').toLowerCase();

      if (mctx?.mentionedJid?.length) {
        target = mctx.mentionedJid[0];
      } else if (target?.startsWith('@')) {
        target = target.slice(1);
      }

      const style = BREATHING_STYLES[wanted];

      if (!target || !wanted || !style) {
        await send(`❌ Nutzung: ${P}atemschenken @user <id>\nSieh dir ${P}atemshop für die Liste an.`);
        return true;
      }
      if (style.secret) {
        await send(`❌ ${style.name} ist nicht verschenkbar — dieser Stil kann nur durch ${P}atemzug erlangt werden.`);
        return true;
      }

      const targetJid = normalizeJid(target);
      if (!targetJid || (!targetJid.endsWith('@s.whatsapp.net') && !targetJid.endsWith('@lid'))) {
        await send(`❌ Nutzung: ${P}atemschenken @user <id>`);
        return true;
      }
      if (isSameJid(targetJid, senderJid)) {
        await send(`❌ Du kannst dir nicht selbst einen Atemstil schenken. Nutze ${P}atemkaufen.`);
        return true;
      }

      const data = load();
      const targetPlayer = ensurePlayer(data, targetJid);

      if (targetPlayer.styles[wanted]) {
        save(data);
        await send(`ℹ️ @${targetJid.split('@')[0]} beherrscht ${style.name} bereits.`, { mentions: [targetJid] });
        return true;
      }

      const price = SHOP_PRICE[style.rarity];
      const coins = ctx.users[senderJid]?.coins || 0;
      if (coins < price) {
        save(data);
        await send(`❌ Du brauchst ${price} Coins, um ${style.name} zu verschenken. (Du hast: ${coins})`);
        return true;
      }

      ctx.users[senderJid].coins -= price;
      ctx.ensureUser(targetJid);
      ctx.save(ctx.FILES.users, ctx.users);

      targetPlayer.styles[wanted] = true;
      if (!targetPlayer.activeStyle) targetPlayer.activeStyle = wanted;
      save(data);

      await send(
        `🎁 @${senderJid.split('@')[0]} hat @${targetJid.split('@')[0]} den Atemstil ${style.name} geschenkt! (${price} Coins)`,
        { mentions: [senderJid, targetJid] }
      );
      return true;
    }

    if (cmd === 'atemzug' || cmd === 'learnbreathing') {
      const data = load();
      const player = ensurePlayer(data, senderJid);

      const coins = ctx.users[senderJid]?.coins || 0;
      if (coins < GACHA_COST) {
        save(data);
        await send(`❌ Du brauchst ${GACHA_COST} Coins, um einen Atemstil zu erlernen. (Du hast: ${coins})`);
        return true;
      }

      ctx.users[senderJid].coins -= GACHA_COST;
      ctx.save(ctx.FILES.users, ctx.users);

      const ownerLike = isPrimaryOwner(senderJid);
      const styleId = rollStyle(ownerLike);
      const style = BREATHING_STYLES[styleId];
      const alreadyOwned = !!player.styles[styleId];

      player.styles[styleId] = true;
      if (!player.activeStyle) player.activeStyle = styleId;
      save(data);

      const isSecret = !!style.secret;
      const isDemonArt = style.type === 'demonart';
      const header = isDemonArt
        ? `🩸🩸🩸 *— ERWACHEN EINER DÄMONENKUNST —* 🩸🩸🩸`
        : isSecret
          ? `🌟🌟🌟 *— LEGENDÄRES ERWACHEN —* 🌟🌟🌟`
          : `⚔️ *— ATEM ERLERNT —* ⚔️`;

      await send(
        `${header}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${style.name}${isSecret ? `\n"${style.trueName}"` : ''}\n` +
        `${RARITY_LABEL[style.rarity]} | Kraft: ${style.power}\n` +
        (alreadyOwned ? `\n_(Du beherrschst diesen Stil bereits — kein doppelter Eintrag in deiner Sammlung.)_\n` : '') +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        (isDemonArt
          ? `Ein Puls dämonischen Blutes rast durch deine Adern... du hast eine Kraft erlangt, die eigentlich keinem Menschen zusteht. 🩸`
          : isSecret
            ? `Ein Licht durchbricht den Nebel von Aincrad... du hast Geschichte geschrieben. 🔥`
            : `Trainiere fleißig und stelle dich dem Dämon mit ${P}demonboss!\nNutze ${P}atemausruesten, um zwischen deinen Stilen zu wechseln.`)
      );
      return true;
    }

    if (cmd === 'atemsammlung' || cmd === 'mybreathings' || cmd === 'breathingcollection') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      save(data);

      const owned = Object.keys(player.styles);
      if (!owned.length) {
        await send(`❌ Du besitzt noch keinen Atemstil. Nutze ${P}atemzug oder ${P}atemshop.`);
        return true;
      }

      const lines = owned
        .sort((a, b) => (BREATHING_STYLES[a]?.power || 0) - (BREATHING_STYLES[b]?.power || 0))
        .map(id => `${player.activeStyle === id ? '✅' : '•'} \`${id}\` — ${styleLine(id)}`);

      await send(
        `🎒 *— DEINE ATEM-SAMMLUNG —* 🎒\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${lines.join('\n')}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `✅ = aktuell ausgerüstet | Wechseln: ${P}atemausrüsten <id>`
      );
      return true;
    }

    if (cmd === 'atemausrüsten' || cmd === 'equipbreathing') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      const wanted = (args[0] || '').toLowerCase();

      if (!wanted) {
        const owned = Object.keys(player.styles);
        save(data);
        if (!owned.length) {
          await send(`❌ Du besitzt noch keinen Atemstil. Nutze ${P}atemzug oder ${P}atemshop.`);
          return true;
        }
        const lines = owned.map(id => `• \`${id}\` — ${styleLine(id)}${player.activeStyle === id ? ' ✅ (aktiv)' : ''}`);
        await send(`🧘 *Nutzung:* ${P}atemausruesten <id>\n\nDeine Stile:\n${lines.join('\n')}`);
        return true;
      }

      if (!BREATHING_STYLES[wanted]) {
        save(data);
        await send(`❌ Unbekannter Atemstil "${wanted}". Nutze ${P}atemliste.`);
        return true;
      }
      if (!player.styles[wanted]) {
        save(data);
        await send(`❌ Du besitzt ${styleLine(wanted)} noch nicht.\nKaufen: ${P}atemkaufen ${wanted} | Gacha: ${P}atemzug`);
        return true;
      }

      player.activeStyle = wanted;
      save(data);
      await send(`✅ Du kämpfst jetzt mit ${styleLine(wanted)}!`);
      return true;
    }

    if (cmd === 'atemstil' || cmd === 'mybreathing') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      save(data);

      if (!player.activeStyle) {
        await send(`❌ Du hast noch keinen aktiven Atemstil. Nutze ${P}atemzug (${GACHA_COST} Coins) oder ${P}atemshop.`);
        return true;
      }
      const style = BREATHING_STYLES[player.activeStyle];
      const ownedCount = Object.keys(player.styles).length;
      await send(
        `🧘 *Dein aktiver Atemstil*\n` +
        `${style.name}${style.secret ? `\n"${style.trueName}"` : ''}\n` +
        `${RARITY_LABEL[style.rarity]} | Kraft: ${style.power}\n\n` +
        `📦 Du besitzt insgesamt ${ownedCount} Stil(e). Nutze ${P}atemsammlung für die volle Liste.`
      );
      return true;
    }

    if (cmd === 'dsrang' || cmd === 'dsrank') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      save(data);
      const rankName = RANKS[player.rankIndex];
      const nextIdx = player.rankIndex + 1;
      const nextInfo = nextIdx < RANKS.length
        ? `Nächster Rang: ${RANKS[nextIdx]} (benötigt ${RANK_XP_REQUIRED(nextIdx)} XP, du hast ${player.xp})`
        : `👑 Du hast den höchsten Rang erreicht: Hashira!`;
      await send(
        `🏅 *Dämonentöter-Rang*\n` +
        `Aktueller Rang: *${rankName}*${rankName === 'Hashira' ? ' 👑' : ''}\n` +
        `XP: ${player.xp} | Siege: ${player.wins}\n` +
        `${nextInfo}`
      );
      return true;
    }

    if (cmd === 'dsaufstieg' || cmd === 'dspromote') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      const nextIdx = player.rankIndex + 1;

      if (nextIdx >= RANKS.length) {
        save(data);
        await send('👑 Du bist bereits Hashira — der höchste Rang des Korps. Es gibt nichts mehr zu erklimmen.');
        return true;
      }

      const required = RANK_XP_REQUIRED(nextIdx);
      if (player.xp < required) {
        save(data);
        await send(`❌ Noch nicht genug XP. Benötigt: ${required}, du hast: ${player.xp}.\nBekämpfe den Dämon mit ${P}demonboss, um XP zu sammeln.`);
        return true;
      }

      player.rankIndex = nextIdx;
      save(data);
      const newRank = RANKS[nextIdx];
      await send(
        `🎉 *AUFSTIEG!* 🎉\n` +
        `@${senderJid.split('@')[0]} ist jetzt Rang *${newRank}*${newRank === 'Hashira' ? ' 👑' : ''}!`,
        { mentions: [senderJid] }
      );
      return true;
    }

    if (cmd === 'demonboss' || cmd === 'dämonenboss' || cmd === 'daemonboss') {
      const data = load();
      const player = ensurePlayer(data, senderJid);

      if (!player.activeStyle) {
        save(data);
        await send(`❌ Du brauchst zuerst einen ausgerüsteten Atemstil! Nutze ${P}atemzug, ${P}atemshop oder ${P}atemausruesten.`);
        return true;
      }

      const now = Date.now();
      if (now - (player.lastBoss || 0) < BOSS_COOLDOWN_MS) {
        save(data);
        const remaining = Math.ceil((BOSS_COOLDOWN_MS - (now - player.lastBoss)) / 1000);
        const min = Math.floor(remaining / 60), sec = remaining % 60;
        await send(`⏰ Du musst dich noch ${min}:${sec.toString().padStart(2, '0')} min erholen, bevor du erneut angreifst.`);
        return true;
      }

      const boss = spawnBossIfNeeded(data);
      const style = BREATHING_STYLES[player.activeStyle];
      const rankMultiplier = 1 + (player.rankIndex * 0.08);
      const baseDamage = style.power * rankMultiplier;
      const variance = randInt(80, 130) / 100;
      const critChance = style.secret ? 0.25 : 0.1;
      const isCrit = Math.random() < critChance;
      let damage = Math.round(baseDamage * variance * (isCrit ? 2 : 1));

      boss.hp = Math.max(0, boss.hp - damage);
      player.lastBoss = now;
      player.xp += randInt(5, 15);

      let resultText =
        `👹 *— ${boss.name} —* 👹\n` +
        `${healthBar(boss.hp, boss.maxHp)}\n` +
        `${boss.hp} / ${boss.maxHp} HP\n\n` +
        `${isCrit ? '💥 *KRITISCHER TREFFER!* ' : ''}Mit ${style.name} fügst du ${damage} Schaden zu!`;

      if (boss.hp <= 0) {
        const xpReward = randInt(80, 200);
        const coinReward = randInt(200, 600);
        player.xp += xpReward;
        player.wins += 1;
        ctx.ensureUser(senderJid);
        ctx.users[senderJid].coins = (ctx.users[senderJid].coins || 0) + coinReward;
        ctx.save(ctx.FILES.users, ctx.users);

        resultText +=
          `\n\n☠️ *DER DÄMON WURDE BESIEGT!* ☠️\n` +
          `+${xpReward} XP, +${coinReward} Coins für @${senderJid.split('@')[0]}!\n` +
          `Ein neuer Dämon wird bald erscheinen...`;
        data.boss = null;
      }

      save(data);
      await send(resultText, boss.hp <= 0 ? { mentions: [senderJid] } : {});
      return true;
    }

    return false;
  }

  const DS_COMMANDS = [
    'dshelp', 'demonslayerhelp',
    'atemzug', 'learnbreathing',
    'atemshop', 'breathingshop',
    'atemkaufen', 'buybreathing',
    'atemschenken', 'giftbreathing',
    'atemsammlung', 'mybreathings', 'breathingcollection',
    'atemausruesten', 'equipbreathing',
    'atemstil', 'mybreathing',
    'atemliste', 'breathinglist',
    'demonboss', 'dämonenboss', 'demonboss',
    'dsrang', 'dsrank',
    'dsaufstieg', 'dspromote'
  ];

  const DS_HELP_TEXT =
    `👹 *Dämonentöter-System*\n` +
    `{P}atemzug — zufälligen Atemstil erlernen | {P}atemshop — gezielt kaufen\n` +
    `{P}atemausrüsten — Kampf-Stil wechseln | {P}atemschenken — verschenken\n` +
    `{P}demonboss — Dämon angreifen | {P}dsrang — Rang | {P}dsaufstieg — aufsteigen`;

  return { handle, DS_COMMANDS, DS_HELP_TEXT };
}