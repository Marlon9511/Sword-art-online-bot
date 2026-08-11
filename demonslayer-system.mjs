import fs from 'fs';
import path from 'path';

// ============================================================
// DEMON SLAYER SYSTEM
// Eigenständiges Modul, speichert seine Daten in data/demonslayer.json
// Folgt dem gleichen Aufruf-Stil wie arena-system.mjs / guildboss-event.mjs:
//   const demonSlayer = createDemonSlayerSystem(DATA_PATH);
//   const handled = await demonSlayer.handle({ cmd, args, sender, ... });
// ============================================================

// ---- ATEMSTILE (Breathing Styles) ----
// rarity-Gewichte: je niedriger "weight", desto seltener.
// Sonnen- & Mondatmung sind bewusst extrem selten für normale Spieler,
// und nur spürbar (aber immer noch klein) höher für den Owner.
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

  // ---- SECRET / SEHR SELTEN ----
  // normale Nutzer: weight 0.05 (bei Gesamtgewicht ~150 → ca. 1:3000 Chance)
  // Owner/Co-Owner: weight 3 (deutlich, aber immer noch klein → ca. 1:50 Chance)
  sonne:     { name: '☀️ Sonnenatmung',     trueName: 'Atmung der Sonne — Hinokami Kagura', rarity: 'legendary', power: 100, weight: 0.05, ownerWeight: 3, secret: true },
  mond:      { name: '🌙 Mondatmung',       trueName: 'Atmung des Mondes', rarity: 'legendary', power: 95,  weight: 0.05, ownerWeight: 3, secret: true }
};

const RARITY_LABEL = {
  common: '⚪ Gewöhnlich',
  uncommon: '🟢 Ungewöhnlich',
  rare: '🔵 Selten',
  legendary: '🟡 Legendär'
};

// ---- RANG-SYSTEM (Dämonentöter-Korps-Ränge) ----
const RANKS = [
  'Mizunoto', 'Mizunoe', 'Kanoto', 'Kanoe', 'Tsuchinoto',
  'Tsuchinoe', 'Hinoto', 'Hinoe', 'Kinoto', 'Kinoe', 'Hashira'
];
// XP-Schwelle, um in den jeweiligen Rang (Index) aufzusteigen
const RANK_XP_REQUIRED = (idx) => idx === 0 ? 0 : idx * 150 + (idx >= RANKS.length - 1 ? 500 : 0);

const GACHA_COST = 300; // Coins pro Atemzug-Versuch
const BOSS_COOLDOWN_MS = 5 * 60 * 1000; // 5 Minuten pro Spieler
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
        style: null,
        rankIndex: 0,
        xp: 0,
        wins: 0,
        lastBoss: 0
      };
    }
    return data.players[jid];
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
    if (!s) return '— (kein Atemstil)';
    return `${s.name} ${RARITY_LABEL[s.rarity] || ''}`;
  }

  async function handle(ctx) {
    const {
      cmd, args, sender, from, isGroup, activePrefix, send,
      normalizeJid, isSameJid, getNumberMention, randInt, isPrimaryOwner
    } = ctx;

    const P = activePrefix;
    const senderJid = normalizeJid(sender);

    // ---- HILFE ----
    if (cmd === 'dshelp' || cmd === 'demonslayerhelp') {
      return send(
        `👹 *— DÄMONENTÖTER-SYSTEM —* 👹\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${P}atemzug — Atemstil erlernen (${GACHA_COST} Coins)\n` +
        `${P}atemstil — deinen aktuellen Atemstil anzeigen\n` +
        `${P}atemliste — alle bekannten Atemstile anzeigen\n` +
        `${P}daemonboss — den Dämon angreifen\n` +
        `${P}dsrang — deinen Korps-Rang anzeigen\n` +
        `${P}dsaufstieg — versuchen, im Rang aufzusteigen\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `☀️🌙 Sonnen- und Mondatmung sind extrem selten — nur wahre Legenden erlernen sie.`
      );
    }

    // ---- ATEMLISTE ----
    if (cmd === 'atemliste' || cmd === 'breathinglist') {
      const lines = Object.values(BREATHING_STYLES)
        .sort((a, b) => a.power - b.power)
        .map(s => `${s.secret ? '❓' : '•'} ${s.secret ? '???' : s.name} ${RARITY_LABEL[s.rarity]}`);
      return send(`📜 *— BEKANNTE ATEMSTILE —* 📜\n\n${lines.join('\n')}\n\n_Manche Stile sind so selten, dass sie als Gerücht gelten..._`);
    }

    // ---- ATEMZUG (Gacha: Stil erlernen) ----
    if (cmd === 'atemzug' || cmd === 'learnbreathing') {
      const data = load();
      const player = ensurePlayer(data, senderJid);

      if (player.style) {
        return send(
          `⚔️ Du beherrschst bereits ${styleLine(player.style)}.\n` +
          `Ein Schwertkämpfer kann nur einen Atemstil wahrhaft meistern.`
        );
      }

      const coins = ctx.users[senderJid]?.coins || 0;
      if (coins < GACHA_COST) {
        return send(`❌ Du brauchst ${GACHA_COST} Coins, um einen Atemstil zu erlernen. (Du hast: ${coins})`);
      }

      ctx.users[senderJid].coins -= GACHA_COST;
      ctx.save(ctx.FILES.users, ctx.users);

      const ownerLike = isPrimaryOwner(senderJid);
      const styleId = rollStyle(ownerLike);
      const style = BREATHING_STYLES[styleId];
      player.style = styleId;
      save(data);

      const isSecret = !!style.secret;
      const header = isSecret
        ? `🌟🌟🌟 *— LEGENDÄRES ERWACHEN —* 🌟🌟🌟`
        : `⚔️ *— ATEM ERLERNT —* ⚔️`;

      return send(
        `${header}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${style.name}${isSecret ? `\n"${style.trueName}"` : ''}\n` +
        `${RARITY_LABEL[style.rarity]} | Kraft: ${style.power}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        (isSecret
          ? `Ein Licht durchbricht den Nebel von Aincrad... du hast Geschichte geschrieben. 🔥`
          : `Trainiere fleißig und stelle dich dem Dämon mit ${P}daemonboss!`)
      );
    }

    // ---- EIGENER ATEMSTIL ----
    if (cmd === 'atemstil' || cmd === 'mybreathing') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      save(data);
      if (!player.style) {
        return send(`❌ Du hast noch keinen Atemstil erlernt. Nutze ${P}atemzug (${GACHA_COST} Coins).`);
      }
      const style = BREATHING_STYLES[player.style];
      return send(
        `🧘 *Dein Atemstil*\n` +
        `${style.name}${style.secret ? `\n"${style.trueName}"` : ''}\n` +
        `${RARITY_LABEL[style.rarity]} | Kraft: ${style.power}`
      );
    }

    // ---- RANG ANZEIGEN ----
    if (cmd === 'dsrang' || cmd === 'dsrank') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      save(data);
      const rankName = RANKS[player.rankIndex];
      const nextIdx = player.rankIndex + 1;
      const nextInfo = nextIdx < RANKS.length
        ? `Nächster Rang: ${RANKS[nextIdx]} (benötigt ${RANK_XP_REQUIRED(nextIdx)} XP, du hast ${player.xp})`
        : `👑 Du hast den höchsten Rang erreicht: Hashira!`;
      return send(
        `🏅 *Dämonentöter-Rang*\n` +
        `Aktueller Rang: *${rankName}*${rankName === 'Hashira' ? ' 👑' : ''}\n` +
        `XP: ${player.xp} | Siege: ${player.wins}\n` +
        `${nextInfo}`
      );
    }

    // ---- RANGAUFSTIEG ----
    if (cmd === 'dsaufstieg' || cmd === 'dspromote') {
      const data = load();
      const player = ensurePlayer(data, senderJid);
      const nextIdx = player.rankIndex + 1;

      if (nextIdx >= RANKS.length) {
        save(data);
        return send('👑 Du bist bereits Hashira — der höchste Rang des Korps. Es gibt nichts mehr zu erklimmen.');
      }

      const required = RANK_XP_REQUIRED(nextIdx);
      if (player.xp < required) {
        save(data);
        return send(`❌ Noch nicht genug XP. Benötigt: ${required}, du hast: ${player.xp}.\nBekämpfe den Dämon mit ${P}daemonboss, um XP zu sammeln.`);
      }

      player.rankIndex = nextIdx;
      save(data);
      const newRank = RANKS[nextIdx];
      return send(
        `🎉 *AUFSTIEG!* 🎉\n` +
        `@${senderJid.split('@')[0]} ist jetzt Rang *${newRank}*${newRank === 'Hashira' ? ' 👑' : ''}!`,
        { mentions: [senderJid] }
      );
    }

    // ---- DÄMONEN-BOSS-KAMPF ----
    if (cmd === 'daemonboss' || cmd === 'demonboss') {
      const data = load();
      const player = ensurePlayer(data, senderJid);

      if (!player.style) {
        save(data);
        return send(`❌ Du brauchst zuerst einen Atemstil! Nutze ${P}atemzug (${GACHA_COST} Coins).`);
      }

      const now = Date.now();
      if (now - (player.lastBoss || 0) < BOSS_COOLDOWN_MS) {
        save(data);
        const remaining = Math.ceil((BOSS_COOLDOWN_MS - (now - player.lastBoss)) / 1000);
        const min = Math.floor(remaining / 60), sec = remaining % 60;
        return send(`⏰ Du musst dich noch ${min}:${sec.toString().padStart(2, '0')} min erholen, bevor du erneut angreifst.`);
      }

      const boss = spawnBossIfNeeded(data);
      const style = BREATHING_STYLES[player.style];
      const rankMultiplier = 1 + (player.rankIndex * 0.08);
      const baseDamage = style.power * rankMultiplier;
      const variance = randInt(80, 130) / 100; // 0.8x - 1.3x
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
      return send(resultText, boss.hp <= 0 ? { mentions: [senderJid] } : {});
    }

    return false;
  }

  const DS_COMMANDS = [
    'dshelp', 'demonslayerhelp', 'atemzug', 'learnbreathing', 'atemstil', 'mybreathing',
    'atemliste', 'breathinglist', 'daemonboss', 'demonboss', 'dsrang', 'dsrank',
    'dsaufstieg', 'dspromote'
  ];

  const DS_HELP_TEXT =
    `👹 *Dämonentöter-System*\n` +
    `{P}atemzug — Atemstil erlernen\n` +
    `{P}daemonboss — Dämon angreifen\n` +
    `{P}dsrang — Rang anzeigen | {P}dsaufstieg — aufsteigen`;

  return { handle, DS_COMMANDS, DS_HELP_TEXT };
}
