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
      await send(
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
      return true;
    }

    // ---- ATEMLISTE ----
    if (cmd === 'atemliste' || cmd === 'breathinglist') {
      const lines = Object.values(BREATHING_STYLES)
        .sort((a, b) => a.power - b.power)
        .map(s => `${s.secret ? '❓' : '•'} ${s.secret ? '???' : s.name} ${RARITY_LABEL[s.rarity]}`);
      await send(`📜 *— BEKANNTE ATEMSTILE —* 📜\n\n${lines.join('\n')}\n\n_Manche Stile sind so selten, dass sie als Gerücht gelten..._`);
      return true;
    }

    // ---- ATEMZUG (Gacha: Stil erlernen) ----
    if (cmd === 'atemzug' || cmd === 'learnbreathing') {
      const data = load();
      const player = ensurePlayer(data, senderJid);

      if (player.style) {
        await send(
          `⚔️ Du beherrschst bereits ${styleLine(player.style)}.\n` +
          `Ein Schwertkämpfer kann nur einen Atemstil wahrhaft meistern.`
        );
        return true;
      }

      const coins = ctx.users[senderJid]?.coins || 0;
      if (coins < GACHA_COST) {
        await send(`❌ Du brauchst ${GACHA_COST} Coins, um einen Atemstil zu erlernen. (Du hast: ${coins})`);
        return true;
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

      await send(
        `${header}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${sty