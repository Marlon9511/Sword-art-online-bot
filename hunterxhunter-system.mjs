import fs from 'fs';
import path from 'path';

// ─────────────────────────────────────────────────────────────
// CHARAKTERE
// ─────────────────────────────────────────────────────────────

const CHARACTERS = {
  gon: {
    id: 'gon',
    name: 'Gon Freecss',
    title: 'Der Erwachsenwerdende Hunter',
    icon: '🎣',
    element: 'Verstärkung (Enhancer)',
    baseStats: { hp: 145, chakra: 110, atk: 25, def: 16, spd: 20 },
    passive: {
      name: 'Rock-Paper-Scissors-Entschlossenheit',
      desc: 'Regeneriert am Ende jeder Runde zusätzlich 3% der maximalen HP.'
    },
    jutsu: [
      { id: 'jajanken_rock', name: 'Jajanken: Stein', cost: 25, cd: 1, dmg: [30, 40], desc: 'Ein geballter Faustschlag voller reiner Nen-Kraft.' },
      { id: 'jajanken_scissors', name: 'Jajanken: Schere', cost: 20, cd: 2, dmg: [22, 30], effect: 'debuff', debuff: { def: 0.8, rounds: 2 }, desc: 'Ein durchbohrender Stoß, der die Deckung des Gegners durchschneidet.' },
      { id: 'jajanken_paper', name: 'Jajanken: Papier', cost: 15, cd: 2, effect: 'buff', buff: { spd: 1.25, rounds: 2 }, dmg: [0, 0], desc: 'Eine ausweichende Bewegung erhöht dein Tempo.' },
      { id: 'first_come', name: 'First Come', cost: 55, cd: 5, dmg: [65, 85], desc: 'Vorangekündigt, aber unaufhaltsam — reine Nen-Wucht.' }
    ],
    awakening: { name: 'Erwachsener Gon', unlockLevel: 15, statMult: 1.6, desc: 'Gon opfert seine Zukunft für explosive Kraft im Hier und Jetzt.' }
  },

  killua: {
    id: 'killua',
    name: 'Killua Zoldyck',
    title: 'Der Attentäter-Prodigy',
    icon: '⚡',
    element: 'Transformation (Transmuter)',
    baseStats: { hp: 125, chakra: 120, atk: 24, def: 14, spd: 28 },
    passive: {
      name: 'Godspeed-Reflexe',
      desc: '22% Chance, dank überragender Geschwindigkeit einem Angriff vollständig auszuweichen.'
    },
    jutsu: [
      { id: 'lightning_palm', name: 'Blitz-Handfläche', cost: 22, cd: 1, dmg: [26, 34], desc: 'Elektrifizierte Nen-Energie schießt durch die Handfläche.' },
      { id: 'godspeed', name: 'Godspeed', cost: 35, cd: 3, effect: 'buff', buff: { spd: 1.4, atk: 1.15, rounds: 3 }, dmg: [15, 22], desc: 'Extreme Beschleunigung durch Transmutation von Elektrizität in Geschwindigkeit.' },
      { id: 'thunderbolt', name: 'Donnerschlag', cost: 40, cd: 3, dmg: [45, 58], effect: 'stun', stunRounds: 1, desc: 'Ein geballter Elektroschock lähmt das Nervensystem des Gegners.' },
      { id: 'assassin_strike', name: 'Attentäter-Schlag', cost: 18, cd: 1, dmg: [20, 28], effect: 'multi2', desc: 'Zwei blitzschnelle, tödliche Präzisionsschläge.' }
    ],
    awakening: { name: 'Godspeed: Grenzenlose Beschleunigung', unlockLevel: 15, statMult: 1.55, desc: 'Killuas Bewegungen werden für das bloße Auge unsichtbar.' }
  },

  kurapika: {
    id: 'kurapika',
    name: 'Kurapika',
    title: 'Der letzte Kurta',
    icon: '⛓️',
    element: 'Spezialist (Ketten-Nen)',
    baseStats: { hp: 130, chakra: 135, atk: 22, def: 18, spd: 19 },
    passive: {
      name: 'Scharlachrote Augen',
      desc: 'Gegen Ziele, die mit den Phantomtruppe-Themen (secret/boss) verknüpft sind: dauerhaft +40% Angriff (Emperor Time-artig).'
    },
    jutsu: [
      { id: 'chain_jail', name: 'Chain Jail', cost: 30, cd: 3, dmg: [20, 28], effect: 'stun', stunRounds: 2, desc: 'Fesselt den Gegner unentrinnbar in Ketten — absolute Bewegungsunfähigkeit.' },
      { id: 'judgment_chain', name: 'Judgment Chain', cost: 45, cd: 4, dmg: [50, 65], desc: 'Die tödlichste Kette — nur gegen wahre Verbrecher voll einsetzbar, verheerender Schaden.' },
      { id: 'dowsing_chain', name: 'Dowsing Chain', cost: 20, cd: 2, effect: 'buff_heal', buff: { def: 1.3, rounds: 2 }, heal: [10, 18], desc: 'Ortungskette schützt und heilt leicht.' },
      { id: 'chain_strike', name: 'Kettenschlag', cost: 24, cd: 1, dmg: [26, 34], desc: 'Ein peitschender Schlag mit der stählernen Kette.' }
    ],
    awakening: { name: 'Emperor Time', unlockLevel: 15, statMult: 1.6, desc: 'Alle sechs Nen-Kategorien gleichzeitig nutzbar — grenzenlose taktische Freiheit.' }
  },

  hisoka: {
    id: 'hisoka',
    name: 'Hisoka Morow',
    title: 'Der Magier',
    icon: '🃏',
    element: 'Transformation (Bungee Gum)',
    baseStats: { hp: 140, chakra: 130, atk: 27, def: 15, spd: 22 },
    passive: {
      name: 'Bungee Gum: Elastizität & Klebrigkeit',
      desc: '15% Chance, den Schaden eines gegnerischen Angriffs zu halbieren, indem er ihn "zurückdehnt".'
    },
    jutsu: [
      { id: 'bungee_gum', name: 'Bungee Gum', cost: 25, cd: 2, dmg: [28, 38], effect: 'debuff', debuff: { spd: 0.75, rounds: 2 }, desc: 'Klebrige, dehnbare Nen-Fäden binden und verlangsamen den Gegner.' },
      { id: 'texture_surprise', name: 'Texture Surprise', cost: 22, cd: 2, dmg: [24, 32], desc: 'Tarnt einen Angriff, der den Gegner völlig überrascht.' },
      { id: 'bloody_dance', name: 'Bloody Dance', cost: 40, cd: 4, dmg: [45, 60], effect: 'burn', burn: { dmg: [10, 16], rounds: 2 }, desc: 'Ein spielerischer, aber tödlicher Kartentanz reißt tiefe Wunden.' },
      { id: 'card_throw', name: 'Kartenwurf', cost: 15, cd: 1, dmg: [18, 26], desc: 'Scharfe, nen-verstärkte Spielkarten fliegen mit tödlicher Präzision.' }
    ],
    awakening: { name: 'Doppelte Bungee Gum', unlockLevel: 15, statMult: 1.5, desc: 'Hisoka setzt sein volles Potential frei — verspielt und tödlich zugleich.' }
  },

  chrollo: {
    id: 'chrollo',
    name: 'Chrollo Lucilfer',
    title: 'Anführer der Phantomtruppe',
    icon: '📖',
    element: 'Spezialist (Skill Hunter)',
    baseStats: { hp: 135, chakra: 145, atk: 23, def: 17, spd: 18 },
    passive: {
      name: 'Bandenbuch: Gestohlene Fähigkeiten',
      desc: '15% Chance, eine gegnerische Fähigkeit kurzzeitig zu kopieren und gegen ihn einzusetzen (Bonusschaden).'
    },
    jutsu: [
      { id: 'skill_hunter', name: 'Skill Hunter', cost: 20, cd: 2, dmg: [22, 30], effect: 'debuff', debuff: { atk: 0.8, rounds: 2 }, desc: 'Stiehlt dem Gegner einen Teil seiner Kampfkraft.' },
      { id: 'black_voice', name: 'Black Voice', cost: 35, cd: 3, dmg: [38, 50], effect: 'stun', stunRounds: 1, desc: 'Beschwört eine gestohlene Puppen-Fähigkeit, die den Gegner betäubt.' },
      { id: 'bandits_secret', name: 'Bandit\'s Secret', cost: 30, cd: 3, effect: 'buff_all', buff: { atk: 1.2, def: 1.2, spd: 1.1, rounds: 3 }, desc: 'Nutzt eine gestohlene Verstärkungs-Fähigkeit an sich selbst.' },
      { id: 'reader', name: 'Der Vorleser', cost: 25, cd: 2, dmg: [26, 36], desc: 'Ruhige, präzise Angriffe, gestützt auf jahrelange gestohlene Erfahrung.' }
    ],
    awakening: { name: 'Vollständiges Bandenbuch', unlockLevel: 15, statMult: 1.55, desc: 'Zugriff auf hunderte gestohlener Fähigkeiten gleichzeitig.' }
  },

  netero: {
    id: 'netero',
    name: 'Isaac Netero',
    title: 'Vorsitzender der Hunter-Vereinigung',
    icon: '🙏',
    element: 'Verstärkung (Kampfmönch)',
    baseStats: { hp: 160, chakra: 125, atk: 28, def: 22, spd: 17 },
    passive: {
      name: '100-Typ Gebets-Bodhisattva',
      desc: 'Permanent +5% auf alle Statuswerte durch jahrzehntelange Nen-Meisterschaft.'
    },
    jutsu: [
      { id: 'hundred_hands', name: 'Hundert-Hand-Beben', cost: 25, cd: 1, dmg: [28, 38], effect: 'multi2', desc: 'Zwei blitzschnelle Handflächenschläge im Zen-Stil.' },
      { id: 'palm_of_bodhisattva', name: 'Handfläche des Bodhisattva', cost: 40, cd: 3, dmg: [45, 58], desc: 'Ein gewaltiger Handflächenschlag mit spirituellem Nachdruck.' },
      { id: 'namu_amida', name: 'Namu Amida Butsu', cost: 65, cd: 5, dmg: [70, 90], desc: 'Die vernichtende Attacke des 100-Typ Gebets-Bodhisattva.' },
      { id: 'meditative_stance', name: 'Meditative Haltung', cost: 20, cd: 2, effect: 'buff_heal', buff: { def: 1.35, rounds: 2 }, heal: [15, 24], desc: 'Sammelt Nen in ruhiger Meditation und heilt leicht.' }
    ],
    awakening: { name: '100-Typ: Gebets-Bodhisattva erscheint', unlockLevel: 15, statMult: 1.65, desc: 'Ein gewaltiger, buddhaähnlicher Nen-Avatar manifestiert sich hinter Netero.' }
  }
};

const CHAR_LIST = Object.values(CHARACTERS);
const CHAR_ALIASES = {
  gon: 'gon', freecss: 'gon',
  killua: 'killua', zoldyck: 'killua',
  kurapika: 'kurapika', kurta: 'kurapika',
  hisoka: 'hisoka', morow: 'hisoka',
  chrollo: 'chrollo', lucilfer: 'chrollo',
  netero: 'netero', isaac: 'netero'
};

// ─────────────────────────────────────────────────────────────
// GEGNER-POOL (PvE — Chimera-Ameisen & Wilde Tiere)
// ─────────────────────────────────────────────────────────────

const ENEMY_POOL = [
  { name: 'Wildes Vieh im Greed Island', icon: '🐗', tier: 1, hp: 60, atk: 10, def: 5, spd: 8, xp: 15, coins: [20, 40] },
  { name: 'Söldner-Hunter', icon: '🗡️', tier: 1, hp: 70, atk: 12, def: 6, spd: 10, xp: 18, coins: [25, 45] },
  { name: 'Chimera-Ameisen-Soldat', icon: '🐜', tier: 2, hp: 100, atk: 16, def: 10, spd: 9, xp: 28, coins: [40, 70] },
  { name: 'Verräterischer Nen-Nutzer', icon: '🔮', tier: 2, hp: 110, atk: 18, def: 8, spd: 12, xp: 32, coins: [45, 75] },
  { name: 'Chimera-Ameisen-Offizier', icon: '🦂', tier: 3, hp: 140, atk: 22, def: 12, spd: 14, xp: 45, coins: [60, 100] },
  { name: 'Spinnenglied der Phantomtruppe', icon: '🕷️', tier: 3, hp: 160, atk: 24, def: 10, spd: 11, xp: 50, coins: [65, 110] },
  { name: 'Königsgardist', icon: '👑', tier: 4, hp: 180, atk: 26, def: 14, spd: 15, xp: 65, coins: [80, 130] },
  { name: 'Chimera-Ameisen-König (Fragment)', icon: '👁️‍🗨️', tier: 5, hp: 260, atk: 32, def: 18, spd: 16, xp: 100, coins: [120, 200], boss: true }
];

// ─────────────────────────────────────────────────────────────
// PERSISTENZ
// ─────────────────────────────────────────────────────────────

export function createHunterSystem(DATA_PATH) {
  const FILE_PATH = path.join(DATA_PATH, 'hunterxhunter.json');

  function loadData() {
    try {
      if (!fs.existsSync(FILE_PATH)) {
        fs.writeFileSync(FILE_PATH, JSON.stringify({}, null, 2));
        return {};
      }
      const raw = fs.readFileSync(FILE_PATH, 'utf-8');
      return raw.trim() ? JSON.parse(raw) : {};
    } catch (e) {
      console.error('[hunterxhunter] Fehler beim Laden:', e);
      return {};
    }
  }

  function saveData() {
    try {
      fs.writeFileSync(FILE_PATH, JSON.stringify(hxhData, null, 2));
    } catch (e) {
      console.error('[hunterxhunter] Fehler beim Speichern:', e);
    }
  }

  let hxhData = loadData();
  const activeBattles = new Map();

  function ensureHxhUser(jid) {
    if (!hxhData[jid]) {
      hxhData[jid] = {
        main: null,
        partner: null,
        level: 1,
        xp: 0,
        wins: 0,
        losses: 0,
        missionsCompleted: 0,
        awakened: false
      };
      saveData();
    }
    return hxhData[jid];
  }

  function xpNeeded(level) {
    return 60 + level * 40;
  }

  function addHxhXp(jid, amount) {
    const nu = ensureHxhUser(jid);
    nu.xp += amount;
    let leveledUp = false;
    while (nu.xp >= xpNeeded(nu.level)) {
      nu.xp -= xpNeeded(nu.level);
      nu.level++;
      leveledUp = true;
    }
    saveData();
    return leveledUp;
  }

  function getCharacter(charId) {
    return CHARACTERS[charId] || null;
  }

  function currentStats(jid) {
    const nu = ensureHxhUser(jid);
    const char = getCharacter(nu.main);
    if (!char) return null;
    const isAwakened = nu.awakened && nu.level >= char.awakening.unlockLevel;
    const mult = isAwakened ? char.awakening.statMult : 1;
    const growth = 1 + (nu.level - 1) * 0.04;
    return {
      hp: Math.round(char.baseStats.hp * growth * mult),
      chakra: Math.round(char.baseStats.chakra * growth * mult),
      atk: Math.round(char.baseStats.atk * growth * mult),
      def: Math.round(char.baseStats.def * growth * mult),
      spd: Math.round(char.baseStats.spd * growth * mult),
      isAwakened
    };
  }

  // ─────────────────────────────────────────────────────────
  // KAMPF-ENGINE
  // ─────────────────────────────────────────────────────────

  function hpBar(cur, max, len = 12) {
    const ratio = Math.max(0, Math.min(1, cur / max));
    const filled = Math.round(ratio * len);
    return '🟩'.repeat(filled) + '⬛'.repeat(len - filled) + ` ${Math.max(0, Math.round(cur))}/${max}`;
  }

  function computeDamage(range, atk, def, randInt) {
    let raw = randInt(range[0], range[1]);
    raw += Math.floor(atk * 0.4) - Math.floor(def * 0.25);
    raw = Math.max(4, raw);
    const varyPct = (Math.random() * 0.3) - 0.15;
    return Math.max(3, Math.round(raw * (1 + varyPct)));
  }

  function buffMult(buffs, stat) {
    let mult = 1;
    for (const b of buffs) {
      if (b.stat === stat) mult *= b.mult;
    }
    return mult;
  }

  function tickBuffs(buffs) {
    for (const b of buffs) b.rounds--;
    return buffs.filter(b => b.rounds > 0);
  }

  function createBattleFighter(stats) {
    return {
      hp: stats.hp, hpMax: stats.hp,
      chakra: stats.chakra, chakraMax: stats.chakra,
      atk: stats.atk, def: stats.def, spd: stats.spd,
      buffs: [], stunned: false, cooldowns: {}, burn: null,
      lastAttackDmg: 0
    };
  }

  function startMissionBattle(jid, char, stats, enemyTemplate) {
    const battle = {
      type: 'mission',
      char,
      player: createBattleFighter(stats),
      enemy: {
        ...enemyTemplate,
        hp: enemyTemplate.hp, hpMax: enemyTemplate.hp,
        buffs: [], stunned: false, lastAttackDmg: 0
      },
      round: 1
    };
    activeBattles.set(jid, battle);
    return battle;
  }

  function pickEnemyForLevel(level, randInt) {
    const tier = Math.min(5, Math.max(1, Math.ceil(level / 4)));
    let pool = ENEMY_POOL.filter(e => e.tier === tier);
    if (!pool.length) pool = ENEMY_POOL.filter(e => e.tier === Math.min(5, tier));
    const base = pool[randInt(0, pool.length - 1)];
    const scale = 1 + (level - 1) * 0.06;
    return {
      ...base,
      hp: Math.round(base.hp * scale),
      atk: Math.round(base.atk * scale),
      def: Math.round(base.def * scale),
      spd: Math.round(base.spd * scale)
    };
  }

  function playerJutsuAction(battle, jutsu, randInt) {
    const p = battle.player, e = battle.enemy;
    const lines = [];
    const atkVal = p.atk * buffMult(p.buffs, 'atk');

    if (jutsu.effect === 'multi2' || jutsu.effect === 'multi3') {
      const hits = jutsu.effect === 'multi3' ? 3 : 2;
      let total = 0;
      for (let i = 0; i < hits; i++) {
        total += Math.round(computeDamage(jutsu.dmg, atkVal, e.def, randInt) / hits * 1.4);
      }
      e.hp -= total;
      lines.push(`${jutsu.name}: ${hits} Treffer für insgesamt ${total} Schaden!`);
    } else if (jutsu.effect === 'stun') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      e.stunned = true; e.stunRoundsLeft = jutsu.stunRounds;
      lines.push(`${jutsu.name} trifft für ${d} Schaden und betäubt den Gegner!`);
    } else if (jutsu.effect === 'burn') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      e.burn = { dmg: jutsu.burn.dmg, rounds: jutsu.burn.rounds };
      lines.push(`${jutsu.name} trifft für ${d} Schaden und reißt anhaltende Wunden!`);
    } else if (jutsu.effect === 'debuff') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      const [stat, val] = Object.entries(jutsu.debuff).filter(([k]) => k !== 'rounds')[0];
      e.buffs.push({ stat, mult: val, rounds: jutsu.debuff.rounds });
      lines.push(`${jutsu.name} trifft für ${d} Schaden und schwächt den Gegner!`);
    } else if (jutsu.effect === 'buff' || jutsu.effect === 'buff_all') {
      const d = jutsu.dmg ? computeDamage(jutsu.dmg, atkVal, e.def, randInt) : 0;
      if (d > 0) e.hp -= d;
      for (const [stat, val] of Object.entries(jutsu.buff)) {
        if (stat === 'rounds') continue;
        p.buffs.push({ stat, mult: val, rounds: jutsu.buff.rounds });
      }
      lines.push(`${jutsu.name} stärkt dich${d > 0 ? ` und verursacht ${d} Schaden` : ''}!`);
    } else if (jutsu.effect === 'buff_heal') {
      const heal = randInt(jutsu.heal[0], jutsu.heal[1]);
      p.hp = Math.min(p.hpMax, p.hp + heal);
      for (const [stat, val] of Object.entries(jutsu.buff)) {
        if (stat === 'rounds') continue;
        p.buffs.push({ stat, mult: val, rounds: jutsu.buff.rounds });
      }
      lines.push(`${jutsu.name} heilt dich um ${heal} HP und stärkt deine Verteidigung!`);
    } else {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      lines.push(`${jutsu.name} trifft für ${d} Schaden!`);
    }

    return lines.join(' ');
  }

  function enemyAction(battle, randInt) {
    const p = battle.player, e = battle.enemy;
    if (e.stunned) {
      e.stunRoundsLeft = (e.stunRoundsLeft || 1) - 1;
      if (e.stunRoundsLeft <= 0) e.stunned = false;
      return `${e.icon || '👤'} ${e.name} ist betäubt und kann nicht handeln!`;
    }
    const useSpecial = e.tier >= 3 && Math.random() < 0.3;
    const eAtk = e.atk * buffMult(e.buffs, 'atk');
    const dmg = Math.max(3, Math.round((useSpecial ? eAtk * 1.5 : eAtk) - p.def * 0.3 + randInt(-4, 6)));
    p.hp -= dmg;
    p.lastAttackDmg = dmg;
    return useSpecial
      ? `${e.icon || '👤'} ${e.name} setzt eine Spezialattacke ein – ${dmg} Schaden!`
      : `${e.icon || '👤'} ${e.name} greift an – ${dmg} Schaden!`;
  }

  function endOfRoundEffects(battle, char) {
    const p = battle.player, e = battle.enemy;
    const lines = [];
    if (char.id === 'gon') {
      const regen = Math.round(p.hpMax * 0.03);
      p.hp = Math.min(p.hpMax, p.hp + regen);
      lines.push(`💚 Entschlossenheit heilt dich um ${regen} HP.`);
    }
    if (e.burn) {
      const bd = Math.round((e.burn.dmg[0] + e.burn.dmg[1]) / 2);
      e.hp -= bd;
      e.burn.rounds--;
      lines.push(`🩸 Anhaltende Wunde fügt dem Gegner ${bd} Schaden zu.`);
      if (e.burn.rounds <= 0) e.burn = null;
    }
    p.buffs = tickBuffs(p.buffs);
    e.buffs = tickBuffs(e.buffs);
    for (const k of Object.keys(p.cooldowns)) {
      if (p.cooldowns[k] > 0) p.cooldowns[k]--;
    }
    battle.round++;
    return lines;
  }

  function battleStatusText(battle) {
    const char = battle.char;
    const p = battle.player, e = battle.enemy;
    return (
      `⚔️ *${char.icon} ${char.name}* vs *${e.icon || '👤'} ${e.name}*\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
      `Du:     ${hpBar(p.hp, p.hpMax)}\n` +
      `Nen:    ${p.chakra}/${p.chakraMax}\n` +
      `Gegner: ${hpBar(e.hp, e.hpMax)}\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
      `Runde ${battle.round}`
    );
  }

  async function resolveMissionOutcome(battle, jid, nu, send, users, save, FILES) {
    const p = battle.player, e = battle.enemy;
    if (e.hp <= 0) {
      activeBattles.delete(jid);
      const coins = e.coins ? Math.round((e.coins[0] + e.coins[1]) / 2 * (e.boss ? 1.5 : 1)) : 30;
      const xp = e.xp || 20;
      if (users[jid]) { users[jid].coins = (users[jid].coins || 0) + coins; save(FILES.users, users); }
      nu.missionsCompleted = (nu.missionsCompleted || 0) + 1;
      const leveledUp = addHxhXp(jid, xp);
      let out = `🎉 Sieg! Du hast *${e.name}* besiegt!\n💰 +${coins} Coins | ✨ +${xp} HxH-XP`;
      if (leveledUp) out += `\n\n📈 Level Up! Du bist jetzt HxH-Level ${nu.level}.`;
      if (!nu.awakened && nu.level >= battle.char.awakening.unlockLevel) {
        out += `\n\n🌟 *${battle.char.awakening.name}* kann jetzt mit "erwachen" freigeschaltet werden!`;
      }
      await send(out);
      return true;
    }
    if (p.hp <= 0) {
      activeBattles.delete(jid);
      nu.losses = (nu.losses || 0) + 1;
      saveData();
      await send(`💀 Niederlage! *${e.name}* war zu stark. Versuch es erneut mit "mission".`);
      return true;
    }
    return false;
  }

  // ─────────────────────────────────────────────────────────
  // TEXTBAUSTEINE
  // ─────────────────────────────────────────────────────────

  function characterListText(prefix) {
    let out = `🎯 *— HUNTER X HUNTER CHARAKTERE —* 🎯\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
    for (const c of CHAR_LIST) {
      out += `${c.icon} *${c.name}* (${c.id}) — ${c.title}\n`;
      out += `   ${c.element} | HP ${c.baseStats.hp} | Nen ${c.baseStats.chakra} | ATK ${c.baseStats.atk} | DEF ${c.baseStats.def} | SPD ${c.baseStats.spd}\n`;
      out += `   Passiv: ${c.passive.name} — ${c.passive.desc}\n\n`;
    }
    out += `Wähle deinen Hauptcharakter mit:\n${prefix}hxh wähle <name>\n`;
    out += `Danach einen Partner mit:\n${prefix}hxh partner <name>`;
    return out;
  }

  function jutsuListText(char, prefix) {
    let out = `${char.icon} *Nen-Fähigkeiten — ${char.name}*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
    for (const j of char.jutsu) {
      out += `• *${j.name}* (${prefix}hxh faehigkeit ${j.id})\n   Kosten: ${j.cost} Nen | CD: ${j.cd} Runden\n   ${j.desc}\n\n`;
    }
    out += `Passiv: *${char.passive.name}* — ${char.passive.desc}\n`;
    out += `Erwachen (ab Lv.${char.awakening.unlockLevel}): *${char.awakening.name}* — ${char.awakening.desc}`;
    return out;
  }

  const HXH_HELP_TEXT =
    `🎯 *— HUNTER X HUNTER: NEN-SYSTEM —* 🎯\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `?hxh start — Charakterübersicht\n` +
    `?hxh wähle <name> — Hauptcharakter wählen\n` +
    `?hxh partner <name> — Partnercharakter wählen\n` +
    `?hxh profil — Deine Werte anzeigen\n` +
    `?hxh faehigkeitenliste — Deine Nen-Fähigkeiten anzeigen\n` +
    `?hxh mission — PvE-Kampf starten\n` +
    `?hxh faehigkeit <id> — Nen-Fähigkeit im Kampf einsetzen\n` +
    `?hxh angriff — Normaler Angriff\n` +
    `?hxh verteidigen — Schaden reduzieren & Nen sammeln\n` +
    `?hxh flucht — Aus dem Kampf fliehen\n` +
    `?hxh erwachen — Erwachen aktivieren (ab Lv.15)\n` +
    `?hxh duell @user — PvP-Duell herausfordern\n` +
    `?hxh rangliste — Bestenliste\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `Charaktere: Gon, Killua, Kurapika, Hisoka, Chrollo, Netero`;

  const HXH_COMMANDS = ['hxh', 'hunter'];

  // ─────────────────────────────────────────────────────────
  // HAUPT-HANDLER
  // ─────────────────────────────────────────────────────────

  async function handle(ctx) {
    const {
      cmd, args, sender, m, activePrefix, send, sock,
      users, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, randInt
    } = ctx;

    if (!HXH_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    ensureUser(jid);
    const nu = ensureHxhUser(jid);
    const sub = (args[0] || '').toLowerCase();

    if (!sub || sub === 'help' || sub === 'hilfe') {
      await send(HXH_HELP_TEXT);
      return true;
    }

    if (sub === 'start' || sub === 'charaktere' || sub === 'characters') {
      await send(characterListText(activePrefix));
      return true;
    }

    if (sub === 'wähle' || sub === 'waehle' || sub === 'select') {
      const raw = (args[1] || '').toLowerCase();
      const charId = CHAR_ALIASES[raw];
      if (!charId) {
        await send(`❌ Unbekannter Charakter. Nutze ${activePrefix}hxh start für die Liste.`);
        return true;
      }
      if (nu.partner === charId) {
        await send('❌ Haupt- und Partner-Charakter dürfen nicht identisch sein.');
        return true;
      }
      nu.main = charId;
      nu.awakened = false;
      saveData();
      const char = getCharacter(charId);
      await send(`✅ *${char.icon} ${char.name}* ist jetzt dein Hauptcharakter!\n\nWähle jetzt noch einen Partner mit:\n${activePrefix}hxh partner <name>`);
      return true;
    }

    if (sub === 'partner') {
      const raw = (args[1] || '').toLowerCase();
      const charId = CHAR_ALIASES[raw];
      if (!charId) {
        await send(`❌ Unbekannter Charakter. Nutze ${activePrefix}hxh start für die Liste.`);
        return true;
      }
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Hauptcharakter mit ${activePrefix}hxh wähle <name>.`);
        return true;
      }
      if (nu.main === charId) {
        await send('❌ Haupt- und Partner-Charakter dürfen nicht identisch sein.');
        return true;
      }
      nu.partner = charId;
      saveData();
      const char = getCharacter(charId);
      await send(`✅ *${char.icon} ${char.name}* unterstützt dich jetzt als Partner!\n\nStarte deine erste Mission mit ${activePrefix}hxh mission`);
      return true;
    }

    if (sub === 'profil' || sub === 'profile') {
      if (!nu.main) {
        await send(`❌ Du hast noch keinen Charakter gewählt. Nutze ${activePrefix}hxh start.`);
        return true;
      }
      const char = getCharacter(nu.main);
      const partner = nu.partner ? getCharacter(nu.partner) : null;
      const stats = currentStats(jid);
      const nextXp = xpNeeded(nu.level);
      const out =
        `👤 *Hunter x Hunter-Profil*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${char.icon} Hauptcharakter: *${char.name}*${stats.isAwakened ? ` (${char.awakening.name} 🌟)` : ''}\n` +
        `${partner ? `${partner.icon} Partner: *${partner.name}*\n` : ''}` +
        `⭐ Level: ${nu.level} (${nu.xp}/${nextXp} XP)\n` +
        `❤️ HP: ${stats.hp} | 🔷 Nen: ${stats.chakra}\n` +
        `⚔️ ATK: ${stats.atk} | 🛡️ DEF: ${stats.def} | 💨 SPD: ${stats.spd}\n` +
        `🏆 Siege: ${nu.wins || 0} | ☠️ Niederlagen: ${nu.losses || 0}\n` +
        `📜 Missionen abgeschlossen: ${nu.missionsCompleted || 0}\n` +
        (nu.level >= char.awakening.unlockLevel && !nu.awakened
          ? `\n🌟 Erwachen verfügbar! Nutze ${activePrefix}hxh erwachen`
          : '');
      await send(out);
      return true;
    }

    if (sub === 'faehigkeitenliste' || sub === 'faehigkeitsliste' || sub === 'jutsuliste' || sub === 'skills') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}hxh start.`);
        return true;
      }
      await send(jutsuListText(getCharacter(nu.main), activePrefix));
      return true;
    }

    if (sub === 'erwachen' || sub === 'awaken') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}hxh start.`);
        return true;
      }
      const char = getCharacter(nu.main);
      if (nu.level < char.awakening.unlockLevel) {
        await send(`❌ Erwachen erfordert HxH-Level ${char.awakening.unlockLevel}. Du bist Level ${nu.level}.`);
        return true;
      }
      if (nu.awakened) {
        await send(`✅ Du bist bereits erwacht: *${char.awakening.name}*.`);
        return true;
      }
      nu.awakened = true;
      saveData();
      await send(`🌟 *${char.awakening.name}* aktiviert!\n${char.awakening.desc}\n\nAlle Werte wurden dauerhaft um ${Math.round((char.awakening.statMult - 1) * 100)}% erhöht.`);
      return true;
    }

    if (sub === 'rangliste' || sub === 'leaderboard' || sub === 'lb') {
      const entries = Object.entries(hxhData).filter(([, v]) => v.main);
      if (!entries.length) return send('📊 Noch keine HxH-Charaktere gewählt.'), true;
      const sorted = entries.sort((a, b) => (b[1].level * 1000 + b[1].wins) - (a[1].level * 1000 + a[1].wins));
      const top = sorted.slice(0, 10);
      const medals = ['🥇', '🥈', '🥉'];
      const lines = await Promise.all(top.map(async ([j, v], i) => {
        const char = getCharacter(v.main);
        const name = await getNumberMention(j, sock);
        return `${medals[i] || `${i + 1}.`} ${char?.icon || '🎯'} ${name} — Lv.${v.level} (${v.wins || 0} Siege) — ${char?.name || '?'}`;
      }));
      const mentions = top.map(([j]) => j);
      await send(`🎯 *HxH-Rangliste*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`, { mentions });
      return true;
    }

    if (sub === 'mission' || sub === 'missionen') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}hxh start.`);
        return true;
      }
      if (activeBattles.has(jid)) {
        const battle = activeBattles.get(jid);
        await send(`⚔️ Du bist bereits in einem Kampf!\n\n${battleStatusText(battle)}`);
        return true;
      }
      const char = getCharacter(nu.main);
      const stats = currentStats(jid);
      const enemy = pickEnemyForLevel(nu.level, randInt);
      const battle = startMissionBattle(jid, char, stats, enemy);
      await send(
        `📜 *Neue Mission!*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `Ein ${enemy.icon} *${enemy.name}* stellt sich dir in den Weg!${enemy.boss ? ' ⚠️ BOSS-GEGNER!' : ''}\n\n` +
        `${battleStatusText(battle)}\n\n` +
        `Nutze ${activePrefix}hxh faehigkeit <id>, ${activePrefix}hxh angriff oder ${activePrefix}hxh verteidigen.`
      );
      return true;
    }

    if (['faehigkeit', 'fähigkeit', 'jutsu', 'angriff', 'verteidigen', 'flucht'].includes(sub)) {
      const battle = activeBattles.get(jid);
      if (!battle || battle.type !== 'mission') {
        await send(`❌ Du bist in keiner aktiven Mission. Starte eine mit ${activePrefix}hxh mission.`);
        return true;
      }
      const char = battle.char;
      const p = battle.player, e = battle.enemy;

      if (sub === 'flucht') {
        activeBattles.delete(jid);
        await send('🏃 Du bist aus dem Kampf geflohen.');
        return true;
      }

      let actionLine = '';

      if (p.stunned) {
        actionLine = '😵 Du bist betäubt und kannst nicht handeln!';
        p.stunned = false;
      } else if (sub === 'angriff') {
        const d = computeDamage([12, 18], p.atk * buffMult(p.buffs, 'atk'), e.def, randInt);
        e.hp -= d;
        p.chakra = Math.min(p.chakraMax, p.chakra + 8);
        actionLine = `👊 Normaler Angriff trifft für ${d} Schaden! (+8 Nen)`;
      } else if (sub === 'verteidigen') {
        p.buffs.push({ stat: 'def', mult: 1.5, rounds: 1 });
        p.chakra = Math.min(p.chakraMax, p.chakra + 20);
        actionLine = '🛡️ Du gehst in Deckung — Verteidigung erhöht, +20 Nen gesammelt.';
      } else if (sub === 'faehigkeit' || sub === 'fähigkeit' || sub === 'jutsu') {
        const jutsuId = (args[1] || '').toLowerCase();
        const jutsu = char.jutsu.find(j => j.id === jutsuId || j.name.toLowerCase() === jutsuId);
        if (!jutsu) {
          await send(`❌ Unbekannte Fähigkeit. Nutze ${activePrefix}hxh faehigkeitenliste zur Übersicht.`);
          return true;
        }
        if (p.cooldowns[jutsu.id] > 0) {
          await send(`⏳ *${jutsu.name}* ist noch ${p.cooldowns[jutsu.id]} Runde(n) auf Cooldown.`);
          return true;
        }
        if (p.chakra < jutsu.cost) {
          await send(`❌ Nicht genug Nen für *${jutsu.name}* (benötigt: ${jutsu.cost}, du hast: ${p.chakra}).`);
          return true;
        }
        p.chakra -= jutsu.cost;
        p.cooldowns[jutsu.id] = jutsu.cd;
        actionLine = `${char.icon} ${playerJutsuAction(battle, jutsu, randInt)}`;
      }

      const partnerChar = nu.partner ? getCharacter(nu.partner) : null;
      let partnerLine = '';
      if (partnerChar && e.hp > 0 && Math.random() < 0.25) {
        const assistDmg = randInt(8, 16);
        e.hp -= assistDmg;
        partnerLine = `\n${partnerChar.icon} *${partnerChar.name}* unterstützt dich — ${assistDmg} Bonus-Schaden!`;
      }

      let out = actionLine + partnerLine;

      if (e.hp <= 0) {
        e.hp = 0;
        await send(out);
        await resolveMissionOutcome(battle, jid, nu, send, users, save, FILES);
        return true;
      }

      let enemyLine = '';
      let dodged = false;
      if (char.id === 'killua' && Math.random() < 0.22) dodged = true;

      if (dodged) {
        enemyLine = `⚡ Godspeed-Reflexe lassen dich dem Angriff von *${e.name}* mühelos entgehen!`;
      } else {
        enemyLine = enemyAction(battle, randInt);
        if (char.id === 'hisoka' && Math.random() < 0.15) {
          const halved = Math.round(p.lastAttackDmg / 2);
          p.hp += (p.lastAttackDmg - halved);
          p.lastAttackDmg = halved;
          enemyLine += `\n🃏 Bungee Gum dehnt den Schaden zurück — nur noch ${halved}!`;
        }
        if (char.id === 'chrollo' && Math.random() < 0.15) {
          const bonus = randInt(12, 20);
          e.hp -= bonus;
          enemyLine += `\n📖 Gestohlene Fähigkeit aktiviert! Zusätzliche ${bonus} Schaden gegen den Gegner!`;
        }
      }

      const roundLines = endOfRoundEffects(battle, char);
      out += `\n${enemyLine}`;
      if (roundLines.length) out += `\n${roundLines.join('\n')}`;

      if (p.hp <= 0) {
        await send(out);
        await resolveMissionOutcome(battle, jid, nu, send, users, save, FILES);
        return true;
      }
      if (e.hp <= 0) {
        await send(out);
        await resolveMissionOutcome(battle, jid, nu, send, users, save, FILES);
        return true;
      }

      out += `\n\n${battleStatusText(battle)}`;
      await send(out);
      return true;
    }

    if (sub === 'duell' || sub === 'duel') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}hxh start.`);
        return true;
      }
      const ctxMsg = m.message?.extendedTextMessage?.contextInfo;
      let targetRaw = args[1];
      if (!targetRaw && ctxMsg?.mentionedJid?.length) targetRaw = ctxMsg.mentionedJid[0];
      if (!targetRaw && ctxMsg?.participant) targetRaw = ctxMsg.participant;
      if (!targetRaw) {
        await send(`❌ Nutzung: ${activePrefix}hxh duell @user`);
        return true;
      }
      const targetJid = normalizeJid(targetRaw);
      if (isSameJid(targetJid, jid)) {
        await send('❌ Du kannst nicht gegen dich selbst duellieren!');
        return true;
      }
      ensureUser(targetJid);
      const targetNu = ensureHxhUser(targetJid);
      if (!targetNu.main) {
        await send(`❌ @${targetJid.split('@')[0]} hat noch keinen HxH-Charakter gewählt.`, { mentions: [targetJid] });
        return true;
      }

      const char1 = getCharacter(nu.main);
      const char2 = getCharacter(targetNu.main);
      const s1 = currentStats(jid);
      const s2 = currentStats(targetJid);
      const f1 = createBattleFighter(s1);
      const f2 = createBattleFighter(s2);

      const log = [];
      let round = 1;
      const MAX_ROUNDS = 20;

      function pickAiJutsu(char, fighter) {
        const usable = char.jutsu.filter(j => (fighter.cooldowns[j.id] || 0) <= 0 && fighter.chakra >= j.cost);
        if (!usable.length) return null;
        return usable[randInt(0, usable.length - 1)];
      }

      while (f1.hp > 0 && f2.hp > 0 && round <= MAX_ROUNDS) {
        if (!f1.stunned) {
          const j1 = pickAiJutsu(char1, f1);
          const fakeBattle1 = { player: f1, enemy: f2 };
          if (j1) {
            f1.chakra -= j1.cost;
            f1.cooldowns[j1.id] = j1.cd;
            log.push(`${char1.icon} ${playerJutsuAction(fakeBattle1, j1, randInt)}`);
          } else {
            const d = computeDamage([12, 18], f1.atk * buffMult(f1.buffs, 'atk'), f2.def, randInt);
            f2.hp -= d;
            f1.chakra = Math.min(f1.chakraMax, f1.chakra + 8);
            log.push(`${char1.icon} Normaler Angriff — ${d} Schaden.`);
          }
        } else {
          f1.stunned = false;
          log.push(`${char1.icon} ist betäubt und kann nicht handeln.`);
        }
        if (f2.hp <= 0) break;

        if (!f2.stunned) {
          const j2 = pickAiJutsu(char2, f2);
          const fakeBattle2 = { player: f2, enemy: f1 };
          if (j2) {
            f2.chakra -= j2.cost;
            f2.cooldowns[j2.id] = j2.cd;
            log.push(`${char2.icon} ${playerJutsuAction(fakeBattle2, j2, randInt)}`);
          } else {
            const d = computeDamage([12, 18], f2.atk * buffMult(f2.buffs, 'atk'), f1.def, randInt);
            f1.hp -= d;
            f2.chakra = Math.min(f2.chakraMax, f2.chakra + 8);
            log.push(`${char2.icon} Normaler Angriff — ${d} Schaden.`);
          }
        } else {
          f2.stunned = false;
          log.push(`${char2.icon} ist betäubt und kann nicht handeln.`);
        }

        f1.buffs = tickBuffs(f1.buffs);
        f2.buffs = tickBuffs(f2.buffs);
        for (const k of Object.keys(f1.cooldowns)) if (f1.cooldowns[k] > 0) f1.cooldowns[k]--;
        for (const k of Object.keys(f2.cooldowns)) if (f2.cooldowns[k] > 0) f2.cooldowns[k]--;
        round++;
      }

      const winnerJid = f1.hp > f2.hp ? jid : (f2.hp > f1.hp ? targetJid : null);
      const loserJid = winnerJid === jid ? targetJid : (winnerJid === targetJid ? jid : null);

      let resultText = `⚔️ *— HxH-DUELL —* ⚔️\n${char1.icon} ${char1.name} vs ${char2.icon} ${char2.name}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
      resultText += log.slice(-10).join('\n');
      resultText += `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;

      if (winnerJid) {
        const wNu = ensureHxhUser(winnerJid);
        const lNu = ensureHxhUser(loserJid);
        wNu.wins = (wNu.wins || 0) + 1;
        lNu.losses = (lNu.losses || 0) + 1;
        const winCoins = randInt(60, 120);
        if (users[winnerJid]) { users[winnerJid].coins = (users[winnerJid].coins || 0) + winCoins; save(FILES.users, users); }
        saveData();
        resultText += `🏆 Sieger: @${winnerJid.split('@')[0]} (+${winCoins} Coins)`;
        await send(resultText, { mentions: [jid, targetJid] });
      } else {
        resultText += '🤝 Unentschieden nach maximaler Rundenzahl!';
        await send(resultText, { mentions: [jid, targetJid] });
      }
      return true;
    }

    await send(`❓ Unbekannter HxH-Befehl. Nutze ${activePrefix}hxh help.`);
    return true;
  }

  return { handle, HXH_HELP_TEXT, HXH_COMMANDS, CHARACTERS };
}
