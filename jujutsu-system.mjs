import fs from 'fs';
import path from 'path';

// ─────────────────────────────────────────────────────────────
// CHARAKTERE
// ─────────────────────────────────────────────────────────────

const CHARACTERS = {
  yuji: {
    id: 'yuji',
    name: 'Yuji Itadori',
    title: 'Sukunas Gefäß',
    icon: '🥊',
    element: 'Körperliche Verstärkung',
    baseStats: { hp: 150, chakra: 100, atk: 26, def: 18, spd: 20 },
    passive: {
      name: 'Zellulare Regeneration',
      desc: 'Regeneriert am Ende jeder Runde zusätzlich 4% der maximalen HP.'
    },
    jutsu: [
      { id: 'divergent_fist', name: 'Divergent Fist', cost: 20, cd: 1, dmg: [22, 30], desc: 'Ein Doppelschlag – der zweite Einschlag trifft eine Frist zu spät und durchschlägt die Deckung.' },
      { id: 'black_flash', name: 'Black Flash', cost: 35, cd: 3, dmg: [50, 65], desc: 'Trifft im perfekten Moment – verheerender Kritischer Schlag.' },
      { id: 'sukuna_boost', name: 'Sukunas Fluchenergie', cost: 30, cd: 4, effect: 'buff', buff: { atk: 1.3, spd: 1.15, rounds: 3 }, dmg: [10, 15], desc: 'Leiht sich kurz Sukunas Kraft — +30% Angriff, +15% Tempo.' },
      { id: 'combo_kick', name: 'Kombo-Tritt', cost: 15, cd: 2, dmg: [18, 26], effect: 'multi2', desc: 'Zwei schnelle Tritte hintereinander.' }
    ],
    awakening: { name: 'Sukunas Malevolent Shrine (geteilte Kontrolle)', unlockLevel: 15, statMult: 1.5, desc: 'Kurzzeitige Kontrolle über einen Teil von Sukunas Domäne.' }
  },

  megumi: {
    id: 'megumi',
    name: 'Megumi Fushiguro',
    title: 'Erbe der Zehn Schatten',
    icon: '🐺',
    element: 'Schattenbeschwörung',
    baseStats: { hp: 130, chakra: 130, atk: 20, def: 16, spd: 19 },
    passive: {
      name: 'Schattenreflex',
      desc: '18% Chance, in den Schatten abzutauchen und einem Angriff vollständig auszuweichen.'
    },
    jutsu: [
      { id: 'divine_dog', name: 'Göttliche Hunde', cost: 25, cd: 2, dmg: [24, 34], effect: 'multi2', desc: 'Zwei Schattenwölfe hetzen den Gegner.' },
      { id: 'nue', name: 'Nue', cost: 30, cd: 3, dmg: [30, 42], effect: 'stun', stunRounds: 1, desc: 'Ein Blitzvogel aus Schatten betäubt beim Einschlag.' },
      { id: 'rabbit_escape', name: 'Häschenflucht', cost: 20, cd: 2, effect: 'buff', buff: { spd: 1.3, rounds: 2 }, dmg: [0, 0], desc: 'Schnelle Schattenkaninchen erhöhen dein Tempo.' },
      { id: 'nue_toad', name: 'Riesenkröte', cost: 22, cd: 2, dmg: [20, 28], effect: 'debuff', debuff: { atk: 0.8, rounds: 2 }, desc: 'Eine Kröte drängt den Gegner zurück und schwächt ihn.' }
    ],
    awakening: { name: 'Mahoraga beschworen', unlockLevel: 15, statMult: 1.55, desc: 'Der göttliche Wächter mit dem Rad der Anpassung erscheint an deiner Seite.' }
  },

  nobara: {
    id: 'nobara',
    name: 'Nobara Kugisaki',
    title: 'Meisterin der Strohpuppentechnik',
    icon: '🔨',
    element: 'Resonanz-Fluchtechnik',
    baseStats: { hp: 125, chakra: 115, atk: 24, def: 14, spd: 18 },
    passive: {
      name: 'Resonanz-Verstärkung',
      desc: '15% Chance auf zusätzlichen Resonanzschaden (+50%) bei einem Treffer.'
    },
    jutsu: [
      { id: 'resonance_nail', name: 'Resonanz-Nagel', cost: 18, cd: 1, dmg: [20, 28], effect: 'burn', burn: { dmg: [8, 12], rounds: 2 }, desc: 'Ein Nagel überträgt Schaden über die Fluchband-Resonanz.' },
      { id: 'hairpin', name: 'Haarnadel', cost: 32, cd: 3, dmg: [40, 52], desc: 'Explosive Straffung des Fluchbandes — massiver Einzeltreffer.' },
      { id: 'straw_doll', name: 'Strohpuppentechnik: Salve', cost: 28, cd: 2, dmg: [16, 22], effect: 'multi3', desc: 'Mehrere Nägel treffen in Serie.' },
      { id: 'hammer_charge', name: 'Hammer-Ladung', cost: 20, cd: 2, effect: 'buff', buff: { atk: 1.25, rounds: 2 }, dmg: [8, 14], desc: 'Lädt den Hammer auf für mehr Wucht in den nächsten Schlägen.' }
    ],
    awakening: { name: 'Vollresonanz — Straw Doll Overdrive', unlockLevel: 15, statMult: 1.45, desc: 'Jeder Nagel sitzt perfekt — das Fluchband bebt im Gleichklang.' }
  },

  gojo: {
    id: 'gojo',
    name: 'Satoru Gojo',
    title: 'Der Stärkste',
    icon: '👁️',
    element: 'Grenzenlos / Sechs Augen',
    baseStats: { hp: 145, chakra: 160, atk: 30, def: 20, spd: 24 },
    passive: {
      name: 'Unendlichkeit',
      desc: '25% Chance, dass ein gegnerischer Angriff die unendliche Distanz nie überwindet (vollständig ausweichen).'
    },
    jutsu: [
      { id: 'blue', name: 'Cursed Technique Lapse: Blau', cost: 30, cd: 2, dmg: [35, 45], effect: 'debuff', debuff: { def: 0.75, rounds: 2 }, desc: 'Zieht den Gegner heran und schwächt seine Deckung.' },
      { id: 'red', name: 'Cursed Technique Reversal: Rot', cost: 32, cd: 2, dmg: [38, 50], desc: 'Stößt mit abstoßender Energie ab — hoher Wucht-Schaden.' },
      { id: 'hollow_purple', name: 'Hollow Technique: Purple', cost: 65, cd: 5, dmg: [80, 100], desc: 'Fusion von Blau und Rot — eine der stärksten Einzeltechniken überhaupt.' },
      { id: 'domain_amplification', name: 'Domänen-Verstärkung', cost: 40, cd: 4, effect: 'buff_all', buff: { atk: 1.2, def: 1.2, spd: 1.2, rounds: 3 }, desc: 'Kurzzeitige Verstärkung durch reine Fluchenergie-Kontrolle.' }
    ],
    awakening: { name: 'Domain Expansion: Unlimited Void', unlockLevel: 15, statMult: 1.65, desc: 'Der Gegner wird mit unendlichen Informationen überflutet — absolute Überlegenheit.' }
  },

  sukuna: {
    id: 'sukuna',
    name: 'Ryomen Sukuna',
    title: 'Der König der Flüche',
    icon: '😈',
    element: 'Schneide & Feuer',
    baseStats: { hp: 155, chakra: 140, atk: 32, def: 18, spd: 21 },
    passive: {
      name: 'Malevolentes Blut',
      desc: 'Verursacht 20% mehr Schaden, wenn der Gegner unter 30% HP steht (Exekution).'
    },
    jutsu: [
      { id: 'dismantle', name: 'Zerlegen', cost: 25, cd: 1, dmg: [30, 40], desc: 'Unsichtbare Schnitte zerteilen den Gegner.' },
      { id: 'cleave', name: 'Spalten', cost: 35, cd: 2, dmg: [40, 55], desc: 'Ein einziger gewaltiger Schnitt, der alles in seinem Weg zerteilt.' },
      { id: 'fire_arrow', name: 'Feuerpfeil', cost: 45, cd: 3, dmg: [50, 65], effect: 'burn', burn: { dmg: [12, 18], rounds: 2 }, desc: 'Ein Pfeil aus Fluchfeuer setzt den Gegner in Brand.' },
      { id: 'domain_prep', name: 'Domänen-Vorbereitung', cost: 30, cd: 3, effect: 'buff', buff: { atk: 1.35, rounds: 2 }, dmg: [15, 20], desc: 'Konzentriert Fluchenergie für einen entscheidenden Schlag.' }
    ],
    awakening: { name: 'Domain Expansion: Malevolent Shrine', unlockLevel: 15, statMult: 1.7, desc: 'Ein unentrinnbares Reich aus tausend Klingen — sichere Trefferquote.' }
  },

  nanami: {
    id: 'nanami',
    name: 'Kento Nanami',
    title: 'Der Salaryman-Zauberer',
    icon: '💼',
    element: 'Ratio-Technik',
    baseStats: { hp: 140, chakra: 110, atk: 23, def: 22, spd: 15 },
    passive: {
      name: 'Feierabend-Disziplin',
      desc: 'Nach dem ersten Treffer in einem Kampf: dauerhaft +15% Angriff für den Rest des Kampfes (Überstunden).'
    },
    jutsu: [
      { id: 'ratio_seven3', name: 'Ratio-Technik: 7:3', cost: 22, cd: 2, dmg: [26, 36], effect: 'debuff', debuff: { def: 0.7, rounds: 2 }, desc: 'Markiert den optimalen Trefferpunkt — massive Verteidigungsschwächung.' },
      { id: 'machete_strike', name: 'Machetenhieb', cost: 15, cd: 1, dmg: [20, 28], desc: 'Präziser, effizienter Schlag mit der Machete.' },
      { id: 'overtime', name: 'Überstunden-Modus', cost: 35, cd: 4, effect: 'buff', buff: { atk: 1.4, def: 1.1, rounds: 3 }, dmg: [0, 0], desc: '"Das ist Überstunden." — Nanami gibt alles.' },
      { id: 'precision_cut', name: 'Präzisionsschnitt', cost: 28, cd: 2, dmg: [32, 42], desc: 'Ein kalkulierter, tödlich genauer Schnitt.' }
    ],
    awakening: { name: 'Volle Überstunden-Konzentration', unlockLevel: 15, statMult: 1.4, desc: 'Nanami rechnet keine Sekunde mehr — nur noch Effizienz.' }
  }
};

const CHAR_LIST = Object.values(CHARACTERS);
const CHAR_ALIASES = {
  yuji: 'yuji', itadori: 'yuji',
  megumi: 'megumi', fushiguro: 'megumi',
  nobara: 'nobara', kugisaki: 'nobara',
  gojo: 'gojo', satoru: 'gojo',
  sukuna: 'sukuna', ryomen: 'sukuna',
  nanami: 'nanami', kento: 'nanami'
};

// ─────────────────────────────────────────────────────────────
// GEGNER-POOL (PvE — Flüche)
// ─────────────────────────────────────────────────────────────

const ENEMY_POOL = [
  { name: 'Fluch der Klasse 4', icon: '👹', tier: 1, hp: 60, atk: 10, def: 5, spd: 8, xp: 15, coins: [20, 40] },
  { name: 'Verirrter Geist', icon: '👻', tier: 1, hp: 70, atk: 12, def: 6, spd: 10, xp: 18, coins: [25, 45] },
  { name: 'Fluch der Klasse 3', icon: '👺', tier: 2, hp: 100, atk: 16, def: 10, spd: 9, xp: 28, coins: [40, 70] },
  { name: 'Verwesungsfluch', icon: '🦠', tier: 2, hp: 110, atk: 18, def: 8, spd: 12, xp: 32, coins: [45, 75] },
  { name: 'Fluch der Klasse 2', icon: '😈', tier: 3, hp: 140, atk: 22, def: 12, spd: 14, xp: 45, coins: [60, 100] },
  { name: 'Verfluchter Leichnam', icon: '🧟', tier: 3, hp: 160, atk: 24, def: 10, spd: 11, xp: 50, coins: [65, 110] },
  { name: 'Fluch der Klasse 1', icon: '👿', tier: 4, hp: 180, atk: 26, def: 14, spd: 15, xp: 65, coins: [80, 130] },
  { name: 'Fluch der Sonderklasse', icon: '🌋', tier: 5, hp: 260, atk: 32, def: 18, spd: 16, xp: 100, coins: [120, 200], boss: true }
];

// ─────────────────────────────────────────────────────────────
// PERSISTENZ
// ─────────────────────────────────────────────────────────────

export function createJujutsuSystem(DATA_PATH) {
  const FILE_PATH = path.join(DATA_PATH, 'jujutsu.json');

  function loadData() {
    try {
      if (!fs.existsSync(FILE_PATH)) {
        fs.writeFileSync(FILE_PATH, JSON.stringify({}, null, 2));
        return {};
      }
      const raw = fs.readFileSync(FILE_PATH, 'utf-8');
      return raw.trim() ? JSON.parse(raw) : {};
    } catch (e) {
      console.error('[jujutsu] Fehler beim Laden:', e);
      return {};
    }
  }

  function saveData() {
    try {
      fs.writeFileSync(FILE_PATH, JSON.stringify(jjkData, null, 2));
    } catch (e) {
      console.error('[jujutsu] Fehler beim Speichern:', e);
    }
  }

  let jjkData = loadData();
  const activeBattles = new Map();

  function ensureJjkUser(jid) {
    if (!jjkData[jid]) {
      jjkData[jid] = {
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
    return jjkData[jid];
  }

  function xpNeeded(level) {
    return 60 + level * 40;
  }

  function addJjkXp(jid, amount) {
    const nu = ensureJjkUser(jid);
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
    const nu = ensureJjkUser(jid);
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
      lastAttackDmg: 0, overtimeApplied: false
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
      lines.push(`${jutsu.name} trifft für ${d} Schaden und entfacht Flammen/Nagel-Resonanz!`);
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
    if (char.id === 'yuji') {
      const regen = Math.round(p.hpMax * 0.04);
      p.hp = Math.min(p.hpMax, p.hp + regen);
      lines.push(`💚 Zellulare Regeneration heilt dich um ${regen} HP.`);
    }
    if (e.burn) {
      const bd = Math.round((e.burn.dmg[0] + e.burn.dmg[1]) / 2);
      e.hp -= bd;
      e.burn.rounds--;
      lines.push(`🔥 Effekt fügt dem Gegner ${bd} Schaden zu.`);
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
      `Energie: ${p.chakra}/${p.chakraMax}\n` +
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
      const leveledUp = addJjkXp(jid, xp);
      let out = `🎉 Sieg! Du hast *${e.name}* exorziert!\n💰 +${coins} Coins | ✨ +${xp} JJK-XP`;
      if (leveledUp) out += `\n\n📈 Level Up! Du bist jetzt JJK-Level ${nu.level}.`;
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
    let out = `🌀 *— JUJUTSU KAISEN CHARAKTERE —* 🌀\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
    for (const c of CHAR_LIST) {
      out += `${c.icon} *${c.name}* (${c.id}) — ${c.title}\n`;
      out += `   ${c.element} | HP ${c.baseStats.hp} | Energie ${c.baseStats.chakra} | ATK ${c.baseStats.atk} | DEF ${c.baseStats.def} | SPD ${c.baseStats.spd}\n`;
      out += `   Passiv: ${c.passive.name} — ${c.passive.desc}\n\n`;
    }
    out += `Wähle deinen Hauptcharakter mit:\n${prefix}jjk wähle <name>\n`;
    out += `Danach einen Partner mit:\n${prefix}jjk partner <name>`;
    return out;
  }

  function jutsuListText(char, prefix) {
    let out = `${char.icon} *Technik-Liste — ${char.name}*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
    for (const j of char.jutsu) {
      out += `• *${j.name}* (${prefix}jjk technik ${j.id})\n   Kosten: ${j.cost} Energie | CD: ${j.cd} Runden\n   ${j.desc}\n\n`;
    }
    out += `Passiv: *${char.passive.name}* — ${char.passive.desc}\n`;
    out += `Erwachen (ab Lv.${char.awakening.unlockLevel}): *${char.awakening.name}* — ${char.awakening.desc}`;
    return out;
  }

  const JJK_HELP_TEXT =
    `🌀 *— JUJUTSU KAISEN SYSTEM —* 🌀\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `?jjk start — Charakterübersicht\n` +
    `?jjk wähle <name> — Hauptcharakter wählen\n` +
    `?jjk partner <name> — Partnercharakter wählen\n` +
    `?jjk profil — Deine Werte anzeigen\n` +
    `?jjk technikliste — Deine Techniken anzeigen\n` +
    `?jjk mission — PvE-Kampf gegen einen Fluch starten\n` +
    `?jjk technik <id> — Technik im Kampf einsetzen\n` +
    `?jjk angriff — Normaler Angriff\n` +
    `?jjk verteidigen — Schaden reduzieren & Energie sammeln\n` +
    `?jjk flucht — Aus dem Kampf fliehen\n` +
    `?jjk erwachen — Domain Expansion / Erwachen (ab Lv.15)\n` +
    `?jjk duell @user — PvP-Duell herausfordern\n` +
    `?jjk rangliste — Bestenliste\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `Charaktere: Yuji, Megumi, Nobara, Gojo, Sukuna, Nanami`;

  const JJK_COMMANDS = ['jjk', 'jujutsu'];

  // ─────────────────────────────────────────────────────────
  // HAUPT-HANDLER
  // ─────────────────────────────────────────────────────────

  async function handle(ctx) {
    const {
      cmd, args, sender, m, activePrefix, send, sock,
      users, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, randInt
    } = ctx;

    if (!JJK_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    ensureUser(jid);
    const nu = ensureJjkUser(jid);
    const sub = (args[0] || '').toLowerCase();

    if (!sub || sub === 'help' || sub === 'hilfe') {
      await send(JJK_HELP_TEXT);
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
        await send(`❌ Unbekannter Charakter. Nutze ${activePrefix}jjk start für die Liste.`);
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
      await send(`✅ *${char.icon} ${char.name}* ist jetzt dein Hauptcharakter!\n\nWähle jetzt noch einen Partner mit:\n${activePrefix}jjk partner <name>`);
      return true;
    }

    if (sub === 'partner') {
      const raw = (args[1] || '').toLowerCase();
      const charId = CHAR_ALIASES[raw];
      if (!charId) {
        await send(`❌ Unbekannter Charakter. Nutze ${activePrefix}jjk start für die Liste.`);
        return true;
      }
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Hauptcharakter mit ${activePrefix}jjk wähle <name>.`);
        return true;
      }
      if (nu.main === charId) {
        await send('❌ Haupt- und Partner-Charakter dürfen nicht identisch sein.');
        return true;
      }
      nu.partner = charId;
      saveData();
      const char = getCharacter(charId);
      await send(`✅ *${char.icon} ${char.name}* unterstützt dich jetzt als Partner!\n\nStarte deine erste Mission mit ${activePrefix}jjk mission`);
      return true;
    }

    if (sub === 'profil' || sub === 'profile') {
      if (!nu.main) {
        await send(`❌ Du hast noch keinen Charakter gewählt. Nutze ${activePrefix}jjk start.`);
        return true;
      }
      const char = getCharacter(nu.main);
      const partner = nu.partner ? getCharacter(nu.partner) : null;
      const stats = currentStats(jid);
      const nextXp = xpNeeded(nu.level);
      const out =
        `👤 *Jujutsu-Kaisen-Profil*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${char.icon} Hauptcharakter: *${char.name}*${stats.isAwakened ? ` (${char.awakening.name} 🌟)` : ''}\n` +
        `${partner ? `${partner.icon} Partner: *${partner.name}*\n` : ''}` +
        `⭐ Level: ${nu.level} (${nu.xp}/${nextXp} XP)\n` +
        `❤️ HP: ${stats.hp} | 🔷 Energie: ${stats.chakra}\n` +
        `⚔️ ATK: ${stats.atk} | 🛡️ DEF: ${stats.def} | 💨 SPD: ${stats.spd}\n` +
        `🏆 Siege: ${nu.wins || 0} | ☠️ Niederlagen: ${nu.losses || 0}\n` +
        `📜 Missionen abgeschlossen: ${nu.missionsCompleted || 0}\n` +
        (nu.level >= char.awakening.unlockLevel && !nu.awakened
          ? `\n🌟 Erwachen verfügbar! Nutze ${activePrefix}jjk erwachen`
          : '');
      await send(out);
      return true;
    }

    if (sub === 'technikliste' || sub === 'jutsuliste' || sub === 'skills') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}jjk start.`);
        return true;
      }
      await send(jutsuListText(getCharacter(nu.main), activePrefix));
      return true;
    }

    if (sub === 'erwachen' || sub === 'awaken') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}jjk start.`);
        return true;
      }
      const char = getCharacter(nu.main);
      if (nu.level < char.awakening.unlockLevel) {
        await send(`❌ Erwachen erfordert JJK-Level ${char.awakening.unlockLevel}. Du bist Level ${nu.level}.`);
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
      const entries = Object.entries(jjkData).filter(([, v]) => v.main);
      if (!entries.length) return send('📊 Noch keine JJK-Charaktere gewählt.'), true;
      const sorted = entries.sort((a, b) => (b[1].level * 1000 + b[1].wins) - (a[1].level * 1000 + a[1].wins));
      const top = sorted.slice(0, 10);
      const medals = ['🥇', '🥈', '🥉'];
      const lines = await Promise.all(top.map(async ([j, v], i) => {
        const char = getCharacter(v.main);
        const name = await getNumberMention(j, sock);
        return `${medals[i] || `${i + 1}.`} ${char?.icon || '🌀'} ${name} — Lv.${v.level} (${v.wins || 0} Siege) — ${char?.name || '?'}`;
      }));
      const mentions = top.map(([j]) => j);
      await send(`🌀 *JJK-Rangliste*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`, { mentions });
      return true;
    }

    if (sub === 'mission' || sub === 'missionen') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}jjk start.`);
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
        `Ein ${enemy.icon} *${enemy.name}* materialisiert sich vor dir!${enemy.boss ? ' ⚠️ SONDERGRAD-FLUCH!' : ''}\n\n` +
        `${battleStatusText(battle)}\n\n` +
        `Nutze ${activePrefix}jjk technik <id>, ${activePrefix}jjk angriff oder ${activePrefix}jjk verteidigen.`
      );
      return true;
    }

    if (['technik', 'jutsu', 'angriff', 'verteidigen', 'flucht'].includes(sub)) {
      const battle = activeBattles.get(jid);
      if (!battle || battle.type !== 'mission') {
        await send(`❌ Du bist in keiner aktiven Mission. Starte eine mit ${activePrefix}jjk mission.`);
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
        actionLine = `👊 Normaler Angriff trifft für ${d} Schaden! (+8 Energie)`;
      } else if (sub === 'verteidigen') {
        p.buffs.push({ stat: 'def', mult: 1.5, rounds: 1 });
        p.chakra = Math.min(p.chakraMax, p.chakra + 20);
        actionLine = '🛡️ Du gehst in Deckung — Verteidigung erhöht, +20 Energie gesammelt.';
      } else if (sub === 'technik' || sub === 'jutsu') {
        const jutsuId = (args[1] || '').toLowerCase();
        const jutsu = char.jutsu.find(j => j.id === jutsuId || j.name.toLowerCase() === jutsuId);
        if (!jutsu) {
          await send(`❌ Unbekannte Technik. Nutze ${activePrefix}jjk technikliste zur Übersicht.`);
          return true;
        }
        if (p.cooldowns[jutsu.id] > 0) {
          await send(`⏳ *${jutsu.name}* ist noch ${p.cooldowns[jutsu.id]} Runde(n) auf Cooldown.`);
          return true;
        }
        if (p.chakra < jutsu.cost) {
          await send(`❌ Nicht genug Fluchenergie für *${jutsu.name}* (benötigt: ${jutsu.cost}, du hast: ${p.chakra}).`);
          return true;
        }
        p.chakra -= jutsu.cost;
        p.cooldowns[jutsu.id] = jutsu.cd;
        actionLine = `${char.icon} ${playerJutsuAction(battle, jutsu, randInt)}`;

        // Nanami Passiv: nach erstem Treffer dauerhaft +15% ATK
        if (char.id === 'nanami' && !p.overtimeApplied) {
          p.overtimeApplied = true;
          p.buffs.push({ stat: 'atk', mult: 1.15, rounds: 999 });
          actionLine += `\n💼 Feierabend-Disziplin: dauerhaft +15% Angriff für diesen Kampf aktiviert!`;
        }
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
      if (char.id === 'megumi' && Math.random() < 0.18) dodged = true;
      if (char.id === 'gojo' && Math.random() < 0.25) dodged = true;

      if (dodged) {
        enemyLine = char.id === 'gojo'
          ? `♾️ Unendlichkeit verhindert, dass *${e.name}* dich überhaupt erreicht!`
          : `🐺 Du weichst in den Schatten aus und entgehst dem Angriff von *${e.name}*!`;
      } else {
        enemyLine = enemyAction(battle, randInt);
        if (char.id === 'sukuna' && e.hp > 0 && e.hp <= e.hpMax * 0.3) {
          const bonus = Math.round(p.lastAttackDmg * 0.2) || 0;
          if (bonus > 0) {
            e.hp -= bonus;
            enemyLine += `\n😈 Malevolentes Blut: +${bonus} Exekutions-Schaden, da der Gegner unter 30% HP ist!`;
          }
        }
        if (char.id === 'yuji' && Math.random() < 0.1) {
          const bf = randInt(15, 25);
          e.hp -= bf;
          enemyLine += `\n⚡ Black Flash Reflex! Zusätzliche ${bf} Schaden!`;
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
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}jjk start.`);
        return true;
      }
      const ctxMsg = m.message?.extendedTextMessage?.contextInfo;
      let targetRaw = args[1];
      if (!targetRaw && ctxMsg?.mentionedJid?.length) targetRaw = ctxMsg.mentionedJid[0];
      if (!targetRaw && ctxMsg?.participant) targetRaw = ctxMsg.participant;
      if (!targetRaw) {
        await send(`❌ Nutzung: ${activePrefix}jjk duell @user`);
        return true;
      }
      const targetJid = normalizeJid(targetRaw);
      if (isSameJid(targetJid, jid)) {
        await send('❌ Du kannst nicht gegen dich selbst duellieren!');
        return true;
      }
      ensureUser(targetJid);
      const targetNu = ensureJjkUser(targetJid);
      if (!targetNu.main) {
        await send(`❌ @${targetJid.split('@')[0]} hat noch keinen JJK-Charakter gewählt.`, { mentions: [targetJid] });
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

      let resultText = `⚔️ *— JJK-DUELL —* ⚔️\n${char1.icon} ${char1.name} vs ${char2.icon} ${char2.name}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
      resultText += log.slice(-10).join('\n');
      resultText += `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;

      if (winnerJid) {
        const wNu = ensureJjkUser(winnerJid);
        const lNu = ensureJjkUser(loserJid);
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

    await send(`❓ Unbekannter JJK-Befehl. Nutze ${activePrefix}jjk help.`);
    return true;
  }

  return { handle, JJK_HELP_TEXT, JJK_COMMANDS, CHARACTERS };
}
