

import fs from 'fs';
import path from 'path';



const CHARACTERS = {
  naruto: {
    id: 'naruto',
    name: 'Naruto Uzumaki',
    title: 'Der Held von Konoha',
    icon: '🍥',
    element: 'Fūton / Chakra',
    baseStats: { hp: 140, chakra: 120, atk: 24, def: 16, spd: 18 },
    passive: {
      name: 'Uzumaki-Vitalität',
      desc: 'Regeneriert am Ende jeder Runde zusätzlich 3% der maximalen HP.'
    },
    jutsu: [
      { id: 'rasengan', name: 'Rasengan', cost: 30, cd: 1, dmg: [38, 50], desc: 'Konzentrierte Chakra-Kugel, hoher physischer Schaden.' },
      { id: 'kagebunshin', name: 'Schattendoppelgänger-Jutsu', cost: 20, cd: 2, dmg: [15, 25], effect: 'multi2', desc: 'Beschwört einen Klon für einen zusätzlichen Treffer.' },
      { id: 'rasenshuriken', name: 'Fūton: Rasenshuriken', cost: 55, cd: 4, dmg: [60, 80], desc: 'Verheerender Windschaden – hoher Chakraverbrauch.' },
      { id: 'kyuubi_mantel', name: 'Kyūbi-Chakra-Mantel', cost: 40, cd: 5, effect: 'buff', buff: { atk: 1.3, spd: 1.2, rounds: 3 }, dmg: [10, 16], desc: '+30% Angriff, +20% Tempo für 3 Runden.' }
    ],
    awakening: { name: 'Kurama-Kyūbi-Modus', unlockLevel: 15, statMult: 1.5, desc: 'Vollständige Fusion mit Kurama – ein goldener Chakramantel umgibt dich.' }
  },

  sasuke: {
    id: 'sasuke',
    name: 'Sasuke Uchiha',
    title: 'Der letzte Uchiha',
    icon: '⚡',
    element: 'Raiton / Katon',
    baseStats: { hp: 125, chakra: 130, atk: 28, def: 14, spd: 22 },
    passive: {
      name: 'Sharingan-Einsicht',
      desc: '15% Chance, den Schaden eines gegnerischen Jutsu zu halbieren (vorhergesehen).'
    },
    jutsu: [
      { id: 'chidori', name: 'Chidori', cost: 28, cd: 1, dmg: [36, 48], desc: 'Blitzschnelle, durchbohrende Klinge aus reinem Chakra.' },
      { id: 'goukakyuu', name: 'Katon: Gōkakyū no Jutsu', cost: 22, cd: 2, dmg: [25, 35], desc: 'Große Feuerball-Technik.' },
      { id: 'kirin', name: 'Kirin', cost: 60, cd: 5, dmg: [65, 85], desc: 'Gebündelter Blitzschlag – Sasukes stärkste Einzeltechnik.' },
      { id: 'amaterasu', name: 'Amaterasu', cost: 50, cd: 4, dmg: [20, 30], effect: 'burn', burn: { dmg: [10, 15], rounds: 3 }, desc: 'Schwarze Flammen, die den Gegner mehrere Runden lang verbrennen.' }
    ],
    awakening: { name: 'Mangekyō-Sharingan: Susanoo', unlockLevel: 15, statMult: 1.55, desc: 'Ein knochiger Rüstungs-Avatar aus Chakra umschließt dich.' }
  },

  hinata: {
    id: 'hinata',
    name: 'Hinata Hyūga',
    title: 'Die sanfte Löwin',
    icon: '👁️',
    element: 'Taijutsu / Byakugan',
    baseStats: { hp: 150, chakra: 110, atk: 18, def: 22, spd: 20 },
    passive: {
      name: 'Byakugan-Weitsicht',
      desc: '20% Chance, einem gegnerischen Angriff vollständig auszuweichen.'
    },
    jutsu: [
      { id: 'jyuuken', name: 'Jyūken: Sanfte Faust', cost: 18, cd: 1, dmg: [20, 28], effect: 'chakra_drain', drain: 10, desc: 'Trifft Chakrapunkte und entzieht dem Gegner Chakra.' },
      { id: 'hakke64', name: 'Hakke Rokujūyon Shō', cost: 45, cd: 3, dmg: [50, 65], effect: 'multi3', desc: '64 Schläge in Sekundenbruchteilen.' },
      { id: 'byakugan_kreis', name: 'Byakugan-Abwehrkreis', cost: 25, cd: 3, dmg: [0, 0], effect: 'buff_heal', buff: { def: 1.4, rounds: 2 }, heal: [15, 25], desc: 'Rotierender Chakra-Schutzschild, heilt leicht und stärkt die Verteidigung.' },
      { id: 'loewenfaust', name: 'Doppelte Löwenfaust', cost: 35, cd: 3, dmg: [32, 42], effect: 'debuff', debuff: { atk: 0.8, rounds: 2 }, desc: 'Schwächt die Angriffskraft des Gegners spürbar.' }
    ],
    awakening: { name: 'Erwachtes Byakugan', unlockLevel: 15, statMult: 1.4, desc: 'Ihr Sichtfeld erfasst nahezu 360° – kein Angriff bleibt verborgen.' }
  },

  kakashi: {
    id: 'kakashi',
    name: 'Kakashi Hatake',
    title: 'Der Kopierninja',
    icon: '🐺',
    element: 'Raiton / Vielseitig',
    baseStats: { hp: 130, chakra: 125, atk: 22, def: 18, spd: 19 },
    passive: {
      name: 'Sharingan des Kopierninja',
      desc: '10% Chance auf einen kostenlosen Bonusangriff in Stärke des letzten gegnerischen Angriffs.'
    },
    jutsu: [
      { id: 'raikiri', name: 'Raikiri', cost: 30, cd: 1, dmg: [38, 50], desc: 'Der "tausendfach kopierte Vogel" – Kakashis Signatur-Attacke.' },
      { id: 'doryuuheki', name: 'Doton: Doryūheki', cost: 20, cd: 2, dmg: [5, 10], effect: 'buff', buff: { def: 1.25, rounds: 2 }, desc: 'Errichtet eine Erdmauer zum Schutz.' },
      { id: 'kamui', name: 'Kamui', cost: 55, cd: 4, dmg: [55, 70], effect: 'stun', stunRounds: 1, desc: 'Warpt einen Teil des Gegners in eine andere Dimension.' },
      { id: 'ninken', name: 'Ninkenrudel-Beschwörung', cost: 25, cd: 3, dmg: [22, 30], effect: 'debuff', debuff: { spd: 0.8, rounds: 2 }, desc: 'Pakkun und die Ninkenmeute halten den Gegner fest.' }
    ],
    awakening: { name: 'Voll erwachtes Mangekyō-Sharingan', unlockLevel: 15, statMult: 1.45, desc: 'Beide Augen brennen im Mangekyō-Muster.' }
  },

  itachi: {
    id: 'itachi',
    name: 'Itachi Uchiha',
    title: 'Der Genjutsu-Meister',
    icon: '🐦‍⬛',
    element: 'Genjutsu / Katon',
    baseStats: { hp: 115, chakra: 140, atk: 26, def: 12, spd: 21 },
    passive: {
      name: 'Genjutsu-Meister',
      desc: '15% Chance, dass der Gegner in einer Illusion gefangen ist und seine Runde verliert.'
    },
    jutsu: [
      { id: 'tsukuyomi', name: 'Tsukuyomi', cost: 45, cd: 4, dmg: [15, 20], effect: 'stun', stunRounds: 2, desc: 'Zieht den Gegner in eine grausame Traumwelt – 3 Tage in Sekunden.' },
      { id: 'goukakyuu_it', name: 'Katon: Gōkakyū no Jutsu', cost: 22, cd: 2, dmg: [25, 35], desc: 'Große Feuerball-Technik.' },
      { id: 'susanoo_it', name: 'Susanoo: Yata-Spiegel & Totsuka-Klinge', cost: 60, cd: 5, dmg: [60, 80], effect: 'buff', buff: { def: 1.3, rounds: 2 }, desc: 'Perfekte Verteidigung trifft unausweichliche Klinge.' },
      { id: 'crow_genjutsu', name: 'Krähen-Täuschung', cost: 20, cd: 2, dmg: [10, 15], effect: 'debuff', debuff: { atk: 0.75, rounds: 2 }, desc: 'Ein Schwarm Krähen verwirrt die Sinne des Gegners.' }
    ],
    awakening: { name: 'Izanami – Erweitertes Mangekyō', unlockLevel: 15, statMult: 1.55, desc: 'Die Realität selbst beugt sich seinem Willen.' }
  },

  deidara: {
    id: 'deidara',
    name: 'Deidara',
    title: 'Der Explosionskünstler',
    icon: '💥',
    element: 'Explosions-Lehm',
    baseStats: { hp: 120, chakra: 130, atk: 27, def: 13, spd: 17 },
    passive: {
      name: 'Kunst ist eine Explosion!',
      desc: '10% Chance, dass ein Jutsu kritischen Zusatzschaden (+50%) verursacht.'
    },
    jutsu: [
      { id: 'c1', name: 'C1: Explosionsvogel', cost: 25, cd: 1, dmg: [30, 40], desc: 'Ein Lehmvogel explodiert beim Aufprall.' },
      { id: 'c2', name: 'C2: Drachenbombe', cost: 35, cd: 2, dmg: [38, 50], desc: 'Ein Lehmdrache rast heran und detoniert.' },
      { id: 'c3', name: 'C3: Riesenspinnen-Bombe', cost: 45, cd: 3, dmg: [45, 58], effect: 'debuff', debuff: { def: 0.8, rounds: 2 }, desc: 'Zersetzt die Verteidigung des Ziels.' },
      { id: 'c0', name: 'C0: Ultimative Kunst', cost: 70, cd: 6, dmg: [80, 100], effect: 'recoil', recoilPct: 0.15, desc: 'Die ultimative Selbstexplosion – riskant, aber verheerend.' }
    ],
    awakening: { name: '"Wahre Kunst" – Maximale Sprengkraft', unlockLevel: 15, statMult: 1.5, desc: 'Jede Kreation wird zu einem Meisterwerk der Zerstörung.' }
  },

  tobi: {
    id: 'tobi',
    name: 'Tobi / Obito Uchiha',
    title: 'Der Maskierte',
    icon: '🌀',
    element: 'Kamui-Raumzeit',
    baseStats: { hp: 135, chakra: 125, atk: 23, def: 17, spd: 23 },
    passive: {
      name: 'Kamui-Phasenverschiebung',
      desc: '20% Chance, durch Intangibilität einem gegnerischen Angriff vollständig auszuweichen.'
    },
    jutsu: [
      { id: 'kamui_warp', name: 'Kamui-Warp', cost: 30, cd: 2, dmg: [35, 45], desc: 'Teleportiert einen Angriff direkt in eine andere Dimension und wieder zurück.' },
      { id: 'gouryuuka', name: 'Katon: Gōryūka', cost: 25, cd: 2, dmg: [28, 38], desc: 'Ein feuriger Drache aus den Flammen.' },
      { id: 'chibaku_tensei', name: 'Chibaku Tensei', cost: 65, cd: 5, dmg: [60, 80], effect: 'stun', stunRounds: 1, desc: 'Erschafft einen künstlichen Mond, der den Gegner unter sich begräbt.' },
      { id: 'maskentaeuschung', name: '"Ich bin nur Tobi"', cost: 0, cd: 3, dmg: [0, 0], effect: 'buff_chakra', buff: { spd: 1.3, rounds: 2 }, chakraRestore: 15, desc: 'Trickserei verwirrt den Gegner und verschafft dir Vorteile.' }
    ],
    awakening: { name: 'Jinchūriki des Jūbi', unlockLevel: 15, statMult: 1.5, desc: 'Rieseigem Chakra des Zehnschwänzigen entfesselt.' }
  },

  toneri: {
    id: 'toneri',
    name: 'Toneri Ōtsutsuki',
    title: 'Der Mondprinz',
    icon: '🌕',
    element: 'Tenseigan',
    baseStats: { hp: 160, chakra: 150, atk: 30, def: 20, spd: 16 },
    passive: {
      name: 'Ōtsutsuki-Blut',
      desc: 'Immun gegen Betäubungs-Effekte. Permanent +5% auf alle Statuswerte.'
    },
    jutsu: [
      { id: 'tenseigan_strahl', name: 'Tenseigan-Strahl', cost: 40, cd: 2, dmg: [42, 55], desc: 'Ein gebündelter Energiestrahl von unglaublicher Kraft.' },
      { id: 'karasu_tengu', name: 'Karasu Tengu – Puppenlegion', cost: 30, cd: 2, dmg: [25, 35], effect: 'multi2', desc: 'Mehrere Kampfpuppen greifen gleichzeitig an.' },
      { id: 'mondfall', name: 'Mondfall-Technik', cost: 70, cd: 6, dmg: [75, 95], desc: 'Lässt einen Meteor aus dem zerbrochenen Mond herabstürzen.' },
      { id: 'tenseigan_fusion', name: 'Byakugan-Tenseigan-Fusion', cost: 50, cd: 4, dmg: [10, 15], effect: 'buff_all', buff: { atk: 1.25, def: 1.25, spd: 1.25, rounds: 3 }, desc: 'Vereint uraltes Blut zu einem gottgleichen Zustand.' }
    ],
    awakening: { name: 'Erwachtes Tenseigan – Gott der Zerstörung', unlockLevel: 15, statMult: 1.6, desc: 'Der Mond selbst gehorcht seinem Willen.' }
  }
};

const CHAR_LIST = Object.values(CHARACTERS);
const CHAR_ALIASES = {
  naruto: 'naruto',
  sasuke: 'sasuke',
  hinata: 'hinata',
  kakashi: 'kakashi',
  itachi: 'itachi',
  deidara: 'deidara',
  tobi: 'tobi', obito: 'tobi',
  toneri: 'toneri'
};

// ─────────────────────────────────────────────────────────────
// GEGNER-POOL (PvE)
// ─────────────────────────────────────────────────────────────

const ENEMY_POOL = [
  { name: 'Nuke-nin Plünderer', icon: '🥷', tier: 1, hp: 60, atk: 10, def: 5, spd: 8, xp: 15, coins: [20, 40] },
  { name: 'Oto-Söldner', icon: '🗡️', tier: 1, hp: 70, atk: 12, def: 6, spd: 10, xp: 18, coins: [25, 45] },
  { name: 'Wanderndes Marionetten-Konstrukt', icon: '🎎', tier: 2, hp: 100, atk: 16, def: 10, spd: 9, xp: 28, coins: [40, 70] },
  { name: 'Edo-Tensei-Schatten', icon: '💀', tier: 2, hp: 110, atk: 18, def: 8, spd: 12, xp: 32, coins: [45, 75] },
  { name: 'Akatsuki-Späher', icon: '☁️', tier: 3, hp: 140, atk: 22, def: 12, spd: 14, xp: 45, coins: [60, 100] },
  { name: 'Verlorener Jinchūriki-Splitter', icon: '🐗', tier: 3, hp: 160, atk: 24, def: 10, spd: 11, xp: 50, coins: [65, 110] },
  { name: 'Weißer Zetsu-Klon', icon: '🌱', tier: 4, hp: 180, atk: 26, def: 14, spd: 15, xp: 65, coins: [80, 130] },
  { name: 'Zehnschwänziger Splitter-Konstrukt', icon: '🌌', tier: 5, hp: 260, atk: 32, def: 18, spd: 16, xp: 100, coins: [120, 200], boss: true }
];

// ─────────────────────────────────────────────────────────────
// PERSISTENZ
// ─────────────────────────────────────────────────────────────

export function createNarutoSystem(DATA_PATH) {
  const FILE_PATH = path.join(DATA_PATH, 'naruto.json');

  function loadData() {
    try {
      if (!fs.existsSync(FILE_PATH)) {
        fs.writeFileSync(FILE_PATH, JSON.stringify({}, null, 2));
        return {};
      }
      const raw = fs.readFileSync(FILE_PATH, 'utf-8');
      return raw.trim() ? JSON.parse(raw) : {};
    } catch (e) {
      console.error('[naruto] Fehler beim Laden:', e);
      return {};
    }
  }

  function saveData() {
    try {
      fs.writeFileSync(FILE_PATH, JSON.stringify(narutoData, null, 2));
    } catch (e) {
      console.error('[naruto] Fehler beim Speichern:', e);
    }
  }

  let narutoData = loadData();
  const activeBattles = new Map(); // jid -> battle state (in-memory)

  function ensureNarutoUser(jid) {
    if (!narutoData[jid]) {
      narutoData[jid] = {
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
    return narutoData[jid];
  }

  function xpNeeded(level) {
    return 60 + level * 40;
  }

  function addNarutoXp(jid, amount, send, mentionJid) {
    const nu = ensureNarutoUser(jid);
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
    const nu = ensureNarutoUser(jid);
    const char = getCharacter(nu.main);
    if (!char) return null;
    const isAwakened = nu.awakened && nu.level >= char.awakening.unlockLevel;
    const mult = isAwakened ? char.awakening.statMult : 1;
    const growth = 1 + (nu.level - 1) * 0.04; // leichtes Wachstum pro Level
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
    const varyPct = (Math.random() * 0.3) - 0.15; // ±15%
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

  function createBattleFighter(stats, char) {
    return {
      hp: stats.hp, hpMax: stats.hp,
      chakra: stats.chakra, chakraMax: stats.chakra,
      atk: stats.atk, def: stats.def, spd: stats.spd,
      buffs: [], stunned: false, cooldowns: {}, burn: null,
      lastAttackDmg: 0
    };
  }

  function startMissionBattle(jid, nu, char, stats, enemyTemplate) {
    const battle = {
      type: 'mission',
      char,
      player: createBattleFighter(stats, char),
      enemy: {
        ...enemyTemplate,
        hp: enemyTemplate.hp, hpMax: enemyTemplate.hp,
        buffs: [], stunned: false, lastAttackDmg: 0
      },
      round: 1,
      log: []
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
        const d = Math.round(computeDamage(jutsu.dmg, atkVal, e.def, randInt) / hits * 1.4);
        total += d;
      }
      e.hp -= total;
      lines.push(`${jutsu.name}: ${hits} Treffer für insgesamt ${total} Schaden!`);
    } else if (jutsu.effect === 'stun') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      if (!e.immuneStun) { e.stunned = true; e.stunRoundsLeft = jutsu.stunRounds; }
      lines.push(`${jutsu.name} trifft für ${d} Schaden und betäubt den Gegner!`);
    } else if (jutsu.effect === 'burn') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      e.burn = { dmg: jutsu.burn.dmg, rounds: jutsu.burn.rounds };
      lines.push(`${jutsu.name} trifft für ${d} Schaden und entfacht Flammen!`);
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
    } else if (jutsu.effect === 'buff_chakra') {
      p.chakra = Math.min(p.chakraMax, p.chakra + (jutsu.chakraRestore || 0));
      for (const [stat, val] of Object.entries(jutsu.buff)) {
        if (stat === 'rounds') continue;
        p.buffs.push({ stat, mult: val, rounds: jutsu.buff.rounds });
      }
      lines.push(`${jutsu.name} verwirrt den Gegner – +${jutsu.chakraRestore || 0} Chakra & Tempo-Boost!`);
    } else if (jutsu.effect === 'chakra_drain') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      lines.push(`${jutsu.name} trifft für ${d} Schaden und blockiert Chakrapunkte!`);
    } else if (jutsu.effect === 'recoil') {
      const d = computeDamage(jutsu.dmg, atkVal, e.def, randInt);
      e.hp -= d;
      const recoil = Math.round(p.hpMax * jutsu.recoilPct);
      p.hp -= recoil;
      lines.push(`${jutsu.name} verursacht ${d} Schaden – der Rückstoß kostet dich ${recoil} HP!`);
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
    // Dodge-Chance (Byakugan / Kamui Passiv wird oben separat behandelt)
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
    // Passiv: Uzumaki-Vitalität
    if (char.id === 'naruto') {
      const regen = Math.round(p.hpMax * 0.03);
      p.hp = Math.min(p.hpMax, p.hp + regen);
      lines.push(`💚 Uzumaki-Vitalität heilt dich um ${regen} HP.`);
    }
    if (e.burn) {
      const bd = Math.round((e.burn.dmg[0] + e.burn.dmg[1]) / 2);
      e.hp -= bd;
      e.burn.rounds--;
      lines.push(`🔥 Verbrennung fügt dem Gegner ${bd} Schaden zu.`);
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

  function battleStatusText(battle, nu) {
    const char = battle.char;
    const p = battle.player, e = battle.enemy;
    return (
      `⚔️ *${char.icon} ${char.name}* vs *${e.icon || '👤'} ${e.name}*\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
      `Du:     ${hpBar(p.hp, p.hpMax)}\n` +
      `Chakra: ${p.chakra}/${p.chakraMax}\n` +
      `Gegner: ${hpBar(e.hp, e.hpMax)}\n` +
      `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
      `Runde ${battle.round}`
    );
  }

  async function resolveMissionOutcome(battle, jid, nu, char, send, sock, users, save, FILES) {
    const p = battle.player, e = battle.enemy;
    if (e.hp <= 0) {
      activeBattles.delete(jid);
      const coins = e.coins ? Math.round((e.coins[0] + e.coins[1]) / 2 * (e.boss ? 1.5 : 1)) : 30;
      const xp = e.xp || 20;
      if (users[jid]) { users[jid].coins = (users[jid].coins || 0) + coins; save(FILES.users, users); }
      nu.missionsCompleted = (nu.missionsCompleted || 0) + 1;
      const leveledUp = addNarutoXp(jid, xp);
      let out = `🎉 Sieg! Du hast *${e.name}* besiegt!\n💰 +${coins} Coins | ✨ +${xp} Naruto-XP`;
      if (leveledUp) out += `\n\n📈 Level Up! Du bist jetzt Naruto-Level ${nu.level}.`;
      if (!nu.awakened && nu.level >= char.awakening.unlockLevel) {
        out += `\n\n🌟 *${char.awakening.name}* kann jetzt mit "erwachen" freigeschaltet werden!`;
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
    let out = `🍥 *— NARUTO-CHARAKTERE —* 🍥\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
    for (const c of CHAR_LIST) {
      out += `${c.icon} *${c.name}* (${c.id}) — ${c.title}\n`;
      out += `   ${c.element} | HP ${c.baseStats.hp} | Chakra ${c.baseStats.chakra} | ATK ${c.baseStats.atk} | DEF ${c.baseStats.def} | SPD ${c.baseStats.spd}\n`;
      out += `   Passiv: ${c.passive.name} — ${c.passive.desc}\n\n`;
    }
    out += `Wähle deinen Hauptcharakter mit:\n${prefix}naruto wähle <name>\n`;
    out += `Danach einen Partner mit:\n${prefix}naruto partner <name>`;
    return out;
  }

  function jutsuListText(char, prefix) {
    let out = `${char.icon} *Jutsu-Liste — ${char.name}*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
    for (const j of char.jutsu) {
      out += `• *${j.name}* (${prefix}naruto jutsu ${j.id})\n   Kosten: ${j.cost} Chakra | CD: ${j.cd} Runden\n   ${j.desc}\n\n`;
    }
    out += `Passiv: *${char.passive.name}* — ${char.passive.desc}\n`;
    out += `Erwachen (ab Lv.${char.awakening.unlockLevel}): *${char.awakening.name}* — ${char.awakening.desc}`;
    return out;
  }

  const NARUTO_HELP_TEXT =
    `🍥 *— NARUTO-SYSTEM —* 🍥\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `?naruto start — Charakterübersicht\n` +
    `?naruto wähle <name> — Hauptcharakter wählen\n` +
    `?naruto partner <name> — Partnercharakter wählen\n` +
    `?naruto profil — Deine Werte anzeigen\n` +
    `?naruto jutsuliste — Deine Jutsu anzeigen\n` +
    `?naruto mission — PvE-Kampf starten\n` +
    `?naruto jutsu <id> — Jutsu im Kampf einsetzen\n` +
    `?naruto angriff — Normaler Angriff\n` +
    `?naruto verteidigen — Schaden reduzieren & Chakra sammeln\n` +
    `?naruto flucht — Aus dem Kampf fliehen\n` +
    `?naruto erwachen — Erwachen aktivieren (ab Lv.15)\n` +
    `?naruto duell @user — PvP-Duell herausfordern\n` +
    `?naruto rangliste — Bestenliste\n` +
    `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
    `Charaktere: Naruto, Sasuke, Hinata, Kakashi, Itachi, Deidara, Tobi/Obito, Toneri`;

  const NARUTO_COMMANDS = ['naruto', 'nrt'];

  // ─────────────────────────────────────────────────────────
  // HAUPT-HANDLER
  // ─────────────────────────────────────────────────────────

  async function handle(ctx) {
    const {
      cmd, args, sender, from, m, isGroup, activePrefix, send, sock,
      users, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, randInt, sleep, isPrimaryOwner
    } = ctx;

    if (!NARUTO_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    ensureUser(jid);
    const nu = ensureNarutoUser(jid);
    const sub = (args[0] || '').toLowerCase();

    // ---- Hilfe ----
    if (!sub || sub === 'help' || sub === 'hilfe') {
      await send(NARUTO_HELP_TEXT);
      return true;
    }

    // ---- Charakterübersicht / Start ----
    if (sub === 'start' || sub === 'charaktere' || sub === 'characters') {
      await send(characterListText(activePrefix));
      return true;
    }

    // ---- Hauptcharakter wählen ----
    if (sub === 'wähle' || sub === 'waehle' || sub === 'select') {
      const raw = (args[1] || '').toLowerCase();
      const charId = CHAR_ALIASES[raw];
      if (!charId) {
        await send(`❌ Unbekannter Charakter. Nutze ${activePrefix}naruto start für die Liste.`);
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
      await send(`✅ *${char.icon} ${char.name}* ist jetzt dein Hauptcharakter!\n\nWähle jetzt noch einen Partner mit:\n${activePrefix}naruto partner <name>`);
      return true;
    }

    // ---- Partner wählen ----
    if (sub === 'partner') {
      const raw = (args[1] || '').toLowerCase();
      const charId = CHAR_ALIASES[raw];
      if (!charId) {
        await send(`❌ Unbekannter Charakter. Nutze ${activePrefix}naruto start für die Liste.`);
        return true;
      }
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Hauptcharakter mit ${activePrefix}naruto wähle <name>.`);
        return true;
      }
      if (nu.main === charId) {
        await send('❌ Haupt- und Partner-Charakter dürfen nicht identisch sein.');
        return true;
      }
      nu.partner = charId;
      saveData();
      const char = getCharacter(charId);
      await send(`✅ *${char.icon} ${char.name}* unterstützt dich jetzt als Partner!\n\nStarte deine erste Mission mit ${activePrefix}naruto mission`);
      return true;
    }

    // ---- Profil ----
    if (sub === 'profil' || sub === 'profile') {
      if (!nu.main) {
        await send(`❌ Du hast noch keinen Charakter gewählt. Nutze ${activePrefix}naruto start.`);
        return true;
      }
      const char = getCharacter(nu.main);
      const partner = nu.partner ? getCharacter(nu.partner) : null;
      const stats = currentStats(jid);
      const nextXp = xpNeeded(nu.level);
      const out =
        `👤 *Naruto-Profil*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${char.icon} Hauptcharakter: *${char.name}*${stats.isAwakened ? ` (${char.awakening.name} 🌟)` : ''}\n` +
        `${partner ? `${partner.icon} Partner: *${partner.name}*\n` : ''}` +
        `⭐ Level: ${nu.level} (${nu.xp}/${nextXp} XP)\n` +
        `❤️ HP: ${stats.hp} | 🔷 Chakra: ${stats.chakra}\n` +
        `⚔️ ATK: ${stats.atk} | 🛡️ DEF: ${stats.def} | 💨 SPD: ${stats.spd}\n` +
        `🏆 Siege: ${nu.wins || 0} | ☠️ Niederlagen: ${nu.losses || 0}\n` +
        `📜 Missionen abgeschlossen: ${nu.missionsCompleted || 0}\n` +
        (nu.level >= char.awakening.unlockLevel && !nu.awakened
          ? `\n🌟 Erwachen verfügbar! Nutze ${activePrefix}naruto erwachen`
          : '');
      await send(out);
      return true;
    }

    // ---- Jutsu-Liste ----
    if (sub === 'jutsuliste' || sub === 'jutsu-liste' || sub === 'skills') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}naruto start.`);
        return true;
      }
      await send(jutsuListText(getCharacter(nu.main), activePrefix));
      return true;
    }

    // ---- Erwachen ----
    if (sub === 'erwachen' || sub === 'awaken') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}naruto start.`);
        return true;
      }
      const char = getCharacter(nu.main);
      if (nu.level < char.awakening.unlockLevel) {
        await send(`❌ Erwachen erfordert Naruto-Level ${char.awakening.unlockLevel}. Du bist Level ${nu.level}.`);
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
      const entries = Object.entries(narutoData).filter(([, v]) => v.main);
      if (!entries.length) return send('📊 Noch keine Naruto-Charaktere gewählt.'), true;
      const sorted = entries.sort((a, b) => (b[1].level * 1000 + b[1].wins) - (a[1].level * 1000 + a[1].wins));
      const top = sorted.slice(0, 10);
      const medals = ['🥇', '🥈', '🥉'];
      const lines = await Promise.all(top.map(async ([j, v], i) => {
        const char = getCharacter(v.main);
        const name = await getNumberMention(j, sock);
        return `${medals[i] || `${i + 1}.`} ${char?.icon || '🍥'} ${name} — Lv.${v.level} (${v.wins || 0} Siege) — ${char?.name || '?'}`;
      }));
      const mentions = top.map(([j]) => j);
      await send(`🍥 *Naruto-Rangliste*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`, { mentions });
      return true;
    }

   
    if (sub === 'mission' || sub === 'missionen') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}naruto start.`);
        return true;
      }
      if (activeBattles.has(jid)) {
        const battle = activeBattles.get(jid);
        await send(`⚔️ Du bist bereits in einem Kampf!\n\n${battleStatusText(battle, nu)}`);
        return true;
      }
      const char = getCharacter(nu.main);
      const stats = currentStats(jid);
      const enemy = pickEnemyForLevel(nu.level, randInt);
      const battle = startMissionBattle(jid, nu, char, stats, enemy);
      await send(
        `📜 *Neue Mission!*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `Ein ${enemy.icon} *${enemy.name}* stellt sich dir in den Weg!${enemy.boss ? ' ⚠️ BOSS-GEGNER!' : ''}\n\n` +
        `${battleStatusText(battle, nu)}\n\n` +
        `Nutze ${activePrefix}naruto jutsu <id>, ${activePrefix}naruto angriff oder ${activePrefix}naruto verteidigen.`
      );
      return true;
    }

    
    if (['jutsu', 'angriff', 'verteidigen', 'flucht'].includes(sub)) {
      const battle = activeBattles.get(jid);
      if (!battle || battle.type !== 'mission') {
        await send(`❌ Du bist in keiner aktiven Mission. Starte eine mit ${activePrefix}naruto mission.`);
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
        actionLine = `👊 Normaler Angriff trifft für ${d} Schaden! (+8 Chakra)`;
      } else if (sub === 'verteidigen') {
        p.buffs.push({ stat: 'def', mult: 1.5, rounds: 1 });
        p.chakra = Math.min(p.chakraMax, p.chakra + 20);
        actionLine = '🛡️ Du gehst in Deckung — Verteidigung erhöht, +20 Chakra gesammelt.';
      } else if (sub === 'jutsu') {
        const jutsuId = (args[1] || '').toLowerCase();
        const jutsu = char.jutsu.find(j => j.id === jutsuId || j.name.toLowerCase() === jutsuId);
        if (!jutsu) {
          await send(`❌ Unbekanntes Jutsu. Nutze ${activePrefix}naruto jutsuliste zur Übersicht.`);
          return true;
        }
        if (p.cooldowns[jutsu.id] > 0) {
          await send(`⏳ *${jutsu.name}* ist noch ${p.cooldowns[jutsu.id]} Runde(n) auf Cooldown.`);
          return true;
        }
        if (p.chakra < jutsu.cost) {
          await send(`❌ Nicht genug Chakra für *${jutsu.name}* (benötigt: ${jutsu.cost}, du hast: ${p.chakra}).`);
          return true;
        }
        p.chakra -= jutsu.cost;
        p.cooldowns[jutsu.id] = jutsu.cd;
        actionLine = `${char.icon} ${playerJutsuAction(battle, jutsu, randInt)}`;
      }

      // Partner-Assist (kleine Chance auf Bonusschaden)
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
        await resolveMissionOutcome(battle, jid, nu, char, send, sock, users, save, FILES);
        return true;
      }

      
      let enemyLine = '';
      let dodged = false;
      if (char.id === 'hinata' && Math.random() < 0.2) dodged = true;
      if (char.id === 'tobi' && Math.random() < 0.2) dodged = true;
      if (char.id === 'sasuke' && Math.random() < 0.15) {
        
      }
      let itachiNegate = false;
      if (char.id === 'itachi' && Math.random() < 0.15) itachiNegate = true;

      if (dodged) {
        enemyLine = `💨 Du weichst dem Angriff von *${e.name}* vollständig aus!`;
      } else if (itachiNegate) {
        enemyLine = `🌀 *${e.name}* verliert sich in einer Illusion und verliert seine Runde!`;
      } else {
        enemyLine = enemyAction(battle, randInt);
        if (char.id === 'sasuke' && Math.random() < 0.15) {
          const halved = Math.round(p.lastAttackDmg / 2);
          p.hp += (p.lastAttackDmg - halved);
          p.lastAttackDmg = halved;
          enemyLine += `\n👁️ Sharingan-Einsicht halbiert den Schaden auf ${halved}!`;
        }
        if (char.id === 'kakashi' && Math.random() < 0.10) {
          const bonus = Math.max(5, Math.round(p.lastAttackDmg * 0.8));
          e.hp -= bonus;
          enemyLine += `\n🐺 Sharingan kopiert die Technik — ${bonus} Bonus-Schaden zurück!`;
        }
      }

      const roundLines = endOfRoundEffects(battle, char);
      out += `\n${enemyLine}`;
      if (roundLines.length) out += `\n${roundLines.join('\n')}`;

      

      if (p.hp <= 0) {
        await send(out);
        await resolveMissionOutcome(battle, jid, nu, char, send, sock, users, save, FILES);
        return true;
      }
      // Gegner nach Burn besiegt?
      if (e.hp <= 0) {
        await send(out);
        await resolveMissionOutcome(battle, jid, nu, char, send, sock, users, save, FILES);
        return true;
      }

      out += `\n\n${battleStatusText(battle, nu)}`;
      await send(out);
      return true;
    }

    
    if (sub === 'duell' || sub === 'duel') {
      if (!nu.main) {
        await send(`❌ Wähle zuerst einen Charakter mit ${activePrefix}naruto start.`);
        return true;
      }
      const ctxMsg = m.message?.extendedTextMessage?.contextInfo;
      let targetRaw = args[1];
      if (!targetRaw && ctxMsg?.mentionedJid?.length) targetRaw = ctxMsg.mentionedJid[0];
      if (!targetRaw && ctxMsg?.participant) targetRaw = ctxMsg.participant;
      if (!targetRaw) {
        await send(`❌ Nutzung: ${activePrefix}naruto duell @user`);
        return true;
      }
      const targetJid = normalizeJid(targetRaw);
      if (isSameJid(targetJid, jid)) {
        await send('❌ Du kannst nicht gegen dich selbst duellieren!');
        return true;
      }
      ensureUser(targetJid);
      const targetNu = ensureNarutoUser(targetJid);
      if (!targetNu.main) {
        await send(`❌ @${targetJid.split('@')[0]} hat noch keinen Naruto-Charakter gewählt.`, { mentions: [targetJid] });
        return true;
      }

      const char1 = getCharacter(nu.main);
      const char2 = getCharacter(targetNu.main);
      const s1 = currentStats(jid);
      const s2 = currentStats(targetJid);
      const f1 = createBattleFighter(s1, char1);
      const f2 = createBattleFighter(s2, char2);

      const log = [];
      let round = 1;
      const MAX_ROUNDS = 20;

      function pickAiJutsu(char, fighter) {
        const usable = char.jutsu.filter(j => (fighter.cooldowns[j.id] || 0) <= 0 && fighter.chakra >= j.cost);
        if (!usable.length) return null;
        return usable[randInt(0, usable.length - 1)];
      }

      while (f1.hp > 0 && f2.hp > 0 && round <= MAX_ROUNDS) {
        for (const k of Object.keys(f1.cooldowns)) if (f1.cooldowns[k] > 0) {}
        
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

      let resultText = `⚔️ *— NARUTO-DUELL —* ⚔️\n${char1.icon} ${char1.name} vs ${char2.icon} ${char2.name}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
      resultText += log.slice(-10).join('\n');
      resultText += `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;

      if (winnerJid) {
        const wNu = ensureNarutoUser(winnerJid);
        const lNu = ensureNarutoUser(loserJid);
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

    await send(`❓ Unbekannter Naruto-Befehl. Nutze ${activePrefix}naruto help.`);
    return true;
  }

  return { handle, NARUTO_HELP_TEXT, NARUTO_COMMANDS, CHARACTERS };
}
