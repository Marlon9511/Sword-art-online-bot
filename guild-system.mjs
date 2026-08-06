/* =====================================================================
   ⚔️ SAO ARENA-SYSTEM — Modul
   =====================================================================
   Eigenständiges, komplett fertiges Modul. Verwaltet:
   - Item-Datenbank (Waffen & Rüstungen, 5 Seltenheitsstufen + Secret)
   - Lootboxen ("kiste" im Shop)
   - Ausrüsten / Ablegen
   - Verkaufen von Items
   - PVP-Duelle mit Coin-Einsatz
   - Arena-Leaderboard (meiste Siege)
   - Floor-System (Aincrad-Fortschrittsanzeige)

   Speichert alles direkt in eurem bestehenden users-Objekt
   (users[jid].items, users[jid].equipped, users[jid].duel) — es werden
   KEINE neuen JSON-Dateien gebraucht. Persistiert wird über euer
   vorhandenes save(FILES.users, users).

   Einbau in bot.js: siehe integration-patch.txt (nur 4 Zeilen nötig).
   ===================================================================== */

// ---------------------------------------------------------------------
// Item-Datenbank
// ---------------------------------------------------------------------
export const RARITY_INFO = {
  common:    { label: 'Gewöhnlich',   emoji: '⚪', weight: 45 },
  uncommon:  { label: 'Ungewöhnlich', emoji: '🟢', weight: 30 },
  rare:      { label: 'Selten',       emoji: '🔵', weight: 15 },
  epic:      { label: 'Episch',       emoji: '🟣', weight: 8 },
  legendary: { label: 'Legendär',     emoji: '🟡', weight: 2 },
  secret:    { label: 'Geheim',       emoji: '⚫', weight: 0 } // nie zufällig ziehbar, nur exklusiv vergeben
};

// type: 'weapon' | 'armor'
export const ITEM_DB = {
  // ---- WAFFEN ----
  w_common_1:    { name: 'Rostiges Schwert',        type: 'weapon', rarity: 'common',    power: 8 },
  w_common_2:    { name: 'Holzstab',                type: 'weapon', rarity: 'common',    power: 6 },
  w_common_3:    { name: 'Alter Dolch',             type: 'weapon', rarity: 'common',    power: 7 },
  w_uncommon_1:  { name: 'Stahlschwert',            type: 'weapon', rarity: 'uncommon',  power: 16 },
  w_uncommon_2:  { name: 'Kampfaxt',                type: 'weapon', rarity: 'uncommon',  power: 18 },
  w_uncommon_3:  { name: 'Kurzbogen',               type: 'weapon', rarity: 'uncommon',  power: 15 },
  w_rare_1:      { name: 'Silberklinge',            type: 'weapon', rarity: 'rare',      power: 28 },
  w_rare_2:      { name: 'Kristalldolch',           type: 'weapon', rarity: 'rare',      power: 26 },
  w_rare_3:      { name: 'Kriegshammer',            type: 'weapon', rarity: 'rare',      power: 30 },
  w_epic_1:      { name: 'Nachtschattenklinge',     type: 'weapon', rarity: 'epic',      power: 45 },
  w_epic_2:      { name: 'Flammenschwert',          type: 'weapon', rarity: 'epic',      power: 48 },
  w_epic_3:      { name: 'Sturmspeer',              type: 'weapon', rarity: 'epic',      power: 46 },
  w_legendary_1: { name: 'Elucidator',              type: 'weapon', rarity: 'legendary', power: 70 },
  w_legendary_2: { name: 'Dark Repulser',           type: 'weapon', rarity: 'legendary', power: 68 },
  w_legendary_3: { name: 'Lambent Light',           type: 'weapon', rarity: 'legendary', power: 66 },

  // ---- RÜSTUNGEN ----
  a_common_1:    { name: 'Lederrüstung',            type: 'armor',  rarity: 'common',    power: 8 },
  a_common_2:    { name: 'Stoffmantel',             type: 'armor',  rarity: 'common',    power: 6 },
  a_common_3:    { name: 'Einfacher Schild',        type: 'armor',  rarity: 'common',    power: 7 },
  a_uncommon_1:  { name: 'Kettenhemd',              type: 'armor',  rarity: 'uncommon',  power: 16 },
  a_uncommon_2:  { name: 'Verstärkte Weste',        type: 'armor',  rarity: 'uncommon',  power: 18 },
  a_uncommon_3:  { name: 'Eisenschild',             type: 'armor',  rarity: 'uncommon',  power: 15 },
  a_rare_1:      { name: 'Silberharnisch',          type: 'armor',  rarity: 'rare',      power: 28 },
  a_rare_2:      { name: 'Drachenschuppen-Umhang',  type: 'armor',  rarity: 'rare',      power: 30 },
  a_rare_3:      { name: 'Kristallschild',          type: 'armor',  rarity: 'rare',      power: 26 },
  a_epic_1:      { name: 'Nachtschatten-Rüstung',   type: 'armor',  rarity: 'epic',      power: 46 },
  a_epic_2:      { name: 'Phönixmantel',            type: 'armor',  rarity: 'epic',      power: 45 },
  a_epic_3:      { name: 'Titanplatte',             type: 'armor',  rarity: 'epic',      power: 48 },
  a_legendary_1: { name: 'Coat of Midnight',        type: 'armor',  rarity: 'legendary', power: 70 },
  a_legendary_2: { name: 'Rune des Kobold-Königs',  type: 'armor',  rarity: 'legendary', power: 68 },
  a_legendary_3: { name: 'Himmlischer Panzer',      type: 'armor',  rarity: 'legendary', power: 66 },

  // ---- SECRET-POOL (via {P}openkiste mit 1:1000-Chance, keine Anzeige in {P}arenaitems) ----
  // Bei einem Secret-Treffer wird zufällig EINES dieser Items vergeben.
  w_secret_dualblades: {
    name: 'Holzstab',
    trueName: 'Kiritos Doppelklingen (Dual Blades)',
    type: 'weapon',
    rarity: 'legendary',
    power: 150,
    secret: true
  },
  w_secret_liberator: {
    name: 'Rostiger schwarzer Rapier',
    trueName: 'Liberator — Asunas Rapier aus Neu-Aincrad',
    type: 'weapon',
    rarity: 'legendary',
    power: 130,
    secret: true
  },
  w_secret_lightflash: {
    name: 'Abgenutztes Übungsrapier',
    trueName: 'Lightning Flash — Asunas erstes Rapier',
    type: 'weapon',
    rarity: 'epic',
    power: 95,
    secret: true
  },
  w_secret_bluerose: {
    name: 'Zersplitterte blaue Klinge',
    trueName: 'Blue Rose Sword',
    type: 'weapon',
    rarity: 'legendary',
    power: 135,
    secret: true
  },
  w_secret_nightsky: {
    name: 'Schwarzes Schattenschwert',
    trueName: 'Night Sky Sword — Klinge des Dunklen Ritters',
    type: 'weapon',
    rarity: 'legendary',
    power: 140,
    secret: true
  },
  w_secret_holyblade: {
    name: 'Verzierte gesegnete Klinge',
    trueName: 'Heathcliffs Heilige Klinge — Rache des Systemadministrators',
    type: 'weapon',
    rarity: 'legendary',
    power: 155,
    secret: true
  },
  a_secret_bwcoat: {
    name: 'Zerschlissener schwarzer Mantel',
    trueName: 'Blackwyrm Coat',
    type: 'armor',
    rarity: 'legendary',
    power: 120,
    secret: true
  },
  a_secret_negacloak: {
    name: 'Nachtschwarzer Umhang',
    trueName: 'Umhang der Laughing Coffin',
    type: 'armor',
    rarity: 'legendary',
    power: 125,
    secret: true
  },
  a_secret_flashcoat: {
    name: 'Zerrissene rote Weste',
    trueName: 'Flash-Panzerung — Asunas Kommandantinnen-Rüstung',
    type: 'armor',
    rarity: 'legendary',
    power: 130,
    secret: true
  },
  a_secret_bloodoath: {
    name: 'Rostiger Plattenpanzer',
    trueName: 'Rüstung der Blutschwur-Ritter (Knights of the Blood Oath)',
    type: 'armor',
    rarity: 'legendary',
    power: 128,
    secret: true
  },

  // ---- EXCALIBUR — ausschließlich für den Haupt-Owner, niemals via Kiste/Shop erhältlich ----
  w_excalibur: {
    name: 'Excalibur',
    trueName: 'Das Schwert des Systemadministrators',
    type: 'weapon',
    rarity: 'secret',
    power: 10000,
    secret: true,
    ownerOnly: true
  },

  // ---- AEGIS DES SYSTEMADMINISTRATORS — Rüstungs-Pendant zu Excalibur,
  // ausschließlich für den Haupt-Owner, niemals via Kiste/Shop erhältlich ----
  a_aegis: {
    name: 'Aegis des Systemadministrators',
    trueName: 'Der unzerbrechliche Schild-Mantel von Kayaba Akihiko',
    type: 'armor',
    rarity: 'secret',
    power: 8000,
    secret: true,
    ownerOnly: true
  }
};

// Feste Referenz auf das "klassische" Secret-Item (weiterhin für ?kiritossecret genutzt)
export const SECRET_ITEM_ID = 'w_secret_dualblades';
export const EXCALIBUR_ITEM_ID = 'w_excalibur';
export const AEGIS_ITEM_ID = 'a_aegis';

// Pool aller Secret-Items, die per Kiste (1:1000) fallen können.
// ownerOnly-Items (Excalibur) sind hiervon immer ausgeschlossen.
export const SECRET_POOL = Object.keys(ITEM_DB).filter(
  (id) => ITEM_DB[id].secret && !ITEM_DB[id].ownerOnly
);

export const ARENA_SHOP_ITEM = {
  id: 'kiste',
  price: 750,
  desc: '⚔️ SAO-Ausrüstungskiste: zufällige Waffe oder Rüstung'
};

// ---------------------------------------------------------------------
// Verkaufspreise (nach Seltenheit). Secret-Items bringen einen Aufschlag.
// ---------------------------------------------------------------------
export const SELL_PRICES = {
  common: 50,
  uncommon: 150,
  rare: 400,
  epic: 900,
  legendary: 2000,
  secret: 5000
};

export function getSellPrice(itemId) {
  const it = ITEM_DB[itemId];
  if (!it) return 0;
  let price = SELL_PRICES[it.rarity] || 0;
  if (it.secret) price = Math.round(price * 1.5);
  return price;
}

export const ARENA_COMMANDS = [
  'openkiste', 'kisteoeffnen', 'openbox', 'gear', 'ausruestung', 'equipment',
  'equip', 'unequip', 'sell', 'verkaufen', 'duell', 'duel', 'arena', 'duelleaderboard',
  'kampfrangliste', 'arenaitems', 'itemliste', 'floor', 'etage', 'excalibur', 'aegis'
];

export const ARENA_HELP_TEXT =
  `▸ {P}buy kiste — Ausrüstungskiste kaufen (${ARENA_SHOP_ITEM.price} Coins)\n` +
  `▸ {P}openkiste — Kiste öffnen (zufällige Waffe/Rüstung)\n` +
  `▸ {P}gear — Ausrüstung & Inventar anzeigen\n` +
  `▸ {P}equip <id> — Waffe/Rüstung ausrüsten\n` +
  `▸ {P}unequip weapon|armor — Ausrüstung ablegen\n` +
  `▸ {P}sell <id> [anzahl|all] — Item(s) verkaufen\n` +
  `▸ {P}duell @user <einsatz> — Zum Duell herausfordern\n` +
  `▸ {P}duell accept/deny/cancel — Duell verwalten\n` +
  `▸ {P}arena — Arena-Rangliste (meiste Siege)\n` +
  `▸ {P}arenaitems — Alle erhältlichen Waffen & Rüstungen anzeigen\n` +
  `▸ {P}floor — Deinen Floor-Fortschritt in Aincrad anzeigen\n` +
  `_Es gibt außerdem geheime Ausrüstung, die niemand kennt, bis sie gefunden wird..._\n`;

// ---------------------------------------------------------------------
// Fabrikfunktion — erstellt eine unabhängige Arena-Instanz.
// ---------------------------------------------------------------------
export function createArenaSystem() {
  const pendingDuels = new Map(); // targetJid -> { from, bet, at }

  function ensureArenaFields(users, jid) {
    if (!users[jid]) return;
    if (!users[jid].items) users[jid].items = {};
    if (!users[jid].equipped) users[jid].equipped = { weapon: null, armor: null };
    if (!users[jid].duel) users[jid].duel = { wins: 0, losses: 0, earnings: 0, fights: 0 };
  }

  function rollRarity() {
    const total = Object.values(RARITY_INFO).reduce((s, r) => s + r.weight, 0);
    let roll = Math.random() * total;
    for (const [key, info] of Object.entries(RARITY_INFO)) {
      roll -= info.weight;
      if (roll <= 0) return key;
    }
    return 'common';
  }

  function rollBoxItem(randInt) {
    const rarity = rollRarity();
    const type = Math.random() < 0.5 ? 'weapon' : 'armor';
    const pool = Object.entries(ITEM_DB).filter(([id, it]) => it.rarity === rarity && it.type === type && !it.secret);
    const fallback = Object.entries(ITEM_DB).filter(([id, it]) => it.rarity === rarity && !it.secret);
    const chosenPool = pool.length ? pool : fallback;
    const [id] = chosenPool[randInt(0, chosenPool.length - 1)];
    return id;
  }

  function rollSecretItem(randInt) {
    if (!SECRET_POOL.length) return SECRET_ITEM_ID;
    return SECRET_POOL[randInt(0, SECRET_POOL.length - 1)];
  }

  function formatItemLine(itemId, count) {
    const it = ITEM_DB[itemId];
    if (!it) return null;
    const rarity = RARITY_INFO[it.rarity];
    const typeIcon = it.type === 'weapon' ? '🗡️' : '🛡️';
    return `${typeIcon} ${rarity.emoji} *${it.name}* (${rarity.label}) — Stärke ${it.power}${count > 1 ? ` x${count}` : ''} [\`${itemId}\`]`;
  }

  function getBattleStats(users, jid) {
    ensureArenaFields(users, jid);
    const u = users[jid];
    const level = u.level || 1;

    const weaponId = u.equipped?.weapon;
    const armorId = u.equipped?.armor;
    const weapon = weaponId ? ITEM_DB[weaponId] : null;
    const armor = armorId ? ITEM_DB[armorId] : null;

    const atk = 10 + Math.floor(level * 1.5) + (weapon ? weapon.power : 0);
    const def = 5 + Math.floor(level * 1) + (armor ? armor.power : 0);
    const hp = 100 + level * 5;

    return { atk, def, hp, weapon, armor, level };
  }

  function simulateDuel(users, jidA, jidB) {
    const statsA = getBattleStats(users, jidA);
    const statsB = getBattleStats(users, jidB);

    let hpA = statsA.hp;
    let hpB = statsB.hp;
    const log = [];
    const MAX_ROUNDS = 15;

    let round = 0;
    while (hpA > 0 && hpB > 0 && round < MAX_ROUNDS) {
      round++;
      const varianceA = 0.85 + Math.random() * 0.3;
      const varianceB = 0.85 + Math.random() * 0.3;

      const dmgAtoB = Math.max(1, Math.round((statsA.atk - statsB.def * 0.4) * varianceA));
      const dmgBtoA = Math.max(1, Math.round((statsB.atk - statsA.def * 0.4) * varianceB));

      hpB -= dmgAtoB;
      hpA -= dmgBtoA;

      log.push(`Runde ${round}: A trifft für ${dmgAtoB} (Gegner-HP: ${Math.max(0, hpB)}) | B trifft für ${dmgBtoA} (Gegner-HP: ${Math.max(0, hpA)})`);
      if (hpA <= 0 || hpB <= 0) break;
    }

    let winner;
    if (hpA <= 0 && hpB <= 0) {
      winner = (statsA.atk + statsA.def) >= (statsB.atk + statsB.def) ? jidA : jidB;
    } else if (hpA <= 0) {
      winner = jidB;
    } else if (hpB <= 0) {
      winner = jidA;
    } else {
      const pctA = hpA / statsA.hp;
      const pctB = hpB / statsB.hp;
      winner = pctA >= pctB ? jidA : jidB;
    }
    const loser = winner === jidA ? jidB : jidA;

    return { winner, loser, log, rounds: round, finalHpA: Math.max(0, hpA), finalHpB: Math.max(0, hpB), statsA, statsB };
  }

  async function mentionText(jid, sock, getNumberMention) {
    if (typeof jid !== 'string') return '';
    if (jid.endsWith('@lid')) {
      try {
        const resolved = await getNumberMention(jid, sock);
        if (resolved) return resolved.startsWith('@') ? resolved : `@${resolved}`;
      } catch {
        // Fällt unten auf den Rohtext zurück, falls Auflösung fehlschlägt
      }
    }
    return `@${jid.split('@')[0]}`;
  }

  async function handle(ctx) {
    const {
      cmd, args, sender, from, m, send, sock,
      users, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, randInt, sleep, activePrefix, isPrimaryOwner
    } = ctx;

    // ---------- FLOOR-SYSTEM ----------
    if (cmd === 'floor' || cmd === 'etage') {
      ensureUser(sender);
      const u = users[sender];
      const level = u.level || 1;
      const floor = Math.min(100, Math.floor(level / 3) + 1);
      const levelInFloor = level % 3 || 3;
      const barLength = 10;
      const filled = Math.round((levelInFloor / 3) * barLength);
      const bar = '█'.repeat(filled) + '░'.repeat(barLength - filled);

      const floorNames = {
        1: 'Stadt der Anfänge', 22: 'Kristallwald', 25: 'Grenzgebiet zur Ostzone',
        50: 'Feenwald', 55: 'Fluchwald', 74: 'Freundlicher Wald', 75: 'Nebeldorf',
        76: 'Lindas Hütte', 100: 'Schloss der Illusion'
      };
      let currentArea = 'Unbekanntes Gebiet';
      for (const [f, name] of Object.entries(floorNames)) {
        if (floor >= parseInt(f)) currentArea = name;
      }

      await send(
        `🏯 *— AINCRAD FLOOR-STATUS —* 🏯\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `⬆️ Aktueller Floor: *${floor} / 100*\n` +
        `📍 Gebiet: ${currentArea}\n` +
        `⭐ Level: ${level}\n` +
        `📊 Fortschritt zum nächsten Floor:\n[${bar}] ${levelInFloor}/3\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        (floor >= 100
          ? `👑 Du hast das Schloss der Illusion erreicht! Aincrad ist besiegt!`
          : `_"Der einzige Weg nach vorn ist durch." Level up mit ${activePrefix}work, ${activePrefix}fish, Duellen..._`)
      );
      return true;
    }

    // ---------- ARENA-ITEMS (öffentliche Item-Übersicht, ohne Secrets) ----------
    if (cmd === 'arenaitems' || cmd === 'itemliste') {
      const order = ['legendary', 'epic', 'rare', 'uncommon', 'common'];
      let out = `⚔️ *— ARENA-ITEMS —* ⚔️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;

      for (const rarity of order) {
        const info = RARITY_INFO[rarity];
        const items = Object.entries(ITEM_DB).filter(([, it]) => it.rarity === rarity && !it.secret);
        if (!items.length) continue;

        out += `\n${info.emoji} *${info.label}*\n`;
        for (const [, it] of items) {
          const typeIcon = it.type === 'weapon' ? '🗡️' : '🛡️';
          out += `  ${typeIcon} ${it.name} — Stärke ${it.power}\n`;
        }
      }

      out += `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n_Erhältlich über ${activePrefix}buy kiste (${ARENA_SHOP_ITEM.price} Coins) + ${activePrefix}openkiste, oder als seltener Fund beim Angeln._\n_Es gibt außerdem ultraseltene Secret-Items (1:1000 Chance) — welche das sind, bleibt geheim..._`;
      await send(out);
      return true;
    }

    // ---------- GEHEIMER OWNER-BEFEHL: Secret-Item verleihen (Dual Blades) ----------
    if (cmd === 'kiritossecret') {
      ensureUser(sender);
      const senderRank = users[sender]?.rank || 'USER';

      console.log('[DEBUG kiritossecret] sender:', sender, '| rank:', senderRank);

      if (senderRank !== 'OWNER') return false;

      const ctx2 = m.message?.extendedTextMessage?.contextInfo;
      let target = args[0];
      if (ctx2?.mentionedJid?.length) target = ctx2.mentionedJid[0];
      else if (ctx2?.participant) target = ctx2.participant;

      const targetJid = target ? normalizeJid(target) : sender;

      ensureUser(targetJid);
      ensureArenaFields(users, targetJid);
      users[targetJid].items[SECRET_ITEM_ID] = (users[targetJid].items[SECRET_ITEM_ID] || 0) + 1;
      save(FILES.users, users);

      const it = ITEM_DB[SECRET_ITEM_ID];
      const isSelf = isSameJid(targetJid, sender);

      const targetMention = await mentionText(targetJid, sock, getNumberMention);

      await send(
        `🤫 *Ein Flüstern durchzieht das System...*\n` +
        (isSelf
          ? `Du hast erhalten: *${it.name}* 🟡\n`
          : `${targetMention} hat erhalten: *${it.name}* 🟡\n`) +
        `_${it.trueName}_\n` +
        `Stärke: ${it.power}\n\n` +
        `Ausrüsten mit: ${activePrefix}equip ${SECRET_ITEM_ID}`,
        { mentions: [targetJid] }
      );
      return true;
    }

    // ---------- EXCALIBUR — exklusiv für den Haupt-Owner, keine Vergabe an andere ----------
    if (cmd === 'excalibur') {
      // Harte Sperre: nur der/die im Code hinterlegte(n) Haupt-Owner
      // (isPrimaryOwner kommt aus index.js und prüft gegen OWNER_LID/OWNER_PRIV,
      // NICHT gegen den vergebbaren "OWNER"-Rang, damit das niemand über
      // ?setrole umgehen kann).
      if (typeof isPrimaryOwner !== 'function' || !isPrimaryOwner(sender)) {
        return false; // tut so, als gäbe es den Befehl nicht
      }

      ensureUser(sender);
      ensureArenaFields(users, sender);

      const it = ITEM_DB[EXCALIBUR_ITEM_ID];
      const rarity = RARITY_INFO[it.rarity];

      users[sender].items[EXCALIBUR_ITEM_ID] = 1;
      users[sender].equipped.weapon = EXCALIBUR_ITEM_ID;
      save(FILES.users, users);

      await send(
        `${rarity.emoji} *EXCALIBUR* wurde beschworen und ausgerüstet! ${rarity.emoji}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `_${it.trueName}_\n` +
        `Seltenheit: ${rarity.label}\n` +
        `⚔️ Schaden: ${it.power}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `_"Nur der Schöpfer selbst kann diese Klinge führen."_`
      );
      return true;
    }

    // ---------- AEGIS — exklusiv für den Haupt-Owner, keine Vergabe an andere ----------
    if (cmd === 'aegis') {
      // Gleiche harte Sperre wie bei Excalibur: nur der im Code hinterlegte
      // Haupt-Owner (isPrimaryOwner), nicht der vergebbare "OWNER"-Rang.
      if (typeof isPrimaryOwner !== 'function' || !isPrimaryOwner(sender)) {
        return false; // tut so, als gäbe es den Befehl nicht
      }

      ensureUser(sender);
      ensureArenaFields(users, sender);

      const it = ITEM_DB[AEGIS_ITEM_ID];
      const rarity = RARITY_INFO[it.rarity];

      users[sender].items[AEGIS_ITEM_ID] = 1;
      users[sender].equipped.armor = AEGIS_ITEM_ID;
      save(FILES.users, users);

      await send(
        `${rarity.emoji} *AEGIS* wurde beschworen und ausgerüstet! ${rarity.emoji}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `_${it.trueName}_\n` +
        `Seltenheit: ${rarity.label}\n` +
        `🛡️ Rüstung: ${it.power}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `_"Kein Schwert der Welt kann diesen Mantel durchdringen."_`
      );
      return true;
    }

    // ---------- KISTE ÖFFNEN ----------
    if (cmd === 'openkiste' || cmd === 'kisteoeffnen' || cmd === 'openbox') {
      ensureUser(sender);
      ensureArenaFields(users, sender);
      const owned = users[sender].items.kiste || 0;
      if (owned < 1) {
        await send(`❌ Du besitzt keine Ausrüstungskiste. Kaufe eine mit ${activePrefix}buy kiste (${ARENA_SHOP_ITEM.price} Coins).`);
        return true;
      }
      users[sender].items.kiste -= 1;
      if (users[sender].items.kiste <= 0) delete users[sender].items.kiste;

      // Secret-Items: 1:1000-Chance (0,1%) bei jedem Kisten-Öffnen.
      // Bei Treffer wird eines der Secret-Items aus SECRET_POOL zufällig vergeben.
      // Excalibur ist hiervon NICHT betroffen — es wird nie über die
      // Kiste ausgegeben, nur exklusiv über ?excalibur an den Owner.
      const SECRET_DROP_CHANCE_PERCENT = 0.1; // 1 zu 1000
      const isSecretDrop = Math.random() * 100 < SECRET_DROP_CHANCE_PERCENT;

      const itemId = isSecretDrop ? rollSecretItem(randInt) : rollBoxItem(randInt);
      const it = ITEM_DB[itemId];
      users[sender].items[itemId] = (users[sender].items[itemId] || 0) + 1;
      save(FILES.users, users);

      const rarity = RARITY_INFO[it.rarity];
      const typeIcon = it.type === 'weapon' ? '🗡️ Waffe' : '🛡️ Rüstung';

      if (isSecretDrop) {
        await send(
          `📦 *Kiste geöffnet!*\n` +
          `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `✨🤯 *DAS SYSTEM ERZITTERT...* 🤯✨\n\n` +
          `${rarity.emoji} *${it.name}*\n` +
          `_${it.trueName}_\n` +
          `Typ: ${typeIcon}\n` +
          `Seltenheit: ${rarity.label} (Secret)\n` +
          `Stärke: ${it.power}\n` +
          `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `🍀 Chance war 1:1000 (0,1%) — du gehörst zu den glücklichsten Spielern in ganz Aincrad!\n` +
          `Ausrüsten mit: ${activePrefix}equip ${itemId}`
        );
        return true;
      }

      await send(
        `📦 *Kiste geöffnet!*\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${rarity.emoji} *${it.name}*\n` +
        `Typ: ${typeIcon}\n` +
        `Seltenheit: ${rarity.label}\n` +
        `Stärke: ${it.power}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `Ausrüsten mit: ${activePrefix}equip ${itemId}`
      );
      return true;
    }

    // ---------- AUSRÜSTUNG ANZEIGEN ----------
    if (cmd === 'gear' || cmd === 'ausruestung' || cmd === 'equipment') {
      ensureUser(sender);
      ensureArenaFields(users, sender);
      const u = users[sender];
      const stats = getBattleStats(users, sender);

      const weaponLine = stats.weapon ? formatItemLine(u.equipped.weapon, 1) : '— (keine Waffe ausgerüstet)';
      const armorLine = stats.armor ? formatItemLine(u.equipped.armor, 1) : '— (keine Rüstung ausgerüstet)';

      const ownedWeapons = Object.entries(u.items || {}).filter(([id]) => ITEM_DB[id]?.type === 'weapon');
      const ownedArmors = Object.entries(u.items || {}).filter(([id]) => ITEM_DB[id]?.type === 'armor');

      let out = `⚔️ *— AUSRÜSTUNG —* ⚔️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;
      out += `🗡️ Waffe: ${weaponLine}\n`;
      out += `🛡️ Rüstung: ${armorLine}\n\n`;
      out += `📊 Kampfwerte:\nATK: ${stats.atk} | DEF: ${stats.def} | HP: ${stats.hp}\n\n`;
      out += `🎒 *Eigene Waffen:*\n`;
      out += ownedWeapons.length ? ownedWeapons.map(([id, c]) => formatItemLine(id, c)).join('\n') : '(keine)';
      out += `\n\n🎒 *Eigene Rüstungen:*\n`;
      out += ownedArmors.length ? ownedArmors.map(([id, c]) => formatItemLine(id, c)).join('\n') : '(keine)';
      out += `\n\n_${activePrefix}equip <id> — Ausrüsten_\n_${activePrefix}unequip weapon|armor — Ablegen_\n_${activePrefix}sell <id> [anzahl|all] — Verkaufen_`;

      await send(out);
      return true;
    }

    // ---------- AUSRÜSTEN ----------
    if (cmd === 'equip') {
      ensureUser(sender);
      ensureArenaFields(users, sender);
      const itemId = (args[0] || '').toLowerCase();
      const it = ITEM_DB[itemId];
      if (!itemId || !it) {
        await send(`❌ Nutzung: ${activePrefix}equip <item-id>\nNutze ${activePrefix}gear um deine Item-IDs zu sehen.`);
        return true;
      }

      // Excalibur darf niemand ausrüsten, der es nicht besitzt UND selbst
      // wenn es (technisch) im Inventar landen würde, ist es weiterhin
      // an den Haupt-Owner gebunden.
      if (it.ownerOnly && (typeof isPrimaryOwner !== 'function' || !isPrimaryOwner(sender))) {
        await send('❌ Diese Waffe kannst du nicht führen.');
        return true;
      }

      const owned = users[sender].items[itemId] || 0;
      if (owned < 1) {
        await send('❌ Du besitzt diesen Gegenstand nicht.');
        return true;
      }
      if (it.type === 'weapon') users[sender].equipped.weapon = itemId;
      else users[sender].equipped.armor = itemId;
      save(FILES.users, users);

      const rarity = RARITY_INFO[it.rarity];
      await send(`✅ *${it.name}* ${rarity.emoji} (${it.type === 'weapon' ? 'Waffe' : 'Rüstung'}) wurde ausgerüstet!`);
      return true;
    }

    // ---------- ABLEGEN ----------
    if (cmd === 'unequip') {
      ensureUser(sender);
      ensureArenaFields(users, sender);
      const slot = (args[0] || '').toLowerCase();
      if (!['weapon', 'armor', 'waffe', 'ruestung', 'rüstung'].includes(slot)) {
        await send(`❌ Nutzung: ${activePrefix}unequip weapon|armor`);
        return true;
      }
      if (slot === 'weapon' || slot === 'waffe') users[sender].equipped.weapon = null;
      else users[sender].equipped.armor = null;
      save(FILES.users, users);
      await send('✅ Ausrüstung abgelegt.');
      return true;
    }

    // ---------- VERKAUFEN ----------
    if (cmd === 'sell' || cmd === 'verkaufen') {
      ensureUser(sender);
      ensureArenaFields(users, sender);
      const itemId = (args[0] || '').toLowerCase();
      const it = ITEM_DB[itemId];

      if (!itemId || !it) {
        await send(
          `❌ Nutzung: ${activePrefix}sell <item-id> [anzahl|all]\n` +
          `Nutze ${activePrefix}gear um deine Item-IDs zu sehen.`
        );
        return true;
      }

      if (it.ownerOnly) {
        await send('❌ Dieser Gegenstand kann nicht verkauft werden.');
        return true;
      }

      const owned = users[sender].items[itemId] || 0;
      if (owned < 1) {
        await send('❌ Du besitzt diesen Gegenstand nicht.');
        return true;
      }

      const isEquippedWeapon = it.type === 'weapon' && users[sender].equipped?.weapon === itemId;
      const isEquippedArmor = it.type === 'armor' && users[sender].equipped?.armor === itemId;

      const amountArgRaw = (args[1] || '1').toLowerCase();
      let amount = (amountArgRaw === 'all' || amountArgRaw === 'alle') ? owned : parseInt(amountArgRaw);
      if (isNaN(amount) || amount < 1) amount = 1;
      amount = Math.min(amount, owned);

      // Wenn das Item aktuell ausgerüstet ist und ALLE Exemplare verkauft würden,
      // muss zuerst abgelegt werden.
      if ((isEquippedWeapon || isEquippedArmor) && amount >= owned) {
        await send(
          `❌ Du kannst dein ausgerüstetes Item nicht (vollständig) verkaufen.\n` +
          `Lege es zuerst ab mit ${activePrefix}unequip ${it.type === 'weapon' ? 'weapon' : 'armor'}.`
        );
        return true;
      }

      const unitPrice = getSellPrice(itemId);
      const total = unitPrice * amount;

      users[sender].items[itemId] -= amount;
      if (users[sender].items[itemId] <= 0) delete users[sender].items[itemId];
      users[sender].coins = (users[sender].coins || 0) + total;
      save(FILES.users, users);

      const rarity = RARITY_INFO[it.rarity];
      await send(
        `💰 *Verkauft!*\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `${rarity.emoji} ${it.name} x${amount}${it.secret ? ' (Secret)' : ''}\n` +
        `Erhalten: 💰 ${total} Coins (${unitPrice}/Stück)\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `Neuer Kontostand: 💰 ${users[sender].coins} Coins`
      );
      return true;
    }

    // ---------- DUELL ----------
    if (cmd === 'duell' || cmd === 'duel') {
      const sub = (args[0] || '').toLowerCase();

      if (sub === 'accept' || sub === 'annehmen') {
        const proposal = pendingDuels.get(sender);
        if (!proposal) { await send('❌ Du hast keine offene Duell-Herausforderung.'); return true; }

        ensureUser(sender);
        ensureUser(proposal.from);
        ensureArenaFields(users, sender);
        ensureArenaFields(users, proposal.from);

        if ((users[sender].coins || 0) < proposal.bet) {
          pendingDuels.delete(sender);
          await send(`❌ Du hast nicht mehr genug Coins (${proposal.bet} benötigt). Duell abgebrochen.`);
          return true;
        }
        if ((users[proposal.from].coins || 0) < proposal.bet) {
          pendingDuels.delete(sender);
          await send('❌ Dein Herausforderer hat nicht mehr genug Coins. Duell abgebrochen.');
          return true;
        }

        pendingDuels.delete(sender);
        const challengerJid = proposal.from;
        const defenderJid = sender;
        const bet = proposal.bet;

        const challengerMention = await mentionText(challengerJid, sock, getNumberMention);
        const defenderMention = await mentionText(defenderJid, sock, getNumberMention);

        await send(
          `⚔️ *DUELL BEGINNT!* ⚔️\n${challengerMention} VS ${defenderMention}\nEinsatz: 💰 ${bet} Coins`,
          { mentions: [challengerJid, defenderJid] }
        );
        await sleep(1500);

        const result = simulateDuel(users, challengerJid, defenderJid);
        const loserJid = result.loser;
        const winnerJid = result.winner;

        users[loserJid].coins = Math.max(0, (users[loserJid].coins || 0) - bet);
        users[winnerJid].coins = (users[winnerJid].coins || 0) + bet;

        ensureArenaFields(users, winnerJid);
        ensureArenaFields(users, loserJid);
        users[winnerJid].duel.wins += 1;
        users[winnerJid].duel.fights += 1;
        users[winnerJid].duel.earnings += bet;
        users[loserJid].duel.losses += 1;
        users[loserJid].duel.fights += 1;
        users[loserJid].duel.earnings -= bet;

        save(FILES.users, users);

        const winnerMention = await mentionText(winnerJid, sock, getNumberMention);
        const loserMention = await mentionText(loserJid, sock, getNumberMention);

        const shortLog = result.log.slice(-4).join('\n');
        await send(
          `🏆 *DUELL-ERGEBNIS* 🏆\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `${shortLog}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
          `🥇 Gewinner: ${winnerMention}\n` +
          `💀 Verlierer: ${loserMention}\n` +
          `💰 ${winnerMention} erhält ${bet} Coins von ${loserMention}!`,
          { mentions: [winnerJid, loserJid] }
        );
        return true;
      }

      if (sub === 'deny' || sub === 'decline' || sub === 'ablehnen') {
        const proposal = pendingDuels.get(sender);
        if (!proposal) { await send('❌ Du hast keine offene Duell-Herausforderung.'); return true; }
        pendingDuels.delete(sender);
        const senderMention = await mentionText(sender, sock, getNumberMention);
        await send(`🛡️ ${senderMention} hat die Herausforderung abgelehnt.`, { mentions: [sender] });
        return true;
      }

      if (sub === 'cancel' || sub === 'abbrechen') {
        let found = null;
        for (const [targetJid, v] of pendingDuels.entries()) {
          if (v.from === sender) { found = targetJid; break; }
        }
        if (!found) { await send('❌ Du hast keine offene Herausforderung zum Zurückziehen.'); return true; }
        pendingDuels.delete(found);
        await send('✅ Deine Herausforderung wurde zurückgezogen.');
        return true;
      }

      const ctxInfo = m.message?.extendedTextMessage?.contextInfo;
      let target = ctxInfo?.mentionedJid?.length ? ctxInfo.mentionedJid[0] : args[0];
      if (!target && ctxInfo?.participant) target = ctxInfo.participant;

      const bet = parseInt(args[1]);

      if (!target || isNaN(bet) || bet <= 0) {
        await send(`❌ Nutzung: ${activePrefix}duell @user <einsatz>\n${activePrefix}duell accept / deny / cancel`);
        return true;
      }

      const targetJid = normalizeJid(target);
      ensureUser(sender);
      ensureUser(targetJid);
      ensureArenaFields(users, sender);
      ensureArenaFields(users, targetJid);

      if (isSameJid(sender, targetJid)) { await send('❌ Du kannst nicht gegen dich selbst kämpfen! 😅'); return true; }
      if ((users[sender].coins || 0) < bet) { await send('❌ Du hast nicht genug Coins für diesen Einsatz.'); return true; }
      if ((users[targetJid].coins || 0) < bet) {
        const targetMention = await mentionText(targetJid, sock, getNumberMention);
        await send(`❌ ${targetMention} hat nicht genug Coins für diesen Einsatz.`, { mentions: [targetJid] });
        return true;
      }

      const existing = pendingDuels.get(targetJid);
      if (existing && existing.from === sender) {
        await send('❌ Du hast bereits eine offene Herausforderung an diese Person.');
        return true;
      }

      pendingDuels.set(targetJid, { from: sender, bet, at: Date.now() });

      const senderMention = await mentionText(sender, sock, getNumberMention);
      const targetMention = await mentionText(targetJid, sock, getNumberMention);

      await send(
        `⚔️ ${senderMention} fordert ${targetMention} zum Duell heraus!\n` +
        `💰 Einsatz: ${bet} Coins\n\n` +
        `${targetMention}, antworte mit:\n` +
        `${activePrefix}duell accept — annehmen\n` +
        `${activePrefix}duell deny — ablehnen`,
        { mentions: [sender, targetJid] }
      );
      return true;
    }

    // ---------- ARENA-LEADERBOARD ----------
    if (cmd === 'arena' || cmd === 'duelleaderboard' || cmd === 'kampfrangliste') {
      const entries = Object.entries(users).filter(([jid, u]) => u?.duel && u.duel.fights > 0);
      if (!entries.length) {
        await send('🏟️ Noch niemand hat an einem Duell teilgenommen. Nutze ' + activePrefix + 'duell @user <einsatz>!');
        return true;
      }

      const sorted = entries.sort((a, b) => (b[1].duel.wins - a[1].duel.wins) || (b[1].duel.earnings - a[1].duel.earnings));
      const top = sorted.slice(0, 10);
      const medals = ['🥇', '🥈', '🥉'];

      const lines = await Promise.all(top.map(async ([jid, u], i) => {
        const name = u.name || u.registrationName || await getNumberMention(jid, sock);
        const icon = medals[i] || `${i + 1}.`;
        return `${icon} ${name} — 🏆 ${u.duel.wins} Siege | ⚔️ ${u.duel.fights} Kämpfe | 💰 ${u.duel.earnings >= 0 ? '+' : ''}${u.duel.earnings} Coins`;
      }));

      const mentions = top.map(([jid]) => jid);
      await send(
        `🏟️ *— ARENA-RANGLISTE —* 🏟️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n⚔️ "Nur die Starken überleben Aincrad." ⚔️`,
        { mentions }
      );
      return true;
    }

    return false; // nicht von der Arena behandelt
  }

  return { handle, ensureArenaFields, getBattleStats, simulateDuel, rollBoxItem, rollSecretItem, formatItemLine, mentionText };
}
