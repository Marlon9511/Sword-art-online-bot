/* =====================================================================
   ⚔️ SAO ARENA-SYSTEM — Modul
   =====================================================================
   Eigenständiges, komplett fertiges Modul. Verwaltet:
   - Item-Datenbank (Waffen & Rüstungen, 5 Seltenheitsstufen)
   - Lootboxen ("kiste" im Shop)
   - Ausrüsten / Ablegen
   - PVP-Duelle mit Coin-Einsatz
   - Arena-Leaderboard (meiste Siege)

   Speichert alles direkt in eurem bestehenden users-Objekt
   (users[jid].items, users[jid].equipped, users[jid].duel) — es werden
   KEINE neuen JSON-Dateien gebraucht. Persistiert wird über euer
   vorhandenes save(FILES.users, users).

   Einbau in bot.js: siehe integration-patch.txt (nur 4 Zeilen nötig).

   ---------------------------------------------------------------------
   ÄNDERUNG: Mention-Formatierung korrigiert.
   Vorher wurde überall `@${jid.split('@')[0]}` als Anzeigetext verwendet.
   Das funktioniert nur für normale WhatsApp-JIDs (...@s.whatsapp.net).
   Bei JIDs vom Typ ...@lid ("linked ID") ist der Teil vor dem @ NICHT
   die Telefonnummer, sondern eine interne LID-Nummer — der Mention-Text
   passt dann nicht zur echten Nummer/Anzeige, obwohl der `mentions`-Array
   die richtige JID enthält. Dadurch wirken @-Erwähnungen bei @lid-Usern
   kaputt oder zeigen falsche Ziffern an.

   Fix: eine zentrale Helper-Funktion `mentionText()`, die für @lid-JIDs
   automatisch über die vorhandene `getNumberMention()`-Funktion (die ihr
   schon fürs Leaderboard nutzt) den korrekten Anzeigenamen/Nummer auflöst,
   und für normale JIDs weiterhin einfach die Nummer aus der JID nimmt.
   Alle Stellen im Duell-Flow nutzen jetzt diesen Helper statt manuellem
   `jid.split('@')[0]`.
   ===================================================================== */

// ---------------------------------------------------------------------
// Item-Datenbank
// ---------------------------------------------------------------------
export const RARITY_INFO = {
  common:    { label: 'Gewöhnlich',   emoji: '⚪', weight: 45 },
  uncommon:  { label: 'Ungewöhnlich', emoji: '🟢', weight: 30 },
  rare:      { label: 'Selten',       emoji: '🔵', weight: 15 },
  epic:      { label: 'Episch',       emoji: '🟣', weight: 8 },
  legendary: { label: 'Legendär',     emoji: '🟡', weight: 2 }
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

  // ---- SECRET (extrem selten via {P}openkiste, keine Anzeige in {P}arenaitems) ----
  // Tarnung: Anzeigename ist absichtlich identisch mit dem gewöhnlichen
  // "Holzstab" (w_common_2) — im Inventar/Profil sieht man auf den ersten
  // Blick nur "Holzstab". Erst Seltenheit + Stärke verraten, dass es sich
  // um Kiritos legendäre Doppelklingen handelt. `secret: true` sorgt dafür,
  // dass rollBoxItem() (die normale, gewichtete Ziehung) und der
  // {P}arenaitems-Befehl dieses Item ignorieren. Der tatsächliche Drop läuft
  // separat über eine eigene 0,0000001%-Chance direkt im openkiste-Handler
  // (siehe SECRET_DROP_CHANCE_PERCENT) — zusätzlich weiterhin auch über den
  // geheimen Owner-Befehl `kiritossecret` erhältlich.
  w_secret_dualblades: {
    name: 'Holzstab',
    trueName: 'Kiritos Doppelklingen (Dual Blades)',
    type: 'weapon',
    rarity: 'legendary',
    power: 150,
    secret: true
  }
};

// Interne ID des Secret-Items — an einer Stelle definiert, damit sie sich
// bei Bedarf leicht ändern lässt, ohne den Code an mehreren Stellen anzufassen.
export const SECRET_ITEM_ID = 'w_secret_dualblades';

export const ARENA_SHOP_ITEM = {
  id: 'kiste',
  price: 750,
  desc: '⚔️ SAO-Ausrüstungskiste: zufällige Waffe oder Rüstung'
};

// WICHTIG: Der geheime Owner-Befehl zum Verleihen des Secret-Items steht
// absichtlich NICHT in dieser Liste. ALL_COMMANDS in bot.js (Tippfehler-
// Vorschläge) und das Hilfe-Menü greifen auf ARENA_COMMANDS zurück — ein
// Eintrag hier würde das Geheimnis sofort verraten.
export const ARENA_COMMANDS = [
  'openkiste', 'kisteoeffnen', 'openbox', 'gear', 'ausruestung', 'equipment',
  'equip', 'unequip', 'duell', 'duel', 'arena', 'duelleaderboard', 'kampfrangliste',
  'arenaitems', 'itemliste'
];

export const ARENA_HELP_TEXT =
  `▸ {P}buy kiste — Ausrüstungskiste kaufen (${ARENA_SHOP_ITEM.price} Coins)\n` +
  `▸ {P}openkiste — Kiste öffnen (zufällige Waffe/Rüstung)\n` +
  `▸ {P}gear — Ausrüstung & Inventar anzeigen\n` +
  `▸ {P}equip <id> — Waffe/Rüstung ausrüsten\n` +
  `▸ {P}unequip weapon|armor — Ausrüstung ablegen\n` +
  `▸ {P}duell @user <einsatz> — Zum Duell herausfordern\n` +
  `▸ {P}duell accept/deny/cancel — Duell verwalten\n` +
  `▸ {P}arena — Arena-Rangliste (meiste Siege)\n` +
  `▸ {P}arenaitems — Alle erhältlichen Waffen & Rüstungen anzeigen\n`;

// ---------------------------------------------------------------------
// Fabrikfunktion — erstellt eine unabhängige Arena-Instanz.
// Hält den pendingDuels-State selbst (kein Eingriff in bot.js nötig).
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
    // `!it.secret` schließt geheime Items (z.B. Kiritos Doppelklingen) aus
    // dem normalen Kisten-Loot komplett aus — die kommen NUR über den
    // geheimen Owner-Befehl ins Spiel.
    const pool = Object.entries(ITEM_DB).filter(([id, it]) => it.rarity === rarity && it.type === type && !it.secret);
    const fallback = Object.entries(ITEM_DB).filter(([id, it]) => it.rarity === rarity && !it.secret);
    const chosenPool = pool.length ? pool : fallback;
    const [id] = chosenPool[randInt(0, chosenPool.length - 1)];
    return id;
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

  /**
   * Liefert den korrekten Anzeigetext für eine @-Erwähnung.
   *
   * Bei normalen JIDs (...@s.whatsapp.net, ...@c.us etc.) ist der Teil vor
   * dem @ die Telefonnummer — der kann direkt als Anzeigetext verwendet
   * werden, WhatsApp matcht ihn dann mit dem `mentions`-Array.
   *
   * Bei JIDs vom Typ ...@lid ist der Teil vor dem @ KEINE Telefonnummer,
   * sondern eine interne LID. Damit die Erwähnung trotzdem korrekt
   * angezeigt wird, wird hier die schon vorhandene getNumberMention()
   * genutzt, die (laut eurem Leaderboard-Code) den passenden Namen/die
   * passende Nummer zu einer JID auflöst.
   */
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

  /**
   * Haupt-Einstiegspunkt. In eurer messages.upsert-Logik aufrufen:
   *
   *   const handled = await arena.handle({ cmd, args, sender, from, m,
   *     isGroup, activePrefix, send, sock, users, save, FILES, ensureUser,
   *     normalizeJid, isSameJid, getNumberMention, randInt, sleep });
   *   if (handled) return;
   *
   * Gibt true zurück, wenn der Befehl von der Arena behandelt wurde.
   */
  async function handle(ctx) {
    const {
      cmd, args, sender, from, m, send, sock,
      users, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, randInt, sleep, activePrefix,
      isOwner // optional — nur nötig für den geheimen Owner-Befehl unten
    } = ctx;

    // ---------- ARENA-ITEMS (öffentliche Item-Übersicht, ohne Secrets) ----------
    if (cmd === 'arenaitems' || cmd === 'itemliste') {
      const order = ['legendary', 'epic', 'rare', 'uncommon', 'common'];
      let out = `⚔️ *— ARENA-ITEMS —* ⚔️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n`;

      for (const rarity of order) {
        const info = RARITY_INFO[rarity];
        // Secret-Items (`secret: true`) werden hier bewusst NICHT gelistet.
        const items = Object.entries(ITEM_DB).filter(([, it]) => it.rarity === rarity && !it.secret);
        if (!items.length) continue;

        out += `\n${info.emoji} *${info.label}*\n`;
        for (const [, it] of items) {
          const typeIcon = it.type === 'weapon' ? '🗡️' : '🛡️';
          out += `  ${typeIcon} ${it.name} — Stärke ${it.power}\n`;
        }
      }

      out += `\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n_Erhältlich über ${activePrefix}buy kiste (${ARENA_SHOP_ITEM.price} Coins) + ${activePrefix}openkiste, oder als seltener Fund beim Angeln._`;
      await send(out);
      return true;
    }

    // ---------- GEHEIMER OWNER-BEFEHL: Secret-Item verleihen ----------
    // Absichtlich NICHT in ARENA_COMMANDS/ARENA_HELP_TEXT gelistet, damit der
    // Befehl in ?help nirgends auftaucht und die "Ähnlicher Befehl?"-Tippfehler-
    // Erkennung in bot.js ihn niemandem verrät. Nicht-Owner bekommen `false`
    // zurück — der Bot reagiert dann wie bei einem völlig unbekannten Befehl,
    // ohne die Existenz des Befehls überhaupt zu bestätigen.
    if (cmd === 'kiritossecret') {
      ensureUser(sender);
      const senderRank = users[sender]?.rank || 'USER';
      if (senderRank !== 'OWNER') return false;

      ensureArenaFields(users, sender);
      users[sender].items[SECRET_ITEM_ID] = (users[sender].items[SECRET_ITEM_ID] || 0) + 1;
      save(FILES.users, users);

      const it = ITEM_DB[SECRET_ITEM_ID];
      await send(
        `🤫 *Ein Flüstern durchzieht das System...*\n` +
        `Du hast erhalten: *${it.name}* 🟡\n` +
        `_${it.trueName}_\n` +
        `Stärke: ${it.power}\n\n` +
        `Ausrüsten mit: ${activePrefix}equip ${SECRET_ITEM_ID}`
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

      // Ultra-seltener Secret-Drop: 0,0000001 % Chance (≈ 1 zu 1 Milliarde)
      // bei JEDEM Kisten-Öffnen. Läuft komplett getrennt von rollRarity()/
      // rollBoxItem(), damit die normalen Gewichtungen unangetastet bleiben.
      const SECRET_DROP_CHANCE_PERCENT = 0.0000001;
      const isSecretDrop = Math.random() * 100 < SECRET_DROP_CHANCE_PERCENT;

      const itemId = isSecretDrop ? SECRET_ITEM_ID : rollBoxItem(randInt);
      const it = ITEM_DB[itemId];
      users[sender].items[itemId] = (users[sender].items[itemId] || 0) + 1;
      save(FILES.users, users);

      const rarity = RARITY_INFO[it.rarity];
      const typeIcon = it.type === 'weapon' ? '🗡️ Waffe' : '🛡️ Rüstung';

      if (isSecretDrop) {
        // Besondere Aufmachung für den Sonderfall — verrät die wahre
        // Identität erst hier, nachdem das Wunder tatsächlich passiert ist.
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
          `🍀 Chance war ${SECRET_DROP_CHANCE_PERCENT}% — du gehörst zu den glücklichsten Spielern in ganz Aincrad!\n` +
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
      out += `\n\n_${activePrefix}equip <id> — Ausrüsten_\n_${activePrefix}unequip weapon|armor — Ablegen_`;

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

      // Neue Herausforderung
      const ctxInfo = m.message?.extendedTextMessage?.contextInfo;
      // Echtes @-Mention hat Vorrang vor dem rohen Text in args[0]: WhatsApp
      // liefert hier die korrekte JID direkt mit (inkl. @lid), während der
      // Text vor dem @ bei @lid-Kontakten NICHT die Telefonnummer ist und
      // sonst fälschlich als @s.whatsapp.net interpretiert würde.
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

  return { handle, ensureArenaFields, getBattleStats, simulateDuel, rollBoxItem, formatItemLine, mentionText };
}
