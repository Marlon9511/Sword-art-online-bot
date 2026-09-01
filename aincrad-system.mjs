import fs from 'fs';
import path from 'path';


export function createAincradSystem(DATA_PATH) {
  const AC_FILE = path.join(DATA_PATH, 'aincrad.json');

  const ensureFile = () => {
    if (!fs.existsSync(AC_FILE)) {
      fs.writeFileSync(AC_FILE, JSON.stringify({}, null, 2));
    }
  };
  ensureFile();

  let acData = {};
  try {
    acData = JSON.parse(fs.readFileSync(AC_FILE, 'utf-8') || '{}');
  } catch (e) {
    acData = {};
  }

  const saveAc = () => {
    try {
      fs.writeFileSync(AC_FILE, JSON.stringify(acData, null, 2));
    } catch (e) {
      console.error('[aincrad] Fehler beim Speichern:', e?.message || e);
    }
  };

  // ---------------------------------------------------------------
  // Waffen-Datenbank
  // ---------------------------------------------------------------
  const RARITY_INFO = {
    common:    { label: 'Gewöhnlich', emoji: '⚪' },
    rare:      { label: 'Selten',     emoji: '🔵' },
    epic:      { label: 'Episch',     emoji: '🟣' },
    legendary: { label: 'Legendär',   emoji: '🟡' },
    mythic:    { label: 'Mythisch',   emoji: '⚫' }
  };

  const WEAPONS = {
    one_hand_sword:  { name: 'Ein-Hand-Schwert',       power: 10, rarity: 'common',    price: 0,    floor: null },
    beginner_rapier: { name: 'Rapier des Anfängers',   power: 8,  rarity: 'common',    price: 200,  floor: null },
    beginner_mace:   { name: 'Streitkolben des Anfängers', power: 9, rarity: 'common', price: 200,  floor: null },

    silverthorn:     { name: 'Silberdorn',             power: 20, rarity: 'rare',      price: 700,  floor: 'floor_5' },
    nightblade:      { name: 'Nachtklinge',            power: 24, rarity: 'rare',      price: 900,  floor: 'floor_10' },
    stormcatcher:    { name: 'Sturmfänger',            power: 28, rarity: 'rare',      price: 1100, floor: 'floor_15' },

    frostrender:     { name: 'Frostreißer',            power: 38, rarity: 'epic',      price: null, floor: 'floor_20' },
    sandstormblade:  { name: 'Sandsturm-Klinge',       power: 44, rarity: 'epic',      price: null, floor: 'floor_25' },
    magmabreaker:    { name: 'Magmabrecher',           power: 50, rarity: 'epic',      price: null, floor: 'floor_30' },

    crystalfang:     { name: 'Kristallzahn',           power: 65, rarity: 'legendary', price: null, floor: 'floor_40' },
    windedge:        { name: 'Windschneide',           power: 75, rarity: 'legendary', price: null, floor: 'floor_50' },
    deathbringer:    { name: 'Todesbringer',           power: 85, rarity: 'legendary', price: null, floor: 'floor_55' },

    kagachis_legacy: { name: "Kagachis Erbe",          power: 105, rarity: 'mythic',   price: null, floor: 'floor_74' },
    heartblade_aincrad: { name: 'Herzschneide von Aincrad', power: 130, rarity: 'mythic', price: null, floor: 'floor_75' },

    seed_of_freedom: {
      name: 'Same der Freiheit',
      type: 'weapon',
      power: 9999,
      rarity: 'mythic',
      price: null,
      floor: null,
      secret: true,
      ownerOnly: true
    }
  };

  // ---------------------------------------------------------------
  // Stockwerke (Story-Reihenfolge)
  // ---------------------------------------------------------------
  const FLOORS = [
    {
      id: 'floor_1', name: '🏰 Stockwerk 1 — Stadt der Anfänge', reqLevel: 1,
      boss: 'Illfang der Kobold-Lord', bossPower: 25, xp: 45, col: 70,
      unlockWeapon: null
    },
    {
      id: 'floor_5', name: '🌲 Stockwerk 5 — Wald der Ranken', reqLevel: 3,
      boss: 'Nepenthes, die Fleischfressende Pflanze', bossPower: 55, xp: 80, col: 140,
      unlockWeapon: null
    },
    {
      id: 'floor_10', name: '⛰️ Stockwerk 10 — Berghöhlen', reqLevel: 6,
      boss: 'Der Steinerne Wächter', bossPower: 90, xp: 120, col: 210,
      unlockWeapon: null
    },
    {
      id: 'floor_15', name: '❄️ Stockwerk 15 — Frostebene', reqLevel: 9,
      boss: 'Frostklaue', bossPower: 125, xp: 160, col: 280,
      unlockWeapon: null
    },
    {
      id: 'floor_20', name: '🐊 Stockwerk 20 — Sumpfland', reqLevel: 12,
      boss: 'Der Sumpfdrache', bossPower: 165, xp: 210, col: 360,
      unlockWeapon: 'frostrender'
    },
    {
      id: 'floor_25', name: '🏜️ Stockwerk 25 — Wüstenstadt', reqLevel: 16,
      boss: 'Sandwyrm', bossPower: 205, xp: 260, col: 440,
      unlockWeapon: 'sandstormblade'
    },
    {
      id: 'floor_30', name: '🌋 Stockwerk 30 — Vulkanregion', reqLevel: 20,
      boss: 'Magmakoloss', bossPower: 250, xp: 320, col: 540,
      unlockWeapon: 'magmabreaker'
    },
    {
      id: 'floor_40', name: '💎 Stockwerk 40 — Kristallgrotte', reqLevel: 25,
      boss: 'Kristallgolem', bossPower: 310, xp: 400, col: 660,
      unlockWeapon: 'crystalfang'
    },
    {
      id: 'floor_50', name: '☁️ Stockwerk 50 — Himmelsinsel', reqLevel: 31,
      boss: 'Der Windfürst', bossPower: 380, xp: 490, col: 800,
      unlockWeapon: 'windedge'
    },
    {
      id: 'floor_55', name: '🌫️ Stockwerk 55 — Gebiet der Todessense', reqLevel: 36,
      boss: 'Die Sichel des Todes', bossPower: 440, xp: 570, col: 940,
      unlockWeapon: 'deathbringer'
    },
    {
      id: 'floor_74', name: '🔴 Stockwerk 74 — Rotes Schloss', reqLevel: 45,
      boss: 'General Kagachi', bossPower: 540, xp: 700, col: 1150,
      unlockWeapon: 'kagachis_legacy'
    },
    {
      id: 'floor_75', name: '🖤 Stockwerk 75 — Herz von Aincrad', reqLevel: 55,
      boss: 'Der Systemadministrator', bossPower: 650, xp: 900, col: 1500,
      unlockWeapon: 'heartblade_aincrad'
    }
  ];
  const floorIndex = id => FLOORS.findIndex(f => f.id === id);

  // Zufällige Gegner für das normale "Erkunden"
  const MOBS = [
    { name: 'Kobold-Wache', emoji: '🗡️', power: 5 },
    { name: 'Waldläufer', emoji: '🌿', power: 9 },
    { name: 'Steinkriecher', emoji: '🪨', power: 13 },
    { name: 'Frostwolf', emoji: '🐺', power: 11 },
    { name: 'Sumpfschleim', emoji: '🟢', power: 8 },
    { name: 'Sandläufer', emoji: '🦂', power: 12 },
    { name: 'Flammensprite', emoji: '🔥', power: 15 }
  ];

  const SHOP = {
    heal_potion: { name: 'Heiltrank',       price: 60,  desc: 'Gibt etwas XP' },
    hi_potion:   { name: 'Starker Heiltrank', price: 150, desc: 'Gibt deutlich mehr XP' },
    crystal:     { name: 'Erinnerungskristall', price: 100, desc: 'Setzt einen Cooldown zurück' },
    beginner_rapier: { name: 'Rapier des Anfängers', price: WEAPONS.beginner_rapier.price, desc: 'Einsteiger-Waffe', weapon: 'beginner_rapier' },
    beginner_mace:   { name: 'Streitkolben des Anfängers', price: WEAPONS.beginner_mace.price, desc: 'Einsteiger-Waffe', weapon: 'beginner_mace' },
    silverthorn:  { name: 'Silberdorn', price: WEAPONS.silverthorn.price, desc: 'Erfordert: Stockwerk 5 besucht', weapon: 'silverthorn', reqFloor: 'floor_5' },
    nightblade:   { name: 'Nachtklinge', price: WEAPONS.nightblade.price, desc: 'Erfordert: Stockwerk 10 besucht', weapon: 'nightblade', reqFloor: 'floor_10' },
    stormcatcher: { name: 'Sturmfänger', price: WEAPONS.stormcatcher.price, desc: 'Erfordert: Stockwerk 15 besucht', weapon: 'stormcatcher', reqFloor: 'floor_15' }
  };

  const AC_COMMANDS = [
    'aincradstart', 'aincradhelp', 'aincradprofile', 'aincradme',
    'aincradfloor', 'aincradfloors', 'aincradtravel', 'aincradexplore', 'aincradboss',
    'aincradshop', 'aincradbuy', 'aincradinventory', 'aincradinv', 'aincradequip', 'aincraduse',
    'aincradduel', 'aincradleaderboard', 'aincradrangliste'
  ];

  const AC_HELP_TEXT =
`⚔️ *— AINCRAD-SYSTEM —* ⚔️
${' '}
{P}aincradstart — Reise beginnen
{P}aincradprofile — Dein Profil
{P}aincradfloors — Stockwerk-Übersicht
{P}aincradtravel <stockwerk> — Zu einem freigeschalteten Stockwerk reisen
{P}aincradexplore — Monster im aktuellen Stockwerk bekämpfen
{P}aincradboss — Stockwerkboss herausfordern (schaltet nächstes Stockwerk frei)
{P}aincradshop — Waffen- & Item-Shop
{P}aincradbuy <item> — Etwas kaufen
{P}aincradinventory — Dein Inventar
{P}aincradequip <waffe> — Waffe ausrüsten
{P}aincraduse <item> — Item benutzen
{P}aincradduel @user — Gegen einen anderen Spieler duellieren
{P}aincradleaderboard — Bestenliste`;

  // ---------------------------------------------------------------
  // Hilfsfunktionen
  // ---------------------------------------------------------------
  function ensureProfile(jid) {
    if (!acData[jid]) {
      acData[jid] = {
        level: 1,
        xp: 0,
        col: 100,
        equipped: 'one_hand_sword',
        weapons: { one_hand_sword: 1 },
        items: {},
        currentFloor: 'floor_1',
        unlockedFloors: ['floor_1'],
        clearedFloors: [],
        wins: 0,
        losses: 0,
        lastExplore: 0,
        lastBoss: 0,
        lastDuel: 0,
        createdAt: Date.now()
      };
      saveAc();
    }
    return acData[jid];
  }

  function xpNeeded(level) {
    return 65 + level * 38;
  }

  function addXp(profile, amount) {
    profile.xp += amount;
    const leveledMessages = [];
    let needed = xpNeeded(profile.level);
    while (profile.xp >= needed) {
      profile.xp -= needed;
      profile.level++;
      leveledMessages.push(profile.level);
      needed = xpNeeded(profile.level);
    }
    return leveledMessages;
  }

  function weaponPower(id) {
    return WEAPONS[id]?.power || 0;
  }

  function totalPower(profile) {
    return profile.level * 6 + weaponPower(profile.equipped);
  }

  function formatWeapon(id, viewerIsPrimaryOwner = false) {
    const w = WEAPONS[id];
    if (!w) return id;
    if (w.ownerOnly && !viewerIsPrimaryOwner) return '❓ Unbekannte Waffe';
    const rarity = RARITY_INFO[w.rarity]?.emoji || '';
    return `${rarity} ${w.name} (⚔️ ${w.power})`;
  }

  function cooldownLeft(lastTs, cooldownMs) {
    const now = Date.now();
    const diff = cooldownMs - (now - lastTs);
    if (diff <= 0) return null;
    const mins = Math.floor(diff / 60000);
    const secs = Math.floor((diff % 60000) / 1000);
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  }

  const EXPLORE_COOLDOWN = 5 * 60 * 1000;   // 5 Min
  const BOSS_COOLDOWN = 30 * 60 * 1000;     // 30 Min
  const DUEL_COOLDOWN = 10 * 60 * 1000;     // 10 Min

  // ---------------------------------------------------------------
  // Command-Handler
  // ---------------------------------------------------------------
  async function handle(ctx) {
    const {
      cmd, args, sender, from, isGroup, activePrefix, send, sock,
      normalizeJid, getNumberMention, randInt, isPrimaryOwner, m
    } = ctx;

    if (!AC_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    const profile = ensureProfile(jid);

    // ---------- aincradstart ----------
    if (cmd === 'aincradstart') {
      await send(
        `⚔️ Willkommen in *Aincrad*, Schwertkämpfer!\n\n` +
        `Du beginnst mit dem *Ein-Hand-Schwert* ausgerüstet und ${profile.col} Col.\n` +
        `Nutze ${activePrefix}aincradfloors um die Stockwerke zu sehen, und ${activePrefix}aincradexplore um deine Reise zu beginnen.`
      );
      return true;
    }

    // ---------- aincradhelp ----------
    if (cmd === 'aincradhelp') {
      await send(AC_HELP_TEXT.replace(/\{P\}/g, activePrefix));
      return true;
    }

    // ---------- aincradprofile / aincradme ----------
    if (cmd === 'aincradprofile' || cmd === 'aincradme') {
      const floor = FLOORS.find(f => f.id === profile.currentFloor);
      const needed = xpNeeded(profile.level);
      const viewerIsOwner = isPrimaryOwner ? isPrimaryOwner(jid) : false;
      const text =
        `⚔️ *— SCHWERTKÄMPFER-PROFIL —* ⚔️\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `⭐ Level: ${profile.level}\n` +
        `✨ XP: ${profile.xp} / ${needed}\n` +
        `💰 Col: ${profile.col}\n` +
        `🗡️ Waffe: ${formatWeapon(profile.equipped, viewerIsOwner)}\n` +
        `🏰 Aktuelles Stockwerk: ${floor ? floor.name : '(unbekannt)'}\n` +
        `🏆 Duelle: ${profile.wins}S / ${profile.losses}N\n` +
        `📜 Freigeschaltete Stockwerke: ${profile.unlockedFloors.length}/${FLOORS.length}`;
      await send(text);
      return true;
    }

    // ---------- aincradfloor / aincradfloors ----------
    if (cmd === 'aincradfloor' || cmd === 'aincradfloors') {
      const lines = FLOORS.map((f, i) => {
        const unlocked = profile.unlockedFloors.includes(f.id);
        const cleared = profile.clearedFloors.includes(f.id);
        const isCurrent = profile.currentFloor === f.id;
        let status = '🔒 Gesperrt';
        if (cleared) status = '✅ Abgeschlossen';
        else if (unlocked) status = '🔓 Freigeschaltet';
        const marker = isCurrent ? ' 👉' : '';
        return `${i + 1}. ${f.name} — ${status}${marker} (ab Lv.${f.reqLevel})`;
      });
      await send(
        `🏰 *— STOCKWERKE VON AINCRAD —* 🏰\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n_Reise mit ${activePrefix}aincradtravel <stockwerk>_`
      );
      return true;
    }

    // ---------- aincradtravel ----------
    if (cmd === 'aincradtravel') {
      const query = args.join(' ').trim().toLowerCase();
      if (!query) {
        await send(`❌ Nutzung: ${activePrefix}aincradtravel <stockwerk>\nBeispiel: ${activePrefix}aincradtravel 10`);
        return true;
      }

      const target = FLOORS.find(f =>
        f.id === `floor_${query}` ||
        f.id.replace('floor_', '') === query ||
        f.name.toLowerCase().includes(query)
      );
      if (!target) {
        await send(`❌ Stockwerk "${query}" nicht gefunden. Nutze ${activePrefix}aincradfloors für die Liste.`);
        return true;
      }
      if (!profile.unlockedFloors.includes(target.id)) {
        await send(`🔒 ${target.name} ist noch gesperrt. Besiege zuerst den Boss des vorherigen Stockwerks.`);
        return true;
      }
      profile.currentFloor = target.id;
      saveAc();
      await send(`🏰 Du reist nach *${target.name}*!\nNutze ${activePrefix}aincradexplore um Monster zu bekämpfen oder ${activePrefix}aincradboss für den Stockwerkboss.`);
      return true;
    }

    // ---------- aincradexplore ----------
    if (cmd === 'aincradexplore') {
      const cd = cooldownLeft(profile.lastExplore, EXPLORE_COOLDOWN);
      if (cd) {
        await send(`⏰ Du musst noch ${cd} warten, bevor du wieder erkunden kannst.`);
        return true;
      }

      const floor = FLOORS.find(f => f.id === profile.currentFloor);
      const mob = MOBS[randInt(0, MOBS.length - 1)];
      const myPower = totalPower(profile);
      const enemyPower = mob.power + randInt(0, 10);

      profile.lastExplore = Date.now();

      if (myPower >= enemyPower || Math.random() < 0.8) {
        const xpGain = randInt(8, 20);
        const colGain = randInt(15, 45);
        profile.col += colGain;
        const levelUps = addXp(profile, xpGain);
        saveAc();
        let text =
          `${mob.emoji} *${mob.name}* in ${floor?.name || 'Aincrad'} besiegt!\n` +
          `+${xpGain} XP, +${colGain} Col`;
        if (levelUps.length) text += `\n🎉 Level-Up! Du bist jetzt Level ${levelUps[levelUps.length - 1]}!`;
        await send(text);
        return true;
      } else {
        const lostCol = Math.min(profile.col, randInt(5, 20));
        profile.col -= lostCol;
        saveAc();
        await send(`${mob.emoji} *${mob.name}* hat dich überwältigt! -${lostCol} Col.\nRüste eine stärkere Waffe aus oder levle zuerst.`);
        return true;
      }
    }

    // ---------- aincradboss ----------
    if (cmd === 'aincradboss') {
      const cd = cooldownLeft(profile.lastBoss, BOSS_COOLDOWN);
      if (cd) {
        await send(`⏰ Der Stockwerkboss regeneriert sich noch. Warte ${cd}.`);
        return true;
      }

      const floor = FLOORS.find(f => f.id === profile.currentFloor);
      if (!floor) {
        await send('❌ Ungültiges aktuelles Stockwerk.');
        return true;
      }
      if (profile.clearedFloors.includes(floor.id)) {
        await send(`✅ Du hast ${floor.name} bereits abgeschlossen. Reise weiter mit ${activePrefix}aincradtravel.`);
        return true;
      }
      if (profile.level < floor.reqLevel) {
        await send(`⚠️ Du solltest mindestens Level ${floor.reqLevel} sein, um gegen *${floor.boss}* zu bestehen (aktuell: Lv.${profile.level}). Nutze ${activePrefix}aincradexplore zum Leveln.`);
        return true;
      }

      profile.lastBoss = Date.now();
      const myPower = totalPower(profile) + randInt(0, 25);
      const bossPower = floor.bossPower + randInt(0, 20);

      if (myPower >= bossPower) {
        if (!profile.clearedFloors.includes(floor.id)) profile.clearedFloors.push(floor.id);
        profile.col += floor.col;
        const levelUps = addXp(profile, floor.xp);

        const idx = floorIndex(floor.id);
        const nextFloor = FLOORS[idx + 1];
        if (nextFloor && !profile.unlockedFloors.includes(nextFloor.id)) {
          profile.unlockedFloors.push(nextFloor.id);
        }

        let rewardLine = '';
        if (floor.unlockWeapon) {
          profile.weapons[floor.unlockWeapon] = (profile.weapons[floor.unlockWeapon] || 0) + 1;
          rewardLine = `\n🗡️ Neue Waffe erhalten: ${formatWeapon(floor.unlockWeapon)}!`;
        }
        saveAc();

        let text =
          `🏆 *${floor.boss}* wurde besiegt!\n` +
          `+${floor.xp} XP, +${floor.col} Col${rewardLine}`;
        if (levelUps.length) text += `\n🎉 Level-Up! Du bist jetzt Level ${levelUps[levelUps.length - 1]}!`;
        if (nextFloor) text += `\n\n🔓 Neues Stockwerk freigeschaltet: *${nextFloor.name}*!`;
        else text += `\n\n✨ Du hast alle Stockwerke abgeschlossen! Aincrad liegt hinter dir.`;

        await send(text);
        return true;
      } else {
        saveAc();
        await send(`💔 *${floor.boss}* war zu stark für dich! Werde stärker mit ${activePrefix}aincradexplore und versuche es erneut.`);
        return true;
      }
    }

    // ---------- aincradshop ----------
    if (cmd === 'aincradshop') {
      let out = '🛒 *— WAFFEN- & ITEM-SHOP —* 🛒\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n';
      for (const [key, item] of Object.entries(SHOP)) {
        out += `• ${key} — ${item.price} 💰 Col | ${item.name} (${item.desc})\n`;
      }
      out += `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\nKaufen mit: ${activePrefix}aincradbuy <item>`;
      await send(out);
      return true;
    }

    // ---------- aincradbuy ----------
    if (cmd === 'aincradbuy') {
      const key = (args[0] || '').toLowerCase();
      const item = SHOP[key];
      if (!item) {
        await send(`❌ Unbekanntes Item. Nutze ${activePrefix}aincradshop für die Liste.`);
        return true;
      }
      if (item.reqFloor && !profile.unlockedFloors.includes(item.reqFloor)) {
        const f = FLOORS.find(x => x.id === item.reqFloor);
        await send(`🔒 Du musst zuerst ${f?.name || item.reqFloor} freigeschaltet haben.`);
        return true;
      }
      if (profile.col < item.price) {
        await send(`💸 Nicht genug Col (du hast ${profile.col}, benötigt: ${item.price}).`);
        return true;
      }

      profile.col -= item.price;
      if (item.weapon) {
        profile.weapons[item.weapon] = (profile.weapons[item.weapon] || 0) + 1;
      } else {
        profile.items[key] = (profile.items[key] || 0) + 1;
      }
      saveAc();
      await send(`✅ ${item.name} gekauft!`);
      return true;
    }

    // ---------- aincradinventory / aincradinv ----------
    if (cmd === 'aincradinventory' || cmd === 'aincradinv') {
      const viewerIsOwner = isPrimaryOwner ? isPrimaryOwner(jid) : false;
      const wLines = Object.entries(profile.weapons)
        .map(([id, qty]) => `${formatWeapon(id, viewerIsOwner)} x${qty}`)
        .join('\n') || '(keine)';
      const itemLines = Object.entries(profile.items)
        .map(([id, qty]) => `${SHOP[id]?.name || id} x${qty}`)
        .join('\n') || '(keine)';
      await send(
        `🎒 *— DEIN INVENTAR —* 🎒\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `🗡️ *Waffen:*\n${wLines}\n\n📦 *Items:*\n${itemLines}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\nAusrüsten mit: ${activePrefix}aincradequip <waffe>`
      );
      return true;
    }

    // ---------- aincradequip ----------
    if (cmd === 'aincradequip') {
      const key = (args.join('_') || args[0] || '').toLowerCase().replace(/\s+/g, '_');
      const matched = Object.keys(WEAPONS).find(id => id === key || WEAPONS[id].name.toLowerCase() === args.join(' ').toLowerCase());
      if (!matched) {
        await send(`❌ Nutzung: ${activePrefix}aincradequip <waffen-id>\nSieh dein Inventar mit ${activePrefix}aincradinventory.`);
        return true;
      }
      if (!profile.weapons[matched]) {
        await send(`❌ Du besitzt diese Waffe nicht.`);
        return true;
      }
      profile.equipped = matched;
      saveAc();
      await send(`✅ ${formatWeapon(matched)} ausgerüstet!`);
      return true;
    }

    // ---------- aincraduse ----------
    if (cmd === 'aincraduse') {
      const key = (args[0] || '').toLowerCase();
      const owned = profile.items[key];

      if (!owned) {
        await send(`❌ Du besitzt "${key || '???'}" nicht. Nutze ${activePrefix}aincradinventory zum Nachsehen.`);
        return true;
      }

      if (key === 'heal_potion' || key === 'hi_potion') {
        const xpGain = key === 'hi_potion' ? randInt(20, 40) : randInt(8, 18);
        profile.items[key]--;
        if (profile.items[key] <= 0) delete profile.items[key];
        const levelUps = addXp(profile, xpGain);
        saveAc();
        let text = `🧪 ${SHOP[key].name} benutzt! +${xpGain} XP`;
        if (levelUps.length) text += `\n🎉 Level-Up! Du bist jetzt Level ${levelUps[levelUps.length - 1]}!`;
        await send(text);
        return true;
      }

      if (key === 'crystal') {
        profile.items.crystal--;
        if (profile.items.crystal <= 0) delete profile.items.crystal;
        profile.lastExplore = 0;
        profile.lastBoss = 0;
        profile.lastDuel = 0;
        saveAc();
        await send(`✨ Erinnerungskristall benutzt! Alle Cooldowns (Erkunden, Boss, Duell) wurden zurückgesetzt.`);
        return true;
      }

      await send(`❌ "${key}" kann nicht benutzt werden.`);
      return true;
    }

    // ---------- aincradduel ----------
    if (cmd === 'aincradduel') {
      const cd = cooldownLeft(profile.lastDuel, DUEL_COOLDOWN);
      if (cd) {
        await send(`⏰ Du musst noch ${cd} warten, bevor du wieder duellieren kannst.`);
        return true;
      }

      const ctxInfo = m?.message?.extendedTextMessage?.contextInfo;
      let targetRaw = args[0];
      if (ctxInfo?.mentionedJid?.length) targetRaw = ctxInfo.mentionedJid[0];
      else if (ctxInfo?.participant) targetRaw = ctxInfo.participant;
      if (!targetRaw) {
        await send(`❌ Nutzung: ${activePrefix}aincradduel @gegner`);
        return true;
      }

      const targetJid = normalizeJid(targetRaw);
      if (targetJid === jid) {
        await send('❌ Du kannst nicht gegen dich selbst duellieren!');
        return true;
      }

      const opponent = acData[targetJid];
      if (!opponent) {
        await send('❌ Dieser Spieler hat noch keine Aincrad-Reise begonnen.');
        return true;
      }

      profile.lastDuel = Date.now();

      const myRoll = totalPower(profile) + randInt(0, 30);
      const oppRoll = totalPower(opponent) + randInt(0, 30);

      const colStake = Math.min(profile.col, opponent.col, 100);
      let text;
      const mentions = [jid, targetJid];

      if (myRoll >= oppRoll) {
        profile.wins++;
        opponent.losses++;
        profile.col += colStake;
        opponent.col = Math.max(0, opponent.col - colStake);
        const xpGain = randInt(10, 25);
        const levelUps = addXp(profile, xpGain);
        text =
          `⚔️ *Duell!* @${jid.split('@')[0]} vs. @${targetJid.split('@')[0]}\n` +
          `🏆 @${jid.split('@')[0]} gewinnt! (+${colStake} Col, +${xpGain} XP)`;
        if (levelUps.length) text += `\n🎉 Level-Up! Jetzt Level ${levelUps[levelUps.length - 1]}!`;
      } else {
        opponent.wins++;
        profile.losses++;
        opponent.col += colStake;
        profile.col = Math.max(0, profile.col - colStake);
        text =
          `⚔️ *Duell!* @${jid.split('@')[0]} vs. @${targetJid.split('@')[0]}\n` +
          `🏆 @${targetJid.split('@')[0]} gewinnt! (+${colStake} Col)`;
      }

      saveAc();
      await send(text, { mentions });
      return true;
    }

    // ---------- aincradleaderboard / aincradrangliste ----------
    if (cmd === 'aincradleaderboard' || cmd === 'aincradrangliste') {
      const entries = Object.entries(acData).sort((a, b) => {
        const scoreA = a[1].level * 1000 + a[1].xp;
        const scoreB = b[1].level * 1000 + b[1].xp;
        return scoreB - scoreA;
      }).slice(0, 10);

      if (!entries.length) {
        await send('📊 Noch keine Schwertkämpfer vorhanden.');
        return true;
      }

      const medals = ['🥇', '🥈', '🥉'];
      const lines = await Promise.all(entries.map(async ([eJid, p], i) => {
        const mention = getNumberMention ? await getNumberMention(eJid, sock) : `@${eJid.split('@')[0]}`;
        return `${medals[i] || `${i + 1}.`} ${mention} — Lv.${p.level} (${p.wins}S/${p.losses}N)`;
      }));

      await send(
        `⚔️ *— AINCRAD-BESTENLISTE —* ⚔️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`,
        { mentions: entries.map(([eJid]) => eJid) }
      );
      return true;
    }

    return false;
  }

  return {
    handle,
    AC_HELP_TEXT,
    AC_COMMANDS,
    FLOORS,
    WEAPONS
  };
}
