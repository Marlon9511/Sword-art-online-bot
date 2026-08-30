import fs from 'fs';
import path from 'path';


export function createKingdomHeartsSystem(DATA_PATH) {
  const KH_FILE = path.join(DATA_PATH, 'kingdomhearts.json');

  const ensureFile = () => {
    if (!fs.existsSync(KH_FILE)) {
      fs.writeFileSync(KH_FILE, JSON.stringify({}, null, 2));
    }
  };
  ensureFile();

  let khData = {};
  try {
    khData = JSON.parse(fs.readFileSync(KH_FILE, 'utf-8') || '{}');
  } catch (e) {
    khData = {};
  }

  const saveKh = () => {
    try {
      fs.writeFileSync(KH_FILE, JSON.stringify(khData, null, 2));
    } catch (e) {
      console.error('[kingdomhearts] Fehler beim Speichern:', e?.message || e);
    }
  };

  // ---------------------------------------------------------------
  // Keyblade-Datenbank
  // ---------------------------------------------------------------
  const RARITY_INFO = {
    common:    { label: 'Gewöhnlich', emoji: '⚪' },
    rare:      { label: 'Selten',     emoji: '🔵' },
    epic:      { label: 'Episch',     emoji: '🟣' },
    legendary: { label: 'Legendär',   emoji: '🟡' },
    mythic:    { label: 'Mythisch',   emoji: '⚫' }
  };

  const KEYBLADES = {
    kingdom_key:      { name: 'Kingdom Key',            power: 10, rarity: 'common',    price: 0,    world: null },
    dream_sword:      { name: 'Traumschwert',           power: 6,  rarity: 'common',    price: 150,  world: null },
    dream_shield:     { name: 'Traumschild',            power: 8,  rarity: 'common',    price: 150,  world: null },
    dream_rod:        { name: 'Traumstab',              power: 7,  rarity: 'common',    price: 150,  world: null },

    star_seeker:      { name: 'Sternensucher',          power: 18, rarity: 'rare',      price: 600,  world: 'traverse_town' },
    lady_luck:        { name: "Lady Luck",              power: 20, rarity: 'rare',      price: 700,  world: 'wonderland' },
    olympia:          { name: 'Olympia',                power: 24, rarity: 'rare',      price: 900,  world: 'olympus_coliseum' },

    jungle_king:      { name: 'Dschungelkönig',         power: 30, rarity: 'epic',      price: null, world: 'deep_jungle' },
    three_wishes:     { name: 'Drei Wünsche',           power: 34, rarity: 'epic',      price: null, world: 'agrabah' },
    pumpkinhead:      { name: 'Kürbiskopf',             power: 38, rarity: 'epic',      price: null, world: 'halloween_town' },
    wishing_star:     { name: 'Wunschstern',            power: 42, rarity: 'epic',      price: null, world: 'atlantica' },
    fairy_harp:       { name: 'Feenharfe',               power: 44, rarity: 'epic',      price: null, world: 'neverland' },

    oblivion:         { name: 'Oblivion',               power: 60, rarity: 'legendary', price: null, world: 'hollow_bastion' },
    oathkeeper:       { name: 'Oathkeeper',             power: 62, rarity: 'legendary', price: null, world: 'hollow_bastion' },

    ultima_weapon:    { name: 'Ultima Weapon',          power: 85, rarity: 'mythic',    price: null, world: 'end_of_the_world' },
    kingdom_key_d:    { name: 'Kingdom Key D',          power: 90, rarity: 'mythic',    price: null, world: 'end_of_the_world', secret: true, ownerOnly: true },
    winners_proof:    { name: "Winner's Proof",         power: 95, rarity: 'mythic',    price: null, world: 'end_of_the_world' }
  };

  // ---------------------------------------------------------------
  // Welten (Story-Reihenfolge)
  // ---------------------------------------------------------------
  const WORLDS = [
    {
      id: 'destiny_islands', name: '🏝️ insel des Schicksals', reqLevel: 1,
      boss: 'Dunkler Nebel', bossPower: 20, xp: 40, munny: 60,
      unlockKeyblade: null
    },
    {
      id: 'traverse_town', name: '🌆 traverse_town', reqLevel: 2,
      boss: 'Guard Armor', bossPower: 45, xp: 70, munny: 120,
      unlockKeyblade: null
    },
    {
      id: 'wonderland', name: '🃏 Wunderland', reqLevel: 4,
      boss: 'Trickmeister', bossPower: 70, xp: 100, munny: 180,
      unlockKeyblade: null
    },
    {
      id: 'olympus_coliseum', name: '🏛️ Olymp-Kolosseum', reqLevel: 6,
      boss: 'Cerberus', bossPower: 100, xp: 140, munny: 260,
      unlockKeyblade: null
    },
    {
      id: 'deep_jungle', name: '🌴 Tiefer Dschungel', reqLevel: 8,
      boss: 'Klumpfuß', bossPower: 130, xp: 180, munny: 320,
      unlockKeyblade: 'jungle_king'
    },
    {
      id: 'agrabah', name: '🏜️ Agrabah', reqLevel: 10,
      boss: 'Jafar', bossPower: 165, xp: 220, munny: 400,
      unlockKeyblade: 'three_wishes'
    },
    {
      id: 'halloween_town', name: '🎃 Halloween Town', reqLevel: 13,
      boss: 'Oogie Boogie', bossPower: 200, xp: 260, munny: 480,
      unlockKeyblade: 'pumpkinhead'
    },
    {
      id: 'atlantica', name: '🌊 Atlantica', reqLevel: 16,
      boss: 'Ursula', bossPower: 235, xp: 300, munny: 560,
      unlockKeyblade: 'wishing_star'
    },
    {
      id: 'neverland', name: '🧚 Nimmerland', reqLevel: 19,
      boss: 'Captain Hook', bossPower: 270, xp: 340, munny: 640,
      unlockKeyblade: 'fairy_harp'
    },
    {
      id: 'hollow_bastion', name: '🏰 Hollow Bastion', reqLevel: 23,
      boss: 'Dragon Maleficent', bossPower: 330, xp: 420, munny: 800,
      unlockKeyblade: 'oblivion'
    },
    {
      id: 'end_of_the_world', name: '🌑 Das Ende der Welt', reqLevel: 28,
      boss: 'Ansem, Sucher der Dunkelheit', bossPower: 420, xp: 600, munny: 1200,
      unlockKeyblade: 'ultima_weapon'
    }
  ];
  const worldIndex = id => WORLDS.findIndex(w => w.id === id);

  // Zufällige Heartless für das normale "Erkunden"
  const HEARTLESS = [
    { name: 'Schatten', emoji: '👤', power: 4 },
    { name: 'Soldat', emoji: '⚔️', power: 8 },
    { name: 'Groß-Körper', emoji: '💪', power: 12 },
    { name: 'Luftschiff', emoji: '🛸', power: 10 },
    { name: 'Wächter', emoji: '🛡️', power: 14 },
    { name: 'Wildwuchs', emoji: '🌵', power: 9 },
    { name: 'Blaues Reptil', emoji: '🦎', power: 11 }
  ];

  const SHOP = {
    potion:  { name: 'Trank',      price: 60,  desc: 'Heilt und gibt etwas XP' },
    hi_potion: { name: 'Hi-Trank', price: 150, desc: 'Stärkerer Trank, mehr XP' },
    ether:   { name: 'Äther',      price: 100, desc: 'Setzt einen Cooldown zurück' },
    dream_sword:  { name: 'Traumschwert', price: KEYBLADES.dream_sword.price, desc: 'Einsteiger-Keyblade', keyblade: 'dream_sword' },
    dream_shield: { name: 'Traumschild',  price: KEYBLADES.dream_shield.price, desc: 'Einsteiger-Keyblade', keyblade: 'dream_shield' },
    dream_rod:    { name: 'Traumstab',    price: KEYBLADES.dream_rod.price, desc: 'Einsteiger-Keyblade', keyblade: 'dream_rod' },
    star_seeker:  { name: 'Sternensucher', price: KEYBLADES.star_seeker.price, desc: 'Erfordert: Niemandsstadt besucht', keyblade: 'star_seeker', reqWorld: 'traverse_town' },
    lady_luck:    { name: 'Lady Luck', price: KEYBLADES.lady_luck.price, desc: 'Erfordert: Wunderland besucht', keyblade: 'lady_luck', reqWorld: 'wonderland' },
    olympia:      { name: 'Olympia', price: KEYBLADES.olympia.price, desc: 'Erfordert: Olymp-Kolosseum besucht', keyblade: 'olympia', reqWorld: 'olympus_coliseum' }
  };

  const KH_COMMANDS = [
    'khstart', 'khhelp', 'khprofile', 'khme',
    'khworld', 'khworlds', 'khtravel', 'khexplore', 'khboss',
    'khshop', 'khbuy', 'khinventory', 'khinv', 'khequip',
    'khduel', 'khleaderboard', 'khrangliste'
  ];

  const KH_HELP_TEXT =
`✨ *— KINGDOM HEARTS SYSTEM —* ✨
${' '}
{P}khstart — Reise beginnen
{P}khprofile — Dein Profil
{P}khworlds — Welten-Übersicht
{P}khtravel <welt> — Zu einer freigeschalteten Welt reisen
{P}khexplore — Heartless in der aktuellen Welt bekämpfen
{P}khboss — Weltboss herausfordern (schaltet nächste Welt frei)
{P}khshop — Keyblade- & Item-Shop
{P}khbuy <item> — Etwas kaufen
{P}khinventory — Dein Inventar
{P}khequip <keyblade> — Keyblade ausrüsten
{P}khduel @user — Gegen einen anderen Spieler duellieren
{P}khleaderboard — Bestenliste`;

  // ---------------------------------------------------------------
  // Hilfsfunktionen
  // ---------------------------------------------------------------
  function ensureProfile(jid) {
    if (!khData[jid]) {
      khData[jid] = {
        level: 1,
        xp: 0,
        munny: 100,
        equipped: 'kingdom_key',
        keyblades: { kingdom_key: 1 },
        items: {},
        currentWorld: 'destiny_islands',
        unlockedWorlds: ['destiny_islands'],
        clearedWorlds: [],
        wins: 0,
        losses: 0,
        lastExplore: 0,
        lastBoss: 0,
        lastDuel: 0,
        createdAt: Date.now()
      };
      saveKh();
    }
    return khData[jid];
  }

  function xpNeeded(level) {
    return 60 + level * 35;
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

  function keybladePower(id) {
    return KEYBLADES[id]?.power || 0;
  }

  function totalPower(profile) {
    return profile.level * 6 + keybladePower(profile.equipped);
  }

  function formatKeyblade(id, viewerIsPrimaryOwner = false) {
    const kb = KEYBLADES[id];
    if (!kb) return id;
    if (kb.ownerOnly && !viewerIsPrimaryOwner) return '❓ Unbekannte Keyblade';
    const rarity = RARITY_INFO[kb.rarity]?.emoji || '';
    return `${rarity} ${kb.name} (⚔️ ${kb.power})`;
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

    if (!KH_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    const profile = ensureProfile(jid);

    // ---------- khstart ----------
    if (cmd === 'khstart') {
      await send(
        `✨ Willkommen in *Kingdom Hearts*, Schlüsselträger!\n\n` +
        `Du beginnst mit der *Kingdom Key* ausgerüstet und ${profile.munny} Munny.\n` +
        `Nutze ${activePrefix}khworlds um die Welten zu sehen, und ${activePrefix}khexplore um deine Reise auf der Insel des Schicksals zu beginnen.`
      );
      return true;
    }

    // ---------- khhelp ----------
    if (cmd === 'khhelp') {
      await send(KH_HELP_TEXT.replace(/\{P\}/g, activePrefix));
      return true;
    }

    // ---------- khprofile / khme ----------
    if (cmd === 'khprofile' || cmd === 'khme') {
      const world = WORLDS.find(w => w.id === profile.currentWorld);
      const needed = xpNeeded(profile.level);
      const viewerIsOwner = isPrimaryOwner ? isPrimaryOwner(jid) : false;
      const text =
        `✨ *— SCHLÜSSELTRÄGER-PROFIL —* ✨\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `⭐ Level: ${profile.level}\n` +
        `✨ XP: ${profile.xp} / ${needed}\n` +
        `💰 Munny: ${profile.munny}\n` +
        `🗝️ Keyblade: ${formatKeyblade(profile.equipped, viewerIsOwner)}\n` +
        `🌍 Aktuelle Welt: ${world ? world.name : '(unbekannt)'}\n` +
        `🏆 Duelle: ${profile.wins}S / ${profile.losses}N\n` +
        `📜 Freigeschaltete Welten: ${profile.unlockedWorlds.length}/${WORLDS.length}`;
      await send(text);
      return true;
    }

    // ---------- khworld / khworlds ----------
    if (cmd === 'khworld' || cmd === 'khworlds') {
      const lines = WORLDS.map((w, i) => {
        const unlocked = profile.unlockedWorlds.includes(w.id);
        const cleared = profile.clearedWorlds.includes(w.id);
        const isCurrent = profile.currentWorld === w.id;
        let status = '🔒 Gesperrt';
        if (cleared) status = '✅ Abgeschlossen';
        else if (unlocked) status = '🔓 Freigeschaltet';
        const marker = isCurrent ? ' 👉' : '';
        return `${i + 1}. ${w.name} — ${status}${marker} (ab Lv.${w.reqLevel})`;
      });
      await send(
        `🌍 *— WELTEN VON KINGDOM HEARTS —* 🌍\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n_Reise mit ${activePrefix}khtravel <weltname>_`
      );
      return true;
    }

    // ---------- khtravel ----------
    if (cmd === 'khtravel') {
      const query = args.join(' ').trim().toLowerCase();
      if (!query) await send(`❌ Nutzung: ${activePrefix}khtravel <weltname>\nBeispiel: ${activePrefix}khtravel agrabah`);
      return true;

      const target = WORLDS.find(w =>
        w.id.replace(/_/g, ' ') === query ||
        w.id === query.replace(/\s+/g, '_') ||
        w.name.toLowerCase().includes(query)
      );
      if (!target) await send(`❌ Welt "${query}" nicht gefunden. Nutze ${activePrefix}khworlds für die Liste.`);
      return true;
      if (!profile.unlockedWorlds.includes(target.id)) {
        await send(`🔒 ${target.name} ist noch gesperrt. Besiege zuerst den Boss der vorherigen Welt.`);
      return true;
      }
      profile.currentWorld = target.id;
      saveKh();
      await send(`🌍 Du reist nach *${target.name}*!\nNutze ${activePrefix}khexplore um Heartless zu bekämpfen oder ${activePrefix}khboss für den Weltboss.`);
      return true;
    }

    // ---------- khexplore ----------
    if (cmd === 'khexplore') {
      const cd = cooldownLeft(profile.lastExplore, EXPLORE_COOLDOWN);
      if (cd) await send(`⏰ Du musst noch ${cd} warten, bevor du wieder erkunden kannst.`);
      return true;

      const world = WORLDS.find(w => w.id === profile.currentWorld);
      const heartless = HEARTLESS[randInt(0, HEARTLESS.length - 1)];
      const myPower = totalPower(profile);
      const enemyPower = heartless.power + randInt(0, 10);

      profile.lastExplore = Date.now();

      if (myPower >= enemyPower || Math.random() < 0.8) {
        const xpGain = randInt(8, 20);
        const munnyGain = randInt(15, 45);
        profile.munny += munnyGain;
        const levelUps = addXp(profile, xpGain);
        saveKh();
        let text =
          `${heartless.emoji} *${heartless.name}* in ${world?.name || 'der Welt'} besiegt!\n` +
          `+${xpGain} XP, +${munnyGain} Munny`;
        if (levelUps.length) text += `\n🎉 Level-Up! Du bist jetzt Level ${levelUps[levelUps.length - 1]}!`;
        await send(text);
      return true;
      } else {
        const lostMunny = Math.min(profile.munny, randInt(5, 20));
        profile.munny -= lostMunny;
        saveKh();
        await send(`${heartless.emoji} *${heartless.name}* hat dich überwältigt! -${lostMunny} Munny.\nRüste eine stärkere Keyblade aus oder leve zuerst.`);
      return true;
      }
    }

    // ---------- khboss ----------
    if (cmd === 'khboss') {
      const cd = cooldownLeft(profile.lastBoss, BOSS_COOLDOWN);
      if (cd) await send(`⏰ Der Weltboss regeneriert sich noch. Warte ${cd}.`);
      return true;

      const world = WORLDS.find(w => w.id === profile.currentWorld);
      if (!world) await send('❌ Ungültige aktuelle Welt.');
      return true;
      if (profile.clearedWorlds.includes(world.id)) {
        await send(`✅ Du hast ${world.name} bereits abgeschlossen. Reise weiter mit ${activePrefix}khtravel.`);
      return true;
      }
      if (profile.level < world.reqLevel) {
        await send(`⚠️ Du solltest mindestens Level ${world.reqLevel} sein, um gegen *${world.boss}* zu bestehen (aktuell: Lv.${profile.level}). Nutze ${activePrefix}khexplore zum Leveln.`);
      return true;
      }

      profile.lastBoss = Date.now();
      const myPower = totalPower(profile) + randInt(0, 25);
      const bossPower = world.bossPower + randInt(0, 20);

      if (myPower >= bossPower) {
        if (!profile.clearedWorlds.includes(world.id)) profile.clearedWorlds.push(world.id);
        profile.munny += world.munny;
        const levelUps = addXp(profile, world.xp);

        const idx = worldIndex(world.id);
        const nextWorld = WORLDS[idx + 1];
        if (nextWorld && !profile.unlockedWorlds.includes(nextWorld.id)) {
          profile.unlockedWorlds.push(nextWorld.id);
        }

        let rewardLine = '';
        if (world.unlockKeyblade) {
          profile.keyblades[world.unlockKeyblade] = (profile.keyblades[world.unlockKeyblade] || 0) + 1;
          rewardLine = `\n🗝️ Neue Keyblade erhalten: ${formatKeyblade(world.unlockKeyblade)}!`;
        }
        saveKh();

        let text =
          `🏆 *${world.boss}* wurde besiegt!\n` +
          `+${world.xp} XP, +${world.munny} Munny${rewardLine}`;
        if (levelUps.length) text += `\n🎉 Level-Up! Du bist jetzt Level ${levelUps[levelUps.length - 1]}!`;
        if (nextWorld) text += `\n\n🔓 Neue Welt freigeschaltet: *${nextWorld.name}*!`;
        else text += `\n\n✨ Du hast alle Welten abgeschlossen! Kingdom Hearts liegt in deiner Hand.`;

        await send(text);
      return true;
      } else {
        saveKh();
        await send(`💔 *${world.boss}* war zu stark für dich! Werde stärker mit ${activePrefix}khexplore und versuche es erneut.`);
      return true;
      }
    }

    // ---------- khshop ----------
    if (cmd === 'khshop') {
      let out = '🛒 *— KEYBLADE- & ITEM-SHOP —* 🛒\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n';
      for (const [key, item] of Object.entries(SHOP)) {
        out += `• ${key} — ${item.price} 💰 Munny | ${item.name} (${item.desc})\n`;
      }
      out += `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\nKaufen mit: ${activePrefix}khbuy <item>`;
      await send(out);
      return true;
    }

    // ---------- khbuy ----------
    if (cmd === 'khbuy') {
      const key = (args[0] || '').toLowerCase();
      const item = SHOP[key];
      if (!item) await send(`❌ Unbekanntes Item. Nutze ${activePrefix}khshop für die Liste.`);
      return true;
      if (item.reqWorld && !profile.unlockedWorlds.includes(item.reqWorld)) {
        const w = WORLDS.find(x => x.id === item.reqWorld);
        await send(`🔒 Du musst zuerst ${w?.name || item.reqWorld} freigeschaltet haben.`);
      return true;
      }
      if (profile.munny < item.price) await send(`💸 Nicht genug Munny (du hast ${profile.munny}, benötigt: ${item.price}).`);
      return true;

      profile.munny -= item.price;
      if (item.keyblade) {
        profile.keyblades[item.keyblade] = (profile.keyblades[item.keyblade] || 0) + 1;
      } else {
        profile.items[key] = (profile.items[key] || 0) + 1;
      }
      saveKh();
      await send(`✅ ${item.name} gekauft!`);
      return true;
    }

    // ---------- khinventory / khinv ----------
    if (cmd === 'khinventory' || cmd === 'khinv') {
      const viewerIsOwner = isPrimaryOwner ? isPrimaryOwner(jid) : false;
      const kbLines = Object.entries(profile.keyblades)
        .map(([id, qty]) => `${formatKeyblade(id, viewerIsOwner)} x${qty}`)
        .join('\n') || '(keine)';
      const itemLines = Object.entries(profile.items)
        .map(([id, qty]) => `${SHOP[id]?.name || id} x${qty}`)
        .join('\n') || '(keine)';
      await send(
        `🎒 *— DEIN INVENTAR —* 🎒\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `🗝️ *Keyblades:*\n${kbLines}\n\n📦 *Items:*\n${itemLines}\n` +
        `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\nAusrüsten mit: ${activePrefix}khequip <keyblade>`
      );
      return true;
    }

    // ---------- khequip ----------
    if (cmd === 'khequip') {
      const key = (args.join('_') || args[0] || '').toLowerCase().replace(/\s+/g, '_');
      const matched = Object.keys(KEYBLADES).find(id => id === key || KEYBLADES[id].name.toLowerCase() === args.join(' ').toLowerCase());
      if (!matched) await send(`❌ Nutzung: ${activePrefix}khequip <keyblade-id>\nSieh dein Inventar mit ${activePrefix}khinventory.`);
      return true;
      if (!profile.keyblades[matched]) await send(`❌ Du besitzt diese Keyblade nicht.`);
      return true;
      profile.equipped = matched;
      saveKh();
      await send(`✅ ${formatKeyblade(matched)} ausgerüstet!`);
      return true;
    }

    // ---------- khduel ----------
    if (cmd === 'khduel') {
      const cd = cooldownLeft(profile.lastDuel, DUEL_COOLDOWN);
      if (cd) await send(`⏰ Du musst noch ${cd} warten, bevor du wieder duellieren kannst.`);
      return true;

      const ctxInfo = m?.message?.extendedTextMessage?.contextInfo;
      let targetRaw = args[0];
      if (ctxInfo?.mentionedJid?.length) targetRaw = ctxInfo.mentionedJid[0];
      else if (ctxInfo?.participant) targetRaw = ctxInfo.participant;
      if (!targetRaw) await send(`❌ Nutzung: ${activePrefix}khduel @gegner`);
      return true;

      const targetJid = normalizeJid(targetRaw);
      if (targetJid === jid) await send('❌ Du kannst nicht gegen dich selbst duellieren!');
      return true;

      const opponent = khData[targetJid];
      if (!opponent) await send('❌ Dieser Spieler hat noch keine Kingdom-Hearts-Reise begonnen.');
      return true;

      profile.lastDuel = Date.now();

      const myRoll = totalPower(profile) + randInt(0, 30);
      const oppRoll = totalPower(opponent) + randInt(0, 30);

      const munnyStake = Math.min(profile.munny, opponent.munny, 100);
      let text;
      const mentions = [jid, targetJid];

      if (myRoll >= oppRoll) {
        profile.wins++;
        opponent.losses++;
        profile.munny += munnyStake;
        opponent.munny = Math.max(0, opponent.munny - munnyStake);
        const xpGain = randInt(10, 25);
        const levelUps = addXp(profile, xpGain);
        text =
          `⚔️ *Duell!* @${jid.split('@')[0]} vs. @${targetJid.split('@')[0]}\n` +
          `🏆 @${jid.split('@')[0]} gewinnt! (+${munnyStake} Munny, +${xpGain} XP)`;
        if (levelUps.length) text += `\n🎉 Level-Up! Jetzt Level ${levelUps[levelUps.length - 1]}!`;
      } else {
        opponent.wins++;
        profile.losses++;
        opponent.munny += munnyStake;
        profile.munny = Math.max(0, profile.munny - munnyStake);
        text =
          `⚔️ *Duell!* @${jid.split('@')[0]} vs. @${targetJid.split('@')[0]}\n` +
          `🏆 @${targetJid.split('@')[0]} gewinnt! (+${munnyStake} Munny)`;
      }

      saveKh();
      await send(text, { mentions });
      return true;
    }

    // ---------- khleaderboard / khrangliste ----------
    if (cmd === 'khleaderboard' || cmd === 'khrangliste') {
      const entries = Object.entries(khData).sort((a, b) => {
        const scoreA = a[1].level * 1000 + a[1].xp;
        const scoreB = b[1].level * 1000 + b[1].xp;
        return scoreB - scoreA;
      }).slice(0, 10);

      if (!entries.length) await send('📊 Noch keine Schlüsselträger vorhanden.');
      return true;

      const medals = ['🥇', '🥈', '🥉'];
      const lines = await Promise.all(entries.map(async ([eJid, p], i) => {
        const mention = getNumberMention ? await getNumberMention(eJid, sock) : `@${eJid.split('@')[0]}`;
        return `${medals[i] || `${i + 1}.`} ${mention} — Lv.${p.level} (${p.wins}S/${p.losses}N)`;
      }));

      await send(
        `✨ *— KINGDOM HEARTS BESTENLISTE —* ✨\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`,
        { mentions: entries.map(([eJid]) => eJid) }
      );
      return true;
    }

    return false;
  }

  return {
    handle,
    KH_HELP_TEXT,
    KH_COMMANDS,
    WORLDS,
    KEYBLADES
  };
}
