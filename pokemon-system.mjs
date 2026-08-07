// ============================================================
// POKEMON-SYSTEM.MJS — Fangen, Leveln, Entwickeln, Pokédex, PVP
// ============================================================

export const POKEMON_COMMANDS = [
  'pokestarter', 'wild', 'catch', 'pokemon', 'p', 'pokeinfo',
  'pokeactive', 'pokename', 'pokerelease', 'pokedex',
  'pokeshop', 'pokebuy', 'poketrain', 'pokevolve', 'pokebattle', 'pokehelp'
];

export const POKEMON_HELP_TEXT = `
▸ {P}pokestarter <feuer|wasser|pflanze> — Starter-Pokémon wählen
▸ {P}wild — Wildes Pokémon suchen
▸ {P}catch [ball] — Aktuelles wildes Pokémon fangen
▸ {P}pokemon / {P}p — Dein Team anzeigen
▸ {P}pokeinfo <nr> — Details zu einem Pokémon
▸ {P}pokeactive <nr> — Aktives Pokémon festlegen
▸ {P}pokename <nr> <name> — Spitzname vergeben
▸ {P}pokerelease <nr> — Pokémon freilassen
▸ {P}pokedex — Deinen Entdeckungsfortschritt anzeigen
▸ {P}pokeshop — Pokébälle kaufen (Preise anzeigen)
▸ {P}pokebuy <ball> <menge> — Pokébälle kaufen
▸ {P}poketrain <nr> — Pokémon trainieren (kostet Coins)
▸ {P}pokevolve <nr> — Pokémon entwickeln (falls bereit)
▸ {P}pokebattle @user — PVP-Kampf mit deinem aktiven Pokémon
`;

const TYPE_EMOJI = {
  feuer: '🔥', wasser: '💧', pflanze: '🌿', elektro: '⚡',
  normal: '⭐', gift: '☠️', boden: '🌍', flug: '🌪️',
  psycho: '🔮', gestein: '🪨', eis: '❄️', drache: '🐉'
};

const RARITY_INFO = {
  starter:   { label: 'Starter',   emoji: '🌟' },
  common:    { label: 'Gewöhnlich', emoji: '⚪' },
  uncommon:  { label: 'Ungewöhnlich', emoji: '🟢' },
  rare:      { label: 'Selten',    emoji: '🔵' },
  legendary: { label: 'Legendär',  emoji: '🟡' }
};

const RARITY_WEIGHTS = { starter: 0, common: 55, uncommon: 30, rare: 12, legendary: 3 };

const POKEMON_DB = {
  bisasam:    { name: 'Bisasam',    type: 'pflanze', catchRate: 60, evolvesAt: 12, evolvesTo: 'bisaknosp', hp: 45, power: 12, rarity: 'starter' },
  bisaknosp:  { name: 'Bisaknosp',  type: 'pflanze', catchRate: 40, evolvesAt: 28, evolvesTo: 'bisaflor',  hp: 65, power: 22, rarity: 'starter' },
  bisaflor:   { name: 'Bisaflor',   type: 'pflanze', catchRate: 15, evolvesAt: null, evolvesTo: null,      hp: 90, power: 38, rarity: 'starter' },

  glumanda:   { name: 'Glumanda',   type: 'feuer', catchRate: 60, evolvesAt: 12, evolvesTo: 'glutexo', hp: 42, power: 14, rarity: 'starter' },
  glutexo:    { name: 'Glutexo',    type: 'feuer', catchRate: 40, evolvesAt: 30, evolvesTo: 'glurak',  hp: 62, power: 24, rarity: 'starter' },
  glurak:     { name: 'Glurak',     type: 'feuer', catchRate: 12, evolvesAt: null, evolvesTo: null,    hp: 88, power: 42, rarity: 'starter' },

  schiggy:    { name: 'Schiggy',    type: 'wasser', catchRate: 60, evolvesAt: 12, evolvesTo: 'schillok', hp: 46, power: 12, rarity: 'starter' },
  schillok:   { name: 'Schillok',   type: 'wasser', catchRate: 40, evolvesAt: 30, evolvesTo: 'turtok',   hp: 66, power: 22, rarity: 'starter' },
  turtok:     { name: 'Turtok',     type: 'wasser', catchRate: 14, evolvesAt: null, evolvesTo: null,     hp: 92, power: 36, rarity: 'starter' },

  rattfratz:  { name: 'Rattfratz',  type: 'normal', catchRate: 85, evolvesAt: 10, evolvesTo: 'rattikarl', hp: 30, power: 8,  rarity: 'common' },
  rattikarl:  { name: 'Rattikarl',  type: 'normal', catchRate: 55, evolvesAt: null, evolvesTo: null,      hp: 55, power: 18, rarity: 'common' },

  taubsi:     { name: 'Taubsi',     type: 'flug', catchRate: 80, evolvesAt: 18, evolvesTo: 'tauboga',   hp: 35, power: 10, rarity: 'common' },
  tauboga:    { name: 'Tauboga',    type: 'flug', catchRate: 50, evolvesAt: 36, evolvesTo: 'taubossi',  hp: 58, power: 20, rarity: 'common' },
  taubossi:   { name: 'Taubossi',   type: 'flug', catchRate: 20, evolvesAt: null, evolvesTo: null,      hp: 85, power: 34, rarity: 'uncommon' },

  pikachu:    { name: 'Pikachu',    type: 'elektro', catchRate: 45, evolvesAt: 24, evolvesTo: 'raichu', hp: 40, power: 20, rarity: 'uncommon' },
  raichu:     { name: 'Raichu',     type: 'elektro', catchRate: 18, evolvesAt: null, evolvesTo: null,   hp: 70, power: 40, rarity: 'rare' },

  sandan:     { name: 'Sandan',     type: 'boden', catchRate: 65, evolvesAt: 22, evolvesTo: 'sandamer', hp: 44, power: 16, rarity: 'common' },
  sandamer:   { name: 'Sandamer',   type: 'boden', catchRate: 35, evolvesAt: null, evolvesTo: null,      hp: 68, power: 28, rarity: 'uncommon' },

  nidoranm:   { name: 'Nidoran♂',   type: 'gift', catchRate: 70, evolvesAt: 16, evolvesTo: 'nidorino', hp: 38, power: 14, rarity: 'common' },
  nidorino:   { name: 'Nidorino',   type: 'gift', catchRate: 45, evolvesAt: 32, evolvesTo: 'nidoking', hp: 60, power: 24, rarity: 'uncommon' },
  nidoking:   { name: 'Nidoking',   type: 'gift', catchRate: 14, evolvesAt: null, evolvesTo: null,     hp: 90, power: 40, rarity: 'rare' },

  evoli:      { name: 'Evoli',      type: 'normal', catchRate: 40, evolvesAt: null, evolvesTo: null, hp: 42, power: 16, rarity: 'uncommon' },

  onix:       { name: 'Onix',       type: 'gestein', catchRate: 30, evolvesAt: null, evolvesTo: null, hp: 75, power: 26, rarity: 'uncommon' },
  digda:      { name: 'Digda',      type: 'boden', catchRate: 60, evolvesAt: 26, evolvesTo: 'digdri', hp: 32, power: 14, rarity: 'common' },
  digdri:     { name: 'Digdri',     type: 'boden', catchRate: 28, evolvesAt: null, evolvesTo: null,   hp: 55, power: 26, rarity: 'uncommon' },

  abra:       { name: 'Abra',       type: 'psycho', catchRate: 55, evolvesAt: 16, evolvesTo: 'kadabra', hp: 30, power: 18, rarity: 'uncommon' },
  kadabra:    { name: 'Kadabra',    type: 'psycho', catchRate: 25, evolvesAt: 36, evolvesTo: 'simsala', hp: 50, power: 32, rarity: 'rare' },
  simsala:    { name: 'Simsala',    type: 'psycho', catchRate: 8,  evolvesAt: null, evolvesTo: null,    hp: 78, power: 48, rarity: 'legendary' },

  karpador:   { name: 'Karpador',   type: 'wasser', catchRate: 90, evolvesAt: 20, evolvesTo: 'garados', hp: 20, power: 4,  rarity: 'common' },
  garados:    { name: 'Garados',    type: 'wasser', catchRate: 6,  evolvesAt: null, evolvesTo: null,    hp: 95, power: 50, rarity: 'legendary' },

  dratini:    { name: 'Dratini',    type: 'drache', catchRate: 20, evolvesAt: 30, evolvesTo: 'dragonair', hp: 42, power: 20, rarity: 'rare' },
  dragonair:  { name: 'Dragonair',  type: 'drache', catchRate: 10, evolvesAt: 55, evolvesTo: 'dragoran',  hp: 70, power: 36, rarity: 'legendary' },
  dragoran:   { name: 'Dragoran',   type: 'drache', catchRate: 3,  evolvesAt: null, evolvesTo: null,      hp: 100, power: 60, rarity: 'legendary' },

  mewtu:      { name: 'Mewtu',      type: 'psycho', catchRate: 1, evolvesAt: null, evolvesTo: null, hp: 106, power: 80, rarity: 'legendary' }
};

const BALL_TYPES = {
  pokeball:    { name: 'Pokéball',    bonus: 1.0, price: 50 },
  superball:   { name: 'Superball',   bonus: 1.6, price: 150 },
  meisterball: { name: 'Meisterball', bonus: null, price: 5000 } // fängt immer
};

const ENCOUNTER_COOLDOWN_MS = 3 * 60 * 1000; // 3 Minuten
const WILD_EXPIRES_MS = 5 * 60 * 1000;       // 5 Minuten Zeit zum Fangen

function xpNeeded(level) {
  return 30 + level * 15;
}

function ensurePoke(users, jid) {
  if (!users[jid]) return;
  if (!users[jid].poke) {
    users[jid].poke = {
      starter: null,
      team: [],
      active: null,
      dex: {},
      lastEncounter: 0,
      wild: null,
      pokeballs: { pokeball: 5, superball: 0, meisterball: 0 }
    };
  }
  const p = users[jid].poke;
  if (!p.team) p.team = [];
  if (!p.dex) p.dex = {};
  if (!p.pokeballs) p.pokeballs = { pokeball: 5, superball: 0, meisterball: 0 };
}

function pickWildSpecies() {
  const pool = Object.entries(POKEMON_DB).filter(([, d]) => RARITY_WEIGHTS[d.rarity] > 0);
  const totalWeight = pool.reduce((sum, [, d]) => sum + RARITY_WEIGHTS[d.rarity], 0);
  let roll = Math.random() * totalWeight;
  for (const [id, d] of pool) {
    roll -= RARITY_WEIGHTS[d.rarity];
    if (roll <= 0) return id;
  }
  return pool[0][0];
}

function formatPoke(species, level, nickname) {
  const d = POKEMON_DB[species];
  const emoji = TYPE_EMOJI[d.type] || '';
  const rarity = RARITY_INFO[d.rarity]?.emoji || '';
  const displayName = nickname ? `${nickname} (${d.name})` : d.name;
  return `${rarity} ${emoji} ${displayName} — Lv.${level}`;
}

function effectivePower(species, level) {
  const d = POKEMON_DB[species];
  return Math.round(d.power + level * 2 + Math.random() * 6);
}

export function createPokemonSystem() {
  async function handle(ctx) {
    const {
      cmd, args, sender, from, m, isGroup, activePrefix, send, sock,
      users, save, FILES, ensureUser, normalizeJid, isSameJid,
      getNumberMention, randInt, sleep, isPrimaryOwner
    } = ctx;

    if (!POKEMON_COMMANDS.includes(cmd)) return false;

    ensureUser(sender);
    ensurePoke(users, sender);
    const p = users[sender].poke;
    const persist = () => save(FILES.users, users);

    // ---- HILFE ----
    if (cmd === 'pokehelp') {
      await send('🐾 *Pokémon-Hilfe*\n' + POKEMON_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, activePrefix)).join('\n'));
      return true;
    }

    // ---- STARTER ----
    if (cmd === 'pokestarter') {
      if (p.starter) {
        await send('❌ Du hast bereits ein Starter-Pokémon gewählt.');
        return true;
      }
      const choice = (args[0] || '').toLowerCase();
      const map = { feuer: 'glumanda', wasser: 'schiggy', pflanze: 'bisasam' };
      if (!map[choice]) {
        await send(`❌ Nutzung: ${activePrefix}pokestarter <feuer|wasser|pflanze>`);
        return true;
      }
      const speciesId = map[choice];
      const uid = 'PK' + Date.now().toString(36) + randInt(100, 999);
      p.team.push({ uid, species: speciesId, level: 5, xp: 0, nickname: null });
      p.active = uid;
      p.starter = speciesId;
      p.dex[speciesId] = true;
      persist();
      await send(`🎉 Du hast dich für *${POKEMON_DB[speciesId].name}* entschieden!\n${formatPoke(speciesId, 5, null)}\n\nNutze ${activePrefix}wild, um wilde Pokémon zu finden.`);
      return true;
    }

    if (!p.starter) {
      await send(`❌ Du brauchst zuerst ein Starter-Pokémon! Nutze ${activePrefix}pokestarter <feuer|wasser|pflanze>`);
      return true;
    }

    // ---- WILD ----
    if (cmd === 'wild') {
      const now = Date.now();
      if (now - (p.lastEncounter || 0) < ENCOUNTER_COOLDOWN_MS) {
        const remaining = Math.ceil((ENCOUNTER_COOLDOWN_MS - (now - p.lastEncounter)) / 1000);
        await send(`⏰ Warte noch ${remaining}s bis zum nächsten wilden Pokémon.`);
        return true;
      }
      const avgLevel = p.team.length ? Math.round(p.team.reduce((s, x) => s + x.level, 0) / p.team.length) : 5;
      const speciesId = pickWildSpecies();
      const level = Math.max(1, randInt(Math.max(1, avgLevel - 3), avgLevel + 5));
      p.wild = { species: speciesId, level, expiresAt: now + WILD_EXPIRES_MS };
      p.lastEncounter = now;
      persist();
      const d = POKEMON_DB[speciesId];
      await send(
        `🌿 Ein wildes Pokémon erscheint!\n${formatPoke(speciesId, level, null)}\nTyp: ${TYPE_EMOJI[d.type] || ''} ${d.type}\n\n` +
        `Fange es mit ${activePrefix}catch [pokeball|superball|meisterball]\n(Du hast 5 Minuten Zeit, sonst flieht es.)`
      );
      return true;
    }

    // ---- CATCH ----
    if (cmd === 'catch') {
      if (!p.wild || Date.now() > p.wild.expiresAt) {
        p.wild = null;
        await send(`❌ Kein wildes Pokémon in Sicht. Nutze ${activePrefix}wild.`);
        persist();
        return true;
      }
      const ballKey = (args[0] || 'pokeball').toLowerCase();
      if (!BALL_TYPES[ballKey]) {
        await send(`❌ Unbekannter Ball. Verfügbar: ${Object.keys(BALL_TYPES).join(', ')}`);
        return true;
      }
      if (!p.pokeballs[ballKey] || p.pokeballs[ballKey] <= 0) {
        await send(`❌ Du hast keine ${BALL_TYPES[ballKey].name} mehr. Nutze ${activePrefix}pokeshop.`);
        return true;
      }

      const species = p.wild.species;
      const level = p.wild.level;
      const d = POKEMON_DB[species];

      let chance;
      if (ballKey === 'meisterball') {
        chance = 100;
      } else {
        const levelPenalty = Math.max(0, level - 10) * 1.2;
        chance = Math.max(3, Math.min(95, d.catchRate * BALL_TYPES[ballKey].bonus - levelPenalty));
      }

      p.pokeballs[ballKey] -= 1;
      const roll = Math.random() * 100;

      if (roll <= chance) {
        const uid = 'PK' + Date.now().toString(36) + randInt(100, 999);
        p.team.push({ uid, species, level, xp: 0, nickname: null });
        p.dex[species] = true;
        p.wild = null;
        persist();
        await send(`✅ Gefangen! ${formatPoke(species, level, null)} wurde deinem Team hinzugefügt! (${BALL_TYPES[ballKey].name}, Chance war ${Math.round(chance)}%)`);
      } else {
        persist();
        await send(`💨 Das Pokémon ist ausgebrochen! (Chance war ${Math.round(chance)}%) Versuch es nochmal mit ${activePrefix}catch.`);
      }
      return true;
    }

    // ---- TEAM ----
    if (cmd === 'pokemon' || cmd === 'p') {
      if (!p.team.length) {
        await send('📋 Dein Team ist leer.');
        return true;
      }
      const lines = p.team.map((pk, i) => {
        const marker = pk.uid === p.active ? ' ⭐(aktiv)' : '';
        return `${i + 1}. ${formatPoke(pk.species, pk.level, pk.nickname)}${marker}`;
      });
      await send(`🐾 *Dein Pokémon-Team*\n\n${lines.join('\n')}`);
      return true;
    }

    // ---- POKEINFO ----
    if (cmd === 'pokeinfo') {
      const idx = parseInt(args[0]) - 1;
      if (isNaN(idx) || !p.team[idx]) {
        await send(`❌ Nutzung: ${activePrefix}pokeinfo <nummer>`);
        return true;
      }
      const pk = p.team[idx];
      const d = POKEMON_DB[pk.species];
      const needed = xpNeeded(pk.level);
      const evolveInfo = d.evolvesAt
        ? (pk.level >= d.evolvesAt ? `✅ Bereit zur Entwicklung zu ${POKEMON_DB[d.evolvesTo].name}!` : `Entwickelt sich ab Level ${d.evolvesAt}`)
        : 'Keine weitere Entwicklung';
      await send(
        `📋 *${pk.nickname || d.name}*\n` +
        `Spezies: ${d.name}\nTyp: ${TYPE_EMOJI[d.type] || ''} ${d.type}\n` +
        `Level: ${pk.level}\nXP: ${pk.xp} / ${needed}\n` +
        `Stärke: ${effectivePower(pk.species, pk.level)}\nHP: ${d.hp + pk.level * 3}\n` +
        `Entwicklung: ${evolveInfo}`
      );
      return true;
    }

    // ---- ACTIVE ----
    if (cmd === 'pokeactive') {
      const idx = parseInt(args[0]) - 1;
      if (isNaN(idx) || !p.team[idx]) {
        await send(`❌ Nutzung: ${activePrefix}pokeactive <nummer>`);
        return true;
      }
      p.active = p.team[idx].uid;
      persist();
      await send(`✅ ${formatPoke(p.team[idx].species, p.team[idx].level, p.team[idx].nickname)} ist jetzt dein aktives Pokémon.`);
      return true;
    }

    // ---- NAME ----
    if (cmd === 'pokename') {
      const idx = parseInt(args[0]) - 1;
      const name = args.slice(1).join(' ').trim();
      if (isNaN(idx) || !p.team[idx] || !name) {
        await send(`❌ Nutzung: ${activePrefix}pokename <nummer> <name>`);
        return true;
      }
      p.team[idx].nickname = name.slice(0, 20);
      persist();
      await send(`✅ Spitzname gesetzt: ${name}`);
      return true;
    }

    // ---- RELEASE ----
    if (cmd === 'pokerelease') {
      const idx = parseInt(args[0]) - 1;
      if (isNaN(idx) || !p.team[idx]) {
        await send(`❌ Nutzung: ${activePrefix}pokerelease <nummer>`);
        return true;
      }
      const removed = p.team.splice(idx, 1)[0];
      if (p.active === removed.uid) p.active = p.team[0]?.uid || null;
      persist();
      await send(`👋 ${formatPoke(removed.species, removed.level, removed.nickname)} wurde freigelassen.`);
      return true;
    }

    // ---- DEX ----
    if (cmd === 'pokedex') {
      const total = Object.keys(POKEMON_DB).length;
      const seen = Object.keys(p.dex).length;
      const lines = Object.entries(POKEMON_DB).map(([id, d]) => {
        const known = p.dex[id];
        return known ? `${RARITY_INFO[d.rarity]?.emoji || ''} ${d.name}` : '❓ ???';
      });
      await send(`📖 *Pokédex* (${seen}/${total} entdeckt)\n\n${lines.join('\n')}`);
      return true;
    }

    // ---- SHOP ----
    if (cmd === 'pokeshop') {
      let out = '🛒 *Pokéball-Shop*\n\n';
      for (const [key, b] of Object.entries(BALL_TYPES)) {
        out += `• ${b.name} — ${b.price} 💰 (Nutze: ${activePrefix}pokebuy ${key} <menge>)\n`;
      }
      out += `\n🎒 Dein Bestand:\n`;
      for (const [key, count] of Object.entries(p.pokeballs)) {
        out += `• ${BALL_TYPES[key].name}: ${count}\n`;
      }
      await send(out);
      return true;
    }

    // ---- BUY ----
    if (cmd === 'pokebuy') {
      const ballKey = (args[0] || '').toLowerCase();
      const amount = parseInt(args[1]) || 1;
      if (!BALL_TYPES[ballKey] || amount <= 0) {
        await send(`❌ Nutzung: ${activePrefix}pokebuy <pokeball|superball|meisterball> <menge>`);
        return true;
      }
      const cost = BALL_TYPES[ballKey].price * amount;
      if ((users[sender].coins || 0) < cost) {
        await send(`❌ Zu wenig Coins. Du brauchst ${cost} 💰.`);
        return true;
      }
      users[sender].coins -= cost;
      p.pokeballs[ballKey] = (p.pokeballs[ballKey] || 0) + amount;
      persist();
      await send(`✅ ${amount}x ${BALL_TYPES[ballKey].name} gekauft (-${cost} Coins).`);
      return true;
    }

    // ---- TRAIN ----
    if (cmd === 'poketrain') {
      const idx = parseInt(args[0]) - 1;
      if (isNaN(idx) || !p.team[idx]) {
        await send(`❌ Nutzung: ${activePrefix}poketrain <nummer>`);
        return true;
      }
      const cost = 30;
      if ((users[sender].coins || 0) < cost) {
        await send(`❌ Zu wenig Coins (benötigt: ${cost} 💰).`);
        return true;
      }
      users[sender].coins -= cost;
      const pk = p.team[idx];
      const gainedXp = randInt(15, 40);
      pk.xp += gainedXp;
      let leveledUp = false;
      while (pk.xp >= xpNeeded(pk.level)) {
        pk.xp -= xpNeeded(pk.level);
        pk.level += 1;
        leveledUp = true;
      }
      persist();
      const d = POKEMON_DB[pk.species];
      let msg = `🏋️ Training abgeschlossen! +${gainedXp} XP für ${pk.nickname || d.name}.`;
      if (leveledUp) msg += `\n🎉 Level-Up! Jetzt Level ${pk.level}.`;
      if (d.evolvesAt && pk.level >= d.evolvesAt) msg += `\n✨ Bereit zur Entwicklung! Nutze ${activePrefix}pokevolve ${idx + 1}.`;
      await send(msg);
      return true;
    }

    // ---- EVOLVE ----
    if (cmd === 'pokevolve') {
      const idx = parseInt(args[0]) - 1;
      if (isNaN(idx) || !p.team[idx]) {
        await send(`❌ Nutzung: ${activePrefix}pokevolve <nummer>`);
        return true;
      }
      const pk = p.team[idx];
      const d = POKEMON_DB[pk.species];
      if (!d.evolvesAt || !d.evolvesTo) {
        await send('❌ Dieses Pokémon kann sich nicht weiterentwickeln.');
        return true;
      }
      if (pk.level < d.evolvesAt) {
        await send(`❌ Benötigt Level ${d.evolvesAt} (aktuell: ${pk.level}).`);
        return true;
      }
      const oldName = d.name;
      pk.species = d.evolvesTo;
      p.dex[d.evolvesTo] = true;
      persist();
      await send(`✨ *${oldName}* hat sich zu *${POKEMON_DB[d.evolvesTo].name}* entwickelt! ${formatPoke(pk.species, pk.level, pk.nickname)}`);
      return true;
    }

    // ---- BATTLE (PVP) ----
    if (cmd === 'pokebattle') {
      const ctxInfo = m.message?.extendedTextMessage?.contextInfo;
      let target = args[0];
      if (!target && ctxInfo?.mentionedJid?.length) target = ctxInfo.mentionedJid[0];
      if (!target && ctxInfo?.participant) target = ctxInfo.participant;
      if (!target) {
        await send(`❌ Nutzung: ${activePrefix}pokebattle @user`);
        return true;
      }
      const targetJid = normalizeJid(target);
      if (isSameJid(sender, targetJid)) {
        await send('❌ Du kannst nicht gegen dich selbst kämpfen.');
        return true;
      }
      ensureUser(targetJid);
      ensurePoke(users, targetJid);
      const opp = users[targetJid].poke;

      if (!p.active || !p.team.find(x => x.uid === p.active)) {
        await send(`❌ Du hast kein aktives Pokémon. Nutze ${activePrefix}pokeactive.`);
        return true;
      }
      if (!opp.active || !opp.team.find(x => x.uid === opp.active)) {
        await send('❌ Der Gegner hat kein aktives Pokémon.');
        return true;
      }

      const myPk = p.team.find(x => x.uid === p.active);
      const oppPk = opp.team.find(x => x.uid === opp.active);

      const myPower = effectivePower(myPk.species, myPk.level);
      const oppPower = effectivePower(oppPk.species, oppPk.level);

      const winner = myPower >= oppPower ? sender : targetJid;
      const winnerPk = winner === sender ? myPk : oppPk;
      const loserPk = winner === sender ? oppPk : myPk;

      const xpGain = randInt(10, 25);
      winnerPk.xp += xpGain;
      while (winnerPk.xp >= xpNeeded(winnerPk.level)) {
        winnerPk.xp -= xpNeeded(winnerPk.level);
        winnerPk.level += 1;
      }
      const coinsReward = randInt(20, 60);
      users[winner].coins = (users[winner].coins || 0) + coinsReward;
      persist();
      save(FILES.users, users);

      const myName = users[sender].name || sender.split('@')[0];
      const oppName = users[targetJid].name || targetJid.split('@')[0];
      const winnerName = winner === sender ? myName : oppName;

      await send(
        `⚔️ *Pokémon-Kampf!*\n\n` +
        `${formatPoke(myPk.species, myPk.level, myPk.nickname)} (${myPower} STR)\nvs.\n` +
        `${formatPoke(oppPk.species, oppPk.level, oppPk.nickname)} (${oppPower} STR)\n\n` +
        `🏆 ${winnerName} gewinnt! +${xpGain} XP, +${coinsReward} Coins`,
        { mentions: [sender, targetJid] }
      );
      return true;
    }

    return false;
  }

  return { handle };
}