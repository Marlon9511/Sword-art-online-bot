export const POKEMON_COMMANDS = [
  'pokestarter', 'wild', 'catch', 'pokemon', 'p', 'pokeinfo',
  'pokeactive', 'pokename', 'pokerelease', 'pokedex',
  'pokeshop', 'pokebuy', 'poketrain', 'pokevolve', 'pokebattle',
  'pokesecret', 'pokehelp'
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
▸ {P}pokesecret <code> — Geheimcode einlösen für ein geheimes Pokémon
`;

const TYPE_EMOJI = {
  feuer: '🔥', wasser: '💧', pflanze: '🌿', elektro: '⚡',
  normal: '⭐', gift: '☠️', boden: '🌍', flug: '🌪️',
  psycho: '🔮', gestein: '🪨', eis: '❄️', drache: '🐉'
};

const RARITY_INFO = {
  starter:   { label: 'Starter',    emoji: '🌟' },
  common:    { label: 'Gewöhnlich', emoji: '⚪' },
  uncommon:  { label: 'Ungewöhnlich', emoji: '🟢' },
  rare:      { label: 'Selten',     emoji: '🔵' },
  legendary: { label: 'Legendär',   emoji: '🟡' },
  secret:    { label: 'Geheim',     emoji: '🟣' }
};

const RARITY_WEIGHTS = { starter: 0, common: 55, uncommon: 30, rare: 12, legendary: 3, secret: 0 };

export const POKEMON_DB = {
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

  mewtu:      { name: 'Mewtu',      type: 'psycho', catchRate: 1, evolvesAt: null, evolvesTo: null, hp: 106, power: 80, rarity: 'legendary' },

  zubat:      { name: 'Zubat',      type: 'gift', catchRate: 75, evolvesAt: 22, evolvesTo: 'golbat', hp: 32, power: 12, rarity: 'common' },
  golbat:     { name: 'Golbat',     type: 'gift', catchRate: 30, evolvesAt: null, evolvesTo: null,    hp: 68, power: 28, rarity: 'uncommon' },

  enton:      { name: 'Enton',      type: 'wasser', catchRate: 70, evolvesAt: 24, evolvesTo: 'entoron', hp: 40, power: 12, rarity: 'common' },
  entoron:    { name: 'Entoron',    type: 'wasser', catchRate: 30, evolvesAt: null, evolvesTo: null,     hp: 70, power: 30, rarity: 'uncommon' },

  tentacha:   { name: 'Tentacha',   type: 'wasser', catchRate: 55, evolvesAt: 30, evolvesTo: 'tentoxa', hp: 38, power: 16, rarity: 'uncommon' },
  tentoxa:    { name: 'Tentoxa',    type: 'wasser', catchRate: 20, evolvesAt: null, evolvesTo: null,     hp: 72, power: 32, rarity: 'rare' },

  krabby:     { name: 'Krabby',     type: 'wasser', catchRate: 65, evolvesAt: 28, evolvesTo: 'kingler', hp: 36, power: 16, rarity: 'common' },
  kingler:    { name: 'Kingler',    type: 'wasser', catchRate: 22, evolvesAt: null, evolvesTo: null,     hp: 66, power: 34, rarity: 'uncommon' },

  mauzi:      { name: 'Mauzi',      type: 'normal', catchRate: 75, evolvesAt: 28, evolvesTo: 'snobilikat', hp: 32, power: 10, rarity: 'common' },
  snobilikat: { name: 'Snobilikat', type: 'normal', catchRate: 35, evolvesAt: null, evolvesTo: null,       hp: 58, power: 24, rarity: 'uncommon' },

  ponita:     { name: 'Ponita',     type: 'feuer', catchRate: 50, evolvesAt: 40, evolvesTo: 'gallopa', hp: 44, power: 22, rarity: 'uncommon' },
  gallopa:    { name: 'Gallopa',    type: 'feuer', catchRate: 18, evolvesAt: null, evolvesTo: null,     hp: 78, power: 40, rarity: 'rare' },

  machollo:   { name: 'Machollo',   type: 'normal', catchRate: 55, evolvesAt: 26, evolvesTo: 'maschock', hp: 48, power: 20, rarity: 'uncommon' },
  maschock:   { name: 'Maschock',   type: 'normal', catchRate: 28, evolvesAt: 48, evolvesTo: 'machomei', hp: 72, power: 34, rarity: 'rare' },
  machomei:   { name: 'Machomei',   type: 'normal', catchRate: 10, evolvesAt: null, evolvesTo: null,      hp: 98, power: 52, rarity: 'legendary' },

  rettan:     { name: 'Rettan',     type: 'gift', catchRate: 65, evolvesAt: 22, evolvesTo: 'arbok', hp: 38, power: 16, rarity: 'common' },
  arbok:      { name: 'Arbok',      type: 'gift', catchRate: 25, evolvesAt: null, evolvesTo: null,   hp: 66, power: 32, rarity: 'uncommon' },

  smogon:     { name: 'Smogon',     type: 'gift', catchRate: 50, evolvesAt: 35, evolvesTo: 'smogmog', hp: 40, power: 18, rarity: 'uncommon' },
  smogmog:    { name: 'Smogmog',    type: 'gift', catchRate: 20, evolvesAt: null, evolvesTo: null,     hp: 70, power: 34, rarity: 'rare' },

  rihorn:     { name: 'Rihorn',     type: 'boden', catchRate: 55, evolvesAt: 42, evolvesTo: 'rizeros', hp: 52, power: 20, rarity: 'uncommon' },
  rizeros:    { name: 'Rizeros',    type: 'boden', catchRate: 15, evolvesAt: null, evolvesTo: null,     hp: 92, power: 44, rarity: 'rare' },

  flegmon:    { name: 'Flegmon',    type: 'wasser', catchRate: 45, evolvesAt: 38, evolvesTo: 'lahmus', hp: 60, power: 14, rarity: 'uncommon' },
  lahmus:     { name: 'Lahmus',     type: 'wasser', catchRate: 12, evolvesAt: null, evolvesTo: null,    hp: 110, power: 38, rarity: 'rare' },

  mew:        { name: 'Mew',        type: 'psycho', catchRate: 3, evolvesAt: null, evolvesTo: null, hp: 100, power: 70, rarity: 'secret' },
  zapdos:     { name: 'Zapdos',     type: 'elektro', catchRate: 2, evolvesAt: null, evolvesTo: null, hp: 108, power: 75, rarity: 'secret' },
  arktos:     { name: 'Arktos',     type: 'eis', catchRate: 2, evolvesAt: null, evolvesTo: null,     hp: 106, power: 74, rarity: 'secret' },
  lavados:    { name: 'Lavados',    type: 'feuer', catchRate: 2, evolvesAt: null, evolvesTo: null,   hp: 104, power: 76, rarity: 'secret' }
};

const BALL_TYPES = {
  pokeball:    { name: 'Pokéball',    bonus: 1.0, price: 50 },
  superball:   { name: 'Superball',   bonus: 1.6, price: 150 },
  meisterball: { name: 'Meisterball', bonus: null, price: 5000 } // fängt immer
};

const SECRET_CODES = {
  'MEWCODE2026':   { species: 'mew',     level: 15 },
  'ZAPCODE2026':   { species: 'zapdos',  level: 15 },
  'ICECODE2026':   { species: 'arktos',  level: 15 },
  'FIRECODE2026':  { species: 'lavados', level: 15 },
  'PIKACODE2026':  { species: 'pikachu', level: 30 },
  'EVOLICODE2026': { species: 'evoli',   level: 30 }
};

const ENCOUNTER_COOLDOWN_MS = 3 * 60 * 1000; // 3 Minuten
const WILD_EXPIRES_MS = 5 * 60 * 1000;       // 5 Minuten Zeit zum Fangen
const SECRET_ENCOUNTER_CHANCE = 0.015;       // 1.5% Chance auf ein geheimes Pokémon bei {P}wild

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
      pokeballs: { pokeball: 5, superball: 0, meisterball: 0 },
      secretCodes: []
    };
  }
  const p = users[jid].poke;
  if (!p.team) p.team = [];
  if (!p.dex) p.dex = {};
  if (!p.pokeballs) p.pokeballs = { pokeball: 5, superball: 0, meisterball: 0 };
  if (!p.secretCodes) p.secretCodes = [];
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

function pickSecretSpecies() {
  const pool = Object.entries(POKEMON_DB).filter(([, d]) => d.rarity === 'secret').map(([id]) => id);
  return pool[Math.floor(Math.random() * pool.length)];
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
      getNumberMention, randInt, sleep, isPrimaryOwner, resolveLidJid
    } = ctx;
    // Fallback, falls resolveLidJid (noch) nicht von index.js durchgereicht wird
    // (siehe Hinweis am Ende) — Verhalten bleibt dann wie vorher.
    const resolveTargetJid = resolveLidJid || (async (jid) => normalizeJid(jid));

    if (!POKEMON_COMMANDS.includes(cmd)) return false;

    ensureUser(sender);
    ensurePoke(users, sender);
    const p = users[sender].poke;
    const persist = () => save(FILES.users, users);

    if (cmd === 'pokehelp') {
      await send('🐾 *Pokémon-Hilfe*\n' + POKEMON_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, activePrefix)).join('\n'));
      return true;
    }

    if (cmd === 'pokesecret') {
      const code = (args[0] || '').toUpperCase();
      if (!code) {
        await send(`❌ Nutzung: ${activePrefix}pokesecret <code>`);
        return true;
      }
      const codeEntry = SECRET_CODES[code];
      if (!codeEntry) {
        await send('❌ Ungültiger Geheimcode.');
        return true;
      }
      if (p.secretCodes.includes(code)) {
        await send('❌ Dieser Code wurde bereits eingelöst.');
        return true;
      }
      const speciesId = codeEntry.species;
      const grantLevel = codeEntry.level || 15;
      p.secretCodes.push(code);
      const uid = 'PK' + Date.now().toString(36) + randInt(100, 999);
      p.team.push({ uid, species: speciesId, level: grantLevel, xp: 0, nickname: null });
      p.dex[speciesId] = true;
      if (!p.active) p.active = uid;
      persist();
      await send(`🌟✨ Geheimcode akzeptiert! *${POKEMON_DB[speciesId].name}* schließt sich deinem Team an!\n${formatPoke(speciesId, grantLevel, null)}`);
      return true;
    }

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

    if (cmd === 'wild') {
      const now = Date.now();
      if (now - (p.lastEncounter || 0) < ENCOUNTER_COOLDOWN_MS) {
        const remaining = Math.ceil((ENCOUNTER_COOLDOWN_MS - (now - p.lastEncounter)) / 1000);
        await send(`⏰ Warte noch ${remaining}s bis zum nächsten wilden Pokémon.`);
        return true;
      }

      const isSecret = Math.random() < SECRET_ENCOUNTER_CHANCE;
      const avgLevel = p.team.length ? Math.round(p.team.reduce((s, x) => s + x.level, 0) / p.team.length) : 5;
      const speciesId = isSecret ? pickSecretSpecies() : pickWildSpecies();
      const level = isSecret
        ? Math.max(20, avgLevel + randInt(5, 15))
        : Math.max(1, randInt(Math.max(1, avgLevel - 3), avgLevel + 5));

      p.wild = { species: speciesId, level, expiresAt: now + WILD_EXPIRES_MS, secret: isSecret };
      p.lastEncounter = now;
      persist();
      const d = POKEMON_DB[speciesId];

      if (isSecret) {
        await send(
          `✨🌌 *Du spürst eine geheimnisvolle Aura...*\nEin geheimes Pokémon erscheint!\n${formatPoke(speciesId, level, null)}\nTyp: ${TYPE_EMOJI[d.type] || ''} ${d.type}\n\n` +
          `Es ist extrem schwer zu fangen! Fange es mit ${activePrefix}catch [pokeball|superball|meisterball]\n(Du hast 5 Minuten Zeit, sonst flieht es.)`
        );
      } else {
        await send(
          `🌿 Ein wildes Pokémon erscheint!\n${formatPoke(speciesId, level, null)}\nTyp: ${TYPE_EMOJI[d.type] || ''} ${d.type}\n\n` +
          `Fange es mit ${activePrefix}catch [pokeball|superball|meisterball]\n(Du hast 5 Minuten Zeit, sonst flieht es.)`
        );
      }
      return true;
    }

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
      const wasSecret = !!p.wild.secret;
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
        const prefix = wasSecret ? '🌟✨ UNGLAUBLICH! Ein geheimes Pokémon wurde gefangen!' : '✅ Gefangen!';
        await send(`${prefix} ${formatPoke(species, level, null)} wurde deinem Team hinzugefügt! (${BALL_TYPES[ballKey].name}, Chance war ${Math.round(chance)}%)`);
      } else {
        persist();
        await send(`💨 Das Pokémon ist ausgebrochen! (Chance war ${Math.round(chance)}%) Versuch es nochmal mit ${activePrefix}catch.`);
      }
      return true;
    }

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
        `Spezies: ${d.name}\nTyp: ${TYPE_EMOJI[d.type] || ''} ${d.type}\nSeltenheit: ${RARITY_INFO[d.rarity]?.emoji || ''} ${RARITY_INFO[d.rarity]?.label || ''}\n` +
        `Level: ${pk.level}\nXP: ${pk.xp} / ${needed}\n` +
        `Stärke: ${effectivePower(pk.species, pk.level)}\nHP: ${d.hp + pk.level * 3}\n` +
        `Entwicklung: ${evolveInfo}`
      );
      return true;
    }

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

    if (cmd === 'pokebattle') {
      // ── @-Erwähnung / Reply hat jetzt IMMER Vorrang vor getipptem Text ──
      // (bei LID-Mentions ist der angezeigte "@Zahl"-Text oft keine echte
      // Telefonnummer mehr, daher darf getippter Text die Mention nie überstimmen)
      const ctxInfo = m.message?.extendedTextMessage?.contextInfo;
      let target = args[0];
      if (ctxInfo?.mentionedJid?.length) target = ctxInfo.mentionedJid[0];
      else if (ctxInfo?.participant) target = ctxInfo.participant;

      if (!target) {
        await send(`❌ Nutzung: ${activePrefix}pokebattle @user`);
        return true;
      }
      const targetJid = await resolveTargetJid(target, sock);
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
