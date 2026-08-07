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
  digda: