import { POKEMON_DB } from './pokemon-system.mjs'; // Pfad ggf. an euer Projekt anpassen

export const TITLE_COMMANDS = [
  'title', 'titel', 'achievements', 'erfolge', 'hpbar', 'addbeta'
];

export const TITLE_HELP_TEXT =
  `▸ {P}title — Deine freigeschalteten Titel anzeigen\n` +
  `▸ {P}title list — Alle Titel im Spiel anzeigen\n` +
  `▸ {P}title set <name> — Aktiven Titel setzen\n` +
  `▸ {P}title info <name> — Freischaltbedingung eines Titels anzeigen\n` +
  `▸ {P}achievements — Deine Erfolge & Fortschritt anzeigen\n` +
  `▸ {P}hpbar — HP-Balken-Anzeige an/aus umschalten\n` +
  `▸ {P}addbeta @user — (Owner) Beta-Tester-Titel vergeben\n`;

const dexCount = (u) => Object.keys(u.poke?.dex || {}).length;
const teamCount = (u) => (u.poke?.team || []).length;
const maxTeamLevel = (u) => (u.poke?.team || []).reduce((m, pk) => Math.max(m, pk.level || 0), 0);
const hasRarity = (u, rarity) =>
  Object.keys(u.poke?.dex || {}).some((id) => POKEMON_DB[id]?.rarity === rarity);
const hasAnyOfType = (u, type) =>
  Object.keys(u.poke?.dex || {}).some((id) => POKEMON_DB[id]?.type === type);

export const TITLES = [
  {
    id: 'black_cat_rookie',
    name: 'Frischling von Aincrad',
    icon: '🐣',
    desc: 'Erreiche Level 5.',
    check: (u) => (u.level || 1) >= 5
  },
  {
    id: 'beater',
    name: 'Beater',
    icon: '⚔️',
    desc: 'Erreiche als einer der Ersten Level 15.',
    check: (u) => (u.level || 1) >= 15
  },
  {
    id: 'lightning_flash',
    name: 'Lichtschwert',
    icon: '💫',
    desc: 'Erreiche Level 30.',
    check: (u) => (u.level || 1) >= 30
  },
  {
    id: 'flash_of_battle',
    name: 'Kampfblitz',
    icon: '🌩️',
    desc: 'Erreiche Level 40.',
    check: (u) => (u.level || 1) >= 40
  },
  {
    id: 'sword_saint',
    name: 'Schwertheiliger',
    icon: '⛩️',
    desc: 'Erreiche Level 50.',
    check: (u) => (u.level || 1) >= 50
  },
  {
    id: 'incarnation',
    name: 'Inkarnation der Klinge',
    icon: '🔥',
    desc: 'Erreiche Level 65.',
    check: (u) => (u.level || 1) >= 65
  },
  {
    id: 'absolute_sword',
    name: 'Absolutes Schwert',
    icon: '🌌',
    desc: 'Erreiche Level 80.',
    check: (u) => (u.level || 1) >= 80
  },
  {
    id: 'god_of_aincrad',
    name: 'Gott von Aincrad',
    icon: '☄️',
    desc: 'Erreiche Level 99.',
    check: (u) => (u.level || 1) >= 99
  },
  {
    id: 'first_blood',
    name: 'Erstschlag',
    icon: '🩸',
    desc: 'Gewinne dein erstes Duell.',
    check: (u) => (u.duel?.wins || 0) >= 1
  },
  {
    id: 'duelist',
    name: 'Duellant',
    icon: '🤺',
    desc: 'Gewinne 10 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 10
  },
  {
    id: 'veteran_fighter',
    name: 'Kampferprobter Veteran',
    icon: '🛡️',
    desc: 'Gewinne 25 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 25
  },
  {
    id: 'black_swordsman',
    name: 'The Black Swordsman',
    icon: '🗡️',
    desc: 'Gewinne 50 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 50
  },
  {
    id: 'blade_master',
    name: 'Klingenmeister',
    icon: '⚔️',
    desc: 'Gewinne 75 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 75
  },
  {
    id: 'aincrad_legend',
    name: 'Legende von Aincrad',
    icon: '🐉',
    desc: 'Gewinne 100 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 100
  },
  {
    id: 'immortal_object',
    name: 'Unsterblicher Gegenstand',
    icon: '💠',
    desc: 'Gewinne 150 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 150
  },
  {
    id: 'blood_king',
    name: 'Blutkönig der Arena',
    icon: '👹',
    desc: 'Gewinne 200 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 200
  },
  {
    id: 'undefeated',
    name: 'Unbesiegt',
    icon: '⚡',
    desc: 'Gewinne 20 Duelle, ohne mehr als 5 zu verlieren.',
    check: (u) => (u.duel?.wins || 0) >= 20 && (u.duel?.losses || 0) <= 5
  },
  {
    id: 'flawless',
    name: 'Makellos',
    icon: '💎',
    desc: 'Gewinne 10 Duelle, ohne ein einziges zu verlieren.',
    check: (u) => (u.duel?.wins || 0) >= 10 && (u.duel?.losses || 0) === 0
  },
  {
    id: 'punching_bag',
    name: 'Der Unverwüstliche',
    icon: '🥊',
    desc: 'Verliere 25 Duelle und kämpfe trotzdem weiter.',
    check: (u) => (u.duel?.losses || 0) >= 25
  },
  {
    id: 'never_give_up',
    name: 'Niemals aufgeben',
    icon: '🔁',
    desc: 'Verliere 10 Duelle, aber gewinne trotzdem mindestens 1.',
    check: (u) => (u.duel?.losses || 0) >= 10 && (u.duel?.wins || 0) >= 1
  },
  {
    id: 'arena_veteran',
    name: 'Arena-Veteran',
    icon: '🏟️',
    desc: 'Bestreite insgesamt 50 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 50
  },
  {
    id: 'arena_addict',
    name: 'Arena-Süchtiger',
    icon: '🎯',
    desc: 'Bestreite insgesamt 150 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 150
  },
  {
    id: 'arena_god',
    name: 'Gott der Arena',
    icon: '🏆',
    desc: 'Bestreite insgesamt 300 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 300
  },
  {
    id: 'flawless_victory',
    name: 'Fehlerloser Sieg',
    icon: '🌟',
    desc: 'Gewinne ein Duell, ohne selbst Schaden zu nehmen.',
    check: (u) => u.__wonWithoutDamage === true
  },
  {
    id: 'gold_hoarder',
    name: 'Goldgräber',
    icon: '💰',
    desc: 'Sammle insgesamt 10.000 Coins.',
    check: (u) => (u.coins || 0) >= 10000
  },
  {
    id: 'coin_baron',
    name: 'Coin-Baron',
    icon: '🪙',
    desc: 'Sammle insgesamt 50.000 Coins.',
    check: (u) => (u.coins || 0) >= 50000
  },
  {
    id: 'coin_emperor',
    name: 'Coin-Kaiser',
    icon: '👑',
    desc: 'Sammle insgesamt 100.000 Coins.',
    check: (u) => (u.coins || 0) >= 100000
  },
  {
    id: 'high_roller',
    name: 'Hochstapler',
    icon: '🎲',
    desc: 'Verdiene netto 5.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 5000
  },
  {
    id: 'bounty_hunter',
    name: 'Kopfgeldjäger',
    icon: '🎯',
    desc: 'Verdiene netto 15.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 15000
  },
  {
    id: 'gambling_king',
    name: 'König des Glücksspiels',
    icon: '🃏',
    desc: 'Verdiene netto 30.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 30000
  },
  {
    id: 'front_liner',
    name: 'Frontkämpfer',
    icon: '🛡️',
    desc: 'Erreiche Etage/Floor 20.',
    check: (u) => (u.floor || 1) >= 20
  },
  {
    id: 'floor_50_climber',
    name: 'Etagenbezwinger',
    icon: '🏯',
    desc: 'Erreiche Etage/Floor 50.',
    check: (u) => (u.floor || 1) >= 50
  },
  {
    id: 'floor_75_climber',
    name: 'Höhenwanderer von Aincrad',
    icon: '🗻',
    desc: 'Erreiche Etage/Floor 75.',
    check: (u) => (u.floor || 1) >= 75
  },
  {
    id: 'clearer',
    name: 'Clearer',
    icon: '🏁',
    desc: 'Erreiche die letzte Etage — Floor 100.',
    check: (u) => (u.floor || 1) >= 100
  },
  {
    id: 'game_cleared',
    name: 'Spielbezwinger',
    icon: '🎖️',
    desc: 'Cleare das gesamte Spiel.',
    check: (u) => u.__clearedGame === true
  },
  {
    id: 'boss_slayer',
    name: 'Bosstöter',
    icon: '🐲',
    desc: 'Besiege einen Etagenboss im Alleingang.',
    check: (u) => u.__soloClearedBoss === true
  },
  {
    id: 'guild_member',
    name: 'Gildenmitglied',
    icon: '🤝',
    desc: 'Tritt einer Gilde bei.',
    check: (u) => !!u.guildId
  },
  {
    id: 'guildmaster',
    name: 'Gildenmeister',
    icon: '🏰',
    desc: 'Gründe eine Gilde.',
    check: (u) => !!u.guildId && (u.__isGuildLeader === true)
  },
  {
    id: 'lone_wolf',
    name: 'Einzelgänger',
    icon: '🐺',
    desc: 'Erreiche Level 25, ohne je einer Gilde beizutreten.',
    check: (u) => (u.level || 1) >= 25 && !u.guildId
  },
  {
    id: 'beta_tester',
    name: 'Beta-Tester',
    icon: '🧪',
    desc: 'Nimm an der Beta-Phase teil.',
    check: (u) => u.__isBetaTester === true
  },
  {
    id: 'event_champion',
    name: 'Event-Champion',
    icon: '🎉',
    desc: 'Nimm an einem Event teil.',
    check: (u) => u.__eventParticipant === true
  },
  {
    id: 'survivor_title',
    name: 'Überlebenskünstler',
    icon: '❤️',
    desc: 'Überstehe ein Duell mit weniger als 10% HP.',
    check: (u) => u.__survivedLowHp === true
  },
  {
    id: 'kayaba',
    name: 'Kayaba Akihiko',
    icon: '👑',
    desc: 'Der Schöpfer selbst. Exklusiv für den Bot-Owner.',
    check: (u) => u.__isOwner === true
  },
  {
    id: 'level_1_newbie',
    name: 'Neuling in Aincrad',
    icon: '🌱',
    desc: 'Erreiche Level 1.',
    check: (u) => (u.level || 1) >= 1
  },
  {
    id: 'level_3',
    name: 'Erste Schritte',
    icon: '👣',
    desc: 'Erreiche Level 3.',
    check: (u) => (u.level || 1) >= 3
  },
  {
    id: 'level_8',
    name: 'Aufstrebender Kämpfer',
    icon: '🔰',
    desc: 'Erreiche Level 8.',
    check: (u) => (u.level || 1) >= 8
  },
  {
    id: 'level_12',
    name: 'Geübter Schwertkämpfer',
    icon: '🗡️',
    desc: 'Erreiche Level 12.',
    check: (u) => (u.level || 1) >= 12
  },
  {
    id: 'level_18',
    name: 'Etablierter Kämpfer',
    icon: '🛡️',
    desc: 'Erreiche Level 18.',
    check: (u) => (u.level || 1) >= 18
  },
  {
    id: 'level_20',
    name: 'Zweite-Tier-Krieger',
    icon: '⚔️',
    desc: 'Erreiche Level 20.',
    check: (u) => (u.level || 1) >= 20
  },
  {
    id: 'level_22',
    name: 'Erfahrener Aincrad-Kämpfer',
    icon: '🎖️',
    desc: 'Erreiche Level 22.',
    check: (u) => (u.level || 1) >= 22
  },
  {
    id: 'level_35',
    name: 'Gestählte Klinge',
    icon: '🔪',
    desc: 'Erreiche Level 35.',
    check: (u) => (u.level || 1) >= 35
  },
  {
    id: 'level_45',
    name: 'Meisterhafter Duellant',
    icon: '🥷',
    desc: 'Erreiche Level 45.',
    check: (u) => (u.level || 1) >= 45
  },
  {
    id: 'level_55',
    name: 'Elite-Schwertkämpfer',
    icon: '🌠',
    desc: 'Erreiche Level 55.',
    check: (u) => (u.level || 1) >= 55
  },
  {
    id: 'level_60',
    name: 'Höhere-Etagen-Krieger',
    icon: '🏔️',
    desc: 'Erreiche Level 60.',
    check: (u) => (u.level || 1) >= 60
  },
  {
    id: 'level_70',
    name: 'Klingenvirtuose',
    icon: '🎻',
    desc: 'Erreiche Level 70.',
    check: (u) => (u.level || 1) >= 70
  },
  {
    id: 'level_75',
    name: 'Bezwinger der Front',
    icon: '🚩',
    desc: 'Erreiche Level 75.',
    check: (u) => (u.level || 1) >= 75
  },
  {
    id: 'level_85',
    name: 'Nahe der Spitze',
    icon: '🏔️',
    desc: 'Erreiche Level 85.',
    check: (u) => (u.level || 1) >= 85
  },
  {
    id: 'level_90',
    name: 'Champion von Aincrad',
    icon: '🏅',
    desc: 'Erreiche Level 90.',
    check: (u) => (u.level || 1) >= 90
  },
  {
    id: 'level_95',
    name: 'Fast Vollkommen',
    icon: '✴️',
    desc: 'Erreiche Level 95.',
    check: (u) => (u.level || 1) >= 95
  },
  {
    id: 'level_100',
    name: 'Höchste Vollendung',
    icon: '💯',
    desc: 'Erreiche Level 100.',
    check: (u) => (u.level || 1) >= 100
  },
  {
    id: 'duel_wins_5',
    name: 'Frischer Kämpfer',
    icon: '🥊',
    desc: 'Gewinne 5 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 5
  },
  {
    id: 'duel_wins_15',
    name: 'Aufsteigender Kämpfer',
    icon: '⚔️',
    desc: 'Gewinne 15 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 15
  },
  {
    id: 'duel_wins_35',
    name: 'Gefürchteter Gegner',
    icon: '🗡️',
    desc: 'Gewinne 35 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 35
  },
  {
    id: 'duel_wins_60',
    name: 'Arena-Terror',
    icon: '😈',
    desc: 'Gewinne 60 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 60
  },
  {
    id: 'duel_wins_90',
    name: 'Klingensturm',
    icon: '🌪️',
    desc: 'Gewinne 90 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 90
  },
  {
    id: 'duel_wins_120',
    name: 'Herrscher der Arena',
    icon: '🏹',
    desc: 'Gewinne 120 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 120
  },
  {
    id: 'duel_wins_175',
    name: 'Furchtloser Champion',
    icon: '🦁',
    desc: 'Gewinne 175 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 175
  },
  {
    id: 'duel_wins_250',
    name: 'Ewiger Sieger',
    icon: '🏆',
    desc: 'Gewinne 250 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 250
  },
  {
    id: 'duel_wins_300',
    name: 'Klingen-Ikone',
    icon: '🌟',
    desc: 'Gewinne 300 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 300
  },
  {
    id: 'duel_wins_400',
    name: 'Titan der Arena',
    icon: '🗿',
    desc: 'Gewinne 400 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 400
  },
  {
    id: 'duel_wins_500',
    name: 'Mythos von Aincrad',
    icon: '🐲',
    desc: 'Gewinne 500 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 500
  },
  {
    id: 'duel_losses_1',
    name: 'Erste Niederlage',
    icon: '💢',
    desc: 'Verliere dein erstes Duell.',
    check: (u) => (u.duel?.losses || 0) >= 1
  },
  {
    id: 'duel_losses_5',
    name: 'Aus Fehlern lernen',
    icon: '📘',
    desc: 'Verliere 5 Duelle.',
    check: (u) => (u.duel?.losses || 0) >= 5
  },
  {
    id: 'duel_losses_15',
    name: 'Zäher Kämpfer',
    icon: '🩹',
    desc: 'Verliere 15 Duelle.',
    check: (u) => (u.duel?.losses || 0) >= 15
  },
  {
    id: 'duel_losses_50',
    name: 'Narben der Arena',
    icon: '🪖',
    desc: 'Verliere 50 Duelle.',
    check: (u) => (u.duel?.losses || 0) >= 50
  },
  {
    id: 'duel_losses_75',
    name: 'Unermüdlicher Kämpfer',
    icon: '🦾',
    desc: 'Verliere 75 Duelle.',
    check: (u) => (u.duel?.losses || 0) >= 75
  },
  {
    id: 'duel_losses_100',
    name: 'Der Sturköpfige',
    icon: '🐂',
    desc: 'Verliere 100 Duelle.',
    check: (u) => (u.duel?.losses || 0) >= 100
  },
  {
    id: 'duel_fights_10',
    name: 'Erste Kämpfe',
    icon: '🤺',
    desc: 'Bestreite insgesamt 10 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 10
  },
  {
    id: 'duel_fights_25',
    name: 'Regelmäßiger Kämpfer',
    icon: '🥋',
    desc: 'Bestreite insgesamt 25 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 25
  },
  {
    id: 'duel_fights_75',
    name: 'Arena-Stammgast',
    icon: '🏟️',
    desc: 'Bestreite insgesamt 75 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 75
  },
  {
    id: 'duel_fights_100',
    name: 'Hundertkämpfer',
    icon: '💯',
    desc: 'Bestreite insgesamt 100 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 100
  },
  {
    id: 'duel_fights_200',
    name: 'Kriegsveteran',
    icon: '🎖️',
    desc: 'Bestreite insgesamt 200 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 200
  },
  {
    id: 'duel_fights_250',
    name: 'Gezeichnet vom Kampf',
    icon: '🔥',
    desc: 'Bestreite insgesamt 250 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 250
  },
  {
    id: 'duel_fights_400',
    name: 'Ewiger Herausforderer',
    icon: '♾️',
    desc: 'Bestreite insgesamt 400 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 400
  },
  {
    id: 'duel_fights_500',
    name: 'Legende der Arena',
    icon: '👑',
    desc: 'Bestreite insgesamt 500 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 500
  },
  {
    id: 'earnings_1000',
    name: 'Erste Beute',
    icon: '🪙',
    desc: 'Verdiene netto 1.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 1000
  },
  {
    id: 'earnings_2500',
    name: 'Kleiner Gewinner',
    icon: '💵',
    desc: 'Verdiene netto 2.500 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 2500
  },
  {
    id: 'earnings_10000',
    name: 'Profitabler Kämpfer',
    icon: '💹',
    desc: 'Verdiene netto 10.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 10000
  },
  {
    id: 'earnings_20000',
    name: 'Arena-Investor',
    icon: '📈',
    desc: 'Verdiene netto 20.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 20000
  },
  {
    id: 'earnings_50000',
    name: 'Wohlhabender Duellant',
    icon: '💸',
    desc: 'Verdiene netto 50.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 50000
  },
  {
    id: 'earnings_75000',
    name: 'Arena-Magnat',
    icon: '🏦',
    desc: 'Verdiene netto 75.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 75000
  },
  {
    id: 'earnings_100000',
    name: 'Herrscher der Wetten',
    icon: '👑',
    desc: 'Verdiene netto 100.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 100000
  },
  {
    id: 'coins_500',
    name: 'Erstes Kapital',
    icon: '🪙',
    desc: 'Besitze 500 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 500
  },
  {
    id: 'coins_1000',
    name: 'Kleiner Sparfuchs',
    icon: '💰',
    desc: 'Besitze 1.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 1000
  },
  {
    id: 'coins_5000',
    name: 'Solide Ersparnisse',
    icon: '💵',
    desc: 'Besitze 5.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 5000
  },
  {
    id: 'coins_25000',
    name: 'Wachsender Reichtum',
    icon: '📦',
    desc: 'Besitze 25.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 25000
  },
  {
    id: 'coins_75000',
    name: 'Angesehener Kaufmann',
    icon: '🏪',
    desc: 'Besitze 75.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 75000
  },
  {
    id: 'coins_200000',
    name: 'Handelsfürst',
    icon: '🏛️',
    desc: 'Besitze 200.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 200000
  },
  {
    id: 'coins_500000',
    name: 'Aincrad-Millionär',
    icon: '💎',
    desc: 'Besitze 500.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 500000
  },
  {
    id: 'coins_1000000',
    name: 'Herr des Reichtums',
    icon: '👑',
    desc: 'Besitze 1.000.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 1000000
  },
  {
    id: 'floor_1',
    name: 'Erste Etage betreten',
    icon: '🚪',
    desc: 'Erreiche Etage/Floor 1.',
    check: (u) => (u.floor || 1) >= 1
  },
  {
    id: 'floor_5',
    name: 'Aufstieg begonnen',
    icon: '🪜',
    desc: 'Erreiche Etage/Floor 5.',
    check: (u) => (u.floor || 1) >= 5
  },
  {
    id: 'floor_10',
    name: 'Zehnte Etage erreicht',
    icon: '🏢',
    desc: 'Erreiche Etage/Floor 10.',
    check: (u) => (u.floor || 1) >= 10
  },
  {
    id: 'floor_30',
    name: 'Über den Wolken',
    icon: '☁️',
    desc: 'Erreiche Etage/Floor 30.',
    check: (u) => (u.floor || 1) >= 30
  },
  {
    id: 'floor_40',
    name: 'Auf halbem Weg',
    icon: '🧗',
    desc: 'Erreiche Etage/Floor 40.',
    check: (u) => (u.floor || 1) >= 40
  },
  {
    id: 'floor_60',
    name: 'Jenseits der Mitte',
    icon: '🌤️',
    desc: 'Erreiche Etage/Floor 60.',
    check: (u) => (u.floor || 1) >= 60
  },
  {
    id: 'floor_90',
    name: 'Am Vorabend des Gipfels',
    icon: '🌄',
    desc: 'Erreiche Etage/Floor 90.',
    check: (u) => (u.floor || 1) >= 90
  },
  {
    id: 'floor_95',
    name: 'Fast am Ziel',
    icon: '🏔️',
    desc: 'Erreiche Etage/Floor 95.',
    check: (u) => (u.floor || 1) >= 95
  },
  {
    id: 'loyal_guild_member',
    name: 'Treues Gildenmitglied',
    icon: '🎗️',
    desc: 'Sei Mitglied einer Gilde und erreiche Level 20.',
    check: (u) => !!u.guildId && (u.level || 1) >= 20
  },
  {
    id: 'front_line_leader',
    name: 'Anführer der Front',
    icon: '🏯',
    desc: 'Sei Gildenleiter und erreiche Level 50.',
    check: (u) => u.__isGuildLeader === true && (u.level || 1) >= 50
  },
  {
    id: 'guild_warrior',
    name: 'Gildenkrieger',
    icon: '🛡️',
    desc: 'Sei Gildenmitglied und gewinne 50 Duelle.',
    check: (u) => !!u.guildId && (u.duel?.wins || 0) >= 50
  },
  {
    id: 'true_champion',
    name: 'Wahrer Champion',
    icon: '🏆',
    desc: 'Erreiche Level 50 und Etage/Floor 50.',
    check: (u) => (u.level || 1) >= 50 && (u.floor || 1) >= 50
  },
  {
    id: 'rich_warrior',
    name: 'Reicher Krieger',
    icon: '💰',
    desc: 'Gewinne 100 Duelle und besitze 50.000 Coins.',
    check: (u) => (u.duel?.wins || 0) >= 100 && (u.coins || 0) >= 50000
  },
  {
    id: 'complete_clearer',
    name: 'Vollendeter Clearer',
    icon: '🏁',
    desc: 'Erreiche Etage/Floor 100 und gewinne 50 Duelle.',
    check: (u) => (u.floor || 1) >= 100 && (u.duel?.wins || 0) >= 50
  },
  {
    id: 'battle_hardened_master',
    name: 'Kampferprobter Meister',
    icon: '🥋',
    desc: 'Erreiche Level 80 und bestreite 200 Duelle.',
    check: (u) => (u.level || 1) >= 80 && (u.duel?.fights || 0) >= 200
  },
  {
    id: 'wealthy_master',
    name: 'Wohlhabender Meister',
    icon: '👑',
    desc: 'Besitze 100.000 Coins und erreiche Level 50.',
    check: (u) => (u.coins || 0) >= 100000 && (u.level || 1) >= 50
  },
  {
    id: 'perfect_champion',
    name: 'Perfekter Champion',
    icon: '💎',
    desc: 'Gewinne 200 Duelle, ohne jemals zu verlieren.',
    check: (u) => (u.duel?.wins || 0) >= 200 && (u.duel?.losses || 0) === 0
  },
  {
    id: 'front_guild',
    name: 'Frontgilde',
    icon: '🚩',
    desc: 'Erreiche Etage/Floor 75 als Gildenmitglied.',
    check: (u) => (u.floor || 1) >= 75 && !!u.guildId
  },
  {
    id: 'perfect_hero',
    name: 'Vollkommener Held',
    icon: '🌌',
    desc: 'Erreiche Level 99, cleare Floor 100 und das gesamte Spiel.',
    check: (u) => (u.level || 1) >= 99 && (u.floor || 1) >= 100 && u.__clearedGame === true
  },
  {
    id: 'master_strategist',
    name: 'Meister-Stratege',
    icon: '🧠',
    desc: 'Verdiene 30.000 Coins durch Duelle und gewinne 100 davon.',
    check: (u) => (u.duel?.earnings || 0) >= 30000 && (u.duel?.wins || 0) >= 100
  },
  {
    id: 'legendary_wealth',
    name: 'Legendärer Reichtum',
    icon: '💎',
    desc: 'Besitze 500.000 Coins und gewinne 200 Duelle.',
    check: (u) => (u.coins || 0) >= 500000 && (u.duel?.wins || 0) >= 200
  },
  {
    id: 'win_streak_3',
    name: 'Serienschläger',
    icon: '🔥',
    desc: 'Gewinne 3 Duelle in Folge.',
    check: (u) => (u.duel?.winStreak || 0) >= 3
  },
  {
    id: 'win_streak_5',
    name: 'Siegesserie',
    icon: '🔥',
    desc: 'Gewinne 5 Duelle in Folge.',
    check: (u) => (u.duel?.winStreak || 0) >= 5
  },
  {
    id: 'win_streak_10',
    name: 'Unaufhaltsam',
    icon: '💥',
    desc: 'Gewinne 10 Duelle in Folge.',
    check: (u) => (u.duel?.winStreak || 0) >= 10
  },
  {
    id: 'win_streak_20',
    name: 'Dominanz pur',
    icon: '☠️',
    desc: 'Gewinne 20 Duelle in Folge.',
    check: (u) => (u.duel?.winStreak || 0) >= 20
  },
  {
    id: 'perfectionist',
    name: 'Perfektionist',
    icon: '✨',
    desc: 'Cleare eine Etage, ohne Schaden zu nehmen.',
    check: (u) => u.__perfectFloorClear === true
  },
  {
    id: 'speedrunner',
    name: 'Geschwindigkeitsrekord',
    icon: '⏱️',
    desc: 'Cleare das gesamte Spiel in Rekordzeit.',
    check: (u) => u.__speedCleared === true
  },
  {
    id: 'rival_slayer',
    name: 'Rivalenbezwinger',
    icon: '⚔️',
    desc: 'Besiege deinen festgelegten Rivalen.',
    check: (u) => u.__defeatedRival === true
  },
  {
    id: 'pioneer',
    name: 'Pionier',
    icon: '🧭',
    desc: 'Sei der Erste, der eine Etage cleart.',
    check: (u) => u.__firstToClearFloor === true
  },
  {
    id: 'skill_master',
    name: 'Meister aller Klingen',
    icon: '🌀',
    desc: 'Maximiere alle deine Skills.',
    check: (u) => u.__allSkillsMaxed === true
  },
  {
    id: 'collector',
    name: 'Sammler',
    icon: '🎒',
    desc: 'Sammle alle seltenen Items im Spiel.',
    check: (u) => u.__itemCollectorComplete === true
  },
  {
    id: 'mentor',
    name: 'Mentor',
    icon: '🧑‍🏫',
    desc: 'Hilf einem neuen Spieler beim Einstieg.',
    check: (u) => u.__helpedNewbie === true
  },
  {
    id: 'aincrad_marriage',
    name: 'Aincrad-Ehe',
    icon: '💍',
    desc: 'Heirate einen anderen Spieler im Spiel.',
    check: (u) => u.__isMarried === true
  },
  {
    id: 'poke_trainer',
    name: 'Taschenmonster-Trainer',
    icon: '🐾',
    desc: 'Wähle dein erstes Starter-Pokémon.',
    check: (u) => !!u.poke?.starter
  },
  {
    id: 'poke_fire_starter',
    name: 'Kind der Flamme',
    icon: '🔥',
    desc: 'Wähle Glumanda als Starter.',
    check: (u) => u.poke?.starter === 'glumanda'
  },
  {
    id: 'poke_water_starter',
    name: 'Kind der Wellen',
    icon: '💧',
    desc: 'Wähle Schiggy als Starter.',
    check: (u) => u.poke?.starter === 'schiggy'
  },
  {
    id: 'poke_grass_starter',
    name: 'Kind der Wurzeln',
    icon: '🌿',
    desc: 'Wähle Bisasam als Starter.',
    check: (u) => u.poke?.starter === 'bisasam'
  },
  {
    id: 'poke_first_catch',
    name: 'Erster Fang',
    icon: '⚾',
    desc: 'Fange dein erstes wildes Pokémon.',
    check: (u) => teamCount(u) >= 2
  },
  {
    id: 'poke_team_3',
    name: 'Kleines Team',
    icon: '👥',
    desc: 'Habe 3 Pokémon im Team.',
    check: (u) => teamCount(u) >= 3
  },
  {
    id: 'poke_team_6',
    name: 'Volle Riege',
    icon: '🎽',
    desc: 'Habe 6 Pokémon im Team.',
    check: (u) => teamCount(u) >= 6
  },
  {
    id: 'poke_team_10',
    name: 'Pokémon-Sammler',
    icon: '🎒',
    desc: 'Habe 10 Pokémon im Team.',
    check: (u) => teamCount(u) >= 10
  },
  {
    id: 'poke_team_15',
    name: 'Wandelnde Pokéarena',
    icon: '🏕️',
    desc: 'Habe 15 Pokémon im Team.',
    check: (u) => teamCount(u) >= 15
  },
  {
    id: 'poke_dex_5',
    name: 'Neugieriger Forscher',
    icon: '🔍',
    desc: 'Entdecke 5 verschiedene Pokémon-Arten.',
    check: (u) => dexCount(u) >= 5
  },
  {
    id: 'poke_dex_10',
    name: 'Feld-Forscher',
    icon: '🧭',
    desc: 'Entdecke 10 verschiedene Pokémon-Arten.',
    check: (u) => dexCount(u) >= 10
  },
  {
    id: 'poke_dex_20',
    name: 'Pokédex-Enthusiast',
    icon: '📗',
    desc: 'Entdecke 20 verschiedene Pokémon-Arten.',
    check: (u) => dexCount(u) >= 20
  },
  {
    id: 'poke_dex_30',
    name: 'Renommierter Professor',
    icon: '🎓',
    desc: 'Entdecke 30 verschiedene Pokémon-Arten.',
    check: (u) => dexCount(u) >= 30
  },
  {
    id: 'poke_dex_complete',
    name: 'Pokédex-Vollender',
    icon: '📘',
    desc: 'Entdecke alle Pokémon-Arten im Spiel.',
    check: (u) => dexCount(u) >= Object.keys(POKEMON_DB).length
  },
  {
    id: 'poke_uncommon_catch',
    name: 'Glückspilz',
    icon: '🟢',
    desc: 'Fange ein ungewöhnliches Pokémon.',
    check: (u) => hasRarity(u, 'uncommon')
  },
  {
    id: 'poke_rare_catch',
    name: 'Schatzsucher',
    icon: '🔵',
    desc: 'Fange ein seltenes Pokémon.',
    check: (u) => hasRarity(u, 'rare')
  },
  {
    id: 'poke_legendary_catch',
    name: 'Legendenjäger',
    icon: '🟡',
    desc: 'Fange ein legendäres Pokémon.',
    check: (u) => hasRarity(u, 'legendary')
  },
  {
    id: 'poke_secret_catch',
    name: 'Hüter des Geheimnisses',
    icon: '🟣',
    desc: 'Fange oder erhalte ein geheimes Pokémon.',
    check: (u) => hasRarity(u, 'secret')
  },
  {
    id: 'poke_all_secrets',
    name: 'Meister der Legenden',
    icon: '🌌',
    desc: 'Besitze alle 4 geheimen Pokémon (Mew, Zapdos, Arktos, Lavados).',
    check: (u) =>
      ['mew', 'zapdos', 'arktos', 'lavados'].every((id) => !!u.poke?.dex?.[id])
  },
  {
    id: 'poke_secret_code',
    name: 'Codeknacker',
    icon: '🔑',
    desc: 'Löse einen Geheimcode ein.',
    check: (u) => (u.poke?.secretCodes || []).length >= 1
  },
  {
    id: 'poke_all_codes',
    name: 'Vollständiger Sammler',
    icon: '🗝️',
    desc: 'Löse alle 4 bekannten Geheimcodes ein.',
    check: (u) => (u.poke?.secretCodes || []).length >= 4
  },
  {
    id: 'poke_level_20',
    name: 'Vielversprechendes Talent',
    icon: '⭐',
    desc: 'Habe ein Pokémon mit Level 20.',
    check: (u) => maxTeamLevel(u) >= 20
  },
  {
    id: 'poke_level_40',
    name: 'Erfahrener Kämpfer',
    icon: '🌟',
    desc: 'Habe ein Pokémon mit Level 40.',
    check: (u) => maxTeamLevel(u) >= 40
  },
  {
    id: 'poke_level_60',
    name: 'Elite-Trainer',
    icon: '✨',
    desc: 'Habe ein Pokémon mit Level 60.',
    check: (u) => maxTeamLevel(u) >= 60
  },
  {
    id: 'poke_level_100',
    name: 'Champion-Trainer',
    icon: '🏆',
    desc: 'Habe ein Pokémon mit Level 100.',
    check: (u) => maxTeamLevel(u) >= 100
  },
  {
    id: 'poke_fully_evolved_starter',
    name: 'Vollendeter Weggefährte',
    icon: '🌠',
    desc: 'Entwickle deinen Starter zur finalen Form.',
    check: (u) => {
      const finals = ['bisaflor', 'glurak', 'turtok'];
      return (u.poke?.team || []).some((pk) => finals.includes(pk.species));
    }
  },
  {
    id: 'poke_first_evolution',
    name: 'Wandlung',
    icon: '🌀',
    desc: 'Entwickle zum ersten Mal ein Pokémon.',
    check: (u) => (u.poke?.evolutions || 0) >= 1
  },
  {
    id: 'poke_evolution_master',
    name: 'Wandlungsmeister',
    icon: '🔮',
    desc: 'Entwickle insgesamt 10 Pokémon.',
    check: (u) => (u.poke?.evolutions || 0) >= 10
  },
  {
    id: 'poke_master_ball',
    name: 'Meisterhafter Fänger',
    icon: '⚫',
    desc: 'Besitze einen Meisterball.',
    check: (u) => (u.poke?.pokeballs?.meisterball || 0) >= 1
  },
  {
    id: 'poke_ball_hoarder',
    name: 'Ballsammler',
    icon: '🧺',
    desc: 'Besitze insgesamt 20 Pokébälle (alle Typen zusammen).',
    check: (u) => {
      const b = u.poke?.pokeballs || {};
      return (b.pokeball || 0) + (b.superball || 0) + (b.meisterball || 0) >= 20;
    }
  },
  {
    id: 'poke_big_spender',
    name: 'Großzügiger Käufer',
    icon: '🛍️',
    desc: 'Kaufe insgesamt 50 Pokébälle.',
    check: (u) => (u.poke?.ballsBought || 0) >= 50
  },
  {
    id: 'poke_dedicated_trainer',
    name: 'Fleißiger Trainer',
    icon: '🏋️',
    desc: 'Trainiere 10 Mal erfolgreich.',
    check: (u) => (u.poke?.trainings || 0) >= 10
  },
  {
    id: 'poke_iron_will_trainer',
    name: 'Eiserner Wille',
    icon: '🦾',
    desc: 'Trainiere 50 Mal erfolgreich.',
    check: (u) => (u.poke?.trainings || 0) >= 50
  },
  {
    id: 'poke_first_battle_win',
    name: 'Erster Ringsieg',
    icon: '🥇',
    desc: 'Gewinne dein erstes Pokémon-PVP.',
    check: (u) => (u.poke?.battleWins || 0) >= 1
  },
  {
    id: 'poke_battle_veteran',
    name: 'Arena-erprobter Trainer',
    icon: '⚔️',
    desc: 'Gewinne 20 Pokémon-PVPs.',
    check: (u) => (u.poke?.battleWins || 0) >= 20
  },
  {
    id: 'poke_battle_champion',
    name: 'Ligameister',
    icon: '👑',
    desc: 'Gewinne 50 Pokémon-PVPs.',
    check: (u) => (u.poke?.battleWins || 0) >= 50
  },
  {
    id: 'poke_battle_active',
    name: 'Kampflustig',
    icon: '🤺',
    desc: 'Bestreite 30 Pokémon-PVPs, egal ob Sieg oder Niederlage.',
    check: (u) => (u.poke?.battleFights || 0) >= 30
  },
  {
    id: 'poke_dragon_tamer',
    name: 'Drachenbändiger',
    icon: '🐉',
    desc: 'Entdecke ein Pokémon vom Typ Drache.',
    check: (u) => hasAnyOfType(u, 'drache')
  },
  {
    id: 'poke_psychic_seeker',
    name: 'Geistessucher',
    icon: '🔮',
    desc: 'Entdecke ein Pokémon vom Typ Psycho.',
    check: (u) => hasAnyOfType(u, 'psycho')
  },
  {
    id: 'poke_electric_soul',
    name: 'Blitzgeist',
    icon: '⚡',
    desc: 'Entdecke ein Pokémon vom Typ Elektro.',
    check: (u) => hasAnyOfType(u, 'elektro')
  },
  {
    id: 'poke_true_champion',
    name: 'Wahrer Pokémon-Champion',
    icon: '🏆',
    desc: 'Habe ein Team von 6 Pokémon und mindestens ein legendäres darunter.',
    check: (u) => teamCount(u) >= 6 && hasRarity(u, 'legendary')
  },
  {
    id: 'poke_ultimate_master',
    name: 'Ultimativer Pokémon-Meister',
    icon: '🌌',
    desc: 'Vervollständige den Pokédex und besitze alle 4 geheimen Pokémon.',
    check: (u) =>
      dexCount(u) >= Object.keys(POKEMON_DB).length &&
      ['mew', 'zapdos', 'arktos', 'lavados'].every((id) => !!u.poke?.dex?.[id])
  }
];

export const ACHIEVEMENTS = [
  {
    id: 'first_duel_win',
    name: 'Erster Sieg',
    icon: '🥊',
    desc: 'Gewinne dein erstes Duell.',
    check: (u) => (u.duel?.wins || 0) >= 1
  },
  {
    id: 'level_10',
    name: 'Aufsteiger',
    icon: '⭐',
    desc: 'Erreiche Level 10.',
    check: (u) => (u.level || 1) >= 10
  },
  {
    id: 'level_25',
    name: 'Veteran',
    icon: '🌟',
    desc: 'Erreiche Level 25.',
    check: (u) => (u.level || 1) >= 25
  },
  {
    id: 'level_50',
    name: 'Meister von Aincrad',
    icon: '✨',
    desc: 'Erreiche Level 50.',
    check: (u) => (u.level || 1) >= 50
  },
  {
    id: 'guild_joined',
    name: 'Teamplayer',
    icon: '🤝',
    desc: 'Tritt einer Gilde bei.',
    check: (u) => !!u.guildId
  },
  {
    id: 'duels_10',
    name: 'Kampferprobt',
    icon: '⚔️',
    desc: 'Gewinne 10 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 10
  },
  {
    id: 'duels_50',
    name: 'Klingenmeister',
    icon: '🗡️',
    desc: 'Gewinne 50 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 50
  },
  {
    id: 'duels_100',
    name: 'Klingengott',
    icon: '🐉',
    desc: 'Gewinne 100 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 100
  },
  {
    id: 'rich_1000',
    name: 'Wohlhabend',
    icon: '💰',
    desc: 'Besitze 1.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 1000
  },
  {
    id: 'rich_10000',
    name: 'Goldgräber',
    icon: '💎',
    desc: 'Besitze 10.000 Coins gleichzeitig.',
    check: (u) => (u.coins || 0) >= 10000
  },
  {
    id: 'fights_25',
    name: 'Erfahrener Kämpfer',
    icon: '🥋',
    desc: 'Bestreite insgesamt 25 Duelle.',
    check: (u) => (u.duel?.fights || 0) >= 25
  },
  {
    id: 'survivor',
    name: 'Überlebenskünstler',
    icon: '❤️',
    desc: 'Überstehe ein Duell mit weniger als 10% HP.',
    check: (u) => u.__survivedLowHp === true
  },
  {
    id: 'floor_20',
    name: 'Aufsteiger der Etagen',
    icon: '🏯',
    desc: 'Erreiche Etage/Floor 20.',
    check: (u) => (u.floor || 1) >= 20
  },
  {
    id: 'floor_100',
    name: 'Spiel gecleart',
    icon: '🏁',
    desc: 'Erreiche die letzte Etage — Floor 100.',
    check: (u) => (u.floor || 1) >= 100
  },
  {
    id: 'earnings_5000',
    name: 'Cleverer Kämpfer',
    icon: '🎲',
    desc: 'Verdiene netto 5.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 5000
  },
  {
    id: 'flawless_10',
    name: 'Makellose Serie',
    icon: '💎',
    desc: 'Gewinne 10 Duelle, ohne ein einziges zu verlieren.',
    check: (u) => (u.duel?.wins || 0) >= 10 && (u.duel?.losses || 0) === 0
  },
  {
    id: 'poke_starter_chosen',
    name: 'Neuer Weggefährte',
    icon: '🐾',
    desc: 'Wähle dein erstes Starter-Pokémon.',
    check: (u) => !!u.poke?.starter
  },
  {
    id: 'poke_dex_10',
    name: 'Angehender Forscher',
    icon: '🔍',
    desc: 'Entdecke 10 verschiedene Pokémon-Arten.',
    check: (u) => dexCount(u) >= 10
  },
  {
    id: 'poke_legendary_found',
    name: 'Legendenjäger',
    icon: '🟡',
    desc: 'Fange ein legendäres Pokémon.',
    check: (u) => hasRarity(u, 'legendary')
  },
  {
    id: 'poke_secret_found',
    name: 'Geheimnisvolle Begegnung',
    icon: '🟣',
    desc: 'Fange oder erhalte ein geheimes Pokémon.',
    check: (u) => hasRarity(u, 'secret')
  }
];

export function renderHpBar(current, max, length = 10) {
  const safeMax = Math.max(1, max || 1);
  const safeCurrent = Math.max(0, Math.min(current ?? safeMax, safeMax));
  const ratio = safeCurrent / safeMax;
  const filled = Math.round(ratio * length);
  const empty = length - filled;

  let color = '🟩';
  if (ratio <= 0.25) color = '🟥';
  else if (ratio <= 0.5) color = '🟨';

  const bar = color.repeat(filled) + '⬛'.repeat(empty);
  const pct = Math.round(ratio * 100);
  return `${bar} ${safeCurrent}/${safeMax} HP (${pct}%)`;
}

// Extrahiert nur die reine Ziffernfolge aus einer JID, egal ob @lid,
// @s.whatsapp.net oder mit :device-Suffix.
function extractRawNumberTitle(jid) {
  if (!jid) return null;
  return String(jid).split(':')[0].split('@')[0].replace(/[^0-9]/g, '') || null;
}

// Sucht in den bekannten Usern nach dem Key, dessen reine Ziffernfolge übereinstimmt.
// Dadurch ist es egal, ob der User als @lid oder @s.whatsapp.net gespeichert wurde.
function findUserJidByRawNumber(users, rawNumber) {
  if (!rawNumber) return null;
  for (const key of Object.keys(users)) {
    if (extractRawNumberTitle(key) === rawNumber) return key;
  }
  return null;
}

// Versucht, gemenionte JIDs (inkl. @lid) aus möglichst vielen gängigen
// Bot-Framework-Strukturen zu ziehen. Deckt Baileys-typische Pfade ab.
function extractMentionedJids(ctx) {
  const candidates = [
    ctx.mentionedJid,
    ctx.mentions,
    ctx.mentionedJids,
    ctx.message?.mentionedJid,
    ctx.message?.extendedTextMessage?.contextInfo?.mentionedJid,
    ctx.contextInfo?.mentionedJid,
    ctx.msg?.mentionedJid,
    ctx.msg?.message?.extendedTextMessage?.contextInfo?.mentionedJid,
    ctx.m?.mentionedJid,
    ctx.m?.message?.extendedTextMessage?.contextInfo?.mentionedJid,
    ctx.quoted?.mentionedJid,
    ctx.raw?.message?.extendedTextMessage?.contextInfo?.mentionedJid
  ];

  for (const c of candidates) {
    if (Array.isArray(c) && c.length) return c;
    if (typeof c === 'string' && c) return [c];
  }

  // Fallback: Nummer direkt aus dem Rohtext ziehen, falls Mentions
  // als Klartext (z.B. "@491701234567") ankommen statt strukturiert.
  const rawText =
    ctx.body || ctx.text || ctx.message?.conversation ||
    ctx.message?.extendedTextMessage?.text || '';
  const match = String(rawText).match(/@(\d{5,15})/);
  if (match) return [match[1]];

  return [];
}

export async function checkProgress(ctx, jid) {
  const { users, save, FILES, send } = ctx;
  const u = users[jid];
  if (!u) return;

  if (!Array.isArray(u.unlockedTitles)) u.unlockedTitles = [];
  if (!u.unlockedAchievements || typeof u.unlockedAchievements !== 'object') u.unlockedAchievements = {};

  if (ctx.guilds && u.guildId && ctx.guilds[u.guildId]) {
    u.__isGuildLeader = ctx.guilds[u.guildId].leader === jid;
  }

  if (Array.isArray(ctx.ownerJids) && ctx.ownerJids.length) {
    const rawNum = extractRawNumberTitle(jid);
    u.__isOwner = !!rawNum && ctx.ownerJids.some(o => extractRawNumberTitle(o) === rawNum);
  } else if (typeof ctx.isOwner === 'function') {
    u.__isOwner = ctx.isOwner(jid);
  } else if (Array.isArray(ctx.owners)) {
    const num = extractRawNumberTitle(jid);
    u.__isOwner = ctx.owners.some(o => extractRawNumberTitle(o) === num);
  } else if (Array.isArray(ctx.OWNER_NUMBERS)) {
    const num = extractRawNumberTitle(jid);
    u.__isOwner = ctx.OWNER_NUMBERS.some(o => extractRawNumberTitle(o) === num);
  } else {
    u.__isOwner = u.__isOwner === true;
  }

  const newTitles = [];
  for (const t of TITLES) {
    if (!u.unlockedTitles.includes(t.id) && t.check(u)) {
      u.unlockedTitles.push(t.id);
      newTitles.push(t);
    }
  }

  const newAchievements = [];
  for (const a of ACHIEVEMENTS) {
    if (!u.unlockedAchievements[a.id] && a.check(u)) {
      u.unlockedAchievements[a.id] = Date.now();
      newAchievements.push(a);
    }
  }

  if (newTitles.length || newAchievements.length) {
    save(FILES.users, users);

    if (typeof send === 'function') {
      for (const a of newAchievements) {
        await send(`🏆 *Erfolg freigeschaltet!*\n${a.icon} *${a.name}* — ${a.desc}`);
      }
      for (const t of newTitles) {
        await send(`🎖️ *Neuer Titel freigeschaltet!*\n${t.icon} *"${t.name}"*\nSetze ihn mit ${ctx.activePrefix}title set ${t.name}`);
      }
    }
  }

  return { newTitles, newAchievements };
}

function findTitleByName(query) {
  const q = query.trim().toLowerCase();
  return TITLES.find(t => t.name.toLowerCase() === q || t.id === q);
}

export function createTitleSystem() {
  async function handle(ctx) {
    const { cmd, args, sender, send, users, save, FILES, ensureUser, activePrefix } = ctx;

    if (!TITLE_COMMANDS.includes(cmd)) return false;

    ensureUser(sender);
    const u = users[sender];
    if (!Array.isArray(u.unlockedTitles)) u.unlockedTitles = [];
    if (!u.unlockedAchievements || typeof u.unlockedAchievements !== 'object') u.unlockedAchievements = {};

    await checkProgress(ctx, sender);

    if (cmd === 'addbeta') {
      // Nur der Owner darf Beta-Tester-Titel vergeben
      if (u.__isOwner !== true) {
        await send('❌ Nur der Bot-Owner kann diesen Befehl nutzen.');
        return true;
      }

      const mentioned = extractMentionedJids(ctx);
      let targetRaw = null;

      if (mentioned.length) {
        targetRaw = extractRawNumberTitle(mentioned[0]);
      } else if (args[0]) {
        // Fallback: Nummer direkt als Argument, z.B. addbeta 4915123456789
        targetRaw = extractRawNumberTitle(args[0]);
      }

      if (!targetRaw) {
        await send(`❌ Nutzung: ${activePrefix}addbeta @user`);
        return true;
      }

      // Versuche zuerst einen bereits bekannten User zu finden (funktioniert auch bei @lid)
      let targetJid = findUserJidByRawNumber(users, targetRaw);

      
      if (!targetJid && mentioned.length && typeof mentioned[0] === 'string' && mentioned[0].includes('@')) {
        targetJid = mentioned[0];
        ensureUser(targetJid);
      }

      if (!targetJid) {
        await send('❌ Dieser User wurde noch nicht im System erfasst (noch keine Interaktion mit dem Bot).');
        return true;
      }

      ensureUser(targetJid);
      const targetUser = users[targetJid];
      targetUser.__isBetaTester = true;
      save(FILES.users, users);

      const result = await checkProgress(ctx, targetJid);
      const already = !result?.newTitles?.some(t => t.id === 'beta_tester');

      await send(
        `✅ Beta-Tester-Status wurde vergeben.\n🧪 Titel "Beta-Tester" ${already ? 'war bereits freigeschaltet oder wird beim nächsten Check aktiv' : 'wurde freigeschaltet'}.`
      );
      return true;
    }

    if (cmd === 'hpbar') {
      const sub = (args[0] || '').toLowerCase();

      if (sub === 'on' || sub === 'an') {
        u.showHpBar = true;
        save(FILES.users, users);
        await send(`✅ HP-Balken aktiviert.\n${renderHpBar(u.hp ?? 100, u.maxHp ?? 100)}`);
        return true;
      }
      if (sub === 'off' || sub === 'aus') {
        u.showHpBar = false;
        save(FILES.users, users);
        await send('❌ HP-Balken deaktiviert.');
        return true;
      }
      if (sub === 'preview' || sub === 'vorschau') {
        const cur = parseInt(args[1], 10);
        const max = parseInt(args[2], 10);
        const example = renderHpBar(
          Number.isFinite(cur) ? cur : 65,
          Number.isFinite(max) ? max : 100
        );
        await send(`👁️ Vorschau:\n${example}`);
        return true;
      }

      const status = u.showHpBar === false ? 'AUS ❌' : 'AN ✅';
      await send(
        `💠 *HP-Balken-Anzeige:* ${status}\n` +
        `Beispiel: ${renderHpBar(u.hp ?? 100, u.maxHp ?? 100)}\n\n` +
        `▸ ${activePrefix}hpbar on/off — umschalten\n` +
        `▸ ${activePrefix}hpbar preview <hp> <maxhp> — Vorschau`
      );
      return true;
    }

    if (cmd === 'achievements' || cmd === 'erfolge') {
      const total = ACHIEVEMENTS.length;
      const unlockedCount = Object.keys(u.unlockedAchievements).length;

      const lines = ACHIEVEMENTS.map(a => {
        const unlockedAt = u.unlockedAchievements[a.id];
        if (unlockedAt) {
          const date = new Date(unlockedAt).toLocaleDateString('de-DE');
          return `✅ ${a.icon} *${a.name}* — ${a.desc} _(${date})_`;
        }
        return `🔒 ${a.icon} *${a.name}* — ${a.desc}`;
      });

      await send(
        `🏆 *— DEINE ERFOLGE —* 🏆\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n` +
        `Fortschritt: ${unlockedCount}/${total}\n\n${lines.join('\n')}`
      );
      return true;
    }

    const sub = (args[0] || '').toLowerCase();

    if (!sub) {
      if (!u.unlockedTitles.length) {
        await send(`🎖️ Du hast noch keine Titel freigeschaltet.\nNutze ${activePrefix}title list, um zu sehen, was es gibt.`);
        return true;
      }
      const lines = u.unlockedTitles.map(id => {
        const t = TITLES.find(x => x.id === id);
        if (!t) return null;
        const active = u.activeTitle === id ? ' (aktiv)' : '';
        return `${t.icon} *"${t.name}"*${active}`;
      }).filter(Boolean);

      await send(
        `🎖️ *— DEINE TITEL —* 🎖️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}\n\n` +
        `Setze einen aktiven Titel mit ${activePrefix}title set <name>`
      );
      return true;
    }

    if (sub === 'list' || sub === 'liste') {
      const lines = TITLES.map(t => {
        const unlocked = u.unlockedTitles.includes(t.id);
        return unlocked
          ? `✅ ${t.icon} *"${t.name}"* — ${t.desc}`
          : `🔒 ${t.icon} *"${t.name}"* — ${t.desc}`;
      });
      await send(`🎖️ *— ALLE TITEL —* 🎖️\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n${lines.join('\n')}`);
      return true;
    }

    if (sub === 'info') {
      const query = args.slice(1).join(' ').trim();
      if (!query) { await send(`❌ Nutzung: ${activePrefix}title info <name>`); return true; }
      const t = findTitleByName(query);
      if (!t) { await send('❌ Diesen Titel gibt es nicht.'); return true; }
      const unlocked = u.unlockedTitles.includes(t.id);
      await send(
        `${t.icon} *"${t.name}"*\n${t.desc}\n` +
        `Status: ${unlocked ? '✅ Freigeschaltet' : '🔒 Gesperrt'}`
      );
      return true;
    }

    if (sub === 'set' || sub === 'setzen') {
      const query = args.slice(1).join(' ').trim();
      if (!query) { await send(`❌ Nutzung: ${activePrefix}title set <name>`); return true; }
      const t = findTitleByName(query);
      if (!t) { await send('❌ Diesen Titel gibt es nicht.'); return true; }
      if (!u.unlockedTitles.includes(t.id)) { await send('❌ Diesen Titel hast du noch nicht freigeschaltet.'); return true; }

      u.activeTitle = t.id;
      save(FILES.users, users);
      await send(`✅ Aktiver Titel gesetzt: ${t.icon} *"${t.name}"*`);
      return true;
    }

    if (sub === 'clear' || sub === 'entfernen') {
      u.activeTitle = null;
      save(FILES.users, users);
      await send('✅ Aktiver Titel entfernt.');
      return true;
    }

    await send(
      `❌ Nutzung:\n${activePrefix}title\n${activePrefix}title list\n` +
      `${activePrefix}title set <name>\n${activePrefix}title info <name>\n${activePrefix}title clear`
    );
    return true;
  }

  return { handle };
}
