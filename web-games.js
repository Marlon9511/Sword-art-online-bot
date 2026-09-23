import express from 'express';

const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

const SLOT_SYMBOLS = ['🍒', '🍋', '🍇', '🍉', '⭐', '💎'];
function spinSlots() {
  return [SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)], SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)], SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)]];
}

const BJ_SUITS = ['♠', '♥', '♦', '♣'];
const BJ_VALUES = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
function bjDraw() { return { value: BJ_VALUES[randInt(0, BJ_VALUES.length - 1)], suit: BJ_SUITS[randInt(0, BJ_SUITS.length - 1)] }; }
function bjVal(card) { if (['J', 'Q', 'K'].includes(card.value)) return 10; if (card.value === 'A') return 11; return parseInt(card.value); }
function bjScore(hand) {
  let s = 0, ac = 0;
  for (const c of hand) { if (c.value === 'A') { ac++; s += 11; } else s += bjVal(c); }
  while (s > 21 && ac > 0) { s -= 10; ac--; }
  return s;
}

const FISH_EVENTS = [
  { chance: 20, text: '🐟 Flusswels gefangen! (+8 Coins)', coins: 8 },
  { chance: 16, text: '🐠 Blauschuppen-Barsch gefangen! (+15 Coins)', coins: 15, xp: 3 },
  { chance: 12, text: '🐡 Kugelfisch gefangen! (+25 Coins)', coins: 25, xp: 5 },
  { chance: 10, text: '💎 Kristallforelle gefangen! (+30 Coins)', coins: 30, xp: 8 },
  { chance: 7, text: '🌫️ Nebelaal gefangen! (+45 Coins)', coins: 45, xp: 10 },
  { chance: 8, text: '🦀 Kobold-Krebs verheddert. (+5 Coins)', coins: 5 },
  { chance: 5, text: '🐍 Riesenaal gefangen! (+40 Coins)', coins: 40, xp: 8 },
  { chance: 4, text: '🦈 Sturmhai gefangen! (+90 Coins)', coins: 90, xp: 20 },
  { chance: 3, text: '🎐 Leuchtqualle gefunden! (+70 Coins)', coins: 70, xp: 15 },
  { chance: 1.5, text: '🐉 Drachenkarpfen! (+180 Coins)', coins: 180, xp: 35 },
  { chance: 0.4, text: '🐋 Der Weiße Wal von Aincrad! JACKPOT! (+600 Coins)', coins: 600, xp: 120 },
  { chance: 3, text: '📦 Schatztruhe entdeckt! (+200 Coins)', coins: 200, xp: 10 },
  { chance: 2, text: '📜 Flaschenpost gefunden. (+50 Coins)', coins: 50, xp: 20 },
  { chance: 3, text: '💊 Heiltrank-Flasche eingesammelt.', item: 'potion', itemQty: 1 },
  { chance: 3, text: '🎁 Mysteriöse Kiste gefunden.', item: 'box', itemQty: 1 },
  { chance: 10, text: '🌿 Nur Algen gefangen...' },
  { chance: 8, text: '💨 Ein Fisch hat deinen Köder gestohlen!' },
  { chance: 4, text: '💦 Ausgerutscht und ins Wasser gefallen!' },
  { chance: 3, text: '🦶 Ein Kobold hat dein Boot umgestoßen! (-30 Coins)', coins: -30 },
  { chance: 2, text: '🪝 Angel an einem Stein zerbrochen! (-10 Coins)', coins: -10 }
];

const SHOP = {
  potion: { price: 100, desc: 'Heilt 10 HP / +10 XP' },
  box: { price: 500, desc: 'Zufälliger Gegenstand' },
  vip: { price: 2000, desc: 'VIP-Rang (7 Tage)' }
};

export function createGameRoutes({ users, save, FILES }) {
  const router = express.Router();

  function getEntry(req) {
    return Object.entries(users).find(([, u]) => u.webId === req.webId) || null;
  }

  function fail(res, msg, code = 400) { return res.status(code).json({ success: false, error: msg }); }
  function summary(u) {
    return { coins: u.coins || 0, xp: u.xp || 0, level: u.level || 1, items: u.items || {} };
  }

  router.use((req, res, next) => {
    const entry = getEntry(req);
    if (!entry) return fail(res, 'Nutzer nicht gefunden.', 404);
    req.userJid = entry[0];
    req.user = entry[1];
    next();
  });

  router.post('/daily', (req, res) => {
    const u = req.user;
    const now = Date.now();
    const last = u.lastDaily || 0;
    if (now - last < 24 * 3600 * 1000) {
      const hours = Math.floor((24 * 3600 * 1000 - (now - last)) / 3600000);
      return fail(res, `Wieder in ca. ${hours} Stunden verfügbar.`);
    }
    const amount = randInt(1, 1000);
    u.coins = (u.coins || 0) + amount;
    u.lastDaily = now;
    save(FILES.users, users);
    res.json({ success: true, amount, ...summary(u) });
  });

  router.post('/work', (req, res) => {
    const u = req.user;
    const earn = randInt(50, 200);
    u.coins = (u.coins || 0) + earn;
    u.xp = (u.xp || 0) + 20;
    save(FILES.users, users);
    res.json({ success: true, earn, ...summary(u) });
  });

  router.post('/slot', (req, res) => {
    const u = req.user;
    const bet = parseInt(req.body?.bet) || 50;
    if (bet <= 0) return fail(res, 'Ungültiger Einsatz.');
    if ((u.coins || 0) < bet) return fail(res, 'Zu wenig Coins.');
    const spin = spinSlots();
    const win = spin[0] === spin[1] && spin[1] === spin[2];
    if (win) { u.coins += bet * 3; u.xp = (u.xp || 0) + 50; }
    else { u.coins -= bet; }
    save(FILES.users, users);
    res.json({ success: true, spin, win, ...summary(u) });
  });

  router.post('/rps', (req, res) => {
    const u = req.user;
    const choice = (req.body?.choice || '').toLowerCase();
    if (!['rock', 'paper', 'scissors'].includes(choice)) return fail(res, 'Ungültige Wahl.');
    const botChoice = ['rock', 'paper', 'scissors'][randInt(0, 2)];
    const draw = choice === botChoice;
    const win = (choice === 'rock' && botChoice === 'scissors') || (choice === 'paper' && botChoice === 'rock') || (choice === 'scissors' && botChoice === 'paper');
    if (draw) { /* nothing */ }
    else if (win) { u.coins = (u.coins || 0) + 50; u.xp = (u.xp || 0) + 10; }
    else { u.coins = Math.max(0, (u.coins || 0) - 20); }
    save(FILES.users, users);
    res.json({ success: true, choice, botChoice, result: draw ? 'draw' : win ? 'win' : 'lose', ...summary(u) });
  });

  router.post('/blackjack/start', (req, res) => {
    const u = req.user;
    if (u.bj?.active) return fail(res, 'Es läuft bereits ein Spiel.');
    const player = [bjDraw(), bjDraw()];
    const dealer = [bjDraw(), bjDraw()];
    u.bj = { player, dealer, active: true };
    save(FILES.users, users);
    res.json({ success: true, player, dealerUp: dealer[0], playerScore: bjScore(player) });
  });

  router.post('/blackjack/hit', (req, res) => {
    const u = req.user;
    if (!u.bj?.active) return fail(res, 'Kein aktives Spiel.');
    u.bj.player.push(bjDraw());
    const p = bjScore(u.bj.player);
    let status = 'ongoing';
    if (p > 21) { status = 'bust'; delete u.bj; }
    else if (p === 21) { u.coins = (u.coins || 0) + 75; u.xp = (u.xp || 0) + 40; status = 'blackjack'; delete u.bj; }
    save(FILES.users, users);
    res.json({ success: true, player: status === 'ongoing' ? u.bj.player : null, playerScore: p, status, ...summary(u) });
  });

  router.post('/blackjack/stand', (req, res) => {
    const u = req.user;
    if (!u.bj?.active) return fail(res, 'Kein aktives Spiel.');
    const bj = u.bj;
    const p = bjScore(bj.player);
    let d = bjScore(bj.dealer);
    while (d < 17) { bj.dealer.push(bjDraw()); d = bjScore(bj.dealer); }
    let status;
    if (p > 21) status = 'bust';
    else if (d > 21 || p > d) { u.coins = (u.coins || 0) + 75; u.xp = (u.xp || 0) + 40; status = 'win'; }
    else if (p === d) status = 'draw';
    else status = 'lose';
    delete u.bj;
    save(FILES.users, users);
    res.json({ success: true, dealer: bj.dealer, dealerScore: d, playerScore: p, status, ...summary(u) });
  });

  router.post('/fish', (req, res) => {
    const u = req.user;
    const totalWeight = FISH_EVENTS.reduce((sum, e) => sum + e.chance, 0);
    let random = Math.random() * totalWeight;
    let event = FISH_EVENTS[FISH_EVENTS.length - 1];
    for (const e of FISH_EVENTS) { random -= e.chance; if (random <= 0) { event = e; break; } }
    if (event.coins) u.coins = Math.max(0, (u.coins || 0) + event.coins);
    if (event.xp) u.xp = (u.xp || 0) + event.xp;
    if (event.item) { if (!u.items) u.items = {}; u.items[event.item] = (u.items[event.item] || 0) + (event.itemQty || 1); }
    save(FILES.users, users);
    res.json({ success: true, text: event.text, ...summary(u) });
  });

  router.get('/shop', (req, res) => {
    res.json({ success: true, shop: SHOP });
  });

  router.post('/buy', (req, res) => {
    const u = req.user;
    const item = req.body?.item;
    if (!item || !SHOP[item]) return fail(res, 'Unbekannter Artikel.');
    if ((u.coins || 0) < SHOP[item].price) return fail(res, 'Zu wenig Coins.');
    u.coins -= SHOP[item].price;
    if (!u.items) u.items = {};
    u.items[item] = (u.items[item] || 0) + 1;
    save(FILES.users, users);
    res.json({ success: true, bought: item, ...summary(u) });
  });

  router.get('/inventory', (req, res) => {
    res.json({ success: true, items: req.user.items || {} });
  });

  router.post('/use', (req, res) => {
    const u = req.user;
    const item = (req.body?.item || '').toLowerCase();
    if (!u.items?.[item]) return fail(res, 'Item nicht vorhanden.');

    let message = 'Item verwendet.';
    if (item === 'potion') { u.items[item]--; u.xp = (u.xp || 0) + 10; message = '+10 XP'; }
    else if (item === 'box') { u.items[item]--; const c = randInt(50, 300); u.coins = (u.coins || 0) + c; message = `+${c} Coins`; }
    else if (item === 'vip') { u.items[item]--; message = 'VIP wird serverseitig über den Bot aktiviert.'; }
    else { u.items[item]--; }

    save(FILES.users, users);
    res.json({ success: true, message, ...summary(u) });
  });

  return router;
}
