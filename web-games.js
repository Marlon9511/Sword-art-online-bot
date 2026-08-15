

import express from 'express';

const BJ_SUITS = ['♠', '♥', '♦', '♣'];
const BJ_VALUES = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
const SLOT_SYMBOLS = ['🍒', '🍋', '🍇', '🍉', '⭐', '💎'];

const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
function bjDraw() { return { value: BJ_VALUES[randInt(0, BJ_VALUES.length - 1)], suit: BJ_SUITS[randInt(0, BJ_SUITS.length - 1)] }; }
function bjVal(c) { if (['J', 'Q', 'K'].includes(c.value)) return 10; if (c.value === 'A') return 11; return parseInt(c.value); }
function bjScore(hand) {
  let s = 0, ac = 0;
  for (const c of hand) { if (c.value === 'A') { ac++; s += 11; } else s += bjVal(c); }
  while (s > 21 && ac > 0) { s -= 10; ac--; }
  return s;
}
function spinSlots() { return [0, 0, 0].map(() => SLOT_SYMBOLS[randInt(0, SLOT_SYMBOLS.length - 1)]); }

/**
 * @param {object} deps
 * @param {object} deps.users - das "users"-Objekt aus bot.js (per Referenz, wird direkt mutiert)
 * @param {function} deps.save - save(file, data) aus bot.js
 * @param {object} deps.FILES - FILES.users aus bot.js
 */
export function createGameRoutes({ users, save, FILES }) {
  const router = express.Router();

  const findUserByWebId = (webId) => Object.entries(users).find(([, u]) => u.webId === webId) || null;
  const stats = (u) => ({ coins: u.coins || 0, level: u.level || 1, xp: u.xp || 0, name: u.name || null });
  const persist = () => save(FILES.users, users);

  function requireUser(req, res) {
    const entry = findUserByWebId(req.webId);
    if (!entry) {
      res.status(404).json({ success: false, error: 'Nutzer nicht gefunden.' });
      return null;
    }
    return entry[1]; // das User-Objekt
  }

  router.get('/me', (req, res) => {
    const u = requireUser(req, res);
    if (!u) return;
    return res.json({ success: true, ...stats(u) });
  });

  // ---- SLOTS ----
  router.post('/slot', (req, res) => {
    const u = requireUser(req, res);
    if (!u) return;
    const bet = parseInt(req.body?.bet);
    if (!Number.isInteger(bet) || bet <= 0) return res.status(400).json({ success: false, error: 'Ungültiger Einsatz.' });
    if ((u.coins || 0) < bet) return res.status(400).json({ success: false, error: 'Zu wenig Coins.' });

    const spin = spinSlots();
    const win = spin[0] === spin[1] && spin[1] === spin[2];
    if (win) { u.coins += bet * 3; u.xp = (u.xp || 0) + 50; }
    else { u.coins -= bet; }
    persist();
    return res.json({ success: true, spin, win, ...stats(u) });
  });

  // ---- SCHERE STEIN PAPIER ----
  router.post('/rps', (req, res) => {
    const u = requireUser(req, res);
    if (!u) return;
    const choice = String(req.body?.choice || '').toLowerCase();
    const valid = ['rock', 'paper', 'scissors'];
    if (!valid.includes(choice)) return res.status(400).json({ success: false, error: 'Ungültige Wahl.' });

    const botChoice = valid[randInt(0, 2)];
    const draw = choice === botChoice;
    const playerWins = (choice === 'rock' && botChoice === 'scissors') ||
                        (choice === 'paper' && botChoice === 'rock') ||
                        (choice === 'scissors' && botChoice === 'paper');

    let result = 'draw';
    if (!draw) {
      if (playerWins) { u.coins = (u.coins || 0) + 50; u.xp = (u.xp || 0) + 10; result = 'win'; }
      else { u.coins = Math.max(0, (u.coins || 0) - 20); result = 'lose'; }
    }
    persist();
    return res.json({ success: true, botChoice, result, ...stats(u) });
  });

  // ---- BLACKJACK ----
  router.post('/blackjack/start', (req, res) => {
    const u = requireUser(req, res);
    if (!u) return;
    if (u.bj?.active) return res.status(400).json({ success: false, error: 'Es läuft bereits ein Spiel. Nutze hit/stand.' });

    const player = [bjDraw(), bjDraw()];
    const dealer = [bjDraw(), bjDraw()];
    u.bj = { player, dealer, active: true };
    persist();
    return res.json({ success: true, player, dealerUpcard: dealer[0], playerScore: bjScore(player) });
  });

  router.post('/blackjack/hit', (req, res) => {
    const u = requireUser(req, res);
    if (!u) return;
    if (!u.bj?.active) return res.status(400).json({ success: false, error: 'Kein aktives Spiel. Starte zuerst eine neue Runde.' });

    u.bj.player.push(bjDraw());
    const score = bjScore(u.bj.player);
    const currentHand = [...u.bj.player];
    let status = 'playing';

    if (score > 21) {
      status = 'bust';
      delete u.bj;
    } else if (score === 21) {
      u.coins = (u.coins || 0) + 75;
      u.xp = (u.xp || 0) + 40;
      status = 'blackjack';
      delete u.bj;
    }
    persist();
    return res.json({ success: true, player: currentHand, score, status, ...stats(u) });
  });

  router.post('/blackjack/stand', (req, res) => {
    const u = requireUser(req, res);
    if (!u) return;
    if (!u.bj?.active) return res.status(400).json({ success: false, error: 'Kein aktives Spiel. Starte zuerst eine neue Runde.' });

    const playerScore = bjScore(u.bj.player);
    let dealer = u.bj.dealer;
    let dealerScore = bjScore(dealer);
    while (dealerScore < 17) { dealer.push(bjDraw()); dealerScore = bjScore(dealer); }

    let result;
    if (playerScore > 21) result = 'lose';
    else if (dealerScore > 21 || playerScore > dealerScore) {
      u.coins = (u.coins || 0) + 75;
      u.xp = (u.xp || 0) + 40;
      result = 'win';
    } else if (playerScore === dealerScore) result = 'draw';
    else result = 'lose';

    delete u.bj;
    persist();
    return res.json({ success: true, dealer, dealerScore, playerScore, result, ...stats(u) });
  });

  return router;
}
