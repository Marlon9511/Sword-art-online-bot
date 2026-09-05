// yugioh.mjs
// Yu-Gi-Oh! System für deinen WhatsApp-Bot – ALLES in einer Datei.
// Enthält: Karten-API, Deck-Manager, Duell-Simulator.
//
// EINBINDEN in deine index.js:
//   import { createYuGiOh } from './yugioh.mjs';
//   const yugioh = createYuGiOh(DATA_PATH);
//
// Danach stehen dir zur Verfügung:
//   yugioh.cardApi.searchCard(name)
//   yugioh.cardApi.getRandomCard()
//   yugioh.cardApi.formatCardText(card)
//   yugioh.cardApi.getCardImageUrl(card)
//   yugioh.decks.createDeck(userId, name) / .setActiveDeck / .listDecks / .addCardToDeck / ...
//   yugioh.duels.createDuel(chatId, user1, user2) / .getDuel(chatId) / .endDuelSession(chatId)
//
// Fertige if(cmd === '...')-Blöcke zum Einfügen in deine bestehende Kommando-Kette
// findest du ganz unten in diesem File als Kommentar (Abschnitt "INTEGRATION").

import fs from 'fs';
import path from 'path';

// ============================================================================
// 1) STORAGE — einfacher JSON-Datei-Speicher (nutzt deinen DATA_PATH-Ordner)
// ============================================================================

function createJsonStore(dataPath, fileName) {
  const filePath = path.join(dataPath, fileName);

  const ensure = () => {
    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, JSON.stringify({}, null, 2), 'utf-8');
    }
  };
  ensure();

  const readAll = () => {
    try {
      return JSON.parse(fs.readFileSync(filePath, 'utf-8') || '{}');
    } catch (e) {
      console.error(`[yugioh-storage] Fehler beim Lesen von ${filePath}:`, e.message);
      return {};
    }
  };

  const writeAll = (data) => {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8');
  };

  return {
    get: (key) => readAll()[key],
    set: (key, value) => {
      const data = readAll();
      data[key] = value;
      writeAll(data);
    },
    delete: (key) => {
      const data = readAll();
      delete data[key];
      writeAll(data);
    },
    readAll,
  };
}

// ============================================================================
// 2) KARTEN-API — YGOPRODeck (kostenlos, kein API-Key nötig)
//    Docs: https://ygoprodeck.com/api-guide/
// ============================================================================

function createCardApi() {
  const BASE_URL = 'https://db.ygoprodeck.com/api/v7/cardinfo.php';
  const cache = new Map();
  const CACHE_TTL_MS = 1000 * 60 * 30; // 30 Minuten

  function getCached(key) {
    const entry = cache.get(key);
    if (entry && entry.expires > Date.now()) return entry.data;
    cache.delete(key);
    return null;
  }
  function setCached(key, data) {
    cache.set(key, { data, expires: Date.now() + CACHE_TTL_MS });
  }

  async function searchCard(name) {
    const key = `fname:${name.toLowerCase()}`;
    const cached = getCached(key);
    if (cached) return cached;

    const res = await fetch(`${BASE_URL}?fname=${encodeURIComponent(name)}`);
    if (!res.ok) {
      const result = { best: null, alternatives: [] };
      setCached(key, result);
      return result;
    }

    const json = await res.json();
    const list = json.data || [];
    const exact = list.find((c) => c.name.toLowerCase() === name.toLowerCase());
    const result = { best: exact || list[0] || null, alternatives: list.slice(0, 10) };
    setCached(key, result);
    return result;
  }

  async function getCardById(id) {
    const key = `id:${id}`;
    const cached = getCached(key);
    if (cached) return cached;

    const res = await fetch(`${BASE_URL}?id=${encodeURIComponent(id)}`);
    if (!res.ok) return null;

    const json = await res.json();
    const card = (json.data && json.data[0]) || null;
    setCached(key, card);
    return card;
  }

  async function getRandomCard() {
    const res = await fetch('https://db.ygoprodeck.com/api/v7/randomcard.php');
    if (!res.ok) return null;
    return res.json();
  }

  function formatCardText(card) {
    if (!card) return '❌ Karte nicht gefunden.';

    const lines = [`🎴 *${card.name}*`, `Typ: ${card.type}`];

    if (card.type.includes('Monster')) {
      lines.push(`Attribut: ${card.attribute || '-'} | Level/Rank: ${card.level ?? card.rank ?? '-'}`);
      lines.push(`ATK: ${card.atk ?? '-'} / DEF: ${card.def ?? '-'}`);
      if (card.race) lines.push(`Art: ${card.race}`);
    } else if (card.race) {
      lines.push(`Unterart: ${card.race}`);
    }

    lines.push(`\n${card.desc}`);

    if (card.card_prices && card.card_prices[0]) {
      const p = card.card_prices[0];
      lines.push(`\n💰 Cardmarket: €${p.cardmarket_price} | TCGplayer: $${p.tcgplayer_price}`);
    }

    return lines.join('\n');
  }

  function getCardImageUrl(card) {
    return card?.card_images?.[0]?.image_url || null;
  }

  return { searchCard, getCardById, getRandomCard, formatCardText, getCardImageUrl };
}

// ============================================================================
// 3) DECK-MANAGER — Deckbau & Verwaltung pro Nutzer (JID)
// ============================================================================

function createDeckManager(dataPath, cardApi) {
  const store = createJsonStore(dataPath, 'yugioh-decks.json');

  const LIMITS = {
    main: { min: 40, max: 60 },
    extra: { min: 0, max: 15 },
    side: { min: 0, max: 15 },
    maxCopies: 3,
  };

  const getUserData = (userId) => store.get(userId) || { decks: {}, activeDeck: null };
  const saveUserData = (userId, data) => store.set(userId, data);

  function createDeck(userId, deckName) {
    const data = getUserData(userId);
    if (data.decks[deckName]) return { ok: false, message: `Deck "${deckName}" existiert bereits.` };
    data.decks[deckName] = { name: deckName, main: [], extra: [], side: [] };
    if (!data.activeDeck) data.activeDeck = deckName;
    saveUserData(userId, data);
    return { ok: true, message: `✅ Deck "${deckName}" erstellt.` };
  }

  function deleteDeck(userId, deckName) {
    const data = getUserData(userId);
    if (!data.decks[deckName]) return { ok: false, message: `Deck "${deckName}" existiert nicht.` };
    delete data.decks[deckName];
    if (data.activeDeck === deckName) {
      const remaining = Object.keys(data.decks);
      data.activeDeck = remaining[0] || null;
    }
    saveUserData(userId, data);
    return { ok: true, message: `🗑️ Deck "${deckName}" gelöscht.` };
  }

  function setActiveDeck(userId, deckName) {
    const data = getUserData(userId);
    if (!data.decks[deckName]) return { ok: false, message: `Deck "${deckName}" existiert nicht.` };
    data.activeDeck = deckName;
    saveUserData(userId, data);
    return { ok: true, message: `👉 Aktives Deck ist jetzt "${deckName}".` };
  }

  function listDecks(userId) {
    const data = getUserData(userId);
    return { decks: Object.keys(data.decks), activeDeck: data.activeDeck };
  }

  async function addCardToDeck(userId, deckName, cardNameOrId, qty = 1, targetSection = null) {
    const data = getUserData(userId);
    const deck = data.decks[deckName];
    if (!deck) return { ok: false, message: `Deck "${deckName}" existiert nicht.` };

    const result = /^\d+$/.test(String(cardNameOrId))
      ? { best: await cardApi.getCardById(cardNameOrId) }
      : await cardApi.searchCard(cardNameOrId);

    const card = result.best;
    if (!card) return { ok: false, message: `❌ Karte "${cardNameOrId}" nicht gefunden.` };

    let section = targetSection;
    if (!section) {
      const extraTypes = ['Fusion', 'Synchro', 'XYZ', 'Link'];
      section = extraTypes.some((t) => card.type.includes(t)) ? 'extra' : 'main';
    }

    const list = deck[section];
    const existing = list.find((c) => c.id === card.id);
    const currentQty = existing ? existing.qty : 0;

    if (currentQty + qty > LIMITS.maxCopies) {
      return { ok: false, message: `❌ Maximal ${LIMITS.maxCopies} Kopien von "${card.name}" erlaubt.` };
    }

    const sectionLimit = LIMITS[section].max;
    const sectionCount = list.reduce((sum, c) => sum + c.qty, 0);
    if (sectionCount + qty > sectionLimit) {
      return { ok: false, message: `❌ ${section}-Deck würde das Limit von ${sectionLimit} Karten überschreiten.` };
    }

    if (existing) existing.qty += qty;
    else list.push({ id: card.id, name: card.name, qty });

    saveUserData(userId, data);
    return { ok: true, message: `✅ ${qty}x "${card.name}" zu ${section} von "${deckName}" hinzugefügt.` };
  }

  function removeCardFromDeck(userId, deckName, cardName, qty = 1) {
    const data = getUserData(userId);
    const deck = data.decks[deckName];
    if (!deck) return { ok: false, message: `Deck "${deckName}" existiert nicht.` };

    for (const section of ['main', 'extra', 'side']) {
      const list = deck[section];
      const idx = list.findIndex((c) => c.name.toLowerCase() === cardName.toLowerCase());
      if (idx !== -1) {
        list[idx].qty -= qty;
        if (list[idx].qty <= 0) list.splice(idx, 1);
        saveUserData(userId, data);
        return { ok: true, message: `✅ ${qty}x "${cardName}" aus ${section} von "${deckName}" entfernt.` };
      }
    }
    return { ok: false, message: `❌ "${cardName}" ist nicht in "${deckName}".` };
  }

  function validateDeck(userId, deckName) {
    const data = getUserData(userId);
    const deck = data.decks[deckName];
    if (!deck) return { ok: false, message: `Deck "${deckName}" existiert nicht.` };

    const mainCount = deck.main.reduce((s, c) => s + c.qty, 0);
    const extraCount = deck.extra.reduce((s, c) => s + c.qty, 0);
    const sideCount = deck.side.reduce((s, c) => s + c.qty, 0);

    const errors = [];
    if (mainCount < LIMITS.main.min || mainCount > LIMITS.main.max) {
      errors.push(`Main-Deck hat ${mainCount} Karten (erlaubt: ${LIMITS.main.min}-${LIMITS.main.max}).`);
    }
    if (extraCount > LIMITS.extra.max) errors.push(`Extra-Deck hat ${extraCount} Karten (max ${LIMITS.extra.max}).`);
    if (sideCount > LIMITS.side.max) errors.push(`Side-Deck hat ${sideCount} Karten (max ${LIMITS.side.max}).`);

    return {
      ok: errors.length === 0,
      counts: { main: mainCount, extra: extraCount, side: sideCount },
      message: errors.length === 0 ? '✅ Deck ist regelkonform.' : errors.join('\n'),
    };
  }

  function formatDeckList(userId, deckName) {
    const data = getUserData(userId);
    const deck = data.decks[deckName];
    if (!deck) return `❌ Deck "${deckName}" existiert nicht.`;

    const fmt = (list) => (list.length ? list.map((c) => `  ${c.qty}x ${c.name}`).join('\n') : '  (leer)');

    return (
      `📦 *Deck: ${deck.name}*\n\n` +
      `*Main Deck* (${deck.main.reduce((s, c) => s + c.qty, 0)} Karten):\n${fmt(deck.main)}\n\n` +
      `*Extra Deck* (${deck.extra.reduce((s, c) => s + c.qty, 0)} Karten):\n${fmt(deck.extra)}\n\n` +
      `*Side Deck* (${deck.side.reduce((s, c) => s + c.qty, 0)} Karten):\n${fmt(deck.side)}`
    );
  }

  function getMainDeckAsCardList(userId, deckName) {
    const data = getUserData(userId);
    const deck = data.decks[deckName];
    if (!deck) return [];
    const list = [];
    for (const c of deck.main) {
      for (let i = 0; i < c.qty; i++) list.push({ id: c.id, name: c.name });
    }
    return list;
  }

  return {
    createDeck,
    deleteDeck,
    setActiveDeck,
    listDecks,
    addCardToDeck,
    removeCardFromDeck,
    validateDeck,
    formatDeckList,
    getMainDeckAsCardList,
  };
}

// ============================================================================
// 4) DUELL-SIMULATOR — vereinfachte Yu-Gi-Oh-Kampfregeln
// ============================================================================

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

class YuGiOhPlayer {
  constructor(userId, name, deckList) {
    this.userId = userId;
    this.name = name;
    this.lp = 8000;
    this.deck = shuffle([...deckList]);
    this.hand = [];
    this.field = { monsters: [] };
    this.graveyard = [];
    this.hasNormalSummoned = false;
  }

  draw(n = 1) {
    const drawn = [];
    for (let i = 0; i < n; i++) {
      if (this.deck.length === 0) {
        this.deckedOut = true;
        break;
      }
      const card = this.deck.shift();
      this.hand.push(card);
      drawn.push(card);
    }
    return drawn;
  }
}

class YuGiOhDuel {
  constructor(duelId, player1, player2, cardApi) {
    this.duelId = duelId;
    this.players = [player1, player2];
    this.cardApi = cardApi;
    this.turn = 0;
    this.turnCount = 1;
    this.phase = 'main';
    this.finished = false;
    this.winner = null;
    this.log = [];
  }

  get current() { return this.players[this.turn]; }
  get opponent() { return this.players[1 - this.turn]; }
  _log(text) { this.log.push(text); }

  start() {
    for (const p of this.players) p.draw(5);
    this._log(`🎬 Duell gestartet! ${this.players[0].name} vs ${this.players[1].name}`);
    this._log(`${this.current.name} ist am Zug (Zug 1).`);
    return this.summarize();
  }

  drawPhase() {
    if (this.finished) return this.summarize();
    const p = this.current;
    const drawn = p.draw(1);
    if (p.deckedOut) {
      this._endDuel(this.opponent, `${p.name} hat keine Karten mehr im Deck (Decked Out)!`);
      return this.summarize();
    }
    this._log(`📥 ${p.name} zieht: ${drawn[0]?.name || '(keine Karte)'}.`);
    this.phase = 'main';
    return this.summarize();
  }

  async normalSummon(cardName, position = 'attack') {
    if (this.finished) return this.summarize();
    const p = this.current;

    if (p.hasNormalSummoned) {
      this._log(`⚠️ ${p.name} hat diesen Zug schon normal beschworen.`);
      return this.summarize();
    }

    const handIdx = p.hand.findIndex((c) => c.name.toLowerCase() === cardName.toLowerCase());
    if (handIdx === -1) {
      this._log(`⚠️ "${cardName}" ist nicht auf der Hand von ${p.name}.`);
      return this.summarize();
    }

    const cardStub = p.hand[handIdx];
    const fullCard = await this.cardApi.getCardById(cardStub.id);
    if (!fullCard || !fullCard.type.includes('Monster')) {
      this._log(`⚠️ "${cardName}" ist kein normal beschwörbares Monster.`);
      return this.summarize();
    }

    const level = fullCard.level || 1;
    const tributesNeeded = level >= 7 ? 2 : level >= 5 ? 1 : 0;

    if (tributesNeeded > p.field.monsters.length) {
      this._log(`⚠️ "${fullCard.name}" (Level ${level}) benötigt ${tributesNeeded} Tribute.`);
      return this.summarize();
    }
    if (p.field.monsters.length - tributesNeeded >= 5) {
      this._log(`⚠️ Feld von ${p.name} ist voll.`);
      return this.summarize();
    }

    for (let i = 0; i < tributesNeeded; i++) {
      const tribute = p.field.monsters.shift();
      p.graveyard.push(tribute.card);
      this._log(`🔻 ${p.name} tributet "${tribute.card.name}".`);
    }

    p.hand.splice(handIdx, 1);
    p.field.monsters.push({
      card: fullCard,
      position,
      faceDown: position === 'defense',
      canAttack: true,
      justSummoned: true,
    });
    p.hasNormalSummoned = true;

    const posText = position === 'attack' ? 'Angriffsposition' : 'verdeckter Verteidigungsposition';
    this._log(`✨ ${p.name} beschwört "${fullCard.name}" (ATK ${fullCard.atk}/DEF ${fullCard.def}) in ${posText}.`);
    return this.summarize();
  }

  attack(attackerName, targetName = null) {
    if (this.finished) return this.summarize();
    const p = this.current;
    const opp = this.opponent;

    const attackerEntry = p.field.monsters.find((m) => m.card.name.toLowerCase() === attackerName.toLowerCase());
    if (!attackerEntry) { this._log(`⚠️ "${attackerName}" steht nicht auf dem Feld von ${p.name}.`); return this.summarize(); }
    if (attackerEntry.position !== 'attack') { this._log(`⚠️ "${attackerName}" ist nicht in Angriffsposition.`); return this.summarize(); }
    if (!attackerEntry.canAttack) { this._log(`⚠️ "${attackerName}" hat diesen Zug schon angegriffen.`); return this.summarize(); }

    attackerEntry.canAttack = false;

    if (!targetName) {
      if (opp.field.monsters.length > 0) {
        this._log(`⚠️ Direktangriff nicht möglich, ${opp.name} hat noch Monster auf dem Feld.`);
        return this.summarize();
      }
      opp.lp -= attackerEntry.card.atk;
      this._log(`⚔️ ${p.name}s "${attackerEntry.card.name}" greift direkt an! ${opp.name} verliert ${attackerEntry.card.atk} LP.`);
      this._checkGameOver();
      return this.summarize();
    }

    const targetEntry = opp.field.monsters.find((m) => m.card.name.toLowerCase() === targetName.toLowerCase());
    if (!targetEntry) { this._log(`⚠️ "${targetName}" steht nicht auf dem Feld von ${opp.name}.`); return this.summarize(); }

    const atkPower = attackerEntry.card.atk;
    const defValue = targetEntry.position === 'attack' ? targetEntry.card.atk : targetEntry.card.def;

    this._log(`⚔️ ${p.name}s "${attackerEntry.card.name}" (ATK ${atkPower}) greift "${targetEntry.card.name}" an!`);

    if (targetEntry.position === 'attack') {
      if (atkPower > defValue) {
        this._destroyMonster(opp, targetEntry);
        opp.lp -= atkPower - defValue;
        this._log(`💥 "${targetEntry.card.name}" zerstört. ${opp.name} verliert ${atkPower - defValue} LP.`);
      } else if (atkPower < defValue) {
        this._destroyMonster(p, attackerEntry);
        p.lp -= defValue - atkPower;
        this._log(`💥 "${attackerEntry.card.name}" zerstört. ${p.name} verliert ${defValue - atkPower} LP.`);
      } else {
        this._destroyMonster(p, attackerEntry);
        this._destroyMonster(opp, targetEntry);
        this._log(`💥 Beide Monster werden zerstört (gleiche ATK).`);
      }
    } else {
      targetEntry.faceDown = false;
      if (atkPower > defValue) {
        this._destroyMonster(opp, targetEntry);
        this._log(`💥 "${targetEntry.card.name}" zerstört (kein LP-Schaden bei Verteidigung).`);
      } else if (atkPower < defValue) {
        p.lp -= defValue - atkPower;
        this._log(`🛡️ Angriff abgewehrt. ${p.name} verliert ${defValue - atkPower} LP.`);
      } else {
        this._log(`🛡️ Gleichstand - kein Effekt.`);
      }
    }

    this._checkGameOver();
    return this.summarize();
  }

  _destroyMonster(player, entry) {
    player.field.monsters = player.field.monsters.filter((m) => m !== entry);
    player.graveyard.push(entry.card);
  }

  _checkGameOver() {
    for (const p of this.players) {
      if (p.lp <= 0) {
        const winner = this.players.find((x) => x !== p);
        this._endDuel(winner, `${p.name} hat 0 LP erreicht!`);
      }
    }
  }

  _endDuel(winner, reason) {
    this.finished = true;
    this.winner = winner;
    this._log(`🏆 ${reason} ${winner.name} gewinnt das Duell!`);
  }

  endTurn() {
    if (this.finished) return this.summarize();
    for (const m of this.current.field.monsters) m.justSummoned = false;
    this.current.hasNormalSummoned = false;
    this.turn = 1 - this.turn;
    this.turnCount += 1;
    for (const m of this.current.field.monsters) m.canAttack = true;
    this._log(`🔁 Zug beendet. Jetzt ist ${this.current.name} am Zug (Zug ${this.turnCount}).`);
    return this.drawPhase();
  }

  summarize() {
    const [p1, p2] = this.players;
    const fieldText = (p) =>
      p.field.monsters.length
        ? p.field.monsters.map((m) => `${m.card.name} (${m.faceDown ? 'verdeckt, ' : ''}${m.position === 'attack' ? 'ATK' : 'DEF'})`).join(', ')
        : '(leer)';

    const lines = [
      `--- Zug ${this.turnCount} | Phase: ${this.phase} ---`,
      `${p1.name}: ${p1.lp} LP | Hand: ${p1.hand.length} | Feld: ${fieldText(p1)}`,
      `${p2.name}: ${p2.lp} LP | Hand: ${p2.hand.length} | Feld: ${fieldText(p2)}`,
      '',
      ...this.log.slice(-6),
    ];

    if (this.finished) lines.push('', `🏁 Duell beendet. Gewinner: ${this.winner.name}`);
    else lines.push('', `➡️ Am Zug: ${this.current.name}`);

    return lines.join('\n');
  }
}

function createDuelSystem(deckManager, cardApi) {
  const activeDuels = new Map();

  async function createDuel(duelId, user1, user2) {
    const list1 = deckManager.getMainDeckAsCardList(user1.userId, user1.deckName);
    const list2 = deckManager.getMainDeckAsCardList(user2.userId, user2.deckName);

    if (list1.length < 40) return { ok: false, message: `❌ ${user1.name}s Deck "${user1.deckName}" hat weniger als 40 Karten.` };
    if (list2.length < 40) return { ok: false, message: `❌ ${user2.name}s Deck "${user2.deckName}" hat weniger als 40 Karten.` };

    const p1 = new YuGiOhPlayer(user1.userId, user1.name, list1);
    const p2 = new YuGiOhPlayer(user2.userId, user2.name, list2);
    const duel = new YuGiOhDuel(duelId, p1, p2, cardApi);
    duel.start();

    activeDuels.set(duelId, duel);
    return { ok: true, message: duel.summarize() };
  }

  const getDuel = (duelId) => activeDuels.get(duelId) || null;
  const endDuelSession = (duelId) => activeDuels.delete(duelId);

  return { createDuel, getDuel, endDuelSession };
}

// ============================================================================
// 5) HAUPT-FACTORY — das hier importierst du in deine index.js
// ============================================================================

export function createYuGiOh(dataPath) {
  const cardApi = createCardApi();
  const decks = createDeckManager(dataPath, cardApi);
  const duels = createDuelSystem(decks, cardApi);
  return { cardApi, decks, duels };
}

/* ============================================================================
INTEGRATION in deine index.js
============================================================================

1) Import ganz oben in index.js:

   import { createYuGiOh } from './yugioh.mjs';

2) Einmalig initialisieren, neben deinen anderen Systemen:

   const yugioh = createYuGiOh(DATA_PATH);
   const pendingYugiohChallenges = new Map(); // chatId -> {challengerId, challengerName, opponentId}

3) Füge diese Blöcke in deine if(cmd === '...')-Kette ein
   (nutzt deine vorhandenen Variablen: cmd, args, sender, send, m, from, isGroup,
   sock, ensureUser, resolveLidJid):

if (cmd === 'yugioh') {
  return send(
    ('🎴 *Yu-Gi-Oh! Befehle*\n\n' +
    '*Karten*\n  $ygocard <name>\n  $ygorandom\n\n' +
    '*Deckbau*\n  $ygodeck new <name>\n  $ygodeck use <name>\n  $ygodeck list\n' +
    '  $ygodeck show <name>\n  $ygodeck add <name> [anzahl] <karte>\n' +
    '  $ygodeck remove <name> <karte>\n  $ygodeck check <name>\n\n' +
    '*Duell*\n  $ygoduel @gegner\n  $ygoaccept\n  $ygosummon <karte> [atk|def]\n' +
    '  $ygoattack <karte> [vs <ziel>]\n  $ygoendturn\n  $ygostatus\n  $ygoforfeit')
      .replaceAll('$', activePrefix)
  );
}

if (cmd === 'ygocard') {
  const name = args.join(' ');
  if (!name) return send(`Nutzung: ${activePrefix}ygocard <kartenname>`);
  const { best, alternatives } = await yugioh.cardApi.searchCard(name);
  if (!best) return send(`❌ Keine Karte gefunden für "${name}".`);
  let text = yugioh.cardApi.formatCardText(best);
  const others = (alternatives || []).filter((c) => c.id !== best.id).slice(0, 3);
  if (others.length) text += `\n\n🔎 Meintest du auch: ${others.map((c) => c.name).join(', ')}?`;
  const imgUrl = yugioh.cardApi.getCardImageUrl(best);
  if (imgUrl) { await sock.sendMessage(from, { image: { url: imgUrl }, caption: text }, { quoted: m }); return; }
  return send(text);
}

if (cmd === 'ygorandom') {
  const card = await yugioh.cardApi.getRandomCard();
  const text = yugioh.cardApi.formatCardText(card);
  const imgUrl = yugioh.cardApi.getCardImageUrl(card);
  if (imgUrl) { await sock.sendMessage(from, { image: { url: imgUrl }, caption: text }, { quoted: m }); return; }
  return send(text);
}

if (cmd === 'ygodeck') {
  const action = (args.shift() || '').toLowerCase();
  ensureUser(sender);
  if (action === 'new') {
    const name = args.join(' ');
    if (!name) return send(`Nutzung: ${activePrefix}ygodeck new <name>`);
    return send(yugioh.decks.createDeck(sender, name).message);
  }
  if (action === 'use') {
    const name = args.join(' ');
    if (!name) return send(`Nutzung: ${activePrefix}ygodeck use <name>`);
    return send(yugioh.decks.setActiveDeck(sender, name).message);
  }
  if (action === 'list') {
    const { decks, activeDeck } = yugioh.decks.listDecks(sender);
    if (!decks.length) return send('Du hast noch keine Decks. Erstelle eins mit "ygodeck new <name>".');
    return send(`📚 Deine Decks:\n${decks.map((d) => (d === activeDeck ? `👉 ${d} (aktiv)` : `   ${d}`)).join('\n')}`);
  }
  if (action === 'show') {
    const name = args.join(' ') || yugioh.decks.listDecks(sender).activeDeck;
    if (!name) return send('Kein Deck angegeben und kein aktives Deck gesetzt.');
    return send(yugioh.decks.formatDeckList(sender, name));
  }
  if (action === 'add') {
    const deckName = args.shift();
    let qty = 1;
    if (args[0] && /^\d+$/.test(args[0])) qty = parseInt(args.shift(), 10);
    const cardName = args.join(' ');
    if (!deckName || !cardName) return send(`Nutzung: ${activePrefix}ygodeck add <deckname> [anzahl] <kartenname>`);
    const res = await yugioh.decks.addCardToDeck(sender, deckName, cardName, qty);
    return send(res.message);
  }
  if (action === 'remove') {
    const deckName = args.shift();
    const cardName = args.join(' ');
    if (!deckName || !cardName) return send(`Nutzung: ${activePrefix}ygodeck remove <deckname> <kartenname>`);
    return send(yugioh.decks.removeCardFromDeck(sender, deckName, cardName).message);
  }
  if (action === 'check') {
    const name = args.join(' ') || yugioh.decks.listDecks(sender).activeDeck;
    if (!name) return send('Kein Deck angegeben und kein aktives Deck gesetzt.');
    return send(yugioh.decks.validateDeck(sender, name).message);
  }
  if (action === 'delete') {
    const name = args.join(' ');
    if (!name) return send(`Nutzung: ${activePrefix}ygodeck delete <name>`);
    return send(yugioh.decks.deleteDeck(sender, name).message);
  }
  return send('❓ Optionen: new, use, list, show, add, remove, check, delete.');
}

if (cmd === 'ygoduel') {
  if (!isGroup) return send('❌ Fordere jemanden in einer Gruppe heraus (Erwähnung nötig).');
  const ctx = m.message?.extendedTextMessage?.contextInfo;
  const opponentId = ctx?.mentionedJid?.[0];
  if (!opponentId) return send(`Nutzung: ${activePrefix}ygoduel @gegner`);
  const opponentLid = await resolveLidJid(opponentId, sock);
  pendingYugiohChallenges.set(from, { challengerId: sender, challengerName: sender.split('@')[0], opponentId: opponentLid });
  return send(`⚔️ @${sender.split('@')[0]} fordert @${opponentLid.split('@')[0]} zu einem Yu-Gi-Oh-Duell heraus! Antworte mit "${activePrefix}ygoaccept".`, { mentions: [sender, opponentLid] });
}

if (cmd === 'ygoaccept') {
  const challenge = pendingYugiohChallenges.get(from);
  if (!challenge) return send('❌ Keine offene Herausforderung in diesem Chat.');
  if (challenge.opponentId !== sender) return send('❌ Diese Herausforderung ist nicht an dich gerichtet.');
  const { activeDeck: challengerActive } = yugioh.decks.listDecks(challenge.challengerId);
  const { activeDeck: opponentActive } = yugioh.decks.listDecks(sender);
  if (!challengerActive) return send('❌ Der Herausforderer hat kein aktives Deck gesetzt.');
  if (!opponentActive) return send(`❌ Du hast kein aktives Deck gesetzt (${activePrefix}ygodeck use <name>).`);
  const result = await yugioh.duels.createDuel(
    from,
    { userId: challenge.challengerId, name: challenge.challengerName, deckName: challengerActive },
    { userId: sender, name: sender.split('@')[0], deckName: opponentActive }
  );
  pendingYugiohChallenges.delete(from);
  return send(result.message);
}

if (cmd === 'ygosummon') {
  const duel = yugioh.duels.getDuel(from);
  if (!duel) return send('❌ Kein aktives Duell in diesem Chat.');
  let position = 'attack';
  if (args[args.length - 1] === 'atk') { position = 'attack'; args.pop(); }
  else if (args[args.length - 1] === 'def') { position = 'defense'; args.pop(); }
  const cardName = args.join(' ');
  if (!cardName) return send(`Nutzung: ${activePrefix}ygosummon <kartenname> [atk|def]`);
  return send(await duel.normalSummon(cardName, position));
}

if (cmd === 'ygoattack') {
  const duel = yugioh.duels.getDuel(from);
  if (!duel) return send('❌ Kein aktives Duell in diesem Chat.');
  const full = args.join(' ');
  const parts = full.split(/\s+vs\s+/i);
  if (parts.length === 2) return send(duel.attack(parts[0].trim(), parts[1].trim()));
  return send(duel.attack(full.trim(), null));
}

if (cmd === 'ygoendturn') {
  const duel = yugioh.duels.getDuel(from);
  if (!duel) return send('❌ Kein aktives Duell in diesem Chat.');
  const result = duel.endTurn();
  if (duel.finished) yugioh.duels.endDuelSession(from);
  return send(result);
}

if (cmd === 'ygostatus') {
  const duel = yugioh.duels.getDuel(from);
  if (!duel) return send('❌ Kein aktives Duell in diesem Chat.');
  return send(duel.summarize());
}

if (cmd === 'ygoforfeit') {
  const duel = yugioh.duels.getDuel(from);
  if (!duel) return send('❌ Kein aktives Duell in diesem Chat.');
  const loser = duel.players.find((p) => p.userId === sender);
  const winner = duel.players.find((p) => p.userId !== sender);
  if (!loser) return send('❌ Du bist nicht Teil dieses Duells.');
  yugioh.duels.endDuelSession(from);
  return send(`🏳️ ${loser.name} gibt auf. ${winner.name} gewinnt!`);
}

============================================================================ */
