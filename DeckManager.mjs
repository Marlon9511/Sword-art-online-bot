// yugioh/deckManager.mjs
import { createJsonStore } from './storage.mjs';
import { searchCard, getCardById } from './cardApi.mjs';

const LIMITS = {
  main: { min: 40, max: 60 },
  extra: { min: 0, max: 15 },
  side: { min: 0, max: 15 },
  maxCopies: 3,
};

/**
 * Erstellt einen Deck-Manager, der seine Daten in <DATA_PATH>/yugioh-decks.json ablegt.
 * Aufruf einmal in deiner index.js: const deckManager = createDeckManager(DATA_PATH);
 *
 * Decks werden pro normalisierter JID (deine `sender`-Variable) gespeichert.
 */
export function createDeckManager(dataPath) {
  const store = createJsonStore(dataPath, 'yugioh-decks.json');

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
      ? { best: await getCardById(cardNameOrId) }
      : await searchCard(cardNameOrId);

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
