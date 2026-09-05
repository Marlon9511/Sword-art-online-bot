// yugioh/cardApi.mjs
// Zugriff auf die kostenlose YGOPRODeck-API. Kein API-Key nötig.
// Docs: https://ygoprodeck.com/api-guide/

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

/**
 * Sucht eine Karte per (Teil-)Namen.
 * @returns {Promise<{best: object|null, alternatives: object[]}>}
 */
export async function searchCard(name) {
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

/** Holt eine Karte exakt per ID (passcode). */
export async function getCardById(id) {
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

/** Zufällige Karte. */
export async function getRandomCard() {
  const res = await fetch('https://db.ygoprodeck.com/api/v7/randomcard.php');
  if (!res.ok) return null;
  return res.json();
}

/** Baut aus einem Karten-Objekt einen WhatsApp-tauglichen Text. */
export function formatCardText(card) {
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

/** Bild-URL der Karte (z.B. für sock.sendMessage({ image: {url}, caption })). */
export function getCardImageUrl(card) {
  return card?.card_images?.[0]?.image_url || null;
}
