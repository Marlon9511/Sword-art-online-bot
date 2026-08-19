import fs from 'fs';
import path from 'path';

/**
 * JID <-> LID Resolver für Baileys.
 *
 * WICHTIG: Eine @lid-Adresse hat NICHT dieselbe Ziffernfolge wie die
 * Telefonnummer (@s.whatsapp.net). Man kann sie also nicht einfach
 * ineinander umwandeln, indem man die Nummer neu zusammensetzt (so wie
 * es die bisherigen toLidJid/toParticipantJid-Funktionen in der index.js
 * tun) — das funktioniert nur "zufällig" bei alten/simplen Fällen.
 *
 * Echte Umwandlung geht nur über Baileys' internes Signal-Repository
 * (sock.signalRepository.lidMapping), das die Zuordnung von WhatsApp
 * selbst bekommt, sobald der Bot mit dem jeweiligen Kontakt in Kontakt
 * kam (Nachricht, Gruppe, etc.). Ergebnisse werden hier lokal gecacht,
 * damit nicht bei jeder Anfrage neu aufgelöst werden muss.
 */
export function createLidSystem(DATA_PATH) {
  const CACHE_FILE = path.join(DATA_PATH, 'lid-cache.json');

  let cache = { pnToLid: {}, lidToPn: {} };
  try {
    if (fs.existsSync(CACHE_FILE)) {
      cache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
    }
  } catch (e) {
    console.error('[lid-system] Cache konnte nicht geladen werden:', e?.message || e);
  }

  function saveCache() {
    try {
      fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
    } catch (e) {
      console.error('[lid-system] Cache konnte nicht gespeichert werden:', e?.message || e);
    }
  }

  function extractNum(jid) {
    if (!jid) return null;
    return String(jid).split(':')[0].split('@')[0].replace(/[^0-9]/g, '') || null;
  }

  /**
   * Wandelt eine Telefon-JID (@s.whatsapp.net) in eine LID (@lid) um.
   * Gibt null zurück, wenn keine Zuordnung bekannt/auflösbar ist.
   */
  async function jidToLid(jid, sock) {
    if (!jid) return null;
    const s = String(jid);
    if (s.endsWith('@lid')) return s; // schon eine LID
    if (!s.endsWith('@s.whatsapp.net')) return null;

    if (cache.pnToLid[s]) return cache.pnToLid[s];

    try {
      const lid = await sock?.signalRepository?.lidMapping?.getLIDForPN(s);
      if (lid) {
        cache.pnToLid[s] = lid;
        cache.lidToPn[lid] = s;
        saveCache();
        return lid;
      }
    } catch (e) {
      console.error('[lid-system] getLIDForPN Fehler:', e?.message || e);
    }
    return null;
  }

  /**
   * Wandelt eine LID (@lid) in die Telefon-JID (@s.whatsapp.net) um.
   * Gibt null zurück, wenn keine Zuordnung bekannt/auflösbar ist.
   */
  async function lidToJid(lid, sock) {
    if (!lid) return null;
    const s = String(lid);
    if (s.endsWith('@s.whatsapp.net')) return s; // schon eine Telefon-JID
    if (!s.endsWith('@lid')) return null;

    if (cache.lidToPn[s]) return cache.lidToPn[s];

    try {
      const pn = await sock?.signalRepository?.lidMapping?.getPNForLID(s);
      if (pn) {
        const num = extractNum(pn);
        const normalizedPn = num ? `${num}@s.whatsapp.net` : pn;
        cache.lidToPn[s] = normalizedPn;
        cache.pnToLid[normalizedPn] = s;
        saveCache();
        return normalizedPn;
      }
    } catch (e) {
      console.error('[lid-system] getPNForLID Fehler:', e?.message || e);
    }
    return null;
  }

  /**
   * Nimmt irgendeine JID (LID oder Telefon) und gibt beide Formen zurück,
   * soweit auflösbar: { pn, lid }
   */
  async function resolveAny(jid, sock) {
    if (!jid) return { pn: null, lid: null };
    const s = String(jid);
    if (s.endsWith('@lid')) {
      const pn = await lidToJid(s, sock);
      return { pn, lid: s };
    }
    if (s.endsWith('@s.whatsapp.net')) {
      const lid = await jidToLid(s, sock);
      return { pn: s, lid };
    }
    return { pn: null, lid: null };
  }

  /**
   * Cached beide Richtungen für eine bekannte (jid, lid)-Paarung, z.B.
   * wenn man sie schon aus groupMetadata.participants[].id / .lid hat
   * (Baileys liefert bei manchen Gruppen-Teilnehmern beide Felder mit).
   */
  function primeCache(pnJid, lidJid) {
    if (!pnJid || !lidJid) return;
    const num = extractNum(pnJid);
    const normalizedPn = num ? `${num}@s.whatsapp.net` : pnJid;
    cache.pnToLid[normalizedPn] = lidJid;
    cache.lidToPn[lidJid] = normalizedPn;
    saveCache();
  }

  /**
   * Command-Handler für ?jid2lid und ?lid2jid.
   * Rückgabewert true = wurde behandelt (Aufrufer kann return machen).
   */
  async function handle({ cmd, args, send, sock, m }) {
    if (cmd === 'jid2lid') {
      const ctx = m?.message?.extendedTextMessage?.contextInfo;
      let target = args[0];
      if (ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
      else if (!target && ctx?.participant) target = ctx.participant;

      if (!target) {
        await send('❌ Nutzung: ?jid2lid <nummer|@user>\nBeispiel: ?jid2lid 4915123456789');
        return true;
      }

      const num = extractNum(target);
      if (!num) {
        await send('❌ Ungültige Nummer/JID.');
        return true;
      }
      const pnJid = `${num}@s.whatsapp.net`;

      const lid = await jidToLid(pnJid, sock);
      await send(
        lid
          ? `✅ ${pnJid}\n→ LID: ${lid}`
          : `❌ Keine LID gefunden für ${pnJid}.\n(Der Bot muss den Kontakt schon mal "gesehen" haben, z.B. über eine Nachricht oder gemeinsame Gruppe.)`
      );
      return true;
    }

    if (cmd === 'lid2jid') {
      const ctx = m?.message?.extendedTextMessage?.contextInfo;
      let target = args[0];
      if (ctx?.mentionedJid?.length) target = ctx.mentionedJid[0];
      else if (!target && ctx?.participant) target = ctx.participant;

      if (!target) {
        await send('❌ Nutzung: ?lid2jid <lid-nummer>\nBeispiel: ?lid2jid 27088878862400');
        return true;
      }

      const num = extractNum(target);
      if (!num) {
        await send('❌ Ungültige LID.');
        return true;
      }
      const lidJid = `${num}@lid`;

      const pn = await lidToJid(lidJid, sock);
      await send(
        pn
          ? `✅ ${lidJid}\n→ Telefon-JID: ${pn}`
          : `❌ Keine Telefonnummer gefunden für ${lidJid}.`
      );
      return true;
    }

    return false;
  }

  return { jidToLid, lidToJid, resolveAny, primeCache, handle };
}
