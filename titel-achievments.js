/* =====================================================================
   🎖️ SAO TITEL- & ACHIEVEMENT-SYSTEM — Modul
   =====================================================================
   Verwaltet: Freischaltbare Titel (?title), Erfolge/Meilensteine
   (?achievements) und einen kosmetischen HP-Balken (?hpbar).

   Speichert direkt in users[jid] (kein eigenes File nötig):
     users[jid].unlockedTitles   -> string[] (Titel-IDs)
     users[jid].activeTitle      -> string | null (Titel-ID)
     users[jid].unlockedAchievements -> { [achievementId]: timestamp }
     users[jid].showHpBar        -> boolean (Anzeige-Präferenz)

   WICHTIG: Nutzt eure echten Stat-Felder aus dem Arena-System:
     users[jid].duel.wins      -> Anzahl gewonnener Duelle
     users[jid].duel.losses    -> Anzahl verlorener Duelle
     users[jid].duel.fights    -> Gesamtanzahl Duelle
     users[jid].duel.earnings  -> Netto-Coins aus Duellen
     users[jid].coins          -> aktuelle Coins (statt "gold")
     users[jid].level          -> Level

   Integration in andere Module:
     - Nach Duellen, Level-Ups, Gilden-Gründung etc. `checkProgress(ctx, jid)`
       aufrufen. Neu freigeschaltete Titel/Erfolge werden automatisch
       per ctx.send an den Spieler gemeldet.
     - Für die Profilanzeige: users[jid].activeTitle -> TITLES.find(...)
       um den Anzeigenamen zu bekommen.
     - Für Duell-/Gear-Ausgaben: renderHpBar(current, max) importieren
       und nur anzeigen, wenn users[jid].showHpBar !== false.
     - Owner-Erkennung für den "Kayaba Akihiko"-Titel: übergib beim
       Handler-Aufruf ctx.ownerJids = [OWNER_LID, OWNER_PRIV, ...]
       (Array eurer Owner-JIDs/-Nummern aus index.js). Der Vergleich
       läuft über die reine Rufnummer, also egal ob @lid oder
       @s.whatsapp.net ankommt.
   ===================================================================== */

export const TITLE_COMMANDS = [
  'title', 'titel', 'achievements', 'erfolge', 'hpbar'
];

export const TITLE_HELP_TEXT =
  `▸ {P}title — Deine freigeschalteten Titel anzeigen\n` +
  `▸ {P}title list — Alle Titel im Spiel anzeigen\n` +
  `▸ {P}title set <name> — Aktiven Titel setzen\n` +
  `▸ {P}title info <name> — Freischaltbedingung eines Titels anzeigen\n` +
  `▸ {P}achievements — Deine Erfolge & Fortschritt anzeigen\n` +
  `▸ {P}hpbar — HP-Balken-Anzeige an/aus umschalten\n`;

/* ---------------------------------------------------------------------
   TITEL-DEFINITIONEN
   check(u) bekommt das users[jid]-Objekt und gibt true/false zurück.
--------------------------------------------------------------------- */
export const TITLES = [
  {
    id: 'beater',
    name: 'Beater',
    icon: '⚔️',
    desc: 'Erreiche als einer der Ersten Level 15.',
    check: (u) => (u.level || 1) >= 15
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
    id: 'aincrad_legend',
    name: 'Legende von Aincrad',
    icon: '🐉',
    desc: 'Gewinne 100 Duelle.',
    check: (u) => (u.duel?.wins || 0) >= 100
  },
  {
    id: 'undefeated',
    name: 'Unbesiegt',
    icon: '⚡',
    desc: 'Gewinne 20 Duelle, ohne mehr als 5 zu verlieren.',
    check: (u) => (u.duel?.wins || 0) >= 20 && (u.duel?.losses || 0) <= 5
  },
  {
    id: 'guildmaster',
    name: 'Gildenmeister',
    icon: '🏰',
    desc: 'Gründe eine Gilde.',
    check: (u) => !!u.guildId && (u.__isGuildLeader === true)
  },
  {
    id: 'lightning_flash',
    name: 'Lichtschwert',
    icon: '💫',
    desc: 'Erreiche Level 30.',
    check: (u) => (u.level || 1) >= 30
  },
  {
    id: 'gold_hoarder',
    name: 'Goldgräber',
    icon: '💰',
    desc: 'Sammle insgesamt 10.000 Coins.',
    check: (u) => (u.coins || 0) >= 10000
  },
  {
    id: 'front_liner',
    name: 'Frontkämpfer',
    icon: '🛡️',
    desc: 'Erreiche Etage/Floor 20.',
    check: (u) => (u.floor || 1) >= 20
  },
  {
    id: 'high_roller',
    name: 'Hochstapler',
    icon: '🎲',
    desc: 'Verdiene netto 5.000 Coins allein durch Duelle.',
    check: (u) => (u.duel?.earnings || 0) >= 5000
  },
  {
    id: 'kayaba',
    name: 'Kayaba Akihiko',
    icon: '👑',
    desc: 'Der Schöpfer selbst. Exklusiv für den Bot-Owner.',
    check: (u) => u.__isOwner === true
  }
];

/* ---------------------------------------------------------------------
   ACHIEVEMENT-DEFINITIONEN
--------------------------------------------------------------------- */
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
  }
];

/* ---------------------------------------------------------------------
   HP-BALKEN — rein kosmetische Darstellung, z.B. für ?gear oder
   Duell-Ergebnisse. Einfach importieren und aufrufen:
     renderHpBar(current, max)
--------------------------------------------------------------------- */
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

/* ---------------------------------------------------------------------
   Extrahiert nur die reine Ziffernfolge aus einer JID, egal ob @lid,
   @s.whatsapp.net oder mit :device-Suffix.
--------------------------------------------------------------------- */
function extractRawNumberTitle(jid) {
  if (!jid) return null;
  return String(jid).split(':')[0].split('@')[0].replace(/[^0-9]/g, '') || null;
}

/* ---------------------------------------------------------------------
   FORTSCHRITTS-CHECK — von anderen Modulen nach relevanten Events
   aufrufen (Duell gewonnen, Level-Up, Gilde beigetreten, ...).
   Schaltet automatisch neue Titel/Achievements frei und meldet sie.
--------------------------------------------------------------------- */
export async function checkProgress(ctx, jid) {
  const { users, save, FILES, send } = ctx;
  const u = users[jid];
  if (!u) return;

  if (!Array.isArray(u.unlockedTitles)) u.unlockedTitles = [];
  if (!u.unlockedAchievements || typeof u.unlockedAchievements !== 'object') u.unlockedAchievements = {};

  // Hilfsflag für Gildenmeister-Titel
  if (ctx.guilds && u.guildId && ctx.guilds[u.guildId]) {
    u.__isGuildLeader = ctx.guilds[u.guildId].leader === jid;
  }

  // Hilfsflag für den exklusiven "Kayaba Akihiko"-Titel (Bot-Owner).
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

    // Bei jedem Aufruf still im Hintergrund prüfen, ob etwas nachzuholen ist
    await checkProgress(ctx, sender);

    // ---------------- ?hpbar ----------------
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

    // ---------------- ?achievements ----------------
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

    // ---------------- ?title / ?titel ----------------
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