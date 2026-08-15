export const MENU_COMMANDS = ['help', 'menu'];

const DIVIDER = '⚔️┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈⚔️';

function buildMainLayer(ctx) {
  const { PREFIX, sender, isAuthorized, hasAdminPerms } = ctx;
  let t = `┏━━━━━━━━━━━━━━━┓\n┃  ▄▄▄▄▄▄▄▄▄▄▄▄▄  ┃\n┃  █ AINCRAD █  ┃\n┃  ▀▀▀▀▀▀▀▀▀▀▀▀▀  ┃\n┗━━━━━━━━━━━━━━━┛\n     🗡️ System Command Window 🗡️\n     ⌈ Floor: Main Menu ⌋\n\n`;

  t += `🔷 *SYSTEM MENU*\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}help — Dieses Command-Window öffnen\n`;
  t += `▸ ${PREFIX}ping — Verbindung zum Server prüfen\n`;
  t += `▸ ${PREFIX}owner — Game Master kontaktieren\n`;
  t += `▸ ${PREFIX}com — Link zur Gilden-Halle\n`;
  t += `▸ ${PREFIX}whoami / ${PREFIX}me — Charakterbogen anzeigen\n`;
  t += `▸ ${PREFIX}afk [grund] — Logout-Status setzen\n`;
  t += `▸ ${PREFIX}usertodo add <text> — Skill vorschlagen\n`;
  t += `▸ ${PREFIX}credits — Alle Beta-Tester des Systems\n`;
  t += `▸ ${PREFIX}marry @user — Verlobungsring überreichen\n`;
  t += `▸ ${PREFIX}divorce — Ring zurückgeben\n`;
  t += `▸ ${PREFIX}sticker — Bild/GIF antworten → Sticker craften\n`;
  t += `▸ ${PREFIX}bewerbung — Gildenbeitritt beantragen\n`;
  t += `▸ ${PREFIX}setinfo <feld> <wert> — Profilinfos setzen (name/alter/hobbys/sexualitaet)\n`;
  t += `▸ ${PREFIX}sao — Zufälligen Sword Art Online Edit abspielen\n\n`;

  t += `📚 *WEITERE MENÜ-EBENEN* — nutze ${PREFIX}menu <name>\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}menu arena — Ausrüstung & PVP-Kämpfe\n`;
  t += `▸ ${PREFIX}menu gilde — Gilden-/Verbunds-System\n`;
  t += `▸ ${PREFIX}menu titel — Titel & Erfolge\n`;
  t += `▸ ${PREFIX}menu pokemon — Pokémon-System\n`;
  t += `▸ ${PREFIX}menu demonslayer — Dämonentöter-System\n`;
  t += `▸ ${PREFIX}menu hunter — Solo-Leveling Hunter-System\n`;
  t += `▸ ${PREFIX}menu social — Interaktions-Skills (hug, kiss, pat, ...)\n`;
  t += `▸ ${PREFIX}menu fun — Fun & Action Skills (kill, yeet, nuke, ...)\n`;
  t += `▸ ${PREFIX}menu chat — Gilden-Chat & Gruppeneinstellungen\n`;

  if (isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) {
    t += `▸ ${PREFIX}menu support — Ticket-System (Team)\n`;
  }
  t += `▸ ${PREFIX}menu admin — Gildenmeister-Befehle\n`;
  if (hasAdminPerms(sender)) {
    t += `▸ ${PREFIX}menu owner — System-Administrator-Befehle\n`;
  }

  t += `\n${DIVIDER}\n_⚔️ "The days of my life... I'll cut through them all." — Nutze Befehle ohne Parameter für mehr Info_`;
  return t;
}

function buildArenaLayer(ctx) {
  const { PREFIX, ARENA_HELP_TEXT } = ctx;
  let t = `⚔️ *ARENA & WIRTSCHAFT* (Cor & Kämpfe)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}daily — Tägliche Quest-Belohnung\n`;
  t += `▸ ${PREFIX}blackjack — Glücksspiel im Coliseum\n`;
  t += `▸ ${PREFIX}slot — Spielautomat in der Taverne\n`;
  t += `▸ ${PREFIX}fish — Angeln am Floor-See\n`;
  t += `▸ ${PREFIX}pet — Begleiter-Status prüfen\n`;
  t += `▸ ${PREFIX}adopt <name> — Begleiter zähmen\n`;
  t += `▸ ${PREFIX}feed — Begleiter füttern\n\n`;
  t += `⚔️ *ARENA-SYSTEM* (Ausrüstung & PVP)\n${DIVIDER}\n`;
  t += ARENA_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n');
  return t;
}

function buildGuildLayer(ctx) {
  const { PREFIX, GUILD_HELP_TEXT, GUILDWAR_HELP_TEXT } = ctx;
  let t = `🏰 *GILDEN-SYSTEM* (Verbünde)\n${DIVIDER}\n`;
  t += GUILD_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n');
  if (GUILDWAR_HELP_TEXT) {
    t += `\n\n${DIVIDER}\n`;
    t += GUILDWAR_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n');
  }
  return t;
}

function buildTitleLayer(ctx) {
  const { PREFIX, TITLE_HELP_TEXT } = ctx;
  let t = `🎖️ *TITEL & ERFOLGE*\n${DIVIDER}\n`;
  t += TITLE_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n');
  return t;
}

function buildPokemonLayer(ctx) {
  const { PREFIX, POKEMON_HELP_TEXT } = ctx;
  let t = `🐾 *POKÉMON-SYSTEM*\n${DIVIDER}\n`;
  t += POKEMON_HELP_TEXT.split('\n').filter(Boolean).map(l => l.replace(/\{P\}/g, PREFIX)).join('\n');
  return t;
}

function buildDemonSlayerLayer(ctx) {
  const { PREFIX, DS_HELP_TEXT } = ctx;
  let t = `👹 *DÄMONENTÖTER-SYSTEM*\n${DIVIDER}\n`;
  t += (DS_HELP_TEXT || '(keine Daten verfügbar)')
    .split('\n')
    .filter(Boolean)
    .map(l => l.replace(/\{P\}/g, PREFIX))
    .join('\n');
  t += `\n\n☀️🌙 _Sonnen- und Mondatmung sind extrem selten — nur wahre Legenden erlernen sie._`;
  return t;
}

function buildSoloLevelingLayer(ctx) {
  const { PREFIX, SL_HELP_TEXT } = ctx;
  let t = `⚡ *HUNTER-SYSTEM* (Solo Leveling)\n${DIVIDER}\n`;
  t += (SL_HELP_TEXT || '(keine Daten verfügbar)')
    .split('\n')
    .filter(Boolean)
    .map(l => l.replace(/\{P\}/g, PREFIX))
    .join('\n');
  t += `\n\n🌑 _"Arise." — Nur wahre Hunter überleben die Gates._`;
  return t;
}

function buildSocialLayer(ctx) {
  const { PREFIX } = ctx;
  let t = `💞 *SOCIAL SKILLS* (Interaktion)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}slap @user — Ohrfeige verpassen\n`;
  t += `▸ ${PREFIX}hug @user — Umarmen\n`;
  t += `▸ ${PREFIX}kiss @user — Küssen\n`;
  t += `▸ ${PREFIX}pat @user — Tätscheln\n`;
  t += `▸ ${PREFIX}poke @user — Anpiksen\n`;
  t += `▸ ${PREFIX}cuddle @user — Kuscheln\n`;
  t += `▸ ${PREFIX}bite @user — Beißen\n`;
  t += `▸ ${PREFIX}punch @user — Schlagen\n`;
  t += `▸ ${PREFIX}love @user — Lieben\n`;
  t += `▸ ${PREFIX}blush @user — Erröten wegen jemandem\n`;
  t += `▸ ${PREFIX}handhold @user — Hand halten\n`;
  t += `▸ ${PREFIX}lick @user — Ablecken\n`;
  t += `▸ ${PREFIX}nervous @user — Nervös wegen jemandem sein\n`;
  t += `▸ ${PREFIX}throw @user — Werfen\n`;
  t += `▸ ${PREFIX}sleep @user — Einschlafen neben\n`;
  t += `▸ ${PREFIX}angrystare @user — Wütend anstarren\n`;
  t += `▸ ${PREFIX}bleh @user — Zunge rausstrecken\n`;
  t += `▸ ${PREFIX}confused @user — Verwirrt sein wegen\n`;
  t += `▸ ${PREFIX}cry @user — Weinen wegen\n`;
  t += `▸ ${PREFIX}evillaugh @user — Böse lachen\n`;
  t += `▸ ${PREFIX}facepalm @user — Facepalm machen\n`;
  t += `▸ ${PREFIX}happy @user — Glücklich sein wegen\n`;
  t += `▸ ${PREFIX}laugh @user — Lachen\n`;
  t += `▸ ${PREFIX}mad @user — Sauer sein wegen\n`;
  t += `▸ ${PREFIX}nuzzle @user — Anschmiegen\n`;
  t += `▸ ${PREFIX}no @user — Nein sagen zu\n`;
  t += `▸ ${PREFIX}nosebleed @user — Nasenbluten wegen\n`;
  t += `▸ ${PREFIX}sad @user — Traurig sein wegen\n`;
  t += `▸ ${PREFIX}scared @user — Angst haben wegen\n`;
  t += `▸ ${PREFIX}shout @user — Anschreien\n`;
  t += `▸ ${PREFIX}shy @user — Schüchtern sein wegen\n`;
  t += `▸ ${PREFIX}sneeze @user — Niesen\n`;
  t += `▸ ${PREFIX}surprised @user — Überrascht sein wegen\n`;
  t += `▸ ${PREFIX}tired @user — Müde sein wegen\n`;
  t += `▸ ${PREFIX}yes @user — Ja sagen zu\n`;
  return t;
}

function buildFunLayer(ctx) {
  const { PREFIX } = ctx;
  let t = `💀 *FUN & ACTION SKILLS* (Giphy-Reactions)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}kill @user — Erledigen\n`;
  t += `▸ ${PREFIX}yeet @user — Yeeten\n`;
  t += `▸ ${PREFIX}nuke @user — Nuken\n`;
  t += `▸ ${PREFIX}banish @user — Verbannen\n`;
  t += `▸ ${PREFIX}stab @user — Durchbohren\n`;
  t += `▸ ${PREFIX}smash @user — Zerschmettern\n`;
  t += `▸ ${PREFIX}vaporize @user — Pulverisieren\n`;
  t += `▸ ${PREFIX}choke @user — Würgen\n`;
  t += `▸ ${PREFIX}kick @user — Treten\n`;
  t += `▸ ${PREFIX}spin @user — Herumwirbeln\n`;
  t += `▸ ${PREFIX}glare @user — Böse anstarren\n`;
  t += `▸ ${PREFIX}smirk @user — Süffisant grinsen\n`;
  t += `▸ ${PREFIX}highfive @user — High Five geben\n`;
  t += `▸ ${PREFIX}dance @user — Zusammen tanzen\n`;
  return t;
}

function buildChatLayer(ctx) {
  const { PREFIX } = ctx;
  let t = `💬 *GILDEN-CHAT* (Chat & Gruppen)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}gi — Gildeneinstellungen anzeigen\n`;
  t += `▸ ${PREFIX}welcome-an / -aus — Willkommens-Portal an/aus\n`;
  t += `▸ ${PREFIX}welcome-set <text> — Willkommenstext setzen\n`;
  t += `▸ ${PREFIX}antilink-an / -aus — Anti-Fremdportal-Bann an/aus\n`;
  t += `▸ ${PREFIX}hidetag <text> — Nachricht mit verstecktem Tag\n`;
  t += `▸ ${PREFIX}delete — Als Reply: Nachricht löschen\n`;
  t += `▸ ${PREFIX}ytmp3 <link> — YouTube als MP3\n`;
  t += `▸ ${PREFIX}nachtsperre an <HH:MM> <HH:MM> — Zeitgesteuerte Gildensperre\n`;
  t += `▸ ${PREFIX}nachtsperre aus / status — Sperre verwalten\n`;
  t += `\n⚙️ *Aktueller System-Befehl:* ${PREFIX}`;
  return t;
}

function buildSupportLayer(ctx) {
  const { PREFIX, sender, isAuthorized } = ctx;
  if (!isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) {
    return null;
  }
  let t = `🎫 *KNIGHTS OF THE BLOOD SUPPORT* (Ticket-System)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}support <nachricht> — Notfall-Ticket erstellen\n`;
  t += `▸ ${PREFIX}answer <id> <text> — Ticket beantworten\n`;
  t += `▸ ${PREFIX}tickets [id|status] — Tickets anzeigen\n`;
  t += `▸ ${PREFIX}cleartickets — Alle Tickets löschen\n`;
  return t;
}

function buildAdminLayer(ctx) {
  const { PREFIX } = ctx;
  let t = `🛡️ *GILDENMEISTER* (Admin)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}warn @user — Verwarnen\n`;
  t += `▸ ${PREFIX}kick @user — Aus der Gilde werfen\n`;
  t += `▸ ${PREFIX}promote / ${PREFIX}demote @user — Gildenadmin-Rechte\n`;
  t += `▸ ${PREFIX}addxp <@user> <menge> — EXP schenken\n`;
  t += `▸ ${PREFIX}addcash <@user> <menge> — Cor schenken\n`;
  t += `▸ ${PREFIX}addvip <@user> <zeit> — VIP-Rang geben\n`;
  t += `▸ ${PREFIX}purge [anzahl] — Nachrichten löschen (alle oder letzte Nachrichten)\n`;
  return t;
}

function buildOwnerLayer(ctx) {
  const { PREFIX, sender, hasAdminPerms } = ctx;
  if (!hasAdminPerms(sender)) {
    return null;
  }
  let t = `👑 *SYSTEM ADMINISTRATOR* (Kayaba-Rechte)\n${DIVIDER}\n`;
  t += `▸ ${PREFIX}broadcast <text> — Serverweite Ansage an alle Gilden\n`;
  t += `▸ ${PREFIX}restart — System neu starten\n`;
  t += `▸ ${PREFIX}updateprofile — Avatar aktualisieren\n`;
  t += `▸ ${PREFIX}bancmd <befehl> [ban|unban] — Skill sperren\n`;
  t += `▸ ${PREFIX}bancmds — Gesperrte Skills anzeigen\n`;
  t += `▸ ${PREFIX}setrole @user <rolle> — Rang setzen\n`;
  t += `▸ ${PREFIX}listroles — Alle Ränge anzeigen\n`;
  t += `▸ ${PREFIX}newsession <name> — Neuen Server starten\n`;
  t += `▸ ${PREFIX}sessions — Aktive Server anzeigen\n`;
  t += `▸ ${PREFIX}stopsession <name> — Server stoppen\n`;
  t += `▸ ${PREFIX}deletesession <name> — Server löschen\n`;
  t += `▸ ${PREFIX}addcredit Name | Rolle — Beta-Tester hinzufügen\n`;
  t += `▸ ${PREFIX}delcredit <nummer> — Beta-Tester entfernen\n`;
  t += `▸ ${PREFIX}com <link> — Gilden-Link ändern\n`;
  t += `▸ ${PREFIX}usertodo — Von Spielern vorgeschlagene Skills ansehen\n`;
  t += `▸ ${PREFIX}resetcoins <@user> — Coins zurücksetzen\n`;
  t += `▸ ${PREFIX}resetlevel <@user> — Level & XP zurücksetzen\n`;
  return t;
}

const LAYERS = {
  main:        { build: buildMainLayer,        aliases: [] },
  system:      { build: buildMainLayer,        aliases: ['start', 'basis'] },
  arena:       { build: buildArenaLayer,       aliases: ['wirtschaft', 'economy'] },
  gilde:       { build: buildGuildLayer,       aliases: ['guild', 'gilden'] },
  titel:       { build: buildTitleLayer,       aliases: ['titles', 'achievements', 'erfolge'] },
  pokemon:     { build: buildPokemonLayer,     aliases: ['poke', 'pokedex'] },
  demonslayer: { build: buildDemonSlayerLayer, aliases: ['dämonentöter', 'daemonslayer', 'ds', 'atmung'] },
  hunter:      { build: buildSoloLevelingLayer, aliases: ['solo', 'sololeveling', 'jaeger', 'hunter-system'] },
  social:      { build: buildSocialLayer,      aliases: ['interaktion', 'interaction'] },
  fun:         { build: buildFunLayer,         aliases: ['action', 'giphy'] },
  chat:        { build: buildChatLayer,        aliases: ['gruppe', 'group', 'gilden-chat'] },
  support:     { build: buildSupportLayer,     aliases: ['ticket', 'tickets'] },
  admin:       { build: buildAdminLayer,       aliases: ['gildenmeister', 'mod'] },
  owner:       { build: buildOwnerLayer,       aliases: ['kayaba', 'sysadmin', 'system-admin'] }
};

function resolveLayerKey(input) {
  const key = (input || '').toLowerCase().trim();
  if (!key) return 'main';
  if (LAYERS[key]) return key;
  for (const [layerKey, info] of Object.entries(LAYERS)) {
    if (info.aliases.includes(key)) return layerKey;
  }
  return null;
}

function buildNavRows(ctx) {
  const { activePrefix, PREFIX, sender, isAuthorized, hasAdminPerms } = ctx;
  const prefix = activePrefix || PREFIX;

  const LABELS = {
    main:        { title: '🔷 Hauptmenü',     desc: 'Übersicht & Basis-Befehle' },
    arena:       { title: '⚔️ Arena',          desc: 'Ausrüstung, PVP & Wirtschaft' },
    gilde:       { title: '🏰 Gilde',          desc: 'Verbünde & Gildensystem' },
    titel:       { title: '🎖️ Titel',          desc: 'Titel & Erfolge' },
    pokemon:     { title: '🐾 Pokémon',        desc: 'Fangen, Leveln, Kämpfen' },
    demonslayer: { title: '👹 Dämonentöter',   desc: 'Atemstile & Dämonen-Boss' },
    hunter:      { title: '⚡ Hunter',          desc: 'Solo-Leveling System' },
    social:      { title: '💞 Social',         desc: 'Interaktions-Skills' },
    fun:         { title: '💀 Fun & Action',   desc: 'Giphy-Reactions' },
    chat:        { title: '💬 Gilden-Chat',    desc: 'Chat- & Gruppeneinstellungen' },
    support:     { title: '🎫 Support',        desc: 'Ticket-System (Team)' },
    admin:       { title: '🛡️ Gildenmeister',  desc: 'Admin-Befehle' },
    owner:       { title: '👑 System-Admin',    desc: 'Kayaba-Rechte' }
  };

  const order = ['main', 'arena', 'gilde', 'titel', 'pokemon', 'demonslayer', 'hunter', 'social', 'fun', 'chat', 'support', 'admin', 'owner'];
  const rows = [];

  for (const key of order) {
    if (key === 'support' && !isAuthorized(sender, ['OWNER', 'COOWNER', 'MOD', 'SUPPORTER', 'TEST_SUPPORTER'])) continue;
    if (key === 'owner' && !hasAdminPerms(sender)) continue;
    const info = LABELS[key];
    rows.push({
      title: info.title,
      description: info.desc,
      rowId: key === 'main' ? `${prefix}menu` : `${prefix}menu ${key}`
    });
  }

  return rows;
}

export function createMenuSystem() {
  function buildMenuText(ctx) {
    const requested = (ctx.args && ctx.args[0]) || '';
    const layerKey = resolveLayerKey(requested);

    if (!layerKey) {
      const available = Object.keys(LAYERS).filter(k => k !== 'system').join(', ');
      return (
        `❌ Unbekannte Menü-Ebene "${requested}".\n\n` +
        `Verfügbare Ebenen: ${available}\n` +
        `Beispiel: ${ctx.activePrefix || ctx.PREFIX}menu pokemon`
      );
    }

    const result = LAYERS[layerKey].build(ctx);
    if (result === null) {
      return '❌ Du hast keinen Zugriff auf diese Menü-Ebene.';
    }
    return result;
  }

  return { buildMenuText, buildNavRows };
}