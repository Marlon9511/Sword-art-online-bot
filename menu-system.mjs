// HELP / MENU
if (cmd === 'help' || cmd === 'menu') {
  const helpText = menuSystem.buildMenuText({
    args, sender, activePrefix, PREFIX,
    isAuthorized, hasAdminPerms,
    ARENA_HELP_TEXT, GUILD_HELP_TEXT, TITLE_HELP_TEXT, POKEMON_HELP_TEXT
  });

  // 1) Menü-Video + Text wie gewohnt senden
  try {
    const videoPath = await downloadShortIfNeeded();
    await sock.sendMessage(from, {
      video: fs.readFileSync(videoPath),
      caption: helpText,
      mimetype: 'video/mp4'
    }, { quoted: m });
  } catch (e) {
    console.error('Video send failed, fallback to text:', e);
    await sock.sendMessage(from, { text: helpText }, { quoted: m });
  }

  // 2) Zusätzlich: Navigations-Buttons zum Wechseln zwischen den Menü-Ebenen
  try {
    const rows = menuSystem.buildNavRows({ activePrefix, PREFIX, sender, isAuthorized, hasAdminPerms });
    if (rows.length) {
      await sock.sendMessage(from, {
        text: '📚 Wähle eine Menü-Ebene:',
        footer: '⚔️ AINCRAD System',
        title: '🗡️ Schnellzugriff',
        buttonText: '📋 Menü-Ebenen öffnen',
        sections: [{ title: 'Verfügbare Ebenen', rows }]
      });
    }
  } catch (e) {
    console.error('[menu] Buttons konnten nicht gesendet werden:', e?.message || e);
  }
  return;
}