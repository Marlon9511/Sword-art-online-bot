// ============================================================
// sololeveling-system.mjs
// Hunter-/Gate-/Schatten-Armee-System (inspiriert von "Solo Leveling")
// Folgt dem selben Muster wie arena-system.mjs / guildboss-event.mjs:
//   createSoloLevelingSystem(DATA_PATH) -> { handle, SL_COMMANDS, SL_HELP_TEXT }
// Persistiert eigenständig in DATA_PATH/sololeveling.json
//
// NEU (diese Version):
//   - Schatten-Armee zählt jetzt sichtbar & spürbar im Gate-Kampf mit
//     (eigener Bonus auf Sieg-Chance, nicht nur auf die Kampfkraft-Zahl).
//   - Es können jetzt auch höhere Gates auftauchen, als das eigene Level
//     eigentlich hergibt ("Überraschungs-Gate") – riskanter, aber lohnender.
//   - Neu: 🔴 Rotes Tor (Red Gate) – seltenes, isoliertes Spezial-Gate mit
//     deutlich höherem Risiko & höherer Belohnung, garantiertem Boss und
//     Gefahr, "eingeschlossen" zu werden, wenn man verliert.
//
// Bei erfolgreicher Schatten-Extraktion (?extract / ?arise) wird
// zusätzlich zur Textnachricht ein fester YouTube-Sound als
// WhatsApp-Sprachnachricht (PTT) verschickt.
// ============================================================

import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export function createSoloLevelingSystem(DATA_PATH) {
  const FILE_PATH = path.join(DATA_PATH, 'sololeveling.json');

  // ---------- ARISE-Sound (fester YouTube-Link -> Sprachnachricht) ----------
  // ⚠️ Hier deinen gewünschten YouTube-Link eintragen (z.B. der "Arise"-Soundeffekt).
  const ARISE_SOUND_URL = 'https://youtube.com/shorts/-q-Ig29sxMA?is=zTlJNUzj2L5Alrmu'; // <-- ANPASSEN

  const ARISE_CACHE_DIR = path.join(__dirname, 'cache', 'sololeveling-arise');
  const ARISE_MP3_PATH = path.join(ARISE_CACHE_DIR, 'arise.mp3');
  const ARISE_OGG_PATH = path.join(ARISE_CACHE_DIR, 'arise.ogg');

  function downloadAriseMp3IfNeeded() {
    return new Promise((resolve, reject) => {
      if (fs.existsSync(ARISE_MP3_PATH)) return resolve(ARISE_MP3_PATH);
      fs.mkdirSync(ARISE_CACHE_DIR, { recursive: true });

      const cmd = `yt-dlp -x --audio-format mp3 --audio-quality 0 --no-playlist -o "${ARISE_MP3_PATH}" "${ARISE_SOUND_URL}"`;
      exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
        if (err) return reject(err);
        if (!fs.existsSync(ARISE_MP3_PATH)) return reject(new Error('Keine MP3-Datei erzeugt.'));
        resolve(ARISE_MP3_PATH);
      });
    });
  }

  function convertToOggOpusIfNeeded(mp3Path) {
    return new Promise((resolve, reject) => {
      if (fs.existsSync(ARISE_OGG_PATH)) return resolve(ARISE_OGG_PATH);
      // -c:a libopus + .ogg-Container = das Format, das WhatsApp für
      // "echte" Sprachnachrichten (ptt: true) mit Wellenform-Anzeige erwartet.
      const cmd = `ffmpeg -y -i "${mp3Path}" -c:a libopus -b:a 64k -vn "${ARISE_OGG_PATH}"`;
      exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
        if (err) return reject(err);
        if (!fs.existsSync(ARISE_OGG_PATH)) return reject(new Error('Keine OGG-Datei erzeugt.'));
        resolve(ARISE_OGG_PATH);
      });
    });
  }

  async function getAriseVoiceBuffer() {
    const mp3 = await downloadAriseMp3IfNeeded();
    const ogg = await convertToOggOpusIfNeeded(mp3);
    return fs.readFileSync(ogg);
  }

  function ensureFile() {
    if (!fs.existsSync(FILE_PATH)) {
      fs.writeFileSync(FILE_PATH, JSON.stringify({}, null, 2));
    }
  }
  ensureFile();

  function loadHunters() {
    try {
      return JSON.parse(fs.readFileSync(FILE_PATH, 'utf-8') || '{}');
    } catch (e) {
      return {};
    }
  }
  function saveHunters(data) {
    try {
      fs.writeFileSync(FILE_PATH, JSON.stringify(data, null, 2));
    } catch (e) {
      console.error('[sololeveling] Speichern fehlgeschlagen:', e?.message || e);
    }
  }

  let hunters = loadHunters();
  const persist = () => saveHunters(hunters);
  setInterval(persist, 60_000);

  // ---------- Konfiguration ----------

  const RANKS = [
    { key: 'E', label: 'E-Rang', minLevel: 1 },
    { key: 'D', label: 'D-Rang', minLevel: 11 },
    { key: 'C', label: 'C-Rang', minLevel: 21 },
    { key: 'B', label: 'B-Rang', minLevel: 36 },
    { key: 'A', label: 'A-Rang', minLevel: 51 },
    { key: 'S', label: 'S-Rang', minLevel: 71 },
    { key: 'National', label: '🏆 Level-Nationaler Hunter', minLevel: 100 }
  ];

  function rankForLevel(level) {
    let cur = RANKS[0];
    for (const r of RANKS) {
      if (level >= r.minLevel) cur = r;
    }
    return cur;
  }

  const GATE_TIERS = [
    { key: 'E', label: 'E-Rang Gate', minLevel: 1, maxLevel: 15, monsters: ['Höhlenratte', 'Schleim-Kobold', 'Verwesender Gänger'], boss: 'Gate-Wächter (klein)' },
    { key: 'D', label: 'D-Rang Gate', minLevel: 8, maxLevel: 25, monsters: ['Klingenspinne', 'Steingolem', 'Nebelschleicher'], boss: 'Höhlentroll' },
    { key: 'C', label: 'C-Rang Gate', minLevel: 18, maxLevel: 40, monsters: ['Orkkrieger', 'Knochenritter', 'Giftechse'], boss: 'Oger-Häuptling' },
    { key: 'B', label: 'B-Rang Gate', minLevel: 32, maxLevel: 55, monsters: ['Eisdrache (jung)', 'Dämonensoldat', 'Blutwolf'], boss: 'Gefallener Paladin' },
    { key: 'A', label: 'A-Rang Gate', minLevel: 48, maxLevel: 75, monsters: ['Todesritter', 'Chimäre', 'Schattenassassine'], boss: 'Erzdämon' },
    { key: 'S', label: 'S-Rang Gate', minLevel: 68, maxLevel: 999, monsters: ['Drachenritter', 'Titan der Tiefe', 'Alptraumherold'], boss: 'Monarch der Verwüstung' }
  ];

  // Chance, dass pro ?gate-Versuch ein Tier über dem eigentlich für das
  // Level vorgesehenen Gate erscheint ("Überraschungs-Gate"). Wird pro
  // Stufe erneut gewürfelt, kann sich also theoretisch mehrfach hochschaukeln.
  const HIGHER_GATE_CHANCE = 0.16;

  function baseGateTierIndexForLevel(level) {
    let idx = 0;
    GATE_TIERS.forEach((g, i) => {
      if (level >= g.minLevel) idx = i;
    });
    return idx;
  }

  // Wählt das Gate für diesen Versuch: Grundlage ist das Level, aber mit
  // steigender (aber abnehmender) Wahrscheinlichkeit kann auch ein höheres,
  // eigentlich noch "zu starkes" Gate auftauchen.
  function pickGateTier(level, randFn) {
    let idx = baseGateTierIndexForLevel(level);
    while (idx < GATE_TIERS.length - 1 && randFn() < HIGHER_GATE_CHANCE) {
      idx += 1;
    }
    return { tier: GATE_TIERS[idx], tierIndex: idx, wasBoosted: idx > baseGateTierIndexForLevel(level) };
  }

  function gateTierForLevel(level) {
    return GATE_TIERS[baseGateTierIndexForLevel(level)];
  }

  const SHADOW_NAMES = [
    { name: 'Schatten des Kriegers', power: 12 },
    { name: 'Schatten der Klingentänzerin', power: 15 },
    { name: 'Schatten des Steinwächters', power: 18 },
    { name: 'Schatten des Bogenschützen', power: 14 },
    { name: 'Schatten des Blutritters', power: 22 },
    { name: 'Schatten der Nachtjägerin', power: 20 },
    { name: 'Schatten des Frostgenerals', power: 28 },
    { name: 'Schatten des Feuerbestien-Königs', power: 32 },
    { name: 'Schatten des Abgrundfürsten', power: 40 },
    { name: 'Schatten des Erzengels', power: 55 }
  ];

  const STAT_KEYS = ['str', 'agi', 'vit', 'int', 'per'];
  const STAT_LABELS = { str: '💪 Stärke', agi: '💨 Beweglichkeit', vit: '❤️ Vitalität', int: '🧠 Intelligenz', per: '👁️ Wahrnehmung' };

  const GATE_COOLDOWN_MS = 10 * 60 * 1000; // 10 Minuten
  const RED_GATE_COOLDOWN_MS = 25 * 60 * 1000; // Rotes Tor: längere Sperre bei Niederlage
  const DAILY_QUEST_COOLDOWN_MS = 24 * 60 * 60 * 1000;
  const EXTRACT_WINDOW_MS = 5 * 60 * 1000; // 5 Minuten nach Bosskill

  // Rotes Tor: seltenes Spezial-Gate, ab Level 5 möglich.
  const RED_GATE_MIN_LEVEL = 5;
  const RED_GATE_CHANCE = 0.06; // 6% pro ?gate-Versuch
  const RED_GATE_BOSSES = ['Blutmond-Ogerfürst', 'Verschlungener Wächter', 'Herold des Roten Tores', 'Abyssaler Flammenkoloss'];

  // ---------- Helper ----------

  function ensureHunter(jid) {
    if (!hunters[jid]) {
      hunters[jid] = {
        awakened: true,
        level: 1,
        exp: 0,
        statPoints: 5,
        stats: { str: 5, agi: 5, vit: 5, int: 5, per: 5 },
        shadows: [],
        gateCooldownUntil: 0,
        dailyQuest: { lastDone: 0, penaltyUntil: 0 },
        pendingExtraction: null, // { bossName, tier, expiresAt }
        gold: 0
      };
      persist();
    }
    return hunters[jid];
  }

  function expNeeded(level) {
    return 100 + level * 40;
  }

  function addExp(h, amount) {
    h.exp += amount;
    let leveledUp = false;
    while (h.exp >= expNeeded(h.level)) {
      h.exp -= expNeeded(h.level);
      h.level += 1;
      h.statPoints += 3;
      leveledUp = true;
    }
    return leveledUp;
  }

  function shadowArmyPower(h) {
    return (h.shadows || []).reduce((sum, sh) => sum + sh.power, 0);
  }

  // "Rohe" Kampfkraft-Anzeige (Stats + Level + Schatten), wie gehabt.
  function combatPower(h) {
    const s = h.stats;
    const shadowPower = shadowArmyPower(h);
    return Math.round(
      s.str * 2.2 + s.agi * 1.6 + s.vit * 1.4 + s.int * 1.1 + s.per * 1.0 + h.level * 3 + shadowPower * 0.8
    );
  }

  // Kampfkraft für Gate-Kämpfe: Basis-Stats/Level zählen normal, die
  // Schatten-Armee steuert zusätzlich einen eigenen, spürbaren Kampfanteil
  // bei (deine Diener kämpfen aktiv mit), statt nur die Anzeigezahl zu heben.
  function gateEffectivePower(h) {
    const s = h.stats;
    const basePower = s.str * 2.2 + s.agi * 1.6 + s.vit * 1.4 + s.int * 1.1 + s.per * 1.0 + h.level * 3;
    const shadowPower = shadowArmyPower(h);
    // Diminishing Returns, damit eine riesige Armee ein Gate nicht komplett trivialisiert.
    const shadowContribution = Math.sqrt(Math.max(0, shadowPower)) * 6;
    return { basePower, shadowPower, shadowContribution, total: basePower + shadowContribution };
  }

  function isInPenalty(h) {
    return h.dailyQuest?.penaltyUntil && Date.now() < h.dailyQuest.penaltyUntil;
  }

  function fmtDuration(ms) {
    if (ms <= 0) return '0s';
    const s = Math.ceil(ms / 1000);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${sec}s`;
    return `${sec}s`;
  }

  const divider = '┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈';

  // ---------- Handler ----------

  async function handle(ctx) {
    const {
      cmd, args, sender, from, send, sock, users, ensureUser, normalizeJid,
      getNumberMention, randInt
    } = ctx;

    if (!SL_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    if (ensureUser) ensureUser(jid);

    // ---- AWAKEN ----
    if (cmd === 'awaken' || cmd === 'erwachen') {
      if (hunters[jid]) {
        await send('⚡ Du bist bereits als Hunter erwacht. Nutze ?hunterinfo für deinen Status.');
        return true;
      }
      ensureHunter(jid);
      persist();
      await send(
        `⚡ *— DAS SYSTEM HAT DICH ERWÄHLT —* ⚡\n${divider}\n` +
        `Eine Stimme hallt in deinem Kopf wider...\n\n` +
        `"Herzlichen Glückwunsch. Du wurdest als Spieler ausgewählt."\n\n` +
        `🎖️ Rang: E-Rang Hunter\n⭐ Level: 1\n🎯 5 freie Statuspunkte warten auf dich.\n\n` +
        `Nutze ?statpoint <str|agi|vit|int|per> <anzahl>, um sie zu verteilen,\n` +
        `?gate um dein erstes Gate zu betreten,\n` +
        `und ?dailyquest für deine tägliche Systemaufgabe.\n${divider}`
      );
      return true;
    }

    const h = hunters[jid];
    if (!h) {
      await send('❓ Du bist noch kein Hunter. Erwache zuerst mit ?awaken.');
      return true;
    }

    // ---- HUNTERINFO / STATUS ----
    if (cmd === 'hunterinfo' || cmd === 'hunterstatus' || cmd === 'systemwindow') {
      const rank = rankForLevel(h.level);
      const needed = expNeeded(h.level);
      const statLines = STAT_KEYS.map(k => `${STAT_LABELS[k]}: ${h.stats[k]}`).join('\n');
      const penaltyLine = isInPenalty(h)
        ? `\n☠️ *Strafquest aktiv!* Noch ${fmtDuration(h.dailyQuest.penaltyUntil - Date.now())}`
        : '';
      await send(
        `📊 *— SYSTEM-FENSTER —* 📊\n${divider}\n` +
        `🎖️ Rang: ${rank.label}\n⭐ Level: ${h.level}\n✨ EXP: ${h.exp} / ${needed}\n` +
        `⚔️ Kampfkraft: ${combatPower(h)}\n🎯 Freie Statuspunkte: ${h.statPoints}\n💰 Gold: ${h.gold || 0}\n\n` +
        `*Attribute:*\n${statLines}\n\n` +
        `👥 Schatten-Armee: ${(h.shadows || []).length} (⚔️ ${shadowArmyPower(h)} Rohkraft)\n` +
        `${penaltyLine}\n${divider}`
      );
      return true;
    }

    // ---- STATPOINT ----
    if (cmd === 'statpoint' || cmd === 'sp') {
      const stat = (args[0] || '').toLowerCase();
      const amount = parseInt(args[1]);
      if (!STAT_KEYS.includes(stat) || isNaN(amount) || amount <= 0) {
        await send(
          `❌ Nutzung: ?statpoint <str|agi|vit|int|per> <anzahl>\n` +
          `Verfügbare Punkte: ${h.statPoints}\n` +
          `str = Stärke, agi = Beweglichkeit, vit = Vitalität, int = Intelligenz, per = Wahrnehmung`
        );
        return true;
      }
      if (amount > h.statPoints) {
        await send(`❌ Du hast nur ${h.statPoints} freie Statuspunkte.`);
        return true;
      }
      h.stats[stat] += amount;
      h.statPoints -= amount;
      persist();
      await send(`✅ ${STAT_LABELS[stat]} um ${amount} erhöht! Neuer Wert: ${h.stats[stat]}\nVerbleibende Punkte: ${h.statPoints}`);
      return true;
    }

    // ---- GATE ----
    if (cmd === 'gate' || cmd === 'dungeon') {
      const now = Date.now();
      if (now < (h.gateCooldownUntil || 0)) {
        await send(`⏳ Das nächste Gate öffnet sich in ${fmtDuration(h.gateCooldownUntil - now)}.`);
        return true;
      }

      const rollRed = h.level >= RED_GATE_MIN_LEVEL && Math.random() < RED_GATE_CHANCE;

      // ================= ROTES TOR =================
      if (rollRed) {
        const { total: cp, shadowContribution } = gateEffectivePower(h);
        const boss = RED_GATE_BOSSES[randInt(0, RED_GATE_BOSSES.length - 1)];
        const difficulty = 140 + (baseGateTierIndexForLevel(h.level) * 30) + randInt(-10, 25);

        let out = `🔴 *— EIN ROTES TOR ÖFFNET SICH! —* 🔴\n${divider}\n`;
        out += `Der Boden unter dir bricht weg... du wirst in eine isolierte, blutrote Dimension gesogen!\n`;
        out += `Erst wenn *${boss}* fällt, öffnet sich der Ausgang wieder.\n\n`;

        const shadowLine = shadowContribution > 0
          ? `🌑 Deine Schatten-Armee kämpft an deiner Seite! (+${Math.round(shadowContribution)} Kampfkraft)\n`
          : '';
        out += shadowLine;

        const winChance = Math.min(0.7, Math.max(0.08, cp / (cp + difficulty)));
        const won = Math.random() < winChance;

        if (!won) {
          h.gateCooldownUntil = now + RED_GATE_COOLDOWN_MS;
          const goldLoss = randInt(60, 150);
          h.gold = Math.max(0, (h.gold || 0) - goldLoss);
          persist();
          out += `\n💀 *${boss}* überwältigt dich! Du entkommst nur knapp, bevor sich das Tor schließt.\n`;
          out += `📉 -${goldLoss} Gold verloren. Das nächste Gate braucht länger, um sich zu öffnen (${fmtDuration(RED_GATE_COOLDOWN_MS)}).\n${divider}`;
          await send(out);
          return true;
        }

        h.gateCooldownUntil = now + GATE_COOLDOWN_MS;
        const expGain = randInt(120, 220);
        const goldGain = randInt(250, 500);
        const leveledUp = addExp(h, expGain);
        h.gold = (h.gold || 0) + goldGain;
        h.pendingExtraction = { bossName: boss, tier: 'Rot', expiresAt: now + EXTRACT_WINDOW_MS };

        out += `\n🏆 *${boss}* fällt! Das Rote Tor bricht zusammen und gibt dich frei.\n`;
        out += `✨ +${expGain} EXP  💰 +${goldGain} Gold\n`;
        out += `🌑 Ein besonders mächtiger Schatten hat sich vom Boss gelöst... Nutze *?extract* innerhalb von 5 Minuten!\n`;

        if (leveledUp) {
          const newRank = rankForLevel(h.level);
          out += `\n🎉 *LEVEL UP!* Du bist jetzt Level ${h.level} (${newRank.label})! +3 Statuspunkte.\n`;
        }

        persist();
        out += divider;
        await send(out);
        return true;
      }

      // ================= NORMALES GATE =================
      const { tier, wasBoosted } = pickGateTier(h.level, Math.random);
      const { total: cp, shadowContribution } = gateEffectivePower(h);
      const tierIdx = GATE_TIERS.indexOf(tier);
      const difficulty = 40 + (tierIdx * 25) + randInt(-15, 15);
      const monster = tier.monsters[randInt(0, tier.monsters.length - 1)];

      let out = `🚪 *— ${tier.label} BETRETEN —* 🚪\n${divider}\n`;
      if (wasBoosted) {
        out += `⚠️ Dieses Gate ist stärker, als dein Level eigentlich hergibt!\n`;
      }
      out += `Du dringst tiefer in den Nebel vor... ein *${monster}* stellt sich dir in den Weg!\n\n`;
      if (shadowContribution > 0) {
        out += `🌑 Deine Schatten unterstützen dich im Kampf! (+${Math.round(shadowContribution)} Kampfkraft)\n\n`;
      }

      const winChance = Math.min(0.92, Math.max(0.15, cp / (cp + difficulty)));
      const won = Math.random() < winChance;

      h.gateCooldownUntil = now + GATE_COOLDOWN_MS;

      if (!won) {
        const goldLoss = randInt(10, 40);
        h.gold = Math.max(0, (h.gold || 0) - goldLoss);
        persist();
        out += `💥 Du wurdest zurückgedrängt und musstest fliehen!\n📉 -${goldLoss} Gold verloren.\n${divider}`;
        await send(out);
        return true;
      }

      const expGain = randInt(20, 40) + tierIdx * 15;
      const goldGain = randInt(30, 90) + tierIdx * 20;
      const leveledUp = addExp(h, expGain);
      h.gold = (h.gold || 0) + goldGain;

      out += `⚔️ Der ${monster} wurde besiegt!\n✨ +${expGain} EXP  💰 +${goldGain} Gold\n`;

      // Chance auf Boss-Begegnung
      const bossChance = 0.22;
      if (Math.random() < bossChance) {
        const bossWinChance = Math.min(0.8, Math.max(0.1, cp / (cp + difficulty * 1.8)));
        const bossWon = Math.random() < bossWinChance;
        out += `\n👹 Der Gate-Boss *${tier.boss}* erscheint!\n`;
        if (bossWon) {
          const bossExp = randInt(50, 90) + tierIdx * 25;
          const bossGold = randInt(80, 200) + tierIdx * 40;
          addExp(h, bossExp);
          h.gold += bossGold;
          h.pendingExtraction = { bossName: tier.boss, tier: tier.key, expiresAt: now + EXTRACT_WINDOW_MS };
          out += `🏆 Boss besiegt! ✨ +${bossExp} EXP  💰 +${bossGold} Gold\n`;
          out += `🌑 Ein Schatten hat sich vom Boss gelöst... Nutze *?extract* innerhalb von 5 Minuten!\n`;
        } else {
          out += `💀 Der Boss war zu stark, du musstest den Rückzug antreten.\n`;
        }
      }

      if (leveledUp) {
        const newRank = rankForLevel(h.level);
        out += `\n🎉 *LEVEL UP!* Du bist jetzt Level ${h.level} (${newRank.label})! +3 Statuspunkte.\n`;
      }

      persist();
      out += divider;
      await send(out);
      return true;
    }

    // ---- EXTRACT ----
    if (cmd === 'extract' || cmd === 'arise') {
      const pending = h.pendingExtraction;
      if (!pending || Date.now() > pending.expiresAt) {
        h.pendingExtraction = null;
        persist();
        await send('❌ Es gibt derzeit keinen Schatten, den du extrahieren kannst. Besiege zuerst einen Gate-Boss.');
        return true;
      }

      const isRed = pending.tier === 'Rot';
      const extractChance = isRed ? 0.4 : 0.55;
      const success = Math.random() < extractChance;
      h.pendingExtraction = null;

      if (!success) {
        persist();
        await send(`💨 "Arise" ... nichts geschah. Der Schatten des *${pending.bossName}* ist entkommen.`);
        return true;
      }

      const template = SHADOW_NAMES[randInt(0, SHADOW_NAMES.length - 1)];
      const powerBonus = isRed ? randInt(10, 25) : randInt(0, 6);
      const shadow = {
        id: `sh_${Date.now()}_${randInt(100, 999)}`,
        name: template.name,
        power: template.power + powerBonus,
        origin: pending.bossName
      };
      h.shadows.push(shadow);
      persist();

      await send(
        `🌑 *"ARISE!"* 🌑\n${divider}\n` +
        `Aus der Dunkelheit erhebt sich: *${shadow.name}*\n` +
        `⚔️ Kraft: ${shadow.power}\nHerkunft: ${pending.bossName}\n\n` +
        `Deine Schatten-Armee zählt nun ${h.shadows.length} Diener.\n${divider}`
      );

      // ---- Sound als Sprachnachricht senden (best effort, blockiert den
      // Befehl nicht, falls Download/Konvertierung fehlschlägt) ----
      if (sock && from) {
        try {
          const voiceBuffer = await getAriseVoiceBuffer();
          await sock.sendMessage(from, {
            audio: voiceBuffer,
            mimetype: 'audio/ogg; codecs=opus',
            ptt: true
          });
        } catch (e) {
          console.error('[sololeveling] Arise-Sound fehlgeschlagen:', e?.message || e);
        }
      }

      return true;
    }

    // ---- SHADOWS LIST ----
    if (cmd === 'shadows' || cmd === 'schattenarmee') {
      if (!h.shadows.length) {
        await send('🌑 Deine Schatten-Armee ist leer. Besiege Gate-Bosse und nutze ?extract.');
        return true;
      }
      const lines = h.shadows
        .sort((a, b) => b.power - a.power)
        .map((s, i) => `${i + 1}. ${s.name} — ⚔️ ${s.power} (aus: ${s.origin})`);
      const totalPower = h.shadows.reduce((sum, s) => sum + s.power, 0);
      const { shadowContribution } = gateEffectivePower(h);
      await send(
        `🌑 *— DEINE SCHATTEN-ARMEE —* 🌑\n${divider}\n${lines.join('\n')}\n${divider}\n` +
        `Gesamtkraft der Armee: ${totalPower}\n` +
        `Bonus im Gate-Kampf: +${Math.round(shadowContribution)} Kampfkraft`
      );
      return true;
    }

    // ---- DAILY QUEST ----
    if (cmd === 'dailyquest' || cmd === 'tagesquest') {
      const now = Date.now();
      const last = h.dailyQuest?.lastDone || 0;

      if (now - last < DAILY_QUEST_COOLDOWN_MS) {
        const remaining = DAILY_QUEST_COOLDOWN_MS - (now - last);
        await send(`📜 Du hast deine heutige Systemaufgabe bereits erledigt.\nNächste in ${fmtDuration(remaining)}.`);
        return true;
      }

      // Strafe, falls die letzte Quest zu lange überfällig war (>48h seit letzter Erledigung, aber nicht beim allerersten Mal)
      if (last !== 0 && now - last > DAILY_QUEST_COOLDOWN_MS * 2) {
        h.dailyQuest.penaltyUntil = now + 60 * 60 * 1000; // 1h Debuff
        h.stats.vit = Math.max(1, h.stats.vit - 1);
        h.dailyQuest.lastDone = now;
        persist();
        await send(
          `☠️ *"Die tägliche Quest wurde nicht erfüllt."*\n${divider}\n` +
          `Das System verhängt eine Strafe: -1 Vitalität, 1h Debuff aktiv.\n` +
          `Erledige deine Quests in Zukunft pünktlich!\n${divider}`
        );
        return true;
      }

      const rewardExp = randInt(15, 30);
      const rewardStat = STAT_KEYS[randInt(0, STAT_KEYS.length - 1)];
      addExp(h, rewardExp);
      h.stats[rewardStat] += 1;
      h.dailyQuest.lastDone = now;
      persist();

      await send(
        `📜 *— TAGESQUEST ABGESCHLOSSEN —* 📜\n${divider}\n` +
        `100 Liegestütze. 100 Kniebeugen. 100 Sit-ups. 10km Lauf.\n\n` +
        `✅ Geschafft! ✨ +${rewardExp} EXP  ${STAT_LABELS[rewardStat]} +1\n${divider}`
      );
      return true;
    }

    // ---- LEADERBOARD ----
    if (cmd === 'hunterrank' || cmd === 'hunterleaderboard' || cmd === 'jaegerrangliste') {
      const entries = Object.entries(hunters).sort((a, b) => {
        const cpA = combatPower(a[1]);
        const cpB = combatPower(b[1]);
        return cpB - cpA;
      }).slice(0, 10);

      if (!entries.length) {
        await send('📊 Es gibt noch keine erwachten Hunter.');
        return true;
      }

      const medals = ['🥇', '🥈', '🥉'];
      const lines = await Promise.all(entries.map(async ([hjid, hu], i) => {
        const name = await getNumberMention(hjid, ctx.sock);
        const rank = rankForLevel(hu.level);
        const icon = medals[i] || `${i + 1}.`;
        return `${icon} ${name} — ${rank.label} | Lv.${hu.level} | ⚔️ ${combatPower(hu)}`;
      }));

      await send(
        `🏆 *— HUNTER-RANGLISTE —* 🏆\n${divider}\n${lines.join('\n')}\n${divider}`,
        { mentions: entries.map(([hjid]) => hjid) }
      );
      return true;
    }

    // ---- HELP ----
    if (cmd === 'sololevelinghelp' || cmd === 'jaegerhilfe') {
      await send(SL_HELP_TEXT);
      return true;
    }

    return false;
  }

  const SL_COMMANDS = [
    'awaken', 'erwachen',
    'hunterinfo', 'hunterstatus', 'systemwindow',
    'statpoint', 'sp',
    'gate', 'dungeon',
    'extract', 'arise',
    'shadows', 'schattenarmee',
    'dailyquest', 'tagesquest',
    'hunterrank', 'hunterleaderboard', 'jaegerrangliste',
    'sololevelinghelp', 'jaegerhilfe'
  ];

  const SL_HELP_TEXT =
    `⚡ *— HUNTER-SYSTEM (Solo Leveling) —* ⚡\n${divider}\n` +
    `?awaken — als Hunter erwachen\n` +
    `?hunterinfo — dein System-Fenster (Stats, Rang, Schatten)\n` +
    `?statpoint <str|agi|vit|int|per> <n> — Statuspunkte verteilen\n` +
    `?gate — ein Gate betreten (Kampf, Loot, Boss-Chance, seltenes 🔴 Rotes Tor)\n` +
    `?extract — Schatten eines besiegten Bosses extrahieren\n` +
    `?shadows — deine Schatten-Armee ansehen (inkl. Gate-Kampfbonus)\n` +
    `?dailyquest — tägliche Systemaufgabe (Vorsicht bei Strafquests!)\n` +
    `?hunterrank — Hunter-Rangliste nach Kampfkraft\n` +
    `${divider}\n` +
    `🌑 Deine Schatten-Armee kämpft bei jedem Gate aktiv mit und erhöht deine Sieg-Chance.\n` +
    `⚠️ Gelegentlich erscheinen Gates, die stärker sind als dein Level – riskant, aber lohnend.\n` +
    `🔴 Ab Level ${RED_GATE_MIN_LEVEL} kann selten ein Rotes Tor auftauchen: hohes Risiko, hohe Belohnung, garantierter Boss.\n` +
    `${divider}`;

  return { handle, SL_COMMANDS, SL_HELP_TEXT };
}
