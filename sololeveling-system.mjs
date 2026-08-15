
import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { fileURLToPath } from 'url';
import fetch from 'node-fetch';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export function createSoloLevelingSystem(DATA_PATH) {
  const FILE_PATH = path.join(DATA_PATH, 'sololeveling.json');

 
  const ARISE_SOUND_URL = 'https://raw.githubusercontent.com/Marlon9511/Sword-art-online-bot/main/AUD-20260814-WA0954.mp3'; 

  const ARISE_CACHE_DIR = path.join(__dirname, 'cache', 'sololeveling-arise');
  
  const ARISE_SOURCE_EXT = (path.extname(new URL(ARISE_SOUND_URL).pathname) || '.mp3').toLowerCase();
  const ARISE_SOURCE_PATH = path.join(ARISE_CACHE_DIR, `arise-source${ARISE_SOURCE_EXT}`);
  const ARISE_OGG_PATH = path.join(ARISE_CACHE_DIR, 'arise.ogg');

  
  async function downloadAriseSourceIfNeeded() {
    if (fs.existsSync(ARISE_SOURCE_PATH)) return ARISE_SOURCE_PATH;
    fs.mkdirSync(ARISE_CACHE_DIR, { recursive: true });

    const res = await fetch(ARISE_SOUND_URL);
    if (!res.ok) {
      throw new Error(`Arise-Sound-Download fehlgeschlagen: HTTP ${res.status} (${ARISE_SOUND_URL})`);
    }
    const arrBuf = await res.arrayBuffer();
    fs.writeFileSync(ARISE_SOURCE_PATH, Buffer.from(arrBuf));

    if (!fs.existsSync(ARISE_SOURCE_PATH) || fs.statSync(ARISE_SOURCE_PATH).size === 0) {
      throw new Error('Heruntergeladene Arise-Datei ist leer oder fehlt.');
    }
    return ARISE_SOURCE_PATH;
  }

  function convertToOggOpusIfNeeded(sourcePath) {
    return new Promise((resolve, reject) => {
      if (fs.existsSync(ARISE_OGG_PATH)) return resolve(ARISE_OGG_PATH);
      
      const cmd = `ffmpeg -y -i "${sourcePath}" -c:a libopus -b:a 64k -vn "${ARISE_OGG_PATH}"`;
      exec(cmd, { maxBuffer: 1024 * 1024 * 50 }, (err) => {
        if (err) return reject(err);
        if (!fs.existsSync(ARISE_OGG_PATH)) return reject(new Error('Keine OGG-Datei erzeugt.'));
        resolve(ARISE_OGG_PATH);
      });
    });
  }

  async function getAriseVoiceBuffer() {
    const source = await downloadAriseSourceIfNeeded();
    const ogg = await convertToOggOpusIfNeeded(source);
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

  
  function getSettings() {
    if (!hunters._settings) hunters._settings = { noCooldownGroups: {} };
    if (!hunters._settings.noCooldownGroups) hunters._settings.noCooldownGroups = {};
    return hunters._settings;
  }

  function isGroupCooldownDisabled(groupJid) {
    return !!getSettings().noCooldownGroups[groupJid];
  }

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

 

  function baseGateTierIndexForLevel(level) {
    let idx = 0;
    GATE_TIERS.forEach((g, i) => {
      if (level >= g.minLevel) idx = i;
    });
    return idx;
  }

  
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

  // ---------- Waffen ----------
  
  const SHOP_WEAPONS = [
    { key: 'rostiges_schwert', name: 'Rostiges Schwert', price: 50, power: 8, rarity: 'Gewöhnlich' },
    { key: 'stahlschwert', name: 'Stahlschwert', price: 150, power: 18, rarity: 'Gewöhnlich' },
    { key: 'kriegsaxt', name: 'Kriegsaxt', price: 320, power: 30, rarity: 'Selten' },
    { key: 'enchantierter_dolch', name: 'Enchantierter Dolch', price: 600, power: 45, rarity: 'Selten' },
    { key: 'runenschwert', name: 'Runenschwert', price: 1000, power: 65, rarity: 'Episch' }
  ];

  
  const RARE_WEAPONS = [
    { key: 'daemonenklinge', name: 'Dämonenklinge', price: 800, power: 90, rarity: 'Episch' },
    { key: 'frostfangschwert', name: 'Frostfangschwert', price: 1200, power: 120, rarity: 'Episch' },
    { key: 'drachentoeter', name: 'Drachentöter', price: 2000, power: 160, rarity: 'Legendär' },
    { key: 'klinge_des_monarchen', name: 'Klinge des Monarchen', price: 3500, power: 220, rarity: 'Legendär' }
  ];

  function findWeaponDef(key) {
    return SHOP_WEAPONS.find(w => w.key === key) || RARE_WEAPONS.find(w => w.key === key) || null;
  }

  function equippedWeaponPower(h) {
    if (!h.equippedWeaponKey) return 0;
    const owned = (h.weapons || []).find(w => w.key === h.equippedWeaponKey);
    return owned ? owned.power : 0;
  }

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
        pendingBlackMarket: null, // { offers: [weaponKey,...], expiresAt }
        weapons: [],
        equippedWeaponKey: null,
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

  
  function combatPower(h) {
    const s = h.stats;
    const shadowPower = shadowArmyPower(h);
    const weaponPower = equippedWeaponPower(h);
    return Math.round(
      s.str * 2.2 + s.agi * 1.6 + s.vit * 1.4 + s.int * 1.1 + s.per * 1.0 + h.level * 3 + shadowPower * 0.8 + weaponPower
    );
  }

  
  function gateEffectivePower(h) {
    const s = h.stats;
    const basePower = s.str * 2.2 + s.agi * 1.6 + s.vit * 1.4 + s.int * 1.1 + s.per * 1.0 + h.level * 3;
    const shadowPower = shadowArmyPower(h);
    
    const shadowContribution = Math.sqrt(Math.max(0, shadowPower)) * 6;
    const weaponPower = equippedWeaponPower(h);
    return { basePower, shadowPower, shadowContribution, weaponPower, total: basePower + shadowContribution + weaponPower };
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
      cmd, args, sender, from, isGroup, send, sock, users, ensureUser, normalizeJid,
      getNumberMention, randInt, isPrimaryOwner
    } = ctx;

    if (!SL_COMMANDS.includes(cmd)) return false;

    const jid = normalizeJid(sender);
    if (ensureUser) ensureUser(jid);
    const ownerBypass = typeof isPrimaryOwner === 'function' && isPrimaryOwner(jid);
    const groupBypass = !!(isGroup && from && isGroupCooldownDisabled(from));
    const cooldownBypass = ownerBypass || groupBypass;

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

    // ---- SLCOOLDOWN — Gate-Cooldown für diese Gruppe an/aus (nur Owner) ----
    if (cmd === 'slcooldown' || cmd === 'gatecooldown') {
      if (!isGroup) {
        await send('❌ Dieser Befehl funktioniert nur innerhalb einer Gruppe.');
        return true;
      }
      if (!ownerBypass) {
        await send('❌ Nur der Bot-Inhaber darf den Gate-Cooldown für eine Gruppe umschalten.');
        return true;
      }

      const sub = (args[0] || '').toLowerCase();
      const settings = getSettings();

      if (!sub || sub === 'status') {
        const active = isGroupCooldownDisabled(from);
        await send(
          `⏳ Gate-Cooldown in dieser Gruppe: ${active ? 'AUS ❌ (kein Cooldown für alle Hunter hier)' : 'AN ✅ (normaler Cooldown)'}\n` +
          `Umschalten: ?slcooldown an | ?slcooldown aus`
        );
        return true;
      }

      if (sub === 'aus' || sub === 'off') {
        settings.noCooldownGroups[from] = true;
        persist();
        await send('✅ Gate-Cooldown für diese Gruppe DEAKTIVIERT. Alle Hunter hier können ?gate ohne Wartezeit nutzen.');
        return true;
      }

      if (sub === 'an' || sub === 'on') {
        delete settings.noCooldownGroups[from];
        persist();
        await send('✅ Gate-Cooldown für diese Gruppe wieder AKTIVIERT (normale Wartezeit gilt wieder).');
        return true;
      }

      await send('❌ Nutzung: ?slcooldown an | aus | status');
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
      const equippedDef = h.equippedWeaponKey ? findWeaponDef(h.equippedWeaponKey) : null;
      const weaponLine = equippedDef
        ? `🗡️ Waffe: ${equippedDef.name} (⚔️ +${equippedDef.power}, ${equippedDef.rarity})`
        : `🗡️ Waffe: keine ausgerüstet`;
      await send(
        `📊 *— SYSTEM-FENSTER —* 📊\n${divider}\n` +
        `🎖️ Rang: ${rank.label}\n⭐ Level: ${h.level}\n✨ EXP: ${h.exp} / ${needed}\n` +
        `⚔️ Kampfkraft: ${combatPower(h)}\n🎯 Freie Statuspunkte: ${h.statPoints}\n💰 Gold: ${h.gold || 0}\n\n` +
        `*Attribute:*\n${statLines}\n\n` +
        `👥 Schatten-Armee: ${(h.shadows || []).length} (⚔️ ${shadowArmyPower(h)} Rohkraft)\n` +
        `${weaponLine}\n` +
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
      if (!cooldownBypass && now < (h.gateCooldownUntil || 0)) {
        await send(`⏳ Das nächste Gate öffnet sich in ${fmtDuration(h.gateCooldownUntil - now)}.`);
        return true;
      }

      const rollRed = h.level >= RED_GATE_MIN_LEVEL && Math.random() < RED_GATE_CHANCE;

      // ================= ROTES TOR =================
      if (rollRed) {
        const { total: cp, shadowContribution, weaponPower } = gateEffectivePower(h);
        const boss = RED_GATE_BOSSES[randInt(0, RED_GATE_BOSSES.length - 1)];
        const difficulty = 140 + (baseGateTierIndexForLevel(h.level) * 30) + randInt(-10, 25);

        let out = `🔴 *— EIN ROTES TOR ÖFFNET SICH! —* 🔴\n${divider}\n`;
        out += `Der Boden unter dir bricht weg... du wirst in eine isolierte, blutrote Dimension gesogen!\n`;
        out += `Erst wenn *${boss}* fällt, öffnet sich der Ausgang wieder.\n\n`;

        const shadowLine = shadowContribution > 0
          ? `🌑 Deine Schatten-Armee kämpft an deiner Seite! (+${Math.round(shadowContribution)} Kampfkraft)\n`
          : '';
        out += shadowLine;
        if (weaponPower > 0) {
          out += `🗡️ Deine Waffe verstärkt deinen Angriff! (+${weaponPower} Kampfkraft)\n`;
        }

        const winChance = Math.min(0.7, Math.max(0.08, cp / (cp + difficulty)));
        const won = Math.random() < winChance;

        if (!won) {
          if (!cooldownBypass) h.gateCooldownUntil = now + RED_GATE_COOLDOWN_MS;
          const goldLoss = randInt(60, 150);
          h.gold = Math.max(0, (h.gold || 0) - goldLoss);
          persist();
          out += `\n💀 *${boss}* überwältigt dich! Du entkommst nur knapp, bevor sich das Tor schließt.\n`;
          out += `📉 -${goldLoss} Gold verloren. Das nächste Gate braucht länger, um sich zu öffnen (${fmtDuration(RED_GATE_COOLDOWN_MS)}).\n${divider}`;
          await send(out);
          return true;
        }

        if (!cooldownBypass) h.gateCooldownUntil = now + GATE_COOLDOWN_MS;
        const expGain = randInt(120, 220);
        const goldGain = randInt(250, 500);
        const leveledUp = addExp(h, expGain);
        h.gold = (h.gold || 0) + goldGain;
        h.pendingExtraction = { bossName: boss, tier: 'Rot', expiresAt: now + EXTRACT_WINDOW_MS };

        
        const shuffled = [...RARE_WEAPONS].sort(() => Math.random() - 0.5);
        const offerKeys = shuffled.slice(0, 2).map(w => w.key);
        h.pendingBlackMarket = { offers: offerKeys, expiresAt: now + EXTRACT_WINDOW_MS };

        out += `\n🏆 *${boss}* fällt! Das Rote Tor bricht zusammen und gibt dich frei.\n`;
        out += `✨ +${expGain} EXP  💰 +${goldGain} Gold\n`;
        out += `🌑 Ein besonders mächtiger Schatten hat sich vom Boss gelöst... Nutze *?extract* innerhalb von 5 Minuten!\n`;
        out += `🕶️ Ein Schwarzhändler taucht kurz aus dem Schatten auf und bietet dir seltene Waffen an — nutze *?huntershop* innerhalb von 5 Minuten!\n`;

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
      const { total: cp, shadowContribution, weaponPower } = gateEffectivePower(h);
      const tierIdx = GATE_TIERS.indexOf(tier);
      const difficulty = 40 + (tierIdx * 25) + randInt(-15, 15);
      const monster = tier.monsters[randInt(0, tier.monsters.length - 1)];

      let out = `🚪 *— ${tier.label} BETRETEN —* 🚪\n${divider}\n`;
      if (wasBoosted) {
        out += `⚠️ Dieses Gate ist stärker, als dein Level eigentlich hergibt!\n`;
      }
      out += `Du dringst tiefer in den Nebel vor... ein *${monster}* stellt sich dir in den Weg!\n\n`;
      if (shadowContribution > 0) {
        out += `🌑 Deine Schatten unterstützen dich im Kampf! (+${Math.round(shadowContribution)} Kampfkraft)\n`;
      }
      if (weaponPower > 0) {
        out += `🗡️ Deine Waffe verstärkt deinen Angriff! (+${weaponPower} Kampfkraft)\n`;
      }
      out += `\n`;

      const winChance = Math.min(0.92, Math.max(0.15, cp / (cp + difficulty)));
      const won = Math.random() < winChance;

      if (!cooldownBypass) h.gateCooldownUntil = now + GATE_COOLDOWN_MS;

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

    // ---- SHOP (Waffenladen + ggf. Schwarzhändler) ----
    if (cmd === 'huntershop' || cmd === 'waffenladen') {
      const lines = SHOP_WEAPONS.map(w =>
        `• ${w.name} (${w.rarity}) — ⚔️ +${w.power} — 💰 ${w.price} — Code: \`${w.key}\``
      );
      let out = `🛒 *— WAFFENLADEN —* 🛒\n${divider}\n${lines.join('\n')}\n${divider}\n`;
      out += `Kaufen: ?buyweapon <code>\nAusrüsten: ?hunterequip <code>\n`;

      const bm = h.pendingBlackMarket;
      if (bm && Date.now() <= bm.expiresAt) {
        const bmLines = bm.offers
          .map(k => RARE_WEAPONS.find(w => w.key === k))
          .filter(Boolean)
          .map(w => `• ${w.name} (${w.rarity}) — ⚔️ +${w.power} — 💰 ${w.price} — Code: \`${w.key}\``);
        out += `\n🕶️ *SCHWARZHÄNDLER (nur jetzt verfügbar!)*\n${bmLines.join('\n')}\n`;
        out += `Noch ${fmtDuration(bm.expiresAt - Date.now())} verfügbar!\n`;
      }

      await send(out);
      return true;
    }

    // ---- BUY WEAPON ----
    if (cmd === 'buyweapon' || cmd === 'waffekaufen' || cmd === 'kaufen') {
      const key = (args[0] || '').toLowerCase();
      if (!key) {
        await send('❌ Nutzung: ?buyweapon <code> — siehe ?huntershop für verfügbare Waffen.');
        return true;
      }

     
      let def = SHOP_WEAPONS.find(w => w.key === key);
      let isRareBuy = false;

      // Sonst prüfen, ob es ein aktives Schwarzhändler-Angebot ist.
      if (!def) {
        const bm = h.pendingBlackMarket;
        if (bm && Date.now() <= bm.expiresAt && bm.offers.includes(key)) {
          def = RARE_WEAPONS.find(w => w.key === key);
          isRareBuy = true;
        }
      }

      if (!def) {
        await send('❌ Diese Waffe ist gerade nicht erhältlich. Seltene Waffen gibt es nur kurz nach einem gewonnenen 🔴 Roten Tor beim Schwarzhändler.');
        return true;
      }

      if ((h.weapons || []).some(w => w.key === def.key)) {
        await send(`❌ Du besitzt *${def.name}* bereits.`);
        return true;
      }

      if ((h.gold || 0) < def.price) {
        await send(`❌ Nicht genug Gold. *${def.name}* kostet 💰 ${def.price}, du hast 💰 ${h.gold || 0}.`);
        return true;
      }

      h.gold -= def.price;
      h.weapons = h.weapons || [];
      h.weapons.push({ key: def.key, name: def.name, power: def.power, rarity: def.rarity });

      if (isRareBuy && h.pendingBlackMarket) {
        h.pendingBlackMarket.offers = h.pendingBlackMarket.offers.filter(k => k !== def.key);
        if (!h.pendingBlackMarket.offers.length) h.pendingBlackMarket = null;
      }

      persist();
      await send(
        `✅ *${def.name}* (${def.rarity}) gekauft! ⚔️ +${def.power} Kampfkraft, wenn ausgerüstet.\n` +
        `Nutze ?hunterequip ${def.key}, um sie auszurüsten.`
      );
      return true;
    }

    // ---- WEAPONS INVENTORY ----
    if (cmd === 'weapons' || cmd === 'waffen' || cmd === 'inventar') {
      if (!h.weapons || !h.weapons.length) {
        await send('🗡️ Du besitzt noch keine Waffen. Schau im ?huntershop vorbei.');
        return true;
      }
      const lines = h.weapons.map(w => {
        const eq = h.equippedWeaponKey === w.key ? ' ✅ (ausgerüstet)' : '';
        return `• ${w.name} (${w.rarity}) — ⚔️ +${w.power} — Code: \`${w.key}\`${eq}`;
      });
      await send(`🗡️ *— DEINE WAFFEN —* 🗡️\n${divider}\n${lines.join('\n')}\n${divider}\nAusrüsten: ?hunterequip <code>`);
      return true;
    }

    // ---- EQUIP WEAPON ----
    if (cmd === 'hunterequip' || cmd === 'ausruesten') {
      const key = (args[0] || '').toLowerCase();
      const owned = (h.weapons || []).find(w => w.key === key);
      if (!owned) {
        await send('❌ Diese Waffe besitzt du nicht. Nutze ?weapons für deine Übersicht.');
        return true;
      }
      h.equippedWeaponKey = owned.key;
      persist();
      await send(`✅ *${owned.name}* ausgerüstet! ⚔️ +${owned.power} Kampfkraft.`);
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
      const entries = Object.entries(hunters)
        .filter(([hjid]) => !hjid.startsWith('_'))
        .sort((a, b) => {
          const cpA = combatPower(a[1]);
          const cpB = combatPower(b[1]);
          return cpB - cpA;
        }).slice(0, 10);

      if (!entries.length) {
        await send('📊 Es gibt noch keine erwachten Hunter.');
        return true;
      }

      const medals = ['🥇', '🥈', '🥉'];
      const lines = entries.map(([hjid, hu], i) => {
        const u = users?.[hjid];
        const name = u?.name || u?.registrationName || hjid.split('@')[0];
        const rank = rankForLevel(hu.level);
        const icon = medals[i] || `${i + 1}.`;
        return `${icon} ${name} — ${rank.label} | Lv.${hu.level} | ⚔️ ${combatPower(hu)}`;
      });

      await send(`🏆 *— HUNTER-RANGLISTE —* 🏆\n${divider}\n${lines.join('\n')}\n${divider}`);
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
    'huntershop', 'waffenladen',
    'buyweapon', 'waffekaufen', 'kaufen',
    'weapons', 'waffen', 'inventar',
    'hunterequip', 'ausruesten',
    'dailyquest', 'tagesquest',
    'hunterrank', 'hunterleaderboard', 'jaegerrangliste',
    'slcooldown', 'gatecooldown',
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
    `?huntershop — Waffenladen ansehen (+ Schwarzhändler, falls aktiv)\n` +
    `?buyweapon <code> — Waffe kaufen\n` +
    `?weapons — deine Waffen ansehen\n` +
    `?hunterequip <code> — Waffe ausrüsten\n` +
    `?dailyquest — tägliche Systemaufgabe (Vorsicht bei Strafquests!)\n` +
    `?hunterrank — Hunter-Rangliste nach Kampfkraft\n` +
    `?slcooldown an|aus|status — Gate-Cooldown für die Gruppe umschalten (nur Owner)\n` +
    `${divider}\n` +
    `🌑 Deine Schatten-Armee kämpft bei jedem Gate aktiv mit und erhöht deine Sieg-Chance.\n` +
    `🗡️ Waffen aus dem Laden erhöhen deine Kampfkraft dauerhaft, wenn ausgerüstet.\n` +
    `⚠️ Gelegentlich erscheinen Gates, die stärker sind als dein Level – riskant, aber lohnend.\n` +
    `🔴 Ab Level ${RED_GATE_MIN_LEVEL} kann selten ein Rotes Tor auftauchen: hohes Risiko, hohe Belohnung, garantierter Boss —\n` +
    `   und ein Schwarzhändler mit seltenen Waffen, die es sonst nirgendwo zu kaufen gibt!\n` +
    `${divider}`;

  return { handle, SL_COMMANDS, SL_HELP_TEXT };
}
