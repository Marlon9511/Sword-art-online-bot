// yugioh/duel/DuelSimulator.mjs
import { getCardById } from '../cardApi.mjs';

const STARTING_LP = 8000;
const STARTING_HAND_SIZE = 5;

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

class Player {
  constructor(userId, name, deckList) {
    this.userId = userId;
    this.name = name;
    this.lp = STARTING_LP;
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

class Duel {
  constructor(duelId, player1, player2) {
    this.duelId = duelId;
    this.players = [player1, player2];
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
    for (const p of this.players) p.draw(STARTING_HAND_SIZE);
    this._log(`🎬 Duell gestartet! ${this.players[0].name} vs ${this.players[1].name}`);
    this._log(`${this.current.name} ist am Zug (Zug 1).`);
    return this.summarize();
  }

  drawPhase() {
    if (this.finished) return this.summarize();
    const p = this.current;
    const drawn = p.draw(1);
    if (p.deckedOut) return this._endDuel(this.opponent, `${p.name} hat keine Karten mehr im Deck (Decked Out)!`) && this.summarize();
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
    const fullCard = await getCardById(cardStub.id);
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
    return true;
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

/**
 * Factory: braucht den deckManager (für getMainDeckAsCardList).
 * Verwaltet aktive Duelle in-memory, Key = z.B. deine `from`-Variable (Chat-JID).
 */
export function createDuelSystem(deckManager) {
  const activeDuels = new Map();

  async function createDuel(duelId, user1, user2) {
    const list1 = deckManager.getMainDeckAsCardList(user1.userId, user1.deckName);
    const list2 = deckManager.getMainDeckAsCardList(user2.userId, user2.deckName);

    if (list1.length < 40) return { ok: false, message: `❌ ${user1.name}s Deck "${user1.deckName}" hat weniger als 40 Karten.` };
    if (list2.length < 40) return { ok: false, message: `❌ ${user2.name}s Deck "${user2.deckName}" hat weniger als 40 Karten.` };

    const p1 = new Player(user1.userId, user1.name, list1);
    const p2 = new Player(user2.userId, user2.name, list2);
    const duel = new Duel(duelId, p1, p2);
    duel.start();

    activeDuels.set(duelId, duel);
    return { ok: true, message: duel.summarize() };
  }

  const getDuel = (duelId) => activeDuels.get(duelId) || null;
  const endDuelSession = (duelId) => activeDuels.delete(duelId);

  return { createDuel, getDuel, endDuelSession };
}
