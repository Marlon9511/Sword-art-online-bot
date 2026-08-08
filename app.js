// app.js — AINCRAD Web-Terminal
// Spricht mit den Endpunkten aus web-auth.js / web-games.js / web-owner.js.

const API_BASE = ''; // gleiche Origin wie die Seite; bei getrenntem Server: 'http://DEIN-SERVER:3001'

const loginScreen = document.getElementById('loginScreen');
const hubScreen = document.getElementById('hubScreen');
const loginForm = document.getElementById('loginForm');
const loginError = document.getElementById('loginError');

let token = localStorage.getItem('aincrad_token') || null;

function authHeaders() {
  return token ? { 'Authorization': `Bearer ${token}` } : {};
}

async function apiPost(path, body) {
  const res = await fetch(API_BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(body || {})
  });
  return res.json();
}
async function apiGet(path) {
  const res = await fetch(API_BASE + path, { headers: authHeaders() });
  return res.json();
}

// ---------------- LOGIN ----------------
loginForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  loginError.hidden = true;
  const id = document.getElementById('loginId').value.trim().toUpperCase();
  const password = document.getElementById('loginPassword').value;

  const data = await apiPost('/api/login', { id, password });
  if (!data.success) {
    loginError.textContent = data.error || 'Login fehlgeschlagen.';
    loginError.hidden = false;
    return;
  }
  token = data.token;
  localStorage.setItem('aincrad_token', token);
  enterHub();
});

document.getElementById('logoutBtn').addEventListener('click', () => {
  token = null;
  localStorage.removeItem('aincrad_token');
  hubScreen.hidden = true;
  loginScreen.hidden = false;
});

async function tryAutoLogin() {
  if (!token) return;
  const data = await apiGet('/api/me');
  if (data.success) enterHub();
  else { token = null; localStorage.removeItem('aincrad_token'); }
}

// ---------------- HUB / HUD ----------------
async function enterHub() {
  loginScreen.hidden = true;
  hubScreen.hidden = false;
  await refreshHud();
  await checkOwnerAccess();
}

function applyStats(s) {
  document.getElementById('hudName').textContent = s.name || 'Unbekannt';
  document.getElementById('hudLevel').textContent = s.level ?? 1;
  document.getElementById('hudCoins').textContent = s.coins ?? 0;
  const needed = 100 + (s.level || 1) * 50;
  const pct = Math.min(100, Math.round(((s.xp || 0) / needed) * 100));
  document.getElementById('xpFill').style.width = pct + '%';
}

async function refreshHud() {
  const data = await apiGet('/api/me');
  if (data.success) applyStats(data);
}

// ---------------- TABS ----------------
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.game-panel').forEach(p => p.hidden = true);
    btn.classList.add('active');
    document.querySelector(`.game-panel[data-panel="${btn.dataset.game}"]`).hidden = false;
    if (btn.dataset.game === 'owner') loadOwnerData();
  });
});

// ---------------- SLOTS ----------------
document.getElementById('slotSpin').addEventListener('click', async () => {
  const bet = parseInt(document.getElementById('slotBet').value) || 50;
  const resultEl = document.getElementById('slotResult');
  resultEl.textContent = '...';
  const data = await apiPost('/api/games/slot', { bet });
  if (!data.success) { resultEl.textContent = '⚠ ' + data.error; return; }

  ['reel0', 'reel1', 'reel2'].forEach((id, i) => {
    const el = document.getElementById(id);
    el.classList.add('spin');
    setTimeout(() => { el.textContent = data.spin[i]; el.classList.remove('spin'); }, 200 + i * 120);
  });

  setTimeout(() => {
    resultEl.textContent = data.win ? `🎉 JACKPOT! +${bet * 3} Coins` : `😢 Verloren -${bet} Coins`;
    applyStats(data);
  }, 600);
});

// ---------------- BLACKJACK ----------------
const CARD_RED = ['♥', '♦'];
function renderHand(containerId, cards) {
  const el = document.getElementById(containerId);
  el.innerHTML = '';
  cards.forEach(c => {
    const div = document.createElement('div');
    div.className = 'card' + (CARD_RED.includes(c.suit) ? ' red' : '');
    div.textContent = c.value + c.suit;
    el.appendChild(div);
  });
}

const bjStart = document.getElementById('bjStart');
const bjHit = document.getElementById('bjHit');
const bjStand = document.getElementById('bjStand');
const bjResult = document.getElementById('bjResult');

bjStart.addEventListener('click', async () => {
  bjResult.textContent = '';
  const data = await apiPost('/api/games/blackjack/start');
  if (!data.success) { bjResult.textContent = '⚠ ' + data.error; return; }
  renderHand('bjPlayerHand', data.player);
  renderHand('bjDealerHand', [data.dealerUpcard]);
  document.getElementById('bjPlayerScore').textContent = `(${data.playerScore})`;
  document.getElementById('bjDealerScore').textContent = '';
  bjHit.disabled = false; bjStand.disabled = false; bjStart.disabled = true;
});

bjHit.addEventListener('click', async () => {
  const data = await apiPost('/api/games/blackjack/hit');
  if (!data.success) { bjResult.textContent = '⚠ ' + data.error; return; }
  renderHand('bjPlayerHand', data.player);
  document.getElementById('bjPlayerScore').textContent = `(${data.score})`;
  if (data.status !== 'playing') {
    bjResult.textContent = data.status === 'bust' ? '💥 Bust! Verloren.' : '🎉 Blackjack! +75 Coins';
    bjHit.disabled = true; bjStand.disabled = true; bjStart.disabled = false;
    applyStats(data);
  }
});

bjStand.addEventListener('click', async () => {
  const data = await apiPost('/api/games/blackjack/stand');
  if (!data.success) { bjResult.textContent = '⚠ ' + data.error; return; }
  renderHand('bjDealerHand', data.dealer);
  document.getElementById('bjDealerScore').textContent = `(${data.dealerScore})`;
  const msg = { win: '🎉 Du gewinnst! +75 Coins', lose: '😢 Dealer gewinnt.', draw: '🤝 Unentschieden.' };
  bjResult.textContent = msg[data.result];
  bjHit.disabled = true; bjStand.disabled = true; bjStart.disabled = false;
  applyStats(data);
});

// ---------------- RPS ----------------
document.querySelectorAll('.rps-btn').forEach(btn => {
  btn.addEventListener('click', async () => {
    const resultEl = document.getElementById('rpsResult');
    resultEl.textContent = '...';
    const data = await apiPost('/api/games/rps', { choice: btn.dataset.choice });
    if (!data.success) { resultEl.textContent = '⚠ ' + data.error; return; }
    const labels = { rock: 'Stein', paper: 'Papier', scissors: 'Schere' };
    const msg = { win: `🎉 Gewonnen! (Bot: ${labels[data.botChoice]}) +50 Coins`,
                  lose: `😢 Verloren (Bot: ${labels[data.botChoice]}) -20 Coins`,
                  draw: `🤝 Unentschieden (Bot: ${labels[data.botChoice]})` };
    resultEl.textContent = msg[data.result];
    applyStats(data);
  });
});

// ---------------- OWNER ----------------
const ownerTabBtn = document.getElementById('ownerTabBtn');
const ownerOverviewEl = document.getElementById('ownerOverview');
const ownerUserBody = document.getElementById('ownerUserBody');
const ownerActionResult = document.getElementById('ownerActionResult');
const RANKS = ['OWNER', 'COOWNER', 'ADMIN', 'MOD', 'VIP', 'SUPPORTER', 'TEST_SUPPORTER', 'USER'];

async function checkOwnerAccess() {
  const data = await apiGet('/api/owner/overview');
  if (data.success && data.isOwner) {
    ownerTabBtn.hidden = false;
  }
}

async function loadOwnerData() {
  const overview = await apiGet('/api/owner/overview');
  if (overview.success) {
    ownerOverviewEl.innerHTML = `
      <div><b>${overview.totalUsers}</b>Nutzer gesamt</div>
      <div><b>${overview.registeredUsers}</b>Registriert</div>
      <div><b>${overview.bannedUsers}</b>Gebannt</div>
      <div><b>${overview.activeSessions.length}</b>Aktive Sessions</div>
    `;
  }

  const usersData = await apiGet('/api/owner/users');
  if (!usersData.success) {
    ownerUserBody.innerHTML = `<tr><td colspan="5">⚠ ${usersData.error}</td></tr>`;
    return;
  }
  renderOwnerTable(usersData.users);
}

function renderOwnerTable(users) {
  ownerUserBody.innerHTML = '';
  users.forEach(u => {
    const tr = document.createElement('tr');

    const rankOptions = RANKS.map(r => `<option value="${r}" ${r === u.rank ? 'selected' : ''}>${r}</option>`).join('');

    tr.innerHTML = `
      <td>${u.name || u.jid.split('@')[0]}${u.banned ? ' 🚫' : ''}</td>
      <td><select data-jid="${u.jid}" class="rankSelect">${rankOptions}</select></td>
      <td>${u.level}</td>
      <td>${u.coins}</td>
      <td>
        <div class="owner-row-actions">
          <button class="owner-mini-btn coinsBtn" data-jid="${u.jid}" data-amount="100">+100💰</button>
          <button class="owner-mini-btn xpBtn" data-jid="${u.jid}" data-amount="100">+100✨</button>
          ${u.banned
            ? `<button class="owner-mini-btn unbanBtn" data-jid="${u.jid}">Entbannen</button>`
            : `<button class="owner-mini-btn danger banBtn" data-jid="${u.jid}">Bannen</button>`}
        </div>
      </td>
    `;
    ownerUserBody.appendChild(tr);
  });

  // Rang ändern
  document.querySelectorAll('.rankSelect').forEach(sel => {
    sel.addEventListener('change', async () => {
      const data = await apiPost('/api/owner/rank', { jid: sel.dataset.jid, rank: sel.value });
      ownerActionResult.textContent = data.success ? `✅ Rang gesetzt: ${sel.value}` : `⚠ ${data.error}`;
    });
  });

  // Coins geben
  document.querySelectorAll('.coinsBtn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const data = await apiPost('/api/owner/coins', { jid: btn.dataset.jid, amount: parseInt(btn.dataset.amount) });
      if (data.success) { ownerActionResult.textContent = `✅ Coins aktualisiert: ${data.coins}`; loadOwnerData(); }
      else ownerActionResult.textContent = `⚠ ${data.error}`;
    });
  });

  // XP geben
  document.querySelectorAll('.xpBtn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const data = await apiPost('/api/owner/xp', { jid: btn.dataset.jid, amount: parseInt(btn.dataset.amount) });
      if (data.success) { ownerActionResult.textContent = `✅ XP aktualisiert: ${data.xp}`; loadOwnerData(); }
      else ownerActionResult.textContent = `⚠ ${data.error}`;
    });
  });

  // Bannen
  document.querySelectorAll('.banBtn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const reason = prompt('Grund für den Bann (optional):') || '';
      const data = await apiPost('/api/owner/ban', { jid: btn.dataset.jid, reason });
      if (data.success) { ownerActionResult.textContent = '✅ Gebannt.'; loadOwnerData(); }
      else ownerActionResult.textContent = `⚠ ${data.error}`;
    });
  });

  // Entbannen
  document.querySelectorAll('.unbanBtn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const data = await apiPost('/api/owner/unban', { jid: btn.dataset.jid });
      if (data.success) { ownerActionResult.textContent = '✅ Entbannt.'; loadOwnerData(); }
      else ownerActionResult.textContent = `⚠ ${data.error}`;
    });
  });
}

document.getElementById('broadcastSend').addEventListener('click', async () => {
  const text = document.getElementById('broadcastText').value.trim();
  const resultEl = document.getElementById('broadcastResult');
  if (!text) { resultEl.textContent = '⚠ Bitte Text eingeben.'; return; }
  resultEl.textContent = '⏳ Sende...';
  const data = await apiPost('/api/owner/broadcast', { text });
  resultEl.textContent = data.success
    ? `✅ An ${data.sentTo}/${data.totalGroups} Gruppen gesendet.`
    : `⚠ ${data.error}`;
});

tryAutoLogin();
