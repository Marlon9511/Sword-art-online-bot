<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Aincrad // Terminal</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#050810;
    --panel:#0c1220;
    --panel-2:#0f1830;
    --line:#1c2c4a;
    --cyan:#4fd8ff;
    --cyan-dim:#1c4a63;
    --amber:#f2b544;
    --text:#dce8f5;
    --text-dim:#7288a6;
    --danger:#ff5c6c;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{
    background:radial-gradient(circle at 50% 0%, #0a1226 0%, var(--bg) 60%);
    color:var(--text);
    font-family:'Inter',sans-serif;
    min-height:100vh;
    display:flex;
    align-items:center;
    justify-content:center;
    padding:24px;
  }
  .frame{width:100%;max-width:420px}
  .brand{
    font-family:'Orbitron',sans-serif;
    font-weight:700;
    font-size:15px;
    letter-spacing:.14em;
    color:var(--cyan);
    text-align:center;
    margin-bottom:4px;
  }
  .brand span{color:var(--text-dim);font-weight:500}
  .sub{text-align:center;color:var(--text-dim);font-size:12.5px;margin-bottom:28px}

  .panel{
    background:var(--panel);
    border:1px solid var(--line);
    border-radius:2px;
    position:relative;
    padding:26px 24px;
  }
  .panel::before{
    content:"";position:absolute;top:0;left:0;width:22px;height:22px;
    border-top:2px solid var(--cyan);border-left:2px solid var(--cyan);
  }
  .panel::after{
    content:"";position:absolute;bottom:0;right:0;width:22px;height:22px;
    border-bottom:2px solid var(--cyan);border-right:2px solid var(--cyan);
  }

  label{display:block;font-size:11.5px;color:var(--text-dim);margin-bottom:6px;margin-top:16px}
  label:first-of-type{margin-top:0}
  input{
    width:100%;background:var(--panel-2);border:1px solid var(--line);
    color:var(--text);padding:11px 12px;font-size:14px;border-radius:2px;
    font-family:'Inter',sans-serif;
  }
  input:focus{outline:none;border-color:var(--cyan)}

  button{
    width:100%;margin-top:22px;padding:12px;background:var(--cyan);
    color:#04101c;border:none;font-weight:600;font-size:13.5px;
    letter-spacing:.04em;border-radius:2px;cursor:pointer;
  }
  button:hover{background:#71e2ff}
  button:disabled{opacity:.5;cursor:default}

  .error{color:var(--danger);font-size:12.5px;margin-top:14px;display:none}

  /* dashboard */
  .hidden{display:none !important}
  .hud-name{font-family:'Orbitron',sans-serif;font-size:18px;color:var(--text)}
  .hud-row{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:18px}
  .logout{background:none;width:auto;margin:0;padding:6px 10px;color:var(--text-dim);
    border:1px solid var(--line);font-size:11px;letter-spacing:.05em}
  .logout:hover{background:var(--panel-2);color:var(--text)}

  .stat-block{margin-bottom:18px}
  .stat-label{display:flex;justify-content:space-between;font-size:11.5px;color:var(--text-dim);margin-bottom:6px}
  .stat-label b{color:var(--text);font-weight:600}
  .bar{height:8px;background:var(--panel-2);border:1px solid var(--line);border-radius:2px;overflow:hidden}
  .bar-fill{height:100%;background:linear-gradient(90deg,var(--cyan-dim),var(--cyan))}

  .coins{
    display:flex;align-items:baseline;gap:8px;padding:14px 16px;
    background:var(--panel-2);border:1px solid var(--line);border-radius:2px;margin-top:8px;
  }
  .coins .num{font-family:'Orbitron',sans-serif;font-size:22px;color:var(--amber)}
  .coins .lbl{font-size:11.5px;color:var(--text-dim)}

  /* games */
  .games-panel{margin-top:16px}
  .game-card{
    background:var(--panel-2);border:1px solid var(--line);border-radius:2px;
    padding:14px 16px;margin-bottom:12px;
  }
  .game-title{font-family:'Orbitron',sans-serif;font-size:12.5px;color:var(--cyan);letter-spacing:.05em;margin-bottom:10px}
  .game-row{display:flex;gap:8px;align-items:center}
  .game-row input{flex:1}
  .game-btn{
    width:auto;margin-top:0;padding:9px 14px;font-size:12.5px;white-space:nowrap;
  }
  .ghost-btn{background:transparent;border:1px solid var(--cyan);color:var(--cyan)}
  .ghost-btn:hover{background:var(--cyan-dim)}
  .game-result{margin-top:10px;font-size:12.5px;color:var(--text-dim);min-height:1.2em}
  .game-result.ok{color:var(--cyan)}
  .game-result.bad{color:var(--danger)}
  .cards{font-family:'Orbitron',sans-serif;font-size:16px;letter-spacing:.06em;margin:8px 0}
  .shop-item{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--line);font-size:12.5px}
  .shop-item:last-child{border-bottom:none}
  .shop-item button{width:auto;margin:0;padding:6px 12px;font-size:11.5px}
  .inv-line{display:flex;justify-content:space-between;font-size:12.5px;padding:5px 0;color:var(--text-dim)}
  .inv-line b{color:var(--text)}
  .rps-choices{display:flex;gap:8px}
  .rps-choices button{flex:1;padding:10px;font-size:12px}
</style>
</head>
<body>
<div class="frame">
  <div class="brand">AINCRAD <span>// PLAYER TERMINAL</span></div>
  <div class="sub">verbindet sich mit deiner lokalen Bot-API</div>

  <div class="panel" id="loginPanel">
    <label for="idInput">Spieler-ID</label>
    <input id="idInput" autocomplete="off" placeholder="z. B. K7Q2XM" maxlength="6">
    <label for="pwInput">Passwort</label>
    <input id="pwInput" type="password" autocomplete="off">
    <button id="loginBtn">Verbinden</button>
    <div class="error" id="loginError"></div>
  </div>

  <div class="panel hidden" id="dashPanel">
    <div class="hud-row">
      <div class="hud-name" id="dName">—</div>
      <button class="logout" id="logoutBtn">TRENNEN</button>
    </div>

    <div class="stat-block">
      <div class="stat-label"><span>Level <b id="dLevel">1</b></span><span id="dXpText">0 / 0 XP</span></div>
      <div class="bar"><div class="bar-fill" id="dXpBar" style="width:0%"></div></div>
    </div>

    <div class="coins">
      <div class="num" id="dCoins">0</div>
      <div class="lbl">Coins</div>
    </div>

    <div class="games-panel">
      <div class="game-card">
        <div class="game-title">TÄGLICH & ARBEIT</div>
        <div class="game-row">
          <button class="game-btn ghost-btn" id="btnDaily">Daily</button>
          <button class="game-btn ghost-btn" id="btnWork">Work</button>
        </div>
        <div class="game-result" id="resDailyWork"></div>
      </div>

      <div class="game-card">
        <div class="game-title">SLOT</div>
        <div class="game-row">
          <input id="slotBet" type="number" value="50" min="1">
          <button class="game-btn" id="btnSlot">Drehen</button>
        </div>
        <div class="cards" id="slotSpin">— — —</div>
        <div class="game-result" id="resSlot"></div>
      </div>

      <div class="game-card">
        <div class="game-title">STEIN · PAPIER · SCHERE</div>
        <div class="rps-choices">
          <button class="ghost-btn" data-rps="rock">🪨 Stein</button>
          <button class="ghost-btn" data-rps="paper">📄 Papier</button>
          <button class="ghost-btn" data-rps="scissors">✂️ Schere</button>
        </div>
        <div class="game-result" id="resRps"></div>
      </div>

      <div class="game-card">
        <div class="game-title">BLACKJACK</div>
        <div class="cards" id="bjCards">—</div>
        <div class="game-row">
          <button class="game-btn ghost-btn" id="btnBjStart">Start</button>
          <button class="game-btn ghost-btn" id="btnBjHit">Hit</button>
          <button class="game-btn ghost-btn" id="btnBjStand">Stand</button>
        </div>
        <div class="game-result" id="resBj"></div>
      </div>

      <div class="game-card">
        <div class="game-title">ANGELN</div>
        <button class="game-btn ghost-btn" id="btnFish">Angeln</button>
        <div class="game-result" id="resFish"></div>
      </div>

      <div class="game-card">
        <div class="game-title">SHOP</div>
        <div id="shopList"></div>
      </div>

      <div class="game-card">
        <div class="game-title">INVENTAR</div>
        <div id="invList" class="game-result">—</div>
      </div>
    </div>
  </div>
</div>

<script>
const API_BASE = 'http://localhost:3001';
const loginPanel = document.getElementById('loginPanel');
const dashPanel = document.getElementById('dashPanel');
const loginError = document.getElementById('loginError');
const loginBtn = document.getElementById('loginBtn');

function showError(msg){
  loginError.textContent = msg;
  loginError.style.display = 'block';
}

async function login(){
  const id = document.getElementById('idInput').value.trim().toUpperCase();
  const password = document.getElementById('pwInput').value;
  if(!id || !password){ showError('Bitte ID und Passwort eingeben.'); return; }

  loginBtn.disabled = true;
  loginError.style.display = 'none';
  try{
    const res = await fetch(`${API_BASE}/api/login`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ id, password })
    });
    const data = await res.json();
    if(!data.success){ showError(data.error || 'Anmeldung fehlgeschlagen.'); return; }
    localStorage.setItem('aincrad_token', data.token);
    await loadDashboard();
  }catch(e){
    showError('Konnte die Bot-API auf ' + API_BASE + ' nicht erreichen. Läuft der Bot?');
  }finally{
    loginBtn.disabled = false;
  }
}

async function loadDashboard(){
  const token = localStorage.getItem('aincrad_token');
  if(!token){ return; }
  try{
    const res = await fetch(`${API_BASE}/api/me`, {
      headers:{ 'Authorization': 'Bearer ' + token }
    });
    const data = await res.json();
    if(!data.success){ localStorage.removeItem('aincrad_token'); return; }

    const level = data.level || 1;
    const xp = data.xp || 0;
    const needed = 100 + level * 50;

    document.getElementById('dName').textContent = data.name || 'Unbekannter Spieler';
    document.getElementById('dLevel').textContent = level;
    document.getElementById('dXpText').textContent = `${xp} / ${needed} XP`;
    document.getElementById('dXpBar').style.width = Math.min(100, (xp/needed)*100) + '%';
    document.getElementById('dCoins').textContent = data.coins || 0;

    loginPanel.classList.add('hidden');
    dashPanel.classList.remove('hidden');
    renderInventory(data.items || {});
    loadShop();
  }catch(e){
    showError('Verbindung zur Bot-API verloren.');
  }
}

// --- Spiele ---
function authHeaders(){
  return { 'Content-Type':'application/json', 'Authorization':'Bearer ' + localStorage.getItem('aincrad_token') };
}
function applySummary(d){
  if(d.coins===undefined) return;
  document.getElementById('dCoins').textContent = d.coins;
  document.getElementById('dLevel').textContent = d.level;
  const needed = 100 + d.level*50;
  document.getElementById('dXpText').textContent = `${d.xp} / ${needed} XP`;
  document.getElementById('dXpBar').style.width = Math.min(100,(d.xp/needed)*100) + '%';
  if(d.items) renderInventory(d.items);
}
function showResult(id, text, ok){
  const el = document.getElementById(id);
  el.textContent = text;
  el.className = 'game-result ' + (ok===true ? 'ok' : ok===false ? 'bad' : '');
}
async function callGame(path, body){
  const res = await fetch(`${API_BASE}/api/games${path}`, {
    method: body===undefined ? 'GET' : 'POST',
    headers: authHeaders(),
    body: body===undefined ? undefined : JSON.stringify(body)
  });
  return res.json();
}

document.getElementById('btnDaily').addEventListener('click', async () => {
  const d = await callGame('/daily');
  showResult('resDailyWork', d.success ? `+${d.amount} Coins` : d.error, d.success);
  applySummary(d);
});
document.getElementById('btnWork').addEventListener('click', async () => {
  const d = await callGame('/work');
  showResult('resDailyWork', d.success ? `+${d.earn} Coins` : d.error, d.success);
  applySummary(d);
});

document.getElementById('btnSlot').addEventListener('click', async () => {
  const bet = parseInt(document.getElementById('slotBet').value) || 50;
  const d = await callGame('/slot', { bet });
  if(d.spin) document.getElementById('slotSpin').textContent = d.spin.join(' ');
  showResult('resSlot', d.success ? (d.win ? `Jackpot! +${bet*3} Coins` : `Verloren -${bet} Coins`) : d.error, d.success ? d.win : false);
  applySummary(d);
});

document.querySelectorAll('[data-rps]').forEach(btn => {
  btn.addEventListener('click', async () => {
    const d = await callGame('/rps', { choice: btn.dataset.rps });
    if(!d.success){ showResult('resRps', d.error, false); return; }
    const label = { win:'Du gewinnst! +50 Coins', lose:'Du verlierst -20 Coins', draw:'Unentschieden' }[d.result];
    showResult('resRps', `Bot: ${d.botChoice} — ${label}`, d.result==='win' ? true : d.result==='lose' ? false : undefined);
    applySummary(d);
  });
});

document.getElementById('btnBjStart').addEventListener('click', async () => {
  const d = await callGame('/blackjack/start');
  if(!d.success){ showResult('resBj', d.error, false); return; }
  document.getElementById('bjCards').textContent = d.player.map(c=>c.value+c.suit).join(' ') + `  (${d.playerScore})`;
  showResult('resBj', 'Dealer zeigt: ' + d.dealerUp.value + d.dealerUp.suit);
});
document.getElementById('btnBjHit').addEventListener('click', async () => {
  const d = await callGame('/blackjack/hit');
  if(!d.success){ showResult('resBj', d.error, false); return; }
  if(d.player) document.getElementById('bjCards').textContent = d.player.map(c=>c.value+c.suit).join(' ') + `  (${d.playerScore})`;
  const msg = { bust:'Bust! Verloren.', blackjack:'21! +75 Coins +40 XP' }[d.status] || `Punkte: ${d.playerScore}`;
  showResult('resBj', msg, d.status==='blackjack' ? true : d.status==='bust' ? false : undefined);
  applySummary(d);
});
document.getElementById('btnBjStand').addEventListener('click', async () => {
  const d = await callGame('/blackjack/stand');
  if(!d.success){ showResult('resBj', d.error, false); return; }
  document.getElementById('bjCards').textContent = 'Dealer: ' + d.dealer.map(c=>c.value+c.suit).join(' ') + `  (${d.dealerScore})`;
  const msg = { win:'Du gewinnst! +75 Coins +40 XP', lose:'Dealer gewinnt', draw:'Unentschieden', bust:'Bust! Verloren.' }[d.status];
  showResult('resBj', msg, d.status==='win' ? true : (d.status==='lose'||d.status==='bust') ? false : undefined);
  applySummary(d);
});

document.getElementById('btnFish').addEventListener('click', async () => {
  const d = await callGame('/fish');
  showResult('resFish', d.success ? d.text : d.error, d.success);
  applySummary(d);
});

async function loadShop(){
  const d = await callGame('/shop');
  if(!d.success) return;
  const list = document.getElementById('shopList');
  list.innerHTML = '';
  Object.entries(d.shop).forEach(([key, item]) => {
    const row = document.createElement('div');
    row.className = 'shop-item';
    row.innerHTML = `<span>${key} — ${item.price} 💰<br><span style="color:var(--text-dim)">${item.desc}</span></span>`;
    const btn = document.createElement('button');
    btn.className = 'ghost-btn';
    btn.textContent = 'Kaufen';
    btn.addEventListener('click', async () => {
      const res = await callGame('/buy', { item: key });
      showResult('resFish', res.success ? `${key} gekauft` : res.error, res.success);
      applySummary(res);
    });
    row.appendChild(btn);
    list.appendChild(row);
  });
}

function renderInventory(items){
  const el = document.getElementById('invList');
  const entries = Object.entries(items || {}).filter(([,v]) => v > 0);
  if(!entries.length){ el.textContent = '(leer)'; return; }
  el.innerHTML = '';
  entries.forEach(([k,v]) => {
    const row = document.createElement('div');
    row.className = 'inv-line';
    row.innerHTML = `<span>${k}</span><b>${v}</b>`;
    el.appendChild(row);
  });
}

document.getElementById('loginBtn').addEventListener('click', login);
document.getElementById('pwInput').addEventListener('keydown', e => { if(e.key==='Enter') login(); });
document.getElementById('logoutBtn').addEventListener('click', () => {
  localStorage.removeItem('aincrad_token');
  dashPanel.classList.add('hidden');
  loginPanel.classList.remove('hidden');
});

loadDashboard();
</script>
</body>
</html>
