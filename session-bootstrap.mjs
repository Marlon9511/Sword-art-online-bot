import express from 'express'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState
} from '@neelify/baileys'
import QRCode from 'qrcode'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const PORT = Number(process.env.QR_PORT || 4010)
const SESSION_ID = (process.env.QR_SESSION || 'DarkBotWeb').trim()
const AUTH_DIR = path.join(__dirname, 'wa_credentials', `${SESSION_ID}_credentials`)
fs.mkdirSync(AUTH_DIR, { recursive: true })

function depVersion(name) {
  try {
    const pkg = JSON.parse(fs.readFileSync(path.resolve('./package.json'), 'utf-8'))
    const v = pkg?.dependencies?.[name] || 'unbekannt'
    return String(v).replace(/^[~^]/, '')
  } catch {
    return 'unbekannt'
  }
}

const versions = {
  baileys: depVersion('@neelify/baileys'),
  libsignal: depVersion('@neelify/libsignal'),
  waApi: depVersion('@neelify/wa-api')
}

const qrState = {
  sessionId: SESSION_ID,
  connected: false,
  qrDataUrl: null,
  qrCount: 0,
  lastQrAt: 0,
  note: 'Starte Session...'
}

let sock

async function startSocket() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)
  const { version } = await fetchLatestBaileysVersion()

  sock = makeWASocket({
    auth: state,
    version,
    browser: Browsers.ubuntu('Chrome'),
    printQRInTerminal: false,
    markOnlineOnConnect: false,
    qrTimeout: 60000
  })

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update

    if (qr) {
      qrState.qrCount += 1
      qrState.connected = false
      qrState.lastQrAt = Date.now()
      qrState.note = qrState.qrCount > 1
        ? 'VORIGER CODE TERMINATED BY THE SHADOW'
        : 'QR bereit'

      qrState.qrDataUrl = await QRCode.toDataURL(qr, {
        errorCorrectionLevel: 'M',
        margin: 1,
        width: 520,
        color: { dark: '#000000', light: '#FFFFFF' }
      })
    }

    if (connection === 'open') {
      qrState.connected = true
      qrState.qrDataUrl = null
      qrState.note = '✅ Verbunden'
    }

    if (connection === 'close') {
      qrState.connected = false
      const code = lastDisconnect?.error?.output?.statusCode
      const shouldReconnect = code !== DisconnectReason.loggedOut

      qrState.note = shouldReconnect
        ? 'Verbindung getrennt – reconnect...'
        : 'Ausgeloggt (neu scannen nötig)'

      if (shouldReconnect) {
        setTimeout(() => {
          startSocket().catch((e) => {
            console.error('Reconnect-Fehler:', e?.message || e)
          })
        }, 2500)
      }
    }
  })
}

const app = express()

app.get(['/','/index'], (_req, res) => {
  res.type('html').send(`<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>DARK SHAN ECHO QR</title>
  <style>
    body{margin:0;background:#0a0a0f;color:#eee;font-family:system-ui,Segoe UI,Arial}
    .wrap{max-width:900px;margin:0 auto;padding:24px}
    .card{background:#12121a;border:1px solid #26263a;border-radius:16px;padding:18px}
    .title{font-size:1.2rem;font-weight:700;margin:0 0 10px;color:#c79bff}
    .row{display:grid;grid-template-columns:1fr 320px;gap:16px}
    .mono{white-space:pre-wrap;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#0f0f16;border-radius:12px;padding:12px;border:1px solid #202033}
    img{width:100%;max-width:320px;background:#fff;border-radius:12px;display:none}
    .ok{color:#7CFC96;font-weight:700}
    .muted{opacity:.8}
    @media (max-width:760px){.row{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1 class="title">🌑 DARK SHAN ECHO v2.0 — SHADOW LINK ESTABLISHED 🌑</h1>
      <p class="muted">Route: <strong>/</strong> oder <strong>/index</strong> · API: <strong>/api/qr</strong></p>
      <div class="row">
        <div>
          <div id="text" class="mono">Lade...</div>
        </div>
        <div>
          <img id="qrImg" alt="QR Code" />
        </div>
      </div>
    </div>
  </div>

<script>
async function refresh(){
  try{
    const r = await fetch('/api/qr', { cache: 'no-store' })
    const d = await r.json()

    const img = document.getElementById('qrImg')
    const text = document.getElementById('text')

    if(d.connected){
      img.style.display = 'none'
      text.innerHTML = '✅ <span class="ok">Session verbunden</span>\\n\\n*Session:* (' + d.sessionId + ')\\n\\nKein QR nötig.'
      return
    }

    const remain = (typeof d.remainingSec === 'number') ? d.remainingSec : 60

    const msg =
\`📷 Dein QR-Code wird aus der verborgenen Schicht gezogen...

*Session:* (\${d.sessionId})

⚠️ *VORIGER CODE TERMINATED BY THE SHADOW*
⏱️ *\${remain}s REMAINING* — scanne, bevor das Echo verstummt und ein neues erscheint.

📦 *SHADOW CORE LOADED*
* @neelify/baileys     → *\${d.versions.baileys}*
* @neelify/libsignal   → *\${d.versions.libsignal}*
* @neelify/wa-api      → *\${d.versions.waApi}*

🖤 Pure Dark Theme. Kein Licht. Nur die Linien der Schattenwelt.\`

    text.textContent = msg

    if(d.qrDataUrl){
      img.src = d.qrDataUrl
      img.style.display = 'block'
    } else {
      img.style.display = 'none'
    }
  }catch(e){
    document.getElementById('text').textContent = 'Fehler beim Laden: ' + (e?.message || e)
  }
}
refresh()
setInterval(refresh, 1500)
</script>
</body>
</html>`)
})

app.get('/api/qr', (_req, res) => {
  const remainingSec = qrState.lastQrAt
    ? Math.max(0, 60 - Math.floor((Date.now() - qrState.lastQrAt) / 1000))
    : null

  res.json({
    ok: true,
    sessionId: qrState.sessionId,
    connected: qrState.connected,
    qrDataUrl: qrState.qrDataUrl,
    qrCount: qrState.qrCount,
    note: qrState.note,
    remainingSec,
    versions
  })
})

await startSocket()

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🌌 QR-Web läuft auf http://localhost:${PORT} (oder /index)`)
})