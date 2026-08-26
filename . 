import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState
} from '@whiskeysockets/baileys'

import { Boom } from '@hapi/boom'
import qrcode from 'qrcode-terminal'

async function startBot() {
  const { state, saveCreds } =
    await useMultiFileAuthState('./auth_info_baileys')

  const sock = makeWASocket({
    auth: state,
    markOnlineOnConnect: false
  })

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update

    if (qr) {
      console.log('Scanne diesen QR-Code mit WhatsApp:')
      qrcode.generate(qr, { small: true })
    }

    if (connection === 'open') {
      console.log('✅ WhatsApp Bot erfolgreich verbunden!')
    }

    if (connection === 'close') {
      const statusCode =
        lastDisconnect?.error instanceof Boom
          ? lastDisconnect.error.output.statusCode
          : 0

      const reconnect =
        statusCode !== DisconnectReason.loggedOut

      console.log('❌ Verbindung geschlossen.')

      if (reconnect) {
        console.log('🔄 Verbinde erneut...')
        startBot()
      } else {
        console.log('⚠️ Du wurdest ausgeloggt.')
      }
    }
  })

  sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const message of messages) {
      if (!message.message) continue
      if (message.key.fromMe) continue

      const text =
        message.message.conversation ||
        message.message.extendedTextMessage?.text ||
        ''

      console.log('📩 Nachricht:', text)

      if (text.trim().toLowerCase() === '?ping') {
        await sock.sendMessage(message.key.remoteJid, {
          text: '🏓 Pong!'
        })
      }
    }
  })
}

startBot()