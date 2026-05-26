export function pairedFunction() {
    console.log
//=========================//
// Connect Bot + Pairing-Code
//=========================//
async function connectBot() {
    const { state, saveCreds } = await useMultiFileAuthState("./auth");

    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: "silent" })
    });

    if (!sock.authState.creds.registered) {
        let phoneNumber = await question(gradient("#ff0000", "#C00000")("📲 Deine Nummer (inkl. Ländervorwahl, z.B. +49123456789): "));
        phoneNumber = phoneNumber.replace(/[^0-9]/g, "");

        let code = await sock.requestPairingCode(phoneNumber, "AAAAAAAA");
        code = code?.match(/.{1,4}/g)?.join("-") || code;
        console.log(gradient("#ff0000", "#C00000")("🔑 Pairing Code: " + code));
    }

    sock.ev.on("connection.update", (update) => {
        const { connection } = update;
        if (connection === "close") {
            console.log(chalk.red("❌ Verbindung geschlossen, reconnect..."));
            setTimeout(connectBot, 5000);
        } else if (connection === "open") {
            console.log(chalk.green("✅ ᭙ꪖ᭢ᡶꫀᦔꪖకꪖ Verbunden mit WhatsApp!"));
            console.log(chalk.green("-----------------------------------------"));
 sock.ev.on('connection.update', async (update) => {  // ✅ Add async here {
  const { connection, qr } = update;

  if (qr) {
    console.log('📱 QR CODE:');
    if (qr) {
  try {
    const dataUrl = await QRCodeImg.toDataURL(qr, { type: 'image/png', scale: 4 });
    const base64 = dataUrl.split(',')[1];
    const qrBuffer = Buffer.from(base64, 'base64');
    await sock.sendMessage(from, {
      image: qrBuffer,
      caption: '🤖 QR-Code zum Scannen' 
  } ); 
  } catch (err) {
    console.error('QR generation error:', err);
    // Fallback to terminal
    QRCode.generate(qr, { small: true });
  }
}

  if (connection === 'open') {
    console.log('✅ Verbunden');
  }

  if (connection === 'close') {
    console.log('⚠ Verbindung geschlossen — neu verbinden in 3s');
    setTimeout(() => startBot(), 3000);
  }
}
  sock.ev.on('creds.update', saveCreds);
function pairedFunction() {
    console.log("Das pairing funktioniert!");
}

module.exports = { pairedFunction };
connectBot()
}) 