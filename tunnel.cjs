'use strict';

const localtunnel = require('localtunnel');

const PORT = process.env.TUNNEL_PORT || 5000;

(async () => {
  try {
    const tunnel = await localtunnel({ port: Number(PORT) });
    console.log(`TUNNEL_URL=${tunnel.url}`);

    tunnel.on('close', () => {
      console.log('Tunnel geschlossen.');
      process.exit(0);
    });

    tunnel.on('error', (err) => {
      console.error('Tunnel-Fehler:', err.message);
      process.exit(1);
    });
  } catch (err) {
    console.error('Konnte Tunnel nicht starten:', err.message);
    process.exit(1);
  }
})();

process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));