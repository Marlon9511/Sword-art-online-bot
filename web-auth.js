
import jwt from 'jsonwebtoken';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';


function getOrCreateSecret(dataPath) {
  const secretFile = path.join(dataPath, 'jwt-secret.txt');
  if (fs.existsSync(secretFile)) {
    return fs.readFileSync(secretFile, 'utf8').trim();
  }
  const secret = crypto.randomBytes(48).toString('hex');
  fs.writeFileSync(secretFile, secret);
  return secret;
}

/**
 * @param {string} dataPath - Pfad zum data/-Ordner des Bots (DATA_PATH)
 */
export function createAuthTools(dataPath) {
  const JWT_SECRET = getOrCreateSecret(dataPath);
  const EXPIRES_IN = '7d';

  function signToken(webId) {
    return jwt.sign({ webId }, JWT_SECRET, { expiresIn: EXPIRES_IN });
  }

  
  function authenticateToken(req, res, next) {
    const header = req.headers['authorization'] || '';
    const token = header.startsWith('Bearer ') ? header.slice(7) : null;
    if (!token) {
      return res.status(401).json({ success: false, error: 'Kein Token angegeben. Bitte erneut einloggen.' });
    }
    try {
      const payload = jwt.verify(token, JWT_SECRET);
      req.webId = payload.webId;
      next();
    } catch (e) {
      return res.status(401).json({ success: false, error: 'Sitzung abgelaufen oder ungültig. Bitte erneut einloggen.' });
    }
  }

  return { signToken, authenticateToken };
}
