const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const dotenv = require('dotenv');
const stream = require('stream');
const nodemailer = require('nodemailer');
const path = require('path');
const cookieParser = require('cookie-parser');
const crypto = require('crypto');
// sharp REMOVED from usage – we leave the import out so nothing touches the image

dotenv.config();

// ---------- CONSTANTS ----------
const LUMI_LOGO_URL =
  'https://raw.githubusercontent.com/LMNRGroup/mayaguez-photoapp/refs/heads/main/Assets/Luminar%20Apps%20Horizontal%20Logo.png';

// Google Drive folders
const PENDING_FOLDER_ID  = '1n7AKxJ7Hc4QMVynY9C3d1fko6H_wT_qs'; // existing uploads
const APPROVED_FOLDER_ID = '1blA55AfkUykFcYzgFzmvthXMvVFv6I0u'; // "Approved" folder

// Logs Google Sheet ID
const SESSION_SHEET_ID =
  process.env.SESSION_SHEET_ID ||
  '1bPctG2H31Ix2N8jVNgFTgiB3FVWyr6BudXNY8YOD4GE';

// Range with columns: timestamp_utc, timestamp_pr, event_type, email, session_id, metadata
const SESSION_SHEET_RANGE = 'A:F';

// ---------- ADMIN SECURITY ----------
const ADMIN_ACCESS_CODE = process.env.ADMIN_ACCESS_CODE; // e.g. "MAYAGUEZ2025!"
const ADMIN_UNLOCK_KEY = process.env.ADMIN_UNLOCK_KEY;   // master unlock key

const ADMIN_MAX_ATTEMPTS = 3;
const ADMIN_BLOCK_MINUTES = 30;

// In-memory IP tracker: { ip: { attempts, blockedUntil } }
const ipTracker = new Map();

// --- Admin session tokens (stateless, safe for Vercel) ---
const ADMIN_SESSION_SECRET =
  process.env.ADMIN_SESSION_SECRET ||
  (ADMIN_ACCESS_CODE ? `${ADMIN_ACCESS_CODE}_secret` : 'fallback_admin_secret');

function createAdminSession(ip) {
  // payload we will sign
  const payload = {
    ip,
    iat: Date.now(), // issued-at
  };

  const payloadJson = JSON.stringify(payload);
  const payloadB64 = Buffer.from(payloadJson, 'utf8').toString('base64url');

  const sig = crypto
    .createHmac('sha256', ADMIN_SESSION_SECRET)
    .update(payloadB64)
    .digest('hex');

  // token = base64url(payload).signature
  return `${payloadB64}.${sig}`;
}

function verifyAdminSessionToken(token) {
  if (!token || typeof token !== 'string') return null;

  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [payloadB64, sig] = parts;

  const expectedSig = crypto
    .createHmac('sha256', ADMIN_SESSION_SECRET)
    .update(payloadB64)
    .digest('hex');

  if (sig !== expectedSig) return null;

  try {
    const payloadJson = Buffer.from(payloadB64, 'base64url').toString('utf8');
    const payload = JSON.parse(payloadJson);

    // Optional: expire after 8 hours
    const maxAgeMs = 8 * 60 * 60 * 1000;
    if (Date.now() - payload.iat > maxAgeMs) {
      return null;
    }

    return payload; // { ip, iat }
  } catch (e) {
    return null;
  }
}

function getAdminSessionFromRequest(req) {
  const headerToken = req.headers['x-admin-token'];
  const queryToken = req.query && req.query.token;
  const token = (headerToken || queryToken || '').toString();

  if (!token) return null;
  const payload = verifyAdminSessionToken(token);
  if (!payload) return null;

  return payload; // { ip, iat }
}

// ---------- Mail setup ----------
let mailTransporter = null;

if (process.env.MAIL_USER && process.env.MAIL_PASS) {
  mailTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: process.env.MAIL_USER, // Luminar Apps email
      pass: process.env.MAIL_PASS  // App password
    }
  });
  console.log('Mail transporter configured');
} else {
  console.log('Mail transporter NOT configured (missing MAIL_USER or MAIL_PASS)');
}

// ---------- Express setup ----------
const app = express();

// Simplified, safe CORS with credentials
const allowedOrigins = [
  'https://mayaguez.luminarapps.com',
  'https://mayaguez-photoapp.vercel.app',
  'http://localhost:3000',
  'http://localhost:5173',
  'http://localhost:4173'
];

const corsOptions = {
  origin: (origin, callback) => {
    // Allow all origins (including same-origin, custom domains, previews)
    callback(null, true);
  },
  credentials: true,
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions)); // handle preflight

app.use(cookieParser());
app.use(express.json());

// ---------- Google Auth (Service Account) ----------
const credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);

// Scopes: Drive + Sheets
const jwtClient = new google.auth.JWT(
  credentials.client_email,
  null,
  credentials.private_key,
  [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
  ]
);

// Clients
const drive = google.drive({ version: 'v3', auth: jwtClient });
const sheets = google.sheets({ version: 'v4', auth: jwtClient });

// Extract ticket number from a filename "T"
function extractTicketNumber(filename) {
  try {
    // filename starts with "T015-..."
    const dashIndex = filename.indexOf("-");
    if (dashIndex === -1) return null;

    const ticketPart = filename.substring(1, dashIndex); // "015"
    return parseInt(ticketPart, 10);
  } catch {
    return null;
  }
}

// ---------- DATE / TIME HELPERS ----------

// Puerto Rico time "now" (GMT-4 fixed, no DST)
function getPRDate() {
  const now = new Date();
  const utcMs = now.getTime() + now.getTimezoneOffset() * 60000;
  return new Date(utcMs - 4 * 60 * 60 * 1000);
}

// Convert a Date (UTC) to PR time
function toPR(date) {
  const utcMs = date.getTime() + date.getTimezoneOffset() * 60000;
  return new Date(utcMs - 4 * 60 * 60 * 1000);
}

// Long date in Spanish: "Miércoles 3 de diciembre de 2025"
function formatPRDateLong() {
  const d = getPRDate();

  const days = [
    'Domingo', 'Lunes', 'Martes', 'Miércoles',
    'Jueves', 'Viernes', 'Sábado'
  ];
  const months = [
    'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
    'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
  ];

  const dayName = days[d.getDay()];
  const dayNum = d.getDate();
  const monthName = months[d.getMonth()];
  const year = d.getFullYear();

  return `${dayName} ${dayNum} de ${monthName} de ${year}`;
}

// Short date/time string for the timestamp_pr column in the Sheet
// Example: "03/12/2025 22:05:31"
function formatPRDateTimeShort(prDate) {
  const pad = (n) => String(n).padStart(2, '0');
  const dd = pad(prDate.getDate());
  const mm = pad(prDate.getMonth() + 1);
  const yyyy = prDate.getFullYear();
  const HH = pad(prDate.getHours());
  const MM = pad(prDate.getMinutes());
  const SS = pad(prDate.getSeconds());
  return `${dd}/${mm}/${yyyy} ${HH}:${MM}:${SS}`;
}

// For reports: "HH:00–HH:59"
function formatHourRange(hour) {
  const h = String(hour).padStart(2, '0');
  return `${h}:00–${h}:59`;
}

// Today range (in PR) but expressed in UTC, for filtering logs
function getTodayPRRangeUtc() {
  const nowPR = getPRDate();

  const startPR = new Date(nowPR);
  startPR.setHours(0, 0, 0, 0);

  const endPR = new Date(nowPR);
  endPR.setHours(23, 59, 59, 999);

  // PR time → UTC (PR = UTC-4, so we add 4h)
  const prToUtc = (dPR) => new Date(dPR.getTime() + 4 * 60 * 60 * 1000);

  return {
    startUtc: prToUtc(startPR),
    endUtc: prToUtc(endPR)
  };
}

// ---------- ADMIN AUTH HELPERS ----------

function getClientIp(req) {
  const xfwd = req.headers['x-forwarded-for'];
  if (xfwd) {
    return xfwd.split(',')[0].trim();
  }
  return req.socket?.remoteAddress || 'unknown';
}

function isBlocked(ip) {
  const record = ipTracker.get(ip);
  if (!record || !record.blockedUntil) return false;
  return record.blockedUntil > Date.now();
}

function remainingBlockMinutes(ip) {
  const record = ipTracker.get(ip);
  if (!record || !record.blockedUntil) return 0;
  const remainingMs = record.blockedUntil - Date.now();
  return Math.max(0, Math.ceil(remainingMs / 60000));
}

// Middleware: reject if IP is blocked
function ensureNotBlocked(req, res, next) {
  const ip = getClientIp(req);
  if (isBlocked(ip)) {
    return res.status(403).json({
      error: 'blocked',
      message: 'IP temporarily blocked',
      blockedMinutes: remainingBlockMinutes(ip),
    });
  }
  next();
}

// Middleware: require valid admin token AND not blocked
function ensureAdminAuth(req, res, next) {
  const ip = getClientIp(req);

  if (isBlocked(ip)) {
    return res.status(403).json({
      error: 'blocked',
      message: 'IP temporarily blocked',
      blockedMinutes: remainingBlockMinutes(ip),
    });
  }

  const session = getAdminSessionFromRequest(req);
  if (!session) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  // Optional binding: token only valid from same IP that logged in
  if (session.ip && session.ip !== ip) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  next();
}

// ---------- DRIVE HELPERS (PHOTOS) ----------

// Ticket label: T001, T002, ...
function formatTicketLabel(counter) {
  const num = String(counter).padStart(3, '0');
  return `T${num}`;
}

// Build filename T001-DD-MM-YY-HH-MM-PR.jpeg
function buildServerFileName(counter) {
  const prNow = getPRDate();

  const pad = (n) => String(n).padStart(2, '0');

  const dd = pad(prNow.getDate());
  const mm = pad(prNow.getMonth() + 1);
  const yy = String(prNow.getFullYear()).slice(-2);
  const HH = pad(prNow.getHours());
  const MM = pad(prNow.getMinutes());

  const ticketLabel = formatTicketLabel(counter);

  return `${ticketLabel}-${dd}-${mm}-${yy}-${HH}-${MM}-PR.jpeg`;
}

// Internal helper: read max index from one folder with a given trashed flag
async function getMaxIndexInFolderWithTrashFlag(folderId, trashedFlag) {
  let pageToken = null;
  let maxIndex = 0;

  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = ${trashedFlag ? 'true' : 'false'}`,
      fields: 'files(name), nextPageToken',
      pageSize: 100,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken
    });

    const files = res.data.files || [];
    for (const file of files) {
      if (!file.name) continue;

      let idx = 0;

      // New pattern: T001-DD-MM-YY...
      const matchNew = /^T(\d{3,})-/.exec(file.name);
      if (matchNew) {
        idx = parseInt(matchNew[1], 10);
      } else {
        // Legacy pattern: 01_DD_MM_YY-...
        const matchOld = /^(\d{2,})_/.exec(file.name);
        if (matchOld) {
          idx = parseInt(matchOld[1], 10);
        }
      }

      if (!Number.isNaN(idx) && idx > maxIndex) {
        maxIndex = idx;
      }
    }

    pageToken = res.data.nextPageToken;
  } while (pageToken);

  return maxIndex;
}

// Find next index for filename in Drive (PENDING + APPROVED, including trashed)
// So even rejected photos keep their ticket number reserved.
async function getMaxIndexInFolderIncludingTrash(folderId) {
  const [maxActive, maxTrashed] = await Promise.all([
    getMaxIndexInFolderWithTrashFlag(folderId, false),
    getMaxIndexInFolderWithTrashFlag(folderId, true),
  ]);
  return Math.max(maxActive, maxTrashed);
}

async function getNextPhotoIndex() {
  const [pendingMax, approvedMax] = await Promise.all([
    getMaxIndexInFolderIncludingTrash(PENDING_FOLDER_ID),
    getMaxIndexInFolderIncludingTrash(APPROVED_FOLDER_ID)
  ]);

  const globalMax = Math.max(pendingMax, approvedMax);
  return globalMax + 1;
}
// Upload file to Drive → PENDING folder
// NO Sharp overlay anymore – we keep filename + ticket metadata only.
async function uploadFile(fileBuffer, originalname, mimetype) {
  // Decide ticket index & labels
  const nextIndex = await getNextPhotoIndex();
  const finalName = buildServerFileName(nextIndex);
  const ticketLabel = formatTicketLabel(nextIndex); // "T001"
  const ticketDisplay = '#' + String(nextIndex).padStart(3, '0'); // "#001" for FE tan ticket

  // Upload original buffer as-is
  const bufferStream = new stream.PassThrough();
  bufferStream.end(fileBuffer);

  const response = await drive.files.create({
    requestBody: {
      name: finalName,
      mimeType: mimetype,
      parents: [PENDING_FOLDER_ID]
    },
    media: {
      mimeType: mimetype,
      body: bufferStream
    },
    supportsAllDrives: true
  });

  return {
    fileId: response.data.id,
    finalName,
    ticketIndex: nextIndex,
    ticketLabel,
    ticketDisplay
  };
}

// Get the oldest pending photo
async function getNextPendingPhoto() {
  const res = await drive.files.list({
    q: `'${PENDING_FOLDER_ID}' in parents and trashed = false`,
    orderBy: 'createdTime asc',
    pageSize: 1,
    fields: 'files(id, name)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  const files = res.data.files || [];
  if (!files.length) return null;
  return files[0]; // { id, name }
}

// List all files in a Drive folder (non-trashed)
async function listFilesInFolder(folderId) {
  let pageToken = null;
  const results = [];

  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime), nextPageToken',
      orderBy: 'createdTime asc',
      pageSize: 100,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    });

    const files = res.data.files || [];
    for (const f of files) {
      results.push({
        id: f.id,
        name: f.name,
        createdTime: f.createdTime,
      });
    }

    pageToken = res.data.nextPageToken;
  } while (pageToken);

  return results;
}

// ---------- SHEETS HELPERS (LOGS) ----------

// Generic log to Google Sheets
async function logEventToSheet(eventType, {
  email = '',
  sessionId = '',
  metadata = null,
  timestampUtc = null
} = {}) {
  if (!SESSION_SHEET_ID) {
    console.warn('SESSION_SHEET_ID missing, skipping logEventToSheet');
    return;
  }

  const utcStr = timestampUtc || new Date().toISOString();
  const dUtc = new Date(utcStr);
  if (Number.isNaN(dUtc.getTime())) {
    console.warn('Invalid UTC timestamp for logEventToSheet, skipping:', utcStr);
    return;
  }

  const dPR = toPR(dUtc);
  const prStr = formatPRDateTimeShort(dPR);

  const metaString = metadata ? JSON.stringify(metadata) : '';

  const values = [[utcStr, prStr, eventType, email || '', sessionId || '', metaString]];

  await sheets.spreadsheets.values.append({
    spreadsheetId: SESSION_SHEET_ID,
    range: SESSION_SHEET_RANGE,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  });

  console.log('Logged event to sheet:', { eventType, utcStr, prStr, email, sessionId });
}

// Read TODAY logs (in PR) from the Sheet and compute stats
async function getTodayStatsFromSheet() {
  if (!SESSION_SHEET_ID) {
    console.warn('SESSION_SHEET_ID missing, cannot compute stats.');
    return {
      visits: 0,
      forms: 0,
      uploads: 0,
      eventsForPrimeHour: []
    };
  }

  const { startUtc, endUtc } = getTodayPRRangeUtc();

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SESSION_SHEET_ID,
    range: SESSION_SHEET_RANGE
  });

  const rows = resp.data.values || [];
  if (rows.length <= 1) {
    // Only headers, nothing else
    return {
      visits: 0,
      forms: 0,
      uploads: 0,
      eventsForPrimeHour: []
    };
  }

  let visits = 0;
  let forms = 0;
  let uploads = 0;
  const eventsForPrimeHour = []; // { ts: string }

  // Skip header row (row 0)
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const tsUtcStr = row[0];
    const eventType = row[2];

    if (!tsUtcStr || !eventType) continue;

    const ts = new Date(tsUtcStr);
    if (Number.isNaN(ts.getTime())) continue;

    if (ts < startUtc || ts > endUtc) continue;

    if (eventType === 'visit') visits++;
    else if (eventType === 'form') forms++;
    else if (eventType === 'upload') uploads++;

    eventsForPrimeHour.push({ ts: tsUtcStr });
  }

  return { visits, forms, uploads, eventsForPrimeHour };
}

// Compute prime hour using today's events (in PR time)
function computePrimeHour(events) {
  if (!events.length) return null;

  const bucket = {}; // hour -> count

  for (const ev of events) {
    const dUTC = new Date(ev.ts);
    if (Number.isNaN(dUTC.getTime())) continue;

    const dPR = toPR(dUTC);
    const h = dPR.getHours(); // 0–23

    bucket[h] = (bucket[h] || 0) + 1;
  }

  let bestHour = null;
  let bestCount = 0;
  for (const [hourStr, count] of Object.entries(bucket)) {
    const hour = Number(hourStr);
    if (count > bestCount) {
      bestCount = count;
      bestHour = hour;
    }
  }

  if (bestHour === null) return null;
  return { hour: bestHour, count: bestCount };
}

// ---------- DAILY REPORT EMAIL (uses Sheet data) ----------
async function sendSessionReportEmailFromSheet() {
  if (!mailTransporter) {
    console.log('Mail transporter not configured, skipping report email.');
    return;
  }

  const { visits, forms, uploads, eventsForPrimeHour } =
    await getTodayStatsFromSheet();

  const totalEvents = visits + forms + uploads;
  if (totalEvents === 0) {
    console.log('No activity today in sheet, skipping report email.');
    return;
  }

  const prime = computePrimeHour(eventsForPrimeHour);
  const longDate = formatPRDateLong();

  const subject = 'Reporte de sesión diaria – Selfie App · Municipio de Mayagüez';

  const textReport =
    `REPORTE DE SESION - SELFIE APP - MUNICIPIO DE MAYAGÜEZ\n` +
    `${longDate}\n\n` +
    `Visitas a la app: ${visits}\n` +
    `Formularios completados: ${forms}\n` +
    `Fotos capturadas/subidas: ${uploads}\n\n` +
    (prime
      ? `Horario de mayor actividad: ${formatHourRange(prime.hour)} ` +
        `(${prime.count} interacciones)\n`
      : `No se pudo determinar un horario de mayor actividad.\n`) +
    `\nTotal de eventos registrados: ${totalEvents}\n`;

  const htmlReport = `<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <title>${subject}</title>
  </head>
  <body style="margin:0;padding:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
      <tr>
        <!-- LEFT aligned main block -->
        <td align="left" style="padding:24px;">
          <!-- Card -->
          <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="560"
                 style="background:#ffffff;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.06);
                        font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
            <!-- Header band (same style as "Nueva familia registrada") -->
            <tr>
              <td style="padding:20px 24px 16px 24px;background:#0a192f;
                         border-radius:8px 8px 0 0;color:#ffffff;text-align:center;">
                <div style="font-size:18px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;">
                  REPORTE DE SESIÓN DIARIA
                </div>
                <div style="margin-top:4px;font-size:10px;opacity:0.9;">
                  Pantalla Plaza Colón · Selfie App · Municipio de Mayagüez
                </div>
                <div style="margin-top:4px;font-size:10px;opacity:0.9;">
                  ${longDate}
                </div>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:18px 24px 8px 24px;font-size:13px;color:#333333;text-align:left;">
                <p style="margin:0 0 10px 0;">
                  A continuación encontrarás el resumen de uso de la aplicación durante esta sesión:
                </p>

                <p style="margin:0 0 6px 0;">
                  <strong>Visitas a la app:</strong> ${visits}
                </p>
                <p style="margin:0 0 6px 0;">
                  <strong>Formularios completados:</strong> ${forms}
                </p>
                <p style="margin:0 0 6px 0;">
                  <strong>Fotos capturadas/subidas:</strong> ${uploads}
                </p>

                ${
                  prime
                    ? `<p style="margin:10px 0 0 0;font-size:13px;">
                         <strong>Horario de mayor actividad:</strong>
                         ${formatHourRange(prime.hour)} (${prime.count} interacciones)
                       </p>`
                    : `<p style="margin:10px 0 0 0;font-size:13px;">
                         No se pudo determinar un horario de mayor actividad.
                       </p>`
                }
              </td>
            </tr>

            <!-- Footer with centered logo (same as other emails) -->
            <tr>
              <td style="padding:18px 24px 20px 24px;text-align:center;border-top:1px solid #f0f0f0;">
                <img
                  src="${LUMI_LOGO_URL}"
                  alt="Luminar Apps"
                  style="display:block;height:40px;margin:0 auto 6px auto;"
                />
                <p style="font-size:9px;color:#aaaaaa;line-height:1.4;margin:0 0 2px 0;">
                  Este correo fue generado automáticamente por Luminar Apps.
                </p>
                <p style="font-size:9px;color:#aaaaaa;line-height:1.4;margin:0;">
                  Favor no responder a este correo electrónico.
                </p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;

  await mailTransporter.sendMail({
    from: `"Luminar Apps" <${process.env.MAIL_FROM || process.env.MAIL_USER}>`,
    to: process.env.MAIL_TO || process.env.MAIL_USER,
    subject,
    text: textReport,
    html: htmlReport,
    attachments: [
      {
        filename: 'reporte_sesion.txt',
        content: textReport
      }
    ]
  });

  console.log('Daily session report email sent (from Sheets data).');
}

// ---------- ROUTES ----------

// Simple visit endpoint: FE can call this on page load
app.post('/ping', async (req, res) => {
  try {
    const sessionId = req.headers['x-session-id'] || '';
    await logEventToSheet('visit', { sessionId });
  } catch (e) {
    console.error('Error logging visit to sheet:', e);
  }
  res.json({ ok: true });
});

// Photo upload (counts as "upload")
app.post('/upload', express.raw({ type: 'image/*', limit: '5mb' }), async (req, res) => {
  if (!req.body || req.body.length === 0) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  try {
    const {
      fileId,
      finalName,
      ticketIndex,
      ticketLabel,
      ticketDisplay
    } = await uploadFile(
      req.body,
      'ignored.jpeg',
      req.headers['content-type']
    );

    // Log to Sheet
    try {
      const sessionId = req.headers['x-session-id'] || '';
      await logEventToSheet('upload', {
        sessionId,
        metadata: {
          driveFileId: fileId,
          filename: finalName,
          ticketLabel,
          ticketDisplay
        }
      });
    } catch (e) {
      console.error('Error logging upload event to sheet:', e);
    }

    res.json({
      ok: true,
      message: 'File uploaded successfully',
      fileId,
      ticketIndex,
      ticketLabel,
      ticketDisplay
    });
  } catch (error) {
    console.error('Error uploading file:', error);
    res.status(500).json({ error: 'Error uploading file', details: error.message });
  }
});

// Visit logging (form submissions)
app.post('/visit', async (req, res) => {
  try {
    const { country, lastName, email, newsletter, timestamp } = req.body || {};

    const timestampUtc = timestamp || new Date().toISOString();

    // Log to Sheet as "form"
    try {
      const sessionId = req.headers['x-session-id'] || '';
      await logEventToSheet('form', {
        email,
        sessionId,
        timestampUtc,
        metadata: {
          country,
          lastName,
          newsletter
        }
      });
    } catch (e) {
      console.error('Error logging form event to sheet:', e);
    }

    console.log('Visit payload:', { country, lastName, email, newsletter, timestampUtc });

    if (!mailTransporter) {
      console.log('Mail transporter is not configured, skipping email.');
      return res.status(500).json({ ok: false, error: 'mail_disabled' });
    }

    const subject = 'Nueva familia registrada (Selfie App)';

    const text =
      'Una nueva familia ha sido registrada en el sistema.\n\n' +
      `La familia nos visita desde: ${country || 'No provisto'}\n` +
      `Apellidos de la familia: ${lastName || 'No provisto'}\n` +
      `Correo electrónico de la familia: ${email || 'No provisto'}\n` +
      `Acepta recibir noticias y ofertas de MUNICIPIO DE MAYAGÜEZ.: ${newsletter ? 'Sí' : 'No'}\n\n` +
      `Fecha y hora (UTC): ${timestampUtc}`;

    const html = `<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <title>${subject}</title>
  </head>
  <body style="margin:0;padding:0;background:#f4f4f4;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
      <tr>
        <!-- LEFT aligned main block -->
        <td align="left" style="padding:24px;">
          <!-- Card -->
          <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="560"
                 style="background:#ffffff;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.06);
                        font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
            <!-- Header band -->
            <tr>
              <td style="padding:20px 24px 16px 24px;background:#0a192f; 
                         border-radius:8px 8px 0 0;color:#ffffff;text-align:center;">
                <div style="font-size:18px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;">
                  NUEVA FAMILIA REGISTRADA
                </div>
                <div style="margin-top:4px;font-size:10px;opacity:0.9;">
                  Pantalla Plaza Colón · Selfie App · Municipio de Mayagüez
                </div>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:18px 24px 8px 24px;font-size:13px;color:#333333;text-align:left;">
                <p style="margin:0 0 12px 0;">
                  Se ha registrado una nueva familia en el sistema:
                </p>

                <p style="margin:0 0 6px 0;">
                  <strong>Nos visitan desde:</strong> ${country || 'No provisto'}
                </p>

                <p style="margin:0 0 6px 0;">
                  <strong>Apellidos de la familia:</strong> ${lastName || 'No provisto'}
                </p>

                <p style="margin:0 0 6px 0;">
                  <strong>Correo electrónico:</strong>
                  ${
                    email
                      ? `<a href="mailto:${email}" style="color:#0073e6;text-decoration:none;">${email}</a>`
                      : 'No provisto'
                  }
                </p>

                <p style="margin:0 0 6px 0;">
                  <strong>Acepta recibir noticias y ofertas:</strong> ${newsletter ? 'Sí' : 'No'}
                </p>

                <p style="margin:10px 0 0 0;font-size:11px;color:#666666;">
                  <strong>Fecha y hora (UTC):</strong> ${timestampUtc}
                </p>
              </td>
            </tr>

            <!-- Footer with centered logo + text -->
            <tr>
              <td style="padding:18px 24px 20px 24px;text-align:center;border-top:1px solid #f0f0f0;">
                <img
                  src="${LUMI_LOGO_URL}"
                  alt="Luminar Apps"
                  style="display:block;height:40px;margin:0 auto 6px auto;"
                />
                <p style="font-size:9px;color:#aaaaaa;line-height:1.4;margin:0 0 2px 0;">
                  Este correo fue generado automáticamente por Luminar Apps.
                </p>
                <p style="font-size:9px;color:#aaaaaa;line-height:1.4;margin:0;">
                  Favor no responder a este correo electrónico.
                </p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;

    await mailTransporter.sendMail({
      from: `"Luminar Apps" <${process.env.MAIL_FROM || process.env.MAIL_USER}>`,
      to: process.env.MAIL_TO || process.env.MAIL_USER,
      subject,
      text,
      html
    });

    console.log('Email sent OK');
    res.json({ ok: true });
  } catch (err) {
    console.error('Error sending visit email:', err);
    res.status(500).json({ ok: false, error: 'email_failed' });
  }
});

// --------- ADMIN AUTH ROUTE ----------

app.post('/admin/auth', ensureNotBlocked, async (req, res) => {
  if (!ADMIN_ACCESS_CODE) {
    console.error('ADMIN_ACCESS_CODE is not configured');
    return res.status(500).json({ error: 'admin_code_not_configured' });
  }

  const ip = getClientIp(req);
  const { code } = req.body || {};

  if (!code || typeof code !== 'string') {
    return res.status(400).json({ error: 'missing_code' });
  }

  let record = ipTracker.get(ip) || { attempts: 0, blockedUntil: 0 };

  // Safety: if already blocked
  if (isBlocked(ip)) {
    return res.status(403).json({
      error: 'blocked',
      blockedMinutes: remainingBlockMinutes(ip),
    });
  }

  if (code === ADMIN_ACCESS_CODE) {
    // Success → reset attempts, clear block, create signed token
    record = { attempts: 0, blockedUntil: 0 };
    ipTracker.set(ip, record);

    const token = createAdminSession(ip);
    return res.json({ ok: true, token });
  }

  // Wrong code
  record.attempts += 1;

  if (record.attempts >= ADMIN_MAX_ATTEMPTS) {
    record.blockedUntil = Date.now() + ADMIN_BLOCK_MINUTES * 60 * 1000;
    ipTracker.set(ip, record);

    return res.status(403).json({
      error: 'blocked',
      blockedMinutes: ADMIN_BLOCK_MINUTES,
    });
  }

  ipTracker.set(ip, record);

  return res.status(401).json({
    error: 'invalid_code',
    remainingAttempts: ADMIN_MAX_ATTEMPTS - record.attempts,
  });
});

// --------- ADMIN UNBLOCK (manual) ----------

app.post('/admin/unblock', (req, res) => {
  if (!ADMIN_UNLOCK_KEY) {
    return res.status(500).json({ error: 'unlock_key_not_configured' });
  }

  const key = req.headers['x-admin-unlock-key'];
  if (key !== ADMIN_UNLOCK_KEY) {
    return res.status(403).json({ error: 'forbidden' });
  }

  const { ip } = req.body || {};
  if (!ip) {
    return res.status(400).json({ error: 'missing_ip' });
  }

  ipTracker.delete(ip);
  return res.json({ ok: true, message: `IP ${ip} unblocked` });
});

// --------- ADMIN API: review photos ----------

// Get next pending photo (for review UI)
app.get('/admin/next-photo', ensureAdminAuth, async (req, res) => {
  try {
    const file = await getNextPendingPhoto();
    if (!file) {
      return res.json({ empty: true });
    }
    res.json({
      empty: false,
      fileId: file.id,
      name: file.name
    });
  } catch (err) {
    console.error('Error fetching next pending photo:', err);
    res.status(500).json({ error: 'failed_next_photo' });
  }
});

// Serve the actual image bytes so admin.html can <img src="...">
app.get('/admin/photo/:id', ensureAdminAuth, async (req, res) => {
  const fileId = req.params.id;

  try {
    const driveRes = await drive.files.get(
      {
        fileId,
        alt: 'media'
      },
      { responseType: 'stream' }
    );

    res.setHeader('Content-Type', 'image/jpeg');
    driveRes.data
      .on('error', (err) => {
        console.error('Error streaming photo:', err);
        if (!res.headersSent) {
          res.status(500).end('Error streaming file');
        }
      })
      .pipe(res);
  } catch (err) {
    console.error('Error getting photo from Drive:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'failed_photo_stream' });
    }
  }
});

// Approve: move file from Pending to Approved
app.post('/admin/approve', ensureAdminAuth, async (req, res) => {
  try {
    const { fileId } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ error: 'missing_fileId' });
    }

    await drive.files.update({
      fileId,
      addParents: APPROVED_FOLDER_ID,
      removeParents: PENDING_FOLDER_ID,
      fields: 'id, parents',
      supportsAllDrives: true
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error approving photo:', err);
    res.status(500).json({ error: 'failed_approve' });
  }
});

// Reject: send file to Drive trash
app.post('/admin/reject', ensureAdminAuth, async (req, res) => {
  try {
    const { fileId } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ error: 'missing_fileId' });
    }

    await drive.files.update({
      fileId,
      requestBody: { trashed: true },
      fields: 'id, trashed',
      supportsAllDrives: true
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error rejecting photo:', err);
    res.status(500).json({ error: 'failed_reject' });
  }
});

// --------- ADMIN: list / manage APPROVED photos ----------

// List approved photos for the admin thumbnail grid
app.get('/admin/approved-list', ensureAdminAuth, async (req, res) => {
  try {
    const files = await listFilesInFolder(APPROVED_FOLDER_ID);
    // admin UI can build thumb src as /admin/photo/:id?token=...
    res.json({ ok: true, files });
  } catch (err) {
    console.error('Error listing approved files for admin:', err);
    res.status(500).json({ ok: false, error: 'list_approved_failed' });
  }
});

// Delete a single approved photo (send to trash)
app.post('/admin/delete-approved', ensureAdminAuth, async (req, res) => {
  try {
    const { fileId } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ error: 'missing_fileId' });
    }

    await drive.files.update({
      fileId,
      requestBody: { trashed: true },
      fields: 'id, trashed',
      supportsAllDrives: true,
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error deleting approved photo:', err);
    res.status(500).json({ ok: false, error: 'delete_approved_failed' });
  }
});

// Clear Drive: send ALL pending + approved photos to trash
// ⚠️ CAREFUL: this is meant for after the event.
app.post('/admin/clear-drive', ensureAdminAuth, async (req, res) => {
  try {
    const pendingFiles = await listFilesInFolder(PENDING_FOLDER_ID);
    const approvedFiles = await listFilesInFolder(APPROVED_FOLDER_ID);

    const allFiles = [...pendingFiles, ...approvedFiles];

    for (const file of allFiles) {
      try {
        await drive.files.update({
          fileId: file.id,
          requestBody: { trashed: true },
          fields: 'id, trashed',
          supportsAllDrives: true,
        });
      } catch (innerErr) {
        console.error('Error trashing file during clear-drive:', file.id, innerErr);
      }
    }

    res.json({
      ok: true,
      trashedCount: allFiles.length,
    });
  } catch (err) {
    console.error('Error clearing Drive folders:', err);
    res.status(500).json({ ok: false, error: 'clear_drive_failed' });
  }
});

// Reset logs (dummy): does NOT touch Sheets yet, just responds OK
app.post('/admin/reset-logs', ensureAdminAuth, async (req, res) => {
  try {
    console.log('Admin requested reset-logs (dummy endpoint, no-op).');
    res.json({ ok: true, message: 'reset-logs dummy endpoint reached (no changes applied).' });
  } catch (err) {
    console.error('Error in reset-logs (dummy):', err);
    res.status(500).json({ ok: false, error: 'reset_logs_failed' });
  }
});

// --------- PUBLIC GALLERY API (for Yodeck / gallery.html) ----------

// List approved photos (virtual rotation, max 12)
app.get('/gallery/approved', async (req, res) => {
  try {
    const response = await drive.files.list({
      q: `'${APPROVED_FOLDER_ID}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime)',
      orderBy: 'createdTime asc',
      pageSize: 1000,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    const all = response.data.files || [];
    const ROTATION_MAX = 12;

    let selected;

    if (all.length <= ROTATION_MAX) {
      // 12 or fewer → keep chronological order
      selected = all;
    } else {
      // More than 12 → ring-buffer behavior:
      // index i goes into slot (i % ROTATION_MAX)
      const ring = new Array(ROTATION_MAX);
      for (let i = 0; i < all.length; i++) {
        const slot = i % ROTATION_MAX;
        ring[slot] = all[i];
      }
      selected = ring;
    }

    const files = selected.map((f) => ({
      id: f.id,
      name: f.name,
      createdTime: f.createdTime,
    }));

    res.json({ ok: true, files });
  } catch (err) {
    console.error('Error listing approved files for gallery', err);
    res.status(500).json({ ok: false, error: 'list_approved_failed' });
  }
});

// Stream a single approved photo (no admin token, for public gallery)
app.get('/gallery/photo/:fileId', async (req, res) => {
  const { fileId } = req.params;

  try {
    const driveRes = await drive.files.get(
      {
        fileId,
        alt: 'media',
      },
      { responseType: 'stream' }
    );

    // You can refine Content-Type by querying file metadata if needed
    res.setHeader('Content-Type', 'image/jpeg');

    driveRes.data
      .on('error', (err) => {
        console.error('Drive stream error (gallery)', err);
        if (!res.headersSent) {
          res.end();
        }
      })
      .pipe(res);
  } catch (err) {
    console.error('Error streaming gallery photo', err);
    if (!res.headersSent) {
      res.status(404).end();
    }
  }
});

// --------- Daily report route (for Vercel cron) ----------
app.get('/session-report-daily', async (req, res) => {
  try {
    console.log('Daily cron hit: /session-report-daily');
    await sendSessionReportEmailFromSheet();
    res.json({ ok: true });
  } catch (e) {
    console.error('Error sending daily session report:', e);
    res.status(500).json({ ok: false, error: 'report_failed' });
  }
});

// Manual trigger for debugging (you can POST from Postman / curl)
app.post('/session-report-now', async (req, res) => {
  try {
    await sendSessionReportEmailFromSheet();
    res.json({ ok: true });
  } catch (e) {
    console.error('Error sending manual session report:', e);
    res.status(500).json({ ok: false, error: 'report_failed' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
