const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const dotenv = require('dotenv');
const stream = require('stream');
const nodemailer = require('nodemailer');
const path = require('path');
const cookieParser = require('cookie-parser');
const crypto = require('crypto');
const multer = require('multer');
// sharp REMOVED from usage – we leave the import out so nothing touches the image

dotenv.config();

// ---------- CONSTANTS ----------
const LUMI_LOGO_URL =
  'https://raw.githubusercontent.com/LMNRGroup/mayaguez-photoapp/refs/heads/main/Assets/Luminar%20Apps%20Horizontal%20Logo.png';

// Google Drive folders
const PENDING_FOLDER_ID  = '1n7AKxJ7Hc4QMVynY9C3d1fko6H_wT_qs'; // existing uploads
const APPROVED_FOLDER_ID = '1blA55AfkUykFcYzgFzmvthXMvVFv6I0u'; // "Approved" folder
const TEMPLATES_BG_FOLDER_ID = '1CcL_VNubTsran1Q8A8rJmgqwARi6asBp'; // Template backgrounds
const TEMPLATES_LOGO_FOLDER_ID = '1YhfEJRXkbfpGcqkLnxFp8mkaCm_eOtrj'; // Template logos

// Logs Google Sheet ID
const SESSION_SHEET_ID =
  process.env.SESSION_SHEET_ID ||
  '1bPctG2H31Ix2N8jVNgFTgiB3FVWyr6BudXNY8YOD4GE';

const SETTINGS_SHEET_ID =
  process.env.SETTINGS_SHEET_ID ||
  '1vAcoPK5Xm588PQULy2eOlNTknihofKuiijlfLidKhbI';
const SETTINGS_SHEET_NAME = process.env.SETTINGS_SHEET_NAME || 'Settings';

const TEMPLATES_SHEET_ID =
  process.env.TEMPLATES_SHEET_ID ||
  '1yIyxpT_lYMBGcPsRbdp1egCzFO89iqEBLZZCzxaeCUY';
const TEMPLATES_SHEET_NAME = process.env.TEMPLATES_SHEET_NAME || 'Templates';
const TEMPLATES_SHEET_RANGE = `${TEMPLATES_SHEET_NAME}!A:D`; // Name, JSON, Created, Active

// Range with columns:
// timestamp_utc, timestamp_pr, event_type, email, session_id,
// country, region, last_name, newsletter, ticket
const SESSION_SHEET_RANGE = 'A:J';
const SETTINGS_SHEET_RANGE = `${SETTINGS_SHEET_NAME}!A:B`;
const SETTINGS_SERVER_SESSION_KEY = 'Server Session';
const SESSION_SHEET_HEADERS = [
  'timestamp_utc',
  'timestamp_pr',
  'event_type',
  'email',
  'session_id',
  'country',
  'region',
  'last_name',
  'newsletter',
  'ticket'
];

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

// ---------- App enabled flag (for /admin/shutdown) ----------
let appEnabled = true; // admin toggle will set this to false after event
let appSessionId = null;

// ---------- APP SETTINGS (admin-configurable) ----------
const DEFAULT_APP_SETTINGS = {
  ticketEnabled: true,
  galleryDisplayLimit: 'all', // 'all', 'last10', 'last25'
  intro: {
    title: '¿Desde dónde nos visitas? 😊',
    subtitle: 'Selecciona tu país y municipio/estado, escribe tus apellidos y continúa a tu selfie.'
  },
  form: {
    locationEnabled: true,
    lastName: {
      enabled: true,
      label: 'Apellidos de la familia',
      placeholder: 'Apellidos de la familia (Ej. Pérez González)'
    },
    email: {
      enabled: true,
      label: 'Correo electrónico',
      placeholder: 'Correo electrónico'
    },
    newsletter: {
      enabled: true,
      label: 'Deseo recibir noticias y ofertas de Municipio de Mayagüez.',
      helper: 'Tu email será utilizado únicamente si autorizas recibir nuestro boletín.'
    }
  }
};

let appSettings = JSON.parse(JSON.stringify(DEFAULT_APP_SETTINGS));
let settingsSheetReady = false;
let settingsLoadedFromSheet = false;

const SETTINGS_FIELDS = [
  { key: 'Ticket Overlay Enabled', type: 'boolean', path: ['ticketEnabled'] },
  { key: 'Gallery Display Limit', type: 'string', path: ['galleryDisplayLimit'] },
  { key: 'Intro Title', type: 'string', path: ['intro', 'title'] },
  { key: 'Intro Subtitle', type: 'string', path: ['intro', 'subtitle'] },
  { key: 'Location Enabled', type: 'boolean', path: ['form', 'locationEnabled'] },
  { key: 'Last Name Enabled', type: 'boolean', path: ['form', 'lastName', 'enabled'] },
  { key: 'Last Name Label', type: 'string', path: ['form', 'lastName', 'label'] },
  { key: 'Last Name Placeholder', type: 'string', path: ['form', 'lastName', 'placeholder'] },
  { key: 'Email Enabled', type: 'boolean', path: ['form', 'email', 'enabled'] },
  { key: 'Email Label', type: 'string', path: ['form', 'email', 'label'] },
  { key: 'Email Placeholder', type: 'string', path: ['form', 'email', 'placeholder'] },
  { key: 'Email Opt-in Enabled', type: 'boolean', path: ['form', 'newsletter', 'enabled'] },
  { key: 'Email Opt-in Label', type: 'string', path: ['form', 'newsletter', 'label'] },
  { key: 'Email Opt-in Helper', type: 'string', path: ['form', 'newsletter', 'helper'] },
];

function coerceBoolean(value, fallback) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const lower = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'y'].includes(lower)) return true;
    if (['false', '0', 'no', 'n'].includes(lower)) return false;
  }
  return fallback;
}

function coerceString(value, fallback) {
  if (typeof value === 'string') return value.trim();
  return fallback;
}

function normalizeBooleanLabel(value) {
  return value ? 'Enabled' : 'Disabled';
}

function parseBooleanLabel(value, fallback) {
  if (typeof value === 'boolean') return value;
  if (value == null) return fallback;
  const lower = String(value).trim().toLowerCase();
  if (['enabled', 'on', 'true', '1', 'yes', 'y'].includes(lower)) return true;
  if (['disabled', 'off', 'false', '0', 'no', 'n'].includes(lower)) return false;
  return fallback;
}

function normalizeStatusLabel(enabled) {
  return enabled ? 'On' : 'Off';
}

function parseStatusLabel(value, fallback) {
  return parseBooleanLabel(value, fallback);
}

function generateServerSessionId() {
  return crypto.randomBytes(12).toString('hex');
}

function getNestedValue(obj, path) {
  return path.reduce((acc, key) => (acc && acc[key] != null ? acc[key] : undefined), obj);
}

function setNestedValue(obj, path, value) {
  let cursor = obj;
  for (let i = 0; i < path.length - 1; i++) {
    const key = path[i];
    if (!cursor[key] || typeof cursor[key] !== 'object') {
      cursor[key] = {};
    }
    cursor = cursor[key];
  }
  cursor[path[path.length - 1]] = value;
}

function mergeAppSettings(patch = {}) {
  const next = JSON.parse(JSON.stringify(appSettings));

  if (Object.prototype.hasOwnProperty.call(patch, 'ticketEnabled')) {
    next.ticketEnabled = coerceBoolean(patch.ticketEnabled, next.ticketEnabled);
  }

  if (Object.prototype.hasOwnProperty.call(patch, 'galleryDisplayLimit')) {
    const limit = String(patch.galleryDisplayLimit || 'all').trim().toLowerCase();
    if (['all', 'last10', 'last25'].includes(limit)) {
      next.galleryDisplayLimit = limit;
    } else {
      next.galleryDisplayLimit = 'all';
    }
  }

  if (patch.intro && typeof patch.intro === 'object') {
    next.intro.title = coerceString(patch.intro.title, next.intro.title);
    next.intro.subtitle = coerceString(patch.intro.subtitle, next.intro.subtitle);
  }

  if (patch.form && typeof patch.form === 'object') {
    if (Object.prototype.hasOwnProperty.call(patch.form, 'locationEnabled')) {
      next.form.locationEnabled = coerceBoolean(patch.form.locationEnabled, next.form.locationEnabled);
    }

    if (patch.form.lastName && typeof patch.form.lastName === 'object') {
      next.form.lastName.enabled = coerceBoolean(
        patch.form.lastName.enabled,
        next.form.lastName.enabled
      );
      next.form.lastName.label = coerceString(
        patch.form.lastName.label,
        next.form.lastName.label
      );
      next.form.lastName.placeholder = coerceString(
        patch.form.lastName.placeholder,
        next.form.lastName.placeholder
      );
    }

    if (patch.form.email && typeof patch.form.email === 'object') {
      next.form.email.enabled = coerceBoolean(patch.form.email.enabled, next.form.email.enabled);
      next.form.email.label = coerceString(patch.form.email.label, next.form.email.label);
      next.form.email.placeholder = coerceString(
        patch.form.email.placeholder,
        next.form.email.placeholder
      );
    }

    if (patch.form.newsletter && typeof patch.form.newsletter === 'object') {
      next.form.newsletter.enabled = coerceBoolean(
        patch.form.newsletter.enabled,
        next.form.newsletter.enabled
      );
      next.form.newsletter.label = coerceString(
        patch.form.newsletter.label,
        next.form.newsletter.label
      );
      next.form.newsletter.helper = coerceString(
        patch.form.newsletter.helper,
        next.form.newsletter.helper
      );
    }
  }

  return next;
}

async function ensureSettingsSheet() {
  if (settingsSheetReady) return true;
  if (!SETTINGS_SHEET_ID) return false;

  try {
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: SETTINGS_SHEET_ID,
      fields: 'sheets(properties(title))'
    });

    const sheetsList = meta.data.sheets || [];
    const hasSettingsSheet = sheetsList.some(
      (sheet) => sheet.properties && sheet.properties.title === SETTINGS_SHEET_NAME
    );

    if (!hasSettingsSheet) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SETTINGS_SHEET_ID,
        requestBody: {
          requests: [
            {
              addSheet: {
                properties: {
                  title: SETTINGS_SHEET_NAME
                }
              }
            }
          ]
        }
      });
    }

    settingsSheetReady = true;
    return true;
  } catch (err) {
    console.warn('Unable to ensure settings sheet exists:', err.message || err);
    return false;
  }
}

async function readSettingsFromSheet() {
  if (!SETTINGS_SHEET_ID) return null;

  try {
    const ready = await ensureSettingsSheet();
    if (!ready) return null;

    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SETTINGS_SHEET_ID,
      range: SETTINGS_SHEET_RANGE
    });

    const rows = resp.data.values || [];
    if (!rows.length) return null;

    const settingsPatch = {};
    let serverStatus = null;
    let serverSessionId = null;

    for (const entry of rows) {
      if (!entry || !entry[0]) continue;
      const key = String(entry[0]).trim();
      const value = entry[1];

      if (key === 'appSettings' && value) {
        try {
          const parsed = JSON.parse(value);
          if (parsed && typeof parsed === 'object') {
            Object.assign(settingsPatch, parsed);
          }
          continue;
        } catch (err) {
          console.warn('Unable to parse legacy appSettings row:', err.message || err);
        }
      }

      if (key === 'Server Status') {
        serverStatus = parseStatusLabel(value, serverStatus);
        continue;
      }
      if (key === SETTINGS_SERVER_SESSION_KEY) {
        if (value != null && String(value).trim()) {
          serverSessionId = String(value).trim();
        }
        continue;
      }

      const field = SETTINGS_FIELDS.find((item) => item.key === key);
      if (!field) continue;

      if (field.type === 'boolean') {
        const fallbackValue = Boolean(getNestedValue(appSettings, field.path));
        setNestedValue(settingsPatch, field.path, parseBooleanLabel(value, fallbackValue));
      } else if (field.type === 'string') {
        if (typeof value === 'string') {
          setNestedValue(settingsPatch, field.path, value.trim());
        }
      }
    }

    return { settingsPatch, serverStatus, serverSessionId };
  } catch (err) {
    console.warn('Unable to read settings from sheet:', err.message || err);
    return null;
  }
}

async function writeSettingsToSheet(settings, enabledStatus, serverSessionId) {
  if (!SETTINGS_SHEET_ID) return false;

  try {
    const ready = await ensureSettingsSheet();
    if (!ready) return false;

    const values = [['key', 'value']];
    values.push(['Server Status', normalizeStatusLabel(enabledStatus)]);
    if (serverSessionId) {
      values.push([SETTINGS_SERVER_SESSION_KEY, String(serverSessionId)]);
    }

    SETTINGS_FIELDS.forEach((field) => {
      const rawValue = getNestedValue(settings, field.path);
      if (field.type === 'boolean') {
        values.push([field.key, normalizeBooleanLabel(Boolean(rawValue))]);
      } else {
        values.push([field.key, rawValue != null ? String(rawValue) : '']);
      }
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SETTINGS_SHEET_ID,
      range: `${SETTINGS_SHEET_NAME}!A1:B${values.length}`,
      valueInputOption: 'RAW',
      requestBody: { values }
    });

    return true;
  } catch (err) {
    console.warn('Unable to persist settings to sheet:', err.message || err);
    return false;
  }
}

async function hydrateSettingsFromSheet() {
  if (settingsLoadedFromSheet) return;
  const sheetPayload = await readSettingsFromSheet();
  if (sheetPayload && sheetPayload.settingsPatch) {
    appSettings = mergeAppSettings(sheetPayload.settingsPatch);
  }
  if (sheetPayload && typeof sheetPayload.serverStatus === 'boolean') {
    appEnabled = sheetPayload.serverStatus;
  }
  if (sheetPayload && sheetPayload.serverSessionId) {
    appSessionId = sheetPayload.serverSessionId;
  }
  if (!appSessionId) {
    appSessionId = generateServerSessionId();
  }
  settingsLoadedFromSheet = true;
}

// ---------- TEMPLATE SHEET HELPERS ----------

let templatesSheetReady = false;

async function ensureTemplatesSheet() {
  if (templatesSheetReady) return true;
  if (!TEMPLATES_SHEET_ID) return false;

  try {
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: TEMPLATES_SHEET_ID,
      fields: 'sheets(properties(title))'
    });

    const sheetsList = meta.data.sheets || [];
    const hasTemplatesSheet = sheetsList.some(
      (sheet) => sheet.properties && sheet.properties.title === TEMPLATES_SHEET_NAME
    );

    if (!hasTemplatesSheet) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: TEMPLATES_SHEET_ID,
        requestBody: {
          requests: [
            {
              addSheet: {
                properties: {
                  title: TEMPLATES_SHEET_NAME
                }
              }
            }
          ]
        }
      });
    }

    templatesSheetReady = true;
    return true;
  } catch (err) {
    console.warn('Unable to ensure templates sheet exists:', err.message || err);
    return false;
  }
}

async function readTemplatesFromSheet() {
  if (!TEMPLATES_SHEET_ID) return [];

  try {
    const ready = await ensureTemplatesSheet();
    if (!ready) return [];

    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: TEMPLATES_SHEET_ID,
      range: TEMPLATES_SHEET_RANGE
    });

    const rows = resp.data.values || [];
    if (rows.length < 2) return []; // No data (header + rows)

    const templates = [];
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const name = row[0] || '';
      const jsonStr = row[1] || '{}';
      const createdAt = row[2] || '';
      const isActive = row[3] === 'true' || row[3] === true;

      if (!name) continue;

      try {
        const data = JSON.parse(jsonStr);
        templates.push({
          id: i + 1, // Row number as ID (i=1 means row 2, so ID = 2)
          name,
          data,
          createdAt,
          isActive
        });
      } catch (err) {
        console.warn(`Unable to parse template JSON for row ${i}:`, err.message);
      }
    }

    return templates;
  } catch (err) {
    console.warn('Unable to read templates from sheet:', err.message || err);
    return [];
  }
}

async function writeTemplateToSheet(name, templateData, isActive = true) {
  if (!TEMPLATES_SHEET_ID) return null;

  try {
    const ready = await ensureTemplatesSheet();
    if (!ready) return null;

    // Read existing to find next row
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: TEMPLATES_SHEET_ID,
      range: TEMPLATES_SHEET_RANGE
    });

    const rows = resp.data.values || [];
    const nextRow = rows.length + 1;

    const createdAt = new Date().toISOString();
    const values = [[name, JSON.stringify(templateData), createdAt, String(isActive)]];

    await sheets.spreadsheets.values.update({
      spreadsheetId: TEMPLATES_SHEET_ID,
      range: `${TEMPLATES_SHEET_NAME}!A${nextRow}:D${nextRow}`,
      valueInputOption: 'RAW',
      requestBody: { values }
    });

    return { id: nextRow, name, createdAt, isActive };
  } catch (err) {
    console.warn('Unable to write template to sheet:', err.message || err);
    return null;
  }
}

async function updateTemplateInSheet(templateId, name, templateData, isActive) {
  if (!TEMPLATES_SHEET_ID) return false;

  try {
    const ready = await ensureTemplatesSheet();
    if (!ready) return false;

    const row = parseInt(templateId, 10);
    if (isNaN(row) || row < 2) return false;

    const values = [[name, JSON.stringify(templateData), '', String(isActive)]];

    await sheets.spreadsheets.values.update({
      spreadsheetId: TEMPLATES_SHEET_ID,
      range: `${TEMPLATES_SHEET_NAME}!A${row}:D${row}`,
      valueInputOption: 'RAW',
      requestBody: { values }
    });

    return true;
  } catch (err) {
    console.warn('Unable to update template in sheet:', err.message || err);
    return false;
  }
}

async function deleteTemplateFromSheet(templateId) {
  if (!TEMPLATES_SHEET_ID) return false;

  try {
    const row = parseInt(templateId, 10);
    if (isNaN(row) || row < 2) return false;

    await sheets.spreadsheets.values.clear({
      spreadsheetId: TEMPLATES_SHEET_ID,
      range: `${TEMPLATES_SHEET_NAME}!A${row}:D${row}`
    });

    return true;
  } catch (err) {
    console.warn('Unable to delete template from sheet:', err.message || err);
    return false;
  }
}

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

hydrateSettingsFromSheet().catch((err) => {
  console.warn('Unable to hydrate settings on startup:', err.message || err);
});

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

// Normalize newsletter input to "Y" / "N" / ""
function getNewsletterFlag(value) {
  if (value === null || value === undefined) return '';

  const val = String(value).toLowerCase();
  if (val === 'true' || val === '1' || val === 'sí' || val === 'si' || val === 'y') {
    return 'Y';
  }
  if (val === 'false' || val === '0' || val === 'no' || val === 'n') {
    return 'N';
  }
  return '';
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

function normalizeIp(ip) {
  if (!ip) return 'unknown';
  if (ip.startsWith('::ffff:')) {
    return ip.slice(7);
  }
  return ip;
}

function safeDecodeHeader(value) {
  if (!value) return '';
  const raw = String(value).trim();
  try {
    return decodeURIComponent(raw);
  } catch (err) {
    return raw;
  }
}

function getClientLocation(req) {
  const city = safeDecodeHeader(req.headers['x-vercel-ip-city']);
  const region = safeDecodeHeader(req.headers['x-vercel-ip-country-region']);
  const countryRaw = safeDecodeHeader(req.headers['x-vercel-ip-country']);

  let country = countryRaw;
  const countryUpper = countryRaw.toUpperCase();
  const countryLabels = {
    PR: 'Puerto Rico',
    US: 'United States',
  };
  if (countryUpper && countryLabels[countryUpper]) {
    country = countryLabels[countryUpper];
  }

  return { city, region, country };
}

function buildSessionIdentifier(req) {
  const ip = normalizeIp(getClientIp(req));
  const { city, region, country } = getClientLocation(req);
  const locationParts = [city, region, country].filter(Boolean);
  const locationLabel = locationParts.join(', ');

  if (locationLabel && ip !== 'unknown') {
    return `IP ${ip} ${locationLabel}`;
  }

  if (ip && ip !== 'unknown') {
    return `IP ${ip}`;
  }

  const fallback = req.headers['x-session-id'];
  return fallback ? String(fallback) : '';
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

// NEW: count pending photos for admin UI "AWAITING APPROVAL"
async function countPendingPhotos() {
  const res = await drive.files.list({
    q: `'${PENDING_FOLDER_ID}' in parents and trashed = false`,
    fields: 'files(id), nextPageToken',
    pageSize: 1000,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  const files = res.data.files || [];
  return files.length;
}

// List all files in a Drive folder (non-trashed)
async function listFilesInFolder(folderId) {
  let pageToken = null;
  const results = [];

  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime), nextPageToken',
      orderBy: 'createdTime desc', // Newest first
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

// Generic log to Google Sheets (new columns)
async function logEventToSheet(
  eventType,
  {
    email = '',
    sessionId = '',
    country = '',
    region = '',
    lastName = '',
    newsletter = null,
    ticket = '',
    timestampUtc = null
  } = {}
) {
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

  // Normalize newsletter to "Y" / "N" / ""
  const newsletterStr = getNewsletterFlag(newsletter);

  const values = [[
    utcStr,          // A timestamp_utc
    prStr,           // B timestamp_pr
    eventType,       // C event_type
    email || '',     // D email
    sessionId || '', // E session_id
    country || '',   // F country
    region || '',    // G region
    lastName || '',  // H last_name
    newsletterStr,   // I newsletter
    ticket || ''     // J ticket
  ]];

  await sheets.spreadsheets.values.append({
    spreadsheetId: SESSION_SHEET_ID,
    range: SESSION_SHEET_RANGE,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  });

  console.log('Logged event to sheet:', {
    eventType,
    utcStr,
    prStr,
    email,
    sessionId,
    country,
    region,
    lastName,
    newsletter: newsletterStr,
    ticket,
  });
}

async function resetSessionLogs() {
  if (!SESSION_SHEET_ID) {
    console.warn('SESSION_SHEET_ID missing, cannot reset logs.');
    return false;
  }

  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SESSION_SHEET_ID,
      range: 'A1:J1',
      valueInputOption: 'RAW',
      requestBody: { values: [SESSION_SHEET_HEADERS] }
    });

    await sheets.spreadsheets.values.clear({
      spreadsheetId: SESSION_SHEET_ID,
      range: 'A2:J'
    });

    return true;
  } catch (err) {
    console.error('Error resetting session logs:', err.message || err);
    return false;
  }
}

async function clearDriveFolders() {
  const result = {
    trashedCount: 0,
    errors: []
  };

  let pendingFiles = [];
  let approvedFiles = [];

  try {
    pendingFiles = await listFilesInFolder(PENDING_FOLDER_ID);
  } catch (err) {
    console.error('Error listing pending files during clear-drive:', err);
    result.errors.push('pending_list_failed');
  }

  try {
    approvedFiles = await listFilesInFolder(APPROVED_FOLDER_ID);
  } catch (err) {
    console.error('Error listing approved files during clear-drive:', err);
    result.errors.push('approved_list_failed');
  }

  const allFiles = [...pendingFiles, ...approvedFiles];

  for (const file of allFiles) {
    try {
      await drive.files.update({
        fileId: file.id,
        requestBody: { trashed: true },
        fields: 'id, trashed',
        supportsAllDrives: true,
      });
      result.trashedCount += 1;
    } catch (innerErr) {
      console.error('Error trashing file during clear-drive:', file.id, innerErr);
      result.errors.push(`trash_failed:${file.id}`);
    }
  }

  return result;
}

async function performSessionCleanup() {
  const cleanup = {
    logsCleared: false,
    trashedCount: 0,
    errors: []
  };

  try {
    cleanup.logsCleared = await resetSessionLogs();
    if (!cleanup.logsCleared) {
      cleanup.errors.push('reset_logs_failed');
    }
  } catch (err) {
    console.error('Error during reset logs cleanup:', err);
    cleanup.errors.push('reset_logs_failed');
  }

  try {
    const driveResult = await clearDriveFolders();
    cleanup.trashedCount = driveResult.trashedCount;
    cleanup.errors.push(...driveResult.errors);
  } catch (err) {
    console.error('Error during drive cleanup:', err);
    cleanup.errors.push('clear_drive_failed');
  }

  return cleanup;
}

// Read TODAY logs (in PR) from the Sheet and compute stats
async function getTodayStatsFromSheet() {
  if (!SESSION_SHEET_ID) {
    console.warn('SESSION_SHEET_ID missing, cannot compute stats.');
    return {
      visits: 0,
      forms: 0,
      uploads: 0,
      eventsForPrimeHour: [],
      newsletterEmails: []
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
      eventsForPrimeHour: [],
      newsletterEmails: []
    };
  }

  let visits = 0;
  let forms = 0;
  let uploads = 0;
  const eventsForPrimeHour = []; // { ts: string }
  const newsletterSet = new Set(); // dedupe emails

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

    // newsletter opt-in lives in column I (index 8), email in D (index 3)
    const email = row[3];
    const newsletterFlag = row[8];
    if (newsletterFlag === 'Y' && email && email.trim()) {
      // we store as-is (no lowercasing) to keep original
      newsletterSet.add(email.trim());
    }
  }

  const newsletterEmails = Array.from(newsletterSet);
  return { visits, forms, uploads, eventsForPrimeHour, newsletterEmails };
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

// Helper: build plain-text session report (used by shutdown + daily email)
function buildPlainTextSessionReport(longDate, visits, forms, uploads, prime, newsletterEmails) {
  const totalEvents = visits + forms + uploads;

  let report =
    `REPORTE DE SESION - SELFIE APP - MUNICIPIO DE MAYAGÜEZ\n` +
    `${longDate}\n\n` +
    `Visitas a la app: ${visits}\n` +
    `Formularios completados: ${forms}\n` +
    `Fotos capturadas/subidas: ${uploads}\n\n`;

  if (prime) {
    report +=
      `Horario de mayor actividad: ${formatHourRange(prime.hour)} ` +
      `(${prime.count} interacciones)\n\n`;
  } else {
    report += `No se pudo determinar un horario de mayor actividad.\n\n`;
  }

  if (newsletterEmails && newsletterEmails.length) {
    report += `Familias que aceptaron recibir noticias y ofertas:\n`;
    for (const email of newsletterEmails) {
      report += `- ${email}\n`;
    }
    report += `\n`;
  } else {
    report += `Ninguna familia aceptó recibir noticias y ofertas en esta sesión.\n\n`;
  }

  report += `Total de eventos registrados: ${totalEvents}\n`;

  return report;
}
// Live in-app report preview (no email, just returns text)
app.get('/session-report-preview', ensureAdminAuth, 
  async (req, res) => {
  try {
    const {
      visits,
      forms,
      uploads,
      eventsForPrimeHour,
      newsletterEmails = []
    } = await getTodayStatsFromSheet();

    const prime = computePrimeHour(eventsForPrimeHour);
    const longDate = formatPRDateLong();

    const reportText = buildPlainTextSessionReport(
      longDate,
      visits,
      forms,
      uploads,
      prime,
      newsletterEmails
    );

    return res.json({
      ok: true,
      reportText
    });
  } catch (err) {
    console.error('Error in /session-report-preview:', err);
    return res.status(500).json({
      ok: false,
      error: 'report_preview_failed'
    });
  }
});

// ---------- DAILY REPORT EMAIL (uses Sheet data) ----------
async function sendSessionReportEmailFromSheet() {
  if (!mailTransporter) {
    console.log('Mail transporter not configured, skipping report email.');
    return;
  }

  const {
    visits,
    forms,
    uploads,
    eventsForPrimeHour,
    newsletterEmails = []
  } = await getTodayStatsFromSheet();

  const totalEvents = visits + forms + uploads;
  if (totalEvents === 0) {
    console.log('No activity today in sheet, skipping report email.');
    return;
  }

  const prime = computePrimeHour(eventsForPrimeHour);
  const longDate = formatPRDateLong();

  const subject = 'Reporte de sesión diaria – Selfie App · Municipio de Mayagüez';

  const textReport = buildPlainTextSessionReport(
    longDate,
    visits,
    forms,
    uploads,
    prime,
    newsletterEmails
  );

  const htmlNewsletterBlock = newsletterEmails.length
    ? `
        <p style="margin:12px 0 4px 0;font-size:13px;">
          <strong>Familias que aceptaron recibir noticias y ofertas:</strong>
        </p>
        <ul style="margin:0 0 10px 20px;padding:0;font-size:12px;color:#333333;">
          ${newsletterEmails
            .map(
              (email) =>
                `<li style="margin:0 0 2px 0;">${email}</li>`
            )
            .join('')}
        </ul>
      `
    : `
        <p style="margin:12px 0 10px 0;font-size:13px;">
          <strong>Familias que aceptaron recibir noticias y ofertas:</strong>
          Ninguna familia aceptó recibir noticias y ofertas en esta sesión.
        </p>
      `;

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
                <p style="margin:0 0 6px 0%;">
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

                ${htmlNewsletterBlock}
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

// Public app settings for the main web app
app.get('/app-settings', (req, res) => {
  hydrateSettingsFromSheet()
    .then(() =>
      res.json({
        ok: true,
        enabled: appEnabled,
        settings: appSettings,
        sessionId: appSessionId
      })
    )
    .catch(() =>
      res.json({
        ok: true,
        enabled: appEnabled,
        settings: appSettings,
        sessionId: appSessionId
      })
    );
});

// Simple visit endpoint: FE can call this on page load
app.post('/ping', async (req, res) => {
  if (!appEnabled) {
    return res.status(503).json({ ok: false, error: 'app_offline' });
  }
  try {
    const sessionId = buildSessionIdentifier(req);
    await logEventToSheet('visit', { sessionId });
  } catch (e) {
    console.error('Error logging visit to sheet:', e);
  }
  res.json({ ok: true });
});

// Photo upload (counts as "upload")
app.post('/upload', express.raw({ type: 'image/*', limit: '5mb' }), async (req, res) => {
  if (!appEnabled) {
    return res.status(503).json({ error: 'app_offline', message: 'Selfie app is offline for this event.' });
  }

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
      const sessionId = buildSessionIdentifier(req);
      await logEventToSheet('upload', {
        sessionId,
        ticket: ticketLabel // e.g. "T015"
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
  if (!appEnabled) {
    return res.status(503).json({ ok: false, error: 'app_offline' });
  }
  try {
    const { country, lastName, email, newsletter, timestamp } = req.body || {};

    const timestampUtc = timestamp || new Date().toISOString();
    const newsletterFlag = getNewsletterFlag(newsletter);
    const newsletterLabel = newsletterFlag === 'Y' ? 'Sí' : 'No';

    // Split "region, country" if it comes combined
    let countryClean = '';
    let region = '';

    if (country) {
      const parts = String(country)
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);

      if (parts.length >= 2) {
        region = parts[0];       // municipality / region
        countryClean = parts[1]; // country
      } else if (parts.length === 1) {
        countryClean = parts[0];
      }
    }

    // Log to Sheet as "form"
    try {
      const sessionId = buildSessionIdentifier(req);
      await logEventToSheet('form', {
        email,
        sessionId,
        timestampUtc,
        country: countryClean,
        region,
        lastName,
        newsletter
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
      `Acepta recibir noticias y ofertas de MUNICIPIO DE MAYAGÜEZ.: ${newsletterLabel}\n\n` +
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
                  <strong>Acepta recibir noticias y ofertas:</strong> ${newsletterLabel}
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

// --------- ADMIN APP STATUS (for toggle) ----------

app.get('/admin/app-status', ensureAdminAuth, (req, res) => {
  hydrateSettingsFromSheet()
    .then(() => res.json({ ok: true, enabled: appEnabled }))
    .catch(() => res.json({ ok: true, enabled: appEnabled }));
});

app.post('/admin/app-status', ensureAdminAuth, async (req, res) => {
  try {
    const { enabled, accessCode } = req.body || {};

    if (typeof enabled !== 'boolean') {
      return res.status(400).json({ error: 'missing_enabled_flag' });
    }

    if (!enabled) {
      if (!accessCode || typeof accessCode !== 'string') {
        return res.status(400).json({ error: 'missing_access_code' });
      }

      if (!ADMIN_ACCESS_CODE && !ADMIN_UNLOCK_KEY) {
        return res.status(500).json({ error: 'admin_code_not_configured' });
      }

      if (accessCode !== ADMIN_ACCESS_CODE && accessCode !== ADMIN_UNLOCK_KEY) {
        return res.status(401).json({ error: 'invalid_access_code' });
      }
      appEnabled = false;
    } else {
      if (!appEnabled) {
        appSessionId = generateServerSessionId();
      }
      appEnabled = true;
    }

    const persisted = await writeSettingsToSheet(appSettings, appEnabled, appSessionId);
    if (!persisted) {
      console.warn('Unable to persist settings during app-status update.');
      return res.json({
        ok: true,
        enabled: appEnabled,
        warning: 'settings_persist_failed'
      });
    }

    return res.json({ ok: true, enabled: appEnabled });
  } catch (err) {
    console.error('Error updating app status:', err);
    return res.status(500).json({ ok: false, error: 'app_status_failed' });
  }
});

// Admin: get current app settings
app.get('/admin/settings', ensureAdminAuth, (req, res) => {
  hydrateSettingsFromSheet()
    .then(() => res.json({ ok: true, settings: appSettings }))
    .catch(() => res.json({ ok: true, settings: appSettings }));
});

// Admin: update app settings
app.post('/admin/settings', ensureAdminAuth, async (req, res) => {
  try {
    appSettings = mergeAppSettings(req.body || {});
    const persisted = await writeSettingsToSheet(appSettings, appEnabled, appSessionId);
    if (!persisted) {
      return res.status(500).json({ ok: false, error: 'settings_persist_failed' });
    }
    settingsLoadedFromSheet = true;
    return res.json({ ok: true, settings: appSettings });
  } catch (err) {
    console.error('Error updating app settings:', err);
    return res.status(400).json({ ok: false, error: 'invalid_settings' });
  }
});

// --------- ADMIN SHUTDOWN (toggle OFF) ----------

app.post('/admin/shutdown', ensureAdminAuth, async (req, res) => {
  try {
    const { accessCode } = req.body || {};

    if (!accessCode || typeof accessCode !== 'string') {
      return res.status(400).json({ error: 'missing_access_code' });
    }

    if (!ADMIN_ACCESS_CODE && !ADMIN_UNLOCK_KEY) {
      return res.status(500).json({ error: 'admin_code_not_configured' });
    }

    // Require re-entering the admin code (or unlock key if you want)
    if (accessCode !== ADMIN_ACCESS_CODE && accessCode !== ADMIN_UNLOCK_KEY) {
      return res.status(401).json({ error: 'invalid_access_code' });
    }

    // Compute session stats and build report text
    const {
      visits,
      forms,
      uploads,
      eventsForPrimeHour,
      newsletterEmails = []
    } = await getTodayStatsFromSheet();

    const prime = computePrimeHour(eventsForPrimeHour);
    const longDate = formatPRDateLong();

    const reportText = buildPlainTextSessionReport(
      longDate,
      visits,
      forms,
      uploads,
      prime,
      newsletterEmails
    );

    // Set app to offline
    appEnabled = false;

    // Optionally also send the same report via email on shutdown
    if (mailTransporter) {
      try {
        await mailTransporter.sendMail({
          from: `"Luminar Apps" <${process.env.MAIL_FROM || process.env.MAIL_USER}>`,
          to: process.env.MAIL_TO || process.env.MAIL_USER,
          subject: 'Reporte de cierre – Selfie App · Municipio de Mayagüez',
          text: reportText,
          attachments: [
            {
              filename: 'reporte_sesion.txt',
              content: reportText
            }
          ]
        });
        console.log('Shutdown report email sent.');
      } catch (mailErr) {
        console.error('Error sending shutdown report email:', mailErr);
      }
    } else {
      console.log('Mail transporter not configured; shutdown report email skipped.');
    }

    const cleanup = await performSessionCleanup();
    if (cleanup.errors.length) {
      console.warn('Shutdown cleanup encountered errors:', cleanup.errors);
    }

    const persisted = await writeSettingsToSheet(appSettings, appEnabled, appSessionId);
    if (!persisted) {
      console.warn('Shutdown: failed to persist settings sheet status.');
    }

    // Return report text to the admin UI so it can be downloaded as .txt
    return res.json({
      ok: true,
      enabled: false,
      reportText,
      cleanup
    });
  } catch (err) {
    console.error('Error in /admin/shutdown:', err);
    return res.status(500).json({ ok: false, error: 'shutdown_failed' });
  }
});

// --------- ADMIN API: review photos ----------

// Get next pending photo (for review UI)
app.get('/admin/next-photo', ensureAdminAuth, async (req, res) => {
  try {
    if (!appEnabled) {
      // app offline – no photo, but we keep response shape compatible
      return res.json({ empty: true, reason: 'app_offline', pendingCount: 0 });
    }

    // Get oldest pending photo + how many remain
    const [file, pendingCount] = await Promise.all([
      getNextPendingPhoto(),
      countPendingPhotos()
    ]);

    if (!file) {
      return res.json({ empty: true, pendingCount });
    }

    // NEW: photoNumber based on filename ticket (T001-…)
    const photoNumber = extractTicketNumber(file.name);

    res.json({
      empty: false,
      fileId: file.id,
      name: file.name,
      photoNumber,    // e.g. 1, 2, 3…
      pendingCount    // total pending in folder
    });
  } catch (err) {
    console.error('Error fetching next pending photo:', err);
    res.status(500).json({ error: 'failed_next_photo' });
  }
});

// Serve the actual image bytes so admin.html can <img src="...">
// Get photo thumbnail (smaller, faster loading for grid)
app.get('/admin/photo/:id/thumbnail', ensureAdminAuth, async (req, res) => {
  const fileId = req.params.id;

  try {
    // Get thumbnail link from Drive metadata
    const fileMeta = await drive.files.get({
      fileId,
      fields: 'thumbnailLink, webContentLink'
    });

    // If Drive provides a thumbnail, redirect to it
    if (fileMeta.data.thumbnailLink) {
      return res.redirect(fileMeta.data.thumbnailLink);
    }

    // Fallback: serve full image if no thumbnail available
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
        console.error('Error streaming photo thumbnail:', err);
        if (!res.headersSent) {
          res.status(500).end('Error streaming file');
        }
      })
      .pipe(res);
  } catch (err) {
    console.error('Error getting photo thumbnail from Drive:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'failed_photo_thumbnail' });
    }
  }
});

// Get full resolution photo (for downloads and overlay preview)
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
// List files in folder with pagination support
async function listFilesInFolderPaginated(folderId, page = 1, pageSize = 24) {
  const pageNumber = Math.max(1, parseInt(page, 10) || 1);
  const size = Math.max(1, Math.min(100, parseInt(pageSize, 10) || 24));
  
  let pageToken = null;
  const allResults = [];
  let totalCount = 0;

  // First, get total count by fetching all (or we could optimize this later)
  // For now, we'll fetch all pages but only return the requested page
  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime), nextPageToken',
      orderBy: 'createdTime desc', // Newest first
      pageSize: 100,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    });

    const files = res.data.files || [];
    for (const f of files) {
      allResults.push({
        id: f.id,
        name: f.name,
        createdTime: f.createdTime,
      });
    }

    pageToken = res.data.nextPageToken;
  } while (pageToken);

  totalCount = allResults.length;

  // Calculate pagination
  const startIndex = (pageNumber - 1) * size;
  const endIndex = startIndex + size;
  const paginatedResults = allResults.slice(startIndex, endIndex);

  return {
    files: paginatedResults,
    total: totalCount,
    page: pageNumber,
    pageSize: size,
    totalPages: Math.ceil(totalCount / size),
  };
}

app.get('/admin/approved-list', ensureAdminAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const pageSize = parseInt(req.query.pageSize, 10) || 24;
    
    const result = await listFilesInFolderPaginated(APPROVED_FOLDER_ID, page, pageSize);
    // admin UI can build thumb src as /admin/photo/:id?token=...
    res.json({ 
      ok: true, 
      files: result.files,
      total: result.total,
      page: result.page,
      pageSize: result.pageSize,
      totalPages: result.totalPages
    });
  } catch (err) {
    console.error('Error listing approved files for admin:', err);
    res.status(500).json({ ok: false, error: 'list_approved_failed' });
  }
});

// --------- EVENT LOGS (for admin dashboard) ---------
// Uses the SAME sheet (SESSION_SHEET_ID / A:J) that already stores all events.

app.get('/admin/event-logs', ensureAdminAuth, async (req, res) => {
  try {
    if (!SESSION_SHEET_ID) {
      console.warn('SESSION_SHEET_ID missing, cannot read event logs.');
      return res.json({ ok: true, events: [] });
    }

    const max = Math.min(parseInt(req.query.limit, 10) || 12, 50);

    const apiRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SESSION_SHEET_ID,
      range: SESSION_SHEET_RANGE, // "A:J"
    });

    const rows = apiRes.data.values || [];
    if (rows.length <= 1) {
      // Only headers
      return res.json({ ok: true, events: [] });
    }

    // Skip header row, then take last N and reverse so newest is on top
    const dataRows = rows.slice(1);
    const lastRows = dataRows.slice(-max).reverse();

    const events = lastRows.map((row) => {
      const tsUtc = row[0] || '';  // timestamp_utc (ISO)
      const tsPr  = row[1] || '';  // timestamp_pr "DD/MM/YYYY HH:MM:SS"
      const eventType = row[2] || '';
      const email     = row[3] || '';
      const sessionId = row[4] || ''; // session_id (e.g., "IP 172.225.248.16 San Juan, Puerto Rico")
      const country = row[5] || '';
      const region = row[6] || '';
      const newsletterFlag = row[8] || ''; // "Y" / "N" / ""

      // Extract location from session_id if country is not available
      // Format: "IP 172.225.248.16 San Juan, Puerto Rico"
      let locationFromSession = '';
      if (!country && sessionId) {
        const ipMatch = sessionId.match(/^IP\s+[\d.]+\s+(.+)$/);
        if (ipMatch && ipMatch[1]) {
          locationFromSession = ipMatch[1].trim(); // "San Juan, Puerto Rico"
        }
      }

      // --- TIME LABEL (we prefer PR time if available) ---
      let timeText = '';
      if (tsPr) {
        // "DD/MM/YYYY HH:MM:SS" -> keep HH:MM
        const parts = tsPr.split(' ');
        if (parts.length >= 2) {
          timeText = parts[1].slice(0, 5); // "HH:MM"
        } else {
          timeText = tsPr;
        }
      } else if (tsUtc) {
        const d = new Date(tsUtc);
        if (!Number.isNaN(d.getTime())) {
          timeText = d.toLocaleTimeString('en-US', {
            hour: 'numeric',
            minute: '2-digit',
          });
        } else {
          timeText = tsUtc;
        }
      } else {
        timeText = '--:--';
      }

      // --- TEXT LABEL (what right panel shows) ---
      let text = 'Activity';

      switch (eventType) {
        case 'visit': {
          // Use country if available, otherwise extract from session_id
          const location = country || locationFromSession;
          if (location) {
            text = `New visit from ${location.toUpperCase()}`;
          } else {
            text = 'New visit';
          }
          break;
        }
        case 'form':
          if (newsletterFlag === 'Y') {
            text = 'User submitted a form & subscribed to newsletter';
          } else {
            text = 'User submitted a form';
          }
          break;
        case 'upload':
          text = 'User uploaded a photo';
          break;
        default:
          // If we ever start logging other types, at least show the type
          text = eventType || 'Activity';
          break;
      }

      return { time: timeText, text };
    });

    return res.json({ ok: true, events });
  } catch (err) {
    console.error('Error loading event logs:', err);
    return res.status(500).json({ ok: false, error: 'event_log_fetch_failed' });
  }
});

app.get('/admin/activity-stats', ensureAdminAuth, async (req, res) => {
  try {
    const { eventsForPrimeHour } = await getTodayStatsFromSheet();
    const counts = Array.from({ length: 24 }, () => 0);

    for (const ev of eventsForPrimeHour) {
      const ts = new Date(ev.ts);
      if (Number.isNaN(ts.getTime())) continue;
      const prDate = toPR(ts);
      const hour = prDate.getHours();
      counts[hour] += 1;
    }

    const currentHour = toPR(new Date()).getHours();

    return res.json({ ok: true, counts, currentHour });
  } catch (err) {
    console.error('Error loading activity stats:', err);
    return res.status(500).json({ ok: false, error: 'activity_stats_failed' });
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
    const result = await clearDriveFolders();
    res.json({ ok: true, trashedCount: result.trashedCount, errors: result.errors });
  } catch (err) {
    console.error('Error clearing Drive folders:', err);
    res.status(500).json({ ok: false, error: 'clear_drive_failed' });
  }
});

// Reset logs (real): clears event logs but keeps header row
app.post('/admin/reset-logs', ensureAdminAuth, async (req, res) => {
  try {
    const ok = await resetSessionLogs();
    if (!ok) {
      return res.status(500).json({ ok: false, error: 'reset_logs_failed' });
    }
    res.json({ ok: true, message: 'reset_logs_completed' });
  } catch (err) {
    console.error('Error in reset-logs:', err);
    res.status(500).json({ ok: false, error: 'reset_logs_failed' });
  }
});

// --------- PUBLIC GALLERY API (for Yodeck / gallery.html) ----------

// List approved photos (respects galleryDisplayLimit setting)
app.get('/gallery/approved', async (req, res) => {
  try {
    // Ensure settings are hydrated
    await hydrateSettingsFromSheet();

    const response = await drive.files.list({
      q: `'${APPROVED_FOLDER_ID}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime)',
      orderBy: 'createdTime asc',
      pageSize: 1000,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    const all = response.data.files || [];
    const displayLimit = appSettings.galleryDisplayLimit || 'all';

    let selected;

    if (displayLimit === 'all') {
      // Show all photos
      selected = all;
    } else if (displayLimit === 'last10') {
      // Show last 10 photos (most recent)
      selected = all.slice(-10);
    } else if (displayLimit === 'last25') {
      // Show last 25 photos (most recent)
      selected = all.slice(-25);
    } else {
      // Fallback to all if invalid setting
      selected = all;
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

// Generate report for a specific PR date (YYYY-MM-DD)
app.post('/session-report-date', async (req, res) => {
  try {
    const { date } = req.body || {};
    if (!date) {
      return res.status(400).json({ ok: false, error: "missing_date" });
    }

    // date = "2025-12-06"
    const [yyyy, mm, dd] = date.split("-");
    const prefix = `${dd}/${mm}/${yyyy}`; // matches timestamp_pr format

    // Pull entire sheet -----------------------------------------
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SESSION_SHEET_ID,
      range: SESSION_SHEET_RANGE
    });

    const rows = resp.data.values || [];
    if (rows.length <= 1) {
      return res.json({ ok: true, reportText: `No data for ${date}.` });
    }

    // Filter rows belonging to this date -------------------------
    const filtered = [];
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const tsPr = row[1] || ""; // timestamp_pr
      if (tsPr.startsWith(prefix)) filtered.push(row);
    }

    // Count metrics ---------------------------------------------
    let visits = 0;
    let forms = 0;
    let uploads = 0;
    let newsletter = 0;

    const newsletterEmails = new Set();

    for (const row of filtered) {
      const eventType = row[2]; // event_type
      const email = row[3];     // email
      const newsletterFlag = row[8]; // I col

      if (eventType === "visit") visits++;
      else if (eventType === "form") forms++;
      else if (eventType === "upload") uploads++;

      if (newsletterFlag === "Y" && email) {
        newsletterEmails.add(email.trim());
      }
    }

    // Build final report ----------------------------------------
    const report =
      `SELFIE APP REPORT – ${date}\n` +
      `--------------------------------------\n` +
      `Visits: ${visits}\n` +
      `Forms: ${forms}\n` +
      `Uploads: ${uploads}\n` +
      `Newsletter Opt-ins: ${newsletterEmails.size}\n\n` +
      (newsletterEmails.size
        ? `Emails:\n${Array.from(newsletterEmails).map(e => "- " + e).join("\n")}\n`
        : ``) +
      `--------------------------------------\n` +
      `Generated at: ${new Date().toISOString()}`;

    // Email it ---------------------------------------------------
    if (mailTransporter && process.env.MAIL_TO) {
      await mailTransporter.sendMail({
        from: `"Luminar Apps" <${process.env.MAIL_FROM || process.env.MAIL_USER}>`,
        to: process.env.MAIL_TO,
        subject: `Selfie App Report – ${date}`,
        text: report,
        attachments: [
          {
            filename: `report_${date}.txt`,
            content: report
          }
        ]
      });
    }

    return res.json({
      ok: true,
      reportText: report
    });

  } catch (err) {
    console.error("Error in /session-report-date:", err);
    return res.status(500).json({ ok: false, error: "report_failed" });
  }
});

// Get photo info (email, country, last name) by ticket number
async function getEmailForPhoto(ticketNumber) {
  try {
    if (!SESSION_SHEET_ID) {
      console.warn('SESSION_SHEET_ID missing, cannot match photo to email');
      return null;
    }

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SESSION_SHEET_ID,
      range: SESSION_SHEET_RANGE,
    });

    const rows = response.data.values || [];
    if (rows.length < 2) return null; // No data (header + rows)

    // Skip header row, map to objects
    const events = rows.slice(1).map(row => ({
      timestamp_utc: row[0] || '',
      timestamp_pr: row[1] || '',
      event_type: row[2] || '',
      email: row[3] || '',
      session_id: row[4] || '',
      country: row[5] || '',
      region: row[6] || '',
      last_name: row[7] || '',
      newsletter: row[8] || '',
      ticket: row[9] || '',
    }));

    // Find upload event with this ticket number
    const uploadEvent = events.find(
      e => e.event_type === 'upload' && e.ticket === ticketNumber
    );

    if (!uploadEvent || !uploadEvent.session_id) {
      return null;
    }

    const sessionId = uploadEvent.session_id;
    const uploadTime = new Date(uploadEvent.timestamp_utc);

    // Find form events with same session_id
    // Prefer form events that happened before or close to upload time
    const formEvents = events
      .filter(e => 
        e.event_type === 'form' && 
        e.session_id === sessionId &&
        e.email // Must have email
      )
      .map(e => ({
        ...e,
        timeDiff: Math.abs(new Date(e.timestamp_utc) - uploadTime),
      }))
      .sort((a, b) => a.timeDiff - b.timeDiff);

    if (formEvents.length > 0) {
      const bestMatch = formEvents[0];
      return {
        email: bestMatch.email || '',
        country: bestMatch.country || '',
        region: bestMatch.region || '',
        last_name: bestMatch.last_name || '',
        newsletter: bestMatch.newsletter === 'Y',
        session_id: sessionId,
        timestamp: bestMatch.timestamp_utc,
        confidence: 'high',
      };
    }

    return null;
  } catch (err) {
    console.error('Error matching photo to email:', err);
    return null;
  }
}

// Admin endpoint to get photo info by ticket number
app.get('/admin/photo-info/:ticketNumber', ensureAdminAuth, async (req, res) => {
  try {
    const ticketNumber = req.params.ticketNumber; // e.g., "T001"
    
    if (!ticketNumber) {
      return res.status(400).json({ ok: false, error: 'missing_ticket_number' });
    }

    const match = await getEmailForPhoto(ticketNumber);
    
    if (match) {
      return res.json({ ok: true, match });
    } else {
      return res.json({ ok: false, error: 'no_match_found' });
    }
  } catch (err) {
    console.error('Error getting photo info:', err);
    res.status(500).json({ ok: false, error: 'server_error' });
  }
});

// ---------- TEMPLATE API ENDPOINTS ----------

// Ensure templates sheet has headers
async function ensureTemplatesSheetHeaders() {
  if (!TEMPLATES_SHEET_ID) return false;
  try {
    const ready = await ensureTemplatesSheet();
    if (!ready) return false;

    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: TEMPLATES_SHEET_ID,
      range: `${TEMPLATES_SHEET_NAME}!A1:D1`
    });

    const rows = resp.data.values || [];
    if (rows.length === 0 || !rows[0] || rows[0][0] !== 'Name') {
      await sheets.spreadsheets.values.update({
        spreadsheetId: TEMPLATES_SHEET_ID,
        range: `${TEMPLATES_SHEET_NAME}!A1:D1`,
        valueInputOption: 'RAW',
        requestBody: { values: [['Name', 'Template JSON', 'Created', 'Active']] }
      });
    }
    return true;
  } catch (err) {
    console.warn('Unable to ensure templates sheet headers:', err.message || err);
    return false;
  }
}

// Initialize templates sheet on startup
ensureTemplatesSheetHeaders().catch(() => {});

// List all templates
app.get('/admin/templates', ensureAdminAuth, async (req, res) => {
  try {
    const templates = await readTemplatesFromSheet();
    res.json({ ok: true, templates });
  } catch (err) {
    console.error('Error listing templates:', err);
    res.status(500).json({ ok: false, error: 'list_templates_failed' });
  }
});

// Get single template
app.get('/admin/templates/:id', ensureAdminAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const templates = await readTemplatesFromSheet();
    const template = templates.find(t => String(t.id) === String(id));
    
    if (!template) {
      return res.status(404).json({ ok: false, error: 'template_not_found' });
    }
    
    res.json({ ok: true, template });
  } catch (err) {
    console.error('Error getting template:', err);
    res.status(500).json({ ok: false, error: 'get_template_failed' });
  }
});

// Create new template
app.post('/admin/templates', ensureAdminAuth, async (req, res) => {
  try {
    const { name, data } = req.body || {};
    
    if (!name || !data) {
      return res.status(400).json({ ok: false, error: 'missing_name_or_data' });
    }

    const result = await writeTemplateToSheet(name, data, true);
    
    if (!result) {
      return res.status(500).json({ ok: false, error: 'save_template_failed' });
    }

    res.json({ ok: true, template: result });
  } catch (err) {
    console.error('Error creating template:', err);
    res.status(500).json({ ok: false, error: 'create_template_failed' });
  }
});

// Update template
app.put('/admin/templates/:id', ensureAdminAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, data, isActive } = req.body || {};
    
    if (!name || !data) {
      return res.status(400).json({ ok: false, error: 'missing_name_or_data' });
    }

    const success = await updateTemplateInSheet(id, name, data, isActive !== false);
    
    if (!success) {
      return res.status(500).json({ ok: false, error: 'update_template_failed' });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('Error updating template:', err);
    res.status(500).json({ ok: false, error: 'update_template_failed' });
  }
});

// Delete template
app.delete('/admin/templates/:id', ensureAdminAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const success = await deleteTemplateFromSheet(id);
    
    if (!success) {
      return res.status(500).json({ ok: false, error: 'delete_template_failed' });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('Error deleting template:', err);
    res.status(500).json({ ok: false, error: 'delete_template_failed' });
  }
});

// Upload template asset (background or logo) to Drive
app.post('/admin/templates/upload-asset', ensureAdminAuth, multer({ storage: multer.memoryStorage() }).single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: 'missing_file' });
    }

    const { assetType } = req.body || {}; // 'background' or 'logo'
    
    if (!assetType || (assetType !== 'background' && assetType !== 'logo')) {
      return res.status(400).json({ ok: false, error: 'invalid_asset_type' });
    }

    const folderId = assetType === 'background' ? TEMPLATES_BG_FOLDER_ID : TEMPLATES_LOGO_FOLDER_ID;
    const timestamp = Date.now();
    const extension = req.file.originalname.split('.').pop() || 'jpg';
    const filename = `template_${assetType}_${timestamp}.${extension}`;

    const bufferStream = new stream.PassThrough();
    bufferStream.end(req.file.buffer);

    const response = await drive.files.create({
      requestBody: {
        name: filename,
        mimeType: req.file.mimetype,
        parents: [folderId]
      },
      media: {
        mimeType: req.file.mimetype,
        body: bufferStream
      },
      supportsAllDrives: true
    });

    res.json({
      ok: true,
      fileId: response.data.id,
      filename: response.data.name,
      type: assetType
    });
  } catch (err) {
    console.error('Error uploading template asset:', err);
    res.status(500).json({ ok: false, error: 'upload_asset_failed' });
  }
});

// Serve uploaded template asset from Drive
app.get('/admin/templates/asset/:fileId', ensureAdminAuth, async (req, res) => {
  const { fileId } = req.params;

  try {
    const driveRes = await drive.files.get(
      {
        fileId,
        alt: 'media'
      },
      { responseType: 'stream' }
    );

    // Try to get file metadata for content type
    try {
      const metadata = await drive.files.get({
        fileId,
        fields: 'mimeType'
      });
      res.setHeader('Content-Type', metadata.data.mimeType || 'image/jpeg');
    } catch {
      res.setHeader('Content-Type', 'image/jpeg');
    }

    driveRes.data
      .on('error', (err) => {
        console.error('Drive stream error (template asset):', err);
        if (!res.headersSent) {
          res.end();
        }
      })
      .pipe(res);
  } catch (err) {
    console.error('Error streaming template asset:', err);
    if (!res.headersSent) {
      res.status(404).end();
    }
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
