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
// Add both of these in Vercel Project Settings -> Environment Variables.
const PHOTO_TEMPLATES_FOLDER_ID =
  process.env.PHOTO_TEMPLATES_FOLDER_ID ||
  '1X-JhUja-ifObDuNiL6a4pOUqjzhTpONE'; // flattened live overlay images
const TEMPLATES_QR_FOLDER_ID =
  process.env.TEMPLATES_QR_FOLDER_ID ||
  '1X824xBfxPLJ6-mL-oxEw_dZhUiQ0IRrZ'; // uploaded QR assets for templates

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

const METRICS_SHEET_ID =
  process.env.METRICS_SHEET_ID ||
  '15hccG6-KL6gGaw08gqYGYM55GrgLB23mBD6fMRBpka0';
const METRICS_EVENTS_SHEET_NAME = process.env.METRICS_EVENTS_SHEET_NAME || 'Events';
const METRICS_DAILY_SHEET_NAME = process.env.METRICS_DAILY_SHEET_NAME || 'Daily Summary';
const METRICS_NEWSLETTER_SHEET_NAME = process.env.METRICS_NEWSLETTER_SHEET_NAME || 'Newsletter Leads';
const METRICS_SCHEMA_SHEET_NAME = process.env.METRICS_SCHEMA_SHEET_NAME || 'Schema';
const METRICS_EVENTS_SHEET_RANGE = `${METRICS_EVENTS_SHEET_NAME}!A:X`;

// Range with columns:
// timestamp_utc, timestamp_pr, event_type, email, session_id,
// country, region, last_name, newsletter, ticket
const SESSION_SHEET_RANGE = 'A:J';
const SETTINGS_SHEET_RANGE = `${SETTINGS_SHEET_NAME}!A:B`;
const SETTINGS_SERVER_SESSION_KEY = 'Server Session';
const DEFAULT_TEMPLATE_PHOTO_BOX = Object.freeze({
  x: 210,
  y: 40,
  width: 1500,
  height: 1000,
});
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
const METRICS_EVENTS_HEADERS = [
  'event_id',
  'timestamp_utc',
  'timestamp_pr',
  'pr_date',
  'pr_hour',
  'event_type',
  'event_family',
  'session_id',
  'server_session_id',
  'email',
  'email_normalized',
  'country',
  'region',
  'location_label',
  'last_name',
  'newsletter',
  'ticket',
  'source',
  'request_ip',
  'user_agent',
  'referrer',
  'path',
  'method',
  'metadata_json'
];
const METRICS_DAILY_HEADERS = [
  'pr_date',
  'visits',
  'forms',
  'uploads',
  'newsletter_opt_ins',
  'unique_sessions',
  'unique_emails',
  'updated_at_utc'
];
const METRICS_NEWSLETTER_HEADERS = [
  'timestamp_utc',
  'timestamp_pr',
  'pr_date',
  'email',
  'email_normalized',
  'last_name',
  'country',
  'region',
  'location_label',
  'session_id',
  'server_session_id',
  'source',
  'metadata_json'
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
  activeTemplateId: '',
  activeTemplateSnapshot: '',
  galleryRuntimeCommand: {
    command: '',
    commandId: '',
    issuedAt: '',
    reason: '',
    clearCache: false,
  },
  intro: {
    title: '¿Desde dónde nos visitas? 😊',
    subtitle: 'Selecciona tu país y municipio/estado, escribe tus apellidos y continúa a tu selfie.'
  },
  form: {
    locationEnabled: true,
    showCountryFlag: true,
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
  },
  galleryRuntime: {
    enablePiSafeLegacyRuntime: true,
    enablePiStablePlayerMode: true,
    enableBlobPhotoLoader: false,
    enableGalleryCrossfade: true,
    enableDirectSwapFallback: true,
    enableFlattenedOverlay: false,
    enableOverlayPolling: false,
    enableDynamicTemplateRuntime: false,
    enableTemplatePolling: false,
    enableGalleryDebugOverlay: true,
    enableRuntimeWatchdog: false,
    enableDirectSwapMode: false,
  }
};

let appSettings = JSON.parse(JSON.stringify(DEFAULT_APP_SETTINGS));
let settingsSheetReady = false;
let settingsLoadedFromSheet = false;
let settingsLastHydratedAt = 0;
let settingsHydrationPromise = null;
const SETTINGS_REFRESH_TTL_MS = 2000;
let metricsSheetsReady = false;
let metricsSheetHeadersReady = false;
const DEVICE_ONLINE_WINDOW_MS = 2 * 60 * 1000;
const DEVICE_WARNING_WINDOW_MS = 15 * 1000;
const DEVICE_OFFLINE_WINDOW_MS = 30 * 1000;
const DEVICE_HISTORY_WINDOW_MS = 24 * 60 * 60 * 1000;
const NON_FAILURE_DEVICE_ERROR_CATEGORIES = new Set(['lifecycle', 'watchdog']);
const DEVICE_FETCH_FAILURE_BURST_THRESHOLD = 5;
const DEVICE_FETCH_FAILURE_BURST_WINDOW_MS = 60 * 1000;
const DEVICE_OLD_CHROMIUM_WARNING_VERSION = 110;
const MAX_DEVICE_ERRORS = 20;
const MAX_DEVICE_LOGS = 200;
const liveGalleryDevices = new Map();

const SETTINGS_FIELDS = [
  { key: 'Ticket Overlay Enabled', type: 'boolean', path: ['ticketEnabled'] },
  { key: 'Gallery Display Limit', type: 'string', path: ['galleryDisplayLimit'] },
  { key: 'Active Template ID', type: 'string', path: ['activeTemplateId'] },
  { key: 'Active Template Snapshot', type: 'string', path: ['activeTemplateSnapshot'] },
  { key: 'Pi Safe Legacy Runtime Enabled', type: 'boolean', path: ['galleryRuntime', 'enablePiSafeLegacyRuntime'] },
  { key: 'Pi Stable Player Mode Enabled', type: 'boolean', path: ['galleryRuntime', 'enablePiStablePlayerMode'] },
  { key: 'Blob Photo Loader Enabled', type: 'boolean', path: ['galleryRuntime', 'enableBlobPhotoLoader'] },
  { key: 'Gallery Crossfade Enabled', type: 'boolean', path: ['galleryRuntime', 'enableGalleryCrossfade'] },
  { key: 'Direct Swap Fallback Enabled', type: 'boolean', path: ['galleryRuntime', 'enableDirectSwapFallback'] },
  { key: 'Flattened Overlay Enabled', type: 'boolean', path: ['galleryRuntime', 'enableFlattenedOverlay'] },
  { key: 'Overlay Polling Enabled', type: 'boolean', path: ['galleryRuntime', 'enableOverlayPolling'] },
  { key: 'Dynamic Template Runtime Enabled', type: 'boolean', path: ['galleryRuntime', 'enableDynamicTemplateRuntime'] },
  { key: 'Template Polling Enabled', type: 'boolean', path: ['galleryRuntime', 'enableTemplatePolling'] },
  { key: 'Gallery Debug Overlay Enabled', type: 'boolean', path: ['galleryRuntime', 'enableGalleryDebugOverlay'] },
  { key: 'Runtime Watchdog Enabled', type: 'boolean', path: ['galleryRuntime', 'enableRuntimeWatchdog'] },
  { key: 'Direct Swap Mode Enabled', type: 'boolean', path: ['galleryRuntime', 'enableDirectSwapMode'] },
  { key: 'Intro Title', type: 'string', path: ['intro', 'title'] },
  { key: 'Intro Subtitle', type: 'string', path: ['intro', 'subtitle'] },
  { key: 'Location Enabled', type: 'boolean', path: ['form', 'locationEnabled'] },
  { key: 'Show Country Flag', type: 'boolean', path: ['form', 'showCountryFlag'] },
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
const GALLERY_RUNTIME_COMMAND_ROW_KEY = 'Gallery Runtime Command';

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

function clampString(value, maxLength = 300) {
  if (value == null) return '';
  const str = String(value);
  return str.length > maxLength ? str.slice(0, maxLength) : str;
}

function clampNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function clampBoolean(value) {
  return typeof value === 'boolean' ? value : null;
}

function clampIsoTimestamp(value) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toISOString();
}

function clampRoutePath(value, maxLength = 240) {
  const raw = clampString(value, maxLength);
  if (!raw) return '';
  const trimmed = raw.split('?')[0].trim();
  return trimmed.length > maxLength ? trimmed.slice(0, maxLength) : trimmed;
}

function clampDeviceStatusFlag(value) {
  const normalized = clampString(value || '', 20).toLowerCase();
  return normalized === 'success' || normalized === 'fail' ? normalized : '';
}

function sanitizeDeviceMemory(rawMemory) {
  if (!rawMemory || typeof rawMemory !== 'object') return null;
  const usedJSHeapSize = clampNumber(rawMemory.usedJSHeapSize);
  const totalJSHeapSize = clampNumber(rawMemory.totalJSHeapSize);
  const jsHeapSizeLimit = clampNumber(rawMemory.jsHeapSizeLimit);

  if (usedJSHeapSize == null && totalJSHeapSize == null && jsHeapSizeLimit == null) {
    return null;
  }

  return {
    usedJSHeapSize,
    totalJSHeapSize,
    jsHeapSizeLimit,
  };
}

function sanitizeDeviceErrors(rawErrors) {
  if (!Array.isArray(rawErrors)) return [];

  return rawErrors
    .slice(-MAX_DEVICE_ERRORS)
    .map((entry) => {
      const item = entry && typeof entry === 'object' ? entry : {};
      return {
        at: clampIsoTimestamp(item.at) || new Date().toISOString(),
        type: clampString(item.type || item.kind || 'error', 80),
        category: clampString(item.category || item.type || 'unknown', 20).toLowerCase() || 'unknown',
        route: clampRoutePath(item.route || item.url || item.path || '', 240),
        message: clampString(item.message || item.error || '', 500),
        currentPhotoId: clampString(item.currentPhotoId || '', 120),
        currentPhotoName: clampString(item.currentPhotoName || '', 200),
      };
    })
    .filter((entry) => entry.message && !NON_FAILURE_DEVICE_ERROR_CATEGORIES.has(entry.category));
}

function sanitizeDeviceStatus(rawStatus) {
  const status = rawStatus && typeof rawStatus === 'object' ? rawStatus : {};
  const overlayStatus = status.overlayStatus && typeof status.overlayStatus === 'object'
    ? {
        enabled: clampBoolean(status.overlayStatus.enabled),
        overlayFileId: clampString(status.overlayStatus.overlayFileId || '', 120),
        overlayVersion: clampString(status.overlayStatus.overlayVersion || '', 120),
      }
    : null;
  const templateStatus = status.templateStatus && typeof status.templateStatus === 'object'
    ? {
        enabled: clampBoolean(status.templateStatus.enabled),
        activeTemplateKey: clampString(status.templateStatus.activeTemplateKey || '', 200),
      }
    : null;
  const galleryRuntimeSettings = status.galleryRuntimeSettings && typeof status.galleryRuntimeSettings === 'object'
    ? {
        enablePiSafeLegacyRuntime: clampBoolean(status.galleryRuntimeSettings.enablePiSafeLegacyRuntime),
        enablePiStablePlayerMode: clampBoolean(status.galleryRuntimeSettings.enablePiStablePlayerMode),
        enableBlobPhotoLoader: clampBoolean(status.galleryRuntimeSettings.enableBlobPhotoLoader),
        enableGalleryCrossfade: clampBoolean(status.galleryRuntimeSettings.enableGalleryCrossfade),
        enableDirectSwapFallback: clampBoolean(status.galleryRuntimeSettings.enableDirectSwapFallback),
        enableFlattenedOverlay: clampBoolean(status.galleryRuntimeSettings.enableFlattenedOverlay),
        enableOverlayPolling: clampBoolean(status.galleryRuntimeSettings.enableOverlayPolling),
        enableDynamicTemplateRuntime: clampBoolean(status.galleryRuntimeSettings.enableDynamicTemplateRuntime),
        enableTemplatePolling: clampBoolean(status.galleryRuntimeSettings.enableTemplatePolling),
        enableGalleryDebugOverlay: clampBoolean(status.galleryRuntimeSettings.enableGalleryDebugOverlay),
        enableRuntimeWatchdog: clampBoolean(status.galleryRuntimeSettings.enableRuntimeWatchdog),
        enableDirectSwapMode: clampBoolean(status.galleryRuntimeSettings.enableDirectSwapMode),
      }
    : null;

  return {
    deviceName: clampString(status.deviceName || '', 120),
    pageUptimeSec: clampNumber(status.pageUptimeSec),
    currentPhotoId: clampString(status.currentPhotoId || '', 120),
    currentPhotoName: clampString(status.currentPhotoName || '', 200),
    currentPhotoIndex: clampNumber(status.currentPhotoIndex),
    lastPhotoChangeAt: clampIsoTimestamp(status.lastPhotoChangeAt),
    lastSuccessfulImageLoadAt: clampIsoTimestamp(status.lastSuccessfulImageLoadAt),
    lastSuccessfulPollAt: clampIsoTimestamp(status.lastSuccessfulPollAt),
    lastSuccessfulOverlayAt: clampIsoTimestamp(status.lastSuccessfulOverlayAt),
    lastPollDurationMs: clampNumber(status.lastPollDurationMs),
    consecutivePollFailures: clampNumber(status.consecutivePollFailures),
    consecutiveImageFailures: clampNumber(status.consecutiveImageFailures),
    consecutiveOverlayFailures: clampNumber(status.consecutiveOverlayFailures),
    totalImageFailures: clampNumber(status.totalImageFailures),
    totalPollFailures: clampNumber(status.totalPollFailures),
    totalOverlayFailures: clampNumber(status.totalOverlayFailures),
    transitionMode: clampString(status.transitionMode || '', 40),
    directSwapCount: clampNumber(status.directSwapCount),
    crossfadeCount: clampNumber(status.crossfadeCount),
    lastVisualSwapAt: clampIsoTimestamp(status.lastVisualSwapAt),
    lastVisualSwapMode: clampString(status.lastVisualSwapMode || '', 40),
    lastVisualSwapMs: clampNumber(status.lastVisualSwapMs),
    lastApprovedFetchStatus: clampDeviceStatusFlag(status.lastApprovedFetchStatus),
    lastPhotoFetchStatus: clampDeviceStatusFlag(status.lastPhotoFetchStatus),
    lastOverlayFetchStatus: clampDeviceStatusFlag(status.lastOverlayFetchStatus),
    lastApprovedFetchError: clampString(status.lastApprovedFetchError || '', 500),
    lastPhotoFetchError: clampString(status.lastPhotoFetchError || '', 500),
    lastOverlayFetchError: clampString(status.lastOverlayFetchError || '', 500),
    lastSuccessfulHeartbeatAt: clampIsoTimestamp(status.lastSuccessfulHeartbeatAt),
    lastHeartbeatError: clampString(status.lastHeartbeatError || '', 500),
    heartbeatFailureCount: clampNumber(status.heartbeatFailureCount),
    visibleImageAgeSec: clampNumber(status.visibleImageAgeSec),
    slideshowStalled: clampBoolean(status.slideshowStalled),
    userAgent: clampString(status.userAgent || '', 400),
    platform: clampString(status.platform || '', 120),
    screenWidth: clampNumber(status.screenWidth),
    screenHeight: clampNumber(status.screenHeight),
    devicePixelRatio: clampNumber(status.devicePixelRatio),
    memory: sanitizeDeviceMemory(status.memory),
    objectUrlCount: clampNumber(status.objectUrlCount),
    objectUrlPeak: clampNumber(status.objectUrlPeak),
    objectUrlWarningActive: clampBoolean(status.objectUrlWarningActive),
    activeBlobCount: clampNumber(status.activeBlobCount),
    playbackHealth: clampString(status.playbackHealth || '', 40),
    telemetryHealth: clampString(status.telemetryHealth || '', 40),
    networkHealth: clampString(status.networkHealth || '', 40),
    browserHealth: clampString(status.browserHealth || '', 40),
    activeImageSlotCount: clampNumber(status.activeImageSlotCount),
    activeTimers: Array.isArray(status.activeTimers)
      ? status.activeTimers.map((entry) => clampString(entry || '', 80)).filter(Boolean).slice(0, 24)
      : [],
    activeTimerCount: clampNumber(status.activeTimerCount),
    activeIntervals: Array.isArray(status.activeIntervals)
      ? status.activeIntervals.map((entry) => clampString(entry || '', 80)).filter(Boolean).slice(0, 24)
      : [],
    activeIntervalCount: clampNumber(status.activeIntervalCount),
    pendingFetches: Array.isArray(status.pendingFetches)
      ? status.pendingFetches.map((entry) => clampString(entry || '', 120)).filter(Boolean).slice(0, 24)
      : [],
    pendingFetchDetails: Array.isArray(status.pendingFetchDetails)
      ? status.pendingFetchDetails.slice(0, 24).map((entry) => ({
          name: clampString(entry && entry.name ? entry.name : '', 120),
          category: clampString(entry && entry.category ? entry.category : '', 40),
          route: clampString(entry && entry.route ? entry.route : '', 200),
          ageSec: clampNumber(entry && entry.ageSec),
          timeoutSec: clampNumber(entry && entry.timeoutSec),
          stale: clampBoolean(entry && entry.stale),
          photoId: clampString(entry && entry.photoId ? entry.photoId : '', 120),
        }))
      : [],
    pendingFetchCount: clampNumber(status.pendingFetchCount),
    staleInFlightFetches: clampNumber(status.staleInFlightFetches),
    totalStaleFetchAborts: clampNumber(status.totalStaleFetchAborts),
    staleFetchEvents: Array.isArray(status.staleFetchEvents)
      ? status.staleFetchEvents.slice(-20).map((entry) => ({
          at: clampIsoTimestamp(entry && entry.at),
          code: clampString(entry && entry.code ? entry.code : '', 40),
          name: clampString(entry && entry.name ? entry.name : '', 120),
          category: clampString(entry && entry.category ? entry.category : '', 40),
          route: clampString(entry && entry.route ? entry.route : '', 200),
          ageMs: clampNumber(entry && entry.ageMs),
          timeoutMs: clampNumber(entry && entry.timeoutMs),
          photoId: clampString(entry && entry.photoId ? entry.photoId : '', 120),
          message: clampString(entry && entry.message ? entry.message : '', 200),
        }))
      : [],
    lastErrorMessage: clampString(status.lastErrorMessage || '', 500),
    lastRecoveryAction: clampString(status.lastRecoveryAction || '', 200),
    galleryRuntimeSettings,
    lastSettingsFetchAt: clampIsoTimestamp(status.lastSettingsFetchAt),
    lastRuntimeSettingsFetchAt: clampIsoTimestamp(status.lastRuntimeSettingsFetchAt),
    lastRuntimeSettingsAppliedAt: clampIsoTimestamp(status.lastRuntimeSettingsAppliedAt),
    runtimeSettingsLastAppliedAgeSec: clampNumber(status.runtimeSettingsLastAppliedAgeSec),
    runtimeSettingsVersion: clampString(status.runtimeSettingsVersion || '', 60),
    settingsFetchFailures: clampNumber(status.settingsFetchFailures),
    settingsApplyCount: clampNumber(status.settingsApplyCount),
    eventListenerInstallCount: clampNumber(status.eventListenerInstallCount),
    debugOverlayEventCount: clampNumber(status.debugOverlayEventCount),
    recentLogsCount: clampNumber(status.recentLogsCount),
    recentErrorsCount: clampNumber(status.recentErrorsCount),
    disabledSystems: Array.isArray(status.disabledSystems)
      ? status.disabledSystems.map((entry) => clampString(entry || '', 60)).filter(Boolean).slice(0, 24)
      : [],
    qrWaitingReason: clampString(status.qrWaitingReason || '', 80),
    lastHandledRuntimeCommandId: clampString(status.lastHandledRuntimeCommandId || '', 120),
    lastHandledRuntimeCommandAt: clampIsoTimestamp(status.lastHandledRuntimeCommandAt),
    overlayStatus,
    templateStatus,
    debugTestMode: Boolean(status.debugTestMode),
    runtimeStatus: status.runtimeStatus && typeof status.runtimeStatus === 'object'
      ? {
          runtimeDegraded: clampBoolean(status.runtimeStatus.runtimeDegraded),
          watchdogTriggered: clampBoolean(status.runtimeStatus.watchdogTriggered),
          watchdogReason: clampString(status.runtimeStatus.watchdogReason || '', 200),
          reloadCount: clampNumber(status.runtimeStatus.reloadCount),
          lastReloadAt: clampIsoTimestamp(status.runtimeStatus.lastReloadAt),
          lastRecoveryStage: clampString(status.runtimeStatus.lastRecoveryStage || '', 80),
          documentVisibility: clampString(status.runtimeStatus.documentVisibility || '', 40),
          navigatorOnLine: clampBoolean(status.runtimeStatus.navigatorOnLine),
          documentBackgrounded: clampBoolean(status.runtimeStatus.documentBackgrounded),
          runtimeDriftState: clampString(status.runtimeStatus.runtimeDriftState || '', 80),
          lastRuntimeDriftAt: clampIsoTimestamp(status.runtimeStatus.lastRuntimeDriftAt),
        }
      : null,
  };
}

function parseChromeMajorVersion(userAgent = '') {
  const match = String(userAgent || '').match(/Chrome\/(\d+)/i);
  if (!match) return null;
  const major = Number(match[1]);
  return Number.isFinite(major) ? major : null;
}

function buildDeviceFailureCategoryCounts(errors = []) {
  const counts = {
    poll: 0,
    image: 0,
    overlay: 0,
    template: 0,
    heartbeat: 0,
    unknown: 0,
  };

  errors.forEach((entry) => {
    const category = clampString(entry && entry.category ? entry.category : '', 20).toLowerCase();
    if (NON_FAILURE_DEVICE_ERROR_CATEGORIES.has(category)) {
      return;
    }
    if (Object.prototype.hasOwnProperty.call(counts, category)) {
      counts[category] += 1;
    } else {
      counts.unknown += 1;
    }
  });

  return counts;
}

function detectDeviceFetchFailureBurst(errors = []) {
  const fetchErrors = errors
    .filter((entry) => ['poll', 'image', 'overlay', 'template'].includes(String(entry && entry.category ? entry.category : '').toLowerCase()))
    .map((entry) => {
      const atMs = Date.parse(entry && entry.at ? entry.at : '') || 0;
      return atMs > 0 ? { at: new Date(atMs).toISOString(), atMs } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.atMs - b.atMs);

  let bestCount = 0;
  let bestFirstAt = '';
  let bestLastAt = '';
  let endIndex = 0;

  for (let startIndex = 0; startIndex < fetchErrors.length; startIndex += 1) {
    while (
      endIndex < fetchErrors.length &&
      (fetchErrors[endIndex].atMs - fetchErrors[startIndex].atMs) <= DEVICE_FETCH_FAILURE_BURST_WINDOW_MS
    ) {
      endIndex += 1;
    }

    const count = endIndex - startIndex;
    if (count > bestCount) {
      bestCount = count;
      bestFirstAt = fetchErrors[startIndex].at;
      bestLastAt = fetchErrors[endIndex - 1] ? fetchErrors[endIndex - 1].at : fetchErrors[startIndex].at;
    }
  }

  return {
    detected: bestCount >= DEVICE_FETCH_FAILURE_BURST_THRESHOLD,
    count: bestCount,
    threshold: DEVICE_FETCH_FAILURE_BURST_THRESHOLD,
    windowMs: DEVICE_FETCH_FAILURE_BURST_WINDOW_MS,
    firstAt: bestFirstAt,
    lastAt: bestLastAt,
  };
}

function getLatestIsoTimestamp(values = []) {
  let latestMs = 0;
  let latestIso = '';

  values.forEach((value) => {
    const iso = clampIsoTimestamp(value);
    if (!iso) return;
    const atMs = Date.parse(iso) || 0;
    if (atMs > latestMs) {
      latestMs = atMs;
      latestIso = iso;
    }
  });

  return latestIso;
}

function isLikelyYodeckChromiumFreeze(userAgent = '', browserHeartbeatOffline = false) {
  const ua = String(userAgent || '');
  return Boolean(
    browserHeartbeatOffline &&
    /Linux armv7l/i.test(ua) &&
    /Chrome\/92/i.test(ua)
  );
}

function buildLiveGalleryDeviceAnalysis(device, context = {}) {
  const status = device && device.status ? device.status : {};
  const recentErrors = Array.isArray(device && device.recentErrors) ? device.recentErrors : [];
  const lastSeenAgoMs = Number.isFinite(context.lastSeenAgoMs) ? context.lastSeenAgoMs : null;
  const lastSeenAtMs = Date.parse(device && device.lastSeenAt ? device.lastSeenAt : '') || 0;
  const browserHeartbeatOffline = lastSeenAgoMs != null && lastSeenAgoMs > DEVICE_OFFLINE_WINDOW_MS;
  const failureCategoryCounts = buildDeviceFailureCategoryCounts(recentErrors);
  const fetchFailureBurst = detectDeviceFetchFailureBurst(recentErrors);
  const fetchFailuresDetected = (
    (Number(status.totalPollFailures) || 0) > 0 ||
    (Number(status.totalImageFailures) || 0) > 0 ||
    (Number(status.totalOverlayFailures) || 0) > 0 ||
    failureCategoryCounts.template > 0
  );
  const currentFetchFailuresDetected = (
    status.lastApprovedFetchStatus === 'fail' ||
    status.lastPhotoFetchStatus === 'fail' ||
    status.lastOverlayFetchStatus === 'fail'
  );
  const firstFailureAt = recentErrors.length ? clampIsoTimestamp(recentErrors[0].at) : '';
  const lastFailureAt = recentErrors.length ? clampIsoTimestamp(recentErrors[recentErrors.length - 1].at) : '';
  const lastHealthyAt = getLatestIsoTimestamp([
    status.lastSuccessfulImageLoadAt,
    status.lastSuccessfulPollAt,
    status.lastSuccessfulOverlayAt,
    status.lastSuccessfulHeartbeatAt,
  ]);
  const chromeMajorVersion = parseChromeMajorVersion(status.userAgent || '');
  const browserVersionWarning = chromeMajorVersion != null && chromeMajorVersion < DEVICE_OLD_CHROMIUM_WARNING_VERSION
    ? 'Old Chromium detected. Test compatibility and consider updating player/browser.'
    : '';
  const staleDataWarning = browserHeartbeatOffline && fetchFailuresDetected
    ? 'Device stopped reporting after repeated fetch errors. Pi may still be powered on, but gallery/browser may be frozen or network fetches may be failing.'
    : '';
  const overlayActiveAtFailure = Boolean(status.overlayStatus && status.overlayStatus.enabled);
  const recentPlaybackNearLastSeen = (() => {
    const checkpoints = [
      status.lastVisualSwapAt || status.lastPhotoChangeAt,
      status.lastSuccessfulPollAt,
      status.lastSuccessfulImageLoadAt,
    ]
      .map((value) => Date.parse(value || '') || 0)
      .filter((value) => value > 0);
    if (!lastSeenAtMs || !checkpoints.length) return false;
    return checkpoints.every((value) => Math.abs(lastSeenAtMs - value) <= 45000);
  })();
  const heartbeatOnlyWarning = !browserHeartbeatOffline
    && !currentFetchFailuresDetected
    && !status.slideshowStalled
    && (Number(status.heartbeatFailureCount) || 0) > 0
    && recentPlaybackNearLastSeen;
  const chromiumOrTelemetryStopped = browserHeartbeatOffline && !currentFetchFailuresDetected && recentPlaybackNearLastSeen;

  let lastKnownState = fetchFailuresDetected ? 'Fetch failures detected' : 'No recent fetch failures detected';
  if (fetchFailureBurst.detected) {
    lastKnownState = 'Fetch failure burst detected';
  } else if (chromiumOrTelemetryStopped) {
    lastKnownState = 'Playback looked healthy before browser heartbeats stopped';
  }

  let likelyDiagnosis = '';
  if (browserHeartbeatOffline && fetchFailuresDetected) {
    likelyDiagnosis = 'Gallery/browser stuck after repeated network fetch failures.';
  } else if (chromiumOrTelemetryStopped) {
    likelyDiagnosis = 'Chromium or the gallery telemetry loop stopped after otherwise healthy playback.';
  } else if (heartbeatOnlyWarning) {
    likelyDiagnosis = 'Telemetry post failures detected while gallery playback still appears healthy.';
  } else if (fetchFailureBurst.detected) {
    likelyDiagnosis = 'Repeated network fetch failures are occurring.';
  } else if (status.slideshowStalled) {
    likelyDiagnosis = 'Slideshow appears stalled.';
  } else if (
    status.lastApprovedFetchStatus === 'fail' ||
    status.lastPhotoFetchStatus === 'fail' ||
    status.lastOverlayFetchStatus === 'fail'
  ) {
    likelyDiagnosis = 'Network fetch failures detected while browser heartbeat is still active.';
  }

  const shouldRecommendWatchdog = fetchFailureBurst.detected && lastSeenAgoMs != null && lastSeenAgoMs > DEVICE_ONLINE_WINDOW_MS;
  const yodeckChromiumFreezeLikely = isLikelyYodeckChromiumFreeze(status.userAgent || '', browserHeartbeatOffline);
  let recommendedNextAction = shouldRecommendWatchdog
    ? 'Recommended next step: Pi-level watchdog or browser auto-reload.'
    : browserVersionWarning || (fetchFailuresDetected
      ? 'Inspect gallery network fetches and browser stability before changing slideshow behavior.'
      : '');
  if (chromiumOrTelemetryStopped) {
    recommendedNextAction = 'Playback was healthy before telemetry stopped; inspect Chromium/Yodeck runtime stability and telemetry loop health.';
  } else if (heartbeatOnlyWarning) {
    recommendedNextAction = 'Telemetry post failures are happening while playback looks healthy; inspect /api/device-heartbeat and browser background network behavior.';
  }
  if (yodeckChromiumFreezeLikely) {
    likelyDiagnosis = likelyDiagnosis || 'Likely Chromium/Yodeck runtime freeze on Raspberry Pi (Linux armv7l, Chrome 92).';
    recommendedNextAction = 'Update or reflash the Yodeck player browser, and add a Pi-level watchdog that restarts Chromium if heartbeats stop.';
  }

  return {
    browserHeartbeatOffline,
    lastKnownState,
    likelyDiagnosis,
    staleDataWarning,
    fetchFailuresDetected,
    fetchFailureBurst: {
      ...fetchFailureBurst,
      label: fetchFailureBurst.detected ? 'WARNING: FETCH FAILURE BURST' : '',
    },
    shouldRecommendWatchdog,
    recommendedNextAction,
    browserVersionWarning,
    chromeMajorVersion,
    lastHealthyAt,
    firstFailureAt,
    lastFailureAt,
    failureCategoryCounts,
    lastKnownRecoveryAction: clampString(status.lastRecoveryAction || '', 200),
    yodeckChromiumFreezeLikely,
    yodeckFreezeRecommendedAction: yodeckChromiumFreezeLikely
      ? 'Update/reflash Yodeck player browser or add Pi-level watchdog.'
      : '',
    playbackHealth: status.slideshowStalled
      ? 'degraded'
      : fetchFailuresDetected
        ? 'warning'
        : 'healthy',
    telemetryHealth: browserHeartbeatOffline
      ? 'offline'
      : (Number(status.heartbeatFailureCount) || 0) > 0
        ? 'warning'
        : 'healthy',
    heartbeatOnlyWarning,
    chromiumOrTelemetryStopped,
    overlayActiveAtFailure,
  };
}

function sanitizeDeviceLogEntry(rawEntry = {}) {
  const entry = rawEntry && typeof rawEntry === 'object' ? rawEntry : {};
  return {
    at: clampIsoTimestamp(entry.at) || new Date().toISOString(),
    level: clampString(entry.level || 'info', 20),
    event: clampString(entry.event || 'device_event', 80),
    message: clampString(entry.message || '', 500),
    details: entry.details && typeof entry.details === 'object'
      ? JSON.parse(JSON.stringify(entry.details, (_, value) => {
          if (typeof value === 'string') return clampString(value, 300);
          if (typeof value === 'number') return Number.isFinite(value) ? value : null;
          if (typeof value === 'boolean' || value == null) return value;
          return value;
        }))
      : null,
  };
}

function getOrCreateLiveGalleryDevice(deviceId, deviceName = '') {
  const now = new Date().toISOString();
  const existing = liveGalleryDevices.get(deviceId);
  if (existing) {
    return existing;
  }

  const created = {
    deviceId,
    createdAt: now,
    lastSeenAt: now,
    deviceName: deviceName || deviceId,
    status: {},
    recentErrors: [],
    recentLogs: [],
  };
  liveGalleryDevices.set(deviceId, created);
  return created;
}

function pruneLiveGalleryDevices() {
  const cutoff = Date.now() - DEVICE_HISTORY_WINDOW_MS;
  for (const [deviceId, device] of liveGalleryDevices.entries()) {
    const lastSeenAtMs = Date.parse(device.lastSeenAt || '') || 0;
    if (lastSeenAtMs < cutoff) {
      liveGalleryDevices.delete(deviceId);
    }
  }
}

function buildLiveGalleryDeviceSummary(device) {
  const now = Date.now();
  const lastSeenAtMs = Date.parse(device.lastSeenAt || '') || 0;
  const lastSuccessfulImageLoadAtMs = Date.parse(device.status?.lastSuccessfulImageLoadAt || '') || 0;
  const lastSuccessfulPollAtMs = Date.parse(device.status?.lastSuccessfulPollAt || '') || 0;
  const lastSuccessfulOverlayAtMs = Date.parse(device.status?.lastSuccessfulOverlayAt || '') || 0;
  const lastSuccessfulHeartbeatAtMs = Date.parse(device.status?.lastSuccessfulHeartbeatAt || '') || 0;
  const online = lastSeenAtMs > 0 && (now - lastSeenAtMs) <= DEVICE_ONLINE_WINDOW_MS;
  const lastSeenAgoMs = lastSeenAtMs > 0 ? Math.max(0, now - lastSeenAtMs) : null;
  const analysis = buildLiveGalleryDeviceAnalysis(device, {
    now,
    lastSeenAgoMs,
    online,
  });

  return {
    deviceId: device.deviceId,
    createdAt: device.createdAt,
    lastSeenAt: device.lastSeenAt,
    lastSeenAgoMs,
    online,
    heartbeatOffline: !online && lastSeenAtMs > 0 && (now - lastSeenAtMs) <= DEVICE_HISTORY_WINDOW_MS,
    retainedInHistory: lastSeenAtMs > 0 && (now - lastSeenAtMs) <= DEVICE_HISTORY_WINDOW_MS,
    status: device.status || {},
    deviceName: clampString(device.deviceName || device.status?.deviceName || device.deviceId, 120),
    recentErrors: Array.isArray(device.recentErrors) ? device.recentErrors : [],
    recentLogs: Array.isArray(device.recentLogs) ? device.recentLogs : [],
    analysis,
    timing: {
      warningAfterMs: DEVICE_WARNING_WINDOW_MS,
      offlineAfterMs: DEVICE_OFFLINE_WINDOW_MS,
      onlineWindowMs: DEVICE_ONLINE_WINDOW_MS,
      lastSuccessfulImageLoadAgoMs: lastSuccessfulImageLoadAtMs > 0 ? Math.max(0, now - lastSuccessfulImageLoadAtMs) : null,
      lastSuccessfulPollAgoMs: lastSuccessfulPollAtMs > 0 ? Math.max(0, now - lastSuccessfulPollAtMs) : null,
      lastSuccessfulOverlayAgoMs: lastSuccessfulOverlayAtMs > 0 ? Math.max(0, now - lastSuccessfulOverlayAtMs) : null,
      lastSuccessfulHeartbeatAgoMs: lastSuccessfulHeartbeatAtMs > 0 ? Math.max(0, now - lastSuccessfulHeartbeatAtMs) : null,
    },
  };
}

function logObservedRouteResult(route, startedAtMs, statusCode, reason = 'ok') {
  const durationMs = Math.max(0, Date.now() - startedAtMs);
  const status = Number(statusCode) || 0;
  const normalizedReason = clampString(reason || 'ok', 160);
  const isOkReason = normalizedReason === 'ok' || normalizedReason.startsWith('ok ');
  const message = `[route] ${route} status=${status} durationMs=${durationMs} reason=${normalizedReason}`;
  if (status >= 500 || !isOkReason) {
    console.warn(message);
  } else {
    console.log(message);
  }
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

function getDefaultGalleryRuntimeSettings() {
  return JSON.parse(JSON.stringify(DEFAULT_APP_SETTINGS.galleryRuntime || {}));
}

function getDefaultGalleryRuntimeCommand() {
  return JSON.parse(JSON.stringify(DEFAULT_APP_SETTINGS.galleryRuntimeCommand || {}));
}

function normalizeGalleryRuntimeSettings(rawSettings = {}) {
  const fallback = getDefaultGalleryRuntimeSettings();
  const candidate = rawSettings && typeof rawSettings === 'object' ? rawSettings : {};
  return {
    enablePiSafeLegacyRuntime: coerceBoolean(candidate.enablePiSafeLegacyRuntime, fallback.enablePiSafeLegacyRuntime),
    enablePiStablePlayerMode: coerceBoolean(candidate.enablePiStablePlayerMode, fallback.enablePiStablePlayerMode),
    enableBlobPhotoLoader: coerceBoolean(candidate.enableBlobPhotoLoader, fallback.enableBlobPhotoLoader),
    enableGalleryCrossfade: coerceBoolean(candidate.enableGalleryCrossfade, fallback.enableGalleryCrossfade),
    enableDirectSwapFallback: coerceBoolean(candidate.enableDirectSwapFallback, fallback.enableDirectSwapFallback),
    enableFlattenedOverlay: coerceBoolean(candidate.enableFlattenedOverlay, fallback.enableFlattenedOverlay),
    enableOverlayPolling: coerceBoolean(candidate.enableOverlayPolling, fallback.enableOverlayPolling),
    enableDynamicTemplateRuntime: coerceBoolean(candidate.enableDynamicTemplateRuntime, fallback.enableDynamicTemplateRuntime),
    enableTemplatePolling: coerceBoolean(candidate.enableTemplatePolling, fallback.enableTemplatePolling),
    enableGalleryDebugOverlay: coerceBoolean(candidate.enableGalleryDebugOverlay, fallback.enableGalleryDebugOverlay),
    enableRuntimeWatchdog: coerceBoolean(candidate.enableRuntimeWatchdog, fallback.enableRuntimeWatchdog),
    enableDirectSwapMode: coerceBoolean(candidate.enableDirectSwapMode, fallback.enableDirectSwapMode),
  };
}

function normalizeGalleryRuntimeCommand(rawCommand = {}) {
  const fallback = getDefaultGalleryRuntimeCommand();
  const candidate = rawCommand && typeof rawCommand === 'object' ? rawCommand : {};
  const command = clampString(candidate.command || fallback.command || '', 80);
  const commandId = clampString(candidate.commandId || fallback.commandId || '', 120);
  const issuedAt = clampIsoTimestamp(candidate.issuedAt) || '';
  const reason = clampString(candidate.reason || fallback.reason || '', 160);
  const clearCache = coerceBoolean(candidate.clearCache, fallback.clearCache);

  if (!command || !commandId || !issuedAt) {
    return { ...fallback, command: '', commandId: '', issuedAt: '', reason: '', clearCache: false };
  }

  return {
    command,
    commandId,
    issuedAt,
    reason,
    clearCache,
  };
}

function buildGalleryRuntimeDisabledSystems(runtimeSettings = {}) {
  const runtime = normalizeGalleryRuntimeSettings(runtimeSettings);
  const disabledSystems = [];
  if (runtime.enablePiSafeLegacyRuntime) {
    disabledSystems.push('runtime_settings_polling');
    disabledSystems.push('telemetry_heartbeat_loop');
    disabledSystems.push('flattened_overlay');
    disabledSystems.push('overlay_polling');
    disabledSystems.push('dynamic_template_runtime');
    disabledSystems.push('template_polling');
  }
  if (!runtime.enableBlobPhotoLoader) disabledSystems.push('blob_photo_loader');
  if (!runtime.enableGalleryCrossfade) disabledSystems.push('gallery_crossfade');
  if (!runtime.enableDirectSwapFallback) disabledSystems.push('direct_swap_fallback');
  if (!runtime.enableFlattenedOverlay) disabledSystems.push('flattened_overlay');
  if (!runtime.enableOverlayPolling) disabledSystems.push('overlay_polling');
  if (!runtime.enableDynamicTemplateRuntime) disabledSystems.push('dynamic_template_runtime');
  if (!runtime.enableTemplatePolling) disabledSystems.push('template_polling');
  if (!runtime.enableGalleryDebugOverlay) disabledSystems.push('gallery_debug_overlay');
  if (!runtime.enableRuntimeWatchdog) disabledSystems.push('runtime_watchdog');
  if (!runtime.enableDirectSwapMode) disabledSystems.push('direct_swap_mode');
  return [...new Set(disabledSystems)];
}

function buildGalleryRuntimeSettingsPayload(settings = appSettings) {
  const runtimeSettings = normalizeGalleryRuntimeSettings(
    settings && typeof settings === 'object' ? settings.galleryRuntime : null
  );
  const galleryRuntimeCommand = normalizeGalleryRuntimeCommand(
    settings && typeof settings === 'object' ? settings.galleryRuntimeCommand : null
  );
  const runtimeSettingsVersion = crypto
    .createHash('sha1')
    .update(JSON.stringify(runtimeSettings))
    .digest('hex')
    .slice(0, 12);

  return {
    runtimeSettings,
    runtimeSettingsVersion,
    disabledSystems: buildGalleryRuntimeDisabledSystems(runtimeSettings),
    galleryRuntimeCommand,
  };
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
    // Support: 'all', 'last10', 'last25', or 'custom:20' format
    if (['all', 'last10', 'last25'].includes(limit)) {
      next.galleryDisplayLimit = limit;
    } else if (limit.startsWith('custom:')) {
      const customNum = parseInt(limit.replace('custom:', ''), 10);
      if (customNum > 0 && customNum <= 1000) {
        next.galleryDisplayLimit = limit; // Keep as 'custom:20'
      } else {
        next.galleryDisplayLimit = 'all'; // Invalid custom number, fallback
      }
    } else {
      next.galleryDisplayLimit = 'all';
    }
  }

  if (Object.prototype.hasOwnProperty.call(patch, 'activeTemplateId')) {
    const rawTemplateId = patch.activeTemplateId;
    next.activeTemplateId = rawTemplateId == null ? '' : String(rawTemplateId).trim();
  }

  if (Object.prototype.hasOwnProperty.call(patch, 'activeTemplateSnapshot')) {
    const rawSnapshot = patch.activeTemplateSnapshot;
    next.activeTemplateSnapshot = rawSnapshot == null ? '' : String(rawSnapshot).trim();
  }

  if (Object.prototype.hasOwnProperty.call(patch, 'galleryRuntimeCommand')) {
    next.galleryRuntimeCommand = normalizeGalleryRuntimeCommand(patch.galleryRuntimeCommand);
  }

  if (patch.galleryRuntime && typeof patch.galleryRuntime === 'object') {
    const currentRuntimeSettings = normalizeGalleryRuntimeSettings(next.galleryRuntime);
    const runtimePatch = patch.galleryRuntime;
    next.galleryRuntime = {
      enablePiSafeLegacyRuntime: coerceBoolean(runtimePatch.enablePiSafeLegacyRuntime, currentRuntimeSettings.enablePiSafeLegacyRuntime),
      enablePiStablePlayerMode: coerceBoolean(runtimePatch.enablePiStablePlayerMode, currentRuntimeSettings.enablePiStablePlayerMode),
      enableBlobPhotoLoader: coerceBoolean(runtimePatch.enableBlobPhotoLoader, currentRuntimeSettings.enableBlobPhotoLoader),
      enableGalleryCrossfade: coerceBoolean(runtimePatch.enableGalleryCrossfade, currentRuntimeSettings.enableGalleryCrossfade),
      enableDirectSwapFallback: coerceBoolean(runtimePatch.enableDirectSwapFallback, currentRuntimeSettings.enableDirectSwapFallback),
      enableFlattenedOverlay: coerceBoolean(runtimePatch.enableFlattenedOverlay, currentRuntimeSettings.enableFlattenedOverlay),
      enableOverlayPolling: coerceBoolean(runtimePatch.enableOverlayPolling, currentRuntimeSettings.enableOverlayPolling),
      enableDynamicTemplateRuntime: coerceBoolean(runtimePatch.enableDynamicTemplateRuntime, currentRuntimeSettings.enableDynamicTemplateRuntime),
      enableTemplatePolling: coerceBoolean(runtimePatch.enableTemplatePolling, currentRuntimeSettings.enableTemplatePolling),
      enableGalleryDebugOverlay: coerceBoolean(runtimePatch.enableGalleryDebugOverlay, currentRuntimeSettings.enableGalleryDebugOverlay),
      enableRuntimeWatchdog: coerceBoolean(runtimePatch.enableRuntimeWatchdog, currentRuntimeSettings.enableRuntimeWatchdog),
      enableDirectSwapMode: coerceBoolean(runtimePatch.enableDirectSwapMode, currentRuntimeSettings.enableDirectSwapMode),
    };
  }

  if (patch.intro && typeof patch.intro === 'object') {
    next.intro.title = coerceString(patch.intro.title, next.intro.title);
    next.intro.subtitle = coerceString(patch.intro.subtitle, next.intro.subtitle);
  }

  if (patch.form && typeof patch.form === 'object') {
    if (Object.prototype.hasOwnProperty.call(patch.form, 'locationEnabled')) {
      next.form.locationEnabled = coerceBoolean(patch.form.locationEnabled, next.form.locationEnabled);
    }
    if (Object.prototype.hasOwnProperty.call(patch.form, 'showCountryFlag')) {
      next.form.showCountryFlag = coerceBoolean(patch.form.showCountryFlag, next.form.showCountryFlag);
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
      if (key === GALLERY_RUNTIME_COMMAND_ROW_KEY) {
        if (typeof value === 'string' && value.trim()) {
          try {
            settingsPatch.galleryRuntimeCommand = normalizeGalleryRuntimeCommand(JSON.parse(value));
          } catch (err) {
            console.warn('Unable to parse gallery runtime command row:', err.message || err);
          }
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
    values.push([
      GALLERY_RUNTIME_COMMAND_ROW_KEY,
      JSON.stringify(normalizeGalleryRuntimeCommand(settings && typeof settings === 'object' ? settings.galleryRuntimeCommand : null)),
    ]);

    await sheets.spreadsheets.values.update({
      spreadsheetId: SETTINGS_SHEET_ID,
      range: `${SETTINGS_SHEET_NAME}!A1:B${values.length}`,
      valueInputOption: 'RAW',
      requestBody: { values }
    });

    settingsLoadedFromSheet = true;
    settingsLastHydratedAt = Date.now();
    return true;
  } catch (err) {
    console.warn('Unable to persist settings to sheet:', err.message || err);
    return false;
  }
}

async function hydrateSettingsFromSheet(options = {}) {
  const force = Boolean(options && options.force);
  const now = Date.now();

  if (!force && settingsLoadedFromSheet && settingsLastHydratedAt && (now - settingsLastHydratedAt) < SETTINGS_REFRESH_TTL_MS) {
    return;
  }

  if (settingsHydrationPromise) {
    return settingsHydrationPromise;
  }

  settingsHydrationPromise = (async () => {
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
    settingsLastHydratedAt = Date.now();
  })();

  try {
    await settingsHydrationPromise;
  } finally {
    settingsHydrationPromise = null;
  }
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

function normalizeTemplateId(value) {
  const normalized = value == null ? '' : String(value).trim();
  return normalized;
}

async function getTemplateById(templateId) {
  const normalizedId = normalizeTemplateId(templateId);
  if (!normalizedId) return null;

  const templates = await readTemplatesFromSheet();
  return templates.find((template) => String(template.id) === normalizedId) || null;
}

function buildPublicTemplateAssetUrl(fileId) {
  return `/gallery/template-asset/${encodeURIComponent(fileId)}`;
}

function buildPublicTemplateOverlayUrl(fileId) {
  return `/gallery/template-overlay/${encodeURIComponent(fileId)}`;
}

function getApprovedPlaybackTimestamp(file) {
  if (!file || typeof file !== 'object') return '';

  const approvedAt =
    file.appProperties && typeof file.appProperties === 'object'
      ? String(file.appProperties.approvedAt || '').trim()
      : '';
  if (approvedAt) return approvedAt;

  const modifiedTime = String(file.modifiedTime || '').trim();
  if (modifiedTime) return modifiedTime;

  return String(file.createdTime || '').trim();
}

function getApprovedPlaybackTimeMs(file) {
  const timestamp = getApprovedPlaybackTimestamp(file);
  if (!timestamp) return 0;

  const ms = Date.parse(timestamp);
  return Number.isFinite(ms) ? ms : 0;
}

function setGalleryJsonNoStoreHeaders(res) {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
}

function buildGalleryApprovedFileEntry(file) {
  return {
    id: file.id,
    name: file.name || '',
    mimeType: file.mimeType || '',
    createdTime: file.createdTime || '',
    modifiedTime: file.modifiedTime || '',
    approvedTime: getApprovedPlaybackTimestamp(file),
  };
}

function buildGalleryApprovedManifestVersion(files) {
  const signature = (Array.isArray(files) ? files : [])
    .map((file) => `${file.id}|${file.approvedTime}|${file.modifiedTime}`)
    .join(';');
  return crypto.createHash('sha1').update(signature).digest('hex').slice(0, 12);
}

function selectGalleryApprovedFiles(allFiles, displayLimit) {
  const all = Array.isArray(allFiles) ? allFiles : [];
  const limit = String(displayLimit || 'all');

  if (limit === 'all') {
    return all;
  }
  if (limit === 'last10') {
    return all.slice(-10);
  }
  if (limit === 'last25') {
    return all.slice(-25);
  }
  if (limit.startsWith('custom:')) {
    const customNum = parseInt(limit.replace('custom:', ''), 10);
    if (customNum > 0 && customNum <= 1000) {
      return all.slice(-customNum);
    }
  }
  return all;
}

async function listApprovedDriveFilesForGallery() {
  let pageToken = null;
  const results = [];

  do {
    const response = await drive.files.list({
      q: `'${APPROVED_FOLDER_ID}' in parents and trashed = false`,
      fields: 'nextPageToken, files(id, name, createdTime, modifiedTime, mimeType, appProperties)',
      pageSize: 200,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    });

    results.push(...(response.data.files || []));
    pageToken = response.data.nextPageToken || null;
  } while (pageToken);

  return results.sort((a, b) => {
    const playbackTimeDiff = getApprovedPlaybackTimeMs(a) - getApprovedPlaybackTimeMs(b);
    if (playbackTimeDiff !== 0) return playbackTimeDiff;

    const createdTimeDiff =
      (Date.parse(String(a.createdTime || '')) || 0) -
      (Date.parse(String(b.createdTime || '')) || 0);
    if (createdTimeDiff !== 0) return createdTimeDiff;

    return String(a.id || '').localeCompare(String(b.id || ''));
  });
}

const GALLERY_APPROVED_MANIFEST_TTL_MS = 4000;
const galleryApprovedManifestCache = {
  value: null,
  expiresAt: 0,
  promise: null,
  cacheKey: '',
};

function invalidateGalleryApprovedManifestCache() {
  galleryApprovedManifestCache.value = null;
  galleryApprovedManifestCache.expiresAt = 0;
  galleryApprovedManifestCache.promise = null;
  galleryApprovedManifestCache.cacheKey = '';
}

async function getGalleryApprovedManifest() {
  await hydrateSettingsFromSheet();

  const displayLimit = appSettings.galleryDisplayLimit || 'all';
  const cacheKey = String(displayLimit);
  const now = Date.now();

  if (
    galleryApprovedManifestCache.value &&
    galleryApprovedManifestCache.cacheKey === cacheKey &&
    galleryApprovedManifestCache.expiresAt > now
  ) {
    return galleryApprovedManifestCache.value;
  }

  if (galleryApprovedManifestCache.promise && galleryApprovedManifestCache.cacheKey === cacheKey) {
    return galleryApprovedManifestCache.promise;
  }

  const refreshPromise = (async () => {
    const allFiles = await listApprovedDriveFilesForGallery();
    const selected = selectGalleryApprovedFiles(allFiles, displayLimit);
    const files = selected.map(buildGalleryApprovedFileEntry);
    const manifest = {
      files,
      manifestVersion: buildGalleryApprovedManifestVersion(files),
      generatedAt: new Date().toISOString(),
    };

    galleryApprovedManifestCache.value = manifest;
    galleryApprovedManifestCache.cacheKey = cacheKey;
    galleryApprovedManifestCache.expiresAt = Date.now() + GALLERY_APPROVED_MANIFEST_TTL_MS;
    galleryApprovedManifestCache.promise = null;
    return manifest;
  })().catch((err) => {
    galleryApprovedManifestCache.promise = null;
    throw err;
  });

  galleryApprovedManifestCache.promise = refreshPromise;
  galleryApprovedManifestCache.cacheKey = cacheKey;
  return refreshPromise;
}

const ADMIN_THUMBNAIL_PLACEHOLDER_SVG = Buffer.from(
  '<svg xmlns="http://www.w3.org/2000/svg" width="160" height="160" viewBox="0 0 160 160"><rect width="160" height="160" fill="#1f2937"/><text x="80" y="84" text-anchor="middle" fill="#9ca3af" font-size="12" font-family="sans-serif">Preview</text></svg>',
  'utf8'
);

function sendAdminThumbnailPlaceholder(res, fileId, reason) {
  console.warn('Admin thumbnail unavailable; serving placeholder instead of full-resolution fallback', {
    fileId: String(fileId || ''),
    reason: String(reason || 'thumbnail_unavailable'),
  });
  res.setHeader('Content-Type', 'image/svg+xml');
  res.setHeader('Cache-Control', 'private, max-age=120');
  res.setHeader('X-Thumbnail-Source', 'placeholder');
  res.status(200).end(ADMIN_THUMBNAIL_PLACEHOLDER_SVG);
}

function normalizeTemplateAssetFileId(value) {
  if (value == null) return '';
  const raw = String(value).trim();
  if (!raw) return '';

  try {
    if (raw.startsWith('http://') || raw.startsWith('https://')) {
      const parsed = new URL(raw);
      const parts = parsed.pathname.split('/');
      return decodeURIComponent(parts[parts.length - 1] || '');
    }
  } catch (_) {
    // Fall through to the string cleanup below.
  }

  const withoutQuery = raw.split('?')[0];
  const parts = withoutQuery.split('/');
  return decodeURIComponent(parts[parts.length - 1] || withoutQuery);
}

function resolveTemplateAsset(asset) {
  if (!asset || !asset.value) return null;

  if (asset.type === 'uploaded') {
    const normalizedFileId = normalizeTemplateAssetFileId(asset.value);
    if (!normalizedFileId) return null;

    return {
      type: 'uploaded',
      value: normalizedFileId,
      url: buildPublicTemplateAssetUrl(normalizedFileId)
    };
  }

  return {
    type: 'preloaded',
    value: asset.value,
    url: asset.value
  };
}

function normalizeTemplatePhotoBox(photoBox) {
  const fallback = { ...DEFAULT_TEMPLATE_PHOTO_BOX };
  if (!photoBox || typeof photoBox !== 'object') {
    return fallback;
  }

  const x = Number(photoBox.x);
  const y = Number(photoBox.y);
  const width = Number(photoBox.width);
  const height = Number(photoBox.height);

  if (![x, y, width, height].every(Number.isFinite) || width <= 0 || height <= 0) {
    return fallback;
  }

  return {
    x: Math.max(0, Math.min(1920, Math.round(x))),
    y: Math.max(0, Math.min(1080, Math.round(y))),
    width: Math.max(1, Math.min(1920, Math.round(width))),
    height: Math.max(1, Math.min(1080, Math.round(height))),
  };
}

function serializeTemplateForPublic(template) {
  if (!template || !template.data) return null;

  const background = resolveTemplateAsset(template.data.background);
  const logos = Array.isArray(template.data.logos)
    ? template.data.logos
        .map((logo) => {
          const resolved = resolveTemplateAsset(logo);
          if (!resolved) return null;

          return {
            type: resolved.type,
            value: resolved.value,
            url: resolved.url,
            x: Number.isFinite(Number(logo.x)) ? Number(logo.x) : 0,
            y: Number.isFinite(Number(logo.y)) ? Number(logo.y) : 0,
            width: Number.isFinite(Number(logo.width)) ? Number(logo.width) : 0,
            height: Number.isFinite(Number(logo.height)) ? Number(logo.height) : 0,
            order: Number.isFinite(Number(logo.order)) ? Number(logo.order) : 0,
          };
        })
        .filter(Boolean)
    : [];
  const qrLayers = Array.isArray(template.data.qrLayers)
    ? template.data.qrLayers
        .map((qrLayer) => {
          const resolved = resolveTemplateAsset(qrLayer);
          if (!resolved) return null;

          return {
            type: resolved.type,
            value: resolved.value,
            url: resolved.url,
            x: Number.isFinite(Number(qrLayer.x)) ? Number(qrLayer.x) : 0,
            y: Number.isFinite(Number(qrLayer.y)) ? Number(qrLayer.y) : 0,
            width: Number.isFinite(Number(qrLayer.width)) ? Number(qrLayer.width) : 0,
            height: Number.isFinite(Number(qrLayer.height)) ? Number(qrLayer.height) : 0,
            label: qrLayer.label ? String(qrLayer.label) : '',
            order: Number.isFinite(Number(qrLayer.order)) ? Number(qrLayer.order) : 0,
          };
        })
        .filter(Boolean)
    : [];
  const photoBox = normalizeTemplatePhotoBox(template.data.photoBox);

  return {
    id: template.id,
    name: template.name,
    createdAt: template.createdAt || '',
    canvasWidth: Number.isFinite(Number(template.data.canvasWidth)) ? Number(template.data.canvasWidth) : 1920,
    canvasHeight: Number.isFinite(Number(template.data.canvasHeight)) ? Number(template.data.canvasHeight) : 1080,
    background,
    logos,
    qrLayers,
    photoBox,
    overlayFileId: template.data.overlayFileId ? String(template.data.overlayFileId) : '',
    overlayUpdatedAt: template.data.overlayUpdatedAt ? String(template.data.overlayUpdatedAt) : '',
    overlayVersion: template.data.overlayVersion ? String(template.data.overlayVersion) : '',
  };
}

function parseActiveTemplateSnapshot(snapshotStr) {
  if (!snapshotStr || typeof snapshotStr !== 'string') return null;
  try {
    const parsed = JSON.parse(snapshotStr);
    if (!parsed || typeof parsed !== 'object' || !parsed.data) return null;
    return parsed;
  } catch (err) {
    console.warn('Unable to parse active template snapshot:', err.message || err);
    return null;
  }
}

function buildActiveTemplateSnapshotString(template) {
  if (!template || !template.data) return '';

  return JSON.stringify({
    id: template.id ? String(template.id) : '',
    name: template.name || '',
    createdAt: template.createdAt || new Date().toISOString(),
    data: template.data
  });
}

function buildTemplateVersion(templateLike) {
  if (!templateLike || !templateLike.data) return '';

  try {
    return crypto
      .createHash('sha1')
      .update(JSON.stringify({
        id: templateLike.id ? String(templateLike.id) : '',
        name: templateLike.name || '',
        createdAt: templateLike.createdAt || '',
        data: templateLike.data
      }))
      .digest('hex')
      .slice(0, 12);
  } catch (err) {
    console.warn('Unable to build template version hash:', err.message || err);
    return '';
  }
}

function buildActiveTemplateResponse(template, meta = {}) {
  const serializedTemplate = serializeTemplateForPublic(template);
  const templateId = template && template.id ? String(template.id) : '';
  const version =
    serializedTemplate && serializedTemplate.overlayVersion
      ? serializedTemplate.overlayVersion
      : buildTemplateVersion(template);

  return {
    ok: true,
    template: serializedTemplate,
    meta: {
      active: Boolean(serializedTemplate),
      disabled: Boolean(meta.disabled),
      reason: meta.reason || (serializedTemplate ? 'active_template_available' : 'template_unavailable'),
      source: meta.source || 'unknown',
      templateId,
      version,
    }
  };
}

function buildActiveOverlayResponse(template, meta = {}) {
  const serializedTemplate = serializeTemplateForPublic(template);
  const enabled = Boolean(
    serializedTemplate &&
      serializedTemplate.overlayFileId
  );
  const overlayFileId = enabled ? String(serializedTemplate.overlayFileId) : '';
  const overlayVersion = enabled
    ? String(serializedTemplate.overlayVersion || buildTemplateVersion(template))
    : '';
  const photoBox = serializedTemplate
    ? normalizeTemplatePhotoBox(serializedTemplate.photoBox)
    : { ...DEFAULT_TEMPLATE_PHOTO_BOX };

  return {
    ok: true,
    enabled,
    templateId: serializedTemplate ? String(serializedTemplate.id || '') : '',
    templateName: serializedTemplate ? serializedTemplate.name || '' : '',
    templateVersion: overlayVersion,
    overlayFileId,
    overlayUrl: overlayFileId ? buildPublicTemplateOverlayUrl(overlayFileId) : '',
    photoBox,
    updatedAt: serializedTemplate ? serializedTemplate.overlayUpdatedAt || '' : '',
    meta: {
      reason: meta.reason || (enabled ? 'active_overlay_available' : 'overlay_unavailable'),
      source: meta.source || 'unknown',
      disabled: Boolean(meta.disabled),
    }
  };
}

function sanitizeDriveFileBaseName(value, fallback = 'asset') {
  const normalized = String(value || '')
    .normalize('NFKD')
    .replace(/[^\w\s.-]/g, '')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/[.]{2,}/g, '.')
    .replace(/^[_\-.]+|[_\-.]+$/g, '')
    .slice(0, 80);

  return normalized || fallback;
}

async function syncActiveTemplateSettings(settings) {
  const next = JSON.parse(JSON.stringify(settings || {}));
  const activeTemplateId = normalizeTemplateId(next.activeTemplateId);

  if (!activeTemplateId) {
    next.activeTemplateId = '';
    next.activeTemplateSnapshot = '';
    return next;
  }

  const snapshot = parseActiveTemplateSnapshot(next.activeTemplateSnapshot);
  if (snapshot && String(snapshot.id || '') === activeTemplateId) {
    next.activeTemplateId = activeTemplateId;
    next.activeTemplateSnapshot = buildActiveTemplateSnapshotString({
      id: activeTemplateId,
      name: snapshot.name || '',
      createdAt: snapshot.createdAt || '',
      data: snapshot.data
    });
    return next;
  }

  const template = await getTemplateById(activeTemplateId);
  if (!template) {
    next.activeTemplateId = '';
    next.activeTemplateSnapshot = '';
    return next;
  }

  next.activeTemplateId = String(template.id);
  next.activeTemplateSnapshot = buildActiveTemplateSnapshotString(template);
  return next;
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

    let createdAt = '';
    try {
      const existingRow = await sheets.spreadsheets.values.get({
        spreadsheetId: TEMPLATES_SHEET_ID,
        range: `${TEMPLATES_SHEET_NAME}!A${row}:D${row}`
      });
      createdAt = existingRow.data?.values?.[0]?.[2] || '';
    } catch (_) {}

    const values = [[name, JSON.stringify(templateData), createdAt, String(isActive)]];

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

function formatPRDateIso(prDate) {
  const pad = (n) => String(n).padStart(2, '0');
  const yyyy = prDate.getFullYear();
  const mm = pad(prDate.getMonth() + 1);
  const dd = pad(prDate.getDate());
  return `${yyyy}-${mm}-${dd}`;
}

function formatPRHourKey(prDate) {
  return `${String(prDate.getHours()).padStart(2, '0')}:00`;
}

function prDateToUtc(dateString, hour = 0, minute = 0, second = 0, millisecond = 0) {
  if (!dateString) return null;
  const match = String(dateString).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const [, year, month, day] = match;
  return new Date(Date.UTC(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour) + 4,
    Number(minute),
    Number(second),
    Number(millisecond)
  ));
}

function formatChartHourLabel(prDate) {
  const hour = prDate.getHours();
  const period = hour >= 12 ? 'PM' : 'AM';
  const hour12 = hour % 12 || 12;
  return `${hour12}${period}`;
}

function formatChartDateLabel(prDate) {
  const month = String(prDate.getMonth() + 1).padStart(2, '0');
  const day = String(prDate.getDate()).padStart(2, '0');
  return `${month}/${day}`;
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

function getMetricsEventFamily(eventType) {
  switch (eventType) {
    case 'visit':
    case 'form':
    case 'upload':
      return 'public';
    case 'approve_photo':
    case 'reject_photo':
    case 'delete_approved_photo':
    case 'batch_delete_approved_photos':
    case 'clear_drive':
    case 'reset_logs':
      return 'admin';
    default:
      return 'system';
  }
}

function extractLocationFromSessionId(sessionId = '') {
  const match = String(sessionId || '').match(/^IP\s+[\d.:a-fA-F]+\s+(.+)$/);
  return match && match[1] ? match[1].trim() : '';
}

function buildLocationLabel(country = '', region = '', sessionId = '') {
  const direct = [region, country].filter(Boolean).join(', ').trim();
  if (direct) return direct;
  return extractLocationFromSessionId(sessionId);
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value || {});
  } catch (err) {
    return '{}';
  }
}

async function ensureMetricsSheets() {
  if (metricsSheetsReady) return true;
  if (!METRICS_SHEET_ID) return false;

  try {
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: METRICS_SHEET_ID,
      fields: 'sheets(properties(title))'
    });

    const existingTitles = new Set(
      (meta.data.sheets || [])
        .map((sheet) => sheet?.properties?.title)
        .filter(Boolean)
    );

    const requiredTitles = [
      METRICS_EVENTS_SHEET_NAME,
      METRICS_DAILY_SHEET_NAME,
      METRICS_NEWSLETTER_SHEET_NAME,
      METRICS_SCHEMA_SHEET_NAME,
    ];

    const addSheetRequests = requiredTitles
      .filter((title) => !existingTitles.has(title))
      .map((title) => ({
        addSheet: {
          properties: { title }
        }
      }));

    if (addSheetRequests.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: METRICS_SHEET_ID,
        requestBody: {
          requests: addSheetRequests
        }
      });
    }

    metricsSheetsReady = true;
    return true;
  } catch (err) {
    console.warn('Unable to ensure metrics workbook:', err.message || err);
    return false;
  }
}

async function ensureMetricsSheetHeaders() {
  if (metricsSheetHeadersReady) return true;
  if (!METRICS_SHEET_ID) return false;

  const ready = await ensureMetricsSheets();
  if (!ready) return false;

  try {
    const sheetConfigs = [
      {
        name: METRICS_EVENTS_SHEET_NAME,
        range: `${METRICS_EVENTS_SHEET_NAME}!A1:X`,
        headers: METRICS_EVENTS_HEADERS,
      },
      {
        name: METRICS_DAILY_SHEET_NAME,
        range: `${METRICS_DAILY_SHEET_NAME}!A1:H`,
        headers: METRICS_DAILY_HEADERS,
      },
      {
        name: METRICS_NEWSLETTER_SHEET_NAME,
        range: `${METRICS_NEWSLETTER_SHEET_NAME}!A1:M`,
        headers: METRICS_NEWSLETTER_HEADERS,
      },
    ];

    for (const config of sheetConfigs) {
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: METRICS_SHEET_ID,
        range: `${config.name}!1:1`
      });
      const headerRow = resp.data.values?.[0] || [];
      if (headerRow[0] !== config.headers[0]) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: METRICS_SHEET_ID,
          range: config.range,
          valueInputOption: 'RAW',
          requestBody: { values: [config.headers] }
        });
      }
    }

    const schemaRows = [
      ['Section', 'Purpose', 'Fields'],
      ['Events', 'Raw append-only metrics stream for every tracked interaction.', METRICS_EVENTS_HEADERS.join(', ')],
      ['Daily Summary', 'Prepared daily rollups for future dashboard expansion.', METRICS_DAILY_HEADERS.join(', ')],
      ['Newsletter Leads', 'Newsletter opt-ins prepared for reporting and export.', METRICS_NEWSLETTER_HEADERS.join(', ')],
      ['Notes', 'Dates are stored in Puerto Rico time for grouping and UTC for canonical timestamps.', 'Timezone: America/Puerto_Rico (UTC-4 fixed)'],
    ];

    await sheets.spreadsheets.values.update({
      spreadsheetId: METRICS_SHEET_ID,
      range: `${METRICS_SCHEMA_SHEET_NAME}!A1:C${schemaRows.length}`,
      valueInputOption: 'RAW',
      requestBody: { values: schemaRows }
    });

    metricsSheetHeadersReady = true;
    return true;
  } catch (err) {
    console.warn('Unable to ensure metrics sheet headers:', err.message || err);
    return false;
  }
}

async function logMetricsEvent(
  eventType,
  req,
  {
    email = '',
    sessionId = '',
    country = '',
    region = '',
    lastName = '',
    newsletter = null,
    ticket = '',
    timestampUtc = null,
    source = '',
    metadata = {},
  } = {}
) {
  if (!METRICS_SHEET_ID) return;

  try {
    const ready = await ensureMetricsSheetHeaders();
    if (!ready) return;

    const utcStr = timestampUtc || new Date().toISOString();
    const dateUtc = new Date(utcStr);
    if (Number.isNaN(dateUtc.getTime())) return;

    const prDate = toPR(dateUtc);
    const normalizedSessionId = sessionId || buildSessionIdentifier(req);
    const normalizedCountry = country || '';
    const normalizedRegion = region || '';
    const locationLabel = buildLocationLabel(normalizedCountry, normalizedRegion, normalizedSessionId);
    const newsletterFlag = getNewsletterFlag(newsletter);
    const normalizedEmail = String(email || '').trim().toLowerCase();
    const requestIp = normalizeIp(getClientIp(req));
    const userAgent = String(req?.headers?.['user-agent'] || '').slice(0, 500);
    const referrer = String(req?.headers?.referer || req?.headers?.referrer || '').slice(0, 500);
    const pathLabel = req?.path || '';
    const methodLabel = req?.method || '';
    const family = getMetricsEventFamily(eventType);
    const row = [[
      crypto.randomUUID(),
      utcStr,
      formatPRDateTimeShort(prDate),
      formatPRDateIso(prDate),
      formatPRHourKey(prDate),
      eventType,
      family,
      normalizedSessionId,
      appSessionId || '',
      email || '',
      normalizedEmail,
      normalizedCountry,
      normalizedRegion,
      locationLabel,
      lastName || '',
      newsletterFlag,
      ticket || '',
      source || family,
      requestIp,
      userAgent,
      referrer,
      pathLabel,
      methodLabel,
      safeJsonStringify(metadata),
    ]];

    await sheets.spreadsheets.values.append({
      spreadsheetId: METRICS_SHEET_ID,
      range: METRICS_EVENTS_SHEET_RANGE,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: row }
    });

    if (newsletterFlag === 'Y' && normalizedEmail) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: METRICS_SHEET_ID,
        range: `${METRICS_NEWSLETTER_SHEET_NAME}!A:M`,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: {
          values: [[
            utcStr,
            formatPRDateTimeShort(prDate),
            formatPRDateIso(prDate),
            email || '',
            normalizedEmail,
            lastName || '',
            normalizedCountry,
            normalizedRegion,
            locationLabel,
            normalizedSessionId,
            appSessionId || '',
            source || family,
            safeJsonStringify(metadata),
          ]]
        }
      });
    }
  } catch (err) {
    console.warn('Unable to log metrics event:', eventType, err.message || err);
  }
}

function parseMetricsEventRow(row = []) {
  const timestampUtc = row[1] || '';
  const timestamp = new Date(timestampUtc);
  if (!timestampUtc || Number.isNaN(timestamp.getTime())) return null;

  return {
    eventId: row[0] || '',
    timestampUtc,
    timestamp,
    timestampPr: row[2] || '',
    prDate: row[3] || '',
    prHour: row[4] || '',
    eventType: row[5] || '',
    eventFamily: row[6] || '',
    sessionId: row[7] || '',
    serverSessionId: row[8] || '',
    email: row[9] || '',
    emailNormalized: row[10] || '',
    country: row[11] || '',
    region: row[12] || '',
    locationLabel: row[13] || '',
    lastName: row[14] || '',
    newsletter: row[15] || '',
    ticket: row[16] || '',
    source: row[17] || '',
  };
}

async function readMetricsEvents() {
  if (!METRICS_SHEET_ID) return [];

  const ready = await ensureMetricsSheetHeaders();
  if (!ready) return [];

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: METRICS_SHEET_ID,
    range: METRICS_EVENTS_SHEET_RANGE
  });

  const rows = resp.data.values || [];
  if (rows.length <= 1) return [];

  return rows
    .slice(1)
    .map(parseMetricsEventRow)
    .filter(Boolean);
}

function getMetricsRangeDefinition(query = {}) {
  const preset = String(query.preset || '24h').trim();
  const now = new Date();
  const definitions = {
    '1h': { startUtc: new Date(now.getTime() - (1 * 60 * 60 * 1000)), endUtc: now, label: 'Last hour', granularity: 'hour' },
    '6h': { startUtc: new Date(now.getTime() - (6 * 60 * 60 * 1000)), endUtc: now, label: 'Last 6 hours', granularity: 'hour' },
    '12h': { startUtc: new Date(now.getTime() - (12 * 60 * 60 * 1000)), endUtc: now, label: 'Last 12 hours', granularity: 'hour' },
    '24h': { startUtc: new Date(now.getTime() - (24 * 60 * 60 * 1000)), endUtc: now, label: 'Last 24 hours', granularity: 'hour' },
    '3d': { startUtc: new Date(now.getTime() - (3 * 24 * 60 * 60 * 1000)), endUtc: now, label: 'Last 3 days', granularity: 'day' },
    '7d': { startUtc: new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000)), endUtc: now, label: 'Last 7 days', granularity: 'day' },
    '15d': { startUtc: new Date(now.getTime() - (15 * 24 * 60 * 60 * 1000)), endUtc: now, label: 'Last 15 days', granularity: 'day' },
    '30d': { startUtc: new Date(now.getTime() - (30 * 24 * 60 * 60 * 1000)), endUtc: now, label: 'Last 30 days', granularity: 'day' },
  };

  if (preset === 'today') {
    const todayPr = getPRDate();
    const startUtc = prDateToUtc(formatPRDateIso(todayPr), 0, 0, 0, 0);
    return {
      preset,
      startUtc,
      endUtc: now,
      label: 'Today',
      granularity: 'hour',
    };
  }

  if (preset === 'this_week') {
    const nowPr = getPRDate();
    const dayOfWeek = nowPr.getDay();
    const startPr = new Date(nowPr);
    startPr.setDate(nowPr.getDate() - dayOfWeek);
    startPr.setHours(0, 0, 0, 0);

    return {
      preset,
      startUtc: new Date(startPr.getTime() + 4 * 60 * 60 * 1000),
      endUtc: now,
      label: 'This week',
      granularity: 'day',
    };
  }

  if (preset === 'custom') {
    const startDate = String(query.startDate || '').trim();
    const endDate = String(query.endDate || '').trim();
    const startUtc = prDateToUtc(startDate, 0, 0, 0, 0);
    const endUtc = prDateToUtc(endDate, 23, 59, 59, 999);
    if (!startUtc || !endUtc || startUtc > endUtc) {
      throw new Error('invalid_custom_range');
    }

    const diffMs = endUtc.getTime() - startUtc.getTime();
    return {
      preset,
      startUtc,
      endUtc,
      label: `${startDate} to ${endDate}`,
      granularity: diffMs <= (36 * 60 * 60 * 1000) ? 'hour' : 'day',
    };
  }

  if (definitions[preset]) {
    return { preset, ...definitions[preset] };
  }

  return { preset: '24h', ...definitions['24h'] };
}

function filterMetricsEventsByRange(events, rangeDef) {
  return events.filter((event) => event.timestamp >= rangeDef.startUtc && event.timestamp <= rangeDef.endUtc);
}

function buildMetricsTimeseries(events, rangeDef) {
  const buckets = [];
  const bucketMap = new Map();

  if (rangeDef.granularity === 'hour') {
    const cursor = new Date(rangeDef.startUtc);
    cursor.setMinutes(0, 0, 0);
    const end = new Date(rangeDef.endUtc);
    end.setMinutes(0, 0, 0);

    while (cursor <= end) {
      const prCursor = toPR(cursor);
      const key = `${formatPRDateIso(prCursor)} ${formatPRHourKey(prCursor)}`;
      const bucket = {
        key,
        label: formatChartHourLabel(prCursor),
        visits: 0,
        forms: 0,
        uploads: 0,
        newsletters: 0,
      };
      buckets.push(bucket);
      bucketMap.set(key, bucket);
      cursor.setHours(cursor.getHours() + 1);
    }

    events.forEach((event) => {
      const prDate = toPR(event.timestamp);
      const key = `${formatPRDateIso(prDate)} ${formatPRHourKey(prDate)}`;
      const bucket = bucketMap.get(key);
      if (!bucket) return;
      if (event.eventType === 'visit') bucket.visits += 1;
      if (event.eventType === 'form') bucket.forms += 1;
      if (event.eventType === 'upload') bucket.uploads += 1;
      if (event.eventType === 'form' && event.newsletter === 'Y') bucket.newsletters += 1;
    });
  } else {
    const startPrIso = formatPRDateIso(toPR(rangeDef.startUtc));
    const endPrIso = formatPRDateIso(toPR(rangeDef.endUtc));
    let cursor = prDateToUtc(startPrIso, 0, 0, 0, 0);
    const endCursor = prDateToUtc(endPrIso, 0, 0, 0, 0);

    while (cursor && endCursor && cursor <= endCursor) {
      const prCursor = toPR(cursor);
      const key = formatPRDateIso(prCursor);
      const bucket = {
        key,
        label: formatChartDateLabel(prCursor),
        visits: 0,
        forms: 0,
        uploads: 0,
        newsletters: 0,
      };
      buckets.push(bucket);
      bucketMap.set(key, bucket);
      cursor = new Date(cursor.getTime() + (24 * 60 * 60 * 1000));
    }

    events.forEach((event) => {
      const key = event.prDate || formatPRDateIso(toPR(event.timestamp));
      const bucket = bucketMap.get(key);
      if (!bucket) return;
      if (event.eventType === 'visit') bucket.visits += 1;
      if (event.eventType === 'form') bucket.forms += 1;
      if (event.eventType === 'upload') bucket.uploads += 1;
      if (event.eventType === 'form' && event.newsletter === 'Y') bucket.newsletters += 1;
    });
  }

  return buckets;
}

function convertMetricsEventsToCsv(events) {
  const rows = [
    [
      'timestamp_utc',
      'timestamp_pr',
      'event_type',
      'event_family',
      'session_id',
      'server_session_id',
      'email',
      'country',
      'region',
      'location_label',
      'last_name',
      'newsletter',
      'ticket',
      'source',
    ],
    ...events.map((event) => [
      event.timestampUtc,
      event.timestampPr,
      event.eventType,
      event.eventFamily,
      event.sessionId,
      event.serverSessionId,
      event.email,
      event.country,
      event.region,
      event.locationLabel,
      event.lastName,
      event.newsletter,
      event.ticket,
      event.source,
    ])
  ];

  return rows
    .map((row) => row.map((value) => {
      const str = String(value || '');
      if (/[",\n]/.test(str)) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    }).join(','))
    .join('\n');
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

  invalidateDriveFolderCaches(PENDING_FOLDER_ID);
  invalidateDriveFolderCaches(APPROVED_FOLDER_ID);

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

app.get('/gallery/runtime-settings', (req, res) => {
  const respond = () => {
    const payload = buildGalleryRuntimeSettingsPayload(appSettings);
    return res.json({
      ok: true,
      runtimeSettings: payload.runtimeSettings,
      runtimeSettingsVersion: payload.runtimeSettingsVersion,
      disabledSystems: payload.disabledSystems,
      galleryRuntimeCommand: payload.galleryRuntimeCommand,
      fetchedAt: new Date().toISOString(),
      templateRenderingEnabled: false,
    });
  };

  hydrateSettingsFromSheet()
    .then(respond)
    .catch(respond);
});

// Simple visit endpoint: FE can call this on page load
app.post('/ping', async (req, res) => {
  if (!appEnabled) {
    return res.status(503).json({ ok: false, error: 'app_offline' });
  }
  try {
    const sessionId = buildSessionIdentifier(req);
    await Promise.allSettled([
      logEventToSheet('visit', { sessionId }),
      logMetricsEvent('visit', req, {
        sessionId,
        source: 'public_app',
        metadata: {
          trigger: 'page_load'
        }
      })
    ]);
  } catch (e) {
    console.error('Error logging visit to sheet:', e);
  }
  res.json({ ok: true });
});

app.post('/api/device-heartbeat', (req, res) => {
  const startedAtMs = Date.now();
  try {
    pruneLiveGalleryDevices();

    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const deviceId = clampString(body.deviceId || '', 120);
    if (!deviceId) {
      logObservedRouteResult('/api/device-heartbeat', startedAtMs, 400, 'missing_device_id');
      return res.status(400).json({ ok: false, error: 'missing_device_id' });
    }

    const status = sanitizeDeviceStatus(body.status);
    const recentErrors = sanitizeDeviceErrors(body.recentErrors);
    const device = getOrCreateLiveGalleryDevice(deviceId, status.deviceName);
    const now = new Date().toISOString();

    device.lastSeenAt = now;
    device.deviceName = status.deviceName || device.deviceName || deviceId;
    device.status = status;
    device.recentErrors = recentErrors;

    logObservedRouteResult('/api/device-heartbeat', startedAtMs, 200, `ok device=${deviceId}`);
    return res.json({ ok: true, deviceId, lastSeenAt: now });
  } catch (err) {
    console.error('Error processing device heartbeat:', err);
    logObservedRouteResult('/api/device-heartbeat', startedAtMs, 500, err && err.message ? err.message : 'device_heartbeat_failed');
    return res.status(500).json({ ok: false, error: 'device_heartbeat_failed' });
  }
});

app.post('/api/device-log', (req, res) => {
  try {
    pruneLiveGalleryDevices();

    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const deviceId = clampString(body.deviceId || '', 120);
    if (!deviceId) {
      return res.status(400).json({ ok: false, error: 'missing_device_id' });
    }

    const deviceName = clampString(body.deviceName || '', 120);
    const device = getOrCreateLiveGalleryDevice(deviceId, deviceName);
    const logEntry = sanitizeDeviceLogEntry(body);

    device.lastSeenAt = clampIsoTimestamp(body.lastSeenAt) || device.lastSeenAt || new Date().toISOString();
    if (deviceName) {
      device.deviceName = deviceName;
    }
    device.recentLogs.push(logEntry);
    if (device.recentLogs.length > MAX_DEVICE_LOGS) {
      device.recentLogs = device.recentLogs.slice(-MAX_DEVICE_LOGS);
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error('Error storing device log:', err);
    return res.status(500).json({ ok: false, error: 'device_log_failed' });
  }
});

app.get('/api/devices/live', ensureAdminAuth, (req, res) => {
  try {
    pruneLiveGalleryDevices();

    const now = Date.now();
    const devices = Array.from(liveGalleryDevices.values())
      .map((device) => buildLiveGalleryDeviceSummary(device))
      .filter((device) => {
        const lastSeenAtMs = Date.parse(device.lastSeenAt || '') || 0;
        if (!lastSeenAtMs) return false;
        if ((now - lastSeenAtMs) <= DEVICE_ONLINE_WINDOW_MS) return true;
        return (now - lastSeenAtMs) <= DEVICE_HISTORY_WINDOW_MS;
      })
      .sort((a, b) => {
        if (a.online !== b.online) return a.online ? -1 : 1;
        const aSeen = Date.parse(a.lastSeenAt || '') || 0;
        const bSeen = Date.parse(b.lastSeenAt || '') || 0;
        return bSeen - aSeen;
      });

    return res.json({
      ok: true,
      devices,
      meta: {
        onlineWindowMs: DEVICE_ONLINE_WINDOW_MS,
        warningWindowMs: DEVICE_WARNING_WINDOW_MS,
        offlineWindowMs: DEVICE_OFFLINE_WINDOW_MS,
        historyWindowMs: DEVICE_HISTORY_WINDOW_MS,
      },
    });
  } catch (err) {
    console.error('Error listing live gallery devices:', err);
    return res.status(500).json({ ok: false, error: 'list_devices_failed' });
  }
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
      await Promise.allSettled([
        logEventToSheet('upload', {
          sessionId,
          ticket: ticketLabel // e.g. "T015"
        }),
        logMetricsEvent('upload', req, {
          sessionId,
          ticket: ticketLabel,
          source: 'public_app',
          metadata: {
            fileId,
            finalName,
            ticketIndex,
            ticketDisplay
          }
        })
      ]);
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
      await Promise.allSettled([
        logEventToSheet('form', {
          email,
          sessionId,
          timestampUtc,
          country: countryClean,
          region,
          lastName,
          newsletter
        }),
        logMetricsEvent('form', req, {
          email,
          sessionId,
          timestampUtc,
          country: countryClean,
          region,
          lastName,
          newsletter,
          source: 'public_form',
          metadata: {
            newsletterEnabled: newsletter !== '',
          }
        })
      ]);
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
  hydrateSettingsFromSheet({ force: true })
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

    adminSystemHealthCache.expiresAt = 0;
    return res.json({ ok: true, enabled: appEnabled });
  } catch (err) {
    console.error('Error updating app status:', err);
    return res.status(500).json({ ok: false, error: 'app_status_failed' });
  }
});

// Admin: get current app settings
app.get('/admin/settings', ensureAdminAuth, (req, res) => {
  hydrateSettingsFromSheet({ force: true })
    .then(() => res.json({ ok: true, settings: appSettings }))
    .catch(() => res.json({ ok: true, settings: appSettings }));
});

// Admin: update app settings
app.post('/admin/settings', ensureAdminAuth, async (req, res) => {
  try {
    const mergedSettings = mergeAppSettings(req.body || {});
    appSettings = await syncActiveTemplateSettings(mergedSettings);
    const persisted = await writeSettingsToSheet(appSettings, appEnabled, appSessionId);
    if (!persisted) {
      return res.status(500).json({ ok: false, error: 'settings_persist_failed' });
    }
    settingsLoadedFromSheet = true;
    adminSystemHealthCache.expiresAt = 0;
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
    const fileMeta = await drive.files.get({
      fileId,
      fields: 'thumbnailLink, mimeType',
      supportsAllDrives: true,
    });

    if (fileMeta.data.thumbnailLink) {
      const thumbRes = await fetch(fileMeta.data.thumbnailLink);
      if (thumbRes.ok) {
        const contentType = thumbRes.headers.get('content-type') || fileMeta.data.mimeType || 'image/jpeg';
        res.setHeader('Content-Type', contentType);
        res.setHeader('Cache-Control', 'private, max-age=300');
        res.setHeader('X-Thumbnail-Source', 'drive_thumbnail');
        const thumbBuffer = Buffer.from(await thumbRes.arrayBuffer());
        return res.end(thumbBuffer);
      }
      console.warn('Admin thumbnail Drive link fetch failed; using placeholder', {
        fileId,
        status: thumbRes.status,
      });
    } else {
      console.warn('Admin thumbnail link missing from Drive metadata; using placeholder', {
        fileId,
        mimeType: fileMeta.data.mimeType || '',
      });
    }

    if (!res.headersSent) {
      return sendAdminThumbnailPlaceholder(res, fileId, 'thumbnail_unavailable');
    }
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

    const approvedAt = new Date().toISOString();

    await drive.files.update({
      fileId,
      addParents: APPROVED_FOLDER_ID,
      removeParents: PENDING_FOLDER_ID,
      fields: 'id, parents, appProperties, modifiedTime',
      supportsAllDrives: true,
      requestBody: {
        appProperties: {
          approvedAt,
        },
      },
    });

    invalidateDriveFolderCaches(APPROVED_FOLDER_ID);
    invalidateDriveFolderCaches(PENDING_FOLDER_ID);

    logMetricsEvent('approve_photo', req, {
      source: 'admin_review',
      metadata: { fileId }
    }).catch(() => {});

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

    invalidateDriveFolderCaches(PENDING_FOLDER_ID);
    invalidateDriveFolderCaches(APPROVED_FOLDER_ID);

    logMetricsEvent('reject_photo', req, {
      source: 'admin_review',
      metadata: { fileId }
    }).catch(() => {});

    res.json({ ok: true });
  } catch (err) {
    console.error('Error rejecting photo:', err);
    res.status(500).json({ error: 'failed_reject' });
  }
});

// --------- ADMIN: list / manage APPROVED photos ----------

const driveFolderPageTokenCache = new Map();
const driveFolderCountCache = new Map();
const DRIVE_FOLDER_COUNT_TTL_MS = 60 * 1000;
const ADMIN_SYSTEM_HEALTH_TTL_MS = 30 * 1000;
let adminSystemHealthCache = {
  value: null,
  expiresAt: 0,
};

function getDriveFolderPageCacheKey(folderId, pageSize) {
  return `${folderId}:${pageSize}`;
}

function invalidateDriveFolderCaches(folderId) {
  for (const key of Array.from(driveFolderPageTokenCache.keys())) {
    if (key.startsWith(`${folderId}:`)) {
      driveFolderPageTokenCache.delete(key);
    }
  }
  driveFolderCountCache.delete(folderId);
  adminSystemHealthCache.expiresAt = 0;
  if (folderId === APPROVED_FOLDER_ID) {
    invalidateGalleryApprovedManifestCache();
  }
}

async function countFilesInFolder(folderId) {
  let pageToken = null;
  let total = 0;

  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false`,
      fields: 'files(id), nextPageToken',
      pageSize: 1000,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    });

    total += (res.data.files || []).length;
    pageToken = res.data.nextPageToken || null;
  } while (pageToken);

  return total;
}

function getCachedFolderCount(folderId) {
  const now = Date.now();
  const existing = driveFolderCountCache.get(folderId);

  if (existing && existing.value != null && existing.expiresAt > now) {
    return existing.value;
  }

  if (!existing || !existing.promise) {
    const refreshPromise = countFilesInFolder(folderId)
      .then((value) => {
        driveFolderCountCache.set(folderId, {
          value,
          expiresAt: Date.now() + DRIVE_FOLDER_COUNT_TTL_MS,
          promise: null
        });
        return value;
      })
      .catch((err) => {
        console.error('Error refreshing folder count cache:', err);
        const staleValue = existing && existing.value != null ? existing.value : null;
        driveFolderCountCache.set(folderId, {
          value: staleValue,
          expiresAt: Date.now() + 5000,
          promise: null
        });
        return staleValue;
      });

    driveFolderCountCache.set(folderId, {
      value: existing && existing.value != null ? existing.value : null,
      expiresAt: existing && existing.expiresAt ? existing.expiresAt : 0,
      promise: refreshPromise
    });
  }

  return existing && existing.value != null ? existing.value : null;
}

async function getFolderCountForHealth(folderId) {
  const cached = getCachedFolderCount(folderId);
  if (typeof cached === 'number') {
    return cached;
  }

  try {
    const fresh = await countFilesInFolder(folderId);
    driveFolderCountCache.set(folderId, {
      value: fresh,
      expiresAt: Date.now() + DRIVE_FOLDER_COUNT_TTL_MS,
      promise: null
    });
    return fresh;
  } catch (err) {
    console.error('Error counting folder for health endpoint:', err);
    return null;
  }
}

async function listFilesInFolderPaginated(folderId, page = 1, pageSize = 24) {
  const pageNumber = Math.max(1, parseInt(page, 10) || 1);
  const size = Math.max(1, Math.min(100, parseInt(pageSize, 10) || 24));
  const cacheKey = getDriveFolderPageCacheKey(folderId, size);

  let tokenMap = driveFolderPageTokenCache.get(cacheKey);
  if (!tokenMap) {
    tokenMap = new Map([[1, null]]);
    driveFolderPageTokenCache.set(cacheKey, tokenMap);
  }

  let currentPage = 1;
  while (currentPage < pageNumber) {
    if (tokenMap.has(currentPage + 1)) {
      currentPage += 1;
      continue;
    }

    const currentToken = tokenMap.get(currentPage) || null;
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false`,
      fields: 'nextPageToken',
      orderBy: 'createdTime desc',
      pageSize: size,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken: currentToken,
    });

    const nextToken = res.data.nextPageToken || null;
    tokenMap.set(currentPage + 1, nextToken);

    if (!nextToken) {
      return {
        files: [],
        total: getCachedFolderCount(folderId),
        page: pageNumber,
        pageSize: size,
        totalPages: null,
        hasNextPage: false,
      };
    }

    currentPage += 1;
  }

  const pageToken = tokenMap.get(pageNumber) || null;
  if (pageNumber > 1 && pageToken === null) {
    return {
      files: [],
      total: getCachedFolderCount(folderId),
      page: pageNumber,
      pageSize: size,
      totalPages: null,
      hasNextPage: false,
    };
  }

  const res = await drive.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: 'files(id, name, createdTime), nextPageToken',
    orderBy: 'createdTime desc',
    pageSize: size,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    pageToken,
  });

  const files = (res.data.files || []).map((f) => ({
    id: f.id,
    name: f.name,
    createdTime: f.createdTime,
  }));
  const nextPageToken = res.data.nextPageToken || null;
  tokenMap.set(pageNumber + 1, nextPageToken);

  const total = getCachedFolderCount(folderId);

  return {
    files,
    total,
    page: pageNumber,
    pageSize: size,
    totalPages: total != null ? Math.ceil(total / size) : null,
    hasNextPage: Boolean(nextPageToken),
  };
}

app.get('/admin/system-health', ensureAdminAuth, async (req, res) => {
  try {
    if (adminSystemHealthCache.value && adminSystemHealthCache.expiresAt > Date.now()) {
      return res.json({ ok: true, health: adminSystemHealthCache.value });
    }

    const healthSheetRange = 'A1:A2';
    const [drivePending, driveApproved, sheetsStatus, pendingCount, approvedCount] = await Promise.all([
      drive.files.get({
        fileId: PENDING_FOLDER_ID,
        fields: 'id',
        supportsAllDrives: true,
      }).then(() => true).catch((err) => {
        console.error('System health pending folder check failed:', err);
        return false;
      }),
      drive.files.get({
        fileId: APPROVED_FOLDER_ID,
        fields: 'id',
        supportsAllDrives: true,
      }).then(() => true).catch((err) => {
        console.error('System health approved folder check failed:', err);
        return false;
      }),
      SESSION_SHEET_ID
        ? sheets.spreadsheets.values.get({
            spreadsheetId: SESSION_SHEET_ID,
            range: healthSheetRange,
          }).then(() => true).catch((err) => {
            console.error('System health sheet check failed:', err);
            return false;
          })
        : Promise.resolve(false),
      getFolderCountForHealth(PENDING_FOLDER_ID),
      getFolderCountForHealth(APPROVED_FOLDER_ID),
    ]);

    const health = {
      serverEnabled: Boolean(appEnabled),
      driveOk: Boolean(drivePending && driveApproved),
      sheetsOk: Boolean(sheetsStatus),
      pendingCount: typeof pendingCount === 'number' ? pendingCount : null,
      approvedCount: typeof approvedCount === 'number' ? approvedCount : null,
      activeTemplateId: String(appSettings?.activeTemplateId || ''),
      checkedAt: new Date().toISOString(),
    };

    adminSystemHealthCache = {
      value: health,
      expiresAt: Date.now() + ADMIN_SYSTEM_HEALTH_TTL_MS,
    };

    return res.json({ ok: true, health });
  } catch (err) {
    console.error('Error loading admin system health:', err);
    return res.status(500).json({ ok: false, error: 'system_health_failed' });
  }
});

app.get('/admin/approved-list', ensureAdminAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const pageSize = parseInt(req.query.pageSize, 10) || 24;
    
    const result = await listFilesInFolderPaginated(APPROVED_FOLDER_ID, page, pageSize);
    res.json({ 
      ok: true, 
      files: result.files,
      total: result.total,
      page: result.page,
      pageSize: result.pageSize,
      totalPages: result.totalPages,
      hasNextPage: result.hasNextPage
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

    const max = Math.min(parseInt(req.query.limit, 10) || 12, 100);

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

    invalidateDriveFolderCaches(APPROVED_FOLDER_ID);

    logMetricsEvent('delete_approved_photo', req, {
      source: 'admin_gallery',
      metadata: { fileId }
    }).catch(() => {});

    res.json({ ok: true });
  } catch (err) {
    console.error('Error deleting approved photo:', err);
    res.status(500).json({ ok: false, error: 'delete_approved_failed' });
  }
});

app.post('/admin/delete-approved-batch', ensureAdminAuth, async (req, res) => {
  try {
    const fileIds = Array.isArray(req.body?.fileIds) ? req.body.fileIds.filter(Boolean) : [];
    if (!fileIds.length) {
      return res.status(400).json({ ok: false, error: 'missing_fileIds' });
    }

    const results = await Promise.allSettled(
      fileIds.map((fileId) =>
        drive.files.update({
          fileId,
          requestBody: { trashed: true },
          fields: 'id, trashed',
          supportsAllDrives: true,
        })
      )
    );

    const deletedIds = [];
    const failedIds = [];

    results.forEach((result, index) => {
      const fileId = fileIds[index];
      if (result.status === 'fulfilled') {
        deletedIds.push(fileId);
      } else {
        failedIds.push(fileId);
        console.error('Error deleting approved photo in batch:', fileId, result.reason);
      }
    });

    invalidateDriveFolderCaches(APPROVED_FOLDER_ID);

    logMetricsEvent('batch_delete_approved_photos', req, {
      source: 'admin_gallery',
      metadata: {
        requestedCount: fileIds.length,
        deletedCount: deletedIds.length,
        deletedIds,
        failedIds,
      }
    }).catch(() => {});

    res.json({
      ok: failedIds.length === 0,
      deletedIds,
      failedIds,
    });
  } catch (err) {
    console.error('Error batch deleting approved photos:', err);
    res.status(500).json({ ok: false, error: 'delete_approved_batch_failed' });
  }
});

// Clear Drive: send ALL pending + approved photos to trash
// ⚠️ CAREFUL: this is meant for after the event.
app.post('/admin/clear-drive', ensureAdminAuth, async (req, res) => {
  try {
    const result = await clearDriveFolders();
    logMetricsEvent('clear_drive', req, {
      source: 'admin_maintenance',
      metadata: {
        trashedCount: result.trashedCount,
        errors: result.errors
      }
    }).catch(() => {});
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
    logMetricsEvent('reset_logs', req, {
      source: 'admin_maintenance',
      metadata: {
        resetTarget: 'session_logs'
      }
    }).catch(() => {});
    res.json({ ok: true, message: 'reset_logs_completed' });
  } catch (err) {
    console.error('Error in reset-logs:', err);
    res.status(500).json({ ok: false, error: 'reset_logs_failed' });
  }
});

app.get('/admin/metrics/overview', ensureAdminAuth, async (req, res) => {
  try {
    const range = getMetricsRangeDefinition(req.query || {});
    const allEvents = await readMetricsEvents();
    const publicEvents = filterMetricsEventsByRange(
      allEvents.filter((event) => event.eventFamily === 'public'),
      range
    ).sort((a, b) => a.timestamp - b.timestamp);

    const visits = publicEvents.filter((event) => event.eventType === 'visit');
    const forms = publicEvents.filter((event) => event.eventType === 'form');
    const uploads = publicEvents.filter((event) => event.eventType === 'upload');
    const newsletterLeads = forms.filter((event) => event.newsletter === 'Y' && event.emailNormalized);
    const uniqueSessions = new Set(publicEvents.map((event) => event.sessionId).filter(Boolean));
    const uniqueEmails = new Set(forms.map((event) => event.emailNormalized).filter(Boolean));
    const locationCounts = new Map();

    publicEvents.forEach((event) => {
      const locationLabel = event.locationLabel || '';
      if (!locationLabel) return;
      locationCounts.set(locationLabel, (locationCounts.get(locationLabel) || 0) + 1);
    });

    const topLocations = Array.from(locationCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([label, count]) => ({ label, count }));

    const newsletterRows = newsletterLeads
      .slice()
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, 12)
      .map((event) => ({
        timestampUtc: event.timestampUtc,
        timestampPr: event.timestampPr,
        email: event.email,
        lastName: event.lastName,
        locationLabel: event.locationLabel,
      }));

    const summary = {
      visits: visits.length,
      forms: forms.length,
      uploads: uploads.length,
      newsletterOptIns: newsletterLeads.length,
      uniqueSessions: uniqueSessions.size,
      uniqueEmails: uniqueEmails.size,
      formConversionRate: visits.length ? Number(((forms.length / visits.length) * 100).toFixed(1)) : 0,
      uploadConversionRate: forms.length ? Number(((uploads.length / forms.length) * 100).toFixed(1)) : 0,
      newsletterRate: forms.length ? Number(((newsletterLeads.length / forms.length) * 100).toFixed(1)) : 0,
    };

    return res.json({
      ok: true,
      range: {
        preset: range.preset,
        label: range.label,
        granularity: range.granularity,
        startUtc: range.startUtc.toISOString(),
        endUtc: range.endUtc.toISOString(),
      },
      summary,
      series: buildMetricsTimeseries(publicEvents, range),
      topLocations,
      newsletterRows,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Error loading metrics overview:', err);
    return res.status(500).json({ ok: false, error: 'metrics_overview_failed' });
  }
});

app.get('/admin/metrics/export', ensureAdminAuth, async (req, res) => {
  try {
    const kind = String(req.query.kind || 'newsletter').trim().toLowerCase();
    const range = getMetricsRangeDefinition(req.query || {});
    const allEvents = await readMetricsEvents();
    const publicEvents = filterMetricsEventsByRange(
      allEvents.filter((event) => event.eventFamily === 'public'),
      range
    ).sort((a, b) => a.timestamp - b.timestamp);

    let csv = '';
    let filename = '';

    if (kind === 'events') {
      csv = convertMetricsEventsToCsv(publicEvents);
      filename = `metrics_events_${range.preset}_${formatPRDateIso(getPRDate())}.csv`;
    } else {
      const newsletterEvents = publicEvents.filter((event) => event.eventType === 'form' && event.newsletter === 'Y');
      const csvRows = [
        ['timestamp_utc', 'timestamp_pr', 'email', 'last_name', 'country', 'region', 'location_label', 'session_id'],
        ...newsletterEvents.map((event) => [
          event.timestampUtc,
          event.timestampPr,
          event.email,
          event.lastName,
          event.country,
          event.region,
          event.locationLabel,
          event.sessionId,
        ])
      ];

      csv = csvRows
        .map((row) => row.map((value) => {
          const str = String(value || '');
          if (/[",\n]/.test(str)) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        }).join(','))
        .join('\n');
      filename = `newsletter_report_${range.preset}_${formatPRDateIso(getPRDate())}.csv`;
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    return res.send(csv);
  } catch (err) {
    console.error('Error exporting metrics report:', err);
    return res.status(500).json({ ok: false, error: 'metrics_export_failed' });
  }
});

// --------- PUBLIC GALLERY API (for Yodeck / gallery.html) ----------

// List approved photos (respects galleryDisplayLimit setting)
app.get('/gallery/approved', async (req, res) => {
  const startedAtMs = Date.now();
  try {
    setGalleryJsonNoStoreHeaders(res);
    const manifest = await getGalleryApprovedManifest();
    const files = manifest.files || [];
    logObservedRouteResult(
      '/gallery/approved',
      startedAtMs,
      200,
      `ok files=${files.length} version=${manifest.manifestVersion || 'unknown'}`
    );
    res.json({
      ok: true,
      files,
      manifestVersion: manifest.manifestVersion || '',
      generatedAt: manifest.generatedAt || '',
    });
  } catch (err) {
    console.error('Error listing approved files for gallery', err);
    logObservedRouteResult('/gallery/approved', startedAtMs, 500, err && err.message ? err.message : 'list_approved_failed');
    if (!res.headersSent) {
      setGalleryJsonNoStoreHeaders(res);
      res.status(500).json({ ok: false, error: 'list_approved_failed' });
    }
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

    res.setHeader('Content-Type', 'image/jpeg');
    res.setHeader('Cache-Control', 'private, max-age=300, stale-while-revalidate=600');

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

app.get('/gallery/template-overlay/:fileId', async (req, res) => {
  const { fileId } = req.params;
  const startedAt = Date.now();
  let respondedStatus = 200;

  try {
    const driveRes = await drive.files.get(
      {
        fileId,
        alt: 'media',
      },
      { responseType: 'stream' }
    );

    try {
      const metadata = await drive.files.get({
        fileId,
        fields: 'mimeType'
      });
      res.setHeader('Content-Type', metadata.data.mimeType || 'image/png');
    } catch {
      res.setHeader('Content-Type', 'image/png');
    }

    console.log('Gallery template overlay stream started', {
      fileId,
      contentType: res.getHeader('Content-Type'),
    });

    driveRes.data
      .on('error', (err) => {
        respondedStatus = res.headersSent ? respondedStatus : 502;
        console.error('Drive stream error (gallery template overlay):', {
          fileId,
          status: respondedStatus,
          durationMs: Date.now() - startedAt,
          error: err && err.message ? err.message : err,
        });
        if (!res.headersSent) {
          res.end();
        }
      })
      .on('end', () => {
        console.log('Gallery template overlay stream completed', {
          fileId,
          status: respondedStatus,
          durationMs: Date.now() - startedAt,
        });
      })
      .pipe(res);
  } catch (err) {
    respondedStatus = 404;
    console.error('Error streaming gallery template overlay', {
      fileId,
      status: respondedStatus,
      durationMs: Date.now() - startedAt,
      error: err && err.message ? err.message : err,
    });
    if (!res.headersSent) {
      res.status(404).end();
    }
  }
});

app.get('/gallery/template-asset/:fileId', async (req, res) => {
  const { fileId } = req.params;
  const startedAt = Date.now();
  let respondedStatus = 200;

  try {
    const driveRes = await drive.files.get(
      {
        fileId,
        alt: 'media',
      },
      { responseType: 'stream' }
    );

    try {
      const metadata = await drive.files.get({
        fileId,
        fields: 'mimeType'
      });
      res.setHeader('Content-Type', metadata.data.mimeType || 'image/jpeg');
    } catch {
      res.setHeader('Content-Type', 'image/jpeg');
    }

    console.log('Gallery template asset stream started', {
      fileId,
      contentType: res.getHeader('Content-Type'),
    });

    driveRes.data
      .on('error', (err) => {
        respondedStatus = res.headersSent ? respondedStatus : 502;
        console.error('Drive stream error (gallery template asset):', {
          fileId,
          status: respondedStatus,
          durationMs: Date.now() - startedAt,
          error: err && err.message ? err.message : err,
        });
        if (!res.headersSent) {
          res.end();
        }
      })
      .on('end', () => {
        console.log('Gallery template asset stream completed', {
          fileId,
          status: respondedStatus,
          durationMs: Date.now() - startedAt,
        });
      })
      .pipe(res);
  } catch (err) {
    respondedStatus = 404;
    console.error('Error streaming gallery template asset', {
      fileId,
      status: respondedStatus,
      durationMs: Date.now() - startedAt,
      error: err && err.message ? err.message : err,
    });
    if (!res.headersSent) {
      res.status(404).end();
    }
  }
});

app.get('/gallery/active-overlay', async (req, res) => {
  const startedAtMs = Date.now();
  let settingsHydrated = false;
  try {
    try {
      await hydrateSettingsFromSheet();
      settingsHydrated = true;
    } catch (hydrateErr) {
      console.warn('Active overlay settings hydration failed; using last in-memory state.', hydrateErr.message || hydrateErr);
    }

    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');

    const snapshot = parseActiveTemplateSnapshot(appSettings.activeTemplateSnapshot);
    if (snapshot && String(snapshot.id || '') === String(appSettings.activeTemplateId || snapshot.id || '')) {
      const snapshotTemplate = {
        id: snapshot.id || appSettings.activeTemplateId || '',
        name: snapshot.name || '',
        createdAt: snapshot.createdAt || '',
        data: snapshot.data
      };
      console.log('Gallery active overlay served from snapshot', {
        settingsHydrated,
        templateId: String(snapshotTemplate.id || ''),
        overlayFileId: String(snapshotTemplate.data?.overlayFileId || ''),
        version: String(snapshotTemplate.data?.overlayVersion || ''),
      });
      logObservedRouteResult('/gallery/active-overlay', startedAtMs, 200, `ok source=${settingsHydrated ? 'settings_snapshot' : 'memory_snapshot_after_settings_failure'}`);
      return res.json(buildActiveOverlayResponse(snapshotTemplate, {
        source: settingsHydrated ? 'settings_snapshot' : 'memory_snapshot_after_settings_failure',
        reason: settingsHydrated ? 'active_overlay_snapshot' : 'settings_hydration_failed_using_snapshot',
      }));
    }

    if (appSettings.activeTemplateId) {
      const liveTemplate = await getTemplateById(appSettings.activeTemplateId);
      if (liveTemplate) {
        console.log('Gallery active overlay served from template row', {
          settingsHydrated,
          templateId: String(liveTemplate.id || ''),
          overlayFileId: String(liveTemplate.data?.overlayFileId || ''),
          version: String(liveTemplate.data?.overlayVersion || ''),
        });
        logObservedRouteResult('/gallery/active-overlay', startedAtMs, 200, 'ok source=template_sheet_row');
        return res.json(buildActiveOverlayResponse(liveTemplate, {
          source: 'template_sheet_row',
          reason: 'active_overlay_row_loaded',
        }));
      }

      console.warn('Gallery active overlay row missing for activeTemplateId', {
        settingsHydrated,
        activeTemplateId: String(appSettings.activeTemplateId || ''),
      });

      if (snapshot) {
        const fallbackTemplate = {
          id: snapshot.id || appSettings.activeTemplateId || '',
          name: snapshot.name || '',
          createdAt: snapshot.createdAt || '',
          data: snapshot.data
        };
        console.log('Gallery active overlay falling back to stale snapshot because row is missing', {
          activeTemplateId: String(appSettings.activeTemplateId || ''),
          templateId: String(fallbackTemplate.id || ''),
          overlayFileId: String(fallbackTemplate.data?.overlayFileId || ''),
          version: String(fallbackTemplate.data?.overlayVersion || ''),
        });
        logObservedRouteResult('/gallery/active-overlay', startedAtMs, 200, 'ok source=stale_snapshot_fallback');
        return res.json(buildActiveOverlayResponse(fallbackTemplate, {
          source: 'stale_snapshot_fallback',
          reason: 'overlay_row_missing_using_snapshot',
        }));
      }
    }

    console.log('Gallery active overlay unavailable', {
      settingsHydrated,
      activeTemplateId: String(appSettings.activeTemplateId || ''),
      hasSnapshot: Boolean(snapshot),
      reason: appSettings.activeTemplateId ? 'overlay_row_missing_no_snapshot' : 'no_active_template_id',
    });

    logObservedRouteResult('/gallery/active-overlay', startedAtMs, 200, appSettings.activeTemplateId ? 'ok source=overlay_row_missing_no_snapshot' : 'ok source=no_active_template_id');
    return res.json(buildActiveOverlayResponse(null, {
      disabled: false,
      source: settingsHydrated ? 'settings_state' : 'memory_state_after_settings_failure',
      reason: appSettings.activeTemplateId ? 'overlay_row_missing_no_snapshot' : 'no_active_template_id',
    }));
  } catch (err) {
    console.error('Error loading active gallery overlay', err);
    const snapshot = parseActiveTemplateSnapshot(appSettings.activeTemplateSnapshot);
    if (snapshot) {
      const fallbackTemplate = {
        id: snapshot.id || appSettings.activeTemplateId || '',
        name: snapshot.name || '',
        createdAt: snapshot.createdAt || '',
        data: snapshot.data
      };
      console.warn('Gallery active overlay route failed; serving last in-memory snapshot fallback', {
        activeTemplateId: String(appSettings.activeTemplateId || ''),
        templateId: String(fallbackTemplate.id || ''),
        overlayFileId: String(fallbackTemplate.data?.overlayFileId || ''),
        version: String(fallbackTemplate.data?.overlayVersion || ''),
        error: err && err.message ? err.message : err,
      });
      logObservedRouteResult('/gallery/active-overlay', startedAtMs, 200, 'ok source=route_exception_snapshot_fallback');
      return res.json(buildActiveOverlayResponse(fallbackTemplate, {
        source: 'route_exception_snapshot_fallback',
        reason: 'route_exception_using_snapshot',
      }));
    }

    logObservedRouteResult('/gallery/active-overlay', startedAtMs, 500, err && err.message ? err.message : 'active_overlay_failed');
    return res.status(500).json({
      ok: false,
      error: 'active_overlay_failed',
      meta: {
        active: false,
        disabled: false,
        reason: 'route_exception_no_snapshot',
        source: 'route_exception',
        templateId: String(appSettings.activeTemplateId || ''),
        version: '',
      }
    });
  }
});

app.get('/gallery/active-template', async (req, res) => {
  let settingsHydrated = false;
  try {
    try {
      // Use the same hydration throttling as other gallery routes. Forcing a sheet read on every
      // poll overloads Google Sheets and increases latency — a common cause of client timeouts on
      // low-memory / kiosk browsers while still refreshing within SETTINGS_REFRESH_TTL_MS.
      await hydrateSettingsFromSheet();
      settingsHydrated = true;
    } catch (hydrateErr) {
      console.warn('Active template settings hydration failed; using last in-memory state.', hydrateErr.message || hydrateErr);
    }
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');

    const snapshot = parseActiveTemplateSnapshot(appSettings.activeTemplateSnapshot);
    if (snapshot && String(snapshot.id || '') === String(appSettings.activeTemplateId || snapshot.id || '')) {
      const snapshotTemplate = {
        id: snapshot.id || appSettings.activeTemplateId || '',
        name: snapshot.name || '',
        createdAt: snapshot.createdAt || '',
        data: snapshot.data
      };
      console.log('Gallery active template served from snapshot', {
        settingsHydrated,
        templateId: String(snapshotTemplate.id || ''),
        version: buildTemplateVersion(snapshotTemplate),
      });
      return res.json(buildActiveTemplateResponse(snapshotTemplate, {
        source: settingsHydrated ? 'settings_snapshot' : 'memory_snapshot_after_settings_failure',
        reason: settingsHydrated ? 'active_template_snapshot' : 'settings_hydration_failed_using_snapshot',
      }));
    }

    if (appSettings.activeTemplateId) {
      const liveTemplate = await getTemplateById(appSettings.activeTemplateId);
      if (liveTemplate) {
        console.log('Gallery active template served from template row', {
          settingsHydrated,
          templateId: String(liveTemplate.id || ''),
          version: buildTemplateVersion(liveTemplate),
        });
        return res.json(buildActiveTemplateResponse(liveTemplate, {
          source: 'template_sheet_row',
          reason: 'active_template_row_loaded',
        }));
      }
      console.warn('Gallery active template row missing for activeTemplateId', {
        settingsHydrated,
        activeTemplateId: String(appSettings.activeTemplateId || ''),
      });
      if (snapshot) {
        const fallbackTemplate = {
          id: snapshot.id || appSettings.activeTemplateId || '',
          name: snapshot.name || '',
          createdAt: snapshot.createdAt || '',
          data: snapshot.data
        };
        console.log('Gallery active template falling back to stale snapshot because row is missing', {
          activeTemplateId: String(appSettings.activeTemplateId || ''),
          templateId: String(fallbackTemplate.id || ''),
          version: buildTemplateVersion(fallbackTemplate),
        });
        return res.json(buildActiveTemplateResponse(fallbackTemplate, {
          source: 'stale_snapshot_fallback',
          reason: 'template_row_missing_using_snapshot',
        }));
      }
    }

    console.log('Gallery active template unavailable', {
      settingsHydrated,
      activeTemplateId: String(appSettings.activeTemplateId || ''),
      hasSnapshot: Boolean(snapshot),
      reason: appSettings.activeTemplateId ? 'template_row_missing_no_snapshot' : 'no_active_template_id',
    });

    return res.json(buildActiveTemplateResponse(null, {
      disabled: false,
      source: settingsHydrated ? 'settings_state' : 'memory_state_after_settings_failure',
      reason: appSettings.activeTemplateId ? 'template_row_missing_no_snapshot' : 'no_active_template_id',
    }));
  } catch (err) {
    console.error('Error loading active gallery template', err);
    const snapshot = parseActiveTemplateSnapshot(appSettings.activeTemplateSnapshot);
    if (snapshot) {
      const fallbackTemplate = {
        id: snapshot.id || appSettings.activeTemplateId || '',
        name: snapshot.name || '',
        createdAt: snapshot.createdAt || '',
        data: snapshot.data
      };
      console.warn('Gallery active template route failed; serving last in-memory snapshot fallback', {
        activeTemplateId: String(appSettings.activeTemplateId || ''),
        templateId: String(fallbackTemplate.id || ''),
        version: buildTemplateVersion(fallbackTemplate),
        error: err && err.message ? err.message : err,
      });
      return res.json(buildActiveTemplateResponse(fallbackTemplate, {
        source: 'route_exception_snapshot_fallback',
        reason: 'route_exception_using_snapshot',
      }));
    }

    return res.status(500).json({
      ok: false,
      error: 'active_template_failed',
      meta: {
        active: false,
        disabled: false,
        reason: 'route_exception_no_snapshot',
        source: 'route_exception',
        templateId: String(appSettings.activeTemplateId || ''),
        version: '',
      }
    });
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
ensureMetricsSheetHeaders().catch(() => {});

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
app.get('/admin/templates/:id(\\d+)', ensureAdminAuth, async (req, res) => {
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
app.put('/admin/templates/:id(\\d+)', ensureAdminAuth, async (req, res) => {
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

    if (String(appSettings.activeTemplateId || '') === String(id)) {
      appSettings = await syncActiveTemplateSettings({
        ...appSettings,
        activeTemplateId: String(id),
        activeTemplateSnapshot: buildActiveTemplateSnapshotString({
          id: String(id),
          name,
          createdAt: new Date().toISOString(),
          data
        })
      });

      const settingsPersisted = await writeSettingsToSheet(appSettings, appEnabled, appSessionId);
      if (!settingsPersisted) {
        return res.status(500).json({ ok: false, error: 'active_template_sync_failed' });
      }
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('Error updating template:', err);
    res.status(500).json({ ok: false, error: 'update_template_failed' });
  }
});

// Delete template
app.delete('/admin/templates/:id(\\d+)', ensureAdminAuth, async (req, res) => {
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

    const { assetType, assetLabel } = req.body || {}; // 'background', 'logo', or 'qr'

    if (!assetType || !['background', 'logo', 'qr'].includes(assetType)) {
      return res.status(400).json({ ok: false, error: 'invalid_asset_type' });
    }

    const folderId = assetType === 'background'
      ? TEMPLATES_BG_FOLDER_ID
      : assetType === 'qr'
        ? TEMPLATES_QR_FOLDER_ID
        : TEMPLATES_LOGO_FOLDER_ID;
    const timestamp = Date.now();
    const originalExtension = req.file.originalname.split('.').pop() || 'jpg';
    const extension = sanitizeDriveFileBaseName(originalExtension, 'jpg').toLowerCase();
    const safeLabel = sanitizeDriveFileBaseName(
      assetType === 'qr' ? assetLabel : '',
      `template_${assetType}_${timestamp}`
    );
    const filename = `${safeLabel}.${extension}`;

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
      type: assetType,
      label: assetType === 'qr' ? safeLabel.replace(/_/g, ' ') : ''
    });
  } catch (err) {
    console.error('Error uploading template asset:', err);
    res.status(500).json({ ok: false, error: 'upload_asset_failed' });
  }
});

app.get('/admin/templates/background-assets', ensureAdminAuth, async (req, res) => {
  try {
    const response = await drive.files.list({
      q: `'${TEMPLATES_BG_FOLDER_ID}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime)',
      orderBy: 'createdTime desc',
      pageSize: 200,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    const assets = (response.data.files || []).map((file) => ({
      id: file.id,
      name: file.name,
      createdTime: file.createdTime || '',
    }));

    res.json({ ok: true, assets });
  } catch (err) {
    console.error('Error listing template background assets:', err);
    res.status(500).json({ ok: false, error: 'list_background_assets_failed' });
  }
});

app.get('/admin/templates/qr-assets', ensureAdminAuth, async (req, res) => {
  try {
    const response = await drive.files.list({
      q: `'${TEMPLATES_QR_FOLDER_ID}' in parents and trashed = false`,
      fields: 'files(id, name, createdTime)',
      orderBy: 'createdTime desc',
      pageSize: 200,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    const assets = (response.data.files || []).map((file) => ({
      id: file.id,
      name: file.name,
      createdTime: file.createdTime || '',
    }));

    res.json({ ok: true, assets });
  } catch (err) {
    console.error('Error listing template QR assets:', err);
    res.status(500).json({ ok: false, error: 'list_qr_assets_failed' });
  }
});

app.delete('/admin/templates/background-assets/:fileId', ensureAdminAuth, async (req, res) => {
  try {
    const { fileId } = req.params;
    if (!fileId) {
      return res.status(400).json({ ok: false, error: 'missing_file_id' });
    }

    await drive.files.update({
      fileId,
      requestBody: { trashed: true },
      supportsAllDrives: true,
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error deleting template background asset:', err);
    res.status(500).json({ ok: false, error: 'delete_background_asset_failed' });
  }
});

app.post('/admin/templates/:id(\\d+)/flattened-overlay', ensureAdminAuth, multer({ storage: multer.memoryStorage() }).single('file'), async (req, res) => {
  const { id } = req.params;
  const startedAt = Date.now();

  try {
    if (!req.file || !req.file.buffer || !req.file.buffer.length) {
      return res.status(400).json({ ok: false, error: 'missing_file' });
    }

    if (!['image/png', 'image/jpeg', 'image/webp'].includes(req.file.mimetype || '')) {
      return res.status(400).json({ ok: false, error: 'invalid_overlay_type' });
    }

    const template = await getTemplateById(id);
    if (!template) {
      return res.status(404).json({ ok: false, error: 'template_not_found' });
    }

    const previousOverlayFileId = normalizeTemplateAssetFileId(
      req.body?.previousOverlayFileId || template.data?.overlayFileId || ''
    );
    const requestedVersion = req.body?.templateVersion ? String(req.body.templateVersion).trim() : '';

    let parsedPhotoBox = null;
    if (req.body?.photoBox) {
      try {
        parsedPhotoBox = JSON.parse(req.body.photoBox);
      } catch (err) {
        console.warn('Invalid photoBox payload for flattened overlay upload:', err.message || err);
      }
    }

    const nextPhotoBox = normalizeTemplatePhotoBox(parsedPhotoBox || template.data?.photoBox);
    const overlayUpdatedAt = new Date().toISOString();
    const overlayVersion = requestedVersion || overlayUpdatedAt;
    const extension = req.file.mimetype === 'image/jpeg' ? 'jpg' : req.file.mimetype === 'image/webp' ? 'webp' : 'png';
    const filename = `${sanitizeDriveFileBaseName(template.name || `template_${id}`, `template_${id}`)}_overlay.${extension}`;

    const bufferStream = new stream.PassThrough();
    bufferStream.end(req.file.buffer);

    const uploadResponse = await drive.files.create({
      requestBody: {
        name: filename,
        mimeType: req.file.mimetype,
        parents: [PHOTO_TEMPLATES_FOLDER_ID]
      },
      media: {
        mimeType: req.file.mimetype,
        body: bufferStream
      },
      supportsAllDrives: true
    });

    const overlayFileId = String(uploadResponse.data.id || '');
    if (!overlayFileId) {
      return res.status(500).json({ ok: false, error: 'missing_overlay_file_id' });
    }

    const nextTemplateData = {
      ...(template.data || {}),
      photoBox: nextPhotoBox,
      overlayFileId,
      overlayUpdatedAt,
      overlayVersion,
    };

    const updateOk = await updateTemplateInSheet(template.id, template.name, nextTemplateData, template.isActive !== false);
    if (!updateOk) {
      await drive.files.update({
        fileId: overlayFileId,
        requestBody: { trashed: true },
        supportsAllDrives: true,
      }).catch(() => {});
      return res.status(500).json({ ok: false, error: 'overlay_sheet_update_failed' });
    }

    if (previousOverlayFileId && previousOverlayFileId !== overlayFileId) {
      drive.files.update({
        fileId: previousOverlayFileId,
        requestBody: { trashed: true },
        supportsAllDrives: true,
      }).catch((err) => {
        console.warn('Unable to trash previous overlay file after replacement:', previousOverlayFileId, err.message || err);
      });
    }

    if (String(appSettings.activeTemplateId || '') === String(template.id)) {
      appSettings = await syncActiveTemplateSettings({
        ...appSettings,
        activeTemplateId: String(template.id),
        activeTemplateSnapshot: buildActiveTemplateSnapshotString({
          id: String(template.id),
          name: template.name || '',
          createdAt: template.createdAt || overlayUpdatedAt,
          data: nextTemplateData
        })
      });
      const persisted = await writeSettingsToSheet(appSettings, appEnabled, appSessionId);
      if (!persisted) {
        console.warn('Active template settings sync failed after flattened overlay upload for template', template.id);
      }
    }

    console.log('Flattened template overlay uploaded', {
      templateId: String(template.id),
      overlayFileId,
      previousOverlayFileId,
      overlayVersion,
      durationMs: Date.now() - startedAt,
    });

    return res.json({
      ok: true,
      templateId: String(template.id),
      overlayFileId,
      overlayUpdatedAt,
      overlayVersion,
      overlayUrl: buildPublicTemplateOverlayUrl(overlayFileId),
      photoBox: nextPhotoBox,
    });
  } catch (err) {
    console.error('Error uploading flattened template overlay:', err);
    return res.status(500).json({ ok: false, error: 'upload_flattened_overlay_failed' });
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
