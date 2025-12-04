// server/index.js

const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const dotenv = require('dotenv');
const stream = require('stream');
const nodemailer = require('nodemailer');
const path = require('path');

dotenv.config();

// ---------- CONSTANTES ----------
const LUMI_LOGO_URL =
  'https://raw.githubusercontent.com/LMNRGroup/mayaguez-photoapp/refs/heads/main/Assets/Luminar%20Apps%20Horizontal%20Logo.png';

// Google Drive folders
const PENDING_FOLDER_ID  = '1n7AKxJ7Hc4QMVynY9C3d1fko6H_wT_qs'; // existing uploads
const APPROVED_FOLDER_ID = '1blA55AfkUykFcYzgFzmvthXMvVFv6I0u'; // new "Approved" folder

// ID de tu Google Sheet de logs
const SESSION_SHEET_ID =
  process.env.SESSION_SHEET_ID ||
  '1bPctG2H31Ix2N8jVNgFTgiB3FVWyr6BudXNY8YOD4GE';

// Rango donde están las columnas: timestamp_utc, timestamp_pr, event_type, email, session_id, metadata
const SESSION_SHEET_RANGE = 'A:F';

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
app.use(cors());
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

// ---------- HELPERS DE FECHA / HORA ----------

// Puerto Rico time "ahora" (GMT-4 fijo, sin DST)
function getPRDate() {
  const now = new Date();
  const utcMs = now.getTime() + now.getTimezoneOffset() * 60000;
  return new Date(utcMs - 4 * 60 * 60 * 1000);
}

// Convertir una Date (UTC) a PR
function toPR(date) {
  const utcMs = date.getTime() + date.getTimezoneOffset() * 60000;
  return new Date(utcMs - 4 * 60 * 60 * 1000);
}

// Fecha larga en español: "Miércoles 3 de diciembre de 2025"
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

// Fecha/hora corta para guardar en la columna timestamp_pr del sheet
// Ej: "03/12/2025 22:05:31"
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

// Para el reporte: "HH:00–HH:59"
function formatHourRange(hour) {
  const h = String(hour).padStart(2, '0');
  return `${h}:00–${h}:59`;
}

// Rango de HOY (en PR) pero expresado en UTC, para filtrar los logs
function getTodayPRRangeUtc() {
  const nowPR = getPRDate();

  const startPR = new Date(nowPR);
  startPR.setHours(0, 0, 0, 0);

  const endPR = new Date(nowPR);
  endPR.setHours(23, 59, 59, 999);

  // PR time → UTC (PR = UTC-4, así que sumamos 4h)
  const prToUtc = (dPR) => new Date(dPR.getTime() + 4 * 60 * 60 * 1000);

  return {
    startUtc: prToUtc(startPR),
    endUtc: prToUtc(endPR)
  };
}

// ---------- HELPERS PARA DRIVE (FOTOS) ----------

// build filename 01_DD_MM_YY-HH_MM_SS.jpeg
function buildServerFileName(counter) {
  const prNow = getPRDate();

  const pad = (n) => String(n).padStart(2, '0');

  const dd = pad(prNow.getDate());
  const mm = pad(prNow.getMonth() + 1);
  const yy = String(prNow.getFullYear()).slice(-2);
  const HH = pad(prNow.getHours());
  const MM = pad(prNow.getMinutes());
  const SS = pad(prNow.getSeconds());

  const num = pad(counter); // 01, 02, 03, ...

  return `${num}_${dd}_${mm}_${yy}-${HH}_${MM}_${SS}.jpeg`;
}

// Buscar el próximo índice para el nombre del archivo en Drive (PENDING)
async function getNextPhotoIndex() {
  let pageToken = null;
  let maxIndex = 0;

  do {
    const res = await drive.files.list({
      q: `'${PENDING_FOLDER_ID}' in parents and trashed = false`,
      fields: 'files(name), nextPageToken',
      pageSize: 100,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken
    });

    const files = res.data.files || [];
    for (const file of files) {
      const match = /^(\d{2,})_/.exec(file.name);
      if (match) {
        const idx = parseInt(match[1], 10);
        if (!Number.isNaN(idx) && idx > maxIndex) {
          maxIndex = idx;
        }
      }
    }

    pageToken = res.data.nextPageToken;
  } while (pageToken);

  return maxIndex + 1;
}

// Subir archivo a Drive → PENDING folder
async function uploadFile(fileBuffer, originalname, mimetype) {
  const bufferStream = new stream.PassThrough();
  bufferStream.end(fileBuffer);

  const nextIndex = await getNextPhotoIndex();
  const finalName = buildServerFileName(nextIndex);

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

  return { fileId: response.data.id, finalName };
}

// Obtener la próxima foto pendiente más vieja
async function getNextPendingPhoto() {
  const res = await drive.files.list({
    q: `'${PENDING_FOLDER_ID}' in parents and trashed = false`,
    orderBy: 'createdTime asc',
    pageSize: 1,
    fields: 'files(id, name)'
  });

  const files = res.data.files || [];
  if (!files.length) return null;
  return files[0]; // { id, name }
}

// ---------- HELPERS PARA SHEETS (LOGS) ----------

// Log genérico a Google Sheets
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

// Leer logs de HOY (en PR) desde el Sheet y computar stats
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
    // Solo headers, nada más
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

  // Saltamos el header row (fila 0)
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

// Calcular prime hour usando eventos del día (en PR)
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

// ---------- EMAIL DE REPORTE DIARIO (usa el Sheet) ----------
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
            <!-- Header band (igual que "Nueva familia registrada") -->
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

            <!-- Footer con logo centrado (igual que otros correos) -->
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

// ---------- RUTAS ----------

// Simple visit endpoint: FE puede llamar esto en page load
app.post('/ping', async (req, res) => {
  try {
    const sessionId = req.headers['x-session-id'] || '';
    await logEventToSheet('visit', { sessionId });
  } catch (e) {
    console.error('Error logging visit to sheet:', e);
  }
  res.json({ ok: true });
});

// Upload de foto (cuenta como "upload")
app.post('/upload', express.raw({ type: 'image/*', limit: '5mb' }), async (req, res) => {
  if (!req.body || req.body.length === 0) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  try {
    const { fileId, finalName } = await uploadFile(
      req.body,
      'ignored.jpeg',
      req.headers['content-type']
    );

    // Log a Sheet
    try {
      const sessionId = req.headers['x-session-id'] || '';
      await logEventToSheet('upload', {
        sessionId,
        metadata: { driveFileId: fileId, filename: finalName }
      });
    } catch (e) {
      console.error('Error logging upload event to sheet:', e);
    }

    res.json({ message: 'File uploaded successfully', fileId });
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

    // Log a Sheet como "form"
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

// --------- ADMIN API: review photos ----------

// Get next pending photo (for review UI)
app.get('/admin/next-photo', async (req, res) => {
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
app.get('/admin/photo/:id', async (req, res) => {
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
app.post('/admin/approve', async (req, res) => {
  try {
    const { fileId } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ error: 'missing_fileId' });
    }

    await drive.files.update({
      fileId,
      addParents: APPROVED_FOLDER_ID,
      removeParents: PENDING_FOLDER_ID,
      fields: 'id, parents'
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error approving photo:', err);
    res.status(500).json({ error: 'failed_approve' });
  }
});

// Reject: send file to Drive trash
app.post('/admin/reject', async (req, res) => {
  try {
    const { fileId } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ error: 'missing_fileId' });
    }

    await drive.files.update({
      fileId,
      requestBody: { trashed: true },
      fields: 'id, trashed'
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Error rejecting photo:', err);
    res.status(500).json({ error: 'failed_reject' });
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

// MANUAL trigger for debugging (puedes hacer POST desde Postman / curl)
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
