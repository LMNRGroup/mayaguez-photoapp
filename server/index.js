const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const dotenv = require('dotenv');
const stream = require('stream');
const nodemailer = require('nodemailer');
const path = require('path');
const LUMI_LOGO_URL = 
  'https://raw.githubusercontent.com/LMNRGroup/mayaguez-photoapp/refs/heads/main/Assets/Luminar%20Apps%20Horizontal%20Logo.png';
dotenv.config();

// ---------- Mail setup ----------
let mailTransporter = null;

if (process.env.MAIL_USER && process.env.MAIL_PASS) {
  mailTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: process.env.MAIL_USER,   // luminar apps email
      pass: process.env.MAIL_PASS    // app password
    }
  });
  console.log('Mail transporter configured');
} else {
  console.log('Mail transporter NOT configured (missing MAIL_USER or MAIL_PASS)');
}

// ---------- Express / Drive setup ----------
const app = express();
app.use(cors());
app.use(express.json());

// Create a new JWT client using the service account
const credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);
const jwtClient = new google.auth.JWT(
  credentials.client_email,
  null,
  credentials.private_key,
  ['https://www.googleapis.com/auth/drive']
);

// Create a new Drive client
const drive = google.drive({ version: 'v3', auth: jwtClient });
const DRIVE_FOLDER_ID = '1n7AKxJ7Hc4QMVynY9C3d1fko6H_wT_qs';

// --- Helper: get Puerto Rico time (GMT-4) ---
function getPRDate() {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  return new Date(utc - 4 * 60 * 60000);
}

// --- Helper: build filename 01_DD_MM_YY-HH_MM_SS.jpeg ---
function buildServerFileName(counter) {
  const prNow = getPRDate();

  const dd = String(prNow.getDate()).padStart(2, '0');
  const mm = String(prNow.getMonth() + 1).padStart(2, '0');
  const yy = String(prNow.getFullYear()).slice(-2);
  const HH = String(prNow.getHours()).padStart(2, '0');
  const MM = String(prNow.getMinutes()).padStart(2, '0');
  const SS = String(prNow.getSeconds()).padStart(2, '0');

  const num = String(counter).padStart(2, '0'); // 01, 02, 03, ...

  return `${num}_${dd}_${mm}_${yy}-${HH}_${MM}_${SS}.jpeg`;
}

// --- Helper: read existing files and find next index ---
async function getNextPhotoIndex() {
  let pageToken = null;
  let maxIndex = 0;

  do {
    const res = await drive.files.list({
      q: `'${DRIVE_FOLDER_ID}' in parents and trashed = false`,
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

// --- Main uploader: uses global index ---
async function uploadFile(fileBuffer, originalname, mimetype) {
  const bufferStream = new stream.PassThrough();
  bufferStream.end(fileBuffer);

  // 1) Look at existing files in the folder and get next number
  const nextIndex = await getNextPhotoIndex();

  // 2) Build filename like 03_02_12_25-20_15_09.jpeg
  const finalName = buildServerFileName(nextIndex);

  const response = await drive.files.create({
    requestBody: {
      name: finalName,
      mimeType: mimetype,
      parents: [DRIVE_FOLDER_ID]
    },
    media: {
      mimeType: mimetype,
      body: bufferStream
    },
    supportsAllDrives: true
  });

  return response.data.id;
}

app.post('/upload', express.raw({ type: 'image/*', limit: '5mb' }), async (req, res) => {
  if (!req.body || req.body.length === 0) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  try {
    const fileId = await uploadFile(
    req.body,
    'ignored.jpeg',
    req.headers['content-type']
  );
    res.json({ message: 'File uploaded successfully', fileId: fileId });
  } catch (error) {
    console.error('Error uploading file:', error);
    res.status(500).json({ error: 'Error uploading file', details: error.message });
  }
});

// ---------- Visit logging ----------
app.post('/visit', async (req, res) => {
  try {
    const { country, lastName, email, newsletter, timestamp } = req.body || {};

    console.log('Visit payload:', { country, lastName, email, newsletter, timestamp });

    if (!mailTransporter) {
      console.log('Mail transporter is not configured, skipping email.');
      return res.status(500).json({ ok: false, error: 'mail_disabled' });
    }

    const subject = 'Nueva familia registrada (Selfie App)';

    // Plain-text fallback (for old email clients)
    const text =
      'Una nueva familia ha sido registrada en el sistema.\n\n' +
      `La familia nos visita desde: ${country || 'No provisto'}\n` +
      `Apellidos de la familia: ${lastName || 'No provisto'}\n` +
      `Correo electrónico de la familia: ${email || 'No provisto'}\n` +
      `Acepta recibir noticias y ofertas de MUNICIPIO DE MAYAGÜEZ.: ${
        newsletter ? 'Sí' : 'No'
      }\n\n` +
      `Fecha y hora (UTC): ${timestamp || new Date().toISOString()}`;

    // Branded HTML version
    const html = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Nueva familia registrada</title>
  <style>
    /* Intento de usar tu font; algunos clientes (Gmail) pueden ignorarlo,
       pero no rompe nada y otros (Apple Mail, etc.) sí lo usarán. */
    @font-face {
      font-family: 'HelveticaNowTextExtraBold';
      src: url('https://raw.githubusercontent.com/LMNRGroup/mayaguez-photoapp/refs/heads/main/Assets/HelveticaNowText-ExtraBold.ttf.woff') format('woff');
      font-weight: 700;
      font-style: normal;
    }

    body {
      margin: 0;
      padding: 0;
      background: #f4f4f6;
      -webkit-font-smoothing: antialiased;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    }

    .wrapper {
      width: 100%;
      padding: 24px 12px;
      box-sizing: border-box;
    }

    .card {
      max-width: 620px;
      margin: 0 auto;
      background: #ffffff;
      border-radius: 14px;
      box-shadow: 0 6px 18px rgba(0, 0, 0, 0.08);
      overflow: hidden;
    }

    .header {
      padding: 18px 24px 10px;
      background: linear-gradient(90deg, #b01b2e, #d7443f);
      color: #ffffff;
    }

    .header-title {
      font-family: 'HelveticaNowTextExtraBold', 'Segoe UI', sans-serif;
      font-size: 18px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .header-chip {
      display: inline-block;
      margin-top: 8px;
      padding: 4px 11px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.16);
      font-size: 11px;
    }

    .content {
      padding: 20px 24px 8px;
      color: #333333;
      font-size: 14px;
      line-height: 1.5;
    }

    .content p {
      margin: 0 0 14px;
    }

    .field-row {
      margin-bottom: 6px;
    }

    .field-label {
      font-weight: 600;
      color: #444444;
    }

    .footer {
      padding: 14px 24px 16px;
      border-top: 1px solid #eeeeee;
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 11px;
      color: #888888;
    }

    .footer-logo {
      display: block;
      max-height: 26px;
      width: auto;
    }

    @media (max-width: 480px) {
      .card {
        border-radius: 0;
      }
      .header, .content, .footer {
        padding-left: 16px;
        padding-right: 16px;
      }
    }
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="card">
      <div class="header">
        <div class="header-title">Nueva familia registrada</div>
        <div class="header-chip">Selfie App • Municipio de Mayagüez</div>
      </div>

      <div class="content">
        <p>Se ha registrado una nueva familia en el sistema:</p>

        <div class="field-row">
          <span class="field-label">Nos visitan desde:</span>
          <span> ${country || 'No provisto'}</span>
        </div>

        <div class="field-row">
          <span class="field-label">Apellidos de la familia:</span>
          <span> ${lastName || 'No provisto'}</span>
        </div>

        <div class="field-row">
          <span class="field-label">Correo electrónico:</span>
          <span> ${email || 'No provisto'}</span>
        </div>

        <div class="field-row">
          <span class="field-label">Acepta recibir noticias y ofertas:</span>
          <span> ${newsletter ? 'Sí' : 'No'}</span>
        </div>

        <div class="field-row" style="margin-top: 10px;">
          <span class="field-label">Fecha y hora (UTC):</span>
          <span> ${timestamp || new Date().toISOString()}</span>
        </div>
      </div>

      <div class="footer">
        <img src="${LUMI_LOGO_URL}" alt="Luminar Apps" class="footer-logo" />
        <span>Este correo fue generado automáticamente por tu aplicación de selfies.</span>
      </div>
    </div>
  </div>
</body>
</html>
`;

    await mailTransporter.sendMail({
      from: process.env.MAIL_FROM || process.env.MAIL_USER,
      to: process.env.MAIL_TO || process.env.MAIL_USER,
      subject,
      text,   // Plain TxtEML Fallback
      html    // Branded Version
    });

    console.log('Email sent OK');
    res.json({ ok: true });
  } catch (err) {
    console.error('Error sending visit email:', err);
    res.status(500).json({ ok: false, error: 'email_failed' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
