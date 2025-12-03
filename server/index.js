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

    // Plain-text fallback (kept simple)
    const text =
      'Una nueva familia ha sido registrada en el sistema.\n\n' +
      `La familia nos visita desde: ${country || 'No provisto'}\n` +
      `Apellidos de la familia: ${lastName || 'No provisto'}\n` +
      `Correo electrónico de la familia: ${email || 'No provisto'}\n` +
      `Acepta recibir noticias y ofertas de MUNICIPIO DE MAYAGÜEZ.: ${newsletter ? 'Sí' : 'No'}\n\n` +
      `Fecha y hora (UTC): ${timestamp || new Date().toISOString()}`;

    // HTML version – left-aligned outer block, inner content centered nicely
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
              <td style="padding:20px 24px 16px 24px;background:#c0392b;
                         border-radius:8px 8px 0 0;color:#ffffff;text-align:center;">
                <div style="font-size:15px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;">
                  NUEVA FAMILIA REGISTRADA
                </div>
                <div style="margin-top:4px;font-size:11px;opacity:0.9;">
                  Selfie App · Municipio de Mayagüez
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
                  <strong>Fecha y hora (UTC):</strong> ${timestamp || new Date().toISOString()}
                </p>
              </td>
            </tr>

            <!-- Footer with logo LEFT and text below -->
            <tr>
              <td style="padding:18px 24px 20px 24px;text-align:left;border-top:1px solid #f0f0f0;">
                <img
                  src="https://raw.githubusercontent.com/LMNRGroup/mayaguez-photoapp/refs/heads/main/Assets/Luminar%20Apps%20Horizontal%20Logo.png"
                  alt="Luminar Apps"
                  style="display:block;height:32px;margin:0 0 6px 0;"
                />
                <div style="font-size:10px;color:#aaaaaa;line-height:1.4;">
                  Este correo fue generado automáticamente por tu aplicación de selfies.
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;

    await mailTransporter.sendMail({
      from: process.env.MAIL_FROM || process.env.MAIL_USER,
      to: process.env.MAIL_TO || process.env.MAIL_USER,
      subject,
      text,  // plain text fallback
      html   // branded version
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
