const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const dotenv = require('dotenv');
const stream = require('stream');
const nodemailer = require('nodemailer');

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
    const { country, lastName, timestamp } = req.body || {};

    console.log('Visit payload:', { country, lastName, timestamp });

    if (!mailTransporter) {
      console.log('Mail transporter is not configured, skipping email.');
      return res.status(500).json({ ok: false, error: 'mail_disabled' });
    }

    const subject = 'Nuevo visitante (Selfie App)';
    const text =
      `País / Ciudad: ${country || 'No provisto'}\n` +
      `Apellidos: ${lastName || 'No provisto'}\n` +
      `Hora (UTC): ${timestamp || new Date().toISOString()}`;

    await mailTransporter.sendMail({
      from: process.env.MAIL_FROM || process.env.MAIL_USER,
      to: process.env.MAIL_TO || process.env.MAIL_USER,
      subject,
      text
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
