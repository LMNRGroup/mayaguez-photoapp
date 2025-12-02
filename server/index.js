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
  ['https://www.googleapis.com/auth/drive.file']
);

// Create a new Drive client
const drive = google.drive({ version: 'v3', auth: jwtClient });

async function uploadFile(fileBuffer, originalname, mimetype) {
  const bufferStream = new stream.PassThrough();
  bufferStream.end(fileBuffer);

  const response = await drive.files.create({
    requestBody: {
      name: originalname,
      mimeType: mimetype,
      parents: ['0AGWrIF2PPvUAUk9PVA'] // Your folder 
    },
    media: {
      mimeType: mimetype,
      body: bufferStream
    },
  });

  return response.data.id;
}

app.post('/upload', express.raw({ type: 'image/*', limit: '5mb' }), async (req, res) => {
  if (!req.body || req.body.length === 0) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  try {
    const fileId = await uploadFile(req.body, req.headers['x-file-name'], req.headers['content-type']);
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
