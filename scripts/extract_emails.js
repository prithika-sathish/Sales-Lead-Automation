const fs = require('fs');
const parser = require('email-addresses');

const filePath = process.argv[2];
if (!filePath) {
  console.log('[]');
  process.exit(0);
}

let text = '';
try {
  text = fs.readFileSync(filePath, 'utf8');
} catch {
  console.log('[]');
  process.exit(0);
}

let parsed = [];
const candidates = text.match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g) || [];
const emails = [];
for (const raw of candidates) {
  const candidate = String(raw).trim();
  if (!candidate) continue;
  try {
    parsed = parser.parseOneAddress(candidate);
    if (parsed && parsed.address && typeof parsed.address === 'string') {
      emails.push(parsed.address.trim().toLowerCase());
    }
  } catch {
    // Skip invalid candidate.
  }
}

const unique = Array.from(new Set(emails));
process.stdout.write(JSON.stringify(unique));
