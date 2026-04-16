var https = require('https');
var http = require('http');
var url = require('url');
var crypto = require('crypto');

var PORT = process.env.PORT || 3000;

// eBay credentials (yours - default)
var CLIENT_ID = process.env.EBAY_CLIENT_ID || 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';
var CLIENT_SECRET = process.env.EBAY_CLIENT_SECRET || 'PRD-6c135696e4a6-8789-475a-8eaf-1662';
var DEV_ID = process.env.EBAY_DEV_ID || '3e7db631-fffe-4cd8-92b6-6bca13515742';
var USER_TOKEN = process.env.EBAY_USER_TOKEN || 'v^1.1#i^1#f^0#r^1#p^3#I^3#t^Ul4xMF8yOkVBM0U2OUZBMEY0MDY0QjYxOEVCQTM2OTZFMTg0OEIwXzJfMSNFXjI2MA==';
var ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || '';
var SENDGRID_KEY = process.env.SENDGRID_KEY || '';
var MONGODB_URI = (process.env.MONGODB_URI || '').trim();
var ADMIN_KEY = process.env.ADMIN_KEY || 'bfa-admin-2025-secret';

// MongoDB connection
var db = null;
var mongoClient = null;

function connectMongo(cb) {
  if (db) { cb(null, db); return; }
  if (!MONGODB_URI) { cb('No MongoDB URI configured'); return; }
  try {
    var mongodb = require('mongodb');
    var client = new mongodb.MongoClient(MONGODB_URI, {
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000,
      socketTimeoutMS: 10000
    });
    client.connect(function(err) {
      if (err) { console.log('MongoDB connect error:', err.message); cb(err); return; }
      mongoClient = client;
      db = client.db('booksforages');
      console.log('MongoDB connected successfully');
      cb(null, db);
    });
  } catch(e) {
    console.log('MongoDB error:', e.message);
    cb('MongoDB not available: ' + e.message);
  }
}

// In-memory fallback when MongoDB is not available
var inMemorySubscribers = {
  'BFA-ADMIN': {
    code: 'BFA-ADMIN',
    businessName: 'Books for Ages HQ',
    email: 'Codexbrothers@yahoo.com',
    active: true,
    isAdmin: true,
    employees: [
      { name: 'Adam', pin: '8792' },
      { name: 'Lizbeth', pin: '7284' },
      { name: 'Josselin', pin: '9373' },
      { name: 'Stephanie', pin: '3842' },
      { name: 'Cris', pin: '8792' }
    ],
    ebayClientId: CLIENT_ID,
    ebayClientSecret: CLIENT_SECRET,
    ebayDevId: DEV_ID,
    ebayUserToken: USER_TOKEN,
    createdAt: new Date().toISOString(),
    notes: 'Master admin account'
  }
};
var inMemoryListings = [];

// Rate limiting
var requestCounts = {};
var RATE_LIMIT = 100;
var RATE_WINDOW = 60 * 1000;
setInterval(function() { requestCounts = {}; }, 5 * 60 * 1000);

function checkRateLimit(ip) {
  var now = Date.now();
  if (!requestCounts[ip]) requestCounts[ip] = { count: 0, start: now };
  if (now - requestCounts[ip].start > RATE_WINDOW) requestCounts[ip] = { count: 0, start: now };
  requestCounts[ip].count++;
  return requestCounts[ip].count <= RATE_LIMIT;
}

function addSecurityHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-admin-key, x-access-code');
}

function parseBody(req, cb) {
  var body = '';
  req.on('data', function(chunk) { body += chunk; if (body.length > 10 * 1024 * 1024) { req.destroy(); cb('Too large'); } });
  req.on('end', function() { try { cb(null, JSON.parse(body || '{}')); } catch(e) { cb(null, {}); } });
}

// eBay token cache per subscriber
var tokenCache = {};

function getToken(clientId, clientSecret, cb) {
  var cacheKey = clientId;
  var cached = tokenCache[cacheKey];
  if (cached && Date.now() < cached.expiry) { cb(null, cached.token); return; }

  var credentials = Buffer.from(clientId + ':' + clientSecret).toString('base64');
  var body = 'grant_type=client_credentials&scope=https%3A%2F%2Fapi.ebay.com%2Foauth%2Fapi_scope';
  var opts = {
    hostname: 'api.ebay.com', path: '/identity/v1/oauth2/token', method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic ' + credentials, 'Content-Length': Buffer.byteLength(body) }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        var json = JSON.parse(data);
        if (json.access_token) {
          tokenCache[cacheKey] = { token: json.access_token, expiry: Date.now() + (json.expires_in - 60) * 1000 };
          cb(null, json.access_token);
        } else { cb('Token error: ' + data); }
      } catch(e) { cb('Token parse error'); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Timeout'); });
  req.write(body); req.end();
}

function searchEbay(keywords, conditionId, token, cb) {
  var condFilter = conditionId ? ',conditions:{' + conditionId + '}' : '';
  var query = '/buy/browse/v1/item_summary/search?q=' + encodeURIComponent(keywords)
    + '&category_ids=267&filter=buyingOptions:{FIXED_PRICE}' + condFilter + '&limit=20&sort=price';
  var opts = {
    hostname: 'api.ebay.com', path: query, method: 'GET',
    headers: { 'Authorization': 'Bearer ' + token, 'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US' }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        var json = JSON.parse(data);
        var items = (json.itemSummaries || []).filter(function(i) { return i.price && parseFloat(i.price.value) > 0; });
        if (!items.length) { cb(null, { average: null, count: 0 }); return; }
        var prices = items.map(function(i) { return parseFloat(i.price.value); });
        var avg = prices.reduce(function(a, b) { return a + b; }, 0) / prices.length;
        cb(null, { average: Math.round(avg * 100) / 100, count: prices.length, items: items.slice(0, 5).map(function(i) { return { title: i.title, price: i.price.value, url: i.itemWebUrl }; }) });
      } catch(e) { cb('Parse error'); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(15000, function() { req.destroy(); cb('Timeout'); });
  req.end();
}

function esc(s) {
  s = s || '';
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Get subscriber by access code
function getSubscriber(code, cb) {
  connectMongo(function(err, database) {
    if (err || !database) {
      var sub = inMemorySubscribers[code.toUpperCase()];
      cb(null, sub || null);
      return;
    }
    database.collection('subscribers').findOne({ code: code.toUpperCase() }, cb);
  });
}

// Send email via SendGrid
function sendEmail(to, subject, html, cb) {
  if (!SENDGRID_KEY) { if (cb) cb('No SendGrid key'); return; }
  var body = JSON.stringify({
    personalizations: [{ to: [{ email: to }] }],
    from: { email: 'reports@booksforages.com', name: 'Books for Ages' },
    subject: subject,
    content: [{ type: 'text/html', value: html }]
  });
  var opts = {
    hostname: 'api.sendgrid.com', path: '/v3/mail/send', method: 'POST',
    headers: { 'Authorization': 'Bearer ' + SENDGRID_KEY, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  };
  var req = https.request(opts, function(res) {
    if (cb) cb(res.statusCode === 202 ? null : 'SendGrid error: ' + res.statusCode);
  });
  req.on('error', function(e) { if (cb) cb(e.message); });
  req.write(body); req.end();
}

// Schedule daily reports at 8pm
function scheduleDailyReports() {
  setInterval(function() {
    var now = new Date();
    if (now.getHours() === 20 && now.getMinutes() === 0) {
      sendDailyReports();
    }
  }, 60 * 1000);
}

function sendDailyReports() {
  var today = new Date().toISOString().split('T')[0];
  connectMongo(function(err, database) {
    if (err || !database) return;
    database.collection('listings').find({ date: today }).toArray(function(err, listings) {
      if (err || !listings.length) return;
      var bySubscriber = {};
      listings.forEach(function(l) {
        if (!bySubscriber[l.subscriberCode]) bySubscriber[l.subscriberCode] = [];
        bySubscriber[l.subscriberCode].push(l);
      });
      Object.keys(bySubscriber).forEach(function(code) {
        getSubscriber(code, function(err, sub) {
          if (err || !sub || !sub.email) return;
          var items = bySubscriber[code];
          var rows = items.map(function(l) {
            return '<tr><td>' + l.date + '</td><td>' + l.time + '</td><td>' + esc(l.bookTitle) + '</td><td>' + esc(l.condition) + '</td><td>$' + l.price + '</td><td>' + esc(l.employee) + '</td></tr>';
          }).join('');
          var html = '<h2>' + esc(sub.businessName) + ' - Daily Report</h2>'
            + '<p>Total listings today: <strong>' + items.length + '</strong></p>'
            + '<table border="1" cellpadding="8" style="border-collapse:collapse;width:100%">'
            + '<tr style="background:#ffc700"><th>Date</th><th>Time</th><th>Book Title</th><th>Condition</th><th>Price</th><th>Employee</th></tr>'
            + rows + '</table>';
          sendEmail(sub.email, 'Daily Report - ' + today + ' - ' + sub.businessName, html);
        });
      });
    });
  });
}

// Log a listing to database
function logListing(data, cb) {
  var now = new Date();
  var entry = {
    subscriberCode: (data.subscriberCode || 'BFA-ADMIN').toUpperCase(),
    employee: data.employee || 'Unknown',
    bookTitle: data.bookTitle || data.title || '',
    condition: data.condition || '',
    price: data.price || 0,
    isbn: data.isbn || '',
    date: now.toISOString().split('T')[0],
    time: now.toTimeString().split(' ')[0],
    createdAt: now.toISOString(),
    ebayListingId: data.listingId || ''
  };
  connectMongo(function(err, database) {
    if (err || !database) {
      inMemoryListings.push(entry);
      if (cb) cb(null, entry);
      return;
    }
    database.collection('listings').insertOne(entry, function(err, result) {
      if (cb) cb(err, entry);
    });
  });
}

var COND_MAP = { '1000': 'NEW', '2750': 'LIKE_NEW', '4000': 'VERY_GOOD', '5000': 'GOOD', '6000': 'ACCEPTABLE' };

function createListing(title, description, price, isbn, conditionId, pictureUrls, language, author, bookTitle, publisher, year, edition, format, signed, signedBy, inscribed, illustrator, topic, features, vintage, sku, userToken, devId, cb) {
  var scheduleTime = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
  var pictures = '';
  if (pictureUrls && pictureUrls.length > 0) {
    pictures = '<PictureDetails>';
    for (var pi = 0; pi < pictureUrls.length && pi < 12; pi++) {
      pictures += '<PictureURL>' + pictureUrls[pi] + '</PictureURL>';
    }
    pictures += '</PictureDetails>';
  }
  var xml = '<?xml version="1.0" encoding="utf-8"?>'
    + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + userToken + '</eBayAuthToken></RequesterCredentials>'
    + '<Item>'
    + '<Title>' + esc(title).substring(0, 80) + '</Title>'
    + '<Description><![CDATA[' + description + ']]></Description>'
    + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
    + '<StartPrice>' + parseFloat(price).toFixed(2) + '</StartPrice>'
    + '<ConditionID>' + conditionId + '</ConditionID>'
    + '<Country>US</Country>'
    + '<Currency>USD</Currency>'
    + '<DispatchTimeMax>3</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<BestOfferDetails><BestOfferEnabled>true</BestOfferEnabled></BestOfferDetails>'
    + '<ScheduleTime>' + scheduleTime + '</ScheduleTime>'
    + '<ShippingDetails>'
    + '<ShippingServiceOptions>'
    + '<ShippingServicePriority>1</ShippingServicePriority>'
    + '<ShippingService>USPSMedia</ShippingService>'
    + '<ShippingServiceCost>3.99</ShippingServiceCost>'
    + '<FreeShipping>false</FreeShipping>'
    + '</ShippingServiceOptions>'
    + '</ShippingDetails>'
    + '<ReturnPolicy>'
    + '<ReturnsAcceptedOption>ReturnsAccepted</ReturnsAcceptedOption>'
    + '<RefundOption>MoneyBack</RefundOption>'
    + '<ReturnsWithinOption>Days_30</ReturnsWithinOption>'
    + '<ShippingCostPaidByOption>Buyer</ShippingCostPaidByOption>'
    + '</ReturnPolicy>'
    + '<ItemSpecifics>'
    + '<NameValueList><n>Book Title</n><Value>' + esc(bookTitle || title).substring(0, 65) + '</Value></NameValueList>'
    + '<NameValueList><n>Author</n><Value>' + esc(author || 'Unknown').substring(0, 65) + '</Value></NameValueList>'
    + '<NameValueList><n>Language</n><Value>' + esc(language || 'English') + '</Value></NameValueList>'
    + (publisher ? '<NameValueList><n>Publisher</n><Value>' + esc(publisher).substring(0, 65) + '</Value></NameValueList>' : '')
    + (year ? '<NameValueList><n>Publication Year</n><Value>' + esc(year) + '</Value></NameValueList>' : '')
    + (edition ? '<NameValueList><n>Edition</n><Value>' + esc(edition).substring(0, 65) + '</Value></NameValueList>' : '')
    + (format ? '<NameValueList><n>Format</n><Value>' + esc(format) + '</Value></NameValueList>' : '')
    + (signed ? '<NameValueList><n>Signed</n><Value>Yes</Value></NameValueList>' : '')
    + (signedBy ? '<NameValueList><n>Signed By</n><Value>' + esc(signedBy).substring(0, 65) + '</Value></NameValueList>' : '')
    + (inscribed ? '<NameValueList><n>Inscribed</n><Value>Yes</Value></NameValueList>' : '')
    + (illustrator ? '<NameValueList><n>Illustrator</n><Value>' + esc(illustrator).substring(0, 65) + '</Value></NameValueList>' : '')
    + (topic ? '<NameValueList><n>Topic</n><Value>' + esc(topic).substring(0, 65) + '</Value></NameValueList>' : '')
    + (vintage ? '<NameValueList><n>Vintage</n><Value>Yes</Value></NameValueList>' : '')
    + (sku ? '<NameValueList><n>Custom SKU</n><Value>' + esc(sku).substring(0, 65) + '</Value></NameValueList>' : '')
    + (features && features.length > 0 ? (function() { var xml2 = '<NameValueList><n>Features</n>'; for (var fi = 0; fi < features.length && fi < 10; fi++) xml2 += '<Value>' + esc(String(features[fi])).substring(0, 65) + '</Value>'; xml2 += '</NameValueList>'; return xml2; })() : '')
    + (isbn && (isbn.replace(/[^0-9]/g, '').substring(0, 3) === '978' || isbn.replace(/[^0-9]/g, '').substring(0, 3) === '979') ? '<NameValueList><n>ISBN</n><Value>' + isbn.replace(/[^0-9X]/gi, '') + '</Value></NameValueList>' : '')
    + '</ItemSpecifics>'
    + pictures
    + (isbn && (isbn.replace(/[^0-9]/g, '').substring(0, 3) === '978' || isbn.replace(/[^0-9]/g, '').substring(0, 3) === '979') ? '<ProductListingDetails><ISBN>' + isbn.replace(/[^0-9X]/gi, '') + '</ISBN><IncludeeBayProductDetails>false</IncludeeBayProductDetails></ProductListingDetails>' : '')
    + '</Item>'
    + '</AddItemRequest>';

  var opts = {
    hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
    headers: {
      'X-EBAY-API-SITEID': '0', 'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'AddItem', 'X-EBAY-API-DEV-NAME': devId,
      'X-EBAY-API-APP-NAME': CLIENT_ID, 'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Type': 'text/xml', 'Content-Length': Buffer.byteLength(xml)
    }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      var idMatch = data.match(/<ItemID>(\d+)<\/ItemID>/);
      var errMatch = data.match(/<LongMessage>(.*?)<\/LongMessage>/);
      if (idMatch) { cb(null, idMatch[1]); }
      else { cb(errMatch ? errMatch[1] : 'Unknown eBay error'); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(30000, function() { req.destroy(); cb('Timeout'); });
  req.write(xml); req.end();
}

function uploadPicture(base64Image, userToken, devId, cb) {
  var mediaType = 'image/jpeg';
  if (base64Image.startsWith('data:')) {
    var parts = base64Image.split(',');
    mediaType = parts[0].split(':')[1].split(';')[0];
    base64Image = parts[1];
  }
  var boundary = 'MIME_boundary_' + Date.now();
  var xml = '<?xml version="1.0" encoding="utf-8"?><UploadSiteHostedPicturesRequest xmlns="urn:ebay:apis:eBLBaseComponents"><RequesterCredentials><eBayAuthToken>' + userToken + '</eBayAuthToken></RequesterCredentials><PictureSet>Supersize</PictureSet></UploadSiteHostedPicturesRequest>';
  var imgBuffer = Buffer.from(base64Image, 'base64');
  var body = '--' + boundary + '\r\nContent-Disposition: form-data; name="XML Payload"\r\nContent-Type: text/xml;charset=utf-8\r\n\r\n' + xml + '\r\n'
    + '--' + boundary + '\r\nContent-Disposition: form-data; name="image"; filename="book.jpg"\r\nContent-Type: ' + mediaType + '\r\nContent-Transfer-Encoding: binary\r\n\r\n';
  var bodyBuffer = Buffer.concat([Buffer.from(body), imgBuffer, Buffer.from('\r\n--' + boundary + '--\r\n')]);
  var opts = {
    hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
    headers: {
      'X-EBAY-API-SITEID': '0', 'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'UploadSiteHostedPictures', 'X-EBAY-API-DEV-NAME': devId,
      'X-EBAY-API-APP-NAME': CLIENT_ID, 'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Type': 'multipart/form-data; boundary=' + boundary, 'Content-Length': bodyBuffer.length
    }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      var match = data.match(/<FullURL>(.*?)<\/FullURL>/);
      if (match) { cb(null, match[1]); } else { cb('Upload failed'); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(30000, function() { req.destroy(); cb('Timeout'); });
  req.write(bodyBuffer); req.end();
}

// ===================== MAIN SERVER =====================
var server = http.createServer(function(req, res) {
  addSecurityHeaders(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  var parsed = url.parse(req.url, true);
  var pathname = parsed.pathname;
  var ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';

  if (!checkRateLimit(ip)) { res.writeHead(429); res.end(JSON.stringify({ error: 'Too many requests' })); return; }

  // ── Health check ──
  if (pathname === '/health') {
    res.writeHead(200); res.end(JSON.stringify({ status: 'ok', mongo: !!db })); return;
  }

  // ── Claude proxy ──
  if (req.method === 'POST' && pathname === '/claude') {
    parseBody(req, function(err, payload) {
      if (err) { res.writeHead(400); res.end('{}'); return; }
      var postData = JSON.stringify(payload);
      var opts = {
        hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(postData) }
      };
      var apiReq = https.request(opts, function(apiRes) {
        var data = '';
        apiRes.on('data', function(c) { data += c; });
        apiRes.on('end', function() { res.writeHead(apiRes.statusCode, { 'Content-Type': 'application/json' }); res.end(data); });
      });
      apiReq.on('error', function(e) { res.writeHead(500); res.end(JSON.stringify({ error: e.message })); });
      apiReq.setTimeout(60000, function() { apiReq.destroy(); res.writeHead(504); res.end('{}'); });
      apiReq.write(postData); apiReq.end();
    });
    return;
  }

  // ── Validate access code ──
  if (req.method === 'POST' && pathname === '/validate-code') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      getSubscriber(code, function(err, sub) {
        if (err || !sub) { res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Invalid access code' })); return; }
        if (!sub.active) { res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Your subscription is inactive. Please contact Books for Ages.' })); return; }
        res.writeHead(200); res.end(JSON.stringify({
          valid: true,
          businessName: sub.businessName,
          employees: sub.employees || [],
          reportEmail: sub.email
        }));
      });
    });
    return;
  }

  // ── Price search ──
  if ((pathname === '/price' || pathname === '/sold') && req.method === 'GET') {
    var title = parsed.query.title || '';
    var author = parsed.query.author || '';
    var isbn = (parsed.query.isbn || '').replace(/[^0-9X]/gi, '');
    var year = parsed.query.year || '';
    var publisher = parsed.query.publisher || '';
    var cond = parsed.query.condition || '3000';
    var signed = parsed.query.signed === '1';
    var conditionId = COND_MAP[cond] || 'GOOD';
    var code = (parsed.query.code || 'BFA-ADMIN').toUpperCase();

    getSubscriber(code, function(err, sub) {
      var clientId = (sub && sub.ebayClientId) || CLIENT_ID;
      var clientSecret = (sub && sub.ebayClientSecret) || CLIENT_SECRET;

      var kwParts = [title, author, year, publisher].filter(Boolean);
      var kw = isbn ? isbn : (kwParts.join(' ') + (signed ? ' signed' : ''));
      if (!kw) { res.writeHead(400); res.end(JSON.stringify({ error: 'missing search terms' })); return; }

      getToken(clientId, clientSecret, function(err, token) {
        if (err) { res.writeHead(200); res.end(JSON.stringify({ error: err, average: null, count: 0 })); return; }
        searchEbay(kw, conditionId, token, function(result, searchErr) {
          res.writeHead(200); res.end(JSON.stringify(searchErr ? { error: searchErr, average: null, count: 0 } : result));
        });
      });
    });
    return;
  }

  // ── Upload photo ──
  if (req.method === 'POST' && pathname === '/upload') {
    parseBody(req, function(err, data) {
      var code = (data.code || 'BFA-ADMIN').toUpperCase();
      getSubscriber(code, function(err, sub) {
        var userToken = (sub && sub.ebayUserToken) || USER_TOKEN;
        var devId = (sub && sub.ebayDevId) || DEV_ID;
        uploadPicture(data.image || '', userToken, devId, function(err, pictureUrl) {
          if (err) { res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
          res.writeHead(200); res.end(JSON.stringify({ pictureUrl: pictureUrl }));
        });
      });
    });
    return;
  }

  // ── List on eBay ──
  if (req.method === 'POST' && pathname === '/list') {
    parseBody(req, function(err, data) {
      var code = (data.subscriberCode || 'BFA-ADMIN').toUpperCase();
      getSubscriber(code, function(err, sub) {
        var userToken = (sub && sub.ebayUserToken) || USER_TOKEN;
        var devId = (sub && sub.ebayDevId) || DEV_ID;
        var price = parseFloat(data.price) || 9.99;
        var isbn = data.isbn || '';
        var conditionId = parseInt(data.conditionId) || 5000;
        var pictureUrl = data.pictureUrl || '';
        var pictureUrls = data.pictureUrls || (pictureUrl ? [pictureUrl] : []);
        var language = data.language || 'English';
        var author = data.author || 'Unknown';
        var bookTitle = data.bookTitle || data.title || '';
        var publisher = data.publisher || '';
        var year = data.year || '';
        var edition = data.edition || '';
        var format = data.format || '';
        var signed = data.signed || '';
        var signedBy = data.signedBy || '';
        var inscribed = data.inscribed || '';
        var illustrator = data.illustrator || '';
        var topic = data.topic || '';
        var vintage = data.vintage || '';
        var sku = data.sku || '';
        var features = data.features || [];

        createListing(data.title, data.description, price, isbn, conditionId, pictureUrls, language, author, bookTitle, publisher, year, edition, format, signed, signedBy, inscribed, illustrator, topic, features, vintage, sku, userToken, devId, function(err, listingId) {
          if (err) { res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
          // Log the listing
          logListing({
            subscriberCode: code,
            employee: data.employee || 'Unknown',
            bookTitle: bookTitle,
            condition: data.conditionLabel || '',
            price: price,
            isbn: isbn,
            listingId: listingId
          });
          res.writeHead(200); res.end(JSON.stringify({ listingId: listingId, url: 'https://www.ebay.com/itm/' + listingId }));
        });
      });
    });
    return;
  }

  // ══════════════════════════════════════════
  // ADMIN ENDPOINTS - require admin key
  // ══════════════════════════════════════════

  var adminKey = req.headers['x-admin-key'] || '';
  var isAdmin = adminKey === ADMIN_KEY;

  // ── Get all subscribers (admin only) ──
  if (pathname === '/admin/subscribers' && req.method === 'GET') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) {
        res.writeHead(200); res.end(JSON.stringify(Object.values(inMemorySubscribers)));
        return;
      }
      database.collection('subscribers').find({}).toArray(function(err, subs) {
        res.writeHead(200); res.end(JSON.stringify(err ? [] : subs));
      });
    });
    return;
  }

  // ── Add subscriber (admin only) ──
  if (pathname === '/admin/subscribers' && req.method === 'POST') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    parseBody(req, function(err, data) {
      var code = (data.code || crypto.randomBytes(4).toString('hex').toUpperCase());
      var sub = {
        code: code.toUpperCase(),
        businessName: data.businessName || 'New Business',
        email: data.email || '',
        active: true,
        isAdmin: false,
        employees: data.employees || [],
        ebayClientId: data.ebayClientId || '',
        ebayClientSecret: data.ebayClientSecret || '',
        ebayDevId: data.ebayDevId || '',
        ebayUserToken: data.ebayUserToken || '',
        createdAt: new Date().toISOString(),
        notes: data.notes || ''
      };
      connectMongo(function(err, database) {
        if (err || !database) {
          inMemorySubscribers[sub.code] = sub;
          res.writeHead(200); res.end(JSON.stringify(sub));
          return;
        }
        database.collection('subscribers').insertOne(sub, function(err) {
          res.writeHead(200); res.end(JSON.stringify(err ? { error: err.message } : sub));
        });
      });
    });
    return;
  }

  // ── Update subscriber (admin only) ──
  if (pathname.startsWith('/admin/subscribers/') && req.method === 'PUT') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    var code = pathname.split('/')[3].toUpperCase();
    parseBody(req, function(err, data) {
      connectMongo(function(err, database) {
        if (err || !database) {
          if (inMemorySubscribers[code]) {
            Object.assign(inMemorySubscribers[code], data);
            res.writeHead(200); res.end(JSON.stringify(inMemorySubscribers[code]));
          } else { res.writeHead(404); res.end('{}'); }
          return;
        }
        database.collection('subscribers').updateOne({ code: code }, { $set: data }, function(err) {
          res.writeHead(200); res.end(JSON.stringify(err ? { error: err.message } : { success: true }));
        });
      });
    });
    return;
  }

  // ── Toggle subscriber active/inactive (admin only) ──
  if (pathname.startsWith('/admin/subscribers/') && pathname.endsWith('/toggle') && req.method === 'POST') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    var code = pathname.split('/')[3].toUpperCase();
    connectMongo(function(err, database) {
      if (err || !database) {
        if (inMemorySubscribers[code]) {
          inMemorySubscribers[code].active = !inMemorySubscribers[code].active;
          res.writeHead(200); res.end(JSON.stringify({ active: inMemorySubscribers[code].active }));
        } else { res.writeHead(404); res.end('{}'); }
        return;
      }
      database.collection('subscribers').findOne({ code: code }, function(err, sub) {
        if (!sub) { res.writeHead(404); res.end('{}'); return; }
        database.collection('subscribers').updateOne({ code: code }, { $set: { active: !sub.active } }, function(err) {
          res.writeHead(200); res.end(JSON.stringify({ active: !sub.active }));
        });
      });
    });
    return;
  }

  // ── Get listings/activity (admin sees all, subscriber sees own) ──
  if (pathname === '/admin/listings' && req.method === 'GET') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    var dateFilter = parsed.query.date || new Date().toISOString().split('T')[0];
    var subFilter = parsed.query.code || null;
    connectMongo(function(err, database) {
      if (err || !database) {
        var filtered = inMemoryListings.filter(function(l) {
          return l.date === dateFilter && (!subFilter || l.subscriberCode === subFilter.toUpperCase());
        });
        res.writeHead(200); res.end(JSON.stringify(filtered));
        return;
      }
      var query = { date: dateFilter };
      if (subFilter) query.subscriberCode = subFilter.toUpperCase();
      database.collection('listings').find(query).sort({ createdAt: -1 }).toArray(function(err, listings) {
        res.writeHead(200); res.end(JSON.stringify(err ? [] : listings));
      });
    });
    return;
  }

  // ── Subscriber self-service: get own listings ──
  if (pathname === '/my/listings' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var dateFilter = parsed.query.date || new Date().toISOString().split('T')[0];
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) {
          var filtered = inMemoryListings.filter(function(l) { return l.subscriberCode === code && l.date === dateFilter; });
          res.writeHead(200); res.end(JSON.stringify(filtered));
          return;
        }
        database.collection('listings').find({ subscriberCode: code, date: dateFilter }).sort({ createdAt: -1 }).toArray(function(err, listings) {
          res.writeHead(200); res.end(JSON.stringify(err ? [] : listings));
        });
      });
    });
    return;
  }

  // ── Subscriber self-service: update own settings ──
  if (pathname === '/my/settings' && req.method === 'PUT') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      getSubscriber(code, function(err, sub) {
        if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        // Only allow updating safe fields
        var allowed = { employees: data.employees, email: data.email, businessName: data.businessName, ebayClientId: data.ebayClientId, ebayClientSecret: data.ebayClientSecret, ebayDevId: data.ebayDevId, ebayUserToken: data.ebayUserToken };
        Object.keys(allowed).forEach(function(k) { if (allowed[k] === undefined) delete allowed[k]; });
        connectMongo(function(err, database) {
          if (err || !database) {
            Object.assign(inMemorySubscribers[code], allowed);
            res.writeHead(200); res.end(JSON.stringify({ success: true }));
            return;
          }
          database.collection('subscribers').updateOne({ code: code }, { $set: allowed }, function(err) {
            res.writeHead(200); res.end(JSON.stringify(err ? { error: err.message } : { success: true }));
          });
        });
      });
    });
    return;
  }

  // ── Send daily report manually ──
  if (pathname === '/admin/send-report' && req.method === 'POST') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    sendDailyReports();
    res.writeHead(200); res.end(JSON.stringify({ success: true, message: 'Reports sending...' }));
    return;
  }

  res.writeHead(404); res.end('{}');
});

// Initialize - connect to MongoDB on startup
console.log('Attempting MongoDB connection...');
console.log('MongoDB URI configured:', MONGODB_URI ? 'YES' : 'NO');
connectMongo(function(err) {
  if (err) {
    console.log('MongoDB connection FAILED:', JSON.stringify(err));
  } else {
    console.log('MongoDB connection SUCCESS - db ready');
  }
  scheduleDailyReports();
});

server.listen(PORT, function() {
  console.log('BFA server running on port ' + PORT);
});
