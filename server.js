var https = require('https');
var http = require('http');
var url = require('url');
var crypto = require('crypto');

var PORT = process.env.PORT || 3000;

// eBay credentials (yours - default)
var CLIENT_ID = process.env.EBAY_CLIENT_ID || 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';
var CLIENT_SECRET = process.env.EBAY_CLIENT_SECRET || 'PRD-6c135696e4a6-8789-475a-8eaf-1662';
var DEV_ID = process.env.EBAY_DEV_ID || '3e7db631-fffe-4cd8-92b6-6bca13515742';
var USER_TOKEN = process.env.EBAY_USER_TOKEN || 'v^1.1#i^1#I^3#f^0#p^3#r^0#t^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';
var ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || '';
var SENDGRID_KEY = process.env.SENDGRID_KEY || '';
var MONGODB_URI = (process.env.MONGODB_URI || 'mongodb+srv://booksforagesbookmobile_db_user:nkBsVNFyqDEUGWQv@booksforages.w8exzl5.mongodb.net/booksforages?retryWrites=true&w=majority&appName=booksforages').replace(/[\r\n]/g,'').trim();
var ADMIN_KEY = (process.env.ADMIN_KEY || 'Booksforages1!').replace(/[\r\n]/g,'').trim();

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
      connectTimeoutMS: 10000
    });
    client.connect().then(function() {
      mongoClient = client;
      db = client.db('booksforages');
      console.log('MongoDB connected successfully');
      cb(null, db);
    }).catch(function(err) {
      console.log('MongoDB connect error:', err.message);
      cb(err.message);
    });
  } catch(e) {
    console.log('MongoDB error:', e.message);
    cb('MongoDB not available: ' + e.message);
  }
}

// In-memory fallback when MongoDB is not available
var inMemorySubscribers = {
  'Booksforages1!': {
    code: 'Booksforages1!',
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
        } else { 
          console.log('Token error response:', data);
          cb('Token error: ' + data); 
        }
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
      // Case-insensitive search in memory
      var found = null;
      var codeLower = code.toLowerCase();
      Object.keys(inMemorySubscribers).forEach(function(k) {
        if (k.toLowerCase() === codeLower) found = inMemorySubscribers[k];
      });
      cb(null, found || null);
      return;
    }
    // Case-insensitive search in MongoDB using regex
    var escapedCode = code.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    database.collection('subscribers').findOne({ code: { $regex: new RegExp('^' + escapedCode + '$', 'i') } })
      .then(function(sub) { cb(null, sub); })
      .catch(function(err) { cb(err); });
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
    database.collection('listings').find({ date: today }).toArray()
      .then(function(listings) {
      if (!listings.length) return;
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
    businessName: data.businessName || '',
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
    database.collection('listings').insertOne(entry)
      .then(function() { if (cb) cb(null, entry); })
      .catch(function(err) { if (cb) cb(err); });
  });
}

var COND_MAP = { '1000': 'NEW', '2750': 'LIKE_NEW', '4000': 'VERY_GOOD', '5000': 'GOOD', '6000': 'ACCEPTABLE' };

function createListing(title, description, price, isbn, conditionId, pictureUrls, language, author, bookTitle, publisher, year, edition, format, signed, signedBy, inscribed, illustrator, topic, features, vintage, sku, userToken, devId, shippingPolicyId, paymentPolicyId, returnPolicyId, cb) {
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
    + '<Location>United States</Location>'
    + '<Currency>USD</Currency>'
    + '<DispatchTimeMax>3</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<BestOfferDetails><BestOfferEnabled>true</BestOfferEnabled></BestOfferDetails>'
    + '<ScheduleTime>' + scheduleTime + '</ScheduleTime>'
    + '<SellerProfiles>'
    + '<SellerShippingProfile><ShippingProfileID>' + shippingPolicyId + '</ShippingProfileID></SellerShippingProfile>'
    + '<SellerReturnProfile><ReturnProfileID>' + returnPolicyId + '</ReturnProfileID></SellerReturnProfile>'
    + '<SellerPaymentProfile><PaymentProfileID>' + paymentPolicyId + '</PaymentProfileID></SellerPaymentProfile>'
    + '</SellerProfiles>'
    + '<ItemSpecifics>'
    + '<NameValueList><Name>Book Title</Name><Value>' + esc(bookTitle || title).substring(0, 65) + '</Value></NameValueList>'
    + '<NameValueList><Name>Author</Name><Value>' + esc(author || 'Unknown').substring(0, 65) + '</Value></NameValueList>'
    + '<NameValueList><Name>Language</Name><Value>' + esc(language || 'English') + '</Value></NameValueList>'
    + (publisher ? '<NameValueList><Name>Publisher</Name><Value>' + esc(publisher).substring(0, 65) + '</Value></NameValueList>' : '')
    + (year ? '<NameValueList><Name>Publication Year</Name><Value>' + esc(year) + '</Value></NameValueList>' : '')
    + (edition ? '<NameValueList><Name>Edition</Name><Value>' + esc(edition).substring(0, 65) + '</Value></NameValueList>' : '')
    + (format ? '<NameValueList><Name>Format</Name><Value>' + esc(format) + '</Value></NameValueList>' : '')
    + (signed ? '<NameValueList><Name>Signed</Name><Value>Yes</Value></NameValueList>' : '')
    + (signedBy ? '<NameValueList><Name>Signed By</Name><Value>' + esc(signedBy).substring(0, 65) + '</Value></NameValueList>' : '')
    + (inscribed ? '<NameValueList><Name>Inscribed</Name><Value>Yes</Value></NameValueList>' : '')
    + (illustrator ? '<NameValueList><Name>Illustrator</Name><Value>' + esc(illustrator).substring(0, 65) + '</Value></NameValueList>' : '')
    + (topic ? '<NameValueList><Name>Topic</Name><Value>' + esc(topic).substring(0, 65) + '</Value></NameValueList>' : '')
    + (vintage ? '<NameValueList><Name>Vintage</Name><Value>Yes</Value></NameValueList>' : '')
    + (sku ? '<NameValueList><Name>Custom SKU</Name><Value>' + esc(sku).substring(0, 65) + '</Value></NameValueList>' : '')
    + (features && features.length > 0 ? (function() { var xml2 = '<NameValueList><Name>Features</Name>'; for (var fi = 0; fi < features.length && fi < 10; fi++) xml2 += '<Value>' + esc(String(features[fi])).substring(0, 65) + '</Value>'; xml2 += '</NameValueList>'; return xml2; })() : '')
    + (isbn && (isbn.replace(/[^0-9]/g, '').substring(0, 3) === '978' || isbn.replace(/[^0-9]/g, '').substring(0, 3) === '979') ? '<NameValueList><Name>ISBN</Name><Value>' + isbn.replace(/[^0-9X]/gi, '') + '</Value></NameValueList>' : '')
    + '</ItemSpecifics>'
    + pictures
    + (isbn && (isbn.replace(/[^0-9]/g, '').substring(0, 3) === '978' || isbn.replace(/[^0-9]/g, '').substring(0, 3) === '979') ? '<ProductListingDetails><ISBN>' + isbn.replace(/[^0-9X]/gi, '') + '</ISBN><IncludeeBayProductDetails>false</IncludeeBayProductDetails><UseStockPhotoURLAsGallery>false</UseStockPhotoURLAsGallery></ProductListingDetails>' : '')
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
      console.log('eBay response:', data.substring(0, 500));
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
      var code = (data.code || '').replace(/[\r\n]/g,'').trim();
      getSubscriber(code, function(err, sub) {
        if (err || !sub) { res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Invalid access code' })); return; }
        if (!sub.active) { res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Your subscription is inactive. Please contact Books for Ages.' })); return; }
        res.writeHead(200); res.end(JSON.stringify({
          valid: true,
          businessName: sub.businessName,
          employees: sub.employees || [],
          reportEmail: sub.email,
          ebayClientId: sub.ebayClientId || '',
          ebayClientSecret: sub.ebayClientSecret || '',
          ebayDevId: sub.ebayDevId || '',
          ebayUserToken: sub.ebayUserToken || '',
          ebayShippingPolicyId: sub.ebayShippingPolicyId || '',
          ebayPaymentPolicyId: sub.ebayPaymentPolicyId || '',
          ebayReturnPolicyId: sub.ebayReturnPolicyId || ''
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
    var cond = parsed.query.condition || '5000';
    var signed = parsed.query.signed === '1';
    var code = (parsed.query.code || 'BFA-ADMIN').toUpperCase();

    // Condition filter: only filter for NEW, otherwise search all conditions
    var conditionId = (cond === '1000') ? 'NEW' : '';

    // 3 keyword combinations to try in order
    var kwOptions = [
      [title, author, year, publisher].filter(Boolean).join(' '),
      [title, author, year].filter(Boolean).join(' '),
      [title, author].filter(Boolean).join(' ')
    ].filter(function(k){ return k.trim().length > 0; });

    if(isbn) kwOptions.unshift(isbn); // if ISBN, try it first

    if(signed) kwOptions = kwOptions.map(function(k){ return k + ' signed'; });

    if (!kwOptions.length) { res.writeHead(400); res.end(JSON.stringify({ error: 'missing search terms' })); return; }

    getSubscriber(code, function(err, sub) {
      var clientId = (sub && sub.ebayClientId) || CLIENT_ID;
      var clientSecret = (sub && sub.ebayClientSecret) || CLIENT_SECRET;

      getToken(clientId, clientSecret, function(err, token) {
        if (err) { 
          console.log('Token error:', err);
          res.writeHead(200); res.end(JSON.stringify({ error: err, average: null, count: 0 })); return; 
        }
        console.log('Got token, searching with options:', kwOptions);
        // Try each keyword combination in order, stop when we find results
        var trySearch = function(idx) {
          if (idx >= kwOptions.length) {
            console.log('All searches exhausted, no results found');
            res.writeHead(200); res.end(JSON.stringify({ average: null, count: 0 }));
            return;
          }
          console.log('Trying search', idx, ':', kwOptions[idx]);
          searchEbay(kwOptions[idx], conditionId, token, function(searchErr, result) {
            console.log('Search', idx, 'result:', JSON.stringify(result), 'err:', searchErr);
            if (!searchErr && result && result.average && result.average > 0) {
              res.writeHead(200); res.end(JSON.stringify(result));
            } else {
              trySearch(idx + 1);
            }
          });
        };
        trySearch(0);
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
        var shippingPolicyId = sub && sub.ebayShippingPolicyId;
        var paymentPolicyId = sub && sub.ebayPaymentPolicyId;
        var returnPolicyId = sub && sub.ebayReturnPolicyId;
        console.log('LIST: code='+code+' shipping='+shippingPolicyId+' payment='+paymentPolicyId+' return='+returnPolicyId);
        if(!shippingPolicyId || !paymentPolicyId || !returnPolicyId){
          res.writeHead(200); res.end(JSON.stringify({ error: 'eBay business policies not configured. Please add your Shipping, Payment, and Return Policy IDs in your Business Portal.' })); return;
        }
        var isbn = data.isbn || '';
        var price = parseFloat(data.price) || 9.99;
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

        createListing(data.title, data.description, price, isbn, conditionId, pictureUrls, language, author, bookTitle, publisher, year, edition, format, signed, signedBy, inscribed, illustrator, topic, features, vintage, sku, userToken, devId, shippingPolicyId, paymentPolicyId, returnPolicyId, function(err, listingId) {
          if (err) { res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
          // Log the listing
          logListing({
            subscriberCode: code,
            businessName: (sub && sub.businessName) || '',
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

  // ── eBay Sales API ──
  if (pathname === '/my/ebay/sales' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var period = parsed.query.period || 'today'; // today, week, month
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var userToken = sub.ebayUserToken || USER_TOKEN;

      // Calculate date range based on period and local timezone
      var now = new Date();
      var localNow = new Date(now.getTime() - offsetMinutes * 60000);
      var startDate;
      if (period === 'today') {
        startDate = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate()));
        startDate = new Date(startDate.getTime() + offsetMinutes * 60000);
      } else if (period === 'week') {
        var dayOfWeek = localNow.getUTCDay();
        var daysFromMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
        var monday = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate() - daysFromMonday));
        startDate = new Date(monday.getTime() + offsetMinutes * 60000);
      } else { // month
        var monthStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), 1));
        startDate = new Date(monthStart.getTime() + offsetMinutes * 60000);
      }

      // Fetch all orders using pagination
      var allOrders = [];
      function fetchOrders(offset) {
        var path = '/sell/fulfillment/v1/order?filter=creationdate:[' + startDate.toISOString() + '..]&limit=50&offset=' + offset;
        var opts = {
          hostname: 'api.ebay.com',
          path: path,
          method: 'GET',
          headers: {
            'Authorization': 'Bearer ' + userToken,
            'Content-Type': 'application/json',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
          }
        };
        var req2 = https.request(opts, function(r) {
          var data = '';
          r.on('data', function(c){ data += c; });
          r.on('end', function(){
            try {
              var json = JSON.parse(data);
              if (json.errors) { res.writeHead(200); res.end(JSON.stringify({ error: json.errors[0].longMessage, orders: [] })); return; }
              var orders = (json.orders || []).filter(function(o){
                return o.orderPaymentStatus === 'PAID' && o.cancelStatus.cancelState === 'NONE_REQUESTED';
              });
              allOrders = allOrders.concat(orders);
              var total = json.total || 0;
              if (allOrders.length < total && json.orders && json.orders.length === 50) {
                fetchOrders(offset + 50);
              } else {
                // Build summary
                var totalRevenue = allOrders.reduce(function(sum, o){
                  return sum + parseFloat(o.pricingSummary.priceSubtotal.value);
                }, 0);
                var simplifiedOrders = allOrders.map(function(o){
                  var item = o.lineItems[0] || {};
                  return {
                    orderId: o.orderId,
                    date: o.creationDate,
                    title: item.title || '',
                    price: parseFloat(o.pricingSummary.priceSubtotal.value),
                    paidToSeller: parseFloat(o.paymentSummary.totalDueSeller.value),
                    status: o.orderFulfillmentStatus,
                    buyer: o.buyer.username
                  };
                });
                res.writeHead(200); res.end(JSON.stringify({
                  count: allOrders.length,
                  totalRevenue: Math.round(totalRevenue * 100) / 100,
                  orders: simplifiedOrders
                }));
              }
            } catch(e) { res.writeHead(200); res.end(JSON.stringify({ error: 'Parse error', orders: [] })); }
          });
        });
        req2.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message, orders: [] })); });
        req2.setTimeout(15000, function(){ req2.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout', orders: [] })); });
        req2.end();
      }
      fetchOrders(0);
    });
    return;
  }

  // ── Test eBay Sales API ──
  if (pathname === '/test-ebay-sales' && req.method === 'GET') {
    var testCode = (parsed.query.code || 'Booksforages1!').replace(/[\r\n]/g,'').trim();
    getSubscriber(testCode, function(err, sub) {
      if (!sub) { res.writeHead(200); res.end(JSON.stringify({ error: 'Subscriber not found' })); return; }
      var userToken = sub.ebayUserToken || USER_TOKEN;
      // Call eBay Orders API for today's orders
      var today = new Date();
      today.setHours(0,0,0,0);
      var opts = {
        hostname: 'api.ebay.com',
        path: '/sell/fulfillment/v1/order?filter=creationdate:[' + today.toISOString() + '..]&limit=10',
        method: 'GET',
        headers: {
          'Authorization': 'Bearer ' + userToken,
          'Content-Type': 'application/json',
          'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
      };
      var req2 = https.request(opts, function(r) {
        var data = '';
        r.on('data', function(c){ data += c; });
        r.on('end', function(){
          try {
            var json = JSON.parse(data);
            res.writeHead(200); res.end(JSON.stringify({ status: r.statusCode, response: json }));
          } catch(e) { res.writeHead(200); res.end(JSON.stringify({ error: 'Parse error', raw: data.substring(0,500) })); }
        });
      });
      req2.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      req2.setTimeout(10000, function(){ req2.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
      req2.end();
    });
    return;
  }

  // ── Test subscriber lookup ──
  if (pathname === '/test-subscriber' && req.method === 'GET') {
    var testCode = parsed.query.code || 'Booksforages1!';
    getSubscriber(testCode, function(err, sub) {
      res.writeHead(200); res.end(JSON.stringify({ found: !!sub, code: testCode, err: err ? err.toString() : null, sub: sub ? { code: sub.code, businessName: sub.businessName, active: sub.active, ebayClientId: sub.ebayClientId, hasToken: !!sub.ebayUserToken, shippingPolicy: sub.ebayShippingPolicyId || 'NOT SET', paymentPolicy: sub.ebayPaymentPolicyId || 'NOT SET', returnPolicy: sub.ebayReturnPolicyId || 'NOT SET' } : null }));
    });
    return;
  }

  // ── Admin login check ──
  if (pathname === '/admin/login' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var key = (data.key || '').replace(/[\r\n\s]/g,'').trim();
      console.log('Login attempt, key length:', key.length, 'expected length:', ADMIN_KEY.length);
      if (key === ADMIN_KEY) {
        res.writeHead(200); res.end(JSON.stringify({ success: true }));
      } else {
        res.writeHead(403); res.end(JSON.stringify({ success: false, error: 'Invalid admin key' }));
      }
    });
    return;
  }

  // ══════════════════════════════════════════
  // ADMIN ENDPOINTS - require admin key
  // ══════════════════════════════════════════

  var adminKey = (req.headers['x-admin-key'] || '').replace(/[\r\n\s]/g,'').trim();
  var isAdmin = adminKey === ADMIN_KEY;

  // ── Get all subscribers (admin only) ──
  if (pathname === '/admin/subscribers' && req.method === 'GET') {
    if (!isAdmin) { res.writeHead(403, {'Content-Type':'application/json'}); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) {
        res.writeHead(200, {'Content-Type':'application/json'}); res.end(JSON.stringify(Object.values(inMemorySubscribers)));
        return;
      }
      database.collection('subscribers').find({}).toArray()
        .then(function(subs) { res.writeHead(200); res.end(JSON.stringify(subs)); })
        .catch(function() { res.writeHead(200); res.end('[]'); });
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
        database.collection('subscribers').insertOne(sub)
          .then(function() { res.writeHead(200); res.end(JSON.stringify(sub)); })
          .catch(function(err) { res.writeHead(200); res.end(JSON.stringify({ error: err.message })); });
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
        database.collection('subscribers').updateOne({ code: code }, { $set: data })
          .then(function() { res.writeHead(200); res.end(JSON.stringify({ success: true })); })
          .catch(function(err) { res.writeHead(200); res.end(JSON.stringify({ error: err.message })); });
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
      database.collection('subscribers').findOne({ code: code })
        .then(function(sub) {
          if (!sub) { res.writeHead(404); res.end('{}'); return; }
          return database.collection('subscribers').updateOne({ code: code }, { $set: { active: !sub.active } })
            .then(function() { res.writeHead(200); res.end(JSON.stringify({ active: !sub.active })); });
        })
        .catch(function(err) { res.writeHead(500); res.end('{}'); });
    });
    return;
  }

  // ── Get listings/activity (admin sees all, subscriber sees own) ──
  if (pathname === '/admin/listings' && req.method === 'GET') {
    if (!isAdmin) { res.writeHead(403); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    var dateFilter = parsed.query.date || new Date().toISOString().split('T')[0];
    var subFilter = parsed.query.code || null;
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    connectMongo(function(err, database) {
      if (err || !database) {
        var filtered = inMemoryListings.filter(function(l) {
          return l.date === dateFilter && (!subFilter || l.subscriberCode === subFilter.toUpperCase());
        });
        res.writeHead(200); res.end(JSON.stringify(filtered));
        return;
      }
      var localMidnight = new Date(dateFilter + 'T00:00:00');
      var utcStart = new Date(localMidnight.getTime() + offsetMinutes * 60000).toISOString();
      var utcEnd = new Date(localMidnight.getTime() + offsetMinutes * 60000 + 86400000).toISOString();
      var query = { createdAt: { $gte: utcStart, $lt: utcEnd } };
      if (subFilter) query.subscriberCode = subFilter.toUpperCase();
      database.collection('listings').find(query).sort({ createdAt: -1 }).toArray()
        .then(function(listings) { res.writeHead(200); res.end(JSON.stringify(listings)); })
        .catch(function() { res.writeHead(200); res.end('[]'); });
    });
    return;
  }

  // ── Subscriber self-service: get own listings ──
  if (pathname === '/my/listings' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var dateFilter = parsed.query.date || new Date().toISOString().split('T')[0];
    var offsetMinutes = parseInt(parsed.query.offset || '0'); // browser timezone offset in minutes
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) {
          var filtered = inMemoryListings.filter(function(l) { return l.subscriberCode === code && l.date === dateFilter; });
          res.writeHead(200); res.end(JSON.stringify(filtered));
          return;
        }
        // Calculate UTC start and end times for the local date
        var localMidnight = new Date(dateFilter + 'T00:00:00');
        var utcStart = new Date(localMidnight.getTime() + offsetMinutes * 60000).toISOString();
        var utcEnd = new Date(localMidnight.getTime() + offsetMinutes * 60000 + 86400000).toISOString();
        database.collection('listings').find({
          subscriberCode: code,
          createdAt: { $gte: utcStart, $lt: utcEnd }
        }).sort({ createdAt: -1 }).toArray()
          .then(function(listings) { res.writeHead(200); res.end(JSON.stringify(listings)); })
          .catch(function() { res.writeHead(200); res.end('[]'); });
      });
    });
    return;
  }

  // ── Subscriber self-service: get monthly listing count ──
  if (pathname === '/my/listings/month' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ count: 0 })); return; }
        // Calculate start and end of current local month in UTC
        var now = new Date();
        var localNow = new Date(now.getTime() - offsetMinutes * 60000);
        var monthStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), 1));
        var monthEnd = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() + 1, 1));
        var utcStart = new Date(monthStart.getTime() + offsetMinutes * 60000).toISOString();
        var utcEnd = new Date(monthEnd.getTime() + offsetMinutes * 60000).toISOString();
        database.collection('listings').countDocuments({
          subscriberCode: code,
          createdAt: { $gte: utcStart, $lt: utcEnd }
        })
          .then(function(count) { res.writeHead(200); res.end(JSON.stringify({ count: count })); })
          .catch(function() { res.writeHead(200); res.end(JSON.stringify({ count: 0 })); });
      });
    });
    return;
  }

  // ── Subscriber: get this week's listings (for leaderboard + revenue) ──
  if (pathname === '/my/listings/week' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ listings: [], weekLabel: 'This Week' })); return; }
        // Find Monday of current week in subscriber's local time
        var now = new Date();
        var localNow = new Date(now.getTime() - offsetMinutes * 60000);
        var dayOfWeek = localNow.getUTCDay(); // 0=Sun, 1=Mon...
        var daysFromMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
        var monday = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate() - daysFromMonday));
        var nextMonday = new Date(monday.getTime() + 7 * 86400000);
        var utcStart = new Date(monday.getTime() + offsetMinutes * 60000).toISOString();
        var utcEnd = new Date(nextMonday.getTime() + offsetMinutes * 60000).toISOString();
        var weekLabel = 'Week of ' + monday.toISOString().split('T')[0] + ' – ' + new Date(nextMonday - 86400000).toISOString().split('T')[0];
        database.collection('listings').find({
          subscriberCode: code,
          createdAt: { $gte: utcStart, $lt: utcEnd }
        }).sort({ createdAt: -1 }).toArray()
          .then(function(listings) { res.writeHead(200); res.end(JSON.stringify({ listings: listings, weekLabel: weekLabel })); })
          .catch(function() { res.writeHead(200); res.end(JSON.stringify({ listings: [], weekLabel: weekLabel })); });
      });
    });
    return;
  }

  // ── Subscriber: get this month's full listings (for revenue) ──
  if (pathname === '/my/listings/month-full' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ listings: [] })); return; }
        var now = new Date();
        var localNow = new Date(now.getTime() - offsetMinutes * 60000);
        var monthStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), 1));
        var monthEnd = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() + 1, 1));
        var utcStart = new Date(monthStart.getTime() + offsetMinutes * 60000).toISOString();
        var utcEnd = new Date(monthEnd.getTime() + offsetMinutes * 60000).toISOString();
        database.collection('listings').find({
          subscriberCode: code,
          createdAt: { $gte: utcStart, $lt: utcEnd }
        }).toArray()
          .then(function(listings) { res.writeHead(200); res.end(JSON.stringify({ listings: listings })); })
          .catch(function() { res.writeHead(200); res.end(JSON.stringify({ listings: [] })); });
      });
    });
    return;
  }

  // ── Subscriber self-service: update own settings ──
  if (pathname === '/my/settings' && req.method === 'PUT') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').replace(/[\r\n]/g,'').trim();
      getSubscriber(code, function(err, sub) {
        if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        // Only allow updating safe fields
        var allowed = { employees: data.employees, email: data.email, businessName: data.businessName, ebayClientId: data.ebayClientId, ebayClientSecret: data.ebayClientSecret, ebayDevId: data.ebayDevId, ebayUserToken: data.ebayUserToken, ebayShippingPolicyId: data.ebayShippingPolicyId, ebayPaymentPolicyId: data.ebayPaymentPolicyId, ebayReturnPolicyId: data.ebayReturnPolicyId };
        Object.keys(allowed).forEach(function(k) { if (allowed[k] === undefined) delete allowed[k]; });
        connectMongo(function(err, database) {
          if (err || !database) {
            Object.assign(inMemorySubscribers[code], allowed);
            res.writeHead(200); res.end(JSON.stringify({ success: true }));
            return;
          }
          database.collection('subscribers').updateOne({ code: code }, { $set: allowed })
            .then(function() { res.writeHead(200); res.end(JSON.stringify({ success: true })); })
            .catch(function(err) { res.writeHead(200); res.end(JSON.stringify({ error: err.message })); });
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
    // Always update token and core fields, but never overwrite policy IDs if already set
    db.collection('subscribers').updateOne(
      { code: 'Booksforages1!' },
      {
        $set: {
          code: 'Booksforages1!',
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
          ebayShippingPolicyId: process.env.EBAY_SHIPPING_POLICY_ID || '193108528015',
          ebayPaymentPolicyId: process.env.EBAY_PAYMENT_POLICY_ID || '226293158015',
          ebayReturnPolicyId: process.env.EBAY_RETURN_POLICY_ID || '129856789015',
          notes: 'Master admin account'
        }
      },
      { upsert: true }
    )
    .then(function() { 
      console.log('Default subscriber upserted successfully');
      // Migrate old listings - backfill businessName where missing
      db.collection('subscribers').find({}).toArray()
        .then(function(subs) {
          subs.forEach(function(sub) {
            db.collection('listings').updateMany(
              { subscriberCode: sub.code.toUpperCase(), businessName: { $in: [null, '', undefined] } },
              { $set: { businessName: sub.businessName || sub.code } }
            )
            .then(function(r) { if(r.modifiedCount > 0) console.log('Backfilled businessName for', r.modifiedCount, 'listings of', sub.businessName); })
            .catch(function(e) { console.log('Backfill error:', e.message); });
          });
        })
        .catch(function(e) { console.log('Migration error:', e.message); });
    })
    .catch(function(err) { console.log('Seed error:', err.message); });
  }
  scheduleDailyReports();
});

server.listen(PORT, function() {
  console.log('BFA server running on port ' + PORT);
});
