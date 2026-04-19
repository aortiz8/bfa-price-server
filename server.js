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
var MONGODB_URI = (process.env.MONGODB_URI || 'mongodb+srv://booksforagesbookmobile_db_user:nkBsVNFyqDEUGWQv@booksforages.w8exzl5.mongodb.net/booksforages?retryWrites=true&w=majority&appName=booksforages').replace(/[\r\n]/g,'').trim();
var ADMIN_KEY = (process.env.ADMIN_KEY || 'Booksforages1!').replace(/[\r\n]/g,'').trim();

// Amazon SP-API credentials
var AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID || '';
var AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET || '';
var AMAZON_REFRESH_TOKEN = process.env.AMAZON_REFRESH_TOKEN || '';
var AMAZON_MARKETPLACE_ID = 'ATVPDKIKX0DER'; // US marketplace
var AMAZON_SELLER_ID = process.env.AMAZON_SELLER_ID || 'ACH3QS6GNTU3L';

// Amazon access token cache
var amazonTokenCache = null;
var amazonTokenExpiry = 0;

function getAmazonAccessToken(cb){
  // Return cached token if still valid
  if(amazonTokenCache && Date.now() < amazonTokenExpiry){
    cb(null, amazonTokenCache); return;
  }
  var body = 'grant_type=refresh_token'
    + '&refresh_token=' + encodeURIComponent(AMAZON_REFRESH_TOKEN)
    + '&client_id=' + encodeURIComponent(AMAZON_CLIENT_ID)
    + '&client_secret=' + encodeURIComponent(AMAZON_CLIENT_SECRET);
  var opts = {
    hostname: 'api.amazon.com', path: '/auth/o2/token', method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) }
  };
  var req = https.request(opts, function(res){
    var data = '';
    res.on('data', function(c){ data += c; });
    res.on('end', function(){
      try {
        var json = JSON.parse(data);
        if(json.access_token){
          amazonTokenCache = json.access_token;
          amazonTokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
          cb(null, json.access_token);
        } else { cb('Amazon token error: ' + data); }
      } catch(e){ cb('Amazon token parse error'); }
    });
  });
  req.on('error', function(e){ cb(e.message); });
  req.setTimeout(10000, function(){ req.destroy(); cb('Timeout'); });
  req.write(body); req.end();
}

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

// ── eBay OAuth Auto-Refresh (every 110 minutes) ──
function refreshAllEbayTokens() {
  connectMongo(function(err, database) {
    if (err || !database) return;
    database.collection('subscribers').find({ ebayRefreshToken: { $exists: true, $ne: '' } }).toArray()
    .then(function(subs) {
      subs.forEach(function(sub) {
        if (!sub.ebayRefreshToken) return;
        var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
        var body = 'grant_type=refresh_token&refresh_token=' + encodeURIComponent(sub.ebayRefreshToken);
        var opts = {
          hostname: 'api.ebay.com', path: '/identity/v1/oauth2/token', method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + credentials,
            'Content-Length': Buffer.byteLength(body)
          }
        };
        var req2 = https.request(opts, function(r) {
          var data = '';
          r.on('data', function(c){ data += c; });
          r.on('end', function(){
            try {
              var json = JSON.parse(data);
              if (json.access_token) {
                database.collection('subscribers').updateOne(
                  { code: sub.code },
                  { $set: { ebayOAuthToken: json.access_token, ebayOAuthExpiry: new Date(Date.now() + (json.expires_in || 7200) * 1000).toISOString() } }
                ).then(function(){ console.log('eBay token refreshed for:', sub.code); })
                .catch(function(e){ console.log('Token save error:', e.message); });
              } else {
                console.log('Token refresh failed for', sub.code, ':', data.substring(0, 200));
              }
            } catch(e){ console.log('Token refresh parse error:', e.message); }
          });
        });
        req2.on('error', function(e){ console.log('Token refresh request error:', e.message); });
        req2.write(body); req2.end();
      });
    })
    .catch(function(e){ console.log('Token refresh DB error:', e.message); });
  });
}

// Run token refresh every 110 minutes (tokens last 2 hours)
setInterval(refreshAllEbayTokens, 110 * 60 * 1000);

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

function cleanDescription(desc){
  // Remove markdown bold **text**
  desc = desc.replace(/\*\*([^*]+)\*\*/g, '$1');
  // Remove markdown italic *text* (but not standalone *)
  desc = desc.replace(/\*([^*\n]+)\*/g, '$1');
  // Convert double line breaks to paragraph breaks
  desc = desc.replace(/\n\n/g, '<br><br>');
  // Convert single line breaks
  desc = desc.replace(/\n/g, '<br>');
  return desc;
}

function createListing(title, description, price, isbn, conditionId, pictureUrls, language, author, bookTitle, publisher, year, edition, format, signed, signedBy, inscribed, illustrator, topic, features, vintage, sku, userToken, devId, shippingPolicyId, paymentPolicyId, returnPolicyId, cb) {
  description = cleanDescription(description);
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
          ebayOAuthToken: sub.ebayOAuthToken || '',
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

  // ── eBay Pick List ──
  if (pathname === '/my/ebay/picklist' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var userToken = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
      // Get snoozed order IDs from MongoDB
      connectMongo(function(err, database) {
        var snoozedIds = [];
        var getSnoozed = database ? database.collection('snoozed_orders').find({ subscriberCode: code }).toArray() : Promise.resolve([]);
        getSnoozed.then(function(snoozed) {
          snoozedIds = (snoozed || []).map(function(s){ return s.orderId; });
          // 30-day date filter
          var thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
          var allOrders = [];
          function fetchOrders(offset) {
            var path = '/sell/fulfillment/v1/order?filter=' + encodeURIComponent('creationdate:[' + thirtyDaysAgo + '..]') + '&limit=50&offset=' + offset;
            var opts = {
              hostname: 'api.ebay.com', path: path, method: 'GET',
              headers: { 'Authorization': 'Bearer ' + userToken, 'Content-Type': 'application/json', 'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US' }
            };
            var req2 = https.request(opts, function(r) {
              var data = '';
              r.on('data', function(c){ data += c; });
              r.on('end', function(){
                try {
                  var json = JSON.parse(data);
                  if (json.errors) { res.writeHead(200); res.end(JSON.stringify({ error: json.errors[0].longMessage, orders: [] })); return; }
                  var orders = json.orders || [];
                  allOrders = allOrders.concat(orders);
                  var total = json.total || 0;
                  if (allOrders.length < total && orders.length === 50) {
                    fetchOrders(offset + 50);
                  } else {
                    var pending = [];
                    var canceled = [];
                    var actionNeeded = [];
                    var now = new Date();
                    allOrders.forEach(function(o) {
                      if (snoozedIds.indexOf(o.orderId) > -1) return; // skip snoozed
                      var cancelState = o.cancelStatus ? o.cancelStatus.cancelState : 'NONE_REQUESTED';
                      if (cancelState === 'CANCELED') {
                        canceled.push(o);
                      } else if (cancelState === 'CANCEL_REQUESTED') {
                        actionNeeded.push(o);
                      } else if (o.orderFulfillmentStatus === 'NOT_STARTED' && o.orderPaymentStatus === 'PAID') {
                        pending.push(o);
                      }
                    });
                    function simplify(o) {
                      var item = o.lineItems[0] || {};
                      var created = new Date(o.creationDate);
                      var ageHours = Math.round((now - created) / 3600000 * 10) / 10;
                      var skuRaw = item.sku || '';
                      var skuPrefix = skuRaw.split(/[-\.]/)[0] || skuRaw;
                      return {
                        orderId: o.orderId,
                        orderDate: o.creationDate,
                        ageHours: ageHours,
                        platform: 'eBay',
                        title: item.title || '',
                        sku: skuRaw,
                        skuPrefix: skuPrefix,
                        price: parseFloat(o.pricingSummary.priceSubtotal.value),
                        condition: item.condition || '',
                        cancelState: o.cancelStatus ? o.cancelStatus.cancelState : 'NONE_REQUESTED'
                      };
                    }
                    pending = pending.map(simplify).sort(function(a, b) {
                      var aIsAlpha = /^[A-Za-z]/.test(a.skuPrefix);
                      var bIsAlpha = /^[A-Za-z]/.test(b.skuPrefix);
                      if (aIsAlpha && !bIsAlpha) return -1;
                      if (!aIsAlpha && bIsAlpha) return 1;
                      return a.sku.localeCompare(b.sku);
                    });
                    res.writeHead(200); res.end(JSON.stringify({
                      pending: pending,
                      canceled: canceled.map(simplify),
                      actionNeeded: actionNeeded.map(simplify)
                    }));
                  }
                } catch(e) { res.writeHead(200); res.end(JSON.stringify({ error: 'Parse error', orders: [] })); }
              });
            });
            req2.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message, orders: [] })); });
            req2.setTimeout(20000, function(){ req2.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout', orders: [] })); });
            req2.end();
          }
          fetchOrders(0);
        }).catch(function(){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error', orders: [] })); });
      });
    });
    return;
  }

  // ── Snooze an order ──
  if (pathname === '/my/ebay/snooze' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      var orderId = data.orderId || '';
      if (!code || !orderId) { res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code or orderId' })); return; }
      getSubscriber(code, function(err, sub) {
        if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        connectMongo(function(err, database) {
          if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
          database.collection('snoozed_orders').updateOne(
            { subscriberCode: code, orderId: orderId },
            { $set: { subscriberCode: code, orderId: orderId, snoozedAt: new Date().toISOString() } },
            { upsert: true }
          )
          .then(function() { res.writeHead(200); res.end(JSON.stringify({ success: true })); })
          .catch(function(e) { res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
        });
      });
    });
    return;
  }

  // ── eBay Sales API ──
  if (pathname === '/my/ebay/sales' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var period = parsed.query.period || 'today'; // today, week, month, date
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    var specificDate = parsed.query.date || null; // YYYY-MM-DD for specific date
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var userToken = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;

      // Calculate date range
      var now = new Date();
      var localNow = new Date(now.getTime() - offsetMinutes * 60000);
      var startDate, endDate;

      if (specificDate) {
        // Specific date range
        var d = new Date(specificDate + 'T00:00:00');
        startDate = new Date(d.getTime() + offsetMinutes * 60000);
        endDate = new Date(d.getTime() + offsetMinutes * 60000 + 86400000);
      } else if (period === 'today') {
        startDate = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate()));
        startDate = new Date(startDate.getTime() + offsetMinutes * 60000);
        endDate = null;
      } else if (period === 'week') {
        var dayOfWeek = localNow.getUTCDay();
        var daysFromMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
        var monday = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate() - daysFromMonday));
        startDate = new Date(monday.getTime() + offsetMinutes * 60000);
        endDate = null;
      } else { // month
        var monthStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), 1));
        startDate = new Date(monthStart.getTime() + offsetMinutes * 60000);
        endDate = null;
      }

      // Build filter
      var dateFilter = 'creationdate:[' + startDate.toISOString() + '..' + (endDate ? endDate.toISOString() : '') + ']';

      // Fetch all orders using pagination
      var allOrders = [];
      function fetchOrders(offset) {
        var path = '/sell/fulfillment/v1/order?filter=' + encodeURIComponent(dateFilter) + '&limit=50&offset=' + offset;
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

  // ── eBay OAuth: Start flow ──
  if (pathname === '/ebay/auth' && req.method === 'GET') {
    var code = (parsed.query.code || '').replace(/[\r\n]/g,'').trim();
    if (!code) { res.writeHead(400); res.end('Missing subscriber code'); return; }
    var RUNAME = 'Codex_Brothers_-CodexBro-Booksf-ixdtwam';
    var scopes = [
      'https://api.ebay.com/oauth/api_scope',
      'https://api.ebay.com/oauth/api_scope/sell.fulfillment.readonly',
      'https://api.ebay.com/oauth/api_scope/sell.finances',
      'https://api.ebay.com/oauth/api_scope/sell.inventory.readonly',
      'https://api.ebay.com/oauth/api_scope/sell.account.readonly'
    ].join(' ');
    var authUrl = 'https://auth.ebay.com/oauth2/authorize?client_id=' + CLIENT_ID
      + '&response_type=code'
      + '&redirect_uri=' + encodeURIComponent(RUNAME)
      + '&scope=' + encodeURIComponent(scopes)
      + '&state=' + encodeURIComponent(code);
    console.log('eBay OAuth start for code:', code);
    console.log('Redirect URL:', authUrl);
    res.writeHead(302, { 'Location': authUrl });
    res.end();
    return;
  }

  // ── eBay OAuth: Callback (both /callback and /ebay/callback) ──
  if ((pathname === '/ebay/callback' || pathname === '/callback') && req.method === 'GET') {
    var authCode = parsed.query.code || '';
    var subscriberCode = (parsed.query.state || '').toUpperCase();
    console.log('eBay callback received. authCode length:', authCode.length, 'subscriberCode:', subscriberCode);
    if (!authCode) {
      res.writeHead(200); res.end('<html><body><h2>❌ eBay connection failed. Please try again.</h2></body></html>');
      return;
    }
    var RUNAME = 'Codex_Brothers_-CodexBro-Booksf-ixdtwam';
    var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
    var body = 'grant_type=authorization_code&code=' + encodeURIComponent(authCode) + '&redirect_uri=' + encodeURIComponent(RUNAME);
    var opts = {
      hostname: 'api.ebay.com', path: '/identity/v1/oauth2/token', method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic ' + credentials,
        'Content-Length': Buffer.byteLength(body)
      }
    };
    var req2 = https.request(opts, function(r) {
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        try {
          var json = JSON.parse(data);
          if (!json.access_token) {
            console.log('eBay token exchange failed:', data);
            res.writeHead(200); res.end('<html><body><h2>❌ Failed to get token: ' + data + '</h2></body></html>');
            return;
          }
          var oauthToken = json.access_token;
          var refreshToken = json.refresh_token || '';
          var expiresIn = json.expires_in || 7200;
          // Save to MongoDB
          connectMongo(function(err, database) {
            if (err || !database) {
              res.writeHead(200); res.end('<html><body><h2>❌ Database error. Please try again.</h2></body></html>');
              return;
            }
            var escapedCode = subscriberCode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            database.collection('subscribers').updateOne(
              { code: { $regex: new RegExp('^' + escapedCode + '$', 'i') } },
              { $set: { ebayOAuthToken: oauthToken, ebayRefreshToken: refreshToken, ebayOAuthExpiry: new Date(Date.now() + expiresIn * 1000).toISOString() } }
            )
            .then(function() {
              res.writeHead(200); res.end('<html><head><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;background:#0a0c10;color:#fff;text-align:center;} h2{color:#00c97a;} p{color:#aaa;}</style></head><body><h2>✅ eBay Account Connected!</h2><p>Your eBay sales data is now linked. You can close this window and return to your portal.</p></body></html>');
            })
            .catch(function(e) {
              res.writeHead(200); res.end('<html><body><h2>❌ Save error: ' + e.message + '</h2></body></html>');
            });
          });
        } catch(e) {
          res.writeHead(200); res.end('<html><body><h2>❌ Parse error</h2></body></html>');
        }
      });
    });
    req2.on('error', function(e){ res.writeHead(200); res.end('<html><body><h2>❌ ' + e.message + '</h2></body></html>'); });
    req2.write(body); req2.end();
    return;
  }

  // ── Debug: Check employees ──
  if (pathname === '/tc/employees' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var emps = (sub.employees || []).map(function(e){
        return { name: e.name, pin: e.pin, hourlyRate: e.hourlyRate, currency: e.currency, payPeriod: e.payPeriod, country: e.country };
      });
      res.writeHead(200); res.end(JSON.stringify({ count: emps.length, employees: emps }));
    });
    return;
  }

  // ── Timeclock: Clear punches ──
  if (pathname === '/tc/clear' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var name = parsed.query.name || '';
    var date = parsed.query.date || '';
    if (!code) { res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
      var query = { subscriberCode: code };
      if (name) query.employeeName = name;
      if (date) query.localDate = date;
      database.collection('timeclock').deleteMany(query)
      .then(function(r){ res.writeHead(200); res.end(JSON.stringify({ deleted: r.deletedCount })); })
      .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
    });
    return;
  }

  // ── Timeclock: Debug punches ──
  if (pathname === '/tc/debug' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var name = parsed.query.name || '';
    connectMongo(function(err, database) {
      if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
      database.collection('timeclock').find({ subscriberCode: code, employeeName: name }).sort({ createdAt: -1 }).limit(20).toArray()
      .then(function(punches){ res.writeHead(200); res.end(JSON.stringify({ count: punches.length, punches: punches })); })
      .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
    });
    return;
  }

  // ── Timeclock: Get QR token ──
  if (pathname === '/tc/qr' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      // Generate a token based on current 60-second window
      var window = Math.floor(Date.now() / 60000);
      var token = crypto.createHmac('sha256', ADMIN_KEY).update(code + ':' + window).digest('hex').substring(0, 12);
      var expiresIn = 60 - (Date.now() % 60000) / 1000;
      var checkinUrl = 'https://heartfelt-pony-7a5b16.netlify.app/checkin.html?token=' + token + '&code=' + code;
      res.writeHead(200); res.end(JSON.stringify({ token: token, checkinUrl: checkinUrl, expiresIn: Math.round(expiresIn) }));
    });
    return;
  }

  // ── Timeclock: Check in/out ──
  if (pathname === '/tc/checkin' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      var token = data.token || '';
      var pin = data.pin || '';
      var offsetMinutes = parseInt(data.offset || '0');
      getSubscriber(code, function(err, sub) {
        if (!sub) { res.writeHead(200); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        // Validate token - check current and previous window (allow 60s grace)
        var now = Date.now();
        var window1 = Math.floor(now / 60000);
        var window2 = window1 - 1;
        var window3 = window1 - 2;
        var validToken1 = crypto.createHmac('sha256', ADMIN_KEY).update(code + ':' + window1).digest('hex').substring(0, 12);
        var validToken2 = crypto.createHmac('sha256', ADMIN_KEY).update(code + ':' + window2).digest('hex').substring(0, 12);
        var validToken3 = crypto.createHmac('sha256', ADMIN_KEY).update(code + ':' + window3).digest('hex').substring(0, 12);
        if (token !== validToken1 && token !== validToken2 && token !== validToken3) {
          res.writeHead(200); res.end(JSON.stringify({ error: 'QR code expired. Please scan the latest QR code.' })); return;
        }
        // Find employee by PIN
        var employees = sub.employees || [];
        var emp = employees.find(function(e){ return e.pin === pin; });
        if (!emp) { res.writeHead(200); res.end(JSON.stringify({ error: 'Invalid PIN. Please try again.' })); return; }
        // Log the punch
        connectMongo(function(err, database) {
          if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'Database error' })); return; }
          var nowDate = new Date();
          var localNow = new Date(nowDate.getTime() - offsetMinutes * 60000);
          var localDate = localNow.getUTCFullYear() + '-' + String(localNow.getUTCMonth()+1).padStart(2,'0') + '-' + String(localNow.getUTCDate()).padStart(2,'0');
          var localTime = localNow.toISOString().substring(11, 19);
          // Find last punch today to determine in/out
          database.collection('timeclock').find({
            subscriberCode: code,
            employeeName: emp.name,
            localDate: localDate
          }).sort({ createdAt: -1 }).limit(1).toArray()
          .then(function(punches) {
            var lastPunch = punches[0];
            var type = (!lastPunch || lastPunch.type === 'out') ? 'in' : 'out';
            var entry = {
              subscriberCode: code,
              employeeName: emp.name,
              employeePin: pin,
              type: type,
              localDate: localDate,
              localTime: localTime,
              createdAt: nowDate.toISOString(),
              offsetMinutes: offsetMinutes
            };
            return database.collection('timeclock').insertOne(entry).then(function() {
              // Calculate hours today
              return database.collection('timeclock').find({
                subscriberCode: code, employeeName: emp.name, localDate: localDate
              }).sort({ createdAt: 1 }).toArray();
            });
          })
          .then(function(todayPunches) {
            var type = todayPunches[todayPunches.length - 1].type;
            // Calculate hours: first in to last out (or now if still in)
            var firstIn = todayPunches.find(function(p){ return p.type === 'in'; });
            var lastOut = null;
            for (var i = todayPunches.length - 1; i >= 0; i--) {
              if (todayPunches[i].type === 'out') { lastOut = todayPunches[i]; break; }
            }
            var hoursToday = 0;
            if (firstIn) {
              var endTime = lastOut ? new Date(lastOut.createdAt) : new Date();
              hoursToday = Math.round((endTime - new Date(firstIn.createdAt)) / 3600000 * 10) / 10;
            }
            res.writeHead(200); res.end(JSON.stringify({
              success: true,
              type: type,
              employeeName: emp.name,
              localTime: localTime,
              hoursToday: hoursToday,
              allPunches: todayPunches.map(function(p){ return { type: p.type, time: p.localTime }; })
            }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
        });
      });
    });
    return;
  }

  // ── Timeclock: Employee hours viewer (for tablet) ──
  if (pathname === '/tc/my-hours' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var pin = parsed.query.pin || '';
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var emp = (sub.employees || []).find(function(e){ return e.pin === pin; });
      if (!emp) { res.writeHead(200); res.end(JSON.stringify({ error: 'Invalid PIN' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        // Get this week's punches
        var now = new Date();
        var localNow = new Date(now.getTime() - offsetMinutes * 60000);
        var dayOfWeek = localNow.getUTCDay();
        var daysFromMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
        var monday = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate() - daysFromMonday));
        var mondayStr = monday.getUTCFullYear() + '-' + String(monday.getUTCMonth()+1).padStart(2,'0') + '-' + String(monday.getUTCDate()).padStart(2,'0');
        database.collection('timeclock').find({
          subscriberCode: code, employeeName: emp.name, localDate: { $gte: mondayStr }
        }).sort({ createdAt: 1 }).toArray()
        .then(function(punches) {
          // Group by date
          var byDate = {};
          punches.forEach(function(p) {
            if (!byDate[p.localDate]) byDate[p.localDate] = [];
            byDate[p.localDate].push(p);
          });
          var days = [];
          var totalHours = 0;
          Object.keys(byDate).sort().forEach(function(date) {
            var dayPunches = byDate[date];
            var firstIn = dayPunches.find(function(p){ return p.type === 'in'; });
            var lastOut = null;
            for (var i = dayPunches.length-1; i >= 0; i--) { if(dayPunches[i].type === 'out'){ lastOut = dayPunches[i]; break; } }
            var hrs = 0;
            if (firstIn) {
              var end = lastOut ? new Date(lastOut.createdAt) : now;
              hrs = Math.round((end - new Date(firstIn.createdAt)) / 3600000 * 10) / 10;
            }
            totalHours += hrs;
            days.push({ date: date, hours: hrs, inProgress: !lastOut && !!firstIn });
          });
          // Check current status
          var todayStr = localNow.getUTCFullYear() + '-' + String(localNow.getUTCMonth()+1).padStart(2,'0') + '-' + String(localNow.getUTCDate()).padStart(2,'0');
          var todayPunches = byDate[todayStr] || [];
          var lastPunch = todayPunches[todayPunches.length - 1];
          var currentStatus = lastPunch ? lastPunch.type : 'out';
          res.writeHead(200); res.end(JSON.stringify({
            name: emp.name,
            currentStatus: currentStatus,
            totalHoursThisWeek: Math.round(totalHours * 10) / 10,
            days: days
          }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Timeclock: Attendance for portal activity page ──
  if (pathname === '/tc/attendance' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var employees = sub.employees || [];
      var now = new Date();
      var localNow = new Date(now.getTime() - offsetMinutes * 60000);
      var todayStr = localNow.getUTCFullYear() + '-' + String(localNow.getUTCMonth()+1).padStart(2,'0') + '-' + String(localNow.getUTCDate()).padStart(2,'0');
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ attendance: [] })); return; }
        database.collection('timeclock').find({
          subscriberCode: code, localDate: todayStr
        }).sort({ createdAt: 1 }).toArray()
        .then(function(punches) {
          var result = employees.map(function(emp) {
            var empPunches = punches.filter(function(p){ return p.employeeName === emp.name; });
            var firstIn = empPunches.find(function(p){ return p.type === 'in'; });
            var lastPunch = empPunches[empPunches.length - 1];
            var currentStatus = lastPunch ? lastPunch.type : 'absent';
            var hoursToday = 0;
            if (firstIn) {
              var lastOut = null;
              for (var i = empPunches.length-1; i >= 0; i--) { if(empPunches[i].type==='out'){lastOut=empPunches[i];break;} }
              var end = lastOut ? new Date(lastOut.createdAt) : now;
              hoursToday = Math.round((end - new Date(firstIn.createdAt)) / 3600000 * 10) / 10;
            }
            return {
              name: emp.name,
              status: currentStatus,
              firstIn: firstIn ? firstIn.localTime : null,
              lastTime: lastPunch ? lastPunch.localTime : null,
              hoursToday: hoursToday,
              allPunches: empPunches.map(function(p){ return { type: p.type, time: p.localTime }; })
            };
          });
          res.writeHead(200); res.end(JSON.stringify({ attendance: result, date: todayStr }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Timeclock: Weekly history (for portal + tablet) ──
  if (pathname === '/tc/weekly-history' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var pin = parsed.query.pin || ''; // optional - if provided, only return that employee
    var weekStart = parsed.query.weekStart || '';
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var employees = sub.employees || [];
      // If PIN provided, filter to that employee only
      if (pin) {
        var emp = employees.find(function(e){ return e.pin === pin; });
        if (!emp) { res.writeHead(200); res.end(JSON.stringify({ error: 'Invalid PIN' })); return; }
        employees = [emp];
      }
      // Calculate week Monday
      var now = new Date();
      var localNow = new Date(now.getTime() - offsetMinutes * 60000);
      var monday;
      if (weekStart) {
        monday = new Date(weekStart + 'T00:00:00Z');
      } else {
        var dayOfWeek = localNow.getUTCDay();
        var daysFromMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
        monday = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate() - daysFromMonday));
      }
      var sunday = new Date(monday.getTime() + 6 * 86400000);
      var mondayStr = monday.toISOString().split('T')[0];
      var sundayStr = sunday.toISOString().split('T')[0];
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        database.collection('timeclock').find({
          subscriberCode: code,
          localDate: { $gte: mondayStr, $lte: sundayStr }
        }).sort({ createdAt: 1 }).toArray()
        .then(function(punches) {
          var punchLabels = ['Checked In', 'Started Lunch', 'Ended Lunch', 'Checked Out', 'Checked In Again', 'Checked Out'];
          var result = employees.map(function(emp) {
            var empPunches = punches.filter(function(p){ return p.employeeName === emp.name; });
            var days = [];
            var totalHours = 0;
            for (var d = 0; d <= 6; d++) {
              var dayDate = new Date(monday.getTime() + d * 86400000);
              var dateStr = dayDate.toISOString().split('T')[0];
              var dayPunches = empPunches.filter(function(p){ return p.localDate === dateStr; });
              var firstIn = dayPunches.find(function(p){ return p.type === 'in'; });
              var lastOut = null;
              for (var i = dayPunches.length-1; i >= 0; i--) { if(dayPunches[i].type==='out'){lastOut=dayPunches[i];break;} }
              var hrs = 0;
              if (firstIn) {
                var end = lastOut ? new Date(lastOut.createdAt) : (dateStr === localNow.toISOString().split('T')[0] ? now : new Date(lastOut ? lastOut.createdAt : firstIn.createdAt));
                if (lastOut) hrs = Math.round((new Date(lastOut.createdAt) - new Date(firstIn.createdAt)) / 3600000 * 10) / 10;
              }
              totalHours += hrs;
              days.push({
                date: dateStr,
                dayName: ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dayDate.getUTCDay()],
                hours: hrs,
                absent: dayPunches.length === 0,
                inProgress: !!firstIn && !lastOut,
                punches: dayPunches.map(function(p, i){ return { type: p.type, time: p.localTime, label: punchLabels[i] || (p.type === 'in' ? 'Checked In' : 'Checked Out') }; })
              });
            }
            return {
              name: emp.name,
              hourlyRate: emp.hourlyRate || 0,
              currency: emp.currency || 'USD',
              payPeriod: emp.payPeriod || 'biweekly',
              country: emp.country || 'US',
              totalHours: Math.round(totalHours * 10) / 10,
              weekStart: mondayStr,
              weekEnd: sundayStr,
              days: days
            };
          });
          res.writeHead(200); res.end(JSON.stringify({ employees: result, weekStart: mondayStr, weekEnd: sundayStr }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Timeclock: Pay period summary ──
  if (pathname === '/tc/pay-period' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    var US_PERIOD_ANCHOR = new Date('2026-04-13T00:00:00Z'); // Apr 13 2026
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var employees = sub.employees || [];
      var now = new Date();
      var localNow = new Date(now.getTime() - offsetMinutes * 60000);
      // Calculate current week Mon-Sun
      var dayOfWeek = localNow.getUTCDay();
      var daysFromMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
      var monday = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), localNow.getUTCDate() - daysFromMonday));
      var sunday = new Date(monday.getTime() + 6 * 86400000);
      // Calculate US biweekly period
      var msSinceAnchor = monday.getTime() - US_PERIOD_ANCHOR.getTime();
      var weeksSinceAnchor = Math.floor(msSinceAnchor / (7 * 86400000));
      var periodWeek = Math.floor(weeksSinceAnchor / 2) * 2;
      var usPeriodStart = new Date(US_PERIOD_ANCHOR.getTime() + periodWeek * 7 * 86400000);
      var usPeriodEnd = new Date(usPeriodStart.getTime() + 13 * 86400000);
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        var mondayStr = monday.toISOString().split('T')[0];
        var sundayStr = sunday.toISOString().split('T')[0];
        var usPeriodStartStr = usPeriodStart.toISOString().split('T')[0];
        var usPeriodEndStr = usPeriodEnd.toISOString().split('T')[0];
        // Fetch all punches for both periods
        var earliestDate = usPeriodStartStr < mondayStr ? usPeriodStartStr : mondayStr;
        var latestDate = usPeriodEndStr > sundayStr ? usPeriodEndStr : sundayStr;
        database.collection('timeclock').find({
          subscriberCode: code,
          localDate: { $gte: earliestDate, $lte: latestDate }
        }).sort({ createdAt: 1 }).toArray()
        .then(function(punches) {
          var punchLabels = ['Checked In', 'Started Lunch', 'Ended Lunch', 'Checked Out', 'Checked In Again', 'Checked Out'];
          var result = employees.map(function(emp) {
            var isMX = emp.country === 'MX' || emp.currency === 'MXN';
            var periodStart = isMX ? mondayStr : usPeriodStartStr;
            var periodEnd = isMX ? sundayStr : usPeriodEndStr;
            var empPunches = punches.filter(function(p){ return p.employeeName === emp.name && p.localDate >= periodStart && p.localDate <= periodEnd; });
            // Build days
            var startDate = new Date(periodStart + 'T00:00:00Z');
            var endDate = new Date(periodEnd + 'T00:00:00Z');
            var days = [];
            var totalHours = 0;
            for (var d = startDate; d <= endDate; d = new Date(d.getTime() + 86400000)) {
              var dateStr = d.toISOString().split('T')[0];
              var dayPunches = empPunches.filter(function(p){ return p.localDate === dateStr; });
              var firstIn = dayPunches.find(function(p){ return p.type === 'in'; });
              var lastOut = null;
              for (var i = dayPunches.length-1; i >= 0; i--) { if(dayPunches[i].type==='out'){lastOut=dayPunches[i];break;} }
              var hrs = 0;
              if (firstIn && lastOut) hrs = Math.round((new Date(lastOut.createdAt) - new Date(firstIn.createdAt)) / 3600000 * 10) / 10;
              else if (firstIn && !lastOut && dateStr === localNow.toISOString().split('T')[0]) hrs = Math.round((now - new Date(firstIn.createdAt)) / 3600000 * 10) / 10;
              totalHours += hrs;
              var dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
              days.push({
                date: dateStr,
                dayName: dayNames[d.getUTCDay()],
                hours: hrs,
                absent: dayPunches.length === 0,
                punches: dayPunches.map(function(p, i){ return { type: p.type, time: p.localTime, label: punchLabels[i] || (p.type==='in'?'Checked In':'Checked Out') }; })
              });
            }
            var rate = parseFloat(emp.hourlyRate || 0);
            var payOwed = Math.round(totalHours * rate * 100) / 100;
            return {
              name: emp.name,
              hourlyRate: rate,
              currency: emp.currency || 'USD',
              payPeriod: isMX ? 'weekly' : 'biweekly',
              country: emp.country || 'US',
              periodStart: periodStart,
              periodEnd: periodEnd,
              totalHours: Math.round(totalHours * 10) / 10,
              payOwed: payOwed,
              days: days
            };
          });
          res.writeHead(200); res.end(JSON.stringify({ employees: result }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Warehouse: Delete item ──
  if (pathname === '/warehouse/delete-item' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      if(!code){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
      getSubscriber(code, function(err, sub){
        if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        // End eBay listing if live
        if(data.ebayItemId){
          var userToken = sub.ebayUserToken || USER_TOKEN;
          var devId = sub.ebayDevId || DEV_ID;
          var endXml = '<?xml version="1.0" encoding="utf-8"?>'
            + '<EndItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
            + '<RequesterCredentials><eBayAuthToken>' + userToken + '</eBayAuthToken></RequesterCredentials>'
            + '<ItemID>' + data.ebayItemId + '</ItemID>'
            + '<EndingReason>NotAvailable</EndingReason>'
            + '</EndItemRequest>';
          var endOpts = {
            hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
            headers: {
              'X-EBAY-API-SITEID': '0', 'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
              'X-EBAY-API-CALL-NAME': 'EndItem', 'X-EBAY-API-DEV-NAME': devId,
              'X-EBAY-API-APP-NAME': (sub.ebayClientId || CLIENT_ID),
              'X-EBAY-API-CERT-NAME': (sub.ebayClientSecret || CLIENT_SECRET),
              'Content-Type': 'text/xml', 'Content-Length': Buffer.byteLength(endXml)
            }
          };
          var endReq = https.request(endOpts, function(endRes){
            var d=''; endRes.on('data',function(c){d+=c;}); endRes.on('end',function(){ console.log('eBay end item:', d.substring(0,200)); });
          });
          endReq.on('error',function(){});
          endReq.write(endXml); endReq.end();
        }
        // Mark as deleted in MongoDB and retire sequence
        connectMongo(function(err, database){
          if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
          var query = data.itemId ? { _id: new (require('mongodb').ObjectId)(data.itemId) } : { code: code, sku: data.sku };
          database.collection('warehouse_inventory').updateOne(query, {
            $set: { status: 'deleted', deletedAt: new Date(), sequenceRetired: true }
          })
          .then(function(){ res.writeHead(200); res.end(JSON.stringify({ success: true })); })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
        });
      });
    });
    return;
  }

  // ── Warehouse: Lookup book from Amazon catalog by ISBN ──
  if (pathname === '/warehouse/book-lookup' && req.method === 'GET') {
    var isbn = (parsed.query.isbn || '').replace(/[^0-9X]/gi,'');
    var code = (parsed.query.code || '').toUpperCase();
    if(!isbn){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing ISBN' })); return; }
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: 'Amazon auth failed: ' + err })); return; }
      // ISBN-13 uses EAN identifier type, ISBN-10 uses ISBN
      var identifierType = isbn.length === 13 ? 'EAN' : 'ISBN';
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: '/catalog/2022-04-01/items?identifiers=' + isbn + '&identifiersType=' + identifierType + '&marketplaceIds=' + AMAZON_MARKETPLACE_ID + '&includedData=attributes,images,summaries,dimensions',
        method: 'GET',
        headers: {
          'x-amz-access-token': accessToken,
          'Accept': 'application/json'
        }
      };
      var amzReq = https.request(opts, function(amzRes){
        var data = '';
        amzRes.on('data', function(c){ data += c; });
        amzRes.on('end', function(){
          console.log('Amazon catalog FULL response:', amzRes.statusCode, data);
          try {
            var json = JSON.parse(data);
            var items = json.items || [];
            if(!items.length){ res.writeHead(200); res.end(JSON.stringify({ error: 'Book not found in Amazon catalog', rawResponse: json })); return; }
            var item = items[0];
            var attrs = item.attributes || {};
            var summaries = (item.summaries && item.summaries[0]) || {};
            var images = item.images || [];
            var dims = item.dimensions || [];

            // Extract all useful fields
            var title = (attrs.item_name && attrs.item_name[0] && attrs.item_name[0].value) || summaries.itemName || '';
            var author = '';
            if(attrs.contributor){ attrs.contributor.forEach(function(c){ if(!author && c.value) author = c.value; }); }
            if(!author && attrs.author) author = (attrs.author[0] && attrs.author[0].value) || '';
            if(!author && attrs.brand) author = (attrs.brand[0] && attrs.brand[0].value) || '';
            if(!author && summaries.author) author = summaries.author;
            if(!author) author = 'Unknown';
            // Flip "Last, First" to "First Last"
            if(author.indexOf(',') > -1){ var ap = author.split(','); author = (ap[1]||'').trim() + ' ' + (ap[0]||'').trim(); }
            author = author.trim();

            var publisher = (attrs.publisher && attrs.publisher[0] && attrs.publisher[0].value) || (summaries.manufacturer) || '';
            var pubDate = (attrs.publication_date && attrs.publication_date[0] && attrs.publication_date[0].value) || '';
            var year = pubDate ? pubDate.substring(0,4) : '';
            var language = (attrs.language && attrs.language[0] && attrs.language[0].value) || 'English';
            // Capitalize language: "english" → "English"
            language = language.charAt(0).toUpperCase() + language.slice(1).toLowerCase();
            var pages = (attrs.number_of_pages && attrs.number_of_pages[0] && attrs.number_of_pages[0].value) || '';
            var edition = (attrs.edition && attrs.edition[0] && attrs.edition[0].value) || '';
            var format = (attrs.binding && attrs.binding[0] && attrs.binding[0].value) || 'Paperback';
            // Capitalize format: "hardcover" → "Hardcover"
            format = format.charAt(0).toUpperCase() + format.slice(1).toLowerCase();
            var description = (attrs.product_description && attrs.product_description[0] && attrs.product_description[0].value) || '';
            var series = (attrs.series && attrs.series[0] && attrs.series[0].value) || '';
            var grade = (attrs.grade && attrs.grade[0] && attrs.grade[0].value) || '';
            var asin = item.asin || '';

            // Get best cover image
            var coverUrl = '';
            var bestSize = 0;
            images.forEach(function(imgGroup){
              var imgs = imgGroup.images || [];
              imgs.forEach(function(img){
                if(img.variant === 'MAIN'){
                  var h = img.height || 0;
                  if(h > bestSize){ bestSize = h; coverUrl = img.link || ''; }
                }
              });
            });

            // Get book dimensions (width for shelf space)
            var widthInches = null;
            dims.forEach(function(dimGroup){
              var d = dimGroup.dimensions || {};
              if(d.width){ widthInches = parseFloat(d.width.value || 0); }
            });

            // Get list price
            var listPrice = null;
            if(attrs.list_price && attrs.list_price[0]) listPrice = parseFloat(attrs.list_price[0].value || 0);

            res.writeHead(200); res.end(JSON.stringify({
              asin: asin,
              isbn: isbn,
              title: title,
              author: author,
              publisher: publisher,
              year: year,
              language: language,
              pages: pages,
              edition: edition,
              format: format,
              description: description,
              series: series,
              coverUrl: coverUrl,
              widthInches: widthInches,
              listPrice: listPrice
            }));
          } catch(e){ res.writeHead(200); res.end(JSON.stringify({ error: 'Parse error: ' + e.message })); }
        });
      });
      amzReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      amzReq.setTimeout(15000, function(){ amzReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
      amzReq.end();
    });
    return;
  }

  // ── Debug: Check Amazon listing restrictions for an ASIN ──
  if (pathname === '/warehouse/check-amazon-restrictions' && req.method === 'GET') {
    var asin = parsed.query.asin || '0525559477';
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      console.log('Using AMAZON_SELLER_ID:', AMAZON_SELLER_ID);
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: '/listings/2021-08-01/restrictions?asin=' + asin + '&sellerId=' + AMAZON_SELLER_ID + '&marketplaceIds=' + AMAZON_MARKETPLACE_ID + '&conditionType=used_good',
        method: 'GET',
        headers: { 'x-amz-access-token': accessToken, 'Accept': 'application/json' }
      };
      var req2 = https.request(opts, function(res2){
        var data = ''; res2.on('data',function(c){data+=c;}); res2.on('end',function(){
          console.log('Amazon restrictions:', res2.statusCode, data);
          res.writeHead(200); res.end(data);
        });
      });
      req2.on('error',function(e){ res.writeHead(200); res.end(JSON.stringify({error:e.message})); });
      req2.end();
    });
    return;
  }

  // ── Debug: Get Amazon product type requirements ──
  if (pathname === '/warehouse/amazon-requirements' && req.method === 'GET') {
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: '/definitions/2020-09-01/productTypes/PRODUCT?marketplaceIds=' + AMAZON_MARKETPLACE_ID + '&requirements=LISTING_OFFER_ONLY',
        method: 'GET',
        headers: { 'x-amz-access-token': accessToken, 'Accept': 'application/json' }
      };
      var req2 = https.request(opts, function(res2){
        var data = ''; res2.on('data',function(c){data+=c;}); res2.on('end',function(){
          console.log('Amazon requirements:', res2.statusCode, data.substring(0,1000));
          res.writeHead(200); res.end(data);
        });
      });
      req2.on('error',function(e){ res.writeHead(200); res.end(JSON.stringify({error:e.message})); });
      req2.end();
    });
    return;
  }

  // ── Debug: Test Amazon listing directly ──
  if (pathname === '/warehouse/test-amazon-list' && req.method === 'GET') {
    var testAsin = parsed.query.asin || '0525559477';
    var testSku = 'test-sku-' + Date.now();
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      var body = JSON.stringify({
        productType: 'PRODUCT',
        requirements: 'LISTING_OFFER_ONLY',
        attributes: {
          merchant_suggested_asin: [{ value: testAsin, marketplace_id: AMAZON_MARKETPLACE_ID }],
          condition_type: [{ value: 'used_good', marketplace_id: AMAZON_MARKETPLACE_ID }],
          purchasable_offer: [{
            marketplace_id: AMAZON_MARKETPLACE_ID,
            currency: 'USD',
            our_price: [{ schedule: [{ value_with_tax: 8.99 }] }]
          }],
          fulfillment_availability: [{
            fulfillment_channel_code: 'DEFAULT',
            quantity: 1,
            marketplace_id: AMAZON_MARKETPLACE_ID
          }]
        }
      });
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: '/listings/2021-08-01/items/' + AMAZON_SELLER_ID + '/' + encodeURIComponent(testSku) + '?marketplaceIds=' + AMAZON_MARKETPLACE_ID,
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'x-amz-access-token': accessToken,
          'Content-Length': Buffer.byteLength(body)
        }
      };
      console.log('TEST Amazon body:', body);
      console.log('TEST Amazon path:', opts.path);
      var amzReq = https.request(opts, function(amzRes){
        var amzData = '';
        amzRes.on('data', function(c){ amzData += c; });
        amzRes.on('end', function(){
          console.log('TEST Amazon response:', amzRes.statusCode, amzData);
          res.writeHead(200); res.end(JSON.stringify({ status: amzRes.statusCode, body: JSON.parse(amzData || '{}') }));
        });
      });
      amzReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      amzReq.write(body); amzReq.end();
    });
    return;
  }

  // ── Warehouse: Delete Amazon listing (rollback) ──
  if (pathname === '/warehouse/delete-amazon' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      getSubscriber(code, function(err, sub){
        if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        getAmazonAccessToken(function(err, accessToken){
          if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
          var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
          var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
          var opts = {
            hostname: 'sellingpartnerapi-na.amazon.com',
            path: '/listings/2021-08-01/items/' + sellerId + '/' + encodeURIComponent(data.sku) + '?marketplaceIds=' + marketplaceId,
            method: 'DELETE',
            headers: { 'x-amz-access-token': accessToken }
          };
          var amzReq = https.request(opts, function(amzRes){
            var d=''; amzRes.on('data',function(c){d+=c;}); amzRes.on('end',function(){
              console.log('Amazon rollback response:', amzRes.statusCode, d.substring(0,200));
              res.writeHead(200); res.end(JSON.stringify({ success: true }));
            });
          });
          amzReq.on('error',function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
          amzReq.end();
        });
      });
    });
    return;
  }

  // ── Warehouse: List item on Amazon ──
  if (pathname === '/warehouse/list-amazon' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      getSubscriber(code, function(err, sub) {
        if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        var clientId = sub.amazonClientId || AMAZON_CLIENT_ID;
        var clientSecret = sub.amazonClientSecret || AMAZON_CLIENT_SECRET;
        var refreshToken = sub.amazonRefreshToken || AMAZON_REFRESH_TOKEN;
        var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
        // Get Amazon access token
        getAmazonAccessToken(function(err, accessToken){
          if(err){ res.writeHead(200); res.end(JSON.stringify({ error: 'Amazon auth failed: ' + err })); return; }
          var sku = data.sku || '';
          var price = parseFloat(data.price || 9.99).toFixed(2);
          var conditionMap = {'New':'new_new','Like New':'used_like_new','Very Good':'used_very_good','Good':'used_good','Acceptable':'used_acceptable'};
          var condition = conditionMap[data.conditionLabel] || 'used_good';
          var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;

          // Amazon requires ASIN to match catalog for used books
          // We use the ASIN from our catalog lookup
          var asin = data.asin || '';
          if(!asin){ res.writeHead(200); res.end(JSON.stringify({ error: 'No ASIN available — cannot list on Amazon without catalog match' })); return; }

          var body = JSON.stringify({
            productType: 'PRODUCT',
            requirements: 'LISTING_OFFER_ONLY',
            attributes: {
              merchant_suggested_asin: [{ value: asin, marketplace_id: marketplaceId }],
              condition_type: [{ value: condition, marketplace_id: marketplaceId }],
              purchasable_offer: [{
                marketplace_id: marketplaceId,
                currency: 'USD',
                our_price: [{
                  schedule: [{
                    value_with_tax: parseFloat(price),
                    start_at: { value: new Date().toISOString() }
                  }]
                }]
              }],
              fulfillment_availability: [{
                fulfillment_channel_code: 'DEFAULT',
                quantity: 1,
                marketplace_id: marketplaceId
              }]
            }
          });
          console.log('Amazon listing body:', body);
          // Double-encode SKU to handle dots and special chars safely
          var encodedSku = encodeURIComponent(encodeURIComponent(sku));
          var opts = {
            hostname: 'sellingpartnerapi-na.amazon.com',
            path: '/listings/2021-08-01/items/' + sellerId + '/' + encodedSku + '?marketplaceIds=' + marketplaceId,
            method: 'PUT',
            headers: {
              'Content-Type': 'application/json',
              'x-amz-access-token': accessToken,
              'Content-Length': Buffer.byteLength(body)
            }
          };
          var amzReq = https.request(opts, function(amzRes){
            var amzData = '';
            amzRes.on('data', function(c){ amzData += c; });
            amzRes.on('end', function(){
              console.log('Amazon listing response:', amzRes.statusCode, amzData);
              try {
                var json = JSON.parse(amzData);
                if(amzRes.statusCode === 200 || amzRes.statusCode === 201){
                  res.writeHead(200); res.end(JSON.stringify({ success: true, asin: json.asin || null }));
                } else {
                  var errMsg = (json.errors && json.errors[0] && json.errors[0].message) || amzData;
                  res.writeHead(200); res.end(JSON.stringify({ error: errMsg }));
                }
              } catch(e){ res.writeHead(200); res.end(JSON.stringify({ error: amzData })); }
            });
          });
          amzReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
          amzReq.setTimeout(30000, function(){ amzReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
          amzReq.write(body); amzReq.end();
        });
      });
    });
    return;
  }

  // ── Warehouse: List item on eBay LIVE (no schedule) ──
  if (pathname === '/warehouse/list-ebay' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      getSubscriber(code, function(err, sub) {
        if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
        var userToken = sub.ebayUserToken || USER_TOKEN;
        var devId = sub.ebayDevId || DEV_ID;
        var shippingPolicyId = sub.ebayShippingPolicyId || '193108528015';
        var paymentPolicyId = sub.ebayPaymentPolicyId || '226293158015';
        var returnPolicyId = sub.ebayReturnPolicyId || '129856789015';
        var conditionId = data.conditionId || 5000;
        var desc = cleanDescription(data.description || '');

        // Build picture URL from cover image
        // Use eBay's own stock photo via ISBN - more reliable than external URLs
        var pictureXml = '';

        var xml = '<?xml version="1.0" encoding="utf-8"?>'
          + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
          + '<RequesterCredentials><eBayAuthToken>' + userToken + '</eBayAuthToken></RequesterCredentials>'
          + '<Item>'
          + '<Title>' + esc(data.title || '').substring(0,80) + '</Title>'
          + '<Description><![CDATA[' + desc + ']]></Description>'
          + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
          + '<StartPrice>' + parseFloat(data.price || 9.99).toFixed(2) + '</StartPrice>'
          + '<ConditionID>' + conditionId + '</ConditionID>'
          + '<Country>US</Country>'
          + '<Location>United States</Location>'
          + '<Currency>USD</Currency>'
          + '<DispatchTimeMax>2</DispatchTimeMax>'
          + '<ListingDuration>GTC</ListingDuration>'
          + '<ListingType>FixedPriceItem</ListingType>'
          + '<PictureDetails><GalleryType>Gallery</GalleryType></PictureDetails>'
          + '<SKU>' + esc(data.sku || '') + '</SKU>'
          + '<BestOfferDetails><BestOfferEnabled>true</BestOfferEnabled></BestOfferDetails>'
          + pictureXml
          + '<SellerProfiles>'
          + '<SellerShippingProfile><ShippingProfileID>' + shippingPolicyId + '</ShippingProfileID></SellerShippingProfile>'
          + '<SellerReturnProfile><ReturnProfileID>' + returnPolicyId + '</ReturnProfileID></SellerReturnProfile>'
          + '<SellerPaymentProfile><PaymentProfileID>' + paymentPolicyId + '</PaymentProfileID></SellerPaymentProfile>'
          + '</SellerProfiles>'
          + '<ItemSpecifics>'
          + '<NameValueList><n>Book Title</n><Value>' + esc((data.bookTitle || data.title || '').replace(/^—+$/, '').substring(0,65) || 'See description') + '</Value></NameValueList>'
          + '<NameValueList><n>Author</n><Value>' + esc((data.author && data.author.replace(/^—+$/, '')) || 'Unknown').substring(0,65) + '</Value></NameValueList>'
          + '<NameValueList><n>Language</n><Value>' + esc(data.language && data.language.length > 1 ? data.language.charAt(0).toUpperCase() + data.language.slice(1).toLowerCase() : 'English') + '</Value></NameValueList>'
          + (data.publisher ? '<NameValueList><n>Publisher</n><Value>' + esc(data.publisher).substring(0,65) + '</Value></NameValueList>' : '')
          + (data.year ? '<NameValueList><n>Publication Year</n><Value>' + esc(data.year) + '</Value></NameValueList>' : '')
          + (data.format ? '<NameValueList><n>Format</n><Value>' + esc(data.format) + '</Value></NameValueList>' : '')
          + (data.edition ? '<NameValueList><n>Edition</n><Value>' + esc(data.edition) + '</Value></NameValueList>' : '')
          + (data.pages ? '<NameValueList><n>Number of Pages</n><Value>' + esc(String(data.pages)) + '</Value></NameValueList>' : '')
          + (data.series ? '<NameValueList><n>Series</n><Value>' + esc(data.series).substring(0,65) + '</Value></NameValueList>' : '')
          + '</ItemSpecifics>'
          + (data.isbn ? '<ProductListingDetails><ISBN>' + esc(data.isbn) + '</ISBN><IncludeStockPhotoURL>true</IncludeStockPhotoURL><UseStockPhotoURLAsGallery>true</UseStockPhotoURLAsGallery></ProductListingDetails>' : '')
          + '</Item>'
          + '</AddItemRequest>';
        console.log('Warehouse eBay XML ItemSpecifics:', xml.substring(xml.indexOf('<ItemSpecifics>'), xml.indexOf('</ItemSpecifics>') + 16));
        var ebayOpts = {
          hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
          headers: {
            'X-EBAY-API-SITEID': '0', 'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
            'X-EBAY-API-CALL-NAME': 'AddItem', 'X-EBAY-API-DEV-NAME': devId,
            'X-EBAY-API-APP-NAME': (sub.ebayClientId || CLIENT_ID),
            'X-EBAY-API-CERT-NAME': (sub.ebayClientSecret || CLIENT_SECRET),
            'Content-Type': 'text/xml', 'Content-Length': Buffer.byteLength(xml)
          }
        };
        var ebayReq = https.request(ebayOpts, function(ebayRes) {
          var ebayData = '';
          ebayRes.on('data', function(c){ ebayData += c; });
          ebayRes.on('end', function(){
            console.log('Warehouse eBay response:', ebayData.substring(0,500));
            var idMatch = ebayData.match(/<ItemID>(\d+)<\/ItemID>/);
            var errMatch = ebayData.match(/<LongMessage>(.*?)<\/LongMessage>/);
            if(idMatch){
              var ebayItemId = idMatch[1];
              if(data.itemId){
                connectMongo(function(err, database){
                  if(database){
                    var mongodb = require('mongodb');
                    try {
                      database.collection('warehouse_inventory').updateOne(
                        { _id: new mongodb.ObjectId(data.itemId) },
                        { $set: { ebayItemId: ebayItemId }, $push: { listedOn: 'ebay' } }
                      ).catch(function(){});
                    } catch(e){}
                  }
                });
              }
              res.writeHead(200); res.end(JSON.stringify({ success: true, ebayItemId: ebayItemId }));
            } else {
              var errMsg = errMatch ? errMatch[1] : 'Unknown eBay error';
              res.writeHead(200); res.end(JSON.stringify({ error: errMsg }));
            }
          });
        });
        ebayReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
        ebayReq.setTimeout(30000, function(){ ebayReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
        ebayReq.write(xml);
        ebayReq.end();
      });
    });
    return;
  }

  // ── Warehouse: Get map fill data ──
  if (pathname === '/warehouse/map' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    if(!code){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ fills: {} })); return; }
      // Get subscriber warehouse config for capacity
      getSubscriber(code, function(err, sub){
        var config = (sub && sub.warehouseConfig) || { inchesPerSection: 366, defaultBookWidthInches: 1.2 };
        database.collection('warehouse_inventory').find({ code: code, status: 'active' }).toArray()
        .then(function(items){
          var fills = {};
          items.forEach(function(item){
            if(!item.location) return;
            var key = item.location.row + '-' + item.location.section;
            if(!fills[key]) fills[key] = { used: 0, capacity: config.inchesPerSection, count: 0 };
            fills[key].used += parseFloat(item.widthInches || config.defaultBookWidthInches || 1.2);
            fills[key].count++;
          });
          res.writeHead(200); res.end(JSON.stringify({ fills: fills }));
        })
        .catch(function(){ res.writeHead(200); res.end(JSON.stringify({ fills: {} })); });
      });
    });
    return;
  }

  // ── Warehouse: Get next location sequence ──
  if (pathname === '/warehouse/next-location' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var row = parseInt(parsed.query.row || '1');
    var section = parsed.query.section || 'A';
    if(!code){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ row: row, section: section, sequence: 1 })); return; }
      // Find highest sequence number for this row+section
      database.collection('warehouse_inventory').find({
        code: code,
        'location.row': row,
        'location.section': section
      }).sort({ 'location.sequence': -1 }).limit(1).toArray()
      .then(function(items){
        var nextSeq = items.length > 0 ? (items[0].location.sequence + 1) : 1;
        res.writeHead(200); res.end(JSON.stringify({ row: row, section: section, sequence: nextSeq }));
      })
      .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ row: row, section: section, sequence: 1 })); });
    });
    return;
  }

  // ── Warehouse: Save item to inventory ──
  if (pathname === '/warehouse/item' && req.method === 'POST') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').toUpperCase();
      if(!code){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
      connectMongo(function(err, database) {
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        // Ensure SKU is unique
        database.collection('warehouse_inventory').findOne({ sku: data.sku })
        .then(function(existing){
          if(existing){ res.writeHead(200); res.end(JSON.stringify({ error: 'SKU already exists' })); return; }
          var item = {
            code: code,
            sku: data.sku,
            isbn: data.isbn || '',
            title: data.title || '',
            author: data.author || '',
            publisher: data.publisher || '',
            year: data.year || '',
            format: data.format || '',
            condition: data.condition || 'Good',
            price: parseFloat(data.price) || 9.99,
            location: data.location || {},
            listedOn: data.listedOn || [],
            status: 'active',
            createdAt: new Date()
          };
          return database.collection('warehouse_inventory').insertOne(item)
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({ success: true, itemId: result.insertedId }));
          });
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Warehouse: Get stats ──
  if (pathname === '/warehouse/stats' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    if(!code){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ totalItems: 0, recentActivity: [] })); return; }
      Promise.all([
        database.collection('warehouse_inventory').countDocuments({ code: code, status: 'active' }),
        database.collection('warehouse_inventory').find({ code: code }).sort({ createdAt: -1 }).limit(6).toArray()
      ]).then(function(results){
        var total = results[0];
        var recent = results[1].map(function(item){
          return {
            type: 'listed',
            message: item.title + ' listed — ' + item.sku,
            createdAt: item.createdAt
          };
        });
        res.writeHead(200); res.end(JSON.stringify({ totalItems: total, recentActivity: recent }));
      })
      .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ totalItems: 0, recentActivity: [] })); });
    });
    return;
  }

  // ── Warehouse: Get inventory list ──
  if (pathname === '/warehouse/inventory' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var status = parsed.query.status || 'active';
    var limit = parseInt(parsed.query.limit || '50');
    if(!code){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    connectMongo(function(err, database) {
      if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ items: [] })); return; }
      database.collection('warehouse_inventory').find({ code: code, status: status }).sort({ createdAt: -1 }).limit(limit).toArray()
      .then(function(items){
        res.writeHead(200); res.end(JSON.stringify({ items: items }));
      })
      .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ items: [] })); });
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
      res.writeHead(200); res.end(JSON.stringify({ found: !!sub, code: testCode, err: err ? err.toString() : null, sub: sub ? { code: sub.code, businessName: sub.businessName, active: sub.active, ebayClientId: sub.ebayClientId, hasToken: !!sub.ebayUserToken, tokenLength: sub.ebayUserToken ? sub.ebayUserToken.length : 0, tokenStart: sub.ebayUserToken ? sub.ebayUserToken.substring(0,20) : 'NONE', hasOAuthToken: !!sub.ebayOAuthToken, oauthTokenLength: sub.ebayOAuthToken ? sub.ebayOAuthToken.length : 0, oauthTokenStart: sub.ebayOAuthToken ? sub.ebayOAuthToken.substring(0,20) : 'NONE', employeeCount: (sub.employees||[]).length, employees: (sub.employees||[]).map(function(e){ return { name: e.name, pin: e.pin, hourlyRate: e.hourlyRate, currency: e.currency, payPeriod: e.payPeriod, country: e.country }; }), shippingPolicy: sub.ebayShippingPolicyId || 'NOT SET', paymentPolicy: sub.ebayPaymentPolicyId || 'NOT SET', returnPolicy: sub.ebayReturnPolicyId || 'NOT SET' } : null }));
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

        // ── Safety validation ──
        // 1. Never overwrite employees with fewer than already saved
        if (data.employees !== undefined) {
          var existingCount = (sub.employees || []).length;
          var newCount = (data.employees || []).length;
          if (existingCount > 0 && newCount < existingCount) {
            res.writeHead(200); res.end(JSON.stringify({ error: 'Save blocked: you currently have ' + existingCount + ' employees but this save only contains ' + newCount + '. Please check and try again.' }));
            return;
          }
          // Never save employees with missing names or PINs
          var invalid = (data.employees || []).filter(function(e){ return !e.name || !e.pin; });
          if (invalid.length > 0) {
            res.writeHead(200); res.end(JSON.stringify({ error: 'Save blocked: one or more employees are missing a name or PIN.' }));
            return;
          }
        }
        // 2. Never overwrite eBay credentials with empty values
        if (data.ebayClientId !== undefined && !data.ebayClientId && sub.ebayClientId) {
          res.writeHead(200); res.end(JSON.stringify({ error: 'Save blocked: eBay Client ID cannot be cleared.' })); return;
        }
        if (data.ebayUserToken !== undefined && !data.ebayUserToken && sub.ebayUserToken) {
          res.writeHead(200); res.end(JSON.stringify({ error: 'Save blocked: eBay User Token cannot be cleared.' })); return;
        }
        // 3. Never overwrite business name with empty
        if (data.businessName !== undefined && !data.businessName.trim() && sub.businessName) {
          res.writeHead(200); res.end(JSON.stringify({ error: 'Save blocked: Business name cannot be empty.' })); return;
        }

        // Only allow updating safe fields
        var allowed = { employees: data.employees, email: data.email, businessName: data.businessName, ebayClientId: data.ebayClientId, ebayClientSecret: data.ebayClientSecret, ebayDevId: data.ebayDevId, ebayUserToken: data.ebayUserToken, ebayOAuthToken: data.ebayOAuthToken, ebayShippingPolicyId: data.ebayShippingPolicyId, ebayPaymentPolicyId: data.ebayPaymentPolicyId, ebayReturnPolicyId: data.ebayReturnPolicyId };
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
          ebayShippingPolicyId: process.env.EBAY_SHIPPING_POLICY_ID || '193108528015',
          ebayPaymentPolicyId: process.env.EBAY_PAYMENT_POLICY_ID || '226293158015',
          ebayReturnPolicyId: process.env.EBAY_RETURN_POLICY_ID || '129856789015',
          notes: 'Master admin account'
        },
        $setOnInsert: {
          ebayUserToken: USER_TOKEN
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
