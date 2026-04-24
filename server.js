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
var AMAZON_SELLER_ID = process.env.AMAZON_SELLER_ID || 'A1A1G57C14ORT4';

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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-admin-key, x-access-code, x-session-token');
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
// Refresh one subscriber's eBay access token using their stored refresh token.
// cb receives (err, newAccessToken). Also updates MongoDB.
function refreshEbayTokenForSubscriber(sub, cb) {
  cb = cb || function(){};
  if (!sub || !sub.ebayRefreshToken) { cb('no-refresh-token'); return; }
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
          var newExpiry = new Date(Date.now() + (json.expires_in || 7200) * 1000).toISOString();
          connectMongo(function(err, database){
            if (err || !database) { cb(null, json.access_token); return; }
            database.collection('subscribers').updateOne(
              { code: sub.code },
              { $set: { ebayOAuthToken: json.access_token, ebayOAuthExpiry: newExpiry } }
            )
            .then(function(){
              console.log('eBay token refreshed for:', sub.code);
              cb(null, json.access_token);
            })
            .catch(function(e){
              console.log('Token save error:', e.message);
              cb(null, json.access_token); // token is good even if save fails
            });
          });
        } else {
          console.log('Token refresh failed for', sub.code, ':', data.substring(0, 200));
          cb('refresh-failed: ' + data.substring(0, 200));
        }
      } catch(e){
        console.log('Token refresh parse error:', e.message);
        cb('parse-error: ' + e.message);
      }
    });
  });
  req2.on('error', function(e){
    console.log('Token refresh request error:', e.message);
    cb('network-error: ' + e.message);
  });
  req2.setTimeout(15000, function(){ req2.destroy(); cb('timeout'); });
  req2.write(body); req2.end();
}

function refreshAllEbayTokens() {
  connectMongo(function(err, database) {
    if (err || !database) return;
    database.collection('subscribers').find({ ebayRefreshToken: { $exists: true, $ne: '' } }).toArray()
    .then(function(subs) {
      subs.forEach(function(sub) {
        refreshEbayTokenForSubscriber(sub, function(){});
      });
    })
    .catch(function(e){ console.log('Token refresh DB error:', e.message); });
  });
}

// Run token refresh shortly after startup (30s grace so DB is ready), then every 90 minutes
setTimeout(refreshAllEbayTokens, 30 * 1000);
setInterval(refreshAllEbayTokens, 90 * 60 * 1000);

// ─────────────────────────────────────────────────────────────
// SESSION SYSTEM for role-based access (owner vs employee)
// In-memory. Sessions survive until server restart (fine for this scale).
// ─────────────────────────────────────────────────────────────
var sessionStore = {}; // token -> { subscriberCode, role, employeeName, expiresAt }

function createSession(subscriberCode, role, employeeName){
  var token = crypto.randomBytes(24).toString('hex');
  sessionStore[token] = {
    subscriberCode: subscriberCode,
    role: role,  // 'owner' or 'employee'
    employeeName: employeeName || null,
    expiresAt: Date.now() + 24 * 60 * 60 * 1000  // 24h sessions
  };
  return token;
}

function getSession(token){
  if(!token) return null;
  var s = sessionStore[token];
  if(!s) return null;
  if(s.expiresAt < Date.now()){ delete sessionStore[token]; return null; }
  return s;
}

// Gets session from X-Session-Token header or sessionToken query param
function getRequestSession(req, parsed){
  var token = req.headers['x-session-token'] || (parsed && parsed.query && parsed.query.sessionToken) || '';
  return getSession(token);
}

// Periodic cleanup of expired sessions
setInterval(function(){
  var now = Date.now();
  Object.keys(sessionStore).forEach(function(t){
    if(sessionStore[t].expiresAt < now) delete sessionStore[t];
  });
}, 60 * 60 * 1000);

// ─────────────────────────────────────────────────────────────
// TOOL HEALTH CHECK SYSTEM
// Runs every 5 minutes. Caches latest result per subscriber.
// Used by portal to show green/red status indicators.
// ─────────────────────────────────────────────────────────────
var toolHealthCache = {}; // { 'SUBSCRIBERCODE': { ebayListingTool: {...}, warehouseTool: {...}, checkedAt: ISO } }

// Test #1: eBay Browse API (price suggestions)
// Obtains app token via client_credentials grant. If it succeeds, price suggestions can work.
function testEbayBrowseApi(clientId, clientSecret, cb){
  if(!clientId || !clientSecret){ cb({ ok: false, error: 'Missing eBay Client ID or Client Secret' }); return; }
  var credentials = Buffer.from(clientId + ':' + clientSecret).toString('base64');
  var body = 'grant_type=client_credentials&scope=' + encodeURIComponent('https://api.ebay.com/oauth/api_scope');
  var opts = {
    hostname: 'api.ebay.com', path: '/identity/v1/oauth2/token', method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic ' + credentials, 'Content-Length': Buffer.byteLength(body) }
  };
  var tReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      try {
        var json = JSON.parse(data);
        if(json.access_token) cb({ ok: true });
        else cb({ ok: false, error: 'Browse API token denied: ' + (json.error_description || json.error || 'unknown') });
      } catch(e){ cb({ ok: false, error: 'Browse API parse error' }); }
    });
  });
  tReq.on('error', function(e){ cb({ ok: false, error: 'Browse API network: ' + e.message }); });
  tReq.setTimeout(10000, function(){ tReq.destroy(); cb({ ok: false, error: 'Browse API timeout' }); });
  tReq.write(body); tReq.end();
}

// Test #2: eBay Trading API LIVE TEST via VerifyAddItem
// VerifyAddItem uses the exact same code path as AddItem but only validates — doesn't create.
// This is the most accurate real-time test because it exercises auth + business policies + item validation.
// Uses a minimal test book payload that's realistic enough to get past Akamai's bot filter.
function testEbayTradingApi(sub, cb){
  var userToken = sub.ebayUserToken || USER_TOKEN;
  var devId = sub.ebayDevId || DEV_ID;
  var clientId = sub.ebayClientId || CLIENT_ID;
  var clientSecret = sub.ebayClientSecret || CLIENT_SECRET;
  var shippingPolicyId = sub.ebayShippingPolicyId;
  var paymentPolicyId = sub.ebayPaymentPolicyId;
  var returnPolicyId = sub.ebayReturnPolicyId;

  if(!userToken){ cb({ ok: false, error: 'Missing eBay User Token' }); return; }
  if(!devId){ cb({ ok: false, error: 'Missing eBay Dev ID' }); return; }
  if(!clientId || !clientSecret){ cb({ ok: false, error: 'Missing App ID or Cert ID' }); return; }
  // VerifyAddItem needs a valid item. Build a minimal test book payload.
  var scheduleTime = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
  var isIafToken = userToken.substring(0, 5) === 'v^1.1';
  var sellerProfiles = (shippingPolicyId && paymentPolicyId && returnPolicyId) ? (
    '<SellerProfiles>'
    + '<SellerShippingProfile><ShippingProfileID>' + shippingPolicyId + '</ShippingProfileID></SellerShippingProfile>'
    + '<SellerReturnProfile><ReturnProfileID>' + returnPolicyId + '</ReturnProfileID></SellerReturnProfile>'
    + '<SellerPaymentProfile><PaymentProfileID>' + paymentPolicyId + '</PaymentProfileID></SellerPaymentProfile>'
    + '</SellerProfiles>'
  ) : '';
  var xml = '<?xml version="1.0" encoding="utf-8"?>'
    + '<VerifyAddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + (isIafToken ? '' : '<RequesterCredentials><eBayAuthToken>' + userToken + '</eBayAuthToken></RequesterCredentials>')
    + '<Item>'
    + '<Title>Health Check Test Book - Do Not List</Title>'
    + '<Description><![CDATA[This is an automated health check, not a real listing.]]></Description>'
    + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
    + '<StartPrice>9.99</StartPrice>'
    + '<ConditionID>5000</ConditionID>'
    + '<Country>US</Country><Location>United States</Location><Currency>USD</Currency>'
    + '<DispatchTimeMax>3</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<ScheduleTime>' + scheduleTime + '</ScheduleTime>'
    + sellerProfiles
    + '<ItemSpecifics>'
    + '<NameValueList><Name>Book Title</Name><Value>Health Check Test</Value></NameValueList>'
    + '<NameValueList><Name>Author</Name><Value>Test</Value></NameValueList>'
    + '<NameValueList><Name>Language</Name><Value>English</Value></NameValueList>'
    + '<NameValueList><Name>Format</Name><Value>Paperback</Value></NameValueList>'
    + '</ItemSpecifics>'
    + '</Item>'
    + '</VerifyAddItemRequest>';

  var attempt = 0;
  var maxAttempts = 3;
  function tryOnce(){
    attempt++;
    var headers = {
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'VerifyAddItem',
      'X-EBAY-API-DEV-NAME': devId,
      'X-EBAY-API-APP-NAME': clientId,
      'X-EBAY-API-CERT-NAME': clientSecret,
      'User-Agent': 'BooksForAgesHealthCheck/1.0',
      'Content-Type': 'text/xml',
      'Content-Length': Buffer.byteLength(xml)
    };
    if(isIafToken) headers['X-EBAY-API-IAF-TOKEN'] = userToken;
    var opts = { hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST', headers: headers };
    var tReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        // VerifyAddItem success cases:
        //   1) Ack=Success — item fully valid
        //   2) Ack=Warning — item valid with minor warnings
        //   3) Ack=Failure BUT only content errors (not auth) — means auth works, just our fake item data incomplete
        // Auth failure cases: token-related error codes
        if(/<Ack>Success<\/Ack>/.test(data) || /<Ack>Warning<\/Ack>/.test(data)){
          cb({ ok: true }); return;
        }
        // Look at error codes to distinguish auth vs content errors
        var errorCodes = (data.match(/<ErrorCode>(\d+)<\/ErrorCode>/g) || [])
          .map(function(m){ return m.replace(/<\/?ErrorCode>/g, ''); });
        // eBay auth-related error codes (invalid token, app, permissions)
        var authErrorCodes = ['931','932','16110','17470','17471','21916249','21919188','10001','2038'];
        var hasAuthError = errorCodes.some(function(c){ return authErrorCodes.indexOf(c) !== -1; });
        // Got an eBay XML response (HTTP 200, parseable) with no auth-specific errors = auth works
        if(r.statusCode === 200 && errorCodes.length > 0 && !hasAuthError){
          cb({ ok: true }); return;
        }
        // Transient 503 — retry
        if(r.statusCode === 503 && attempt < maxAttempts){
          setTimeout(tryOnce, 1500 * attempt); return;
        }
        // Real failure — parse the main message
        var longMsg = data.match(/<LongMessage>([^<]+)<\/LongMessage>/);
        var shortMsg = data.match(/<ShortMessage>([^<]+)<\/ShortMessage>/);
        var errMsg = '';
        if(hasAuthError && longMsg) errMsg = longMsg[1];
        else if(hasAuthError) errMsg = 'auth error code ' + errorCodes.filter(function(c){ return authErrorCodes.indexOf(c)!==-1; })[0];
        else if(longMsg) errMsg = longMsg[1];
        else if(shortMsg) errMsg = shortMsg[1];
        else if(r.statusCode === 503) errMsg = 'eBay CDN 503 after ' + attempt + ' attempts';
        else if(r.statusCode !== 200) errMsg = 'HTTP ' + r.statusCode;
        else errMsg = 'unrecognized response';
        cb({ ok: false, error: 'VerifyAddItem: ' + errMsg.substring(0, 200) });
      });
    });
    tReq.on('error', function(e){
      if(attempt < maxAttempts){ setTimeout(tryOnce, 1500 * attempt); return; }
      cb({ ok: false, error: 'Trading API network: ' + e.message });
    });
    tReq.setTimeout(15000, function(){
      tReq.destroy();
      if(attempt < maxAttempts){ setTimeout(tryOnce, 1500 * attempt); return; }
      cb({ ok: false, error: 'Trading API timeout' });
    });
    tReq.write(xml); tReq.end();
  }
  tryOnce();
}

// Test #3: eBay Business Policies are saved
function testEbayPolicies(sub){
  if(!sub.ebayShippingPolicyId) return { ok: false, error: 'Missing Shipping Policy ID' };
  if(!sub.ebayPaymentPolicyId)  return { ok: false, error: 'Missing Payment Policy ID' };
  if(!sub.ebayReturnPolicyId)   return { ok: false, error: 'Missing Return Policy ID' };
  return { ok: true };
}

// Test #4: Amazon SP-API — get access token + ping marketplaceParticipations
function testAmazonSpApi(cb){
  getAmazonAccessToken(function(err, accessToken){
    if(err){ cb({ ok: false, error: 'Amazon token: ' + String(err).substring(0, 160) }); return; }
    var opts = {
      hostname: 'sellingpartnerapi-na.amazon.com',
      path: '/sellers/v1/marketplaceParticipations',
      method: 'GET',
      headers: { 'x-amz-access-token': accessToken }
    };
    var tReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        if(r.statusCode === 200){ cb({ ok: true }); }
        else {
          var short = data.substring(0, 200).replace(/\s+/g, ' ');
          cb({ ok: false, error: 'Amazon SP-API ' + r.statusCode + ': ' + short });
        }
      });
    });
    tReq.on('error', function(e){ cb({ ok: false, error: 'Amazon SP-API network: ' + e.message }); });
    tReq.setTimeout(12000, function(){ tReq.destroy(); cb({ ok: false, error: 'Amazon SP-API timeout' }); });
    tReq.end();
  });
}

// Run all tests for one subscriber, cache results
function runHealthCheckForSubscriber(sub){
  if(!sub || !sub.code) return;
  var code = sub.code.toUpperCase();
  var clientId = sub.ebayClientId || CLIENT_ID;
  var clientSecret = sub.ebayClientSecret || CLIENT_SECRET;
  var userToken = sub.ebayUserToken || USER_TOKEN;
  var devId = sub.ebayDevId || DEV_ID;

  var results = { browseApi: null, tradingApi: null, policies: null, amazon: null };
  var completed = 0;

  function done(){
    completed++;
    if(completed < 3) return; // wait for browseApi, tradingApi, amazon (policies is sync)
    results.policies = testEbayPolicies(sub);

    // eBay Listing Tool = browseApi + tradingApi + policies all pass
    var eBayOk = results.browseApi.ok && results.tradingApi.ok && results.policies.ok;
    var eBayFails = [];
    if(!results.browseApi.ok)  eBayFails.push('Price suggestions: ' + results.browseApi.error);
    if(!results.tradingApi.ok) eBayFails.push('Listing auth: ' + results.tradingApi.error);
    if(!results.policies.ok)   eBayFails.push('Business policies: ' + results.policies.error);

    // Warehouse Tool = everything eBay Listing Tool needs + Amazon
    var whOk = eBayOk && results.amazon.ok;
    var whFails = eBayFails.slice();
    if(!results.amazon.ok) whFails.push('Amazon: ' + results.amazon.error);

    toolHealthCache[code] = {
      ebayListingTool: { ok: eBayOk, errors: eBayFails, details: results },
      warehouseTool:   { ok: whOk, errors: whFails, details: results },
      checkedAt: new Date().toISOString()
    };
    console.log('Tool health for', code, '— eBay:', eBayOk ? 'OK' : 'FAIL', '| Warehouse:', whOk ? 'OK' : 'FAIL');
  }

  testEbayBrowseApi(clientId, clientSecret, function(res){ results.browseApi = res; done(); });
  testEbayTradingApi(sub, function(res){ results.tradingApi = res; done(); });
  testAmazonSpApi(function(res){ results.amazon = res; done(); });
}

// Run health checks for all subscribers
function runAllHealthChecks(){
  connectMongo(function(err, database){
    if(err || !database) return;
    database.collection('subscribers').find({}).toArray()
    .then(function(subs){
      (subs || []).forEach(function(sub){ runHealthCheckForSubscriber(sub); });
    })
    .catch(function(e){ console.log('Health check DB error:', e.message); });
  });
}

// Run 45 seconds after startup, then every 5 minutes
setTimeout(runAllHealthChecks, 45 * 1000);
setInterval(runAllHealthChecks, 5 * 60 * 1000);

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
      if (match) { cb(null, match[1]); return; }
      // Extract eBay's actual error so we can see WHY the upload failed
      var errMatch = data.match(/<LongMessage>(.*?)<\/LongMessage>/);
      var shortMatch = data.match(/<ShortMessage>(.*?)<\/ShortMessage>/);
      var reason = (errMatch && errMatch[1]) || (shortMatch && shortMatch[1]) || 'no FullURL in response';
      console.log('[uploadPicture] eBay rejected upload: ' + reason + ' — response sample: ' + data.substring(0, 300));
      cb('Upload failed: ' + reason);
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(30000, function() { req.destroy(); cb('Timeout'); });
  req.write(bodyBuffer); req.end();
}

// Parse JPEG/PNG header to read image dimensions without a full decode.
// Returns {width, height} or null if format is unrecognized / buffer too short.
function getImageDimensions(buf){
  if(!buf || buf.length < 24) return null;
  // PNG: 89 50 4E 47 ... then IHDR at offset 16 (width=16-19, height=20-23)
  if(buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4E && buf[3] === 0x47){
    try { return { width: buf.readUInt32BE(16), height: buf.readUInt32BE(20) }; } catch(e){ return null; }
  }
  // JPEG: FF D8 ... walk segments until we hit a Start-Of-Frame marker (FFC0-FFC3)
  if(buf[0] === 0xFF && buf[1] === 0xD8){
    var i = 2;
    while(i < buf.length - 8){
      if(buf[i] !== 0xFF){ i++; continue; }
      var marker = buf[i+1];
      if(marker >= 0xC0 && marker <= 0xC3){
        try { return { height: buf.readUInt16BE(i+5), width: buf.readUInt16BE(i+7) }; } catch(e){ return null; }
      }
      var segLen;
      try { segLen = buf.readUInt16BE(i+2); } catch(e){ break; }
      if(segLen < 2) break;
      i += 2 + segLen;
    }
  }
  return null;
}

// Fetch an image URL and return the raw bytes via cb(err, buffer).
// Follows HTTP redirects (OpenLibrary returns 302 to its actual CDN).
function fetchImageBuffer(imageUrl, cb, redirectsLeft){
  if(typeof redirectsLeft !== 'number') redirectsLeft = 5;
  var parsedUrl;
  try { parsedUrl = require('url').parse(imageUrl); } catch(e){ cb('parse'); return; }
  var lib = parsedUrl.protocol === 'http:' ? require('http') : https;
  var req = lib.request({
    hostname: parsedUrl.hostname,
    path: parsedUrl.path,
    method: 'GET',
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; BFA-Platform)',
      'Accept': 'image/jpeg,image/png,image/*'
    }
  }, function(r){
    // Follow redirects (OpenLibrary does 302 → their actual CDN)
    if([301, 302, 303, 307, 308].indexOf(r.statusCode) !== -1 && r.headers.location){
      if(redirectsLeft <= 0){ cb('too many redirects'); return; }
      // Drain the body then follow
      r.resume();
      var nextUrl = r.headers.location;
      // Handle relative redirects (rare for images but safe to support)
      if(nextUrl.indexOf('://') === -1){
        nextUrl = parsedUrl.protocol + '//' + parsedUrl.hostname + (nextUrl.charAt(0) === '/' ? '' : '/') + nextUrl;
      }
      console.log('[fetchImage] ' + r.statusCode + ' redirect → ' + nextUrl);
      fetchImageBuffer(nextUrl, cb, redirectsLeft - 1);
      return;
    }
    if(r.statusCode !== 200){ cb('HTTP ' + r.statusCode); return; }
    var chunks = [];
    r.on('data', function(c){ chunks.push(c); });
    r.on('end', function(){ try { cb(null, Buffer.concat(chunks)); } catch(e){ cb(e.message); } });
  });
  req.on('error', function(e){ cb(e.message); });
  req.setTimeout(15000, function(){ req.destroy(); cb('timeout'); });
  req.end();
}

// Get a cover image for an eBay listing and host it on eBay's CDN. Tries sources
// in order and uses the first that works:
//   1. OpenLibrary -L (large, ~800px) by ISBN — usually same publisher cover as Amazon
//   2. Amazon's catalog image (suffix stripped for full size)
// If the resulting image is under eBay's 500px-longest-side minimum (common for
// old/obscure books where publishers only uploaded small thumbnails), the URL is
// re-fetched through images.weserv.nl which upscales to 1000px. Zero npm deps.
function rehostCoverForEbay(imageUrl, isbn, userToken, devId, cb){
  if(!imageUrl && !isbn){ cb(null, ''); return; }

  var amazonFull = imageUrl ? imageUrl.replace(/\._[A-Z][A-Z0-9_,]*_(?=\.)/g, '') : '';
  var cleanIsbn = isbn ? String(isbn).replace(/[^0-9X]/gi, '') : '';
  // default=false → OpenLibrary returns 404 instead of a 1x1 placeholder when cover missing
  var openLibUrl = cleanIsbn ? ('https://covers.openlibrary.org/b/isbn/' + cleanIsbn + '-L.jpg?default=false') : null;

  function uploadAndDone(buf, sourceName){
    var b64 = buf.toString('base64');
    uploadPicture(b64, userToken, devId, function(upErr, ebayUrl){
      if(upErr || !ebayUrl){
        console.log('[rehostCover] eBay upload FAILED from ' + sourceName + ': ' + upErr);
        cb(null, '');
        return;
      }
      console.log('[rehostCover] uploaded from ' + sourceName + ' → ' + ebayUrl);
      cb(null, ebayUrl);
    });
  }

  // Fetch src URL, returning a buffer that meets eBay's 500px minimum. If the
  // source is too small, re-fetches through weserv.nl (free image proxy) with
  // w=1000 fit=inside which upscales small images to 1000px longest side.
  // onDone(buf, label) — buf is null if both direct + weserv fail.
  function fetchAndEnsureEbayReady(srcUrl, name, onDone){
    fetchImageBuffer(srcUrl, function(err, buf){
      if(err || !buf){ console.log('[rehostCover] ' + name + ' direct fetch failed: ' + err); onDone(null); return; }
      var dims = getImageDimensions(buf);
      var longest = dims ? Math.max(dims.width, dims.height) : 0;
      console.log('[rehostCover] ' + name + ' image: ' + buf.length + ' bytes, dims=' + (dims ? dims.width+'x'+dims.height : 'unknown'));
      if(dims && longest >= 500){
        onDone(buf, name);
        return;
      }
      // Too small → upscale via weserv.nl. It accepts protocol-stripped URLs and
      // follows redirects internally, so OpenLibrary's 302 chain is handled by them.
      var stripped = srcUrl.replace(/^https?:\/\//, '');
      var proxyUrl = 'https://images.weserv.nl/?url=' + encodeURIComponent(stripped) + '&w=1000&fit=inside&output=jpg';
      console.log('[rehostCover] ' + name + ' under 500px — upscaling via weserv.nl');
      fetchImageBuffer(proxyUrl, function(pErr, pBuf){
        if(pErr || !pBuf){
          console.log('[rehostCover] weserv upscale failed for ' + name + ': ' + pErr);
          onDone(null);
          return;
        }
        var pDims = getImageDimensions(pBuf);
        var pLongest = pDims ? Math.max(pDims.width, pDims.height) : 0;
        console.log('[rehostCover] weserv upscaled ' + name + ': ' + pBuf.length + ' bytes, dims=' + (pDims ? pDims.width+'x'+pDims.height : 'unknown'));
        if(pDims && pLongest < 500){
          console.log('[rehostCover] weserv returned image still under 500px — giving up on this source');
          onDone(null);
          return;
        }
        onDone(pBuf, name + ' via weserv');
      });
    });
  }

  function tryAmazon(){
    if(!amazonFull){ console.log('[rehostCover] no Amazon URL and OpenLibrary failed — imageless listing'); cb(null, ''); return; }
    fetchAndEnsureEbayReady(amazonFull, 'Amazon', function(buf, label){
      if(!buf){ console.log('[rehostCover] all sources exhausted — imageless listing'); cb(null, ''); return; }
      uploadAndDone(buf, label);
    });
  }

  if(openLibUrl){
    fetchAndEnsureEbayReady(openLibUrl, 'OpenLibrary', function(buf, label){
      if(buf){ uploadAndDone(buf, label); return; }
      console.log('[rehostCover] OpenLibrary path exhausted — trying Amazon');
      tryAmazon();
    });
  } else {
    tryAmazon();
  }
}

// ===================== REPRICER ENGINE =====================
// Tracks running cycles per subscriber so we don't double-run
var repricerRunning = {};
// Tracks last cycle start time (for scheduler interval)
var repricerSchedulerInterval = null;

// Sleep helper
function sleep(ms){ return new Promise(function(resolve){ setTimeout(resolve, ms); }); }

// Normalize an Amazon condition string for comparison.
// Amazon sometimes sends "very_good", "VeryGood", "Very Good" etc.
// Our allowed-conditions list uses camelCase like "UsedVeryGood".
// Stripping underscores, spaces, and lowercasing makes all forms comparable.
//   "Usedvery_good" → "usedverygood"
//   "UsedVeryGood"  → "usedverygood"
//   "Used Very Good" → "usedverygood"
function normalizeCondition(s){
  return (s || '').toString().toLowerCase().replace(/[\s_\-]/g, '');
}

// Build the list of allowed conditions given "my" condition + match mode
function allowedConditionsFor(myCondition, matchMode){
  var c = (myCondition || '').toLowerCase();
  if(matchMode === 'strict'){
    if(c === 'new') return ['New'];
    if(c === 'like new' || c === 'likenew') return ['UsedLikeNew'];
    if(c === 'very good' || c === 'verygood') return ['UsedVeryGood'];
    if(c === 'good') return ['UsedGood'];
    if(c === 'acceptable') return ['UsedAcceptable'];
    return [];
  }
  if(matchMode === 'loose'){
    if(c === 'new') return ['New'];
    return ['UsedLikeNew','UsedVeryGood','UsedGood','UsedAcceptable'];
  }
  // smart (default) — each condition competes with itself + all better conditions.
  // Rationale: a worse-condition book can undercut better-condition listings
  // because buyers may still pick it for price; but a New/LikeNew buyer won't
  // typically settle for a worse condition, so we don't look down.
  //   New            → New (stands alone at the top)
  //   Like New       → Like New + New
  //   Very Good      → Very Good + Like New + New
  //   Good           → Good + Very Good + Like New + New
  //   Acceptable     → everything (bottom tier competes with all)
  if(c === 'new')                                    return ['New'];
  if(c === 'like new' || c === 'likenew')            return ['UsedLikeNew','New'];
  if(c === 'very good' || c === 'verygood')          return ['UsedVeryGood','UsedLikeNew','New'];
  if(c === 'good')                                   return ['UsedGood','UsedVeryGood','UsedLikeNew','New'];
  if(c === 'acceptable')                             return ['UsedAcceptable','UsedGood','UsedVeryGood','UsedLikeNew','New'];
  return [];
}

// Apply the configured undercut strategy to a set of competing offers
// offers = [{price, condition, fulfillment, sellerId}] sorted ascending by price
// Returns { targetPrice, reason } or null if no valid target
function calcTargetFromOffers(offers, mySellerId, config){
  var competing = offers.filter(function(o){ return o.sellerId !== mySellerId; });
  // Fulfillment filter
  if(config.fulfillmentFilter === 'fbm-only'){
    competing = competing.filter(function(o){ return o.fulfillment === 'FBM'; });
  }
  // else 'all' — keep everything; 'fbm-plus-fba-above-threshold' requires threshold config which we skip for now
  if(!competing.length) return null;
  competing.sort(function(a,b){ return a.price - b.price; });

  var lowest = competing[0];
  var secondLowest = competing[1];
  var anchor = lowest;
  var reasonPrefix = 'Lowest match';
  if(config.undercutStrategy === 'second-lowest-penny' && secondLowest){
    anchor = secondLowest;
    reasonPrefix = '2nd lowest match';
  }

  var target;
  switch(config.undercutStrategy){
    case 'match':       target = anchor.price; break;
    case 'percent1':    target = anchor.price * 0.99; break;
    case 'percent2':    target = anchor.price * 0.98; break;
    case 'percent5':    target = anchor.price * 0.95; break;
    case 'dollar50':    target = anchor.price - 0.50; break;
    case 'dollar100':   target = anchor.price - 1.00; break;
    case 'second-lowest-penny':
    case 'penny':
    default:            target = anchor.price - 0.01; break;
  }
  // Round to 2 decimals
  target = Math.round(target * 100) / 100;
  return {
    targetPrice: target,
    reason: reasonPrefix + ' $' + anchor.price.toFixed(2) + ' (' + anchor.condition + ', ' + anchor.fulfillment + ')'
  };
}

// Apply guards (floor, direction, 24h rolling drop/increase) to a proposed target.
//
// When isFirstPass is true (book has never been processed), the 24h guards are
// SKIPPED — engine trusts the rules and converges to market in one shot.
// Floor + direction always apply because those are absolute constraints, not
// rate-limits.
//
// 24h ROLLING WINDOW LOGIC
// ------------------------
// The anchor price is the OLDEST `oldPrice` in priceChangeLog within the last
// 24 hours. "Used %" is how far the current price has already dropped (or
// risen) from that anchor. The proposed change is measured from the SAME
// anchor, not from the current price. The guard blocks moves that would push
// total-since-anchor past maxDrop24hPct / maxIncrease24hPct.
//
// If the log is empty or all entries are >24h old, we treat this as a fresh
// window: anchor = current price, used = 0%.
//
// priceChangeLog = array of {at: Date, oldPrice: Number, newPrice: Number}
// Pruning of old entries happens in the caller so applyGuards stays pure.
//
// Returns { finalPrice, skipped, reason, capped: 'drop'|'increase'|null,
//           anchorPrice, windowUsedPct, proposedPct }
function applyGuards(currentPrice, targetPrice, config, isFirstPass, priceChangeLog){
  var floor = config.floorPrice || 0;
  if(targetPrice < floor){
    targetPrice = floor;
  }
  if(targetPrice === currentPrice){
    return { skipped: true, reason: 'Already at target price' };
  }
  // Direction filter
  if(config.direction === 'down' && targetPrice > currentPrice){
    return { skipped: true, reason: 'Direction=down, target higher than current' };
  }
  if(config.direction === 'up' && targetPrice < currentPrice){
    return { skipped: true, reason: 'Direction=up, target lower than current' };
  }

  // 24h rolling window guard. Find anchor from priceChangeLog.
  // Default: anchor = current price, used = 0% (fresh window scenario).
  var nowMs = Date.now();
  var windowMs = 24 * 60 * 60 * 1000;
  var inWindow = (priceChangeLog || []).filter(function(e){
    var at = e.at ? (e.at instanceof Date ? e.at.getTime() : new Date(e.at).getTime()) : 0;
    return (nowMs - at) < windowMs;
  });
  // Oldest entry in window = first chronologically
  inWindow.sort(function(a,b){
    var aa = a.at instanceof Date ? a.at.getTime() : new Date(a.at).getTime();
    var bb = b.at instanceof Date ? b.at.getTime() : new Date(b.at).getTime();
    return aa - bb;
  });
  var anchorPrice = (inWindow.length > 0 && typeof inWindow[0].oldPrice === 'number')
    ? inWindow[0].oldPrice
    : currentPrice;

  var windowUsedPct = 0;
  var proposedPct = 0;
  if(anchorPrice > 0){
    windowUsedPct = ((anchorPrice - currentPrice) / anchorPrice) * 100;  // positive = drop, negative = rise
    proposedPct   = ((anchorPrice - targetPrice)  / anchorPrice) * 100;
  }

  var capped = null;
  if(!isFirstPass && anchorPrice > 0){
    // DROP case: proposedPct > windowUsedPct (further drop from anchor)
    if(targetPrice < currentPrice){
      var dropLimit = config.maxDrop24hPct || 0;
      if(proposedPct > dropLimit){
        // Cap to the budget we have left. New target = anchor × (1 - dropLimit%)
        var cappedPrice = Math.round(anchorPrice * (1 - dropLimit / 100) * 100) / 100;
        // Never cap UP (if cappedPrice > currentPrice we can't drop at all — budget exhausted)
        if(cappedPrice >= currentPrice){
          return {
            skipped: true,
            reason: '24h drop budget exhausted (used ' + windowUsedPct.toFixed(2) + '% of ' + dropLimit + '% from anchor $' + anchorPrice.toFixed(2) + ')',
            anchorPrice: anchorPrice, windowUsedPct: windowUsedPct, proposedPct: proposedPct
          };
        }
        targetPrice = cappedPrice;
        capped = 'drop';
      }
    }
    // RISE case: proposedPct is negative, and |proposedPct| > |windowUsedPct| means rising above anchor
    else if(targetPrice > currentPrice){
      var riseLimit = config.maxIncrease24hPct || 0;
      // For rises we measure rise-from-anchor as a positive number
      var riseFromAnchor = ((targetPrice - anchorPrice) / anchorPrice) * 100;
      if(riseFromAnchor > riseLimit){
        var cappedPriceUp = Math.round(anchorPrice * (1 + riseLimit / 100) * 100) / 100;
        if(cappedPriceUp <= currentPrice){
          return {
            skipped: true,
            reason: '24h increase budget exhausted (anchor $' + anchorPrice.toFixed(2) + ', current already at or above cap $' + cappedPriceUp.toFixed(2) + ')',
            anchorPrice: anchorPrice, windowUsedPct: windowUsedPct, proposedPct: proposedPct
          };
        }
        targetPrice = cappedPriceUp;
        capped = 'increase';
      }
    }
  }

  // After clamping, re-check floor
  if(targetPrice < floor) targetPrice = floor;
  if(targetPrice === currentPrice){
    return { skipped: true, reason: 'After guards, no change needed',
             anchorPrice: anchorPrice, windowUsedPct: windowUsedPct, proposedPct: proposedPct };
  }
  return {
    finalPrice: targetPrice,
    capped: capped,
    anchorPrice: anchorPrice,
    windowUsedPct: windowUsedPct,
    proposedPct: proposedPct
  };
}

// Fetch competing offers from Amazon SP-API Product Pricing for a given ASIN
// Returns [{price, condition, fulfillment, sellerId}]
function fetchAmazonOffers(accessToken, marketplaceId, asin, cb){
  if(!asin) { cb('No ASIN', []); return; }
  var path = '/products/pricing/v0/items/' + encodeURIComponent(asin) + '/offers'
           + '?MarketplaceId=' + marketplaceId
           + '&ItemCondition=Used';
  var opts = {
    hostname: 'sellingpartnerapi-na.amazon.com',
    path: path, method: 'GET',
    headers: { 'x-amz-access-token': accessToken }
  };
  var aReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      try {
        var json = JSON.parse(data);
        if(json.errors && json.errors[0]){
          cb(json.errors[0].message || json.errors[0].code, []);
          return;
        }
        var payload = (json.payload && json.payload.Offers) || [];
        var offers = payload.map(function(o){
          var price = 0;
          if(o.ListingPrice && typeof o.ListingPrice.Amount === 'number') price = o.ListingPrice.Amount;
          var shipping = (o.Shipping && typeof o.Shipping.Amount === 'number') ? o.Shipping.Amount : 0;
          return {
            price: price + shipping, // landed price
            condition: o.SubCondition ? 'Used' + o.SubCondition : (o.ItemCondition || 'Unknown'),
            fulfillment: o.IsFulfilledByAmazon ? 'FBA' : 'FBM',
            sellerId: o.SellerId || ''
          };
        });
        cb(null, offers);
      } catch(e){ cb('Parse error: ' + e.message, []); }
    });
  });
  aReq.on('error', function(e){ cb(e.message, []); });
  aReq.setTimeout(15000, function(){ aReq.destroy(); cb('Timeout', []); });
  aReq.end();
}

// PATCH an Amazon listing price (requires Listings API)
function patchAmazonListingPrice(accessToken, sellerId, sku, newPrice, marketplaceId, cb){
  var body = JSON.stringify({
    productType: 'PRODUCT',
    patches: [{
      op: 'replace',
      path: '/attributes/purchasable_offer',
      value: [{
        marketplace_id: marketplaceId,
        currency: 'USD',
        our_price: [{ schedule: [{ value_with_tax: newPrice.toFixed(2) }] }]
      }]
    }]
  });
  var path = '/listings/2021-08-01/items/' + encodeURIComponent(sellerId) + '/' + encodeURIComponent(sku)
           + '?marketplaceIds=' + marketplaceId;
  var opts = {
    hostname: 'sellingpartnerapi-na.amazon.com',
    path: path, method: 'PATCH',
    headers: {
      'x-amz-access-token': accessToken,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body)
    }
  };
  var aReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      try {
        var json = JSON.parse(data);
        if(json.status === 'ACCEPTED' || json.status === 'VALID' || (json.issues && json.issues.length === 0)){
          cb(null, json);
        } else if(json.errors && json.errors[0]){
          cb(json.errors[0].message || 'Patch failed', json);
        } else {
          cb(null, json);
        }
      } catch(e){ cb('Parse error', null); }
    });
  });
  aReq.on('error', function(e){ cb(e.message, null); });
  aReq.setTimeout(20000, function(){ aReq.destroy(); cb('Timeout', null); });
  aReq.write(body); aReq.end();
}

// Update an eBay listing price via Trading API ReviseItem
function reviseEbayListingPrice(subscriberCode, itemId, newPrice, cb){
  getSubscriber(subscriberCode, function(err, sub){
    if(!sub){ cb('Subscriber not found'); return; }
    var token = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
    var xml = '<?xml version="1.0" encoding="utf-8"?>'
      + '<ReviseItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      +   '<RequesterCredentials><eBayAuthToken>' + token + '</eBayAuthToken></RequesterCredentials>'
      +   '<Item>'
      +     '<ItemID>' + itemId + '</ItemID>'
      +     '<StartPrice>' + newPrice.toFixed(2) + '</StartPrice>'
      +   '</Item>'
      + '</ReviseItemRequest>';
    var opts = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'X-EBAY-API-CALL-NAME': 'ReviseItem',
        'X-EBAY-API-SITEID': '0',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
        'X-EBAY-API-IAF-TOKEN': token,
        'Content-Type': 'text/xml',
        'Content-Length': Buffer.byteLength(xml)
      }
    };
    var eReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        if(data.indexOf('<Ack>Success</Ack>') !== -1 || data.indexOf('<Ack>Warning</Ack>') !== -1){
          cb(null);
        } else {
          var m = data.match(/<ShortMessage>([^<]+)<\/ShortMessage>/);
          cb(m ? m[1] : 'eBay revise failed');
        }
      });
    });
    eReq.on('error', function(e){ cb(e.message); });
    eReq.setTimeout(15000, function(){ eReq.destroy(); cb('Timeout'); });
    eReq.write(xml); eReq.end();
  });
}

// Fetch current Quantity and QuantitySold on an existing eBay listing. Used by the
// multi-qty merge path: when AddItem is rejected as duplicate for a book the seller
// already has live, we need the current Quantity so we can bump it by N (copies being
// added) via ReviseItem. Calls cb(err, { quantity, sold, available }).
function getEbayItemQuantity(subscriberCode, itemId, cb){
  getSubscriber(subscriberCode, function(err, sub){
    if(!sub){ cb('Subscriber not found'); return; }
    var token = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
    var xml = '<?xml version="1.0" encoding="utf-8"?>'
      + '<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      +   '<RequesterCredentials><eBayAuthToken>' + token + '</eBayAuthToken></RequesterCredentials>'
      +   '<ItemID>' + itemId + '</ItemID>'
      +   '<IncludeItemSpecifics>false</IncludeItemSpecifics>'
      + '</GetItemRequest>';
    var opts = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'X-EBAY-API-CALL-NAME': 'GetItem',
        'X-EBAY-API-SITEID': '0',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
        'X-EBAY-API-IAF-TOKEN': token,
        'Content-Type': 'text/xml',
        'Content-Length': Buffer.byteLength(xml)
      }
    };
    var eReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        if(data.indexOf('<Ack>Success</Ack>') === -1 && data.indexOf('<Ack>Warning</Ack>') === -1){
          var m = data.match(/<ShortMessage>([^<]+)<\/ShortMessage>/);
          cb(m ? m[1] : 'GetItem failed');
          return;
        }
        var qtyMatch = data.match(/<Quantity>(\d+)<\/Quantity>/);
        var soldMatch = data.match(/<QuantitySold>(\d+)<\/QuantitySold>/);
        var quantity = qtyMatch ? parseInt(qtyMatch[1], 10) : 0;
        var sold = soldMatch ? parseInt(soldMatch[1], 10) : 0;
        cb(null, { quantity: quantity, sold: sold, available: Math.max(0, quantity - sold) });
      });
    });
    eReq.on('error', function(e){ cb(e.message); });
    eReq.setTimeout(15000, function(){ eReq.destroy(); cb('Timeout'); });
    eReq.write(xml); eReq.end();
  });
}

// Set the total Quantity on an existing eBay listing via ReviseItem. For multi-qty
// merge: set newQuantity = currentQuantity + N (where N is how many new copies the
// user is adding). eBay tracks sold separately (QuantitySold) so setting Quantity=10
// on a listing with 2 already sold results in "8 available" to buyers.
function reviseEbayItemQuantity(subscriberCode, itemId, newQuantity, cb){
  getSubscriber(subscriberCode, function(err, sub){
    if(!sub){ cb('Subscriber not found'); return; }
    var token = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
    var xml = '<?xml version="1.0" encoding="utf-8"?>'
      + '<ReviseItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      +   '<RequesterCredentials><eBayAuthToken>' + token + '</eBayAuthToken></RequesterCredentials>'
      +   '<Item>'
      +     '<ItemID>' + itemId + '</ItemID>'
      +     '<Quantity>' + newQuantity + '</Quantity>'
      +   '</Item>'
      + '</ReviseItemRequest>';
    var opts = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'X-EBAY-API-CALL-NAME': 'ReviseItem',
        'X-EBAY-API-SITEID': '0',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
        'X-EBAY-API-IAF-TOKEN': token,
        'Content-Type': 'text/xml',
        'Content-Length': Buffer.byteLength(xml)
      }
    };
    var eReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        if(data.indexOf('<Ack>Success</Ack>') !== -1 || data.indexOf('<Ack>Warning</Ack>') !== -1){
          cb(null);
        } else {
          var m = data.match(/<ShortMessage>([^<]+)<\/ShortMessage>/);
          cb(m ? m[1] : 'eBay ReviseItem(quantity) failed');
        }
      });
    });
    eReq.on('error', function(e){ cb(e.message); });
    eReq.setTimeout(15000, function(){ eReq.destroy(); cb('Timeout'); });
    eReq.write(xml); eReq.end();
  });
}

// Log a repricing event to MongoDB
function logRepriceHistory(subscriberCode, sku, platform, oldPrice, newPrice, reason, dryRun, skipped, cycleId, diagnostics){
  connectMongo(function(err, database){
    if(err || !database) return;
    var record = {
      subscriberCode: subscriberCode,
      sku: sku,
      platform: platform,
      oldPrice: oldPrice || 0,
      newPrice: newPrice || 0,
      reason: reason || '',
      dryRun: !!dryRun,
      skipped: !!skipped,
      cycleId: cycleId || '',
      createdAt: new Date()
    };
    if(diagnostics) record.diagnostics = diagnostics;
    database.collection('reprice_history').insertOne(record).catch(function(){});
  });
}

// Main cycle runner — async, processes all qualifying warehouse-tool listings
async function runRepricerCycle(subscriberCode, singleSku){
  if(repricerRunning[subscriberCode]){
    console.log('[repricer] Cycle already running for', subscriberCode);
    return;
  }
  repricerRunning[subscriberCode] = true;
  var cycleId = Date.now().toString();
  console.log('[repricer] Starting cycle', cycleId, 'for', subscriberCode, singleSku ? '(single SKU: ' + singleSku + ')' : '(all)');

  try {
    // Load subscriber + config
    var sub = await new Promise(function(resolve){ getSubscriber(subscriberCode, function(err, s){ resolve(s); }); });
    if(!sub){ console.log('[repricer] No sub found'); repricerRunning[subscriberCode] = false; return; }
    var config = Object.assign({
      enabled: false, dryRun: true, direction: 'both', floorPrice: 5.99,
      maxDrop24hPct: 10, maxIncrease24hPct: 20, cycleIntervalHours: 24, ebayDiscountPct: 15,
      conditionMatch: 'smart', undercutStrategy: 'penny', fulfillmentFilter: 'fbm-only',
      excludedSkus: []
    }, sub.repricer || {});

    var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
    var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
    var excludedSet = {};
    (config.excludedSkus || []).forEach(function(s){ excludedSet[s] = true; });

    // Mark start
    var database = await new Promise(function(resolve){ connectMongo(function(err, d){ resolve(d); }); });
    if(!database){ console.log('[repricer] No DB'); repricerRunning[subscriberCode] = false; return; }
    await database.collection('subscribers').updateOne(
      { code: { $regex: new RegExp('^' + subscriberCode.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '$', 'i') } },
      { $set: { 'repricer.lastRunStartedAt': new Date().toISOString(), 'repricer.lastRunCompletedAt': null } }
    );

    // Build query — single SKU mode or all
    // Exclude any book with a "sold" status. The $nin operator matches docs
    // where status field doesn't exist OR status is not in the excluded list.
    var query = {
      code: subscriberCode,
      status: { $nin: ['sold', 'sold-amazon', 'sold-ebay'] }
    };
    if(singleSku) query.sku = singleSku;

    // Fetch listings for this subscriber
    var listings = await database.collection('warehouse_inventory').find(query).toArray();

    console.log('[repricer]', listings.length, 'listings to check for', subscriberCode);

    // Mode transition: if this is a LIVE cycle, wipe simulatedPrice,
    // lastRepriceAttempt, AND priceChangeLog on every SKU in scope. This makes
    // the first live run a genuine first-pass (guards skipped) and the 24h
    // rolling-window starts fresh. Skipped in dry-run to preserve the rehearsal.
    if(!config.dryRun && listings.length){
      var wipeQuery = {
        code: subscriberCode,
        status: { $nin: ['sold', 'sold-amazon', 'sold-ebay'] }
      };
      if(singleSku) wipeQuery.sku = singleSku;
      await database.collection('warehouse_inventory').updateMany(wipeQuery, {
        $unset: { simulatedPrice: '', lastRepriceAttempt: '', priceChangeLog: '' }
      });
      // Reflect the wipe in our in-memory listings so this cycle sees them as
      // first-pass too (without re-fetching).
      listings.forEach(function(l){
        delete l.simulatedPrice;
        delete l.lastRepriceAttempt;
        delete l.priceChangeLog;
      });
      console.log('[repricer] Live mode: wiped simulatedPrice + lastRepriceAttempt + priceChangeLog on ' + listings.length + ' record(s) to force first-pass');
    }

    // Get Amazon access token
    var accessToken = await new Promise(function(resolve, reject){
      getAmazonAccessToken(function(err, tok){ if(err) reject(err); else resolve(tok); });
    });

    var processed = 0, patched = 0, skipped = 0, errors = 0;

    for(var i = 0; i < listings.length; i++){
      var listing = listings[i];
      var sku = listing.sku;
      if(!sku){ continue; }
      if(excludedSet[sku]){
        logRepriceHistory(subscriberCode, sku, 'amazon', listing.price, listing.price, 'Excluded by user', config.dryRun, true, cycleId);
        skipped++; continue;
      }
      if(!listing.asin){
        skipped++; continue;
      }

      processed++;

      // Throttle: 1 request/sec to stay within SP-API Pricing quota
      await sleep(1100);

      try {
        // Fetch offers
        var offers = await new Promise(function(resolve){
          fetchAmazonOffers(accessToken, marketplaceId, listing.asin, function(err, o){
            if(err){ resolve([]); } else { resolve(o); }
          });
        });

        // Filter by allowed conditions. Use normalizeCondition to handle
        // Amazon's inconsistent formatting (e.g. "Usedvery_good" vs "UsedVeryGood").
        var allowedConds = allowedConditionsFor(listing.condition, config.conditionMatch);
        var allowedNorm = allowedConds.map(normalizeCondition);
        var filteredOffers = offers.filter(function(o){
          return allowedNorm.indexOf(normalizeCondition(o.condition)) !== -1;
        });

        // Build diagnostics payload for single-SKU test mode
        var diag = null;
        if(singleSku){
          // Get all competing offers after full filter chain (condition + seller + fulfillment)
          var postFilter = filteredOffers.filter(function(o){ return o.sellerId !== sellerId; });
          if(config.fulfillmentFilter === 'fbm-only'){
            postFilter = postFilter.filter(function(o){ return o.fulfillment === 'FBM'; });
          }
          postFilter.sort(function(a,b){ return a.price - b.price; });
          diag = {
            myAsin: listing.asin,
            mySellerId: sellerId,
            myCondition: listing.condition,
            myPrice: listing.price,
            configMatch: config.conditionMatch,
            configFulfillment: config.fulfillmentFilter,
            configUndercut: config.undercutStrategy,
            configMaxDrop24hPct: config.maxDrop24hPct,
            configMaxIncrease24hPct: config.maxIncrease24hPct,
            configCycleIntervalHours: config.cycleIntervalHours,
            allowedConds: allowedConds,
            totalOffers: offers.length,
            offersAll: offers.slice(0, 30).map(function(o){
              return { price: o.price, condition: o.condition, fulfillment: o.fulfillment, sellerId: o.sellerId, isMe: o.sellerId === sellerId };
            }),
            afterConditionFilter: filteredOffers.length,
            afterAllFilters: postFilter.length,
            competingAfterFilters: postFilter.slice(0, 10).map(function(o){
              return { price: o.price, condition: o.condition, fulfillment: o.fulfillment, sellerId: o.sellerId };
            })
          };
        }

        if(!filteredOffers.length){
          logRepriceHistory(subscriberCode, sku, 'amazon', listing.price || 0, listing.price || 0, 'No matching-condition offers', config.dryRun, true, cycleId, diag);
          skipped++;
          continue;
        }

        var result = calcTargetFromOffers(filteredOffers, sellerId, config);
        if(!result){
          logRepriceHistory(subscriberCode, sku, 'amazon', listing.price || 0, listing.price || 0, 'No competing offers', config.dryRun, true, cycleId, diag);
          skipped++;
          continue;
        }

        // ─── First-pass + simulated-price + 24h-log logic ───
        // effectivePrice: the price we treat as "current" for guard math.
        //   Live mode → always use real price (simulatedPrice is wiped at live
        //   transition, see "mode transition" handling below).
        //   Dry-run → use simulatedPrice if set (carries dry-run trail forward),
        //   otherwise real price.
        // isFirstPass: null lastRepriceAttempt means virgin SKU, 24h guards skipped.
        // priceChangeLog: rolling history of actual changes. We prune entries
        //   older than 24h here so applyGuards sees a clean window.
        var realPrice = parseFloat(listing.price) || 0;
        var simPrice = (typeof listing.simulatedPrice === 'number') ? listing.simulatedPrice : null;
        var effectivePrice = (config.dryRun && simPrice !== null) ? simPrice : realPrice;
        var isFirstPass = !listing.lastRepriceAttempt;

        var nowMs = Date.now();
        var windowMs = 24 * 60 * 60 * 1000;
        var rawLog = Array.isArray(listing.priceChangeLog) ? listing.priceChangeLog : [];
        var prunedLog = rawLog.filter(function(e){
          var at = e.at ? (e.at instanceof Date ? e.at.getTime() : new Date(e.at).getTime()) : 0;
          return (nowMs - at) < windowMs;
        });
        // If pruning changed the array (old entries existed), persist the pruned
        // version so we don't have to re-prune forever.
        var didPrune = prunedLog.length !== rawLog.length;

        var guards = applyGuards(effectivePrice, result.targetPrice, config, isFirstPass, prunedLog);
        if(guards.skipped){
          // Even when skipped, bump lastRepriceAttempt so the NEXT run treats this
          // SKU as subsequent-pass. Persist simulatedPrice in dry-run. If we pruned
          // old log entries, persist the cleaner log too.
          var skipUpdates = { lastRepriceAttempt: new Date() };
          if(config.dryRun) skipUpdates.simulatedPrice = effectivePrice;
          if(didPrune) skipUpdates.priceChangeLog = prunedLog;
          await database.collection('warehouse_inventory').updateOne(
            { code: subscriberCode, sku: sku },
            { $set: skipUpdates }
          );
          // Enrich the diagnostic payload with pass-tracking fields.
          if(diag){
            diag.firstPass = isFirstPass;
            diag.effectivePriceUsed = effectivePrice;
            diag.simulatedPriceBeforeRun = simPrice;
            diag.anchorPrice = (typeof guards.anchorPrice === 'number') ? guards.anchorPrice : null;
            diag.windowUsedPct = (typeof guards.windowUsedPct === 'number') ? guards.windowUsedPct : null;
            diag.proposedPct = (typeof guards.proposedPct === 'number') ? guards.proposedPct : null;
            diag.priceLogInWindow = prunedLog.length;
          }
          logRepriceHistory(subscriberCode, sku, 'amazon', effectivePrice, effectivePrice, result.reason + ' · ' + guards.reason, config.dryRun, true, cycleId, diag);
          skipped++;
          continue;
        }

        var finalAmazonPrice = guards.finalPrice;
        // Enrich diagnostic with pass-tracking + guard outcome.
        if(diag){
          diag.firstPass = isFirstPass;
          diag.effectivePriceUsed = effectivePrice;
          diag.simulatedPriceBeforeRun = simPrice;
          diag.cappedBy = guards.capped || null;
          diag.anchorPrice = guards.anchorPrice;
          diag.windowUsedPct = guards.windowUsedPct;
          diag.proposedPct = guards.proposedPct;
          diag.priceLogInWindow = prunedLog.length;
        }

        // Build the reason string so log rows tell the story. Examples:
        //   "Lowest match $6.95 · firstPass (no 24h cap)"
        //   "Lowest match $6.95 · Capped by maxDrop24hPct 10% · anchor $30.00"
        var passLabel = isFirstPass ? 'firstPass (no 24h cap)' : null;
        var capLabel = null;
        if(guards.capped === 'drop') capLabel = 'Capped by maxDrop24hPct ' + (config.maxDrop24hPct||0) + '% · anchor $' + (guards.anchorPrice||0).toFixed(2);
        else if(guards.capped === 'increase') capLabel = 'Capped by maxIncrease24hPct ' + (config.maxIncrease24hPct||0) + '% · anchor $' + (guards.anchorPrice||0).toFixed(2);
        var reasonFull = result.reason
          + (passLabel ? ' · ' + passLabel : '')
          + (capLabel  ? ' · ' + capLabel  : '');

        // New entry to push into the rolling log (regardless of dry vs live so
        // dry-run rehearsals are also window-aware).
        var logEntry = { at: new Date(), oldPrice: effectivePrice, newPrice: finalAmazonPrice };
        var newLog = prunedLog.concat([logEntry]);

        // Amazon patch (live only) + MongoDB update
        if(!config.dryRun){
          await new Promise(function(resolve){
            patchAmazonListingPrice(accessToken, sellerId, sku, finalAmazonPrice, marketplaceId, function(err){
              resolve();
            });
          });
          // Live: actual price changed. Clear simulatedPrice (no longer relevant).
          await database.collection('warehouse_inventory').updateOne(
            { code: subscriberCode, sku: sku },
            {
              $set: {
                price: finalAmazonPrice,
                lastRepriced: new Date(),
                lastRepriceAttempt: new Date(),
                priceChangeLog: newLog
              },
              $unset: { simulatedPrice: '' }
            }
          );
        } else {
          // Dry-run: bump lastRepriceAttempt, save simulatedPrice, and push the
          // rehearsal entry into priceChangeLog so the 24h guard sees it next cycle.
          await database.collection('warehouse_inventory').updateOne(
            { code: subscriberCode, sku: sku },
            { $set: {
                simulatedPrice: finalAmazonPrice,
                lastRepriceAttempt: new Date(),
                priceChangeLog: newLog
            } }
          );
        }
        logRepriceHistory(subscriberCode, sku, 'amazon', effectivePrice, finalAmazonPrice, reasonFull, config.dryRun, false, cycleId, diag);

        // eBay mirror price — Amazon price × (1 - ebayDiscountPct/100)
        var ebayPrice = Math.round(finalAmazonPrice * (1 - (config.ebayDiscountPct || 0) / 100) * 100) / 100;
        if(ebayPrice < config.floorPrice) ebayPrice = config.floorPrice;
        var currentEbayPrice = parseFloat(listing.ebayPrice || listing.price) || 0;
        if(Math.abs(ebayPrice - currentEbayPrice) >= 0.01 && listing.ebayItemId){
          if(!config.dryRun){
            await new Promise(function(resolve){
              reviseEbayListingPrice(subscriberCode, listing.ebayItemId, ebayPrice, function(err){ resolve(); });
            });
            await database.collection('warehouse_inventory').updateOne(
              { code: subscriberCode, sku: sku },
              { $set: { ebayPrice: ebayPrice } }
            );
          }
          logRepriceHistory(subscriberCode, sku, 'ebay', currentEbayPrice, ebayPrice, 'Mirror of Amazon price (-' + (config.ebayDiscountPct||0) + '%)', config.dryRun, false, cycleId);
        }

        patched++;
      } catch(e){
        console.log('[repricer] SKU', sku, 'error:', e.message);
        errors++;
      }
    }

    // Mark complete
    var summary = { processed: processed, patched: patched, skipped: skipped, errors: errors, total: listings.length };
    await database.collection('subscribers').updateOne(
      { code: { $regex: new RegExp('^' + subscriberCode.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '$', 'i') } },
      { $set: { 'repricer.lastRunCompletedAt': new Date().toISOString(), 'repricer.lastRunSummary': summary } }
    );
    console.log('[repricer] Cycle done:', JSON.stringify(summary));
  } catch(e){
    console.log('[repricer] Cycle error:', e.message);
  } finally {
    repricerRunning[subscriberCode] = false;
  }
}

// Scheduler — checks every hour if any subscriber needs a cycle based on their interval
function startRepricerScheduler(){
  if(repricerSchedulerInterval) return;
  repricerSchedulerInterval = setInterval(function(){
    connectMongo(function(err, database){
      if(err || !database) return;
      database.collection('subscribers').find({ 'repricer.enabled': true }).toArray()
        .then(function(subs){
          subs.forEach(function(sub){
            var cfg = sub.repricer || {};
            if(!cfg.enabled) return;
            // Back-compat: accept old cycleIntervalDays config, convert to hours
            var hours = (typeof cfg.cycleIntervalHours === 'number' && cfg.cycleIntervalHours > 0)
              ? cfg.cycleIntervalHours
              : ((typeof cfg.cycleIntervalDays === 'number' && cfg.cycleIntervalDays > 0) ? cfg.cycleIntervalDays * 24 : 24);
            var intervalMs = hours * 60 * 60 * 1000;
            var lastStart = cfg.lastRunStartedAt ? new Date(cfg.lastRunStartedAt).getTime() : 0;
            if(Date.now() - lastStart >= intervalMs){
              console.log('[repricer] Scheduler triggering cycle for', sub.code, '(every ' + hours + 'h)');
              runRepricerCycle(sub.code.toUpperCase());
            }
          });
        })
        .catch(function(){});
    });
  }, 15 * 60 * 1000); // every 15 min (so a 5h cycle interval triggers within ~5:15)
  console.log('[repricer] Scheduler started');
}

// Kick off scheduler on startup
setTimeout(startRepricerScheduler, 10000);

// ===================== EBAY ITEM ID ENRICH =====================
var ebayEnrichRunning = {};
var ebayEnrichStatus = {};

// Fetch one page of eBay active listings via GetMyeBaySelling
function fetchEbayActiveListingsPage(token, pageNum, entriesPerPage, cb){
  var xml = '<?xml version="1.0" encoding="utf-8"?>'
    + '<GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    +   '<RequesterCredentials><eBayAuthToken>' + token + '</eBayAuthToken></RequesterCredentials>'
    +   '<ActiveList>'
    +     '<Include>true</Include>'
    +     '<Pagination>'
    +       '<EntriesPerPage>' + entriesPerPage + '</EntriesPerPage>'
    +       '<PageNumber>' + pageNum + '</PageNumber>'
    +     '</Pagination>'
    +   '</ActiveList>'
    +   '<DetailLevel>ReturnAll</DetailLevel>'
    + '</GetMyeBaySellingRequest>';
  var opts = {
    hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
    headers: {
      'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
      'X-EBAY-API-IAF-TOKEN': token,
      'Content-Type': 'text/xml',
      'Content-Length': Buffer.byteLength(xml)
    }
  };
  var eReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      // Extract all <Item> blocks under ActiveList
      var items = [];
      var activeMatch = data.match(/<ActiveList>[\s\S]*?<\/ActiveList>/);
      var section = activeMatch ? activeMatch[0] : '';
      var re = /<Item>([\s\S]*?)<\/Item>/g;
      var m;
      while((m = re.exec(section)) !== null){
        var block = m[1];
        var idMatch = block.match(/<ItemID>([^<]+)<\/ItemID>/);
        var skuMatch = block.match(/<SKU>([^<]+)<\/SKU>/);
        if(idMatch){
          items.push({
            itemId: idMatch[1],
            sku: skuMatch ? skuMatch[1] : ''
          });
        }
      }
      var totalPagesMatch = section.match(/<TotalNumberOfPages>(\d+)<\/TotalNumberOfPages>/);
      var totalPages = totalPagesMatch ? parseInt(totalPagesMatch[1]) : 1;
      var totalEntriesMatch = section.match(/<TotalNumberOfEntries>(\d+)<\/TotalNumberOfEntries>/);
      var totalEntries = totalEntriesMatch ? parseInt(totalEntriesMatch[1]) : items.length;
      cb(null, { items: items, totalPages: totalPages, totalEntries: totalEntries });
    });
  });
  eReq.on('error', function(e){ cb(e.message); });
  eReq.setTimeout(30000, function(){ eReq.destroy(); cb('Timeout'); });
  eReq.write(xml); eReq.end();
}

async function runEbayEnrich(subscriberCode){
  if(ebayEnrichRunning[subscriberCode]) return;
  ebayEnrichRunning[subscriberCode] = true;
  ebayEnrichStatus[subscriberCode] = {
    running: true, startedAt: new Date().toISOString(),
    page: 0, totalPages: null, matched: 0, noSku: 0, unmatched: 0
  };
  console.log('[ebay-enrich] Starting for', subscriberCode);

  try {
    var sub = await new Promise(function(resolve){ getSubscriber(subscriberCode, function(err, s){ resolve(s); }); });
    if(!sub){ ebayEnrichStatus[subscriberCode] = { running: false, error: 'Subscriber not found' }; ebayEnrichRunning[subscriberCode] = false; return; }
    var token = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
    if(!token){ ebayEnrichStatus[subscriberCode] = { running: false, error: 'No eBay token configured' }; ebayEnrichRunning[subscriberCode] = false; return; }

    var database = await new Promise(function(resolve){ connectMongo(function(err, d){ resolve(d); }); });
    if(!database){ ebayEnrichStatus[subscriberCode] = { running: false, error: 'DB unavailable' }; ebayEnrichRunning[subscriberCode] = false; return; }

    var entriesPerPage = 200;
    var page = 1;
    var totalPages = null;
    var matched = 0, noSku = 0, unmatched = 0;

    while(true){
      // Fetch page
      var pageData = await new Promise(function(resolve, reject){
        fetchEbayActiveListingsPage(token, page, entriesPerPage, function(err, d){
          if(err) reject(new Error(err));
          else resolve(d);
        });
      });

      if(totalPages === null){ totalPages = pageData.totalPages; }
      ebayEnrichStatus[subscriberCode].page = page;
      ebayEnrichStatus[subscriberCode].totalPages = totalPages;

      // Process each item
      for(var i=0; i<pageData.items.length; i++){
        var it = pageData.items[i];
        if(!it.sku){ noSku++; continue; }
        var result = await database.collection('warehouse_inventory').updateOne(
          { code: subscriberCode, sku: it.sku },
          { $set: { ebayItemId: it.itemId, ebayEnrichedAt: new Date() } }
        );
        if(result.matchedCount > 0){ matched++; } else { unmatched++; }
      }

      ebayEnrichStatus[subscriberCode].matched = matched;
      ebayEnrichStatus[subscriberCode].noSku = noSku;
      ebayEnrichStatus[subscriberCode].unmatched = unmatched;

      // Throttle between pages (eBay ~5k calls/day limit, pace safely)
      await sleep(1200);

      if(page >= totalPages) break;
      page++;
    }

    ebayEnrichStatus[subscriberCode] = Object.assign({}, ebayEnrichStatus[subscriberCode], {
      running: false,
      completedAt: new Date().toISOString()
    });
    console.log('[ebay-enrich] Done: matched=' + matched + ' noSku=' + noSku + ' unmatched=' + unmatched);
  } catch(e){
    console.log('[ebay-enrich] Error:', e.message);
    ebayEnrichStatus[subscriberCode] = Object.assign({}, ebayEnrichStatus[subscriberCode] || {}, {
      running: false, error: e.message
    });
  } finally {
    ebayEnrichRunning[subscriberCode] = false;
  }
}

// ===================== CROSS-PLATFORM SYNC ENGINE =====================
// Polls every 2 min for new orders on Amazon and eBay.
// When a sale is detected on one platform, ends/deletes the twin on the other.
var syncRunning = {};           // Locks per subscriber
var syncSchedulerInterval = null;

// Fetch recent Amazon orders created after a timestamp (MFN only)
function fetchRecentAmazonOrders(accessToken, marketplaceId, sinceIso, cb){
  var qp = 'MarketplaceIds=' + marketplaceId
         + '&CreatedAfter=' + encodeURIComponent(sinceIso);
  var opts = {
    hostname: 'sellingpartnerapi-na.amazon.com',
    path: '/orders/v0/orders?' + qp,
    method: 'GET',
    headers: { 'x-amz-access-token': accessToken }
  };
  var aReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      try {
        var json = JSON.parse(data);
        if(json.errors && json.errors[0]){
          cb(json.errors[0].message, []);
          return;
        }
        var rawOrders = ((json.payload && json.payload.Orders) || []);
        var orders = rawOrders.filter(function(o){
          return o.FulfillmentChannel === 'MFN';
        });
        var channels = rawOrders.map(function(o){ return o.FulfillmentChannel || 'null'; });
        cb(null, orders, { rawCount: rawOrders.length, channels: channels });
      } catch(e){ cb('Parse error', []); }
    });
  });
  aReq.on('error', function(e){ cb(e.message, []); });
  aReq.setTimeout(20000, function(){ aReq.destroy(); cb('Timeout', []); });
  aReq.end();
}

// ─────────────────────────────────────────────────────────────
// SHARED AMAZON ORDERS CACHE
// One source of truth for "recent Amazon orders for this subscriber."
// Used by: sync cycle, pick list, sales today.
// TTL: 15 min — aligned with pick list / sales cache.
// ─────────────────────────────────────────────────────────────
var SHARED_AMZ_ORDERS_TTL = 15 * 60 * 1000;
var SHARED_AMZ_ORDERS_LOOKBACK_DAYS = 7;

function getRecentAmazonOrdersShared(subscriberCode, sub, cb){
  if(!global._sharedAmzOrders) global._sharedAmzOrders = {};
  if(!global._sharedAmzOrdersInFlight) global._sharedAmzOrdersInFlight = {};
  var cacheEntry = global._sharedAmzOrders[subscriberCode];
  var nowMs = Date.now();
  if(cacheEntry && (nowMs - cacheEntry.ts) < SHARED_AMZ_ORDERS_TTL){
    cb(null, cacheEntry.orders, { cached: true, ts: cacheEntry.ts });
    return;
  }

  // In-flight dedup: if another request is already fetching, queue this callback
  var inFlight = global._sharedAmzOrdersInFlight[subscriberCode];
  if(inFlight){
    inFlight.callbacks.push(cb);
    return;
  }
  global._sharedAmzOrdersInFlight[subscriberCode] = { callbacks: [cb] };

  function resolveAll(err, orders, info){
    var queue = global._sharedAmzOrdersInFlight[subscriberCode];
    delete global._sharedAmzOrdersInFlight[subscriberCode];
    if(queue && queue.callbacks){
      queue.callbacks.forEach(function(c){
        try { c(err, orders, info); } catch(e){}
      });
    }
  }

  // Cache miss — fetch fresh
  var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
  var sinceIso = new Date(Date.now() - SHARED_AMZ_ORDERS_LOOKBACK_DAYS * 24 * 60 * 60 * 1000).toISOString();

  getAmazonAccessToken(function(tokenErr, accessToken){
    if(tokenErr){
      if(cacheEntry){ resolveAll(null, cacheEntry.orders, { stale: true, ts: cacheEntry.ts, error: 'auth: ' + tokenErr }); return; }
      resolveAll(tokenErr, [], null);
      return;
    }
    // Paginated fetch — NextToken support
    var allOrders = [];
    function fetchPage(nextToken){
      var qp = 'MarketplaceIds=' + marketplaceId + '&CreatedAfter=' + encodeURIComponent(sinceIso);
      if(nextToken) qp += '&NextToken=' + encodeURIComponent(nextToken);
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: '/orders/v0/orders?' + qp,
        method: 'GET',
        headers: { 'x-amz-access-token': accessToken }
      };
      var aReq = https.request(opts, function(r){
        var data = '';
        r.on('data', function(c){ data += c; });
        r.on('end', function(){
          try {
            var json = JSON.parse(data);
            if(json.errors && json.errors[0]){
              var errMsg = json.errors[0].message || json.errors[0].code;
              if(cacheEntry){ resolveAll(null, cacheEntry.orders, { stale: true, ts: cacheEntry.ts, error: errMsg }); return; }
              resolveAll(errMsg, [], null);
              return;
            }
            var payload = json.payload || {};
            var pageOrders = (payload.Orders || []);
            allOrders = allOrders.concat(pageOrders);
            if(payload.NextToken){ fetchPage(payload.NextToken); return; }
            // Done. Save to cache.
            global._sharedAmzOrders[subscriberCode] = { ts: Date.now(), orders: allOrders };
            resolveAll(null, allOrders, { cached: false, fresh: true });
          } catch(e){
            if(cacheEntry){ resolveAll(null, cacheEntry.orders, { stale: true, ts: cacheEntry.ts, error: 'parse' }); return; }
            resolveAll('Parse error', [], null);
          }
        });
      });
      aReq.on('error', function(e){
        if(cacheEntry){ resolveAll(null, cacheEntry.orders, { stale: true, ts: cacheEntry.ts, error: e.message }); return; }
        resolveAll(e.message, [], null);
      });
      aReq.setTimeout(20000, function(){
        aReq.destroy();
        if(cacheEntry){ resolveAll(null, cacheEntry.orders, { stale: true, ts: cacheEntry.ts, error: 'timeout' }); return; }
        resolveAll('Timeout', [], null);
      });
      aReq.end();
    }
    fetchPage(null);
  });
}

// Get orders from shared cache filtered by CreatedAfter timestamp
function getAmazonOrdersSince(subscriberCode, sub, sinceIso, cb){
  getRecentAmazonOrdersShared(subscriberCode, sub, function(err, orders, info){
    if(err){ cb(err, [], info); return; }
    var filtered = (orders || []).filter(function(o){
      return o.PurchaseDate && o.PurchaseDate >= sinceIso;
    });
    cb(null, filtered, info);
  });
}

// Fetch items in an Amazon order (returns [{sku, quantity}])
function fetchAmazonOrderItems(accessToken, orderId, cb){
  var opts = {
    hostname: 'sellingpartnerapi-na.amazon.com',
    path: '/orders/v0/orders/' + encodeURIComponent(orderId) + '/orderItems',
    method: 'GET',
    headers: { 'x-amz-access-token': accessToken }
  };
  var aReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      try {
        var json = JSON.parse(data);
        if(json.errors && json.errors[0]){ cb(json.errors[0].message, []); return; }
        var items = ((json.payload && json.payload.OrderItems) || []).map(function(it){
          return { sku: it.SellerSKU, quantity: it.QuantityOrdered || 1 };
        });
        cb(null, items);
      } catch(e){ cb('Parse error', []); }
    });
  });
  aReq.on('error', function(e){ cb(e.message, []); });
  aReq.setTimeout(15000, function(){ aReq.destroy(); cb('Timeout', []); });
  aReq.end();
}

// Fetch eBay orders via GetOrders since a timestamp
function fetchRecentEbayOrders(subscriberCode, sinceIso, cb){
  getSubscriber(subscriberCode, function(err, sub){
    if(!sub){ cb('Subscriber not found'); return; }
    var token = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
    if(!token){ cb('No eBay token'); return; }
    var xml = '<?xml version="1.0" encoding="utf-8"?>'
      + '<GetOrdersRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      +   '<RequesterCredentials><eBayAuthToken>' + token + '</eBayAuthToken></RequesterCredentials>'
      +   '<CreateTimeFrom>' + sinceIso + '</CreateTimeFrom>'
      +   '<CreateTimeTo>' + new Date().toISOString() + '</CreateTimeTo>'
      +   '<OrderStatus>All</OrderStatus>'
      +   '<DetailLevel>ReturnAll</DetailLevel>'
      + '</GetOrdersRequest>';
    var opts = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'X-EBAY-API-CALL-NAME': 'GetOrders',
        'X-EBAY-API-SITEID': '0',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
        'X-EBAY-API-IAF-TOKEN': token,
        'Content-Type': 'text/xml',
        'Content-Length': Buffer.byteLength(xml)
      }
    };
    var eReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        // Extract orders → transactions → SKU
        var orders = [];
        var reOrder = /<Order>([\s\S]*?)<\/Order>/g;
        var m;
        while((m = reOrder.exec(data)) !== null){
          var block = m[1];
          var orderIdMatch = block.match(/<OrderID>([^<]+)<\/OrderID>/);
          var orderId = orderIdMatch ? orderIdMatch[1] : null;
          var reTx = /<Transaction>([\s\S]*?)<\/Transaction>/g;
          var tx;
          while((tx = reTx.exec(block)) !== null){
            var txBlock = tx[1];
            var skuMatch = txBlock.match(/<SKU>([^<]+)<\/SKU>/);
            var itemIdMatch = txBlock.match(/<ItemID>([^<]+)<\/ItemID>/);
            var qtyMatch = txBlock.match(/<QuantityPurchased>(\d+)<\/QuantityPurchased>/);
            if(skuMatch){
              orders.push({
                orderId: orderId,
                sku: skuMatch[1],
                itemId: itemIdMatch ? itemIdMatch[1] : null,
                quantity: qtyMatch ? parseInt(qtyMatch[1]) : 1
              });
            }
          }
        }
        cb(null, orders);
      });
    });
    eReq.on('error', function(e){ cb(e.message, []); });
    eReq.setTimeout(30000, function(){ eReq.destroy(); cb('Timeout', []); });
    eReq.write(xml); eReq.end();
  });
}

// End an eBay listing by ItemID via Trading API EndItem
function endEbayListing(subscriberCode, itemId, cb){
  getSubscriber(subscriberCode, function(err, sub){
    if(!sub){ cb('Subscriber not found'); return; }
    var token = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
    var xml = '<?xml version="1.0" encoding="utf-8"?>'
      + '<EndItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      +   '<RequesterCredentials><eBayAuthToken>' + token + '</eBayAuthToken></RequesterCredentials>'
      +   '<ItemID>' + itemId + '</ItemID>'
      +   '<EndingReason>NotAvailable</EndingReason>'
      + '</EndItemRequest>';
    var opts = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'X-EBAY-API-CALL-NAME': 'EndItem',
        'X-EBAY-API-SITEID': '0',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
        'X-EBAY-API-IAF-TOKEN': token,
        'Content-Type': 'text/xml',
        'Content-Length': Buffer.byteLength(xml)
      }
    };
    var eReq = https.request(opts, function(r){
      var data = '';
      r.on('data', function(c){ data += c; });
      r.on('end', function(){
        if(data.indexOf('<Ack>Success</Ack>') !== -1 || data.indexOf('<Ack>Warning</Ack>') !== -1){
          cb(null);
          return;
        }
        // Check if the error is "already ended" or similar — treat as success
        var errCodeMatch = data.match(/<ErrorCode>([^<]+)<\/ErrorCode>/);
        var msgMatch = data.match(/<ShortMessage>([^<]+)<\/ShortMessage>/);
        var longMsgMatch = data.match(/<LongMessage>([^<]+)<\/LongMessage>/);
        var errCode = errCodeMatch ? errCodeMatch[1] : '';
        var errMsg = msgMatch ? msgMatch[1] : 'eBay EndItem failed';
        var longMsg = longMsgMatch ? longMsgMatch[1] : '';
        var combined = (errMsg + ' ' + longMsg).toLowerCase();
        // Common eBay "already ended" indicators
        var alreadyGone = (
          errCode === '1047' ||                          // Auction has already ended
          errCode === '291' ||                           // Item cannot be accessed
          combined.indexOf('already ended') !== -1 ||
          combined.indexOf('already been ended') !== -1 ||
          combined.indexOf('does not exist') !== -1 ||
          combined.indexOf('invalid item id') !== -1
        );
        if(alreadyGone){
          cb(null, { alreadyGone: true, reason: errMsg });
          return;
        }
        cb(errMsg);
      });
    });
    eReq.on('error', function(e){ cb(e.message); });
    eReq.setTimeout(15000, function(){ eReq.destroy(); cb('Timeout'); });
    eReq.write(xml); eReq.end();
  });
}

// Delete an Amazon listing (Listings API DELETE)
function deleteAmazonListing(accessToken, sellerId, sku, marketplaceId, cb){
  var opts = {
    hostname: 'sellingpartnerapi-na.amazon.com',
    path: '/listings/2021-08-01/items/' + encodeURIComponent(sellerId) + '/' + encodeURIComponent(sku)
         + '?marketplaceIds=' + marketplaceId,
    method: 'DELETE',
    headers: { 'x-amz-access-token': accessToken }
  };
  var aReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      if(r.statusCode >= 200 && r.statusCode < 300){ cb(null); return; }
      // 403/404 MAY mean the listing is already gone, OR it may mean a real permission issue.
      // Verify by GET-ing the listing and checking if it exists.
      if(r.statusCode === 403 || r.statusCode === 404){
        verifyAmazonListingGone(accessToken, sellerId, sku, marketplaceId, r.statusCode, function(verifyInfo){
          cb(null, verifyInfo);
        });
        return;
      }
      try {
        var json = JSON.parse(data);
        cb(json.errors && json.errors[0] ? json.errors[0].message : ('HTTP ' + r.statusCode));
      } catch(e){ cb('HTTP ' + r.statusCode); }
    });
  });
  aReq.on('error', function(e){ cb(e.message); });
  aReq.setTimeout(15000, function(){ aReq.destroy(); cb('Timeout'); });
  aReq.end();
}

// Verify a listing is actually gone from Amazon by GET-ing it.
// Returns verification info to attach to the sync log.
function verifyAmazonListingGone(accessToken, sellerId, sku, marketplaceId, deleteStatusCode, cb){
  var opts = {
    hostname: 'sellingpartnerapi-na.amazon.com',
    path: '/listings/2021-08-01/items/' + encodeURIComponent(sellerId) + '/' + encodeURIComponent(sku)
         + '?marketplaceIds=' + marketplaceId,
    method: 'GET',
    headers: { 'x-amz-access-token': accessToken }
  };
  var gReq = https.request(opts, function(r){
    var data = '';
    r.on('data', function(c){ data += c; });
    r.on('end', function(){
      // If GET returns 404, listing is truly gone — confirmed alreadyGone.
      if(r.statusCode === 404){
        cb({ alreadyGone: true, statusCode: deleteStatusCode, verified: true, verifiedGone: true });
        return;
      }
      // If GET returns 200, listing EXISTS — the 403 was a permissions/other issue.
      if(r.statusCode >= 200 && r.statusCode < 300){
        // Parse to check if it has status indicating it's ended or active
        try {
          var json = JSON.parse(data);
          var status = (json.summaries && json.summaries[0] && json.summaries[0].status) || [];
          var isActive = status.some(function(s){ return s.toUpperCase && s.toUpperCase() !== 'DISCOVERABLE' && s.toUpperCase() !== 'BUYABLE' && s.toUpperCase() === 'BUYABLE'; });
          // If the listing exists at all, flag as "exists - not actually gone"
          cb({ alreadyGone: false, statusCode: deleteStatusCode, verified: true, verifiedGone: false, verifyNote: 'Listing still exists on Amazon despite 403 on delete' });
        } catch(e){
          cb({ alreadyGone: false, statusCode: deleteStatusCode, verified: true, verifiedGone: false, verifyNote: 'Listing still exists on Amazon (parse error)' });
        }
        return;
      }
      // 403 on GET also — in practice this means the SKU is deleted.
      // Amazon's Listings API returns 403 (not 404) for deleted SKUs.
      if(r.statusCode === 403){
        cb({ alreadyGone: true, statusCode: deleteStatusCode, verified: true, verifiedGone: true, verifyNote: 'confirmed deleted (GET:403)' });
        return;
      }
      cb({ alreadyGone: true, statusCode: deleteStatusCode, verified: false, verifyNote: 'GET returned ' + r.statusCode });
    });
  });
  gReq.on('error', function(){ cb({ alreadyGone: true, statusCode: deleteStatusCode, verified: false, verifyNote: 'GET verification failed' }); });
  gReq.setTimeout(10000, function(){ gReq.destroy(); cb({ alreadyGone: true, statusCode: deleteStatusCode, verified: false, verifyNote: 'GET verification timeout' }); });
  gReq.end();
}

// Log a sync action
function logSyncAction(subscriberCode, record){
  connectMongo(function(err, database){
    if(err || !database) return;
    database.collection('sync_log').insertOne(Object.assign({
      subscriberCode: subscriberCode,
      createdAt: new Date()
    }, record)).catch(function(){});
  });
}

// Main sync cycle for one subscriber
async function runSyncCycle(subscriberCode){
  if(syncRunning[subscriberCode]) return;
  syncRunning[subscriberCode] = true;

  try {
    var sub = await new Promise(function(resolve){ getSubscriber(subscriberCode, function(err, s){ resolve(s); }); });
    if(!sub){ syncRunning[subscriberCode] = false; return; }
    if(!sub.sync || !sub.sync.enabled){ syncRunning[subscriberCode] = false; return; }

    var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
    var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
    var database = await new Promise(function(resolve){ connectMongo(function(err, d){ resolve(d); }); });
    if(!database){ syncRunning[subscriberCode] = false; return; }

    // Determine "since" timestamp for eBay side only (Amazon sync no longer
    // filters by timestamp — see Option B comment below).
    var lastEbay = sub.sync.lastEbayCheckedAt ? new Date(sub.sync.lastEbayCheckedAt) : new Date(Date.now() - 10*60*1000);
    var ebaySince = new Date(lastEbay.getTime()).toISOString();

    // HEARTBEAT: log every single sync cycle so we can confirm sync is actually running
    logSyncAction(subscriberCode, {
      sku: '_debug_',
      soldPlatform: 'amazon',
      action: 'sync-cycle-start',
      reason: 'ebaySince=' + ebaySince + ' (amazon=full-cache-dedup)',
      success: true
    });

    // ── AMAZON SIDE — Option B: full-cache scan with dedup ──
    // We used to filter Amazon orders by a "since" timestamp (last time sync
    // successfully ran). That created a race condition: if the shared cache
    // was stale when a new order arrived, the "since" timestamp would advance
    // past the order's creation time on the next cycle, and we'd never see it.
    //
    // New approach: pull the full 7-day shared cache every cycle (no timestamp
    // filter), then rely on sync_log dedup (processedAmazonSet) to skip orders
    // we've already handled. Cost: a few extra MongoDB lookups per cycle. Gain:
    // cannot miss an order due to cache/timestamp race.
    var amazonFetchErr = null;
    var amazonAllOrders = await new Promise(function(resolve){
      // Pass sinceIso = 7 days ago so filter becomes no-op (cache already holds 7d).
      var amazonSinceFull = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      getAmazonOrdersSince(subscriberCode, sub, amazonSinceFull, function(err, orders, info){
        if(err) amazonFetchErr = err;
        if(info && info.stale && info.error) amazonFetchErr = info.error;
        resolve(err ? [] : (orders || []));
      });
    });
    // Filter to MFN only (sync only processes orders Amazon doesn't ship for us)
    var amazonOrders = amazonAllOrders.filter(function(o){ return o.FulfillmentChannel === 'MFN'; });

    // Only log on real errors — not every heartbeat
    if(amazonFetchErr){
      logSyncAction(subscriberCode, {
        sku: '_error_',
        soldPlatform: 'amazon',
        action: 'amazon-fetch-error',
        reason: amazonFetchErr,
        success: false
      });
    }

    // Get Amazon access token for orderItems calls (only if we have orders to enrich)
    var accessToken = null;
    if(amazonOrders.length > 0){
      try {
        accessToken = await new Promise(function(resolve, reject){
          getAmazonAccessToken(function(err, tok){ if(err) reject(err); else resolve(tok); });
        });
      } catch(e){
        accessToken = null;
      }
    }

    // Track already-processed Amazon order IDs so we don't re-end eBay listings
    var processedAmazon = await database.collection('sync_log').find({
      subscriberCode: subscriberCode,
      soldPlatform: 'amazon',
      amazonOrderId: { $in: amazonOrders.map(function(o){ return o.AmazonOrderId; }) }
    }).project({ amazonOrderId: 1 }).toArray();
    var processedAmazonSet = {};
    processedAmazon.forEach(function(p){ processedAmazonSet[p.amazonOrderId] = true; });

    // DIAGNOSTIC: log what sync sees on Amazon side every cycle (always)
    var newOrders = amazonOrders.filter(function(o){ return !processedAmazonSet[o.AmazonOrderId]; });
    logSyncAction(subscriberCode, {
      sku: '_debug_',
      soldPlatform: 'amazon',
      action: 'amazon-orders-seen',
      reason: 'total=' + amazonAllOrders.length
            + ' mfn=' + amazonOrders.length
            + ' already-processed=' + processedAmazon.length
            + ' new=' + newOrders.length
            + (amazonFetchErr ? ' FETCH-ERROR=' + amazonFetchErr : '')
            + (newOrders.length ? ' newIds=' + newOrders.slice(0,3).map(function(o){ return o.AmazonOrderId; }).join(',') : ''),
      success: true
    });

    for(var i = 0; i < amazonOrders.length; i++){
      var order = amazonOrders[i];
      if(processedAmazonSet[order.AmazonOrderId]) continue;

      if(!accessToken) continue; // Can't fetch items without token

      var items = await new Promise(function(resolve){
        fetchAmazonOrderItems(accessToken, order.AmazonOrderId, function(err, its){ resolve(err ? [] : its); });
      });

      for(var j = 0; j < items.length; j++){
        var sku = items[j].sku;
        if(!sku) continue;

        // Look up in warehouse_inventory
        var record = await database.collection('warehouse_inventory').findOne({ code: subscriberCode, sku: sku });

        if(!record){
          logSyncAction(subscriberCode, {
            sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
            action: 'skip', reason: 'SKU not in warehouse_inventory (listed via another system)', success: true
          });
          continue;
        }

        if(!record.ebayItemId){
          logSyncAction(subscriberCode, {
            sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
            action: 'skip', reason: 'No eBay ItemID linked', success: true
          });
          // Still mark sold
          await database.collection('warehouse_inventory').updateOne(
            { code: subscriberCode, sku: sku },
            { $set: { status: 'sold-amazon', soldAt: new Date(), soldOrderId: order.AmazonOrderId } }
          );
          continue;
        }

        // eBay-side cleanup. For single-qty listings (one eBay ItemID = one DB
        // record) we EndItem the listing entirely. For multi-qty listings (one eBay
        // ItemID shared by N records), we can't end the whole listing because the
        // other sibling records are still for sale on Amazon and logically still
        // available on eBay. Instead we ReviseItem to decrement Quantity by 1 and
        // only EndItem when the last sibling is sold.
        //
        // Strategy: first mark this record sold (so the sibling count reflects
        // reality), then count remaining unsold siblings. If 0 → EndItem. If N>0 →
        // GetItem + ReviseItem with Quantity = N + QuantitySold (preserves eBay's
        // own sold count, reduces the "available" count by 1).
        await database.collection('warehouse_inventory').updateOne(
          { code: subscriberCode, sku: sku },
          { $set: { status: 'sold-amazon', soldAt: new Date(), soldOrderId: order.AmazonOrderId } }
        );

        var siblingCount = await database.collection('warehouse_inventory').countDocuments({
          code: subscriberCode,
          ebayItemId: record.ebayItemId,
          status: { $nin: ['sold', 'sold-amazon', 'sold-ebay', 'deleted'] }
        });

        if(siblingCount === 0){
          // Last copy — end the whole eBay listing.
          var endResult = await new Promise(function(resolve){
            endEbayListing(subscriberCode, record.ebayItemId, function(err, info){
              resolve({ err: err, info: info });
            });
          });
          if(endResult.err){
            logSyncAction(subscriberCode, {
              sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
              ebayItemId: record.ebayItemId, action: 'end-ebay-listing',
              success: false, error: endResult.err, needsReview: true
            });
          } else {
            logSyncAction(subscriberCode, {
              sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
              ebayItemId: record.ebayItemId, action: 'end-ebay-listing', success: true
            });
          }
        } else {
          // Siblings remain — decrement eBay Quantity by 1. Read current state first
          // so we preserve QuantitySold and only reduce the "available" count.
          var qInfo = await new Promise(function(resolve){
            getEbayItemQuantity(subscriberCode, record.ebayItemId, function(err, info){
              resolve({ err: err, info: info });
            });
          });
          if(qInfo.err || !qInfo.info){
            logSyncAction(subscriberCode, {
              sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
              ebayItemId: record.ebayItemId, action: 'decrement-ebay-qty',
              success: false, error: 'GetItem failed: ' + (qInfo.err || 'no data'), needsReview: true
            });
          } else {
            var newEbayQty = siblingCount + qInfo.info.sold;
            var revResult = await new Promise(function(resolve){
              reviseEbayItemQuantity(subscriberCode, record.ebayItemId, newEbayQty, function(err){
                resolve(err);
              });
            });
            if(revResult){
              logSyncAction(subscriberCode, {
                sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
                ebayItemId: record.ebayItemId, action: 'decrement-ebay-qty',
                success: false, error: 'ReviseItem failed: ' + revResult, needsReview: true
              });
            } else {
              logSyncAction(subscriberCode, {
                sku: sku, soldPlatform: 'amazon', amazonOrderId: order.AmazonOrderId,
                ebayItemId: record.ebayItemId, action: 'decrement-ebay-qty',
                success: true,
                reason: 'Multi-qty listing: ' + siblingCount + ' sibling(s) remain, set eBay Quantity=' + newEbayQty + ' (was ' + qInfo.info.quantity + ', sold=' + qInfo.info.sold + ')'
              });
            }
          }
        }
      }
    }

    // ── EBAY SIDE ──
    var ebayOrders = await new Promise(function(resolve){
      fetchRecentEbayOrders(subscriberCode, ebaySince, function(err, orders){
        resolve(err ? [] : orders);
      });
    });

    // Dedupe by order ID to avoid re-processing
    var processedEbay = await database.collection('sync_log').find({
      subscriberCode: subscriberCode,
      soldPlatform: 'ebay',
      ebayOrderId: { $in: ebayOrders.map(function(o){ return o.orderId; }).filter(Boolean) }
    }).project({ ebayOrderId: 1 }).toArray();
    var processedEbaySet = {};
    processedEbay.forEach(function(p){ processedEbaySet[p.ebayOrderId] = true; });

    for(var k = 0; k < ebayOrders.length; k++){
      var eo = ebayOrders[k];
      if(eo.orderId && processedEbaySet[eo.orderId]) continue;
      if(!eo.sku) continue;

      // eBay-only SKU prefixes — these books are listed only on eBay, never on Amazon.
      // Skip Amazon deletion entirely, log as clean success.
      //   MX-*  Mexico-only inventory
      //   UP-*  old program legacy prefix
      //   0-*   old program legacy prefix (e.g. 0-A-159)
      //   5-*   old program legacy prefix (e.g. 5-B-42)
      // The warehouse tool generates SKUs like bfa.xxxx, so these prefixes can never
      // collide with new tool-listed inventory — safe to treat as eBay-only.
      var skuUpper = (eo.sku || '').toUpperCase();
      if(skuUpper.indexOf('MX-') === 0 || skuUpper.indexOf('UP-') === 0 ||
         skuUpper.indexOf('0-') === 0 || skuUpper.indexOf('5-') === 0){
        logSyncAction(subscriberCode, {
          sku: eo.sku, soldPlatform: 'ebay', ebayOrderId: eo.orderId,
          action: 'skip', reason: 'eBay-only SKU (no Amazon twin)', success: true
        });
        continue;
      }

      // Match eBay sale to a DB record. Direct SKU match for single-qty listings.
      // For multi-qty listings, eBay attaches the same SKU to every unit sold (the
      // SKU of the first copy we listed). So the second, third, etc. units all come
      // in with eo.sku = bfa.aaaa while our DB has bfa.aaaa already marked sold and
      // bfa.bbbb, bfa.cccc, ... still active but sharing the same ebayItemId.
      // Strategy: look for an ACTIVE record with that SKU first. If none, fall back
      // to looking up by ebayItemId + oldest unsold record (FIFO by shelf sequence).
      var record2 = await database.collection('warehouse_inventory').findOne({
        code: subscriberCode,
        sku: eo.sku,
        status: { $nin: ['sold', 'sold-amazon', 'sold-ebay', 'deleted'] }
      });
      if(!record2){
        // Fall back to ItemID + FIFO. First find ANY record with this SKU (even
        // sold) so we can pull its ebayItemId.
        var skuAny = await database.collection('warehouse_inventory').findOne({
          code: subscriberCode,
          sku: eo.sku
        });
        if(skuAny && skuAny.ebayItemId){
          record2 = await database.collection('warehouse_inventory').findOne({
            code: subscriberCode,
            ebayItemId: skuAny.ebayItemId,
            status: { $nin: ['sold', 'sold-amazon', 'sold-ebay', 'deleted'] }
          }, { sort: { 'location.sequence': 1 } });
          if(record2){
            console.log('[sync] eBay sale on SKU ' + eo.sku + ' matched by ItemID FIFO → DB SKU ' + record2.sku + ' (seq ' + (record2.location && record2.location.sequence) + ')');
          }
        }
      }
      if(!record2){
        logSyncAction(subscriberCode, {
          sku: eo.sku, soldPlatform: 'ebay', ebayOrderId: eo.orderId,
          action: 'skip', reason: 'SKU not in warehouse_inventory (listed via another system)', success: true
        });
        continue;
      }

      if(!record2.asin){
        logSyncAction(subscriberCode, {
          sku: record2.sku, soldPlatform: 'ebay', ebayOrderId: eo.orderId,
          action: 'skip', reason: 'No Amazon ASIN linked', success: true
        });
        await database.collection('warehouse_inventory').updateOne(
          { code: subscriberCode, sku: record2.sku },
          { $set: { status: 'sold-ebay', soldAt: new Date(), soldOrderId: eo.orderId } }
        );
        continue;
      }

      // Delete Amazon listing (use record2.sku — may differ from eo.sku on multi-qty)
      var delResult = await new Promise(function(resolve){
        deleteAmazonListing(accessToken, sellerId, record2.sku, marketplaceId, function(err, info){
          resolve({ err: err, info: info });
        });
      });
      var delErr = delResult.err;
      var delInfo = delResult.info || {};

      if(delErr){
        logSyncAction(subscriberCode, {
          sku: record2.sku, soldPlatform: 'ebay', ebayOrderId: eo.orderId,
          asin: record2.asin, action: 'delete-amazon-listing',
          success: false, error: delErr, needsReview: true
        });
      } else if(delInfo.alreadyGone === false && delInfo.verified){
        // The 403 was misleading — GET confirmed the listing STILL EXISTS on Amazon.
        // This is a real failure that needs manual review.
        logSyncAction(subscriberCode, {
          sku: record2.sku, soldPlatform: 'ebay', ebayOrderId: eo.orderId,
          asin: record2.asin, action: 'delete-amazon-listing',
          success: false,
          error: 'Delete got 403 but listing still exists on Amazon',
          reason: delInfo.verifyNote || 'Verified: listing still active',
          needsReview: true
        });
      } else {
        // Success OR confirmed alreadyGone (404 on GET verifies it's truly gone)
        var verifiedTxt = '';
        if(delInfo.alreadyGone){
          if(delInfo.verifiedGone) verifiedTxt = ' · verified gone';
          else if(delInfo.verified === false) verifiedTxt = ' · verify ' + (delInfo.verifyNote || 'skipped');
        }
        logSyncAction(subscriberCode, {
          sku: record2.sku, soldPlatform: 'ebay', ebayOrderId: eo.orderId,
          asin: record2.asin, action: 'delete-amazon-listing',
          success: true,
          reason: delInfo.alreadyGone ? ('Already removed from Amazon (' + delInfo.statusCode + ')' + verifiedTxt) : undefined
        });
        await database.collection('warehouse_inventory').updateOne(
          { code: subscriberCode, sku: record2.sku },
          { $set: { status: 'sold-ebay', soldAt: new Date(), soldOrderId: eo.orderId } }
        );
      }
    }

    // Update last-checked timestamps — but only for platforms whose fetch succeeded.
    // If a fetch fails (quota error etc.), we must NOT advance the checkpoint or
    // we'd permanently miss any orders that arrived during the outage.
    var updateFields = { 'sync.lastRunAt': new Date().toISOString() };
    if(!amazonFetchErr){
      updateFields['sync.lastAmazonCheckedAt'] = new Date().toISOString();
    }
    // eBay fetch success check — assume success if we got this far without an eBay-specific error.
    // (ebayFetchErr if it exists — fall back to updating eBay checkpoint normally)
    if(typeof ebayFetchErr === 'undefined' || !ebayFetchErr){
      updateFields['sync.lastEbayCheckedAt'] = new Date().toISOString();
    }
    await database.collection('subscribers').updateOne(
      { code: { $regex: new RegExp('^' + subscriberCode.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '$', 'i') } },
      { $set: updateFields }
    );
  } catch(e){
    console.log('[sync] Cycle error for', subscriberCode, ':', e.message);
  } finally {
    syncRunning[subscriberCode] = false;
  }
}

// Scheduler — fires every 2 minutes, processes all sync-enabled subscribers
function startSyncScheduler(){
  if(syncSchedulerInterval) return;
  syncSchedulerInterval = setInterval(function(){
    connectMongo(function(err, database){
      if(err || !database) return;
      database.collection('subscribers').find({ 'sync.enabled': true }).toArray()
        .then(function(subs){
          subs.forEach(function(sub){ runSyncCycle(sub.code.toUpperCase()); });
        })
        .catch(function(){});
    });
  }, 5 * 60 * 1000); // every 5 min — quota-friendly, checkpoint fix guarantees no data loss
  console.log('[sync] Scheduler started');
}

setTimeout(startSyncScheduler, 15000);

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
      var pin = (data.pin || '').toString().trim();
      getSubscriber(code, function(err, sub) {
        if (err || !sub) { res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Invalid access code' })); return; }
        if (!sub.active) { res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Your subscription is inactive. Please contact Books for Ages.' })); return; }

        var employees = sub.employees || [];
        var hasAdmin = employees.some(function(e){ return e.isAdmin === true; });

        // Role resolution:
        // - PIN provided: match against employees, use isAdmin flag
        // - No PIN + no admin exists yet: bootstrap mode → grant admin (so user can set up their admin PIN)
        // - No PIN + admin exists: reject
        var role = null;
        var employeeName = null;

        if (pin) {
          var emp = employees.find(function(e){ return e.pin === pin; });
          if (!emp) {
            res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'Invalid PIN for this business.' }));
            return;
          }
          role = emp.isAdmin ? 'admin' : 'employee';
          employeeName = emp.name;
        } else {
          if (!hasAdmin) {
            // Bootstrap: no admin set up yet, allow code-only admin access for initial setup
            role = 'admin';
            employeeName = null;
          } else {
            res.writeHead(200); res.end(JSON.stringify({ valid: false, message: 'PIN required. Please enter your 4-digit PIN.' }));
            return;
          }
        }

        var sessionToken = createSession(sub.code.toUpperCase(), role, employeeName);

        var response = {
          valid: true,
          role: role,
          employeeName: employeeName,
          sessionToken: sessionToken,
          businessName: sub.businessName,
          employees: employees.map(function(e){
            // Admins see everything; employees see names only
            return role === 'admin' ? e : { name: e.name };
          })
        };

        // Admin-only sensitive fields
        if (role === 'admin') {
          response.reportEmail = sub.email;
          response.ebayClientId = sub.ebayClientId || '';
          response.ebayClientSecret = sub.ebayClientSecret || '';
          response.ebayDevId = sub.ebayDevId || '';
          response.ebayUserToken = sub.ebayUserToken || '';
          response.ebayOAuthToken = sub.ebayOAuthToken || '';
          response.ebayShippingPolicyId = sub.ebayShippingPolicyId || '';
          response.ebayPaymentPolicyId = sub.ebayPaymentPolicyId || '';
          response.ebayReturnPolicyId = sub.ebayReturnPolicyId || '';
          response.businessAddressLine1 = sub.businessAddressLine1 || '';
          response.businessAddressLine2 = sub.businessAddressLine2 || '';
          response.businessCity = sub.businessCity || '';
          response.businessState = sub.businessState || '';
          response.businessZip = sub.businessZip || '';
          response.businessPhone = sub.businessPhone || '';
          response.vendors = sub.vendors || [];
          response.customers = sub.customers || [];
          response.savedDescriptions = sub.savedDescriptions || [];
          response.invoicePayableTo = sub.invoicePayableTo || '';
        }

        res.writeHead(200); res.end(JSON.stringify(response));
      });
    });
    return;
  }

  // ── Session resume (whoami) ──
  // Frontend calls this on page load with a saved session token. If valid,
  // returns the same shape /validate-code does so the UI can skip the login
  // screen. If invalid/expired, returns { valid: false } and the frontend
  // clears its stored token and shows the login screen as normal.
  if (pathname === '/whoami' && req.method === 'GET') {
    var waSess = getRequestSession(req, parsed);
    if(!waSess){
      res.writeHead(200); res.end(JSON.stringify({ valid: false, reason: 'Session expired or invalid' })); return;
    }
    getSubscriber(waSess.subscriberCode, function(err, sub){
      if(err || !sub || !sub.active){
        res.writeHead(200); res.end(JSON.stringify({ valid: false, reason: 'Subscriber not found or inactive' })); return;
      }
      var employees = sub.employees || [];
      var role = waSess.role;
      var employeeName = waSess.employeeName || null;
      var response = {
        valid: true,
        role: role,
        employeeName: employeeName,
        sessionToken: null, // client already has its token, don't re-issue
        businessName: sub.businessName,
        employees: employees.map(function(e){
          return role === 'admin' ? e : { name: e.name };
        })
      };
      // Admin-only sensitive fields — same as /validate-code
      if (role === 'admin') {
        response.reportEmail = sub.email;
        response.ebayClientId = sub.ebayClientId || '';
        response.ebayClientSecret = sub.ebayClientSecret || '';
        response.ebayDevId = sub.ebayDevId || '';
        response.ebayUserToken = sub.ebayUserToken || '';
        response.ebayOAuthToken = sub.ebayOAuthToken || '';
        response.ebayShippingPolicyId = sub.ebayShippingPolicyId || '';
        response.ebayPaymentPolicyId = sub.ebayPaymentPolicyId || '';
        response.ebayReturnPolicyId = sub.ebayReturnPolicyId || '';
        response.businessAddressLine1 = sub.businessAddressLine1 || '';
        response.businessAddressLine2 = sub.businessAddressLine2 || '';
        response.businessCity = sub.businessCity || '';
        response.businessState = sub.businessState || '';
        response.businessZip = sub.businessZip || '';
        response.businessPhone = sub.businessPhone || '';
        response.vendors = sub.vendors || [];
        response.customers = sub.customers || [];
        response.savedDescriptions = sub.savedDescriptions || [];
        response.invoicePayableTo = sub.invoicePayableTo || '';
      }
      res.writeHead(200); res.end(JSON.stringify(response));
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
  // ── Amazon Pick List ──
  // Returns Amazon MFN (seller-shipped) orders that are Unshipped.
  // Fetches item details (SKU, title) per order, caching in MongoDB to avoid re-fetching.
  if (pathname === '/my/amazon/picklist' && req.method === 'GET') {
    var aCode = (parsed.query.code || '').toUpperCase();
    var bypassCache = parsed.query.bypass === '1';

    // Server-side cache — 15 min fresh, serves stale on quota errors
    var CACHE_TTL = 15 * 60 * 1000;
    if(!global._amzPicklistCache) global._amzPicklistCache = {};
    var cached = global._amzPicklistCache[aCode];
    var nowMs = Date.now();
    if(cached && !bypassCache && (nowMs - cached.ts) < CACHE_TTL){
      res.writeHead(200); res.end(JSON.stringify(cached.data)); return;
    }
    // If bypassing, also invalidate the shared Amazon orders cache so we pull fresh from Amazon
    if(bypassCache && global._sharedAmzOrders){
      delete global._sharedAmzOrders[aCode];
    }

    getSubscriber(aCode, function(err, sub){
      if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;

      // Helper: serve stale cache if we have one, else pass-through error
      function serveStaleOrError(errorMsg, fallback){
        if(cached && cached.data){
          var stale = Object.assign({}, cached.data, { stale: true, staleReason: errorMsg });
          res.writeHead(200); res.end(JSON.stringify(stale)); return;
        }
        res.writeHead(200); res.end(JSON.stringify(fallback)); return;
      }

      getAmazonAccessToken(function(tokenErr, accessToken){
        if(tokenErr){ serveStaleOrError('Amazon auth: ' + tokenErr, { error: 'Amazon auth: ' + tokenErr, pending: [] }); return; }
        connectMongo(function(dbErr, database){
          var snoozedPromise = database
            ? database.collection('snoozed_orders').find({ subscriberCode: aCode }).toArray()
            : Promise.resolve([]);
          snoozedPromise.then(function(snoozed){
            var snoozedIds = (snoozed || []).map(function(s){ return s.orderId; });

            // Pick list needs CURRENT status — shared cache has stale statuses.
            // Do a targeted fetch with OrderStatuses=Unshipped,PartiallyShipped.
            // This is a small, fast query (usually <20 orders for active sellers).
            getAmazonAccessToken(function(plTokenErr, plAccessToken){
              if(plTokenErr){
                serveStaleOrError('Amazon auth: ' + plTokenErr, { error: 'Amazon auth: ' + plTokenErr, pending: [] });
                return;
              }
              var thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
              var pickAllOrders = [];
              function fetchPickPage(nextToken){
                var qp = 'MarketplaceIds=' + marketplaceId
                       + '&CreatedAfter=' + encodeURIComponent(thirtyDaysAgo)
                       + '&OrderStatuses=Unshipped,PartiallyShipped';
                if(nextToken) qp += '&NextToken=' + encodeURIComponent(nextToken);
                var opts = {
                  hostname: 'sellingpartnerapi-na.amazon.com',
                  path: '/orders/v0/orders?' + qp,
                  method: 'GET',
                  headers: { 'x-amz-access-token': plAccessToken }
                };
                var aReq = https.request(opts, function(r){
                  var data = '';
                  r.on('data', function(c){ data += c; });
                  r.on('end', function(){
                    try {
                      var json = JSON.parse(data);
                      if(json.errors && json.errors[0]){
                        var errMsg = json.errors[0].message || json.errors[0].code;
                        serveStaleOrError(errMsg, { error: errMsg, pending: [] });
                        return;
                      }
                      var payload = json.payload || {};
                      var pageOrders = (payload.Orders || []).filter(function(o){
                        if(o.FulfillmentChannel !== 'MFN') return false;
                        if(snoozedIds.indexOf(o.AmazonOrderId) !== -1) return false;
                        return true;
                      });
                      pickAllOrders = pickAllOrders.concat(pageOrders);
                      if(payload.NextToken){ fetchPickPage(payload.NextToken); return; }
                      // Done — enrich and respond
                      if(!pickAllOrders.length){
                        var emptyBody = { pending: [], canceled: [], actionNeeded: [] };
                        global._amzPicklistCache[aCode] = { ts: Date.now(), data: emptyBody };
                        res.writeHead(200); res.end(JSON.stringify(emptyBody)); return;
                      }
                      enrichAndRespond(pickAllOrders, plAccessToken, database);
                    } catch(e){ serveStaleOrError('Parse error', { error: 'Parse error', pending: [] }); }
                  });
                });
                aReq.on('error', function(e){ serveStaleOrError(e.message, { error: e.message, pending: [] }); });
                aReq.setTimeout(20000, function(){ aReq.destroy(); serveStaleOrError('Timeout', { error: 'Timeout', pending: [] }); });
                aReq.end();
              }
              fetchPickPage(null);
            });

            function enrichAndRespond(allOrders, accessToken, database){
              // Look up cached items for these orders in MongoDB
              var orderIds = allOrders.map(function(o){ return o.AmazonOrderId; });
              var cachedById = {};
              var cacheFetch = database
                ? database.collection('amazon_order_items_cache').find({ orderId: { $in: orderIds } }).toArray()
                : Promise.resolve([]);
              cacheFetch.then(function(rows){
                (rows || []).forEach(function(r){
                  // Only use cache if it has allItems (new format).
                  // Old entries without allItems need to be refetched to support multi-item display.
                  if(r.allItems) cachedById[r.orderId] = r;
                });
                // Determine which need live fetch
                var toFetch = allOrders.filter(function(o){ return !cachedById[o.AmazonOrderId]; });
                // Fetch with 2 concurrent at a time (rate limit friendly)
                var concurrency = 2;
                var idx = 0;
                var active = 0;
                function startNext(){
                  while(active < concurrency && idx < toFetch.length){
                    var order = toFetch[idx++];
                    active++;
                    fetchOrderItems(order.AmazonOrderId, function(itemInfo){
                      active--;
                      cachedById[order.AmazonOrderId] = itemInfo;
                      // Save to cache (fire-and-forget)
                      if(database){
                        database.collection('amazon_order_items_cache').updateOne(
                          { orderId: order.AmazonOrderId },
                          { $set: Object.assign({ orderId: order.AmazonOrderId, cachedAt: new Date() }, itemInfo) },
                          { upsert: true }
                        ).catch(function(){});
                      }
                      if(idx >= toFetch.length && active === 0){ finish(); }
                      else { startNext(); }
                    });
                  }
                  if(toFetch.length === 0){ finish(); }
                }
                function finish(){
                  var now = new Date();
                  var pending = allOrders.map(function(o){
                    var cached = cachedById[o.AmazonOrderId] || {};
                    var created = new Date(o.PurchaseDate);
                    var ageHours = Math.round((now - created) / 3600000 * 10) / 10;
                    var skuRaw = cached.sku || '';
                    var skuPrefix = skuRaw.split(/[-\.]/)[0] || skuRaw;
                    var itemCount = parseInt(o.NumberOfItemsUnshipped || 0) + parseInt(o.NumberOfItemsShipped || 0);
                    if(!itemCount) itemCount = 1;
                    return {
                      orderId: o.AmazonOrderId,
                      orderDate: o.PurchaseDate,
                      ageHours: ageHours,
                      platform: 'Amazon',
                      title: cached.title || '',
                      sku: skuRaw,
                      skuPrefix: skuPrefix,
                      price: o.OrderTotal ? parseFloat(o.OrderTotal.Amount) : 0,
                      condition: cached.condition || '',
                      cancelState: 'NONE_REQUESTED',
                      itemCount: itemCount,
                      allItems: cached.allItems || [],
                      shippingCategory: o.ShipmentServiceLevelCategory || '',  // 'Expedited', 'Standard', 'NextDay', etc.
                      location: null
                    };
                  });
                  // Enrich with warehouse location from warehouse_inventory
                  // Collect ALL SKUs including from multi-item orders
                  var allSkusSet = {};
                  pending.forEach(function(o){
                    if(o.sku) allSkusSet[o.sku] = true;
                    (o.allItems || []).forEach(function(it){
                      if(it.sku) allSkusSet[it.sku] = true;
                    });
                  });
                  var skusForLookup = Object.keys(allSkusSet);
                  if(skusForLookup.length && database){
                    database.collection('warehouse_inventory')
                      .find({ code: aCode, sku: { $in: skusForLookup } })
                      .project({ sku: 1, location: 1 })
                      .toArray()
                      .then(function(rows){
                        var locMap = {};
                        (rows || []).forEach(function(r){ if(r.sku) locMap[r.sku] = r.location || null; });
                        pending.forEach(function(o){
                          o.location = locMap[o.sku] || null;
                          // Also enrich each item in allItems with its location
                          (o.allItems || []).forEach(function(it){
                            it.location = locMap[it.sku] || null;
                          });
                        });
                        var responseBody = { pending: pending, canceled: [], actionNeeded: [] };
                        global._amzPicklistCache[aCode] = { ts: Date.now(), data: responseBody };
                        res.writeHead(200); res.end(JSON.stringify(responseBody));
                      })
                      .catch(function(){
                        var responseBody = { pending: pending, canceled: [], actionNeeded: [] };
                        global._amzPicklistCache[aCode] = { ts: Date.now(), data: responseBody };
                        res.writeHead(200); res.end(JSON.stringify(responseBody));
                      });
                  } else {
                    var responseBody = { pending: pending, canceled: [], actionNeeded: [] };
                    global._amzPicklistCache[aCode] = { ts: Date.now(), data: responseBody };
                    res.writeHead(200); res.end(JSON.stringify(responseBody));
                  }
                }
                startNext();
              }).catch(function(){ res.writeHead(200); res.end(JSON.stringify({ error: 'Cache read error', pending: [] })); });
            }

            function fetchOrderItems(orderId, cb){
              var opts = {
                hostname: 'sellingpartnerapi-na.amazon.com',
                path: '/orders/v0/orders/' + encodeURIComponent(orderId) + '/orderItems',
                method: 'GET',
                headers: { 'x-amz-access-token': accessToken }
              };
              var iReq = https.request(opts, function(r){
                var data = '';
                r.on('data', function(c){ data += c; });
                r.on('end', function(){
                  try {
                    var json = JSON.parse(data);
                    var items = ((json.payload || {}).OrderItems) || [];
                    var first = items[0] || {};
                    cb({
                      sku: first.SellerSKU || '',
                      title: first.Title || '',
                      condition: first.ConditionId || '',
                      // All items in the order for multi-item picks
                      allItems: items.map(function(it){
                        return {
                          sku: it.SellerSKU || '',
                          title: it.Title || '',
                          condition: it.ConditionId || '',
                          quantity: it.QuantityOrdered || 1
                        };
                      })
                    });
                  } catch(e){ cb({ sku: '', title: '', condition: '', allItems: [] }); }
                });
              });
              iReq.on('error', function(){ cb({ sku: '', title: '', condition: '', allItems: [] }); });
              iReq.setTimeout(10000, function(){ iReq.destroy(); cb({ sku: '', title: '', condition: '', allItems: [] }); });
              iReq.end();
            }

            fetchOrderItems; // function defined above

          }).catch(function(){ res.writeHead(200); res.end(JSON.stringify({ error: 'Snoozed lookup failed', pending: [] })); });
        });
      });
    });
    return;
  }

  if (pathname === '/my/ebay/picklist' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var bypassEbayCache = parsed.query.bypass === '1';

    // Server-side cache — 15 min fresh, serves stale on errors
    var EBAY_PL_CACHE_TTL = 15 * 60 * 1000;
    if(!global._ebayPicklistCache) global._ebayPicklistCache = {};
    var ebayCached = global._ebayPicklistCache[code];
    var ebayNowMs = Date.now();
    if(ebayCached && !bypassEbayCache && (ebayNowMs - ebayCached.ts) < EBAY_PL_CACHE_TTL){
      res.writeHead(200); res.end(JSON.stringify(ebayCached.data)); return;
    }

    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var userToken = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
      var hasRetriedAuth = false;
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
                // Auto-refresh + retry on 401
                if (r.statusCode === 401 && !hasRetriedAuth && sub.ebayRefreshToken) {
                  hasRetriedAuth = true;
                  console.log('eBay picklist 401 — refreshing token and retrying for', code);
                  refreshEbayTokenForSubscriber(sub, function(refreshErr, newToken){
                    if (refreshErr || !newToken) {
                      res.writeHead(200); res.end(JSON.stringify({ error: 'eBay token expired and auto-refresh failed. Please reconnect eBay in the portal.', orders: [] }));
                      return;
                    }
                    userToken = newToken;
                    allOrders = [];
                    fetchOrders(0);
                  });
                  return;
                }
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
                        cancelState: o.cancelStatus ? o.cancelStatus.cancelState : 'NONE_REQUESTED',
                        itemCount: (o.lineItems || []).length || 1,
                        shippingCategory: '' // eBay orders don't have expedited flag in this context
                      };
                    }
                    var simplePending = pending.map(simplify);
                    var simpleCanceled = canceled.map(simplify);
                    var simpleActionNeeded = actionNeeded.map(simplify);

                    // Enrich each order with its warehouse location (row/section/sequence)
                    // from warehouse_inventory, keyed by SKU. Mexico SKUs (MX/UP prefix) won't match;
                    // frontend handles those with SKU-based sorting.
                    var allOrdersForLookup = simplePending.concat(simpleCanceled).concat(simpleActionNeeded);
                    var skusToLookup = allOrdersForLookup.map(function(o){ return o.sku; }).filter(Boolean);
                    function sendEbayPlResponse(){
                      var body = {
                        pending: simplePending,
                        canceled: simpleCanceled,
                        actionNeeded: simpleActionNeeded
                      };
                      global._ebayPicklistCache[code] = { ts: Date.now(), data: body };
                      res.writeHead(200); res.end(JSON.stringify(body));
                    }
                    if(skusToLookup.length && database){
                      database.collection('warehouse_inventory')
                        .find({ code: code, sku: { $in: skusToLookup } })
                        .project({ sku: 1, location: 1 })
                        .toArray()
                        .then(function(rows){
                          var locMap = {};
                          (rows || []).forEach(function(r){ if(r.sku) locMap[r.sku] = r.location || null; });
                          allOrdersForLookup.forEach(function(o){ o.location = locMap[o.sku] || null; });
                          sendEbayPlResponse();
                        })
                        .catch(function(){
                          sendEbayPlResponse();
                        });
                    } else {
                      sendEbayPlResponse();
                    }
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

  // ── Amazon Sales API ──
  // Uses SP-API Orders endpoint. Mirrors /my/ebay/sales logic for consistency.
  // Has in-memory caching per subscriber+period to survive SP-API rate limits.
  if (pathname === '/my/amazon/sales' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var period = parsed.query.period || 'today';
    var offsetMinutes = parseInt(parsed.query.offset || '0');
    var specificDate = parsed.query.date || null;

    // All periods unified at 15min. Historical dates keep 60min cache.
    var CACHE_TTL_MS = {
      'today': 15 * 60 * 1000,
      'week':  15 * 60 * 1000,
      'month': 15 * 60 * 1000,
      'lastmonth-to-date': 30 * 60 * 1000,
      'date':  60 * 60 * 1000
    };
    var cacheKey = code + '|' + (specificDate ? 'date:' + specificDate : period);
    var ttl = specificDate ? CACHE_TTL_MS.date : (CACHE_TTL_MS[period] || 15 * 60 * 1000);

    // Bypass cache when ?bypass=1 (used by refresh button)
    var bypassSalesCache = parsed.query.bypass === '1';

    if(!global._amzSalesCache) global._amzSalesCache = {};
    var cached = global._amzSalesCache[cacheKey];
    var nowMs = Date.now();
    if(cached && !bypassSalesCache && (nowMs - cached.ts) < ttl){
      // Fresh cache hit
      res.writeHead(200); res.end(JSON.stringify(cached.data)); return;
    }
    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
      var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;

      // Calculate date range — identical logic to eBay
      var now = new Date();
      var localNow = new Date(now.getTime() - offsetMinutes * 60000);
      var startDate, endDate;
      if (specificDate) {
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
      } else if (period === 'lastmonth-to-date') {
        var lmStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() - 1, 1));
        var lmEnd = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() - 1, localNow.getUTCDate() + 1));
        startDate = new Date(lmStart.getTime() + offsetMinutes * 60000);
        endDate = new Date(lmEnd.getTime() + offsetMinutes * 60000);
      } else {
        var monthStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), 1));
        startDate = new Date(monthStart.getTime() + offsetMinutes * 60000);
        endDate = null;
      }

      getAmazonAccessToken(function(tokenErr, accessToken){
        if(tokenErr){ res.writeHead(200); res.end(JSON.stringify({ error: 'Amazon auth: ' + tokenErr, orders: [] })); return; }
        // SP-API Orders: /orders/v0/orders
        var allOrders = [];
        var responded = false;
        function sendResponse(body){
          if(responded) return;
          responded = true;
          res.writeHead(200); res.end(JSON.stringify(body));
        }
        function serveStaleOrError(errorMsg){
          // If we have ANY cached data for this key (even expired), serve it and mark as stale.
          if(cached && cached.data){
            var stale = Object.assign({}, cached.data, { stale: true, staleReason: errorMsg });
            sendResponse(stale);
            return true;
          }
          return false;
        }
        function fetchOrders(nextToken, retriesLeft){
          if(typeof retriesLeft !== 'number') retriesLeft = 1;
          // Amazon SP-API rejects CreatedBefore timestamps within 2 minutes of "now"
          // Clamp to 3 minutes before now for safety.
          var safeEnd = endDate;
          if(endDate){
            var maxAllowed = new Date(Date.now() - 3 * 60 * 1000);
            if(endDate > maxAllowed) safeEnd = maxAllowed;
          }
          var qp = 'MarketplaceIds=' + marketplaceId
                 + '&CreatedAfter=' + encodeURIComponent(startDate.toISOString());
          if(safeEnd) qp += '&CreatedBefore=' + encodeURIComponent(safeEnd.toISOString());
          if(nextToken) qp += '&NextToken=' + encodeURIComponent(nextToken);
          var opts = {
            hostname: 'sellingpartnerapi-na.amazon.com',
            path: '/orders/v0/orders?' + qp,
            method: 'GET',
            headers: { 'x-amz-access-token': accessToken }
          };
          var aReq = https.request(opts, function(r){
            var data = '';
            r.on('data', function(c){ data += c; });
            r.on('end', function(){
              try {
                var json = JSON.parse(data);
                if(json.errors && json.errors[0]){
                  var errMsg = json.errors[0].message || json.errors[0].code || 'Unknown error';
                  var isQuota = /quota|throttl|rate/i.test(errMsg);
                  // Retry once with 3s delay for quota errors
                  if(isQuota && retriesLeft > 0){
                    setTimeout(function(){ fetchOrders(nextToken, retriesLeft - 1); }, 3000);
                    return;
                  }
                  // Still failing — try to serve stale cache, else pass the error
                  if(serveStaleOrError(errMsg)) return;
                  sendResponse({ error: errMsg, orders: [] });
                  return;
                }
                var payload = json.payload || {};
                var orders = (payload.Orders || []).filter(function(o){
                  return o.OrderStatus !== 'Canceled' && o.OrderStatus !== 'Pending' && o.OrderTotal;
                });
                allOrders = allOrders.concat(orders);
                if(payload.NextToken){ fetchOrders(payload.NextToken, 1); return; }
                // Done — compute totals
                var totalRevenue = allOrders.reduce(function(sum, o){
                  var amt = o.OrderTotal && parseFloat(o.OrderTotal.Amount);
                  return sum + (isNaN(amt) ? 0 : amt);
                }, 0);
                var simplified = allOrders.map(function(o){
                  return {
                    orderId: o.AmazonOrderId,
                    date: o.PurchaseDate,
                    title: '',
                    sku: '',
                    price: o.OrderTotal ? parseFloat(o.OrderTotal.Amount) : 0,
                    paidToSeller: o.OrderTotal ? parseFloat(o.OrderTotal.Amount) : 0,
                    status: o.OrderStatus,
                    buyer: ''
                  };
                });
                // For date-filtered queries, enrich with title+sku from orderItems
                // (small N of orders, worth the extra SP-API calls). Sequential to avoid quota spikes.
                function respondWithResult(){
                  var result = {
                    count: allOrders.length,
                    totalRevenue: Math.round(totalRevenue * 100) / 100,
                    orders: simplified
                  };
                  global._amzSalesCache[cacheKey] = { ts: Date.now(), data: result };
                  sendResponse(result);
                }
                if((specificDate || period === 'today') && simplified.length && simplified.length <= 30){
                  var idx = 0;
                  function enrichNext(){
                    if(idx >= simplified.length){ respondWithResult(); return; }
                    var o = simplified[idx];
                    var iOpts = {
                      hostname: 'sellingpartnerapi-na.amazon.com',
                      path: '/orders/v0/orders/' + encodeURIComponent(o.orderId) + '/orderItems',
                      method: 'GET',
                      headers: { 'x-amz-access-token': accessToken }
                    };
                    var iReq = https.request(iOpts, function(r2){
                      var d2 = '';
                      r2.on('data', function(c){ d2 += c; });
                      r2.on('end', function(){
                        try {
                          var j2 = JSON.parse(d2);
                          var items = ((j2.payload || {}).OrderItems) || [];
                          if(items[0]){
                            o.title = items[0].Title || '';
                            o.sku = items[0].SellerSKU || '';
                          }
                        } catch(e){}
                        idx++;
                        // Throttle between orderItems calls (SP-API quota)
                        setTimeout(enrichNext, 250);
                      });
                    });
                    iReq.on('error', function(){ idx++; setTimeout(enrichNext, 250); });
                    iReq.setTimeout(8000, function(){ iReq.destroy(); idx++; setTimeout(enrichNext, 250); });
                    iReq.end();
                  }
                  enrichNext();
                } else {
                  respondWithResult();
                }
              } catch(e){
                if(serveStaleOrError('Parse error')) return;
                sendResponse({ error: 'Parse error', orders: [] });
              }
            });
          });
          aReq.on('error', function(e){
            if(serveStaleOrError(e.message)) return;
            sendResponse({ error: e.message, orders: [] });
          });
          aReq.setTimeout(20000, function(){
            aReq.destroy();
            if(serveStaleOrError('Timeout')) return;
            sendResponse({ error: 'Timeout', orders: [] });
          });
          aReq.end();
        }
        fetchOrders(null, 1);
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

    // All periods unified at 15min — matches Amazon sales cache
    var EBAY_CACHE_TTL_MS = {
      'today': 15 * 60 * 1000,
      'week':  15 * 60 * 1000,
      'month': 15 * 60 * 1000,
      'lastmonth-to-date': 30 * 60 * 1000,
      'date':  60 * 60 * 1000
    };
    var ebayCacheKey = code + '|' + (specificDate ? 'date:' + specificDate : period);
    var ebayTtl = specificDate ? EBAY_CACHE_TTL_MS.date : (EBAY_CACHE_TTL_MS[period] || 15 * 60 * 1000);
    var bypassEbaySalesCache = parsed.query.bypass === '1';
    if(!global._ebaySalesCache) global._ebaySalesCache = {};
    var ebayCached = global._ebaySalesCache[ebayCacheKey];
    var ebayNowMs = Date.now();
    if(ebayCached && !bypassEbaySalesCache && (ebayNowMs - ebayCached.ts) < ebayTtl){
      res.writeHead(200); res.end(JSON.stringify(ebayCached.data)); return;
    }
    function serveEbayStaleOrFresh(body, isError){
      if(isError && ebayCached && ebayCached.data){
        var stale = Object.assign({}, ebayCached.data, { stale: true });
        res.writeHead(200); res.end(JSON.stringify(stale)); return;
      }
      if(!isError){
        global._ebaySalesCache[ebayCacheKey] = { ts: Date.now(), data: body };
      }
      res.writeHead(200); res.end(JSON.stringify(body));
    }

    getSubscriber(code, function(err, sub) {
      if (!sub) { res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var userToken = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
      var hasRetriedAuth = false;

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
      } else if (period === 'lastmonth-to-date') {
        // Same day range as this month, but one month back. e.g. if today = Apr 20, range = Mar 1 00:00 → Mar 20 23:59:59
        var lmStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() - 1, 1));
        var lmEnd = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() - 1, localNow.getUTCDate() + 1));
        startDate = new Date(lmStart.getTime() + offsetMinutes * 60000);
        endDate = new Date(lmEnd.getTime() + offsetMinutes * 60000);
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
            // Auto-refresh + retry on 401
            if (r.statusCode === 401 && !hasRetriedAuth && sub.ebayRefreshToken) {
              hasRetriedAuth = true;
              console.log('eBay sales 401 — refreshing token and retrying for', code);
              refreshEbayTokenForSubscriber(sub, function(refreshErr, newToken){
                if (refreshErr || !newToken) {
                  res.writeHead(200); res.end(JSON.stringify({ error: 'eBay token expired and auto-refresh failed. Please reconnect eBay in the portal.', orders: [] }));
                  return;
                }
                userToken = newToken;
                allOrders = []; // reset accumulator
                fetchOrders(0); // retry from start
              });
              return;
            }
            try {
              var json = JSON.parse(data);
              if (json.errors) { serveEbayStaleOrFresh({ error: json.errors[0].longMessage, orders: [] }, true); return; }
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
                // Save to cache
                try {
                  global._ebaySalesCache[ebayCacheKey] = {
                    ts: Date.now(),
                    data: {
                      count: allOrders.length,
                      totalRevenue: Math.round(totalRevenue * 100) / 100,
                      orders: simplifiedOrders
                    }
                  };
                } catch(e){}
              }
            } catch(e) { serveEbayStaleOrFresh({ error: 'Parse error', orders: [] }, true); }
          });
        });
        req2.on('error', function(e){ serveEbayStaleOrFresh({ error: e.message, orders: [] }, true); });
        req2.setTimeout(15000, function(){ req2.destroy(); serveEbayStaleOrFresh({ error: 'Timeout', orders: [] }, true); });
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
    var sessTcEmp = getRequestSession(req, parsed);
    if(!sessTcEmp || sessTcEmp.role !== 'admin' || sessTcEmp.subscriberCode !== code){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
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
  // ── Timeclock: Get all punches for an employee + date (admin view) ──
  // GET /tc/day-punches?code=X&name=Y&date=YYYY-MM-DD
  if (pathname === '/tc/day-punches' && req.method === 'GET') {
    var dpCode = (parsed.query.code || '').toUpperCase();
    var dpSess = getRequestSession(req, parsed);
    if(!dpSess || dpSess.role !== 'admin' || dpSess.subscriberCode !== dpCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    var dpName = parsed.query.name || '';
    var dpDate = parsed.query.date || '';
    if(!dpCode || !dpName || !dpDate){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code/name/date' })); return; }
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ punches: [] })); return; }
      database.collection('timeclock').find({
        subscriberCode: dpCode,
        employeeName: dpName,
        localDate: dpDate
      }).sort({ createdAt: 1 }).toArray()
        .then(function(rows){
          res.writeHead(200); res.end(JSON.stringify({
            punches: (rows || []).map(function(p){
              return {
                id: p._id.toString(),
                type: p.type,
                localDate: p.localDate,
                localTime: p.localTime,
                createdAt: p.createdAt
              };
            })
          }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ punches: [], error: e.message })); });
    });
    return;
  }

  // ── Timeclock: Edit a single punch (admin) ──
  // POST /tc/punch-edit  body: { code, id, localTime, type, localDate }
  if (pathname === '/tc/punch-edit' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var peCode = (data.code || '').toUpperCase();
      var peSess = getRequestSession(req, parsed);
      if(!peSess || peSess.role !== 'admin' || peSess.subscriberCode !== peCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var peId = data.id;
      if(!peId){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing id' })); return; }
      var update = {};
      if(typeof data.localTime === 'string') update.localTime = data.localTime;
      if(data.type === 'in' || data.type === 'out') update.type = data.type;
      if(typeof data.localDate === 'string' && data.localDate) update.localDate = data.localDate;
      if(!Object.keys(update).length){ res.writeHead(400); res.end(JSON.stringify({ error: 'Nothing to update' })); return; }
      update.editedBy = peSess.employeeName || 'Admin';
      update.editedAt = new Date().toISOString();
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var ObjectId;
        try { ObjectId = require('mongodb').ObjectId; } catch(e){ res.writeHead(500); res.end(JSON.stringify({ error: 'Server config error' })); return; }
        var oid;
        try { oid = new ObjectId(peId); } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid id' })); return; }
        database.collection('timeclock').updateOne(
          { _id: oid, subscriberCode: peCode },
          { $set: update }
        )
          .then(function(r){
            if(r.matchedCount === 0){ res.writeHead(404); res.end(JSON.stringify({ error: 'Punch not found' })); return; }
            res.writeHead(200); res.end(JSON.stringify({ success: true }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Timeclock: Delete a single punch (admin) ──
  // POST /tc/punch-delete  body: { code, id }
  if (pathname === '/tc/punch-delete' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var pdCode = (data.code || '').toUpperCase();
      var pdSess = getRequestSession(req, parsed);
      if(!pdSess || pdSess.role !== 'admin' || pdSess.subscriberCode !== pdCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var pdId = data.id;
      if(!pdId){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing id' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var ObjectId;
        try { ObjectId = require('mongodb').ObjectId; } catch(e){ res.writeHead(500); res.end(JSON.stringify({ error: 'Server config error' })); return; }
        var oid;
        try { oid = new ObjectId(pdId); } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid id' })); return; }
        database.collection('timeclock').deleteOne({ _id: oid, subscriberCode: pdCode })
          .then(function(r){
            res.writeHead(200); res.end(JSON.stringify({ success: true, deleted: r.deletedCount }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── Timeclock: Add a manual punch (admin, for forgotten in/out) ──
  // POST /tc/punch-add  body: { code, name, type, localDate, localTime }
  if (pathname === '/tc/punch-add' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var paCode = (data.code || '').toUpperCase();
      var paSess = getRequestSession(req, parsed);
      if(!paSess || paSess.role !== 'admin' || paSess.subscriberCode !== paCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var paName = data.name || '';
      var paType = data.type;
      var paDate = data.localDate || '';
      var paTime = data.localTime || '';
      if(!paCode || !paName || !paDate || !paTime){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing required fields' })); return; }
      if(paType !== 'in' && paType !== 'out'){ res.writeHead(400); res.end(JSON.stringify({ error: 'Type must be in or out' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        // Build createdAt from localDate + localTime so the punch slots correctly in time-order sort
        var fakeIso;
        try { fakeIso = new Date(paDate + 'T' + paTime + ':00').toISOString(); }
        catch(e){ fakeIso = new Date().toISOString(); }
        var entry = {
          subscriberCode: paCode,
          employeeName: paName,
          employeePin: data.pin || '',
          type: paType,
          localDate: paDate,
          localTime: paTime,
          createdAt: fakeIso,
          offsetMinutes: typeof data.offsetMinutes === 'number' ? data.offsetMinutes : 0,
          manuallyAddedBy: paSess.employeeName || 'Admin',
          manuallyAddedAt: new Date().toISOString()
        };
        database.collection('timeclock').insertOne(entry)
          .then(function(r){
            res.writeHead(200); res.end(JSON.stringify({ success: true, id: r.insertedId.toString() }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  if (pathname === '/tc/clear' && req.method === 'GET') {
    var code = (parsed.query.code || '').toUpperCase();
    var sessTcClr = getRequestSession(req, parsed);
    if(!sessTcClr || sessTcClr.role !== 'admin' || sessTcClr.subscriberCode !== code){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
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
    var sessTcDbg = getRequestSession(req, parsed);
    if(!sessTcDbg || sessTcDbg.role !== 'admin' || sessTcDbg.subscriberCode !== code){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
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
        // Validate token - check current and previous window (allow ~1min grace)
        var now = Date.now();
        var window1 = Math.floor(now / 60000);
        var window2 = window1 - 1;
        var validToken1 = crypto.createHmac('sha256', ADMIN_KEY).update(code + ':' + window1).digest('hex').substring(0, 12);
        var validToken2 = crypto.createHmac('sha256', ADMIN_KEY).update(code + ':' + window2).digest('hex').substring(0, 12);
        if (token !== validToken1 && token !== validToken2) {
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
        connectMongo(function(err, database){
          if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
          // Look up the record first so we know which platforms it was listed on.
          var lookupQuery;
          try {
            lookupQuery = data.itemId
              ? { _id: new (require('mongodb').ObjectId)(data.itemId) }
              : { code: code, sku: data.sku };
          } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid itemId' })); return; }

          database.collection('warehouse_inventory').findOne(lookupQuery).then(function(record){
            if(!record){ res.writeHead(404); res.end(JSON.stringify({ error: 'Item not found' })); return; }

            var listed = record.listedOn || [];
            var ebayItemId = data.ebayItemId || record.ebayItemId;
            var asin = record.asin;
            var sku = record.sku || data.sku;
            var results = { ebay: 'not-listed', amazon: 'not-listed' };
            // eBay: needs ItemID because EndItem API requires it (can't end by SKU).
            // Amazon: only needs SKU — the DELETE endpoint is /items/{sellerId}/{sku}.
            //   ASIN is a catalog identifier, not required for deletion. Using just
            //   listedOn means we can still clean up records where asin wasn't saved.
            var doEbay = (listed.indexOf('ebay') !== -1 || !!data.ebayItemId) && ebayItemId;
            var doAmazon = listed.indexOf('amazon') !== -1 && sku;

            function stepEbay(after){
              if(!doEbay){ after(); return; }
              // Sibling-aware eBay cleanup: if other active records share this
              // ebayItemId (multi-qty listing), don't end the whole listing —
              // decrement its Quantity by 1 instead. Only EndItem when this is
              // the last sibling.
              database.collection('warehouse_inventory').countDocuments({
                code: code,
                ebayItemId: ebayItemId,
                _id: { $ne: record._id },  // exclude the record being deleted
                status: { $nin: ['sold', 'sold-amazon', 'sold-ebay', 'deleted'] }
              }).then(function(siblingCount){
                if(siblingCount === 0){
                  // Last copy — end whole listing
                  endEbayListing(code, ebayItemId, function(endErr, endInfo){
                    if(endErr){ results.ebay = 'error: ' + endErr; }
                    else if(endInfo && endInfo.alreadyGone){ results.ebay = 'already-ended'; }
                    else { results.ebay = 'ended'; }
                    after();
                  });
                } else {
                  // Siblings remain — decrement Quantity. Read current state
                  // first so we preserve QuantitySold.
                  getEbayItemQuantity(code, ebayItemId, function(getErr, info){
                    if(getErr || !info){
                      results.ebay = 'error: GetItem failed (' + (getErr || 'no data') + ')';
                      after();
                      return;
                    }
                    var newQty = siblingCount + info.sold;
                    reviseEbayItemQuantity(code, ebayItemId, newQty, function(revErr){
                      if(revErr){ results.ebay = 'error: ReviseItem failed (' + revErr + ')'; }
                      else { results.ebay = 'qty-decremented (was ' + info.quantity + ', now ' + newQty + ', ' + siblingCount + ' sibling(s) remain)'; }
                      after();
                    });
                  });
                }
              }).catch(function(e){
                results.ebay = 'error: sibling check failed (' + e.message + ')';
                after();
              });
            }

            function stepAmazon(after){
              if(!doAmazon){ after(); return; }
              getAmazonAccessToken(function(tokErr, accessToken){
                if(tokErr){ results.amazon = 'error: token ' + tokErr; after(); return; }
                var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
                var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
                deleteAmazonListing(accessToken, sellerId, sku, marketplaceId, function(delErr, delInfo){
                  if(delErr){ results.amazon = 'error: ' + delErr; }
                  else if(delInfo && delInfo.alreadyGone){ results.amazon = 'already-gone'; }
                  else { results.amazon = 'deleted'; }
                  after();
                });
              });
            }

            function stepDb(after){
              database.collection('warehouse_inventory').updateOne(lookupQuery, {
                $set: { status: 'deleted', deletedAt: new Date(), sequenceRetired: true }
              }).then(function(){ after(); }).catch(function(e){ results.dbError = e.message; after(); });
            }

            stepEbay(function(){
              stepAmazon(function(){
                stepDb(function(){
                  res.writeHead(200); res.end(JSON.stringify({
                    success: true,
                    ebay: results.ebay,
                    amazon: results.amazon,
                    dbError: results.dbError
                  }));
                });
              });
            });
          }).catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
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

  // ── Suggested price for warehouse listing tool (Amazon rule engine) ──
  // Calls Amazon SP-API for competing offers on the ASIN, filters by condition
  // per subscriber's repricer config, applies undercut strategy, enforces floor.
  // Returns the same shape of result the repricer produces, so the warehouse
  // tool shows prices consistent with what the repricer would set on day 2.
  //
  // Query: ?code=X&asin=Y&condition=Good
  // Response: {
  //   suggested: Number,           // the price to show in the UI
  //   source: 'amazon-rules' | 'no-amazon-offers' | 'no-matching-condition' | 'error',
  //   reason: String,              // human-readable explanation
  //   diag: {...}                  // same shape as repricer diagnostics
  // }
  if (pathname === '/warehouse/suggest-price' && req.method === 'GET') {
    var spCode = (parsed.query.code || '').toUpperCase();
    var spAsin = (parsed.query.asin || '').trim();
    var spCondition = (parsed.query.condition || 'Good').trim();
    if(!spCode || !spAsin){
      res.writeHead(400); res.end(JSON.stringify({ error: 'Missing code or asin' })); return;
    }
    getSubscriber(spCode, function(subErr, sub){
      if(subErr || !sub){ res.writeHead(200); res.end(JSON.stringify({ error: 'Subscriber not found', suggested: 9.99, source: 'error' })); return; }
      // Use same defaults as runRepricerCycle so behavior is consistent.
      var config = Object.assign({
        floorPrice: 5.99,
        conditionMatch: 'smart',
        undercutStrategy: 'penny',
        fulfillmentFilter: 'fbm-only'
      }, sub.repricer || {});
      var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
      var marketplaceId = AMAZON_MARKETPLACE_ID;

      getAmazonAccessToken(function(tokErr, accessToken){
        if(tokErr){
          res.writeHead(200); res.end(JSON.stringify({ error: 'Amazon auth failed', suggested: 9.99, source: 'error' })); return;
        }
        fetchAmazonOffers(accessToken, marketplaceId, spAsin, function(offErr, offers){
          if(offErr || !offers){
            // No Amazon offers at all — per user's rule, default to $100.
            res.writeHead(200); res.end(JSON.stringify({
              suggested: 100,
              source: 'no-amazon-offers',
              reason: 'Amazon returned no offers for ASIN ' + spAsin + ' — defaulting to $100',
              diag: { asin: spAsin, condition: spCondition, totalOffers: 0 }
            }));
            return;
          }

          var allowedConds = allowedConditionsFor(spCondition, config.conditionMatch);
          var allowedNorm = allowedConds.map(normalizeCondition);
          var filteredOffers = offers.filter(function(o){
            return allowedNorm.indexOf(normalizeCondition(o.condition)) !== -1;
          });

          // Build diagnostic payload in the same shape the repricer uses
          var postFilter = filteredOffers.filter(function(o){ return o.sellerId !== sellerId; });
          if(config.fulfillmentFilter === 'fbm-only'){
            postFilter = postFilter.filter(function(o){ return o.fulfillment === 'FBM'; });
          }
          postFilter.sort(function(a,b){ return a.price - b.price; });
          var diag = {
            asin: spAsin,
            condition: spCondition,
            mySellerId: sellerId,
            configMatch: config.conditionMatch,
            configFulfillment: config.fulfillmentFilter,
            configUndercut: config.undercutStrategy,
            configFloorPrice: config.floorPrice,
            allowedConds: allowedConds,
            totalOffers: offers.length,
            afterConditionFilter: filteredOffers.length,
            afterAllFilters: postFilter.length,
            offersAll: offers.slice(0, 30).map(function(o){
              return { price: o.price, condition: o.condition, fulfillment: o.fulfillment, sellerId: o.sellerId, isMe: o.sellerId === sellerId };
            }),
            competingAfterFilters: postFilter.slice(0, 10).map(function(o){
              return { price: o.price, condition: o.condition, fulfillment: o.fulfillment, sellerId: o.sellerId };
            })
          };

          if(!filteredOffers.length){
            // Amazon has offers but none matching the book's condition.
            res.writeHead(200); res.end(JSON.stringify({
              suggested: 100,
              source: 'no-matching-condition',
              reason: 'No Amazon offers in matching condition bucket — defaulting to $100',
              diag: diag
            }));
            return;
          }

          var result = calcTargetFromOffers(filteredOffers, sellerId, config);
          if(!result){
            // Rules engine couldn't produce a target (e.g. all my own offers)
            res.writeHead(200); res.end(JSON.stringify({
              suggested: 100,
              source: 'no-competing-offers',
              reason: 'After excluding own offers, nothing left to undercut — defaulting to $100',
              diag: diag
            }));
            return;
          }

          // Floor enforcement (same rule as applyGuards but without the 24h
          // window or first-pass checks — those don't apply to a brand-new book).
          var suggested = result.targetPrice;
          var floorEnforced = false;
          if(suggested < config.floorPrice){
            suggested = config.floorPrice;
            floorEnforced = true;
          }
          suggested = Math.round(suggested * 100) / 100;

          diag.rulesOutput = result.targetPrice;
          diag.floorEnforced = floorEnforced;
          diag.finalSuggestion = suggested;

          res.writeHead(200); res.end(JSON.stringify({
            suggested: suggested,
            source: 'amazon-rules',
            reason: result.reason + (floorEnforced ? ' · floor $' + config.floorPrice.toFixed(2) + ' enforced' : ''),
            diag: diag
          }));
        });
      });
    });
    return;
  }

  // ── Debug: Get seller info from token ──
  if (pathname === '/warehouse/check-seller' && req.method === 'GET') {
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: '/sellers/v1/marketplaceParticipations',
        method: 'GET',
        headers: { 'x-amz-access-token': accessToken, 'Accept': 'application/json' }
      };
      var req2 = https.request(opts, function(res2){
        var data = ''; res2.on('data',function(c){data+=c;}); res2.on('end',function(){
          console.log('Seller info:', res2.statusCode, data.substring(0,500));
          res.writeHead(200); res.end(data);
        });
      });
      req2.on('error',function(e){ res.writeHead(200); res.end(JSON.stringify({error:e.message})); });
      req2.end();
    });
    return;
  }

  // ── Debug: Check Amazon listing restrictions for an ASIN ──
  if (pathname === '/warehouse/check-amazon-restrictions' && req.method === 'GET') {
    var asin = parsed.query.asin || '0525559493';
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      console.log('Using AMAZON_SELLER_ID:', AMAZON_SELLER_ID);
      var restrictionsPath = '/listings/2021-08-01/restrictions?marketplaceIds=' + AMAZON_MARKETPLACE_ID + '&sellerId=' + AMAZON_SELLER_ID + '&asin=' + asin + '&conditionType=used_good';
      console.log('Restrictions path:', restrictionsPath);
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: restrictionsPath,
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

  // ── Debug: raw eBay Trading API test (shows full response) ──
  // Hit: /debug-ebay-trading?code=BOOKSFORAGES1!
  // Debug: look up a SKU in warehouse_inventory
  // Hit: /debug-location?code=BOOKSFORAGES1!&sku=SHM.5STL
  // Debug: fetch everything Amazon has for a given seller SKU
  // Hit: /debug-amazon-listing?code=BOOKSFORAGES1!&sku=SHM.5STL
  if (pathname === '/debug-amazon-listing' && req.method === 'GET') {
    var alCode = (parsed.query.code || '').toUpperCase();
    var alSku = parsed.query.sku || '';
    if(!alSku){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing sku query param' })); return; }
    getSubscriber(alCode, function(err, sub){
      if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
      var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;
      getAmazonAccessToken(function(tokenErr, accessToken){
        if(tokenErr){ res.writeHead(200); res.end(JSON.stringify({ error: 'Amazon auth: ' + tokenErr })); return; }
        // Listings Items API — returns everything about this SKU
        var path = '/listings/2021-08-01/items/' + encodeURIComponent(sellerId) + '/' + encodeURIComponent(alSku)
          + '?marketplaceIds=' + marketplaceId
          + '&includedData=summaries,attributes,issues,offers,fulfillmentAvailability,procurement,relationships,productTypes';
        var opts = {
          hostname: 'sellingpartnerapi-na.amazon.com',
          path: path,
          method: 'GET',
          headers: { 'x-amz-access-token': accessToken }
        };
        var aReq = https.request(opts, function(r){
          var data = '';
          r.on('data', function(c){ data += c; });
          r.on('end', function(){
            try {
              var json = JSON.parse(data);
              res.writeHead(200); res.end(JSON.stringify({
                sku: alSku,
                sellerId: sellerId,
                httpStatus: r.statusCode,
                response: json
              }, null, 2));
            } catch(e){
              res.writeHead(200); res.end(JSON.stringify({
                sku: alSku,
                httpStatus: r.statusCode,
                parseError: e.message,
                raw: data.substring(0, 3000)
              }, null, 2));
            }
          });
        });
        aReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: 'Network: ' + e.message })); });
        aReq.setTimeout(20000, function(){ aReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
        aReq.end();
      });
    });
    return;
  }

  if (pathname === '/debug-location' && req.method === 'GET') {
    var dlCode = (parsed.query.code || '').toUpperCase();
    var dlSku = parsed.query.sku || '';
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      // Query BOTH collections — warehouse_inventory (warehouse tool) and listings (eBay tool)
      Promise.all([
        database.collection('warehouse_inventory').find({ sku: dlSku }).toArray(),
        database.collection('listings').find({ sku: dlSku }).toArray()
      ]).then(function(results){
        var warehouseRows = results[0] || [];
        var listingsRows = results[1] || [];
        res.writeHead(200); res.end(JSON.stringify({
          skuQueried: dlSku,
          codeQueried: dlCode,
          warehouse_inventory: {
            matchesFound: warehouseRows.length,
            results: warehouseRows.map(function(r){
              return { sku: r.sku, code: r.code, location: r.location, status: r.status, title: r.title ? r.title.substring(0,60) : '' };
            })
          },
          listings: {
            matchesFound: listingsRows.length,
            results: listingsRows.map(function(r){
              return { sku: r.sku, subscriberCode: r.subscriberCode, location: r.location || null, allFields: Object.keys(r), title: r.title ? r.title.substring(0,60) : '' };
            })
          }
        }, null, 2));
      }).catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
    });
    return;
  }

  if (pathname === '/debug-ebay-trading' && req.method === 'GET') {
    var dCode = (parsed.query.code || '').toUpperCase();
    getSubscriber(dCode, function(err, sub){
      if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var clientId = sub.ebayClientId || CLIENT_ID;
      var clientSecret = sub.ebayClientSecret || CLIENT_SECRET;
      var userToken = sub.ebayUserToken || USER_TOKEN;
      var devId = sub.ebayDevId || DEV_ID;
      var info = {
        hasClientId: !!clientId,  clientIdFirst: clientId ? clientId.substring(0,12) + '...' : null,
        hasClientSecret: !!clientSecret, clientSecretFirst: clientSecret ? clientSecret.substring(0,10) + '...' : null,
        hasUserToken: !!userToken, userTokenLen: userToken ? userToken.length : 0, userTokenFirst: userToken ? userToken.substring(0,20) + '...' : null,
        hasDevId: !!devId, devIdFirst: devId ? devId.substring(0,12) + '...' : null
      };
      var isIafToken2 = userToken && userToken.substring(0,5) === 'v^1.1';
      info.tokenFormat = isIafToken2 ? 'OAuth IAF (X-EBAY-API-IAF-TOKEN header)' : 'Auth\'n\'Auth (XML element)';
      info.hasShippingPolicy = !!sub.ebayShippingPolicyId;
      info.hasPaymentPolicy = !!sub.ebayPaymentPolicyId;
      info.hasReturnPolicy = !!sub.ebayReturnPolicyId;
      var scheduleTime = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
      var sellerProfiles2 = (sub.ebayShippingPolicyId && sub.ebayPaymentPolicyId && sub.ebayReturnPolicyId) ? (
        '<SellerProfiles>'
        + '<SellerShippingProfile><ShippingProfileID>' + sub.ebayShippingPolicyId + '</ShippingProfileID></SellerShippingProfile>'
        + '<SellerReturnProfile><ReturnProfileID>' + sub.ebayReturnPolicyId + '</ReturnProfileID></SellerReturnProfile>'
        + '<SellerPaymentProfile><PaymentProfileID>' + sub.ebayPaymentPolicyId + '</PaymentProfileID></SellerPaymentProfile>'
        + '</SellerProfiles>'
      ) : '';
      var xml = '<?xml version="1.0" encoding="utf-8"?>'
        + '<VerifyAddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
        + (isIafToken2 ? '' : '<RequesterCredentials><eBayAuthToken>' + (userToken || '') + '</eBayAuthToken></RequesterCredentials>')
        + '<Item>'
        + '<Title>Health Check Test Book - Do Not List</Title>'
        + '<Description><![CDATA[Automated health check, not a real listing.]]></Description>'
        + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
        + '<StartPrice>9.99</StartPrice>'
        + '<ConditionID>5000</ConditionID>'
        + '<Country>US</Country><Location>United States</Location><Currency>USD</Currency>'
        + '<DispatchTimeMax>3</DispatchTimeMax>'
        + '<ListingDuration>GTC</ListingDuration>'
        + '<ListingType>FixedPriceItem</ListingType>'
        + '<ScheduleTime>' + scheduleTime + '</ScheduleTime>'
        + sellerProfiles2
        + '<ItemSpecifics>'
        + '<NameValueList><Name>Book Title</Name><Value>Health Check Test</Value></NameValueList>'
        + '<NameValueList><Name>Author</Name><Value>Test</Value></NameValueList>'
        + '<NameValueList><Name>Language</Name><Value>English</Value></NameValueList>'
        + '<NameValueList><Name>Format</Name><Value>Paperback</Value></NameValueList>'
        + '</ItemSpecifics>'
        + '</Item>'
        + '</VerifyAddItemRequest>';
      var dbgHeaders = {
        'X-EBAY-API-SITEID': '0',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
        'X-EBAY-API-CALL-NAME': 'VerifyAddItem',
        'X-EBAY-API-DEV-NAME': devId || '',
        'X-EBAY-API-APP-NAME': clientId || '',
        'X-EBAY-API-CERT-NAME': clientSecret || '',
        'User-Agent': 'BooksForAgesHealthCheck/1.0',
        'Content-Type': 'text/xml',
        'Content-Length': Buffer.byteLength(xml)
      };
      if(isIafToken2) dbgHeaders['X-EBAY-API-IAF-TOKEN'] = userToken;
      var opts = { hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST', headers: dbgHeaders };
      var tReq = https.request(opts, function(r){
        var data = '';
        r.on('data', function(c){ data += c; });
        r.on('end', function(){
          info.httpStatus = r.statusCode;
          info.rawResponse = data.substring(0, 2000);
          res.writeHead(200); res.end(JSON.stringify(info, null, 2));
        });
      });
      tReq.on('error', function(e){ info.networkError = e.message; res.writeHead(200); res.end(JSON.stringify(info, null, 2)); });
      tReq.setTimeout(15000, function(){ tReq.destroy(); info.timeout = true; res.writeHead(200); res.end(JSON.stringify(info, null, 2)); });
      tReq.write(xml); tReq.end();
    });
    return;
  }

  // ── ADMIN: Wipe all test listings for a subscriber (BOTH collections) ──
  // Requires confirm=YES to prevent accidental hits.
  // Hit: /admin/wipe-test-listings?code=BOOKSFORAGES1!&confirm=YES
  if (pathname === '/admin/wipe-test-listings' && req.method === 'GET') {
    var wCode = (parsed.query.code || '').toUpperCase();
    var sessWipe = getRequestSession(req, parsed);
    if(!sessWipe || sessWipe.role !== 'admin' || sessWipe.subscriberCode !== wCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    var wConfirm = parsed.query.confirm || '';
    if (wConfirm !== 'YES') {
      res.writeHead(200); res.end(JSON.stringify({
        error: 'Missing confirm=YES. This endpoint deletes all listings data for the subscriber. Add &confirm=YES to proceed.'
      }));
      return;
    }
    getSubscriber(wCode, function(err, sub){
      if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        Promise.all([
          database.collection('listings').deleteMany({ subscriberCode: wCode }),
          database.collection('warehouse_inventory').deleteMany({ code: wCode })
        ]).then(function(results){
          res.writeHead(200); res.end(JSON.stringify({
            success: true,
            subscriberCode: wCode,
            deleted: {
              ebayToolListings: results[0].deletedCount || 0,
              warehouseToolListings: results[1].deletedCount || 0
            },
            note: 'Database records only. Actual Amazon/eBay listings on those platforms are untouched.'
          }));
        }).catch(function(e){
          res.writeHead(200); res.end(JSON.stringify({ error: e.message }));
        });
      });
    });
    return;
  }

  // ── Debug: Check eBay token status and test against eBay API ──
  // Hit: /debug-ebay-token?code=BOOKSFORAGES1!
  if (pathname === '/debug-ebay-token' && req.method === 'GET') {
    var eCode = (parsed.query.code || '').toUpperCase();
    getSubscriber(eCode, function(err, sub){
      if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }

      var result = {
        subscriberCode: eCode,
        hasOAuthToken: !!sub.ebayOAuthToken,
        hasRefreshToken: !!sub.ebayRefreshToken,
        hasUserToken: !!sub.ebayUserToken,
        oauthExpiry: sub.ebayOAuthExpiry || null,
        oauthExpiryPassed: null,
        tokenFirstChars: null,
        tokenUsedInTest: null,
        testCall: { status: null, error: null, response: null }
      };

      if(sub.ebayOAuthExpiry){
        result.oauthExpiryPassed = new Date(sub.ebayOAuthExpiry) < new Date();
      }

      // Which token will actually be used for sales calls
      var tokenToTest = sub.ebayOAuthToken || sub.ebayUserToken || USER_TOKEN;
      result.tokenUsedInTest = sub.ebayOAuthToken ? 'ebayOAuthToken (OAuth)'
                             : sub.ebayUserToken ? 'ebayUserToken (legacy)'
                             : 'USER_TOKEN (server default)';
      result.tokenFirstChars = tokenToTest ? (tokenToTest.substring(0, 20) + '...') : null;

      if(!tokenToTest){
        result.testCall.error = 'No token found — subscriber has never connected eBay';
        res.writeHead(200); res.end(JSON.stringify(result, null, 2)); return;
      }

      // Make a tiny test call to eBay: fetch 1 order from last 24hrs
      var yesterdayIso = new Date(Date.now() - 86400000).toISOString();
      var testPath = '/sell/fulfillment/v1/order?filter='
                   + encodeURIComponent('creationdate:[' + yesterdayIso + '..]')
                   + '&limit=1';
      var testReq = https.request({
        hostname: 'api.ebay.com',
        path: testPath,
        method: 'GET',
        headers: {
          'Authorization': 'Bearer ' + tokenToTest,
          'Content-Type': 'application/json',
          'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
      }, function(r){
        var data = '';
        r.on('data', function(c){ data += c; });
        r.on('end', function(){
          result.testCall.status = r.statusCode;
          try {
            var json = JSON.parse(data);
            result.testCall.response = json;
            if(json.errors && json.errors[0]){
              result.testCall.error = json.errors[0].message || json.errors[0].longMessage;
            }
          } catch(e){
            result.testCall.response = data.substring(0, 300);
            result.testCall.error = 'Non-JSON response';
          }

          // Diagnose
          if(r.statusCode === 200){
            result.diagnosis = 'TOKEN OK — eBay API call succeeded. Sales data should be available.';
          } else if(r.statusCode === 401){
            result.diagnosis = 'TOKEN REJECTED BY EBAY — token is expired, revoked, or malformed. Reconnect via portal.';
          } else if(r.statusCode === 403){
            result.diagnosis = 'TOKEN LACKS SCOPE — token is valid but missing sell.fulfillment scope. Reconnect to request all scopes.';
          } else if(r.statusCode === 400){
            result.diagnosis = 'BAD REQUEST — token may be fine, but request parameters rejected.';
          } else {
            result.diagnosis = 'Unexpected status ' + r.statusCode;
          }
          res.writeHead(200); res.end(JSON.stringify(result, null, 2));
        });
      });
      testReq.on('error', function(e){
        result.testCall.error = 'Network error: ' + e.message;
        res.writeHead(200); res.end(JSON.stringify(result, null, 2));
      });
      testReq.setTimeout(15000, function(){
        testReq.destroy();
        result.testCall.error = 'Request timeout';
        res.writeHead(200); res.end(JSON.stringify(result, null, 2));
      });
      testReq.end();
    });
    return;
  }

  // ── Debug: Read back what Amazon has stored for a SKU (includes issues + attributes) ──
  // Hit: /warehouse/debug-amazon-getitem?sku=bfa.4r3q
  if (pathname === '/warehouse/debug-amazon-getitem' && req.method === 'GET') {
    var gSku = parsed.query.sku || '';
    if(!gSku){ res.writeHead(400); res.end(JSON.stringify({ error: 'sku required' })); return; }
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      var gPath = '/listings/2021-08-01/items/' + encodeURIComponent(AMAZON_SELLER_ID) + '/' + encodeURIComponent(gSku)
                + '?marketplaceIds=' + AMAZON_MARKETPLACE_ID
                + '&includedData=summaries,attributes,issues,offers,fulfillmentAvailability';
      var gReq = https.request({
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: gPath,
        method: 'GET',
        headers: { 'x-amz-access-token': accessToken, 'Accept': 'application/json' }
      }, function(gRes){
        var gData = ''; gRes.on('data', function(c){ gData += c; });
        gRes.on('end', function(){
          console.log('Amazon GET item:', gRes.statusCode, gData.substring(0, 2000));
          var gJson; try { gJson = JSON.parse(gData); } catch(e){ gJson = { raw: gData }; }
          res.writeHead(200); res.end(JSON.stringify({ status: gRes.statusCode, item: gJson }, null, 2));
        });
      });
      gReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      gReq.end();
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

  // ── Debug: Test Amazon listing in VALIDATION_PREVIEW mode ──
  if (pathname === '/warehouse/test-amazon-validate' && req.method === 'GET') {
    var testAsin = parsed.query.asin || '0525559493';
    var testSku = 'test-validate-' + Date.now();
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      var body = JSON.stringify({
        productType: 'PRODUCT',
        requirement: 'LISTING_OFFER_ONLY',
        attributes: {
          merchant_suggested_asin: [{ value: testAsin, marketplace_id: AMAZON_MARKETPLACE_ID }],
          condition_type: [{ value: 'used_good', marketplace_id: AMAZON_MARKETPLACE_ID }],
          merchant_shipping_group: [{ value: 'Base shipping - standard', marketplace_id: AMAZON_MARKETPLACE_ID }],
          fulfillment_availability: [{ fulfillment_channel_code: 'DEFAULT', quantity: 1 }],
          purchasable_offer: [{
            marketplace_id: AMAZON_MARKETPLACE_ID,
            currency: 'USD',
            our_price: [{ schedule: [{ value_with_tax: 8.99 }] }]
          }]
        }
      });
      var path = '/listings/2021-08-01/items/' + AMAZON_SELLER_ID + '/' + encodeURIComponent(testSku) + '?marketplaceIds=' + AMAZON_MARKETPLACE_ID;
      console.log('Validation path:', path);
      console.log('Validation body:', body);
      var opts = {
        hostname: 'sellingpartnerapi-na.amazon.com',
        path: path,
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
          console.log('Validation response:', amzRes.statusCode, amzData);
          res.writeHead(200); res.end(JSON.stringify({ status: amzRes.statusCode, response: JSON.parse(amzData || '{}') }));
        });
      });
      amzReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      amzReq.setTimeout(15000, function(){ amzReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
      amzReq.write(body); amzReq.end();
    });
    return;
  }

  // ── Debug: Test Amazon listing directly ──
  if (pathname === '/warehouse/test-amazon-list' && req.method === 'GET') {
    var testAsin = parsed.query.asin || '0525559493';
    var testSku = 'test-sku-' + Date.now();
    getAmazonAccessToken(function(err, accessToken){
      if(err){ res.writeHead(200); res.end(JSON.stringify({ error: err })); return; }
      var body = JSON.stringify({
        productType: 'PRODUCT',
        requirement: 'LISTING_OFFER_ONLY',
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
          'x-amzn-api-version': '2021-08-01',
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
          var conditionMap = {'New':'New','Like New':'UsedLikeNew','Very Good':'UsedVeryGood','Good':'UsedGood','Acceptable':'UsedAcceptable'};
          var condition = conditionMap[data.conditionLabel] || 'UsedGood';
          var asin = data.asin || '';
          var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
          var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;

          if(!asin){ res.writeHead(200); res.end(JSON.stringify({ error: 'No ASIN available' })); return; }

          var conditionMap2 = {'New':'new_new','Like New':'used_like_new','Very Good':'used_very_good','Good':'used_good','Acceptable':'used_acceptable'};
          var condition2 = conditionMap2[data.conditionLabel] || 'used_good';
          // Note: we don't send main_product_image_locator — Amazon's API ignores it for
          // ABIS_BOOK (catalog image is used from the matched ASIN). Sending it produces
          // a WARNING on every response. When ASIN match succeeds, Amazon's own catalog
          // image is used. When it fails, the listing flags "missing info" and we'd need
          // a different product type anyway.
          //
          // purchasable_offer MUST include audience:"ALL" for B2C retail offers. Per
          // Amazon's docs: "only the offer record that matches the specified audience,
          // currency, and marketplace_id is updated" — audience is a KEY identifier.
          // Without it, the offer has no audience context and Amazon treats the listing
          // as having no retail offer (Seller Central shows "Missing offer" even though
          // the API returns ACCEPTED with no validation issues).
          // Other valid audience values: "B2B" (Amazon Business), "BZR" (Amazon Haul).
          var body = JSON.stringify({
            productType: 'ABIS_BOOK',
            requirements: 'LISTING_OFFER_ONLY',
            attributes: {
              merchant_suggested_asin: [{ value: asin, marketplace_id: marketplaceId }],
              condition_type: [{ value: condition2, marketplace_id: marketplaceId }],
              fulfillment_availability: [{ fulfillment_channel_code: 'DEFAULT', quantity: 1 }],
              purchasable_offer: [{
                marketplace_id: marketplaceId,
                currency: 'USD',
                audience: 'ALL',
                our_price: [{ schedule: [{ value_with_tax: parseFloat(price) }] }]
              }]
            }
          });
          console.log('Amazon PUT body:', body);
          var opts = {
            hostname: 'sellingpartnerapi-na.amazon.com',
            path: '/listings/2021-08-01/items/' + sellerId + '/' + encodeURIComponent(sku) + '?marketplaceIds=' + marketplaceId,
            method: 'PUT',
            headers: {
              'Content-Type': 'application/json',
              'x-amz-access-token': accessToken,
              'x-amzn-api-version': '2021-08-01',
              'Content-Length': Buffer.byteLength(body)
            }
          };
          var amzReq = https.request(opts, function(amzRes){
            var amzData = ''; amzRes.on('data',function(c){amzData+=c;}); amzRes.on('end',function(){
              console.log('Amazon PUT response:', amzRes.statusCode, amzData);
              try{
                var json = JSON.parse(amzData);
                if(json.status === 'ACCEPTED'){
                  res.writeHead(200); res.end(JSON.stringify({ success: true, asin: asin, submissionId: json.submissionId }));
                } else {
                  var errMsg = (json.errors && json.errors[0] && json.errors[0].message) || (json.issues && json.issues[0] && json.issues[0].message) || amzData;
                  res.writeHead(200); res.end(JSON.stringify({ error: errMsg }));
                }
              }catch(e){ res.writeHead(200); res.end(JSON.stringify({ error: amzData })); }
            });
          });
          amzReq.on('error',function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
          amzReq.setTimeout(30000,function(){ amzReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
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
        // Listing quantity. For multi-qty FixedPriceItem, N identical copies share
        // one listing. Default 1 for backwards compatibility with all existing callers.
        var listingQty = Math.max(1, Math.min(99, parseInt(data.quantity, 10) || 1));

        // Build picture URLs — eBay requires at least 1 photo and requires ≥500px on
        // the longest side. Amazon's catalog image is sometimes under 500px for older
        // books. rehostCoverForEbay tries OpenLibrary (800+px) first using the ISBN,
        // falls back to Amazon, then uploads the best one to eBay's picture hosting.
        rehostCoverForEbay(data.coverUrl || '', data.isbn || '', userToken, devId, function(rehostErr, effectiveCoverUrl){
          var pictureXml = '';
          if(effectiveCoverUrl){
            pictureXml = '<PictureDetails><GalleryType>Gallery</GalleryType><PictureURL>' + esc(effectiveCoverUrl) + '</PictureURL></PictureDetails>';
          } else {
            pictureXml = '<PictureDetails><GalleryType>Gallery</GalleryType></PictureDetails>';
          }

        var xml = '<?xml version="1.0" encoding="utf-8"?>'
          + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
          + '<RequesterCredentials><eBayAuthToken>' + userToken + '</eBayAuthToken></RequesterCredentials>'
          + '<Item>'
          + '<Title>' + esc(data.title || '').substring(0,80) + '</Title>'
          + '<Description><![CDATA[' + desc + ']]></Description>'
          + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
          + '<StartPrice>' + parseFloat(data.price || 9.99).toFixed(2) + '</StartPrice>'
          + '<Quantity>' + listingQty + '</Quantity>'
          + '<ConditionID>' + conditionId + '</ConditionID>'
          + '<Country>US</Country>'
          + '<Location>United States</Location>'
          + '<Currency>USD</Currency>'
          + '<DispatchTimeMax>2</DispatchTimeMax>'
          + '<ListingDuration>GTC</ListingDuration>'
          + '<ListingType>FixedPriceItem</ListingType>'
          + pictureXml
          + '<SKU>' + esc(data.sku || '') + '</SKU>'
          + '<BestOfferDetails><BestOfferEnabled>true</BestOfferEnabled></BestOfferDetails>'
          + '<SellerProfiles>'
          + '<SellerShippingProfile><ShippingProfileID>' + shippingPolicyId + '</ShippingProfileID></SellerShippingProfile>'
          + '<SellerReturnProfile><ReturnProfileID>' + returnPolicyId + '</ReturnProfileID></SellerReturnProfile>'
          + '<SellerPaymentProfile><PaymentProfileID>' + paymentPolicyId + '</PaymentProfileID></SellerPaymentProfile>'
          + '</SellerProfiles>'
          + '<ItemSpecifics>'
          + '<NameValueList><Name>Book Title</Name><Value>' + esc((data.bookTitle || data.title || '').replace(/^—+$/, '').substring(0,65) || 'See description') + '</Value></NameValueList>'
          + '<NameValueList><Name>Author</Name><Value>' + esc((data.author && data.author.replace(/^—+$/, '')) || 'Unknown').substring(0,65) + '</Value></NameValueList>'
          + '<NameValueList><Name>Language</Name><Value>' + esc(data.language && data.language.length > 1 ? data.language.charAt(0).toUpperCase() + data.language.slice(1).toLowerCase() : 'English') + '</Value></NameValueList>'
          + (data.publisher ? '<NameValueList><Name>Publisher</Name><Value>' + esc(data.publisher).substring(0,65) + '</Value></NameValueList>' : '')
          + (data.year ? '<NameValueList><Name>Publication Year</Name><Value>' + esc(data.year) + '</Value></NameValueList>' : '')
          + (data.format ? '<NameValueList><Name>Format</Name><Value>' + esc(data.format) + '</Value></NameValueList>' : '')
          + (data.edition ? '<NameValueList><Name>Edition</Name><Value>' + esc(data.edition) + '</Value></NameValueList>' : '')
          + (data.pages ? '<NameValueList><Name>Number of Pages</Name><Value>' + esc(String(data.pages)) + '</Value></NameValueList>' : '')
          + (data.series ? '<NameValueList><Name>Series</Name><Value>' + esc(data.series).substring(0,65) + '</Value></NameValueList>' : '')
          + '</ItemSpecifics>'
          + (data.isbn ? '<ProductListingDetails><ISBN>' + esc(data.isbn) + '</ISBN><IncludeeBayProductDetails>false</IncludeeBayProductDetails><UseStockPhotoURLAsGallery>false</UseStockPhotoURLAsGallery></ProductListingDetails>' : '')
          + '</Item>'
          + '</AddItemRequest>';
        console.log('Warehouse eBay XML ItemSpecifics:', xml.substring(xml.indexOf('<ItemSpecifics>'), xml.indexOf('</ItemSpecifics>') + 16));
        console.log('Warehouse eBay Quantity=' + listingQty);
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
              return;
            }

            // AddItem failed. Check whether it was a "duplicate listing" rejection,
            // which happens when the seller already has an active listing for the
            // same book. eBay embeds the existing ItemID inside the LongMessage in
            // parens: "... you already have on eBay: Title (327118721621). We don't
            // allow...". When that pattern matches, auto-merge: GetItem to read the
            // current Quantity, ReviseItem to bump it by N. Return the existing
            // ItemID so the client saves all N new MongoDB records against it.
            var errMsg = errMatch ? errMatch[1] : 'Unknown eBay error';
            var dupIdMatch = errMsg.match(/\((\d{9,})\)/);
            var looksLikeDuplicate = /already have on eBay|identical items from the same seller/i.test(errMsg);
            if(dupIdMatch && looksLikeDuplicate){
              var existingItemId = dupIdMatch[1];
              console.log('[list-ebay] Duplicate detected, attempting merge into existing ItemID ' + existingItemId + ' (adding ' + listingQty + ' to current qty)');
              getEbayItemQuantity(code, existingItemId, function(getErr, info){
                if(getErr || !info){
                  console.log('[list-ebay] GetItem on existing listing failed: ' + getErr);
                  res.writeHead(200); res.end(JSON.stringify({ error: errMsg + ' — auto-merge failed (GetItem): ' + (getErr || 'no data') }));
                  return;
                }
                var newQty = info.quantity + listingQty;
                console.log('[list-ebay] Existing qty=' + info.quantity + ' sold=' + info.sold + ' → setting to ' + newQty);
                reviseEbayItemQuantity(code, existingItemId, newQty, function(revErr){
                  if(revErr){
                    console.log('[list-ebay] ReviseItem failed: ' + revErr);
                    res.writeHead(200); res.end(JSON.stringify({ error: errMsg + ' — auto-merge failed (ReviseItem): ' + revErr }));
                    return;
                  }
                  console.log('[list-ebay] Merge succeeded — existing ItemID ' + existingItemId + ' now has qty ' + newQty);
                  res.writeHead(200); res.end(JSON.stringify({
                    success: true,
                    ebayItemId: existingItemId,
                    mergedExisting: true,
                    previousQuantity: info.quantity,
                    newQuantity: newQty
                  }));
                });
              });
              return;
            }

            // Regular (non-duplicate) error — surface as-is.
            res.writeHead(200); res.end(JSON.stringify({ error: errMsg }));
          });
        });
        ebayReq.on('error', function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
        ebayReq.setTimeout(30000, function(){ ebayReq.destroy(); res.writeHead(200); res.end(JSON.stringify({ error: 'Timeout' })); });
        ebayReq.write(xml);
        ebayReq.end();
        }); // close rehostCoverForEbay callback
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
      // Find highest sequence number for this row+section. Exclude deleted records
      // so that deleting the NEWEST slot (the current max) frees that sequence for
      // reuse on the next scan. Deleted slots BELOW the max stay as permanent gaps
      // (next is still max+1). Sold records stay counted — we don't reuse sold slots.
      database.collection('warehouse_inventory').find({
        code: code,
        'location.row': row,
        'location.section': section,
        status: { $ne: 'deleted' }
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
          // Save every field the client sends. Previously we only saved a subset
          // which meant ebayItemId and asin were silently dropped — breaking the
          // delete tool's platform-cleanup logic (it uses those IDs to call EndItem
          // and DELETE against the platforms). Also save coverUrl so we can rebuild
          // the Recent Items list with images on reload and so the image is
          // available for future cleanup/reprint operations.
          var item = {
            code: code,
            sku: data.sku,
            isbn: data.isbn || '',
            asin: data.asin || '',
            ebayItemId: data.ebayItemId || '',
            title: data.title || '',
            author: data.author || '',
            publisher: data.publisher || '',
            year: data.year || '',
            format: data.format || '',
            language: data.language || '',
            pages: data.pages || '',
            edition: data.edition || '',
            series: data.series || '',
            condition: data.condition || 'Good',
            price: parseFloat(data.price) || 9.99,
            location: data.location || {},
            listedOn: data.listedOn || [],
            coverUrl: data.coverUrl || '',
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
        database.collection('warehouse_inventory').find({ code: code, source: { $ne: 'csv-import' } }).sort({ createdAt: -1 }).limit(6).toArray()
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
  // ── Subscriber: tool health status (for portal status indicators) ──
  if (pathname === '/my/tool-status' && req.method === 'GET') {
    var tCode = (parsed.query.code || '').toUpperCase();
    var forceRun = parsed.query.refresh === '1';
    getSubscriber(tCode, function(err, sub){
      if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
      var cached = toolHealthCache[tCode];
      // If no cached result yet or user forced refresh, kick off a check now and return whatever we have
      if(!cached || forceRun){
        runHealthCheckForSubscriber(sub);
        // If nothing cached at all, return "checking" state
        if(!cached){
          res.writeHead(200); res.end(JSON.stringify({
            ebayListingTool: { ok: null, errors: [], status: 'checking' },
            warehouseTool:   { ok: null, errors: [], status: 'checking' },
            checkedAt: null
          }));
          return;
        }
      }
      res.writeHead(200); res.end(JSON.stringify(cached));
    });
    return;
  }

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
        var utcStartDate = new Date(utcStart);
        var utcEndDate = new Date(utcEnd);
        // Query BOTH collections in parallel
        Promise.all([
          database.collection('listings').find({
            subscriberCode: code,
            createdAt: { $gte: utcStart, $lt: utcEnd }
          }).sort({ createdAt: -1 }).toArray(),
          database.collection('warehouse_inventory').find({
            code: code,
            createdAt: { $gte: utcStartDate, $lt: utcEndDate },
            source: { $ne: 'csv-import' },  // Exclude bulk-imported records from daily listing counts
            status: { $ne: 'deleted' }       // Exclude records deleted after listing
          }).sort({ createdAt: -1 }).toArray()
        ]).then(function(results){
          var ebayToolItems = (results[0] || []).map(function(l){ l.source = 'ebay-tool'; return l; });
          var warehouseToolItems = (results[1] || []).map(function(l){
            // normalize shape so frontend can render the same way
            return {
              source: 'warehouse-tool',
              subscriberCode: l.code,
              employee: l.employee || '',
              bookTitle: l.title || '',
              title: l.title || '',
              condition: l.condition || '',
              price: l.price || 0,
              isbn: l.isbn || '',
              sku: l.sku || '',
              date: (l.createdAt instanceof Date ? l.createdAt.toISOString() : l.createdAt || '').split('T')[0],
              createdAt: (l.createdAt instanceof Date ? l.createdAt.toISOString() : l.createdAt),
              listedOn: l.listedOn || []
            };
          });
          var merged = ebayToolItems.concat(warehouseToolItems)
            .sort(function(a,b){ return (b.createdAt || '').localeCompare(a.createdAt || ''); });
          res.writeHead(200); res.end(JSON.stringify(merged));
        }).catch(function(){ res.writeHead(200); res.end('[]'); });
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
        if (err || !database) { res.writeHead(200); res.end(JSON.stringify({ count: 0, ebayTool: 0, warehouseTool: 0 })); return; }
        // Calculate start and end of current local month in UTC
        var now = new Date();
        var localNow = new Date(now.getTime() - offsetMinutes * 60000);
        var monthStart = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth(), 1));
        var monthEnd = new Date(Date.UTC(localNow.getUTCFullYear(), localNow.getUTCMonth() + 1, 1));
        var utcStart = new Date(monthStart.getTime() + offsetMinutes * 60000).toISOString();
        var utcEnd = new Date(monthEnd.getTime() + offsetMinutes * 60000).toISOString();
        var utcStartDate = new Date(utcStart);
        var utcEndDate = new Date(utcEnd);
        Promise.all([
          database.collection('listings').countDocuments({
            subscriberCode: code,
            createdAt: { $gte: utcStart, $lt: utcEnd }
          }),
          database.collection('warehouse_inventory').countDocuments({
            code: code,
            createdAt: { $gte: utcStartDate, $lt: utcEndDate },
            source: { $ne: 'csv-import' },  // exclude bulk-imported records
            status: { $ne: 'deleted' }       // exclude records deleted after listing
          })
        ]).then(function(results){
          var ebayTool = results[0] || 0;
          var warehouseTool = results[1] || 0;
          res.writeHead(200); res.end(JSON.stringify({
            count: ebayTool + warehouseTool,  // kept for backwards compat
            ebayTool: ebayTool,
            warehouseTool: warehouseTool
          }));
        }).catch(function() { res.writeHead(200); res.end(JSON.stringify({ count: 0, ebayTool: 0, warehouseTool: 0 })); });
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
        var utcStartDate = new Date(utcStart);
        var utcEndDate = new Date(utcEnd);
        var weekLabel = 'Week of ' + monday.toISOString().split('T')[0] + ' – ' + new Date(nextMonday - 86400000).toISOString().split('T')[0];
        Promise.all([
          database.collection('listings').find({
            subscriberCode: code,
            createdAt: { $gte: utcStart, $lt: utcEnd }
          }).sort({ createdAt: -1 }).toArray(),
          database.collection('warehouse_inventory').find({
            code: code,
            createdAt: { $gte: utcStartDate, $lt: utcEndDate },
            source: { $ne: 'csv-import' },  // Exclude bulk-imported records from weekly counts
            status: { $ne: 'deleted' }       // Exclude records deleted after listing
          }).sort({ createdAt: -1 }).toArray()
        ]).then(function(results){
          var ebayToolItems = (results[0] || []).map(function(l){ l.source = 'ebay-tool'; return l; });
          var warehouseToolItems = (results[1] || []).map(function(l){
            return {
              source: 'warehouse-tool',
              subscriberCode: l.code,
              employee: l.employee || '',
              bookTitle: l.title || '',
              title: l.title || '',
              condition: l.condition || '',
              price: l.price || 0,
              isbn: l.isbn || '',
              sku: l.sku || '',
              date: (l.createdAt instanceof Date ? l.createdAt.toISOString() : l.createdAt || '').split('T')[0],
              createdAt: (l.createdAt instanceof Date ? l.createdAt.toISOString() : l.createdAt),
              listedOn: l.listedOn || []
            };
          });
          var merged = ebayToolItems.concat(warehouseToolItems)
            .sort(function(a,b){ return (b.createdAt || '').localeCompare(a.createdAt || ''); });
          res.writeHead(200); res.end(JSON.stringify({ listings: merged, weekLabel: weekLabel }));
        }).catch(function() { res.writeHead(200); res.end(JSON.stringify({ listings: [], weekLabel: weekLabel })); });
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
        var utcStartDate = new Date(utcStart);
        var utcEndDate = new Date(utcEnd);
        Promise.all([
          database.collection('listings').find({
            subscriberCode: code,
            createdAt: { $gte: utcStart, $lt: utcEnd }
          }).toArray(),
          database.collection('warehouse_inventory').find({
            code: code,
            createdAt: { $gte: utcStartDate, $lt: utcEndDate },
            source: { $ne: 'csv-import' },
            status: { $ne: 'deleted' }
          }).toArray()
        ]).then(function(results){
          var ebayToolItems = (results[0] || []).map(function(l){ l.source = 'ebay-tool'; return l; });
          var warehouseToolItems = (results[1] || []).map(function(l){
            return {
              source: 'warehouse-tool',
              subscriberCode: l.code,
              employee: l.employee || '',
              bookTitle: l.title || '',
              title: l.title || '',
              condition: l.condition || '',
              price: l.price || 0,
              isbn: l.isbn || '',
              sku: l.sku || '',
              date: (l.createdAt instanceof Date ? l.createdAt.toISOString() : l.createdAt || '').split('T')[0],
              createdAt: (l.createdAt instanceof Date ? l.createdAt.toISOString() : l.createdAt),
              listedOn: l.listedOn || []
            };
          });
          var merged = ebayToolItems.concat(warehouseToolItems);
          res.writeHead(200); res.end(JSON.stringify({ listings: merged }));
        }).catch(function() { res.writeHead(200); res.end(JSON.stringify({ listings: [] })); });
      });
    });
    return;
  }

  // ── Subscriber self-service: update own settings ──
  // ───────────────────────────────────────────────────────────
  // BULK WIPE — Warehouse Tool Listings
  // ───────────────────────────────────────────────────────────
  // Warehouse-tool listings are those inserted via /warehouse/item (no `source` field).
  // CSV imports have `source: 'csv-import'`; backfilled SKU→location mappings have
  // `backfilled: true`. We exclude both so we never touch bulk-imported inventory or
  // legacy location mappings.
  //
  // Filter used in both endpoints (must match exactly):
  //   { code: X, source: { $ne: 'csv-import' }, backfilled: { $ne: true } }
  //
  // Two endpoints:
  //   GET  /my/warehouse-listings/preview?code=X  — count + sample list (safe, read-only)
  //   POST /my/warehouse-listings/delete          — destructive; requires typed confirm
  //
  // Database records only. Does NOT touch live Amazon/eBay listings.

  if (pathname === '/my/warehouse-listings/preview' && req.method === 'GET') {
    var pCode = (parsed.query.code || '').toUpperCase();
    var pSess = getRequestSession(req, parsed);
    if(!pSess || pSess.role !== 'admin' || pSess.subscriberCode !== pCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    // Optional scope filters — caller sends ISO timestamps (computed client-side
    // using browser local time so "today"/"yesterday" respect the user's timezone)
    // OR a specific SKU to look up (from scan/type input).
    var pSinceIso = parsed.query.sinceIso || null;
    var pUntilIso = parsed.query.untilIso || null;
    var pSku = (parsed.query.sku || '').trim();
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      // Base filter — never touch CSV imports or backfilled location-only stubs.
      var warehouseFilter = { code: pCode, source: { $ne: 'csv-import' }, backfilled: { $ne: true } };
      // SKU lookup takes precedence (exact match) over date range
      if(pSku){
        warehouseFilter.sku = pSku;
      } else {
        // Layer on the createdAt bounds if provided.
        var dateBounds = {};
        if(pSinceIso){ try { dateBounds.$gte = new Date(pSinceIso); } catch(e){} }
        if(pUntilIso){ try { dateBounds.$lte = new Date(pUntilIso); } catch(e){} }
        if(dateBounds.$gte || dateBounds.$lte){ warehouseFilter.createdAt = dateBounds; }
      }

      // Bump sample limit to 500 so the UI can show the full list for typical test batches.
      var SAMPLE_LIMIT = 500;
      Promise.all([
        database.collection('warehouse_inventory').countDocuments(warehouseFilter),
        database.collection('warehouse_inventory').countDocuments({ code: pCode, source: 'csv-import' }),
        database.collection('warehouse_inventory').countDocuments({ code: pCode, backfilled: true }),
        database.collection('warehouse_inventory').find(warehouseFilter)
          .project({ sku:1, title:1, author:1, createdAt:1, listedOn:1, status:1, asin:1, ebayItemId:1 })
          .sort({ createdAt: -1 })
          .limit(SAMPLE_LIMIT)
          .toArray(),
        // Status breakdown so Adam can see active vs sold vs deleted before nuking
        database.collection('warehouse_inventory').aggregate([
          { $match: warehouseFilter },
          { $group: { _id: '$status', count: { $sum: 1 } } }
        ]).toArray()
      ]).then(function(results){
        var statusBreakdown = {};
        (results[4] || []).forEach(function(s){ statusBreakdown[s._id || 'unknown'] = s.count; });
        res.writeHead(200); res.end(JSON.stringify({
          totalToDelete: results[0],
          willPreserve: {
            csvImports: results[1],
            backfilled: results[2]
          },
          statusBreakdown: statusBreakdown,
          scope: { sinceIso: pSinceIso, untilIso: pUntilIso, sku: pSku || null },
          sample: results[3] || [],
          sampleNote: results[3].length < results[0]
            ? 'Showing ' + results[3].length + ' most recent of ' + results[0] + ' total'
            : 'Showing all ' + results[0] + ' records'
        }));
      }).catch(function(e){
        res.writeHead(200); res.end(JSON.stringify({ error: e.message }));
      });
    });
    return;
  }

  if (pathname === '/my/warehouse-listings/delete' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var wCode = (data.code || '').toUpperCase();
      var wSess = getRequestSession(req, parsed);
      if(!wSess || wSess.role !== 'admin' || wSess.subscriberCode !== wCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      // Typed confirmation required — prevents accidental nukes from replay / bad clients.
      if(data.confirm !== 'DELETE WAREHOUSE'){
        res.writeHead(400); res.end(JSON.stringify({
          error: 'Confirmation required. Send { confirm: "DELETE WAREHOUSE" } to proceed.'
        }));
        return;
      }

      // Optional scope filters (must match the preview that was just shown).
      var wSinceIso = data.sinceIso || null;
      var wUntilIso = data.untilIso || null;
      var wSku = (data.sku || '').trim();
      // When true, we also end the live eBay listing and delete the Amazon listing
      // before removing the MongoDB record. When false, DB-only (legacy behavior).
      var killLive = !!data.killLive;

      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var warehouseFilter = { code: wCode, source: { $ne: 'csv-import' }, backfilled: { $ne: true } };
        // SKU lookup takes precedence over date range (matches preview behavior)
        if(wSku){
          warehouseFilter.sku = wSku;
        } else {
          var dateBounds = {};
          if(wSinceIso){ try { dateBounds.$gte = new Date(wSinceIso); } catch(e){} }
          if(wUntilIso){ try { dateBounds.$lte = new Date(wUntilIso); } catch(e){} }
          if(dateBounds.$gte || dateBounds.$lte){ warehouseFilter.createdAt = dateBounds; }
        }

        // ── DB-ONLY PATH (backwards-compatible, fast) ──
        if(!killLive){
          database.collection('warehouse_inventory').deleteMany(warehouseFilter)
            .then(function(r){
              res.writeHead(200); res.end(JSON.stringify({
                success: true,
                deleted: r.deletedCount || 0,
                note: 'MongoDB records removed. Live Amazon/eBay listings on those platforms are untouched.'
              }));
            })
            .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
          return;
        }

        // ── FULL CLEANUP PATH (DB + platforms) ──
        // Need the full record list so we know each item's ebayItemId / asin / listedOn.
        database.collection('warehouse_inventory').find(warehouseFilter).toArray()
          .then(function(items){
            if(!items.length){
              res.writeHead(200); res.end(JSON.stringify({
                success: true, deleted: 0, results: [], killLive: true,
                note: 'Nothing matched the filter.'
              }));
              return;
            }

            getSubscriber(wCode, function(err, sub){
              if(!sub){ res.writeHead(403); res.end(JSON.stringify({ error: 'Invalid code' })); return; }
              var sellerId = sub.amazonSellerId || AMAZON_SELLER_ID;
              var marketplaceId = sub.amazonMarketplaceId || AMAZON_MARKETPLACE_ID;

              // One Amazon access token for the whole batch (valid 1hr, plenty).
              getAmazonAccessToken(function(tokErr, accessToken){
                if(tokErr) accessToken = null; // we'll still try the rest

                // Batch-level state for sibling-aware eBay cleanup. Multiple records
                // in this batch may share the same ebayItemId (multi-qty listings).
                // We only want to hit eBay once per unique ItemID:
                //   - batchIdSet: Set of _ids being deleted, used to exclude them
                //     from the "external siblings" count on eBay cleanup
                //   - handledEbayIds: ItemIDs already processed — subsequent records
                //     sharing that ItemID get marked without redundant API calls
                var batchIdSet = new Set(items.map(function(x){ return String(x._id); }));
                var handledEbayIds = {};  // ebayItemId → result string to reuse

                var results = [];
                var idx = 0;
                function processNext(){
                  if(idx >= items.length){
                    // Done with per-item cleanup → now bulk-delete the MongoDB records.
                    var idsToDelete = results.filter(function(r){ return r._id; }).map(function(r){ return r._id; });
                    if(!idsToDelete.length){
                      res.writeHead(200); res.end(JSON.stringify({
                        success: true, deleted: 0, results: results, killLive: true
                      }));
                      return;
                    }
                    database.collection('warehouse_inventory').deleteMany({ _id: { $in: idsToDelete } })
                      .then(function(delResult){
                        // Mark each result row with the DB outcome.
                        results.forEach(function(r){ r.db = delResult.deletedCount ? 'deleted' : 'not-found'; });
                        res.writeHead(200); res.end(JSON.stringify({
                          success: true,
                          deleted: delResult.deletedCount || 0,
                          total: items.length,
                          killLive: true,
                          results: results.map(function(r){ var c = Object.assign({}, r); delete c._id; return c; })
                        }));
                      })
                      .catch(function(e){
                        res.writeHead(200); res.end(JSON.stringify({
                          error: 'Platform cleanup completed but DB deleteMany failed: ' + e.message,
                          results: results
                        }));
                      });
                    return;
                  }

                  var it = items[idx++];
                  var itemResult = {
                    _id: it._id, // kept internal for the final bulk delete
                    sku: it.sku,
                    title: (it.title || '').substring(0, 80),
                    ebay: 'not-listed',
                    amazon: 'not-listed',
                    db: 'pending'
                  };

                  var listed = it.listedOn || [];
                  var doEbay = listed.indexOf('ebay') !== -1 && it.ebayItemId;
                  // Amazon DELETE only needs SKU (path is /items/{sellerId}/{sku}).
                  // ASIN is a catalog identifier — not required — so don't gate on it.
                  var doAmazon = listed.indexOf('amazon') !== -1 && it.sku;

                  function stepEbay(after){
                    if(!doEbay){ after(); return; }
                    // If another record in this batch already handled this ItemID
                    // (multi-qty shared listing), reuse its result — don't hit eBay
                    // again.
                    if(handledEbayIds[it.ebayItemId]){
                      itemResult.ebay = handledEbayIds[it.ebayItemId];
                      after();
                      return;
                    }
                    // Count siblings for this ItemID that are (a) still active and
                    // (b) NOT in this delete batch. If 0, this delete empties the
                    // listing — EndItem. Otherwise ReviseItem to decrement by the
                    // number of copies being removed from this batch.
                    database.collection('warehouse_inventory').countDocuments({
                      code: wCode,
                      ebayItemId: it.ebayItemId,
                      _id: { $nin: Array.from(batchIdSet).map(function(s){ try { return new (require('mongodb').ObjectId)(s); } catch(e){ return s; } }) },
                      status: { $nin: ['sold', 'sold-amazon', 'sold-ebay', 'deleted'] }
                    }).then(function(externalSiblings){
                      // Also count how many rows in THIS batch share this ItemID
                      // (they're all being removed simultaneously).
                      var inBatchWithSameId = items.filter(function(x){
                        return x.ebayItemId === it.ebayItemId;
                      }).length;

                      if(externalSiblings === 0){
                        // No external copies remain — end the whole listing.
                        endEbayListing(wCode, it.ebayItemId, function(endErr, endInfo){
                          var label;
                          if(endErr){ label = 'error: ' + endErr; }
                          else if(endInfo && endInfo.alreadyGone){ label = 'already-ended'; }
                          else { label = 'ended'; }
                          handledEbayIds[it.ebayItemId] = label;
                          itemResult.ebay = label;
                          after();
                        });
                      } else {
                        // Decrement. New Quantity should only reflect the external
                        // siblings (preserves eBay's own QuantitySold).
                        getEbayItemQuantity(wCode, it.ebayItemId, function(getErr, info){
                          if(getErr || !info){
                            var label = 'error: GetItem failed (' + (getErr || 'no data') + ')';
                            handledEbayIds[it.ebayItemId] = label;
                            itemResult.ebay = label;
                            after();
                            return;
                          }
                          var newQty = externalSiblings + info.sold;
                          reviseEbayItemQuantity(wCode, it.ebayItemId, newQty, function(revErr){
                            var label;
                            if(revErr){ label = 'error: ReviseItem failed (' + revErr + ')'; }
                            else { label = 'qty-decremented (' + inBatchWithSameId + ' in batch, ' + externalSiblings + ' external remain, eBay Quantity ' + info.quantity + '→' + newQty + ')'; }
                            handledEbayIds[it.ebayItemId] = label;
                            itemResult.ebay = label;
                            after();
                          });
                        });
                      }
                    }).catch(function(e){
                      itemResult.ebay = 'error: sibling check failed (' + e.message + ')';
                      after();
                    });
                  }

                  function stepAmazon(after){
                    if(!doAmazon){ after(); return; }
                    if(!accessToken){ itemResult.amazon = 'error: no Amazon token'; after(); return; }
                    deleteAmazonListing(accessToken, sellerId, it.sku, marketplaceId, function(delErr, delInfo){
                      if(delErr){ itemResult.amazon = 'error: ' + delErr; }
                      else if(delInfo && delInfo.alreadyGone){
                        itemResult.amazon = delInfo.verifiedGone ? 'already-gone (verified)' : 'already-gone';
                      }
                      else { itemResult.amazon = 'deleted'; }
                      after();
                    });
                  }

                  stepEbay(function(){
                    stepAmazon(function(){
                      results.push(itemResult);
                      processNext();
                    });
                  });
                }
                processNext();
              });
            });
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // Bulk-upsert SKU → location mappings into warehouse_inventory.
  // Admin-only. Used to backfill locations for items listed before location tracking existed.
  // POST /my/backfill-locations  body: { code, entries: [{sku, row, section, sequence}, ...] }
  if (pathname === '/my/backfill-locations' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var bfCode = (data.code || '').toUpperCase();
      var session = getRequestSession(req, parsed);
      if(!session || session.role !== 'admin' || session.subscriberCode !== bfCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var entries = Array.isArray(data.entries) ? data.entries : [];
      if(!entries.length){ res.writeHead(400); res.end(JSON.stringify({ error: 'No entries provided.' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var results = { ok: 0, updated: 0, inserted: 0, errors: [] };
        var idx = 0;
        function next(){
          if(idx >= entries.length){
            res.writeHead(200); res.end(JSON.stringify(results)); return;
          }
          var e = entries[idx++];
          var sku = (e.sku || '').trim();
          var row = parseInt(e.row);
          var section = (e.section || '').toString().trim().toUpperCase();
          var sequence = parseInt(e.sequence);
          if(!sku || isNaN(row) || !section || isNaN(sequence)){
            results.errors.push({ entry: e, error: 'Missing or invalid field(s)' });
            return next();
          }
          var location = { row: row, section: section, sequence: sequence };
          // Upsert: if SKU exists, update location; otherwise insert minimal record
          database.collection('warehouse_inventory').updateOne(
            { sku: sku },
            {
              $set: { location: location, code: bfCode },
              $setOnInsert: {
                sku: sku, status: 'active', createdAt: new Date(),
                title: '', isbn: '', author: '', price: 0, listedOn: [],
                backfilled: true
              }
            },
            { upsert: true }
          )
          .then(function(r){
            results.ok++;
            if(r.upsertedCount) results.inserted++;
            else if(r.modifiedCount) results.updated++;
            next();
          })
          .catch(function(err){
            results.errors.push({ sku: sku, error: err.message });
            next();
          });
        }
        next();
      });
    });
    return;
  }

  // ── MESSAGES: List recent messages ──
  // ───────────────────────────────────────────────────────────
  // REPRICER ENDPOINTS
  // ───────────────────────────────────────────────────────────

  // Default repricer config used when subscriber has none saved
  function getDefaultRepricerConfig(){
    return {
      enabled: false,
      dryRun: true,
      direction: 'both',              // 'down', 'up', 'both'
      floorPrice: 5.99,
      maxDrop24hPct: 10,              // rolling 24h window cap on drops
      maxIncrease24hPct: 20,          // rolling 24h window cap on increases
      cycleIntervalHours: 24,         // how often the scheduler triggers a cycle
      ebayDiscountPct: 15,
      conditionMatch: 'smart',        // 'strict', 'smart', 'loose'
      undercutStrategy: 'penny',      // 'penny', 'match', 'percent1', 'percent2', 'percent5', 'dollar50', 'dollar100', 'second-lowest-penny'
      fulfillmentFilter: 'fbm-only',  // 'fbm-only', 'all', 'fbm-plus-fba-above-threshold'
      excludedSkus: [],
      lastRunStartedAt: null,
      lastRunCompletedAt: null,
      lastRunSummary: null
    };
  }

  // GET /my/repricer/settings?code=X — admin fetches their config
  // ───────────────────────────────────────────────────────────
  // IMPORT: Upload CSV of existing listings into warehouse_inventory
  // ───────────────────────────────────────────────────────────
  // Expected CSV columns (order of first 9 matters, rest ignored):
  // ASIN, ISBN, Cond, SKU, UnitID#, SKUID#, Loc Name, Loc Seq#, Price, [Binding], [Title]
  // - Loc Name format: "row-section" like "1-A". Row 0 = skipped.
  // - Upserts by SKU. Existing SKUs updated, new SKUs inserted.

  // POST /my/import/csv  body: { code, csvText }
  // ───────────────────────────────────────────────────────────
  // EBAY ITEM ID ENRICH — one-time background process
  // Pulls active eBay listings via GetMyeBaySelling, matches by SKU,
  // stores ebayItemId on each warehouse_inventory record.
  // ───────────────────────────────────────────────────────────
  // ───────────────────────────────────────────────────────────
  // CROSS-PLATFORM SYNC settings and log endpoints
  // ───────────────────────────────────────────────────────────
  if (pathname === '/my/sync/settings' && req.method === 'GET') {
    var syCode = (parsed.query.code || '').toUpperCase();
    var sySess = getRequestSession(req, parsed);
    if(!sySess || sySess.role !== 'admin' || sySess.subscriberCode !== syCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    getSubscriber(syCode, function(err, sub){
      if(!sub){ res.writeHead(404); res.end(JSON.stringify({ error: 'Subscriber not found' })); return; }
      var cfg = sub.sync || { enabled: false };
      res.writeHead(200); res.end(JSON.stringify({ config: {
        enabled: !!cfg.enabled,
        lastAmazonCheckedAt: cfg.lastAmazonCheckedAt || null,
        lastEbayCheckedAt: cfg.lastEbayCheckedAt || null,
        lastRunAt: cfg.lastRunAt || null
      }}));
    });
    return;
  }

  if (pathname === '/my/sync/settings' && req.method === 'PUT') {
    parseBody(req, function(err, data){
      var syCode = (data.code || '').toUpperCase();
      var sySess = getRequestSession(req, parsed);
      if(!sySess || sySess.role !== 'admin' || sySess.subscriberCode !== syCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      if(typeof data.enabled !== 'boolean'){
        res.writeHead(400); res.end(JSON.stringify({ error: 'enabled (boolean) required' })); return;
      }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        database.collection('subscribers').updateOne(
          { code: { $regex: new RegExp('^' + syCode.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '$', 'i') } },
          { $set: { 'sync.enabled': data.enabled } }
        )
          .then(function(){ res.writeHead(200); res.end(JSON.stringify({ success: true })); })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // GET /my/sync/log?code=X&limit=100&needsReview=true
  if (pathname === '/my/sync/log' && req.method === 'GET') {
    var lCode = (parsed.query.code || '').toUpperCase();
    var lSess = getRequestSession(req, parsed);
    if(!lSess || lSess.role !== 'admin' || lSess.subscriberCode !== lCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    var limit = Math.min(parseInt(parsed.query.limit || '100') || 100, 500);
    var filter = { subscriberCode: lCode };
    if(parsed.query.needsReview === 'true') filter.needsReview = true;
    // Hide debug + error diagnostic rows by default (clean UX).
    // Pass ?showDebug=1 to include them (for troubleshooting).
    if(parsed.query.showDebug !== '1'){
      filter.sku = { $nin: ['_debug_', '_error_'] };
    }
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ log: [] })); return; }
      database.collection('sync_log').find(filter).sort({ createdAt: -1 }).limit(limit).toArray()
        .then(function(rows){
          res.writeHead(200); res.end(JSON.stringify({ log: rows || [] }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ log: [], error: e.message })); });
    });
    return;
  }

  // Delete all debug/error entries older than now (cleanup noisy entries)
  // GET /my/sync/health?code=X — returns health summary for sync dashboard indicator
  if (pathname === '/my/sync/health' && req.method === 'GET') {
    var hCode = (parsed.query.code || '').toUpperCase();
    var hSess = getRequestSession(req, parsed);
    if(!hSess || hSess.role !== 'admin' || hSess.subscriberCode !== hCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    getSubscriber(hCode, function(err, sub){
      if(!sub){ res.writeHead(404); res.end(JSON.stringify({ error: 'Not found' })); return; }
      connectMongo(function(err, database){
        if(err || !database){
          res.writeHead(200); res.end(JSON.stringify({ status: 'unknown', reason: 'DB unavailable' }));
          return;
        }
        var syncCfg = sub.sync || {};
        var enabled = !!syncCfg.enabled;
        var lastRunAt = syncCfg.lastRunAt ? new Date(syncCfg.lastRunAt) : null;
        var lastAmazon = syncCfg.lastAmazonCheckedAt ? new Date(syncCfg.lastAmazonCheckedAt) : null;

        // Compute minutes since last successful run
        var now = Date.now();
        var minutesSinceRun = lastRunAt ? Math.round((now - lastRunAt.getTime()) / 60000) : null;
        var minutesSinceAmazon = lastAmazon ? Math.round((now - lastAmazon.getTime()) / 60000) : null;

        // Fetch recent sync log entries (last hour) to count errors and review items
        var oneHourAgo = new Date(now - 60 * 60 * 1000);
        database.collection('sync_log').find({
          subscriberCode: hCode,
          createdAt: { $gte: oneHourAgo }
        }).sort({ createdAt: -1 }).limit(200).toArray()
          .then(function(rows){
            var errorCount = 0;
            var reviewCount = 0;
            var successCount = 0;
            var lastCycleStart = null;
            var lastRealAction = null;
            (rows || []).forEach(function(r){
              if(r.sku === '_error_') errorCount++;
              if(r.needsReview) reviewCount++;
              if(r.action === 'end-ebay-listing' || r.action === 'delete-amazon-listing' || r.action === 'skip'){
                if(!lastRealAction) lastRealAction = r.createdAt;
              }
              if(r.action === 'sync-cycle-start' && !lastCycleStart) lastCycleStart = r.createdAt;
            });

            // Also count open (undismissed) review items across ALL time
            return database.collection('sync_log').countDocuments({
              subscriberCode: hCode,
              needsReview: true
            }).then(function(totalReviewOpen){
              // Determine health status
              var status = 'green';
              var reasons = [];

              if(!enabled){
                status = 'red';
                reasons.push('Auto-sync is OFF');
              } else if(minutesSinceRun === null){
                status = 'yellow';
                reasons.push('No cycle has run yet');
              } else if(minutesSinceRun > 15){
                status = 'red';
                reasons.push('Last cycle was ' + minutesSinceRun + ' min ago');
              } else if(minutesSinceRun > 7){
                status = 'yellow';
                reasons.push('Last cycle was ' + minutesSinceRun + ' min ago');
              }

              if(totalReviewOpen > 0){
                if(status === 'green') status = 'yellow';
                reasons.push(totalReviewOpen + ' item' + (totalReviewOpen > 1 ? 's' : '') + ' need review');
              }

              if(errorCount >= 5){
                if(status !== 'red') status = 'yellow';
                reasons.push(errorCount + ' errors in last hour');
              }

              if(minutesSinceAmazon !== null && minutesSinceAmazon > 10){
                if(status === 'green') status = 'yellow';
                reasons.push('Amazon checkpoint ' + minutesSinceAmazon + ' min behind');
              }

              // Short user-friendly label
              var label = 'Healthy';
              if(status === 'yellow') label = 'Minor issues';
              if(status === 'red') label = 'Attention needed';
              if(!enabled) label = 'Paused';

              res.writeHead(200); res.end(JSON.stringify({
                status: status,
                label: label,
                enabled: enabled,
                lastRunAt: lastRunAt ? lastRunAt.toISOString() : null,
                lastAmazonCheckedAt: lastAmazon ? lastAmazon.toISOString() : null,
                minutesSinceRun: minutesSinceRun,
                minutesSinceAmazon: minutesSinceAmazon,
                lastHourStats: {
                  errors: errorCount,
                  review: reviewCount,
                  success: successCount
                },
                totalReviewOpen: totalReviewOpen,
                reasons: reasons
              }));
            });
          })
          .catch(function(e){
            res.writeHead(200); res.end(JSON.stringify({ status: 'unknown', reason: e.message }));
          });
      });
    });
    return;
  }

  if (pathname === '/my/sync/log/clear-debug' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var cCode = (data.code || '').toUpperCase();
      var cSess = getRequestSession(req, parsed);
      if(!cSess || cSess.role !== 'admin' || cSess.subscriberCode !== cCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        database.collection('sync_log').deleteMany({
          subscriberCode: cCode,
          sku: { $in: ['_debug_', '_error_'] }
        })
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({ success: true, deleted: result.deletedCount || 0 }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // Rewind sync checkpoints — forces next cycle to re-scan last N hours.
  // Use after quota outage to catch missed orders.
  if (pathname === '/my/sync/rewind' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var rCode = (data.code || '').toUpperCase();
      var rSess = getRequestSession(req, parsed);
      if(!rSess || rSess.role !== 'admin' || rSess.subscriberCode !== rCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var hours = Math.min(parseInt(data.hours || '6') || 6, 72); // max 72hr lookback
      var rewindTo = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        database.collection('subscribers').updateOne(
          { code: { $regex: new RegExp('^' + rCode.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '$', 'i') } },
          { $set: {
              'sync.lastAmazonCheckedAt': rewindTo,
              'sync.lastEbayCheckedAt': rewindTo
            }
          }
        )
          .then(function(){
            res.writeHead(200); res.end(JSON.stringify({ success: true, rewoundTo: rewindTo, hours: hours }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // Dismiss a specific sync log entry (clear needsReview flag)
  if (pathname === '/my/sync/log/dismiss' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var dCode = (data.code || '').toUpperCase();
      var dSess = getRequestSession(req, parsed);
      if(!dSess || dSess.role !== 'admin' || dSess.subscriberCode !== dCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      if(!data.entryId){ res.writeHead(400); res.end(JSON.stringify({ error: 'Missing entryId' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB error' })); return; }
        var ObjectId;
        try { ObjectId = require('mongodb').ObjectId; } catch(e){ res.writeHead(200); res.end(JSON.stringify({ error: 'No ObjectId' })); return; }
        var oid;
        try { oid = new ObjectId(data.entryId); } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid entryId' })); return; }
        database.collection('sync_log').updateOne(
          { _id: oid, subscriberCode: dCode },
          { $set: { needsReview: false, dismissedAt: new Date() } }
        )
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({ success: true, modified: result.modifiedCount || 0 }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  if (pathname === '/my/ebay/enrich/run' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var iCode = (data.code || '').toUpperCase();
      var iSess = getRequestSession(req, parsed);
      if(!iSess || iSess.role !== 'admin' || iSess.subscriberCode !== iCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      if(ebayEnrichRunning[iCode]){
        res.writeHead(200); res.end(JSON.stringify({ success: false, message: 'Enrich already running.' })); return;
      }
      try { runEbayEnrich(iCode); } catch(e){}
      res.writeHead(200); res.end(JSON.stringify({ success: true, message: 'Enrich started. Check status for progress.' }));
    });
    return;
  }

  if (pathname === '/my/ebay/enrich/status' && req.method === 'GET') {
    var sCode = (parsed.query.code || '').toUpperCase();
    var sSess = getRequestSession(req, parsed);
    if(!sSess || sSess.role !== 'admin' || sSess.subscriberCode !== sCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    var st = ebayEnrichStatus[sCode] || { running: false };
    res.writeHead(200); res.end(JSON.stringify(st));
    return;
  }

  if (pathname === '/my/import/csv' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var iCode = (data.code || '').toUpperCase();
      var iSess = getRequestSession(req, parsed);
      if(!iSess || iSess.role !== 'admin' || iSess.subscriberCode !== iCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var csvText = data.csvText || '';
      if(!csvText || csvText.length < 10){
        res.writeHead(400); res.end(JSON.stringify({ error: 'CSV content is empty or too small.' })); return;
      }

      // Parse CSV — handle quoted fields containing commas
      function parseCsvLine(line){
        var out = [];
        var cur = '';
        var inQuote = false;
        for(var i=0; i<line.length; i++){
          var c = line.charCodeAt(i);
          var ch = line[i];
          if(inQuote){
            if(ch === '"' && line[i+1] === '"'){ cur += '"'; i++; }
            else if(ch === '"'){ inQuote = false; }
            else { cur += ch; }
          } else {
            if(ch === '"'){ inQuote = true; }
            else if(ch === ','){ out.push(cur); cur = ''; }
            else { cur += ch; }
          }
        }
        out.push(cur);
        return out;
      }

      var lines = csvText.split(/\r?\n/).filter(function(l){ return l.trim().length > 0; });
      if(lines.length < 2){
        res.writeHead(400); res.end(JSON.stringify({ error: 'CSV needs a header row + at least 1 data row.' })); return;
      }

      // Skip header; first data row starts at index 1
      var records = [];
      var skippedRow0 = 0;
      var skippedMx = 0;
      var malformed = 0;
      for(var i=1; i<lines.length; i++){
        var cols = parseCsvLine(lines[i]);
        if(cols.length < 9){ malformed++; continue; }
        var asin = (cols[0]||'').trim();
        var isbn = (cols[1]||'').trim();
        var cond = (cols[2]||'').trim();
        var sku  = (cols[3]||'').trim();
        var locName = (cols[6]||'').trim();
        var locSeq = parseInt(cols[7]) || 0;
        var price = parseFloat(cols[8]) || 0;
        var binding = (cols[9]||'').trim();
        var title = (cols[10]||'').trim();
        if(!sku){ malformed++; continue; }
        // Skip MX-prefixed locations (Mexican inventory, handle later)
        if(/^MX-/i.test(locName)){ skippedMx++; continue; }
        // Parse Loc Name "1-A" → row=1, section=A
        var locParts = locName.split('-');
        if(locParts.length !== 2){ malformed++; continue; }
        var row = parseInt(locParts[0]);
        var section = (locParts[1]||'').trim().toUpperCase();
        if(row === 0){ skippedRow0++; continue; } // User said row 0 is invalid
        if(isNaN(row) || !section){ malformed++; continue; }

        records.push({
          sku: sku,
          asin: asin,
          isbn: isbn,
          condition: cond,
          price: price,
          title: title,
          binding: binding,
          location: { row: row, section: section, sequence: locSeq }
        });
      }

      if(!records.length){
        res.writeHead(400); res.end(JSON.stringify({ error: 'No valid rows found.', skippedRow0: skippedRow0, skippedMx: skippedMx, malformed: malformed })); return;
      }

      // Bulk upsert into MongoDB
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var bulk = database.collection('warehouse_inventory').initializeUnorderedBulkOp();
        var now = new Date();
        records.forEach(function(r){
          bulk.find({ code: iCode, sku: r.sku }).upsert().updateOne({
            $set: {
              code: iCode,
              sku: r.sku,
              asin: r.asin,
              isbn: r.isbn,
              condition: r.condition,
              price: r.price,
              title: r.title,
              binding: r.binding,
              location: r.location,
              source: 'csv-import',
              status: 'active',
              importedAt: now
            },
            $setOnInsert: { createdAt: now }
          });
        });
        bulk.execute()
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({
              success: true,
              inserted: result.nUpserted || 0,
              updated: result.nModified || 0,
              total: records.length,
              skippedRow0: skippedRow0,
              skippedMx: skippedMx,
              malformed: malformed
            }));
          })
          .catch(function(e){
            res.writeHead(200); res.end(JSON.stringify({ error: 'Import failed: ' + e.message }));
          });
      });
    });
    return;
  }

  if (pathname === '/my/repricer/settings' && req.method === 'GET') {
    var rCode = (parsed.query.code || '').toUpperCase();
    var rSess = getRequestSession(req, parsed);
    if(!rSess || rSess.role !== 'admin' || rSess.subscriberCode !== rCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    getSubscriber(rCode, function(err, sub){
      if(!sub){ res.writeHead(404); res.end(JSON.stringify({ error: 'Subscriber not found' })); return; }
      var config = Object.assign({}, getDefaultRepricerConfig(), sub.repricer || {});
      res.writeHead(200); res.end(JSON.stringify({ config: config }));
    });
    return;
  }

  // PUT /my/repricer/settings — admin saves their config
  if (pathname === '/my/repricer/settings' && req.method === 'PUT') {
    parseBody(req, function(err, data){
      var rCode = (data.code || '').toUpperCase();
      var rSess = getRequestSession(req, parsed);
      if(!rSess || rSess.role !== 'admin' || rSess.subscriberCode !== rCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      // Whitelist only known fields, coerce types
      var defaults = getDefaultRepricerConfig();
      var update = {};
      if(typeof data.enabled === 'boolean') update['repricer.enabled'] = data.enabled;
      if(typeof data.dryRun === 'boolean') update['repricer.dryRun'] = data.dryRun;
      if(['down','up','both'].indexOf(data.direction) !== -1) update['repricer.direction'] = data.direction;
      if(typeof data.floorPrice === 'number' && data.floorPrice >= 0) update['repricer.floorPrice'] = data.floorPrice;
      if(typeof data.maxDrop24hPct === 'number' && data.maxDrop24hPct >= 0 && data.maxDrop24hPct <= 100) update['repricer.maxDrop24hPct'] = data.maxDrop24hPct;
      if(typeof data.maxIncrease24hPct === 'number' && data.maxIncrease24hPct >= 0 && data.maxIncrease24hPct <= 1000) update['repricer.maxIncrease24hPct'] = data.maxIncrease24hPct;
      if(typeof data.cycleIntervalHours === 'number' && data.cycleIntervalHours > 0 && data.cycleIntervalHours <= 720) update['repricer.cycleIntervalHours'] = data.cycleIntervalHours;
      if(typeof data.ebayDiscountPct === 'number' && data.ebayDiscountPct >= 0 && data.ebayDiscountPct <= 100) update['repricer.ebayDiscountPct'] = data.ebayDiscountPct;
      if(['strict','smart','loose'].indexOf(data.conditionMatch) !== -1) update['repricer.conditionMatch'] = data.conditionMatch;
      if(['penny','match','percent1','percent2','percent5','dollar50','dollar100','second-lowest-penny'].indexOf(data.undercutStrategy) !== -1) update['repricer.undercutStrategy'] = data.undercutStrategy;
      if(['fbm-only','all','fbm-plus-fba-above-threshold'].indexOf(data.fulfillmentFilter) !== -1) update['repricer.fulfillmentFilter'] = data.fulfillmentFilter;
      if(Array.isArray(data.excludedSkus)) update['repricer.excludedSkus'] = data.excludedSkus.map(function(s){ return (s||'').trim(); }).filter(Boolean);

      if(!Object.keys(update).length){
        res.writeHead(400); res.end(JSON.stringify({ error: 'No valid settings provided.' })); return;
      }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        database.collection('subscribers').updateOne(
          { code: { $regex: new RegExp('^' + rCode.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '$', 'i') } },
          { $set: update }
        )
          .then(function(r){
            if(r.matchedCount === 0){ res.writeHead(404); res.end(JSON.stringify({ error: 'Subscriber not found' })); return; }
            res.writeHead(200); res.end(JSON.stringify({ success: true }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // GET /my/repricer/history?code=X&limit=50 — last N price changes
  if (pathname === '/my/repricer/history' && req.method === 'GET') {
    var hCode = (parsed.query.code || '').toUpperCase();
    var hSess = getRequestSession(req, parsed);
    if(!hSess || hSess.role !== 'admin' || hSess.subscriberCode !== hCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    var hLimit = Math.min(parseInt(parsed.query.limit || '50') || 50, 500);
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ history: [] })); return; }
      database.collection('reprice_history').find({ subscriberCode: hCode })
        .sort({ createdAt: -1 }).limit(hLimit).toArray()
        .then(function(rows){
          res.writeHead(200); res.end(JSON.stringify({
            history: (rows || []).map(function(h){
              return {
                sku: h.sku,
                platform: h.platform,
                oldPrice: h.oldPrice,
                newPrice: h.newPrice,
                reason: h.reason,
                dryRun: !!h.dryRun,
                skipped: !!h.skipped,
                createdAt: h.createdAt,
                diagnostics: h.diagnostics || null
              };
            })
          }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ history: [], error: e.message })); });
    });
    return;
  }

  // POST /my/repricer/run — admin triggers a cycle manually
  // For now this is a stub that returns success — the actual engine is built in Step 3.
  if (pathname === '/my/repricer/run' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var rCode = (data.code || '').toUpperCase();
      var rSess = getRequestSession(req, parsed);
      if(!rSess || rSess.role !== 'admin' || rSess.subscriberCode !== rCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      var testSku = (data.sku || '').trim() || null;
      // Kick off the repricer cycle in background (non-blocking)
      if(typeof runRepricerCycle === 'function'){
        try { runRepricerCycle(rCode, testSku); } catch(e){}
        res.writeHead(200); res.end(JSON.stringify({
          success: true,
          message: testSku ? ('Single-SKU test started for ' + testSku + '. Check history in a few seconds.') : 'Cycle started on all SKUs. Check history for progress.'
        }));
      } else {
        res.writeHead(200); res.end(JSON.stringify({ success: false, message: 'Repricer engine not yet deployed. Settings can be saved, but runs are disabled.' }));
      }
    });
    return;
  }

  // GET /my/repricer/status?code=X — check if a cycle is running
  if (pathname === '/my/repricer/status' && req.method === 'GET') {
    var sCode = (parsed.query.code || '').toUpperCase();
    var sSess = getRequestSession(req, parsed);
    if(!sSess || sSess.role !== 'admin' || sSess.subscriberCode !== sCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
    }
    getSubscriber(sCode, function(err, sub){
      if(!sub){ res.writeHead(404); res.end(JSON.stringify({ error: 'Subscriber not found' })); return; }
      var cfg = sub.repricer || {};
      var running = !!(cfg.lastRunStartedAt && !cfg.lastRunCompletedAt);
      res.writeHead(200); res.end(JSON.stringify({
        running: running,
        lastRunStartedAt: cfg.lastRunStartedAt || null,
        lastRunCompletedAt: cfg.lastRunCompletedAt || null,
        lastRunSummary: cfg.lastRunSummary || null
      }));
    });
    return;
  }

  // POST /my/repricer/reset-sku — clear simulatedPrice + lastRepriceAttempt on
  // one SKU, putting it back to "virgin" state for re-testing first-pass logic.
  // Body: { code, sku }. Admin-only.
  if (pathname === '/my/repricer/reset-sku' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var rsCode = (data.code || '').toUpperCase();
      var rsSku = (data.sku || '').trim();
      var rsSess = getRequestSession(req, parsed);
      if(!rsSess || rsSess.role !== 'admin' || rsSess.subscriberCode !== rsCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required.' })); return;
      }
      if(!rsSku){ res.writeHead(400); res.end(JSON.stringify({ error: 'SKU required' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        database.collection('warehouse_inventory').updateOne(
          { code: rsCode, sku: rsSku },
          { $unset: { simulatedPrice: '', lastRepriceAttempt: '', priceChangeLog: '' } }
        )
          .then(function(r){
            if(r.matchedCount === 0){ res.writeHead(404); res.end(JSON.stringify({ error: 'SKU not found in inventory' })); return; }
            res.writeHead(200); res.end(JSON.stringify({
              success: true,
              message: 'SKU ' + rsSku + ' reset to virgin state (next run = first-pass, no cap)'
            }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  if (pathname === '/my/messages' && req.method === 'GET') {
    var mCode = (parsed.query.code || '').toUpperCase();
    var session = getRequestSession(req, parsed);
    if(!session || session.subscriberCode !== mCode){
      res.writeHead(403); res.end(JSON.stringify({ error: 'Session required.' })); return;
    }
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ messages: [] })); return; }
      database.collection('messages').find({ subscriberCode: mCode })
        .sort({ createdAt: -1 }).limit(50).toArray()
        .then(function(rows){
          res.writeHead(200); res.end(JSON.stringify({
            messages: (rows || []).map(function(m){
              return {
                id: m._id.toString(),
                author: m.author,
                authorRole: m.authorRole,
                body: m.body,
                createdAt: m.createdAt,
                acknowledgements: m.acknowledgements || [],
                replies: m.replies || []
              };
            })
          }));
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ messages: [], error: e.message })); });
    });
    return;
  }

  // ── MESSAGES: Post a new message ──
  if (pathname === '/my/messages' && req.method === 'POST') {
    parseBody(req, function(err, data){
      var mCode = (data.code || '').toUpperCase();
      var session = getRequestSession(req, parsed);
      if(!session || session.subscriberCode !== mCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Session required.' })); return;
      }
      var body = (data.body || '').trim();
      if(!body){ res.writeHead(400); res.end(JSON.stringify({ error: 'Message body required.' })); return; }
      if(body.length > 2000){ res.writeHead(400); res.end(JSON.stringify({ error: 'Message too long (2000 char max).' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var msg = {
          subscriberCode: mCode,
          author: session.employeeName || 'Admin',
          authorRole: session.role,
          body: body,
          createdAt: new Date(),
          acknowledgements: [],
          replies: []
        };
        database.collection('messages').insertOne(msg)
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({ success: true, id: result.insertedId.toString() }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── MESSAGES: Acknowledge a message ──
  // /my/messages/:id/ack
  if (pathname.indexOf('/my/messages/') === 0 && pathname.endsWith('/ack') && req.method === 'POST') {
    var ackId = pathname.replace('/my/messages/', '').replace('/ack', '');
    parseBody(req, function(err, data){
      var mCode = (data.code || '').toUpperCase();
      var session = getRequestSession(req, parsed);
      if(!session || session.subscriberCode !== mCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Session required.' })); return;
      }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var ObjectId;
        try { ObjectId = require('mongodb').ObjectId; } catch(e){ res.writeHead(500); res.end(JSON.stringify({ error: 'Server config error' })); return; }
        var oid;
        try { oid = new ObjectId(ackId); } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid message id' })); return; }
        var ackEntry = {
          name: session.employeeName || (session.role === 'admin' ? 'Admin' : 'User'),
          at: new Date()
        };
        // Prevent duplicate acknowledgements by same user
        database.collection('messages').updateOne(
          { _id: oid, subscriberCode: mCode, 'acknowledgements.name': { $ne: ackEntry.name } },
          { $push: { acknowledgements: ackEntry } }
        )
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({ success: true, newAck: result.modifiedCount > 0 }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── MESSAGES: Reply to a message ──
  if (pathname.indexOf('/my/messages/') === 0 && pathname.endsWith('/reply') && req.method === 'POST') {
    var replyId = pathname.replace('/my/messages/', '').replace('/reply', '');
    parseBody(req, function(err, data){
      var mCode = (data.code || '').toUpperCase();
      var session = getRequestSession(req, parsed);
      if(!session || session.subscriberCode !== mCode){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Session required.' })); return;
      }
      var replyBody = (data.body || '').trim();
      if(!replyBody){ res.writeHead(400); res.end(JSON.stringify({ error: 'Reply body required.' })); return; }
      if(replyBody.length > 1000){ res.writeHead(400); res.end(JSON.stringify({ error: 'Reply too long.' })); return; }
      connectMongo(function(err, database){
        if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        var ObjectId;
        try { ObjectId = require('mongodb').ObjectId; } catch(e){ res.writeHead(500); res.end(JSON.stringify({ error: 'Server config error' })); return; }
        var oid;
        try { oid = new ObjectId(replyId); } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid message id' })); return; }
        var reply = {
          author: session.employeeName || (session.role === 'admin' ? 'Admin' : 'User'),
          body: replyBody,
          at: new Date()
        };
        database.collection('messages').updateOne(
          { _id: oid, subscriberCode: mCode },
          { $push: { replies: reply } }
        )
          .then(function(result){
            res.writeHead(200); res.end(JSON.stringify({ success: true, posted: result.modifiedCount > 0 }));
          })
          .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
      });
    });
    return;
  }

  // ── MESSAGES: Delete a message (author or admin only) ──
  if (pathname.indexOf('/my/messages/') === 0 && req.method === 'DELETE') {
    var delId = pathname.replace('/my/messages/', '');
    var session = getRequestSession(req, parsed);
    if(!session){ res.writeHead(403); res.end(JSON.stringify({ error: 'Session required.' })); return; }
    connectMongo(function(err, database){
      if(err || !database){ res.writeHead(200); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      var ObjectId;
      try { ObjectId = require('mongodb').ObjectId; } catch(e){ res.writeHead(500); res.end(JSON.stringify({ error: 'Server config error' })); return; }
      var oid;
      try { oid = new ObjectId(delId); } catch(e){ res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid message id' })); return; }
      database.collection('messages').findOne({ _id: oid, subscriberCode: session.subscriberCode })
        .then(function(msg){
          if(!msg){ res.writeHead(404); res.end(JSON.stringify({ error: 'Message not found' })); return; }
          var authorName = session.employeeName || (session.role === 'admin' ? 'Admin' : 'User');
          if(msg.author !== authorName && session.role !== 'admin'){
            res.writeHead(403); res.end(JSON.stringify({ error: 'Only the author or an admin can delete.' })); return;
          }
          return database.collection('messages').deleteOne({ _id: oid })
            .then(function(){ res.writeHead(200); res.end(JSON.stringify({ success: true })); });
        })
        .catch(function(e){ res.writeHead(200); res.end(JSON.stringify({ error: e.message })); });
    });
    return;
  }

  if (pathname === '/my/settings' && req.method === 'PUT') {
    parseBody(req, function(err, data) {
      var code = (data.code || '').replace(/[\r\n]/g,'').trim();
      // Require valid owner session to modify settings
      var session = getRequestSession(req, parsed);
      if(!session || session.role !== 'admin' || session.subscriberCode !== code.toUpperCase()){
        res.writeHead(403); res.end(JSON.stringify({ error: 'Admin access required to modify settings.' })); return;
      }
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
        var allowed = { employees: data.employees, email: data.email, businessName: data.businessName, ebayClientId: data.ebayClientId, ebayClientSecret: data.ebayClientSecret, ebayDevId: data.ebayDevId, ebayUserToken: data.ebayUserToken, ebayOAuthToken: data.ebayOAuthToken, ebayShippingPolicyId: data.ebayShippingPolicyId, ebayPaymentPolicyId: data.ebayPaymentPolicyId, ebayReturnPolicyId: data.ebayReturnPolicyId, businessAddressLine1: data.businessAddressLine1, businessAddressLine2: data.businessAddressLine2, businessCity: data.businessCity, businessState: data.businessState, businessZip: data.businessZip, businessPhone: data.businessPhone, vendors: data.vendors, customers: data.customers, savedDescriptions: data.savedDescriptions, invoicePayableTo: data.invoicePayableTo };
        Object.keys(allowed).forEach(function(k) { if (allowed[k] === undefined) delete allowed[k]; });
        connectMongo(function(err, database) {
          if (err || !database) {
            Object.assign(inMemorySubscribers[code], allowed);
            res.writeHead(200); res.end(JSON.stringify({ success: true }));
            return;
          }
          // Case-insensitive match — subscriber may have been stored with different casing
          var escapedCode = code.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          var codeFilter = { code: { $regex: new RegExp('^' + escapedCode + '$', 'i') } };
          database.collection('subscribers').updateOne(codeFilter, { $set: allowed })
            .then(function(result) {
              if (result.matchedCount === 0) {
                // Diagnostic: list all subscriber codes to help figure out the mismatch
                database.collection('subscribers').find({}).project({ code: 1 }).toArray()
                  .then(function(all){
                    var codes = (all || []).map(function(s){ return s.code; });
                    res.writeHead(200); res.end(JSON.stringify({
                      error: 'Save failed: no subscriber matched "' + code + '". Codes in DB: ' + JSON.stringify(codes)
                    }));
                  })
                  .catch(function(){
                    res.writeHead(200); res.end(JSON.stringify({ error: 'Save failed: no match for "' + code + '"' }));
                  });
                return;
              }
              res.writeHead(200); res.end(JSON.stringify({ success: true, matched: result.matchedCount, modified: result.modifiedCount }));
            })
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
      { code: { $regex: new RegExp('^Booksforages1!$', 'i') } },
      {
        $set: {
          businessName: 'Books for Ages HQ',
          email: 'Codexbrothers@yahoo.com',
          active: true,
          ebayClientId: CLIENT_ID,
          ebayClientSecret: CLIENT_SECRET,
          ebayDevId: DEV_ID,
          ebayShippingPolicyId: process.env.EBAY_SHIPPING_POLICY_ID || '193108528015',
          ebayPaymentPolicyId: process.env.EBAY_PAYMENT_POLICY_ID || '226293158015',
          ebayReturnPolicyId: process.env.EBAY_RETURN_POLICY_ID || '129856789015',
          notes: 'Master admin account'
        },
        $setOnInsert: {
          code: 'Booksforages1!',
          isAdmin: true,
          // Seed employees ONLY on first insert. Never overwrite existing on restart.
          employees: [
            { name: 'Adam', pin: '8792' },
            { name: 'Lizbeth', pin: '7284' },
            { name: 'Josselin', pin: '9373' },
            { name: 'Stephanie', pin: '3842' },
            { name: 'Cris', pin: '8792' }
          ],
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
