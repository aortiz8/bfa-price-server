var https = require('https');
var http = require('http');

var PORT = process.env.PORT || 3000;
var CLIENT_ID = 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';
var CLIENT_SECRET = 'PRD-6c135696e4a6-8789-475a-8eaf-1662';
var DEV_ID = '3e7db631-fffe-4cd8-92b6-6bca13515742';
var USER_TOKEN = 'v^1.1#i^1#f^0#r^1#p^3#I^3#t^Ul4xMF8yOkVBM0U2OUZBMEY0MDY0QjYxOEVCQTM2OTZFMTg0OEIwXzJfMSNFXjI2MA==';

var COND_MAP = {
  '1000': 'NEW',
  '1500': 'LIKE_NEW',
  '2500': 'VERY_GOOD',
  '3000': 'GOOD',
  '7000': 'ACCEPTABLE'
};

var cachedToken = null;
var tokenExpiry = 0;

function getToken(cb) {
  if (cachedToken && Date.now() < tokenExpiry) { cb(null, cachedToken); return; }
  var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
  var body = 'grant_type=client_credentials&scope=https%3A%2F%2Fapi.ebay.com%2Foauth%2Fapi_scope';
  var opts = {
    hostname: 'api.ebay.com',
    path: '/identity/v1/oauth2/token',
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': 'Basic ' + credentials,
      'Content-Length': Buffer.byteLength(body)
    }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        var json = JSON.parse(data);
        if (json.access_token) {
          cachedToken = json.access_token;
          tokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
          cb(null, cachedToken);
        } else { cb('Token error: ' + data); }
      } catch(e) { cb('Token parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Token timeout'); });
  req.write(body);
  req.end();
}

function searchEbay(keywords, conditionId, token, cb) {
  var condFilter = conditionId ? ',conditions:{' + conditionId + '}' : '';
  var query = '/buy/browse/v1/item_summary/search?q=' + encodeURIComponent(keywords)
    + '&category_ids=267'
    + '&filter=buyingOptions:{FIXED_PRICE}' + condFilter
    + '&limit=20'
    + '&sort=price';
  var opts = {
    hostname: 'api.ebay.com',
    path: query,
    method: 'GET',
    headers: {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json',
      'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
    }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        var json = JSON.parse(data);
        var items = json.itemSummaries || [];
        if (items.length === 0) { cb(null, 'No items'); return; }
        var prices = [];
        for (var i = 0; i < items.length; i++) {
          var price = items[i].price && parseFloat(items[i].price.value);
          if (!price || isNaN(price) || price <= 0) continue;
          prices.push(price);
        }
        if (prices.length === 0) { cb(null, 'No prices'); return; }
        var sum = 0;
        for (var j = 0; j < prices.length; j++) sum += prices[j];
        var avg = Math.round(sum / prices.length * 100) / 100;
        cb({ count: prices.length, average: Math.round((avg + 3.99) * 100) / 100 });
      } catch(e) { cb(null, 'Parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(null, e.message); });
  req.setTimeout(15000, function() { req.destroy(); cb(null, 'Timeout'); });
  req.end();
}

function createListing(title, description, price, isbn, conditionId, pictureUrl, language, author, cb) {
  var xmlBody = '<?xml version="1.0" encoding="utf-8"?>'
    + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<Item>'
    + '<Title>' + title.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').substring(0,80) + '</Title>'
    + '<Description><![CDATA[' + description + ']]></Description>'
    + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
    + '<StartPrice>' + price.toFixed(2) + '</StartPrice>'
    + '<CategoryMappingAllowed>true</CategoryMappingAllowed>'
    + '<ConditionID>' + conditionId + '</ConditionID>'
    + '<ItemSpecifics>'
    + '<NameValueList><Name>Book Title</Name><Value>' + title.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').substring(0,65) + '</Value></NameValueList>'
    + '<NameValueList><Name>Language</Name><Value>' + (language || 'English') + '</Value></NameValueList>'
    + '</ItemSpecifics>'
    + (pictureUrl ? '<PictureDetails><PictureURL>' + pictureUrl + '</PictureURL></PictureDetails>' : '')
    + '<Country>US</Country>'
    + '<Currency>USD</Currency>'
    + '<DispatchTimeMax>2</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<Quantity>1</Quantity>'
    + '<SellerProfiles>'
    + '<SellerShippingProfile><ShippingProfileName>Shipping 2020-01-02</ShippingProfileName></SellerShippingProfile>'
    + '<SellerReturnProfile><ReturnProfileName>Returns Accepted,Buyer,30 Days,Money Back#0</ReturnProfileName></SellerReturnProfile>'
    + '<SellerPaymentProfile><PaymentProfileName>eBay Payments</PaymentProfileName></SellerPaymentProfile>'
    + '</SellerProfiles>'
    + '<PostalCode>92105</PostalCode>'
    + '<Site>US</Site>'
    + '</Item>'
    + '</AddItemRequest>';

  var opts = {
    hostname: 'api.ebay.com',
    path: '/ws/api.dll',
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'AddItem',
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID,
      'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': Buffer.byteLength(xmlBody)
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      console.log('eBay response:', data.substring(0, 500));
      var idMatch = data.match(/<ItemID>(\d+)<\/ItemID>/);
      var errMatch = data.match(/<LongMessage>(.*?)<\/LongMessage>/);
      if (idMatch) {
        cb({ listingId: idMatch[1] });
      } else {
        cb({ error: errMatch ? errMatch[1] : 'Unknown eBay error', raw: data.substring(0, 500) });
      }
    });
  });
  req.on('error', function(e) { cb({ error: e.message }); });
  req.setTimeout(20000, function() { req.destroy(); cb({ error: 'Timeout' }); });
  req.write(xmlBody);
  req.end();
}

var server = http.createServer(function(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  var url = new URL(req.url, 'http://localhost');

  // /list — POST only, must be before the GET-only guard
  if (url.pathname === '/list') {
    if (req.method !== 'POST') { res.writeHead(405); res.end('{}'); return; }
    var body = '';
    req.on('data', function(c) { body += c; });
    req.on('end', function() {
      try {
        var data = JSON.parse(body);
        var title = data.title || '';
        var description = data.description || '';
        var price = parseFloat(data.price) || 9.99;
        var isbn = data.isbn || '';
        var conditionId = parseInt(data.conditionId) || 3000;
        if (!title) { res.writeHead(400); res.end('{"error":"missing title"}'); return; }
        createListing(title, description, price, isbn, conditionId, data.pictureUrl || '', data.language || 'English', data.author || '', function(result) {
          res.writeHead(200);
          res.end(JSON.stringify(result));
        });
      } catch(e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON: ' + e.message }));
      }
    });
    return;
  }

  // /upload — POST only
  if (url.pathname === '/upload') {
    if (req.method !== 'POST') { res.writeHead(405); res.end('{}'); return; }
    var body = '';
    req.on('data', function(c) { body += c; });
    req.on('end', function() {
      try {
        var data = JSON.parse(body);
        var b64 = data.image || '';
        // Strip data URL prefix if present
        b64 = b64.replace(/^data:image\/[a-z]+;base64,/, '');
        var imgBuffer = Buffer.from(b64, 'base64');
        uploadPicture(imgBuffer, function(result) {
          res.writeHead(200);
          res.end(JSON.stringify(result));
        });
      } catch(e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON: ' + e.message }));
      }
    });
    return;
  }

  // All other routes are GET only
  if (req.method !== 'GET') { res.writeHead(405); res.end('{}'); return; }

  if (url.pathname === '/health') {
    res.writeHead(200);
    res.end('{"status":"ok"}');
    return;
  }

  if (url.pathname === '/category') {
    getExistingCategory(function(result) {
      res.writeHead(200);
      res.end(JSON.stringify(result));
    });
    return;
  }

  var title = url.searchParams.get('title') || '';
  var author = url.searchParams.get('author') || '';
  var isbn = (url.searchParams.get('isbn') || '').replace(/[^0-9X]/gi, '');
  var cond = url.searchParams.get('condition') || '3000';
    if (url.pathname === '/policies') {
      getSellerProfiles(function(result) {
        res.writeHead(200);
        res.end(JSON.stringify(result));
      });
      return;
    }

  var signed = url.searchParams.get('signed') === '1';
  var conditionId = COND_MAP[cond] || 'GOOD';

  if (!title && !isbn) {
    res.writeHead(400);
    res.end('{"error":"missing title or isbn"}');
    return;
  }

  var kw = isbn
    ? isbn
    : (title + (author ? ' ' + author : '') + (signed ? ' signed' : ''));

  console.log('Search:', kw, 'Condition:', conditionId, 'Path:', url.pathname);

  getToken(function(err, token) {
    if (err) {
      res.writeHead(200);
      res.end(JSON.stringify({ error: 'Auth error: ' + err, average: null, count: 0 }));
      return;
    }

    if (url.pathname === '/sold') {
      searchEbay(kw, conditionId, token, function(result, searchErr) {
        res.writeHead(200);
        res.end(JSON.stringify(searchErr ? { error: searchErr, average: null, count: 0 } : result));
      });
      return;
    }

    if (url.pathname === '/price') {
      searchEbay(kw, conditionId, token, function(result, searchErr) {
        if (searchErr && isbn && title) {
          var kw2 = title + (author ? ' ' + author : '') + (signed ? ' signed' : '');
          searchEbay(kw2, conditionId, token, function(r2, e2) {
            res.writeHead(200);
            res.end(JSON.stringify(e2 ? { error: e2, average: null, count: 0 } : r2));
          });
          return;
        }
        res.writeHead(200);
        res.end(JSON.stringify(searchErr ? { error: searchErr, average: null, count: 0 } : result));
      });
      return;
    }

    res.writeHead(404);
    res.end('{}');
  });
});

server.listen(PORT, function() {
  console.log('BFA price server running on port ' + PORT);
});

// Fetch seller business policy IDs

function getSellerProfiles(cb) {
  var xmlBody = '<?xml version="1.0" encoding="utf-8"?>'
    + '<GetUserPreferencesRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<ShowSellerProfilePreferences>true</ShowSellerProfilePreferences>'
    + '</GetUserPreferencesRequest>';

  var opts = {
    hostname: 'api.ebay.com',
    path: '/ws/api.dll',
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'GetUserPreferences',
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID,
      'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': Buffer.byteLength(xmlBody)
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      console.log('Preferences response:', data.substring(0, 3000));
      var shipping = [];
      var returns = [];
      var payments = [];

      var shipMatches = data.match(/<ShippingPolicyProfile>[\s\S]*?<\/ShippingPolicyProfile>/g) || [];
      shipMatches.forEach(function(m) {
        var id = (m.match(/<ShippingProfileID>(\d+)<\/ShippingProfileID>/) || [])[1];
        var name = (m.match(/<ProfileName>(.*?)<\/ProfileName>/) || [])[1];
        if (id) shipping.push({ id: id, name: name });
      });

      var retMatches = data.match(/<ReturnPolicyProfile>[\s\S]*?<\/ReturnPolicyProfile>/g) || [];
      retMatches.forEach(function(m) {
        var id = (m.match(/<ReturnProfileID>(\d+)<\/ReturnProfileID>/) || [])[1];
        var name = (m.match(/<ProfileName>(.*?)<\/ProfileName>/) || [])[1];
        if (id) returns.push({ id: id, name: name });
      });

      var payMatches = data.match(/<PaymentProfile>[\s\S]*?<\/PaymentProfile>/g) || [];
      payMatches.forEach(function(m) {
        var id = (m.match(/<PaymentProfileID>(\d+)<\/PaymentProfileID>/) || [])[1];
        var name = (m.match(/<ProfileName>(.*?)<\/ProfileName>/) || [])[1];
        if (id) payments.push({ id: id, name: name });
      });

      cb({ shipping: shipping, returns: returns, payments: payments, raw: data.substring(0, 4000) });
    });
  });
  req.on('error', function(e) { cb({ error: e.message }); });
  req.setTimeout(15000, function() { req.destroy(); cb({ error: 'Timeout' }); });
  req.write(xmlBody);
  req.end();
}

function getExistingCategory(cb) {
  var xmlBody = '<?xml version="1.0" encoding="utf-8"?>'
    + '<GetSellerListRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<StartTimeFrom>2024-01-01T00:00:00.000Z</StartTimeFrom>'
    + '<StartTimeTo>2026-01-01T00:00:00.000Z</StartTimeTo>'
    + '<Pagination><EntriesPerPage>1</EntriesPerPage><PageNumber>1</PageNumber></Pagination>'
    + '<DetailLevel>ReturnAll</DetailLevel>'
    + '</GetSellerListRequest>';

  var opts = {
    hostname: 'api.ebay.com',
    path: '/ws/api.dll',
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'GetSellerList',
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID,
      'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': Buffer.byteLength(xmlBody)
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      var catMatch = data.match(/<PrimaryCategory>[\s\S]*?<CategoryID>(\d+)<\/CategoryID>/);
      var catNameMatch = data.match(/<PrimaryCategory>[\s\S]*?<CategoryName>(.*?)<\/CategoryName>/);
      cb({
        categoryId: catMatch ? catMatch[1] : null,
        categoryName: catNameMatch ? catNameMatch[1] : null,
        raw: data.substring(0, 1000)
      });
    });
  });
  req.on('error', function(e) { cb({ error: e.message }); });
  req.setTimeout(15000, function() { req.destroy(); cb({ error: 'Timeout' }); });
  req.write(xmlBody);
  req.end();
}

function uploadPicture(imgBuffer, cb) {
  var boundary = '---BFA_BOUNDARY_' + Date.now();
  var xmlPart = '<?xml version="1.0" encoding="utf-8"?>'
    + '<UploadSiteHostedPicturesRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<PictureName>bookcover</PictureName>'
    + '<PictureSet>Supersize</PictureSet>'
    + '</UploadSiteHostedPicturesRequest>';

  var body = Buffer.concat([
    Buffer.from(
      '--' + boundary + '\r\n'
      + 'Content-Disposition: form-data; name="XML Payload"\r\n'
      + 'Content-Type: text/xml;charset=utf-8\r\n\r\n'
      + xmlPart + '\r\n'
      + '--' + boundary + '\r\n'
      + 'Content-Disposition: form-data; name="image"; filename="bookcover.jpg"\r\n'
      + 'Content-Type: image/jpeg\r\n'
      + 'Content-Transfer-Encoding: binary\r\n\r\n'
    ),
    imgBuffer,
    Buffer.from('\r\n--' + boundary + '--\r\n')
  ]);

  var opts = {
    hostname: 'api.ebay.com',
    path: '/ws/api.dll',
    method: 'POST',
    headers: {
      'Content-Type': 'multipart/form-data; boundary=' + boundary,
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'UploadSiteHostedPictures',
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID,
      'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': body.length
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      console.log('Upload response:', data.substring(0, 500));
      var urlMatch = data.match(/<FullURL>(.*?)<\/FullURL>/);
      if (urlMatch) {
        cb({ pictureUrl: urlMatch[1] });
      } else {
        var errMatch = data.match(/<LongMessage>(.*?)<\/LongMessage>/);
        cb({ error: errMatch ? errMatch[1] : 'Upload failed', raw: data.substring(0, 300) });
      }
    });
  });
  req.on('error', function(e) { cb({ error: e.message }); });
  req.setTimeout(30000, function() { req.destroy(); cb({ error: 'Upload timeout' }); });
  req.write(body);
  req.end();
}
