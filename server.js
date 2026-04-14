var https = require('https');
var http = require('http');

var PORT = process.env.PORT || 3000;
var CLIENT_ID = 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';
var CLIENT_SECRET = 'PRD-6c135696e4a6-8789-475a-8eaf-1662';
var DEV_ID = '3e7db631-fffe-4cd8-92b6-6bca13515742';
var USER_TOKEN = 'v^1.1#i^1#f^0#r^1#p^3#I^3#t^Ul4xMF8yOkVBM0U2OUZBMEY0MDY0QjYxOEVCQTM2OTZFMTg0OEIwXzJfMSNFXjI2MA==';

var COND_MAP = {
  '1000': 'NEW', '1500': 'LIKE_NEW', '2500': 'VERY_GOOD', '3000': 'GOOD', '4000': 'GOOD', '5000': 'GOOD', '7000': 'ACCEPTABLE'
};

var cachedToken = null;
var tokenExpiry = 0;

function getToken(cb) {
  if (cachedToken && Date.now() < tokenExpiry) { cb(null, cachedToken); return; }
  var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
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
          cachedToken = json.access_token;
          tokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
          cb(null, cachedToken);
        } else { cb('Token error: ' + data); }
      } catch(e) { cb('Token parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Timeout'); });
  req.write(body);
  req.end();
}

function searchEbay(keywords, conditionId, token, cb) {
  var condFilter = conditionId ? ',conditions:{' + conditionId + '}' : '';
  var query = '/buy/browse/v1/item_summary/search?q=' + encodeURIComponent(keywords)
    + '&category_ids=267&filter=buyingOptions:{FIXED_PRICE}' + condFilter + '&limit=20&sort=price';
  var opts = {
    hostname: 'api.ebay.com', path: query, method: 'GET',
    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US' }
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

function uploadPicture(imgBuffer, cb) {
  var boundary = 'BFA_BOUNDARY_' + Date.now();
  var xmlPart = '<?xml version="1.0" encoding="utf-8"?>'
    + '<UploadSiteHostedPicturesRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<PictureName>bookcover</PictureName>'
    + '</UploadSiteHostedPicturesRequest>';
  var body = Buffer.concat([
    Buffer.from('--' + boundary + '\r\nContent-Disposition: form-data; name="XML Payload"\r\nContent-Type: text/xml;charset=utf-8\r\n\r\n' + xmlPart + '\r\n--' + boundary + '\r\nContent-Disposition: form-data; name="image"; filename="bookcover.jpg"\r\nContent-Type: image/jpeg\r\nContent-Transfer-Encoding: binary\r\n\r\n'),
    imgBuffer,
    Buffer.from('\r\n--' + boundary + '--\r\n')
  ]);
  var opts = {
    hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
    headers: {
      'Content-Type': 'multipart/form-data; boundary=' + boundary,
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967', 'X-EBAY-API-CALL-NAME': 'UploadSiteHostedPictures',
      'X-EBAY-API-SITEID': '0', 'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID, 'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': body.length
    }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      var urlMatch = data.match(/<FullURL>(.*?)<\/FullURL>/);
      if (urlMatch) { cb({ pictureUrl: urlMatch[1] }); }
      else { var e = data.match(/<LongMessage>(.*?)<\/LongMessage>/); cb({ error: e ? e[1] : 'Upload failed' }); }
    });
  });
  req.on('error', function(e) { cb({ error: e.message }); });
  req.setTimeout(30000, function() { req.destroy(); cb({ error: 'Upload timeout' }); });
  req.write(body);
  req.end();
}

function esc(s) {
  return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function createListing(title, description, price, isbn, conditionId, pictureUrl, language, author, cb) {
  // Schedule 7 days from now so it goes to Scheduled folder
  var scheduleTime = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();

  var pictures = pictureUrl
    ? '<PictureDetails><PictureURL>' + pictureUrl + '</PictureURL></PictureDetails>'
    : '';

  var xmlBody = '<?xml version="1.0" encoding="utf-8"?>'
    + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<Item>'
    + '<Title>' + esc(title).substring(0, 80) + '</Title>'
    + '<Description><![CDATA[' + description + ']]></Description>'
    + '<PrimaryCategory><CategoryID>261186</CategoryID></PrimaryCategory>'
    + '<StartPrice>' + price.toFixed(2) + '</StartPrice>'
    + '<CategoryMappingAllowed>true</CategoryMappingAllowed>'
    + '<ConditionID>' + conditionId + '</ConditionID>'
    + '<ItemSpecifics>'
    + '<NameValueList><Name>Book Title</Name><Value>' + esc(title).substring(0, 65) + '</Value></NameValueList>'
    + '<NameValueList><Name>Author</Name><Value>' + esc(author || 'Unknown').substring(0, 65) + '</Value></NameValueList>'
    + '<NameValueList><Name>Language</Name><Value>' + esc(language || 'English') + '</Value></NameValueList>'
    + '</ItemSpecifics>'
    + pictures
    + '<Country>US</Country>'
    + '<Currency>USD</Currency>'
    + '<DispatchTimeMax>2</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<Quantity>1</Quantity>'
    + '<ScheduleTime>' + scheduleTime + '</ScheduleTime>'
    + '<PostalCode>92105</PostalCode>'
    + '<SellerProfiles>'
    + '<SellerShippingProfile><ShippingProfileName>Shipping 2020-01-02</ShippingProfileName></SellerShippingProfile>'
    + '<SellerReturnProfile><ReturnProfileName>Returns Accepted,Buyer,30 Days,Money Back#0</ReturnProfileName></SellerReturnProfile>'
    + '<SellerPaymentProfile><PaymentProfileName>eBay Payments</PaymentProfileName></SellerPaymentProfile>'
    + '</SellerProfiles>'
    + '<Site>US</Site>'
    + '</Item>'
    + '</AddItemRequest>';

  var opts = {
    hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967', 'X-EBAY-API-CALL-NAME': 'AddItem',
      'X-EBAY-API-SITEID': '0', 'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID, 'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': Buffer.byteLength(xmlBody)
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      var condMatch = xmlBody.indexOf('<ConditionID>'); console.log('eBay CONDITIONID:', xmlBody.substring(condMatch+13, condMatch+17)); console.log('eBay response:', data.substring(0, 800));
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
        var pictureUrl = data.pictureUrl || '';
        var language = data.language || 'English';
        var author = data.author || 'Unknown';
        if (!title) { res.writeHead(400); res.end('{"error":"missing title"}'); return; }
        createListing(title, description, price, isbn, conditionId, pictureUrl, language, author, function(result) {
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

  if (url.pathname === '/upload') {
    if (req.method !== 'POST') { res.writeHead(405); res.end('{}'); return; }
    var body = '';
    req.on('data', function(c) { body += c; });
    req.on('end', function() {
      try {
        var data = JSON.parse(body);
        var b64 = (data.image || '').replace(/^data:image\/[a-z]+;base64,/, '');
        uploadPicture(Buffer.from(b64, 'base64'), function(result) {
          res.writeHead(200); res.end(JSON.stringify(result));
        });
      } catch(e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  if (req.method !== 'GET') { res.writeHead(405); res.end('{}'); return; }

  if (url.pathname === '/health') { res.writeHead(200); res.end('{"status":"ok"}'); return; }

  if (url.pathname === '/conditions') {
    var xmlBody2 = '<?xml version="1.0" encoding="utf-8"?>'
      + '<GetCategoryFeaturesRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
      + '<CategoryID>261186</CategoryID>'
      + '<FeatureID>ConditionValues</FeatureID>'
      + '<DetailLevel>ReturnAll</DetailLevel>'
      + '</GetCategoryFeaturesRequest>';
    var opts2 = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'Content-Type': 'text/xml',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '967', 'X-EBAY-API-CALL-NAME': 'GetCategoryFeatures',
        'X-EBAY-API-SITEID': '0', 'X-EBAY-API-APP-NAME': CLIENT_ID,
        'X-EBAY-API-DEV-NAME': DEV_ID, 'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
        'Content-Length': Buffer.byteLength(xmlBody2)
      }
    };
    var req2 = https.request(opts2, function(res2) {
      var data2 = '';
      res2.on('data', function(c) { data2 += c; });
      res2.on('end', function() {
        var results = [];
        var idx = 0;
        while (true) {
          var start = data2.indexOf('<ConditionValue>', idx);
          if (start === -1) break;
          var end = data2.indexOf('</ConditionValue>', start);
          var chunk = data2.substring(start, end);
          var idStart = chunk.indexOf('<ID>') + 4;
          var idEnd = chunk.indexOf('</ID>');
          var nameStart = chunk.indexOf('<DisplayName>') + 13;
          var nameEnd = chunk.indexOf('</DisplayName>');
          var id = chunk.substring(idStart, idEnd);
          var name = chunk.substring(nameStart, nameEnd);
          if (id && name) results.push({ id: id, name: name });
          idx = end + 1;
        }
        res.writeHead(200);
        res.end(JSON.stringify({ conditions: results, raw: data2.substring(0, 2000) }));
      });
    });
    req2.on('error', function(e) { res.writeHead(500); res.end(JSON.stringify({error: e.message})); });
    req2.setTimeout(15000, function() { req2.destroy(); });
    req2.write(xmlBody2);
    req2.end();
    return;
  }

  if (url.pathname === '/getitem') {
    var itemId = url.searchParams.get('id') || '';
    if (!itemId) { res.writeHead(400); res.end('{"error":"missing id"}'); return; }
    var xmlBody = '<?xml version="1.0" encoding="utf-8"?>'
      + '<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
      + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
      + '<ItemID>' + itemId + '</ItemID>'
      + '<DetailLevel>ReturnAll</DetailLevel>'
      + '</GetItemRequest>';
    var opts = {
      hostname: 'api.ebay.com', path: '/ws/api.dll', method: 'POST',
      headers: {
        'Content-Type': 'text/xml',
        'X-EBAY-API-COMPATIBILITY-LEVEL': '967', 'X-EBAY-API-CALL-NAME': 'GetItem',
        'X-EBAY-API-SITEID': '0', 'X-EBAY-API-APP-NAME': CLIENT_ID,
        'X-EBAY-API-DEV-NAME': DEV_ID, 'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
        'Content-Length': Buffer.byteLength(xmlBody)
      }
    };
    var req = https.request(opts, function(res2) {
      var data = '';
      res2.on('data', function(c) { data += c; });
      res2.on('end', function() {
        var condId = (data.match(/<ConditionID>(\d+)<\/ConditionID>/) || [])[1];
        var condName = (data.match(/<ConditionDisplayName>(.*?)<\/ConditionDisplayName>/) || [])[1];
        res.writeHead(200);
        res.end(JSON.stringify({ conditionId: condId, conditionName: condName }));
      });
    });
    req.on('error', function(e) { res.writeHead(500); res.end(JSON.stringify({error: e.message})); });
    req.setTimeout(10000, function() { req.destroy(); });
    req.write(xmlBody);
    req.end();
    return;
  }

  var title = url.searchParams.get('title') || '';
  var author = url.searchParams.get('author') || '';
  var isbn = (url.searchParams.get('isbn') || '').replace(/[^0-9X]/gi, '');
  var cond = url.searchParams.get('condition') || '3000';
  var signed = url.searchParams.get('signed') === '1';
  var conditionId = COND_MAP[cond] || 'GOOD';

  if (!title && !isbn) { res.writeHead(400); res.end('{"error":"missing title or isbn"}'); return; }

  var kw = isbn ? isbn : (title + (author ? ' ' + author : '') + (signed ? ' signed' : ''));

  getToken(function(err, token) {
    if (err) { res.writeHead(200); res.end(JSON.stringify({ error: 'Auth error: ' + err, average: null, count: 0 })); return; }

    if (url.pathname === '/sold' || url.pathname === '/price') {
      searchEbay(kw, conditionId, token, function(result, searchErr) {
        if (searchErr && isbn && title && url.pathname === '/price') {
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

    res.writeHead(404); res.end('{}');
  });
});

server.listen(PORT, function() {
  console.log('BFA price server running on port ' + PORT);
});
