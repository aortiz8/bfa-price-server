var https = require('https');
var http = require('http');

var PORT = process.env.PORT || 3000;
var CLIENT_ID = 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';
var CLIENT_SECRET = 'PRD-6c135696e4a6-8789-475a-8eaf-1662';

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
  if (cachedToken && Date.now() < tokenExpiry) {
    cb(null, cachedToken);
    return;
  }
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
        } else {
          cb('Token error: ' + data);
        }
      } catch(e) { cb('Token parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Token timeout'); });
  req.write(body);
  req.end();
}

function searchActive(keywords, conditionId, token, cb) {
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
          var item = items[i];
          var price = item.price && parseFloat(item.price.value);
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

function searchSold(keywords, conditionId, token, cb) {
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
          var item = items[i];
          var price = item.price && parseFloat(item.price.value);
          if (!price || isNaN(price) || price <= 0) continue;
          prices.push(price);
        }
        if (prices.length === 0) { cb(null, 'No prices'); return; }
        var sum = 0;
        for (var j = 0; j < prices.length; j++) sum += prices[j];
        var avg2 = Math.round(sum / prices.length * 100) / 100;
        cb({ count: prices.length, average: Math.round((avg2 + 3.99) * 100) / 100 });
      } catch(e) { cb(null, 'Parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(null, e.message); });
  req.setTimeout(15000, function() { req.destroy(); cb(null, 'Timeout'); });
  req.end();
}

var server = http.createServer(function(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS');
  res.setHeader('Content-Type', 'application/json');
  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }
  if (req.method !== 'GET') { res.writeHead(405); res.end('{}'); return; }
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
        if (!title) { res.writeHead(400); res.end('{"error":"missing title"}'); return; }
        createListing(title, description, price, isbn, conditionId, function(result) {
          res.writeHead(200);
          res.end(JSON.stringify(result));
        });
      } catch(e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
    return;
  }

  if (url.pathname === '/health') {
    res.writeHead(200);
    res.end('{"status":"ok"}');
    return;
  }

  var title = url.searchParams.get('title') || '';
  var author = url.searchParams.get('author') || '';
  var isbn = (url.searchParams.get('isbn') || '').replace(/[^0-9X]/gi, '');
  var cond = url.searchParams.get('condition') || '3000';
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
      searchSold(kw, conditionId, token, function(result, searchErr) {
        res.writeHead(200);
        res.end(JSON.stringify(searchErr
          ? { error: searchErr, average: null, count: 0 }
          : result));
      });
      return;
    }

    if (url.pathname === '/price') {
      searchActive(kw, conditionId, token, function(result, searchErr) {
        if (searchErr && isbn && title) {
          var kw2 = title + (author ? ' ' + author : '') + (signed ? ' signed' : '');
          searchActive(kw2, conditionId, token, function(r2, e2) {
            res.writeHead(200);
            res.end(JSON.stringify(e2
              ? { error: e2, average: null, count: 0 }
              : r2));
          });
          return;
        }
        res.writeHead(200);
        res.end(JSON.stringify(searchErr
          ? { error: searchErr, average: null, count: 0 }
          : result));
      });
      return;
    }

    if (url.pathname === '/list') {
      var body = '';
      req.on('data', function(c) { body += c; });
      req.on('end', function() {
        try {
          var item = JSON.parse(body);
          createEbayListing(item, function(result, err) {
            res.writeHead(200);
            res.end(JSON.stringify(err ? { error: err } : result));
          });
        } catch(e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }

    res.writeHead(404);
    res.end('{}');
  });
});


var USER_TOKEN = 'v^1.1#i^1#f^0#r^1#p^3#I^3#t^Ul4xMF8yOkVBM0U2OUZBMEY0MDY0QjYxOEVCQTM2OTZFMTg0OEIwXzJfMSNFXjI2MA==';

function createListing(title, description, price, isbn, conditionId, cb) {
  var condMap = {
    1000: 'NEW',
    1500: 'LIKE_NEW',
    2500: 'VERY_GOOD',
    3000: 'GOOD',
    7000: 'ACCEPTABLE'
  };
  var condStr = condMap[conditionId] || 'GOOD';

  var xmlBody = '<?xml version="1.0" encoding="utf-8"?>'
    + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<Item>'
    + '<Title>' + title.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').substring(0,80) + '</Title>'
    + '<Description><![CDATA[' + description + ']]></Description>'
    + '<PrimaryCategory><CategoryID>267</CategoryID></PrimaryCategory>'
    + '<StartPrice>' + price.toFixed(2) + '</StartPrice>'
    + '<CategoryMappingAllowed>true</CategoryMappingAllowed>'
    + '<ConditionID>' + conditionId + '</ConditionID>'
    + '<Country>US</Country>'
    + '<Currency>USD</Currency>'
    + '<DispatchTimeMax>2</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<PaymentMethods>PayPal</PaymentMethods>'
    + '<PayPalEmailAddress>booksforages@email.com</PayPalEmailAddress>'
    + '<PictureDetails><GalleryType>Gallery</GalleryType></PictureDetails>'
    + '<ReturnPolicy>'
    + '<ReturnsAcceptedOption>ReturnsAccepted</ReturnsAcceptedOption>'
    + '<RefundOption>MoneyBack</RefundOption>'
    + '<ReturnsWithinOption>Days_30</ReturnsWithinOption>'
    + '<ShippingCostPaidByOption>Buyer</ShippingCostPaidByOption>'
    + '</ReturnPolicy>'
    + '<ShippingDetails>'
    + '<ShippingType>Flat</ShippingType>'
    + '<ShippingServiceOptions>'
    + '<ShippingServicePriority>1</ShippingServicePriority>'
    + '<ShippingService>USPSMediaMail</ShippingService>'
    + '<ShippingServiceCost>3.99</ShippingServiceCost>'
    + '</ShippingServiceOptions>'
    + '</ShippingDetails>'
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
      'X-EBAY-API-APP-NAME': 'CodexBro-Booksfor-PRD-66c135696-2728b4d0',
      'X-EBAY-API-DEV-NAME': '3e7db631-fffe-4cd8-92b6-6bca13515742',
      'X-EBAY-API-CERT-NAME': 'PRD-6c135696e4a6-8789-475a-8eaf-1662',
      'Content-Length': Buffer.byteLength(xmlBody)
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      var idMatch = data.match(/<ItemID>(\d+)<\/ItemID>/);
      var errMatch = data.match(/<LongMessage>(.*?)<\/LongMessage>/);
      if (idMatch) {
        cb({ listingId: idMatch[1] });
      } else {
        cb({ error: errMatch ? errMatch[1] : 'Unknown eBay error', raw: data.substring(0, 300) });
      }
    });
  });
  req.on('error', function(e) { cb({ error: e.message }); });
  req.setTimeout(20000, function() { req.destroy(); cb({ error: 'Timeout' }); });
  req.write(xmlBody);
  req.end();
}

server.listen(PORT, function() {
  console.log('BFA price server running on port ' + PORT);
});

// eBay Trading API - Create Draft Listing
var DEV_ID = '3e7db631-fffe-4cd8-92b6-6bca13515742';
var USER_TOKEN = 'v^1.1#i^1#f^0#r^1#p^3#I^3#t^Ul4xMF8yOkVBM0U2OUZBMEF0MDY0QjYxOEVCQTM2OTZFMTg0OEIwXzJfMSNFXjI2MA==';

function createEbayListing(item, cb) {
  var condMap = {'0':'1000','1':'1500','2':'2500','3':'3000','4':'7000'};
  var condId = condMap[String(item.gradeIdx)] || '3000';

  var xml = '<?xml version="1.0" encoding="utf-8"?>'
    + '<AddItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">'
    + '<RequesterCredentials><eBayAuthToken>' + USER_TOKEN + '</eBayAuthToken></RequesterCredentials>'
    + '<Item>'
    + '<Title>' + (item.title||'').replace(/[<>&]/g,'').substring(0,80) + '</Title>'
    + '<Description><![CDATA[' + (item.description||'') + ']]></Description>'
    + '<PrimaryCategory><CategoryID>267</CategoryID></PrimaryCategory>'
    + '<StartPrice>' + (item.price||'9.99') + '</StartPrice>'
    + '<CategoryMappingAllowed>true</CategoryMappingAllowed>'
    + '<ConditionID>' + condId + '</ConditionID>'
    + '<Country>US</Country>'
    + '<Currency>USD</Currency>'
    + '<DispatchTimeMax>2</DispatchTimeMax>'
    + '<ListingDuration>GTC</ListingDuration>'
    + '<ListingType>FixedPriceItem</ListingType>'
    + '<PaymentMethods>MOCC</PaymentMethods>'
    + '<PostalCode>90001</PostalCode>'
    + '<Quantity>1</Quantity>'
    + '<ShippingDetails>'
    + '<ShippingType>Flat</ShippingType>'
    + '<ShippingServiceOptions>'
    + '<ShippingServicePriority>1</ShippingServicePriority>'
    + '<ShippingService>USPSMedia</ShippingService>'
    + '<ShippingServiceCost>3.99</ShippingServiceCost>'
    + '</ShippingServiceOptions>'
    + '</ShippingDetails>'
    + '<Site>US</Site>'
    + '</Item>'
    + '</AddItemRequest>';

  var opts = {
    hostname: 'api.ebay.com',
    path: '/ws/api.dll',
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
      'X-EBAY-API-CALL-NAME': 'AddItem',
      'X-EBAY-API-APP-NAME': CLIENT_ID,
      'X-EBAY-API-DEV-NAME': DEV_ID,
      'X-EBAY-API-CERT-NAME': CLIENT_SECRET,
      'Content-Length': Buffer.byteLength(xml)
    }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        var idMatch = data.match(/<ItemID>(\d+)<\/ItemID>/);
        var errMatch = data.match(/<LongMessage>(.*?)<\/LongMessage>/);
        if (idMatch) {
          cb({ itemId: idMatch[1], url: 'https://www.ebay.com/itm/' + idMatch[1] });
        } else {
          cb(null, errMatch ? errMatch[1] : 'Unknown error: ' + data.substring(0,200));
        }
      } catch(e) { cb(null, 'Parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(null, e.message); });
  req.setTimeout(20000, function() { req.destroy(); cb(null, 'Timeout'); });
  req.write(xml);
  req.end();
}
