var https = require('https');
var http = require('http');

var PORT = process.env.PORT || 3000;
var CLIENT_ID = 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';
var CLIENT_SECRET = 'PRD-6c135696e4a6-8789-475a-8eaf-1662';
var DEV_ID = '3e7db631-fffe-4cd8-92b6-6bca13515742';
var RUNAME = 'Codex_Brothers_-CodexBro-Booksf-ixdtwam';
var USER_TOKEN = 'v^1.1#i^1#f^0#r^1#p^3#I^3#t^Ul4xMF8yOkVBM0U2OUZBMEY0MDY0QjYxOEVCQTM2OTZFMTg0OEIwXzJfMSNFXjI2MA==';

var COND_MAP = {
  '1000': 'NEW', '1500': 'LIKE_NEW', '2500': 'VERY_GOOD', '3000': 'GOOD', '7000': 'ACCEPTABLE'
};

// OAuth user token storage
var userAccessToken = null;
var userRefreshToken = 'v^1.1#i^1#f^0#I^3#r^1#p^3#t^Ul4xMF84OkZBODQ2ODc2OUFEMUEzRTg2NjExQkVFMkI1QkRCMDc1XzJfMSNFXjI2MA==';
var userTokenExpiry = 0;

var cachedAppToken = null;
var appTokenExpiry = 0;

function getAppToken(cb) {
  if (cachedAppToken && Date.now() < appTokenExpiry) { cb(null, cachedAppToken); return; }
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
          cachedAppToken = json.access_token;
          appTokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
          cb(null, cachedAppToken);
        } else { cb('Token error: ' + data); }
      } catch(e) { cb('Token parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Timeout'); });
  req.write(body);
  req.end();
}

function refreshUserToken(cb) {
  if (!userRefreshToken) { cb('No refresh token - visit /auth first'); return; }
  if (userAccessToken && Date.now() < userTokenExpiry) { cb(null, userAccessToken); return; }
  var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
  var body = 'grant_type=refresh_token&refresh_token=' + encodeURIComponent(userRefreshToken)
    + '&scope=https%3A%2F%2Fapi.ebay.com%2Foauth%2Fapi_scope%2Fsell.inventory';
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
          userAccessToken = json.access_token;
          userTokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
          cb(null, userAccessToken);
        } else { console.log('Refresh failed:', data); cb('Refresh error: ' + data); }
      } catch(e) { cb('Refresh parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Timeout'); });
  req.write(body);
  req.end();
}

function exchangeCodeForToken(code, cb) {
  var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
  var body = 'grant_type=authorization_code&code=' + encodeURIComponent(code)
    + '&redirect_uri=' + encodeURIComponent(RUNAME);
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
          userAccessToken = json.access_token;
          userRefreshToken = json.refresh_token;
          userTokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
          console.log('SAVE THIS REFRESH TOKEN:', json.refresh_token);
          cb(null, json);
        } else { cb('Exchange error: ' + data); }
      } catch(e) { cb('Exchange parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(e.message); });
  req.setTimeout(10000, function() { req.destroy(); cb('Timeout'); });
  req.write(body);
  req.end();
}

function createDraftListing(title, description, price, conditionId, pictureUrl, language, author, userToken, cb) {
  var condMap = { 1000:'NEW', 1500:'LIKE_NEW', 2500:'USED_VERY_GOOD', 3000:'USED_GOOD', 7000:'USED_ACCEPTABLE' };
  var condStr = condMap[conditionId] || 'GOOD';
  var sku = 'BFA-' + Date.now();

  // Step 1: Create inventory item
  var product = {
    title: title.substring(0, 80),
    description: description
  };
  if (pictureUrl) product.imageUrls = [pictureUrl];

  var itemBody = JSON.stringify({
    availability: { shipToLocationAvailability: { quantity: 1 } },
    condition: condStr,
    product: product
  });

  var itemOpts = {
    hostname: 'api.ebay.com',
    path: '/sell/inventory/v1/inventory_item/' + sku,
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + userToken,
      'Content-Language': 'en-US',
      'Content-Length': Buffer.byteLength(itemBody)
    }
  };

  var req1 = https.request(itemOpts, function(res1) {
    var data1 = '';
    res1.on('data', function(c) { data1 += c; });
    res1.on('end', function() {
      console.log('Inventory item response:', res1.statusCode, data1.substring(0, 300));
      if (res1.statusCode !== 200 && res1.statusCode !== 204) {
        cb({ error: 'Inventory item failed: ' + data1.substring(0, 200) });
        return;
      }

      // Step 2: Create offer (unpublished = draft)
      var offerBody = JSON.stringify({
        sku: sku,
        marketplaceId: 'EBAY_US',
        format: 'FIXED_PRICE',
        listingDescription: description,
        availableQuantity: 1,
        categoryId: '261186',
        listingPolicies: {
          fulfillmentPolicyId: '',
          paymentPolicyId: '',
          returnPolicyId: ''
        },
        pricingSummary: { price: { currency: 'USD', value: price.toFixed(2) } },
        merchantLocationKey: 'default'
      });

      // Fetch policies first then create offer
      getPolicyIds(userToken, function(policies) {
        var offerData = {
          sku: sku,
          marketplaceId: 'EBAY_US',
          format: 'FIXED_PRICE',
          listingDescription: description,
          availableQuantity: 1,
          categoryId: '261186',
          pricingSummary: { price: { currency: 'USD', value: price.toFixed(2) } },
          tax: { applyTax: false }
        };
        if (policies && policies.fulfillmentPolicyId) {
          offerData.listingPolicies = {
            fulfillmentPolicyId: policies.fulfillmentPolicyId,
            returnPolicyId: policies.returnPolicyId,
            paymentPolicyId: policies.paymentPolicyId
          };
        }

        var offerBodyStr = JSON.stringify(offerData);
        var offerOpts = {
          hostname: 'api.ebay.com',
          path: '/sell/inventory/v1/offer',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + userToken,
            'Content-Language': 'en-US',
            'Content-Length': Buffer.byteLength(offerBodyStr)
          }
        };

        var req2 = https.request(offerOpts, function(res2) {
          var data2 = '';
          res2.on('data', function(c) { data2 += c; });
          res2.on('end', function() {
            console.log('Offer response:', res2.statusCode, data2.substring(0, 500));
            try {
              var json = JSON.parse(data2);
              if (json.offerId) {
                cb({ listingId: json.offerId, message: 'Draft created in eBay Drafts folder' });
              } else {
                cb({ error: JSON.stringify(json).substring(0, 300) });
              }
            } catch(e) { cb({ error: 'Parse error: ' + e.message + ' raw: ' + data2.substring(0,200) }); }
          });
        });
        req2.on('error', function(e) { cb({ error: e.message }); });
        req2.setTimeout(20000, function() { req2.destroy(); cb({ error: 'Timeout' }); });
        req2.write(offerBodyStr);
        req2.end();
      });
    });
  });
  req1.on('error', function(e) { cb({ error: e.message }); });
  req1.setTimeout(20000, function() { req1.destroy(); cb({ error: 'Timeout' }); });
  req1.write(itemBody);
  req1.end();
}

function getPolicyIds(userToken, cb) {
  // Get fulfillment policies
  var opts = {
    hostname: 'api.ebay.com',
    path: '/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US',
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + userToken, 'Content-Type': 'application/json' }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      console.log('Policies response:', data.substring(0, 500));
      try {
        var json = JSON.parse(data);
        var policies = json.fulfillmentPolicies || [];
        // Find media mail policy
        var ship = policies.find(function(p) { return p.name && p.name.indexOf('Shipping') > -1; }) || policies[0];
        if (!ship) { cb({}); return; }
        // Now get return policy
        var opts2 = {
          hostname: 'api.ebay.com',
          path: '/sell/account/v1/return_policy?marketplace_id=EBAY_US',
          method: 'GET',
          headers: { 'Authorization': 'Bearer ' + userToken, 'Content-Type': 'application/json' }
        };
        var req2 = https.request(opts2, function(res2) {
          var data2 = '';
          res2.on('data', function(c) { data2 += c; });
          res2.on('end', function() {
            try {
              var json2 = JSON.parse(data2);
              var retPolicies = json2.returnPolicies || [];
              var ret = retPolicies[0];
              // Get payment policy
              var opts3 = {
                hostname: 'api.ebay.com',
                path: '/sell/account/v1/payment_policy?marketplace_id=EBAY_US',
                method: 'GET',
                headers: { 'Authorization': 'Bearer ' + userToken, 'Content-Type': 'application/json' }
              };
              var req3 = https.request(opts3, function(res3) {
                var data3 = '';
                res3.on('data', function(c) { data3 += c; });
                res3.on('end', function() {
                  try {
                    var json3 = JSON.parse(data3);
                    var payPolicies = json3.paymentPolicies || [];
                    var pay = payPolicies[0];
                    cb({
                      fulfillmentPolicyId: ship ? ship.fulfillmentPolicyId : '',
                      returnPolicyId: ret ? ret.returnPolicyId : '',
                      paymentPolicyId: pay ? pay.paymentPolicyId : ''
                    });
                  } catch(e) { cb({}); }
                });
              });
              req3.on('error', function() { cb({}); });
              req3.end();
            } catch(e) { cb({}); }
          });
        });
        req2.on('error', function() { cb({}); });
        req2.end();
      } catch(e) { cb({}); }
    });
  });
  req.on('error', function() { cb({}); });
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

var server = http.createServer(function(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.setHeader('Content-Type','application/json'); res.writeHead(200); res.end(); return; }

  var url = new URL(req.url, 'http://localhost');

  // OAuth callback from eBay
  if (url.pathname === '/callback') {
    var code = url.searchParams.get('code');
    var error = url.searchParams.get('error');
    var errorDesc = url.searchParams.get('error_description');
    if (!code) {
      res.setHeader('Content-Type', 'text/html');
      res.writeHead(400);
      res.end('<h1>No code received</h1><p>Error: ' + (error||'none') + '</p><p>Description: ' + (errorDesc||'none') + '</p><p>All params: ' + url.search + '</p>');
      return;
    }
    exchangeCodeForToken(code, function(err, json) {
      res.setHeader('Content-Type', 'text/html');
      if (err) {
        res.writeHead(500);
        res.end('<h1>Auth failed: ' + err + '</h1>');
      } else {
        // Store refresh token in memory and show success
        res.writeHead(200);
        res.end('<h1>Auth successful!</h1><p>Refresh token saved. You can close this window.</p><p>Refresh token: <code>' + json.refresh_token + '</code></p><p><strong>Copy this refresh token and add it to your server as REFRESH_TOKEN variable!</strong></p>');
      }
    });
    return;
  }

  // Auth redirect to eBay
  if (url.pathname === '/auth') {
    var scope = 'https://api.ebay.com/oauth/api_scope/sell.inventory';
    var authUrl = 'https://auth.ebay.com/oauth2/authorize?client_id=' + CLIENT_ID
      + '&response_type=code&redirect_uri=' + encodeURIComponent(RUNAME)
      + '&scope=' + encodeURIComponent(scope);
    res.setHeader('Content-Type', 'text/html');
    res.writeHead(200);
    res.end('<h1>Click to authorize eBay</h1><a href="' + authUrl + '" style="font-size:24px;padding:20px;background:#0064d2;color:white;text-decoration:none;border-radius:8px;">Authorize with eBay</a>');
    return;
  }

  res.setHeader('Content-Type', 'application/json');

  // /list — POST only
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
        var conditionId = parseInt(data.conditionId) || 3000;
        var pictureUrl = data.pictureUrl || '';
        var language = data.language || 'English';
        var author = data.author || 'Unknown';
        if (!title) { res.writeHead(400); res.end('{"error":"missing title"}'); return; }
        refreshUserToken(function(err, token) {
          if (err) { res.writeHead(200); res.end(JSON.stringify({ error: 'Auth failed: ' + err })); return; }
          createDraftListing(title, description, price, conditionId, pictureUrl, language, author, token, function(result) {
            res.writeHead(200);
            res.end(JSON.stringify(result));
          });
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

  var title = url.searchParams.get('title') || '';
  var author = url.searchParams.get('author') || '';
  var isbn = (url.searchParams.get('isbn') || '').replace(/[^0-9X]/gi, '');
  var cond = url.searchParams.get('condition') || '3000';
  var signed = url.searchParams.get('signed') === '1';
  var conditionId = COND_MAP[cond] || 'GOOD';

  if (!title && !isbn) { res.writeHead(400); res.end('{"error":"missing title or isbn"}'); return; }

  var kw = isbn ? isbn : (title + (author ? ' ' + author : '') + (signed ? ' signed' : ''));

  getAppToken(function(err, token) {
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
