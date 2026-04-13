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
        var totals = [];
        for (var i = 0; i < items.length; i++) {
          var item = items[i];
          var price = item.price && parseFloat(item.price.value);
          if (!price || isNaN(price) || price <= 0) continue;
          var shipping = 0;
          if (item.shippingOptions && item.shippingOptions.length > 0) {
            var s = item.shippingOptions[0];
            if (s.shippingCost && s.shippingCost.value) {
              shipping = parseFloat(s.shippingCost.value) || 0;
            }
          }
          totals.push(price + shipping);
        }
        if (totals.length === 0) { cb(null, 'No prices'); return; }
        var sum = 0;
        for (var j = 0; j < totals.length; j++) sum += totals[j];
        cb({ count: totals.length, average: Math.round(sum / totals.length * 100) / 100 });
      } catch(e) { cb(null, 'Parse error: ' + e.message); }
    });
  });
  req.on('error', function(e) { cb(null, e.message); });
  req.setTimeout(15000, function() { req.destroy(); cb(null, 'Timeout'); });
  req.end();
}

function searchSold(keywords, cb) {
  // Fetch eBay sold listings with free shipping filter
  var path = '/sch/i.html?_nkw=' + encodeURIComponent(keywords)
    + '&LH_Sold=1&LH_Complete=1&LH_FS=1&_sop=12&_ipg=25';
  var opts = {
    hostname: 'www.ebay.com',
    path: path,
    method: 'GET',
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; BooksForAges/1.0)',
      'Accept': 'text/html'
    }
  };
  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        // Parse prices from eBay search results HTML
        var prices = [];
        var regex = /s-item__price[^>]*>[\s]*<span[^>]*>\$([0-9]+\.[0-9]+)/g;
        var match;
        while ((match = regex.exec(data)) !== null) {
          var p = parseFloat(match[1]);
          if (p && !isNaN(p) && p > 0 && p < 500) prices.push(p);
        }
        // Also try another pattern
        if (prices.length === 0) {
          var regex2 = /\$([0-9]+\.[0-9]{2})(?:[^0-9])/g;
          var seen = {};
          while ((match = regex2.exec(data)) !== null) {
            var p2 = parseFloat(match[1]);
            if (p2 && !isNaN(p2) && p2 > 0.5 && p2 < 200 && !seen[p2]) {
              seen[p2] = true;
              prices.push(p2);
              if (prices.length >= 20) break;
            }
          }
        }
        if (prices.length === 0) { cb(null, 'No sold prices found'); return; }
        var sum = 0;
        for (var j = 0; j < prices.length; j++) sum += prices[j];
        cb({ count: prices.length, average: Math.round(sum / prices.length * 100) / 100 });
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

  if (url.pathname === '/sold') {
    searchSold(kw, function(result, err) {
      res.writeHead(200);
      res.end(JSON.stringify(err
        ? { error: err, average: null, count: 0 }
        : result));
    });
    return;
  }

  if (url.pathname === '/price') {
    getToken(function(err, token) {
      if (err) {
        res.writeHead(200);
        res.end(JSON.stringify({ error: 'Auth error: ' + err, average: null, count: 0 }));
        return;
      }
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
    });
    return;
  }

  res.writeHead(404);
  res.end('{}');
});

server.listen(PORT, function() {
  console.log('BFA price server running on port ' + PORT);
});
