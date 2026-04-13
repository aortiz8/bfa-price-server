var https = require('https');
var http = require('http');

var PORT = process.env.PORT || 3000;
var APP_ID = 'CodexBro-Booksfor-PRD-66c135696-2728b4d0';

var FALLBACK = {
  '1000': ['1000', '1500', '2500'],
  '1500': ['1500', '2500'],
  '2500': ['2500', '3000'],
  '3000': ['3000'],
  '7000': ['7000']
};

function search(keywords, condIds, cb) {
  var filters = '';
  for (var i = 0; i < condIds.length; i++) {
    filters += '&itemFilter(' + (i + 2) + ').name=Condition'
      + '&itemFilter(' + (i + 2) + ').value(' + i + ')=' + condIds[i];
  }

  var path = '/services/search/FindingService/v1'
    + '?OPERATION-NAME=findCompletedItems'
    + '&SERVICE-VERSION=1.0.0'
    + '&SECURITY-APPNAME=' + APP_ID
    + '&RESPONSE-DATA-FORMAT=JSON'
    + '&REST-PAYLOAD'
    + '&keywords=' + encodeURIComponent(keywords)
    + '&categoryId=267'
    + '&itemFilter(0).name=SoldItemsOnly'
    + '&itemFilter(0).value=true'
    + '&itemFilter(1).name=ListingType'
    + '&itemFilter(1).value=FixedPrice'
    + filters
    + '&paginationInput.entriesPerPage=20'
    + '&sortOrder=EndTimeSoonest';

  var opts = {
    hostname: 'svcs.ebay.com',
    path: path,
    method: 'GET',
    headers: { 'Content-Type': 'application/json' }
  };

  var req = https.request(opts, function(res) {
    var data = '';
    res.on('data', function(c) { data += c; });
    res.on('end', function() {
      try {
        var json = JSON.parse(data);
        var resp = json['findCompletedItemsResponse'];
        if (!resp || !resp[0]) { cb(null, 'No response'); return; }
        if (resp[0].ack[0] !== 'Success') { cb(null, 'API error: ' + resp[0].ack[0]); return; }
        var items = resp[0].searchResult
          && resp[0].searchResult[0]
          && resp[0].searchResult[0].item;
        if (!items || items.length === 0) { cb(null, 'No items'); return; }

        var prices = [];
        for (var i = 0; i < items.length; i++) {
          var ss = items[i].sellingStatus && items[i].sellingStatus[0];
          var priceObj = ss
            && ss.convertedCurrentPrice
            && ss.convertedCurrentPrice[0];
          var p = priceObj && parseFloat(priceObj['__value__']);
          if (p && !isNaN(p) && p > 0) prices.push(p);
        }

        if (prices.length === 0) { cb(null, 'No prices found'); return; }

        var sum = 0;
        for (var j = 0; j < prices.length; j++) sum += prices[j];
        cb({ count: prices.length, average: Math.round(sum / prices.length * 100) / 100 });

      } catch (e) {
        cb(null, 'Parse error: ' + e.message);
      }
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

  if (url.pathname !== '/price') {
    res.writeHead(404);
    res.end('{}');
    return;
  }

  var title = url.searchParams.get('title') || '';
  var author = url.searchParams.get('author') || '';
  var isbn = (url.searchParams.get('isbn') || '').replace(/[^0-9X]/gi, '');
  var cond = url.searchParams.get('condition') || '3000';
  var signed = url.searchParams.get('signed') === '1';

  var condIds = FALLBACK[cond] || ['3000'];

  if (!title && !isbn) {
    res.writeHead(400);
    res.end('{"error":"missing title or isbn"}');
    return;
  }

  var kw = isbn
    ? isbn
    : (title + (author ? ' ' + author : '') + (signed ? ' signed' : ''));

  console.log('Search:', kw, 'Cond IDs:', condIds.join(','));

  search(kw, condIds, function(result, err) {
    if (err && isbn && title) {
      var kw2 = title + (author ? ' ' + author : '') + (signed ? ' signed' : '');
      console.log('Fallback search:', kw2);
      search(kw2, condIds, function(r2, e2) {
        res.writeHead(200);
        res.end(JSON.stringify(e2
          ? { error: e2, average: null, count: 0 }
          : r2));
      });
      return;
    }
    res.writeHead(200);
    res.end(JSON.stringify(err
      ? { error: err, average: null, count: 0 }
      : result));
  });
});

server.listen(PORT, function() {
  console.log('BFA price server running on port ' + PORT);
});
