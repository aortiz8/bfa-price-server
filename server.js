const https = require(‘https’);
const http = require(‘http’);

const PORT = process.env.PORT || 3000;
const EBAY_APP_ID = ‘CodexBro-Booksfor-PRD-66c135696-2728b4d0’;

// eBay condition IDs for Finding API
var CONDITION_FALLBACK = {
‘1000’: [‘1000’,‘1500’,‘2500’],
‘1500’: [‘1500’,‘2500’],
‘2500’: [‘2500’,‘3000’],
‘3000’: [‘3000’],
‘7000’: [‘7000’]
};

// eBay condition names for filtering
var CONDITION_NAMES = {
‘1000’: ‘New’,
‘1500’: ‘Like New’,
‘2500’: ‘Very Good’,
‘3000’: ‘Good’,
‘7000’: ‘Acceptable’
};

function fetchEbayFindingAPI(keywords, conditionIds, callback) {
// Build condition filters
var conditionFilters = ‘’;
for (var i = 0; i < conditionIds.length; i++) {
conditionFilters += ‘&itemFilter(’ + (i+2) + ‘).name=Condition’
+ ‘&itemFilter(’ + (i+2) + ‘).value(’ + i + ‘)=’ + CONDITION_NAMES[conditionIds[i]];
}

var path = ‘/services/search/FindingService/v1’
+ ‘?OPERATION-NAME=findCompletedItems’
+ ‘&SERVICE-VERSION=1.0.0’
+ ‘&SECURITY-APPNAME=’ + EBAY_APP_ID
+ ‘&RESPONSE-DATA-FORMAT=JSON’
+ ‘&REST-PAYLOAD’
+ ‘&keywords=’ + encodeURIComponent(keywords)
+ ‘&categoryId=267’
+ ‘&itemFilter(0).name=SoldItemsOnly’
+ ‘&itemFilter(0).value=true’
+ ‘&itemFilter(1).name=ListingType’
+ ‘&itemFilter(1).value=FixedPrice’
+ conditionFilters
+ ‘&paginationInput.entriesPerPage=20’
+ ‘&sortOrder=EndTimeSoonest’;

console.log(‘eBay Finding API keywords:’, keywords, ‘| conditions:’, conditionIds.join(’,’));

var options = {
hostname: ‘svcs.ebay.com’,
path: path,
method: ‘GET’,
headers: {
‘Content-Type’: ‘application/json’
}
};

var req = https.request(options, function(res) {
var data = ‘’;
res.on(‘data’, function(chunk) { data += chunk; });
res.on(‘end’, function() {
try {
var json = JSON.parse(data);
var response = json[‘findCompletedItemsResponse’];
if (!response || !response[0]) { callback(null, ‘No response from eBay API’); return; }
var ack = response[0].ack && response[0].ack[0];
if (ack !== ‘Success’) {
var errMsg = response[0].errorMessage && response[0].errorMessage[0]
&& response[0].errorMessage[0].error && response[0].errorMessage[0].error[0]
&& response[0].errorMessage[0].error[0].message && response[0].errorMessage[0].error[0].message[0];
callback(null, ’eBay API error: ’ + (errMsg || ack));
return;
}
var items = response[0].searchResult && response[0].searchResult[0]
&& response[0].searchResult[0].item;
if (!items || items.length === 0) { callback(null, ‘No sold items found’); return; }

```
    var prices = [];
    for (var i = 0; i < items.length; i++) {
      var item = items[i];
      var sellingStatus = item.sellingStatus && item.sellingStatus[0];
      var price = sellingStatus && sellingStatus.convertedCurrentPrice
        && sellingStatus.convertedCurrentPrice[0]
        && parseFloat(sellingStatus.convertedCurrentPrice[0]['__value__']);
      if (!isNaN(price) && price > 0.99 && price < 10000) {
        prices.push(price);
      }
    }

    if (prices.length === 0) { callback(null, 'No valid prices found'); return; }

    var sum = 0;
    for (var j = 0; j < prices.length; j++) sum += prices[j];
    callback({ count: prices.length, average: Math.round(sum/prices.length*100)/100 });
  } catch(e) {
    callback(null, 'Parse error: ' + e.message);
  }
});
```

});

req.on(‘error’, function(e) { callback(null, ’Request error: ’ + e.message); });
req.setTimeout(15000, function() { req.destroy(); callback(null, ‘Timeout’); });
req.end();
}

var server = http.createServer(function(req, res) {
res.setHeader(‘Access-Control-Allow-Origin’, ‘*’);
res.setHeader(‘Access-Control-Allow-Methods’, ‘GET, OPTIONS’);
res.setHeader(‘Access-Control-Allow-Headers’, ‘Content-Type’);
res.setHeader(‘Content-Type’, ‘application/json’);

if (req.method === ‘OPTIONS’) { res.writeHead(200); res.end(); return; }
if (req.method !== ‘GET’) { res.writeHead(405); res.end(JSON.stringify({error:‘Method not allowed’})); return; }

var url = new URL(req.url, ‘http://localhost’);

if (url.pathname === ‘/health’) { res.writeHead(200); res.end(JSON.stringify({status:‘ok’})); return; }
if (url.pathname !== ‘/price’) { res.writeHead(404); res.end(JSON.stringify({error:‘Not found’})); return; }

var title = url.searchParams.get(‘title’) || ‘’;
var author = url.searchParams.get(‘author’) || ‘’;
var isbn = (url.searchParams.get(‘isbn’) || ‘’).replace(/[^0-9X]/gi, ‘’);
var conditionId = url.searchParams.get(‘condition’) || ‘3000’;
var isSigned = url.searchParams.get(‘signed’) === ‘1’;
var conditionIds = CONDITION_FALLBACK[conditionId] || [‘3000’];

if (!title && !isbn) {
res.writeHead(400);
res.end(JSON.stringify({error: ‘Missing title or isbn’}));
return;
}

// Build keywords - ISBN is most specific, otherwise title + author
var keywords;
if (isbn) {
keywords = isbn;
} else {
keywords = title + (author ? ’ ’ + author : ‘’) + (isSigned ? ’ signed’ : ‘’);
}

console.log(‘Price request | Keywords:’, keywords, ‘| Conditions:’, conditionIds.join(’,’));

fetchEbayFindingAPI(keywords, conditionIds, function(result, error) {
if (error && isbn && title) {
// Fallback to title search if ISBN didn’t work
var fallback = title + (author ? ’ ’ + author : ‘’) + (isSigned ? ’ signed’ : ‘’);
console.log(‘Falling back to title search:’, fallback);
fetchEbayFindingAPI(fallback, conditionIds, function(r2, e2) {
if (e2) { res.writeHead(200); res.end(JSON.stringify({error:e2, average:null, count:0})); return; }
res.writeHead(200); res.end(JSON.stringify(r2));
});
return;
}
if (error) { res.writeHead(200); res.end(JSON.stringify({error:error, average:null, count:0})); return; }
res.writeHead(200); res.end(JSON.stringify(result));
});
});

server.listen(PORT, function() {
console.log(’Books for Ages price server on port ’ + PORT);
});
