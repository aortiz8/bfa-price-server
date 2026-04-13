const https = require(‘https’);
const http = require(‘http’);

const PORT = process.env.PORT || 3000;

var CONDITION_FALLBACK = {
‘1000’: [‘1000’,‘1500’,‘2500’],
‘1500’: [‘1500’,‘2500’],
‘2500’: [‘2500’,‘3000’],
‘3000’: [‘3000’],
‘7000’: [‘7000’]
};

// Words to ignore when matching titles
var STOP_WORDS = [‘the’,‘a’,‘an’,‘of’,‘by’,‘and’,‘or’,‘in’,‘on’,‘at’,‘to’,‘for’,‘with’,‘from’,‘is’,‘it’];

function getKeyWords(str) {
if (!str) return [];
return str.toLowerCase()
.replace(/[^a-z0-9 ]/g, ‘’)
.split(’ ’)
.filter(function(w) {
return w.length > 1 && STOP_WORDS.indexOf(w) === -1;
});
}

function titlesMatch(ebayTitle, searchTitle) {
if (!searchTitle) return true;
var searchWords = getKeyWords(searchTitle);
if (searchWords.length === 0) return true;
var ebay = ebayTitle.toLowerCase();
var matchCount = 0;
for (var i = 0; i < searchWords.length; i++) {
if (ebay.indexOf(searchWords[i]) > -1) matchCount++;
}
// At least 60% of key words must match
return matchCount >= Math.ceil(searchWords.length * 0.6);
}

function extractPrices(html, title) {
// Extract price+title pairs from eBay results
var prices = [];

// Try to match prices with their listing titles
var itemRegex = /s-item__title[^>]*>([^<]+)<[\s\S]*?s-item__price[^>]*>[\s\S]*?$([0-9,]+.?[0-9]*)/g;
var match;
while ((match = itemRegex.exec(html)) !== null && prices.length < 15) {
var itemTitle = match[1].trim();
var price = parseFloat(match[2].replace(’,’, ‘’));
if (isNaN(price) || price < 0.99 || price > 10000) continue;
if (!title || titlesMatch(itemTitle, title)) {
prices.push(price);
}
}

// If that didn’t work, fall back to just extracting prices
if (prices.length === 0) {
var priceRegex = /$([0-9]+.[0-9]{2})/g;
var all = [];
while ((match = priceRegex.exec(html)) !== null) {
var p = parseFloat(match[1]);
if (!isNaN(p) && p > 0.99 && p < 10000) all.push(p);
}
if (all.length > 0) {
all.sort(function(a,b){return a-b;});
var s = Math.floor(all.length * 0.1);
var e = Math.floor(all.length * 0.9);
for (var i=s; i<e && prices.length<10; i++) prices.push(all[i]);
}
}

return prices;
}

function fetchEbay(searchQuery, conditionIds, title, callback) {
var condParam = conditionIds.map(function(c){ return ‘LH_ItemCondition=’+c; }).join(’&’);
var path = ‘/sch/i.html?_nkw=’ + encodeURIComponent(searchQuery)
+ ‘&LH_Sold=1&LH_Complete=1&’ + condParam + ‘&_sacat=267’;

console.log(‘eBay search:’, searchQuery, ‘| Conditions:’, conditionIds.join(’,’));

var options = {
hostname: ‘www.ebay.com’,
path: path,
method: ‘GET’,
headers: {
‘User-Agent’: ‘Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1’,
‘Accept’: ‘text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8’,
‘Accept-Language’: ‘en-US,en;q=0.9’,
‘Accept-Encoding’: ‘identity’
}
};

var req = https.request(options, function(res) {
var data = ‘’;
res.on(‘data’, function(chunk) { data += chunk; });
res.on(‘end’, function() {
try {
var prices = extractPrices(data, title);
if (prices.length === 0) { callback(null, ‘No matching prices found’); return; }
var sum = 0;
for (var j = 0; j < prices.length; j++) sum += prices[j];
callback({ count: prices.length, average: Math.round(sum/prices.length*100)/100 });
} catch(e) { callback(null, ’Parse error: ’ + e.message); }
});
});

req.on(‘error’, function(e) { callback(null, ’Request error: ’ + e.message); });
req.setTimeout(12000, function() { req.destroy(); callback(null, ‘Timeout’); });
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
res.end(JSON.stringify({error:‘Missing title or isbn’}));
return;
}

console.log(‘Request | Title:’, title, ‘| Author:’, author, ‘| ISBN:’, isbn, ‘| Condition:’, conditionId);

// Build search: ISBN is most exact, otherwise title + author
var primaryQuery = isbn
? isbn
: (title + (author ? ’ ’ + author : ‘’) + (isSigned ? ’ signed’ : ‘’));

fetchEbay(primaryQuery, conditionIds, title, function(result, error) {
if (error && isbn && title) {
// ISBN search failed - fall back to title + author
var fallback = title + (author ? ’ ’ + author : ‘’) + (isSigned ? ’ signed’ : ‘’);
console.log(‘Falling back to title search:’, fallback);
fetchEbay(fallback, conditionIds, title, function(r2, e2) {
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
