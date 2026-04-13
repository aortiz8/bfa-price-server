const https = require('https');
const http = require('http');

const PORT = process.env.PORT || 3000;

function fetchEbayPrices(query, callback) {
  const searchUrl = 'https://www.ebay.com/sch/i.html?_nkw=' + encodeURIComponent(query) + '&LH_Sold=1&LH_Complete=1&LH_ItemCondition=3000&_sacat=267';
  
  const options = {
    hostname: 'www.ebay.com',
    path: '/sch/i.html?_nkw=' + encodeURIComponent(query) + '&LH_Sold=1&LH_Complete=1&LH_ItemCondition=3000&_sacat=267',
    method: 'GET',
    headers: {
      'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity'
    }
  };

  const req = https.request(options, function(res) {
    let data = '';
    res.on('data', function(chunk) { data += chunk; });
    res.on('end', function() {
      try {
        const prices = [];
        // Match sold prices in eBay HTML
        const priceRegex = /s-item__price[^>]*>[\s\S]*?\$([0-9,]+\.?[0-9]*)/g;
        let match;
        while ((match = priceRegex.exec(data)) !== null && prices.length < 10) {
          const price = parseFloat(match[1].replace(',', ''));
          if (!isNaN(price) && price > 0 && price < 10000) {
            prices.push(price);
          }
        }
        
        if (prices.length === 0) {
          // Try alternate price pattern
          const altRegex = /\$([0-9]+\.[0-9]{2})/g;
          const allPrices = [];
          while ((match = altRegex.exec(data)) !== null) {
            const price = parseFloat(match[1]);
            if (!isNaN(price) && price > 0.99 && price < 10000) {
              allPrices.push(price);
            }
          }
          // Take median range to avoid outliers
          if (allPrices.length > 0) {
            allPrices.sort(function(a, b) { return a - b; });
            const start = Math.floor(allPrices.length * 0.1);
            const end = Math.floor(allPrices.length * 0.9);
            for (let i = start; i < end && prices.length < 10; i++) {
              prices.push(allPrices[i]);
            }
          }
        }

        if (prices.length === 0) {
          callback(null, 'No sold prices found');
          return;
        }

        const avg = prices.reduce(function(a, b) { return a + b; }, 0) / prices.length;
        callback({
          count: prices.length,
          average: Math.round(avg * 100) / 100,
          prices: prices
        });
      } catch (e) {
        callback(null, 'Parse error: ' + e.message);
      }
    });
  });

  req.on('error', function(e) {
    callback(null, 'Request error: ' + e.message);
  });

  req.setTimeout(10000, function() {
    req.destroy();
    callback(null, 'Timeout');
  });

  req.end();
}

const server = http.createServer(function(req, res) {
  // CORS headers - allow our Netlify app
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  if (req.method !== 'GET') {
    res.writeHead(405);
    res.end(JSON.stringify({ error: 'Method not allowed' }));
    return;
  }

  const url = new URL(req.url, 'http://localhost');
  
  if (url.pathname === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  if (url.pathname !== '/price') {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  const query = url.searchParams.get('q');
  if (!query) {
    res.writeHead(400);
    res.end(JSON.stringify({ error: 'Missing query parameter q' }));
    return;
  }

  console.log('Price search:', query);

  fetchEbayPrices(query, function(result, error) {
    if (error) {
      res.writeHead(200);
      res.end(JSON.stringify({ error: error, average: null, count: 0 }));
      return;
    }
    res.writeHead(200);
    res.end(JSON.stringify(result));
  });
});

server.listen(PORT, function() {
  console.log('Books for Ages price server running on port ' + PORT);
});
