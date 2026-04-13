function openEbay(){
  var desc = el('final-desc').value || '';
  if(navigator.clipboard){
    navigator.clipboard.writeText(desc).then(function(){ toast('Description copied!'); });
  } else {
    var ta=document.createElement('textarea'); ta.value=desc;
    document.body.appendChild(ta); ta.select();
    document.execCommand('copy'); document.body.removeChild(ta);
    toast('Description copied!');
  }

  var condMap = {0:1000, 1:1500, 2:2500, 3:3000, 4:7000};
  var ebayCondId = condMap[book.gradeIdx] || 3000;
  var price = parseFloat(el('final-price').value) || 9.99;

  el('btn-ebay-list').textContent = 'Uploading photo...';
  el('btn-ebay-list').disabled = true;

  function doList(pictureUrl) {
    el('btn-ebay-list').textContent = 'Creating listing...';
    fetch('https://bfa-price-server.onrender.com/list', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: el('final-title').value,
        description: el('final-desc').value,
        price: price,
        isbn: book.isbn,
        conditionId: ebayCondId,
        pictureUrl: pictureUrl
      })
    })
    .then(function(r){ return r.json(); })
    .then(function(data){
      el('btn-ebay-list').textContent = 'List on eBay';
      el('btn-ebay-list').disabled = false;
      if(data.listingId){
        toast('Draft listing created!');
        var a=document.createElement('a');
        a.href='https://www.ebay.com/sh/lst/active';
        a.target='_blank'; a.rel='noopener';
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
      } else {
        toast('Error: '+(data.error||'Unknown error'));
      }
    })
    .catch(function(){
      el('btn-ebay-list').textContent = 'List on eBay';
      el('btn-ebay-list').disabled = false;
      toast('Error connecting to server');
    });
  }

  // Upload cover photo first if we have one
  if(coverImg) {
    compressImg(coverImg, function(compressed) {
      fetch('https://bfa-price-server.onrender.com/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image: compressed })
      })
      .then(function(r){ return r.json(); })
      .then(function(data){
        doList(data.pictureUrl || '');
      })
      .catch(function(){ doList(''); });
    });
  } else {
    doList('');
  }
}
