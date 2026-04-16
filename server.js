<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Books for Ages — Admin</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

*{margin:0;padding:0;box-sizing:border-box;}
:root{
  --bg:#0a0c10;
  --card:#111318;
  --border:#1e2230;
  --gold:#ffc700;
  --gold2:#e6a800;
  --green:#00c97a;
  --red:#ff4466;
  --blue:#4499ff;
  --text:#e8eaf0;
  --muted:#5a6478;
  --font:'Syne',sans-serif;
  --mono:'JetBrains Mono',monospace;
}
body{background:var(--bg);color:var(--text);font-family:var(--font);min-height:100vh;}

/* LOGIN */
#login-screen{display:flex;align-items:center;justify-content:center;min-height:100vh;padding:40px 20px;position:relative;z-index:50;overflow-y:auto;}
.login-box{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:40px 32px 48px;width:100%;max-width:420px;text-align:center;position:relative;z-index:5;margin:auto;}
.login-logo{font-size:13px;font-weight:700;letter-spacing:0.2em;color:var(--gold);text-transform:uppercase;margin-bottom:8px;}
.login-title{font-size:32px;font-weight:800;margin-bottom:6px;}
.login-sub{font-size:13px;color:var(--muted);margin-bottom:32px;}
.inp{width:100%;background:#0d0f14;border:1px solid var(--border);border-radius:10px;padding:14px 16px;color:var(--text);font-family:var(--mono);font-size:15px;letter-spacing:0.1em;outline:none;transition:border 0.2s;}
.inp:focus{border-color:var(--gold);}
.btn-login{width:100%;background:var(--gold);color:#0a0c10;border:none;border-radius:10px;padding:16px;font-family:var(--font);font-size:15px;font-weight:700;cursor:pointer;margin-top:16px;margin-bottom:8px;transition:background 0.2s;position:relative;z-index:10;display:block;}
.btn-login:hover{background:var(--gold2);}
.err{color:var(--red);font-size:12px;margin-top:8px;}

/* DASHBOARD */
#dashboard{display:none;position:relative;z-index:1;}
.topbar{background:var(--card);border-bottom:1px solid var(--border);padding:0 24px;display:flex;align-items:center;justify-content:space-between;height:60px;position:relative;z-index:10;}
.topbar-logo{font-size:13px;font-weight:800;letter-spacing:0.15em;color:var(--gold);}
.topbar-right{display:flex;align-items:center;gap:12px;}
.btn-sm{background:rgba(255,199,0,0.1);color:var(--gold);border:1px solid rgba(255,199,0,0.2);border-radius:8px;padding:7px 14px;font-family:var(--font);font-size:12px;font-weight:700;cursor:pointer;letter-spacing:0.05em;}
.btn-sm:hover{background:rgba(255,199,0,0.2);}
.btn-sm.danger{background:rgba(255,68,102,0.1);color:var(--red);border-color:rgba(255,68,102,0.2);}
.btn-sm.danger:hover{background:rgba(255,68,102,0.2);}
.btn-sm.green{background:rgba(0,201,122,0.1);color:var(--green);border-color:rgba(0,201,122,0.2);}
.btn-sm.green:hover{background:rgba(0,201,122,0.2);}

.main{padding:24px;max-width:1200px;margin:0 auto;}

/* TABS */
.tabs{display:flex;gap:4px;margin-bottom:24px;background:var(--card);border:1px solid var(--border);border-radius:12px;padding:4px;}
.tab{flex:1;padding:10px;text-align:center;border-radius:9px;cursor:pointer;font-size:13px;font-weight:700;letter-spacing:0.05em;color:var(--muted);transition:all 0.2s;}
.tab.active{background:var(--gold);color:#0a0c10;}

/* CARDS */
.section-title{font-size:11px;font-weight:700;letter-spacing:0.15em;text-transform:uppercase;color:var(--muted);margin-bottom:12px;}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:12px;}
.card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;}
.card-title{font-size:16px;font-weight:700;}
.stat-row{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:24px;}
.stat{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;text-align:center;}
.stat-val{font-size:32px;font-weight:800;color:var(--gold);}
.stat-lbl{font-size:11px;color:var(--muted);letter-spacing:0.1em;text-transform:uppercase;margin-top:4px;}

/* SUBSCRIBER LIST */
.sub-row{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;}
.sub-info{flex:1;}
.sub-name{font-size:15px;font-weight:700;margin-bottom:2px;}
.sub-meta{font-size:12px;color:var(--muted);font-family:var(--mono);}
.sub-actions{display:flex;gap:8px;align-items:center;}
.badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;letter-spacing:0.05em;}
.badge.active{background:rgba(0,201,122,0.1);color:var(--green);border:1px solid rgba(0,201,122,0.2);}
.badge.inactive{background:rgba(255,68,102,0.1);color:var(--red);border:1px solid rgba(255,68,102,0.2);}

/* FORM */
.form-group{margin-bottom:14px;}
.lbl{display:block;font-size:11px;font-weight:700;letter-spacing:0.1em;color:var(--muted);text-transform:uppercase;margin-bottom:6px;}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
.btn-primary{background:var(--gold);color:#0a0c10;border:none;border-radius:10px;padding:13px 24px;font-family:var(--font);font-size:14px;font-weight:700;cursor:pointer;width:100%;margin-top:8px;}
.btn-primary:hover{background:var(--gold2);}

/* ACTIVITY TABLE */
.activity-table{width:100%;border-collapse:collapse;}
.activity-table th{font-size:10px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:var(--muted);padding:10px 12px;text-align:left;border-bottom:1px solid var(--border);}
.activity-table td{padding:12px;font-size:13px;border-bottom:1px solid rgba(255,255,255,0.03);}
.activity-table tr:hover td{background:rgba(255,255,255,0.02);}

/* EMPLOYEE EDITOR */
.emp-row{display:flex;align-items:center;gap:8px;margin-bottom:8px;}
.emp-row .inp{flex:1;}
.emp-row .inp.pin{width:90px;flex:none;font-family:var(--mono);}
.btn-icon{background:rgba(255,68,102,0.1);color:var(--red);border:1px solid rgba(255,68,102,0.2);border-radius:8px;padding:8px 12px;cursor:pointer;font-size:14px;}
.btn-add-emp{background:rgba(0,201,122,0.1);color:var(--green);border:1px solid rgba(0,201,122,0.2);border-radius:8px;padding:9px 16px;font-family:var(--font);font-size:12px;font-weight:700;cursor:pointer;margin-top:4px;}

/* MODAL */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.8);z-index:200;display:none;align-items:center;justify-content:center;padding:20px;pointer-events:none;}
.modal-overlay.show{display:flex;pointer-events:all;}
.modal{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:28px;width:100%;max-width:560px;max-height:90vh;overflow-y:auto;}
.modal-title{font-size:20px;font-weight:800;margin-bottom:20px;}
.modal-close{float:right;cursor:pointer;color:var(--muted);font-size:20px;line-height:1;}

/* NOTES */
.notes-inp{width:100%;background:#0d0f14;border:1px solid var(--border);border-radius:8px;padding:10px 12px;color:var(--text);font-family:var(--font);font-size:13px;outline:none;resize:vertical;min-height:60px;}
.notes-inp:focus{border-color:var(--gold);}

/* TOGGLE */
.toggle-wrap{display:flex;align-items:center;gap:10px;cursor:pointer;}
.toggle{width:42px;height:24px;border-radius:12px;background:#1e2230;position:relative;transition:background 0.2s;border:none;cursor:pointer;}
.toggle.on{background:var(--green);}
.toggle::after{content:'';position:absolute;top:3px;left:3px;width:18px;height:18px;border-radius:50%;background:#fff;transition:transform 0.2s;}
.toggle.on::after{transform:translateX(18px);}

.search-inp{background:#0d0f14;border:1px solid var(--border);border-radius:8px;padding:10px 14px;color:var(--text);font-family:var(--font);font-size:13px;outline:none;width:100%;margin-bottom:16px;}
.search-inp:focus{border-color:var(--gold);}
.empty{text-align:center;padding:40px;color:var(--muted);font-size:14px;}
.spin-sm{width:20px;height:20px;border:2px solid var(--border);border-top-color:var(--gold);border-radius:50%;animation:spin 0.8s linear infinite;display:inline-block;}
@keyframes spin{to{transform:rotate(360deg);}}

.date-inp{background:#0d0f14;border:1px solid var(--border);border-radius:8px;padding:9px 12px;color:var(--text);font-family:var(--mono);font-size:13px;outline:none;}
.date-inp:focus{border-color:var(--gold);}
</style>
</head>
<body>

<!-- LOGIN -->
<div id="login-screen">
  <div class="login-box">
    <div class="login-logo">Books for Ages</div>
    <div class="login-title">Admin Portal</div>
    <div class="login-sub">Enter your admin key to continue</div>
    <div style="position:relative;">
      <input class="inp" type="password" id="admin-key-inp" placeholder="Admin key" onkeydown="if(event.key==='Enter')doLogin()" style="padding-right:48px;"/>
      <button onclick="var i=document.getElementById('admin-key-inp');i.type=i.type==='password'?'text':'password';this.textContent=i.type==='password'?'👁':'🙈'" style="position:absolute;right:12px;top:50%;transform:translateY(-50%);background:none;border:none;cursor:pointer;font-size:18px;z-index:10;">👁</button>
    </div>
    <button class="btn-login" onclick="doLogin()" style="position:relative;z-index:999;pointer-events:all;">Sign In</button>
    <div class="err" id="login-err"></div>
  </div>
</div>

<!-- DASHBOARD -->
<div id="dashboard" style="display:none;">
  <div class="topbar">
    <div class="topbar-logo">📚 BFA ADMIN</div>
    <div class="topbar-right">
      <button class="btn-sm green" onclick="sendReports()">📧 Send Reports Now</button>
      <button class="btn-sm" onclick="doLogout()">Sign Out</button>
    </div>
  </div>

  <div class="main">
    <!-- STATS -->
    <div class="stat-row" id="stats-row">
      <div class="stat"><div class="stat-val" id="stat-subs">—</div><div class="stat-lbl">Active Subscribers</div></div>
      <div class="stat"><div class="stat-val" id="stat-today">—</div><div class="stat-lbl">Listings Today</div></div>
      <div class="stat"><div class="stat-val" id="stat-total">—</div><div class="stat-lbl">Total Listings</div></div>
    </div>

    <!-- TABS -->
    <div class="tabs">
      <div class="tab active" onclick="switchTab('subscribers')">Subscribers</div>
      <div class="tab" onclick="switchTab('activity')">Activity</div>
      <div class="tab" onclick="switchTab('add')">+ Add Subscriber</div>
    </div>

    <!-- SUBSCRIBERS TAB -->
    <div id="tab-subscribers">
      <div class="section-title">All Subscribers</div>
      <input class="search-inp" placeholder="Search by name or code..." oninput="filterSubs(this.value)"/>
      <div id="subs-list"><div class="empty"><span class="spin-sm"></span></div></div>
    </div>

    <!-- ACTIVITY TAB -->
    <div id="tab-activity" style="display:none;">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap;">
        <div>
          <div class="section-title" style="margin-bottom:4px;">Date</div>
          <input class="date-inp" type="date" id="activity-date" onchange="loadActivity()"/>
        </div>
        <div style="flex:1;">
          <div class="section-title" style="margin-bottom:4px;">Filter by Subscriber</div>
          <select class="date-inp" id="activity-sub-filter" onchange="loadActivity()" style="min-width:200px;">
            <option value="">All Subscribers</option>
          </select>
        </div>
      </div>
      <div class="card">
        <div id="activity-content"><div class="empty">Select a date to view activity</div></div>
      </div>
    </div>

    <!-- ADD SUBSCRIBER TAB -->
    <div id="tab-add" style="display:none;">
      <div class="card">
        <div class="card-title" style="margin-bottom:20px;">New Subscriber</div>
        <div class="form-row">
          <div class="form-group">
            <label class="lbl">Business Name</label>
            <input class="inp" id="new-biz-name" placeholder="e.g. Read & Sell Books"/>
          </div>
          <div class="form-group">
            <label class="lbl">Access Code (leave blank to auto-generate)</label>
            <input class="inp" id="new-code" placeholder="e.g. READER04" style="font-family:var(--mono);letter-spacing:0.1em;"/>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label class="lbl">Email (for reports)</label>
            <input class="inp" id="new-email" type="email" placeholder="owner@business.com"/>
          </div>
          <div class="form-group">
            <label class="lbl">Notes</label>
            <input class="inp" id="new-notes" placeholder="e.g. Paid January 2025"/>
          </div>
        </div>

        <div class="section-title" style="margin-top:8px;">eBay Credentials (optional — they can add later)</div>
        <div class="form-row">
          <div class="form-group">
            <label class="lbl">eBay Client ID</label>
            <input class="inp" id="new-ebay-id" placeholder="eBay App ID"/>
          </div>
          <div class="form-group">
            <label class="lbl">eBay Client Secret</label>
            <input class="inp" id="new-ebay-secret" placeholder="eBay Cert ID"/>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label class="lbl">eBay Dev ID</label>
            <input class="inp" id="new-ebay-dev" placeholder="eBay Dev ID"/>
          </div>
          <div class="form-group">
            <label class="lbl">eBay User Token</label>
            <input class="inp" id="new-ebay-token" placeholder="User token"/>
          </div>
        </div>

        <div class="section-title" style="margin-top:8px;">Employees</div>
        <div id="new-employees"></div>
        <button class="btn-add-emp" onclick="addEmpRow('new-employees')">+ Add Employee</button>

        <button class="btn-primary" onclick="createSubscriber()" style="margin-top:20px;">Create Subscriber</button>
        <div class="err" id="add-err" style="margin-top:8px;"></div>
        <div style="color:var(--green);font-size:13px;margin-top:8px;display:none;" id="add-success">✅ Subscriber created successfully!</div>
      </div>
    </div>
  </div>
</div>

<!-- EDIT MODAL -->
<div class="modal-overlay" id="edit-modal">
  <div class="modal">
    <div class="modal-title">Edit Subscriber <span class="modal-close" onclick="closeModal()">×</span></div>
    <input type="hidden" id="edit-code"/>
    <div class="form-group">
      <label class="lbl">Business Name</label>
      <input class="inp" id="edit-biz-name"/>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label class="lbl">Email</label>
        <input class="inp" id="edit-email" type="email"/>
      </div>
      <div class="form-group">
        <label class="lbl">Notes</label>
        <input class="inp" id="edit-notes"/>
      </div>
    </div>
    <div class="section-title" style="margin-top:4px;">eBay Credentials</div>
    <div class="form-row">
      <div class="form-group">
        <label class="lbl">eBay Client ID</label>
        <input class="inp" id="edit-ebay-id"/>
      </div>
      <div class="form-group">
        <label class="lbl">eBay Client Secret</label>
        <input class="inp" id="edit-ebay-secret"/>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label class="lbl">eBay Dev ID</label>
        <input class="inp" id="edit-ebay-dev"/>
      </div>
      <div class="form-group">
        <label class="lbl">eBay User Token</label>
        <input class="inp" id="edit-ebay-token"/>
      </div>
    </div>
    <div class="section-title" style="margin-top:4px;">Employees</div>
    <div id="edit-employees"></div>
    <button class="btn-add-emp" onclick="addEmpRow('edit-employees')">+ Add Employee</button>
    <button class="btn-primary" onclick="saveEdit()" style="margin-top:16px;">Save Changes</button>
    <div class="err" id="edit-err" style="margin-top:8px;"></div>
  </div>
</div>

<script>
var SERVER = 'https://bfa-price-server.onrender.com';
var ADMIN_KEY = '';
var allSubs = [];

function doLogin(){
  var key = document.getElementById('admin-key-inp').value.trim();
  if(!key){ document.getElementById('login-err').textContent = 'Please enter your admin key.'; return; }
  // Verify by hitting the server
  fetch(SERVER + '/admin/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key: key })
  })
    .then(function(r){ return r.json(); })
    .then(function(data){
      if(!data.success){ document.getElementById('login-err').textContent = 'Invalid admin key.'; return; }
      ADMIN_KEY = key;
      document.getElementById('login-screen').style.display = 'none';
      document.getElementById('dashboard').style.display = 'block';
      loadAll();
    })
    .catch(function(){ document.getElementById('login-err').textContent = 'Could not connect to server.'; });
}

function doLogout(){
  ADMIN_KEY = '';
  document.getElementById('login-screen').style.display = 'flex';
  document.getElementById('dashboard').style.display = 'none';
  document.getElementById('admin-key-inp').value = '';
}

function loadAll(){
  loadSubscribers();
  loadStats();
  // Set today's date on activity tab
  var today = new Date().toISOString().split('T')[0];
  document.getElementById('activity-date').value = today;
}

function loadSubscribers(){
  fetch(SERVER + '/admin/subscribers', { headers: { 'x-admin-key': ADMIN_KEY } })
    .then(function(r){ return r.json(); })
    .then(function(subs){
      allSubs = subs;
      renderSubs(subs);
      // Populate filter dropdown
      var sel = document.getElementById('activity-sub-filter');
      sel.innerHTML = '<option value="">All Subscribers</option>';
      subs.forEach(function(s){
        var opt = document.createElement('option');
        opt.value = s.code;
        opt.textContent = s.businessName + ' (' + s.code + ')';
        sel.appendChild(opt);
      });
      // Update stats
      var active = subs.filter(function(s){ return s.active; }).length;
      document.getElementById('stat-subs').textContent = active;
    })
    .catch(function(){ document.getElementById('subs-list').innerHTML = '<div class="empty">Could not load subscribers.</div>'; });
}

function loadStats(){
  var today = new Date().toISOString().split('T')[0];
  fetch(SERVER + '/admin/listings?date=' + today, { headers: { 'x-admin-key': ADMIN_KEY } })
    .then(function(r){ return r.json(); })
    .then(function(listings){
      document.getElementById('stat-today').textContent = listings.length;
      document.getElementById('stat-total').textContent = listings.length + '+';
    })
    .catch(function(){});
}

function renderSubs(subs){
  var html = '';
  if(!subs.length){ document.getElementById('subs-list').innerHTML = '<div class="empty">No subscribers yet. Add one using the + tab.</div>'; return; }
  subs.forEach(function(s){
    html += '<div class="sub-row">'
      + '<div class="sub-info">'
      + '<div class="sub-name">' + esc(s.businessName) + '</div>'
      + '<div class="sub-meta">Code: ' + s.code + ' &nbsp;|&nbsp; ' + (s.email || 'No email') + ' &nbsp;|&nbsp; ' + (s.employees||[]).length + ' employees'
      + (s.notes ? ' &nbsp;|&nbsp; 📝 ' + esc(s.notes) : '') + '</div>'
      + '</div>'
      + '<div class="sub-actions">'
      + '<span class="badge ' + (s.active ? 'active' : 'inactive') + '">' + (s.active ? 'ACTIVE' : 'INACTIVE') + '</span>'
      + '<button class="btn-sm" onclick="openEdit(\'' + s.code + '\')">Edit</button>'
      + '<button class="toggle ' + (s.active ? 'on' : '') + '" onclick="toggleSub(\'' + s.code + '\', this)" title="' + (s.active ? 'Deactivate' : 'Activate') + '"></button>'
      + '</div>'
      + '</div>';
  });
  document.getElementById('subs-list').innerHTML = html;
}

function filterSubs(query){
  var q = query.toLowerCase();
  var filtered = allSubs.filter(function(s){
    return s.businessName.toLowerCase().includes(q) || s.code.toLowerCase().includes(q);
  });
  renderSubs(filtered);
}

function toggleSub(code, btn){
  fetch(SERVER + '/admin/subscribers/' + code + '/toggle', { method: 'POST', headers: { 'x-admin-key': ADMIN_KEY } })
    .then(function(r){ return r.json(); })
    .then(function(data){
      btn.classList.toggle('on', data.active);
      // Update badge
      var row = btn.closest('.sub-row');
      var badge = row.querySelector('.badge');
      badge.className = 'badge ' + (data.active ? 'active' : 'inactive');
      badge.textContent = data.active ? 'ACTIVE' : 'INACTIVE';
      // Update in allSubs
      var sub = allSubs.find(function(s){ return s.code === code; });
      if(sub) sub.active = data.active;
      loadStats();
    });
}

function openEdit(code){
  var sub = allSubs.find(function(s){ return s.code === code; });
  if(!sub) return;
  document.getElementById('edit-code').value = code;
  document.getElementById('edit-biz-name').value = sub.businessName || '';
  document.getElementById('edit-email').value = sub.email || '';
  document.getElementById('edit-notes').value = sub.notes || '';
  document.getElementById('edit-ebay-id').value = sub.ebayClientId || '';
  document.getElementById('edit-ebay-secret').value = sub.ebayClientSecret || '';
  document.getElementById('edit-ebay-dev').value = sub.ebayDevId || '';
  document.getElementById('edit-ebay-token').value = sub.ebayUserToken || '';
  // Employees
  var empContainer = document.getElementById('edit-employees');
  empContainer.innerHTML = '';
  (sub.employees || []).forEach(function(e){ addEmpRow('edit-employees', e.name, e.pin); });
  document.getElementById('edit-modal').classList.add('show');
}

function closeModal(){
  document.getElementById('edit-modal').classList.remove('show');
}

function saveEdit(){
  var code = document.getElementById('edit-code').value;
  var employees = getEmployees('edit-employees');
  var data = {
    businessName: document.getElementById('edit-biz-name').value,
    email: document.getElementById('edit-email').value,
    notes: document.getElementById('edit-notes').value,
    ebayClientId: document.getElementById('edit-ebay-id').value,
    ebayClientSecret: document.getElementById('edit-ebay-secret').value,
    ebayDevId: document.getElementById('edit-ebay-dev').value,
    ebayUserToken: document.getElementById('edit-ebay-token').value,
    employees: employees
  };
  fetch(SERVER + '/admin/subscribers/' + code, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', 'x-admin-key': ADMIN_KEY },
    body: JSON.stringify(data)
  }).then(function(r){ return r.json(); })
    .then(function(){
      closeModal();
      loadSubscribers();
    })
    .catch(function(){ document.getElementById('edit-err').textContent = 'Save failed.'; });
}

function createSubscriber(){
  var employees = getEmployees('new-employees');
  var data = {
    code: document.getElementById('new-code').value.trim(),
    businessName: document.getElementById('new-biz-name').value.trim(),
    email: document.getElementById('new-email').value.trim(),
    notes: document.getElementById('new-notes').value.trim(),
    ebayClientId: document.getElementById('new-ebay-id').value.trim(),
    ebayClientSecret: document.getElementById('new-ebay-secret').value.trim(),
    ebayDevId: document.getElementById('new-ebay-dev').value.trim(),
    ebayUserToken: document.getElementById('new-ebay-token').value.trim(),
    employees: employees
  };
  if(!data.businessName){ document.getElementById('add-err').textContent = 'Business name is required.'; return; }
  document.getElementById('add-err').textContent = '';
  fetch(SERVER + '/admin/subscribers', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-key': ADMIN_KEY },
    body: JSON.stringify(data)
  }).then(function(r){ return r.json(); })
    .then(function(sub){
      if(sub.error){ document.getElementById('add-err').textContent = sub.error; return; }
      document.getElementById('add-success').style.display = 'block';
      document.getElementById('add-success').textContent = '✅ Subscriber created! Access code: ' + sub.code;
      setTimeout(function(){
        document.getElementById('add-success').style.display = 'none';
        // Clear form
        ['new-biz-name','new-code','new-email','new-notes','new-ebay-id','new-ebay-secret','new-ebay-dev','new-ebay-token'].forEach(function(id){ document.getElementById(id).value = ''; });
        document.getElementById('new-employees').innerHTML = '';
        loadSubscribers();
        switchTab('subscribers');
      }, 3000);
    })
    .catch(function(){ document.getElementById('add-err').textContent = 'Failed to create subscriber.'; });
}

function loadActivity(){
  var date = document.getElementById('activity-date').value;
  var code = document.getElementById('activity-sub-filter').value;
  if(!date) return;
  document.getElementById('activity-content').innerHTML = '<div class="empty"><span class="spin-sm"></span></div>';
  var url = SERVER + '/admin/listings?date=' + date + (code ? '&code=' + code : '');
  fetch(url, { headers: { 'x-admin-key': ADMIN_KEY } })
    .then(function(r){ return r.json(); })
    .then(function(listings){
      if(!listings.length){ document.getElementById('activity-content').innerHTML = '<div class="empty">No listings found for this date.</div>'; return; }
      var html = '<table class="activity-table">'
        + '<tr><th>Time</th><th>Employee</th><th>Business</th><th>Book Title</th><th>Condition</th><th>Price</th><th>eBay ID</th></tr>';
      listings.forEach(function(l){
        html += '<tr>'
          + '<td style="font-family:var(--mono);color:var(--muted)">' + (l.time||'') + '</td>'
          + '<td><strong>' + esc(l.employee||'') + '</strong></td>'
          + '<td style="color:var(--muted);font-size:12px">' + esc(l.subscriberCode||'') + '</td>'
          + '<td>' + esc(l.bookTitle||'') + '</td>'
          + '<td><span class="badge active" style="font-size:10px">' + esc(l.condition||'') + '</span></td>'
          + '<td style="color:var(--gold);font-weight:700">$' + (l.price||0) + '</td>'
          + '<td style="font-family:var(--mono);font-size:11px;color:var(--muted)">' + (l.ebayListingId ? '<a href="https://www.ebay.com/itm/'+l.ebayListingId+'" target="_blank" style="color:var(--blue)">'+l.ebayListingId+'</a>' : '—') + '</td>'
          + '</tr>';
      });
      html += '</table>';
      html += '<div style="font-size:12px;color:var(--muted);margin-top:12px;text-align:right;">Total: <strong style="color:var(--text)">' + listings.length + ' listings</strong></div>';
      document.getElementById('activity-content').innerHTML = html;
    })
    .catch(function(){ document.getElementById('activity-content').innerHTML = '<div class="empty">Failed to load activity.</div>'; });
}

function sendReports(){
  fetch(SERVER + '/admin/send-report', { method: 'POST', headers: { 'x-admin-key': ADMIN_KEY } })
    .then(function(){ alert('Reports are being sent to all subscribers!'); })
    .catch(function(){ alert('Failed to send reports.'); });
}

function switchTab(tab){
  ['subscribers','activity','add'].forEach(function(t){
    document.getElementById('tab-' + t).style.display = t === tab ? 'block' : 'none';
  });
  document.querySelectorAll('.tab').forEach(function(el, i){
    el.classList.toggle('active', ['subscribers','activity','add'][i] === tab);
  });
  if(tab === 'activity') loadActivity();
}

function addEmpRow(containerId, name, pin){
  var container = document.getElementById(containerId);
  var row = document.createElement('div');
  row.className = 'emp-row';
  row.innerHTML = '<input class="inp" placeholder="Employee name" value="' + esc(name||'') + '"/>'
    + '<input class="inp pin" placeholder="4-digit PIN" maxlength="4" value="' + esc(pin||'') + '" style="font-family:var(--mono);letter-spacing:0.2em;"/>'
    + '<button class="btn-icon" onclick="this.parentElement.remove()">×</button>';
  container.appendChild(row);
}

function getEmployees(containerId){
  var rows = document.getElementById(containerId).querySelectorAll('.emp-row');
  var emps = [];
  rows.forEach(function(row){
    var inputs = row.querySelectorAll('input');
    var name = inputs[0].value.trim();
    var pin = inputs[1].value.trim();
    if(name && pin) emps.push({ name: name, pin: pin });
  });
  return emps;
}

function esc(s){ return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// Close modal on overlay click
document.getElementById('edit-modal').addEventListener('click', function(e){ if(e.target === this) closeModal(); });
</script>
</body>
</html>
