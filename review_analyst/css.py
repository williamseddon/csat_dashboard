"""App-wide CSS for StarWalk Review Analyst. Extracted from app.py for maintainability."""
from __future__ import annotations

APP_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
:root {
  --navy:#1e293b; --navy-mid:#334155; --navy-soft:#475569;
  --slate-600:#64748b; --slate-500:#94a3b8; --slate-400:#cbd5e1;
  --slate-200:#e2e8f0; --slate-100:#f1f5f9; --slate-50:#f8fafc; --white:#ffffff;
  --accent:#6366f1; --accent-bg:rgba(99,102,241,.06);
  --success:#10b981; --danger:#ef4444; --warning:#f59e0b; --info:#3b82f6;
  --page-bg:#f8fafc; --surface:#ffffff; --border:#e5e7eb; --border-strong:#d1d5db;
  --shadow-xs:0 1px 2px rgba(0,0,0,.04);
  --shadow-sm:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
  --shadow-md:0 4px 12px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.04);
  --shadow-lg:0 8px 24px rgba(0,0,0,.09),0 4px 8px rgba(0,0,0,.04);
  --radius-sm:10px; --radius-md:14px; --radius-lg:18px; --radius-xl:22px;
}
/* ── Force light mode on every Streamlit surface ── */
html,body,[data-testid="stAppViewContainer"],.stApp{font-family:'Inter',system-ui,-apple-system,sans-serif;color:#1e293b!important;background:#f8fafc!important;}
.main,.block-container,.stMainBlockContainer,[data-testid="stAppViewBlockContainer"]{background:#f8fafc!important;color:#1e293b!important;}
[data-testid="stHeader"]{background:#ffffff!important;}
[data-testid="stSidebar"],[data-testid="stSidebar"]>div{background:#ffffff!important;color:#1e293b!important;}
[data-testid="stSidebar"] p,[data-testid="stSidebar"] span,[data-testid="stSidebar"] label{color:#1e293b!important;}
[data-testid="stMarkdownContainer"] p,[data-testid="stMarkdownContainer"] li,[data-testid="stMarkdownContainer"] span{color:#1e293b;}
[data-testid="stExpander"]{background:#ffffff!important;color:#1e293b!important;}
[data-testid="stMetric"]{background:#ffffff!important;color:#1e293b!important;}
[data-testid="stMetric"] [data-testid="stMetricValue"]{color:#1e293b!important;}
[data-testid="stMetric"] [data-testid="stMetricLabel"]{color:#64748b!important;}
.block-container{
  padding-top:2rem!important;
  padding-bottom:3rem!important;
  max-width:1500px!important;
}
.hero-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xl);padding:18px 22px;box-shadow:var(--shadow-sm);margin-bottom:.9rem;}
.metric-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:16px 18px 14px;box-shadow:var(--shadow-xs);min-height:108px;display:flex;flex-direction:column;gap:4px;transition:box-shadow .15s,border-color .15s;}
.metric-card:hover{box-shadow:var(--shadow-md);border-color:rgba(99,102,241,.30);}
.metric-card.accent{border-color:rgba(99,102,241,.25);background:linear-gradient(145deg,#f5f3ff 0%,var(--surface) 100%);}
.hero-kicker{font-size:10.5px;text-transform:uppercase;letter-spacing:.11em;color:var(--accent);font-weight:700;margin-bottom:3px;}
.hero-title{font-size:22px;font-weight:800;letter-spacing:-.028em;color:var(--navy);line-height:1.15;}
.metric-label{font-size:10.5px;text-transform:uppercase;letter-spacing:.09em;color:var(--slate-600);font-weight:600;}
.metric-value{font-size:clamp(1.6rem,2.1vw,2.1rem);font-weight:800;color:var(--navy);line-height:1;letter-spacing:-.04em;}
.metric-sub{color:var(--slate-600);font-size:12px;line-height:1.35;margin-top:2px;}
.section-title{font-size:18px;font-weight:800;margin:6px 0 8px;color:var(--navy);letter-spacing:-.025em;}
.section-sub{color:var(--slate-600);font-size:13px;margin:0 0 12px;line-height:1.5;}
.badge-row,.chip-wrap{display:flex;gap:6px;flex-wrap:wrap;align-items:center;}
.chip{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:999px;font-size:11.5px;font-weight:600;line-height:1;border:1.5px solid transparent;letter-spacing:-.01em;}
.chip.blue{background:#eff6ff;border-color:#bfdbfe;color:#2563eb;}
.chip.green{background:#ecfdf5;border-color:#6ee7b7;color:#059669;}
.chip.red{background:#fef2f2;border-color:#fca5a5;color:#dc2626;}
.chip.yellow{background:#fffbeb;border-color:#fcd34d;color:#b45309;}
.chip.indigo{background:#eef2ff;border-color:#c7d2fe;color:#4f46e5;}
.chip.gray{background:#f9fafb;border-color:var(--border);color:var(--slate-600);}
.hero-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-top:12px;}
.hero-stat{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-md);padding:13px 15px;box-shadow:var(--shadow-xs);}
.hero-stat.accent{border-color:rgba(99,102,241,.30);background:linear-gradient(145deg,#f5f3ff,var(--surface));}
.hero-stat .label{color:var(--slate-600);font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;font-weight:600;}
.hero-stat .value{font-size:24px;font-weight:800;margin-top:4px;color:var(--navy);letter-spacing:-.035em;}
.stButton>button{border-radius:var(--radius-sm)!important;font-weight:600!important;font-size:13.5px!important;min-height:38px!important;height:auto!important;border:1.5px solid var(--border-strong)!important;background:var(--surface)!important;color:var(--navy-soft)!important;box-shadow:var(--shadow-xs)!important;transition:all .14s ease!important;letter-spacing:-.01em!important;white-space:normal!important;line-height:1.2!important;padding:8px 12px!important;}
.stButton>button:hover{border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(99,102,241,.10)!important;color:var(--accent)!important;}
[data-testid="baseButton-primary"],[data-testid="baseButton-primary"]:hover{background:var(--accent)!important;color:#ffffff!important;border-color:var(--accent)!important;}
[data-testid="baseButton-primary"]:hover{background:#4f46e5!important;border-color:#4f46e5!important;box-shadow:0 0 0 3px rgba(99,102,241,.18)!important;}
[data-testid="stTextInput"] input,[data-testid="stTextArea"] textarea,[data-testid="stNumberInput"] input{border-radius:var(--radius-sm)!important;border-color:var(--border-strong)!important;background:var(--surface)!important;font-family:'Inter',sans-serif!important;font-size:13.5px!important;}
[data-testid="stTextInput"] input:focus,[data-testid="stTextArea"] textarea:focus{border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(99,102,241,.10)!important;}
[data-testid="stSelectbox"]>div>div,[data-testid="stMultiselect"]>div>div{border-radius:var(--radius-sm)!important;border-color:var(--border-strong)!important;background:var(--surface)!important;}
[data-testid="stContainer"][data-border="true"]{border-radius:var(--radius-lg)!important;border-color:var(--border)!important;background:var(--surface)!important;}
[data-testid="stExpander"]{border-radius:var(--radius-md)!important;border-color:var(--border)!important;background:var(--surface)!important;}
[data-testid="stProgressBar"]>div>div{background:var(--accent)!important;border-radius:999px!important;}
[data-testid="stMetric"]{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-md);padding:14px 16px;box-shadow:var(--shadow-xs);}
[data-testid="stDataFrame"]{border-radius:var(--radius-md);overflow:hidden;border:1px solid var(--border);}
[data-testid="stSidebar"]{background:#ffffff!important;border-right:1px solid var(--border)!important;}
[data-testid="stSidebar"] .stButton>button{width:100%;}
.ws-status-bar{display:flex;align-items:center;justify-content:space-between;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:10px 16px;margin-bottom:.5rem;box-shadow:var(--shadow-xs);font-size:13px;gap:12px;flex-wrap:wrap;}
.ws-status-dot{width:8px;height:8px;border-radius:50%;background:var(--success);display:inline-block;margin-right:6px;box-shadow:0 0 0 3px rgba(5,150,105,.18);}
.ws-filter-pill{background:var(--slate-100);border:1px solid var(--border);border-radius:999px;padding:3px 10px;font-size:11.5px;font-weight:600;color:var(--slate-600);}
.review-body{font-size:13.5px;line-height:1.6;color:var(--navy);margin:6px 0 4px;white-space:pre-wrap;word-break:break-word;}
.ev-highlight{background:#fef08a;border-radius:3px;padding:0 .15em;cursor:help;position:relative;}
.ev-highlight.ev-det{background:rgba(239,68,68,.12);}
.ev-highlight.ev-del{background:rgba(16,185,129,.12);}
.ev-highlight::after{content:attr(data-tag);position:absolute;left:50%;top:calc(100% + 6px);transform:translateX(-50%);width:min(260px,60vw);background:#1e293b;color:#f8fafc;border-radius:var(--radius-md);padding:.5rem .65rem;font-size:.72rem;line-height:1.35;box-shadow:var(--shadow-lg);white-space:normal;z-index:1000;pointer-events:none;opacity:0;transition:opacity .12s ease;}
.ev-highlight:hover::after{opacity:1;}
.sw-table-wrap{overflow-y:auto;overflow-x:hidden;border-radius:var(--radius-md);border:1px solid var(--border);}
.sw-table{width:100%;border-collapse:collapse;font-size:12.5px;font-family:'Inter',sans-serif;}
.sw-table thead tr{background:var(--slate-50);border-bottom:2px solid var(--border);}
.sw-table thead th{padding:8px 12px;text-align:left;font-size:10.5px;text-transform:uppercase;letter-spacing:.07em;color:var(--slate-500);font-weight:700;white-space:nowrap;}
.sw-table tbody tr{border-bottom:1px solid var(--border);}
.sw-table tbody tr:last-child{border-bottom:none;}
.sw-table tbody tr:hover{background:var(--slate-50);}
.sw-table tbody td{padding:7px 12px;color:var(--navy);max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.sw-td-right{text-align:right!important;font-variant-numeric:tabular-nums;}
.sw-star-good{color:var(--success);font-weight:700;}
.sw-star-bad{color:var(--danger);font-weight:700;}
.sw-divider{border:none;border-top:1px solid var(--border);margin:1.4rem 0 1rem;}
.compact-pager-status{display:flex;flex-direction:column;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:var(--navy);height:38px;letter-spacing:-.01em;}
.compact-pager-sub{font-size:11px;font-weight:400;color:var(--slate-400);margin-top:1px;}
.sym-state-banner{background:var(--surface);border:1px dashed var(--border-strong);border-radius:var(--radius-xl);padding:2rem;text-align:center;margin:1rem 0;}
.sym-state-banner .icon{font-size:2.4rem;margin-bottom:.6rem;}
.sym-state-banner .title{font-size:15px;font-weight:800;color:var(--navy);margin-bottom:.4rem;}
.sym-state-banner .sub{font-size:13px;color:var(--slate-500);line-height:1.55;max-width:540px;margin:0 auto;}
.cohort-table{width:100%;border-collapse:collapse;font-size:12.5px;}
.cohort-table th{background:var(--slate-50);padding:7px 12px;font-size:10.5px;text-transform:uppercase;letter-spacing:.07em;color:var(--slate-500);font-weight:700;border-bottom:2px solid var(--border);text-align:left;}
.cohort-table td{padding:6px 12px;border-bottom:1px solid var(--border);color:var(--navy);}
.cohort-table tr:last-child td{border-bottom:none;}
.thinking-overlay{position:fixed;inset:0;background:rgba(255,255,255,.65);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;z-index:99999;}
.thinking-card{width:min(400px,92vw);background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xl);box-shadow:var(--shadow-lg);padding:1.6rem;text-align:center;}
.thinking-spinner{width:40px;height:40px;border:3px solid var(--slate-200);border-top-color:var(--accent);border-radius:50%;margin:0 auto 1rem;animation:tw-spin .8s linear infinite;}
.thinking-title{color:var(--navy);font-weight:800;font-size:1.05rem;margin-bottom:.25rem;letter-spacing:-.02em;}
.thinking-sub{color:var(--slate-500);font-size:.92rem;line-height:1.4;}
.nav-tabs-wrap{background:var(--surface);border-radius:var(--radius-xl);padding:8px 10px;border:1px solid var(--border);box-shadow:var(--shadow-sm);margin:1.1rem 0 1.4rem;}
.nav-tabs-label{font-size:11px;color:var(--slate-500);font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;}
.soft-panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:12px 14px;box-shadow:var(--shadow-xs);margin:.55rem 0 .95rem;}
.pill-row{display:flex;flex-wrap:wrap;gap:7px;margin-top:8px;align-items:center;}
.pill{display:inline-flex;align-items:center;gap:6px;padding:5px 10px;border-radius:999px;background:var(--slate-50);border:1px solid var(--border);font-size:11.5px;font-weight:600;color:var(--navy);}
.pill .muted{color:var(--slate-500);font-weight:700;}
.small-muted{font-size:12px;color:var(--slate-500);}
.ref-wrap{display:inline-flex;position:relative;vertical-align:middle;margin-left:4px;margin-right:2px;line-height:1;z-index:40;isolation:isolate;contain:paint;}
.ref-wrap:hover,.ref-wrap:focus-within{z-index:2147483000!important;}
.ref-wrap::after{content:"";position:absolute;left:50%;transform:translateX(-50%);top:100%;width:min(500px,calc(100vw - 24px));height:26px;background:transparent;z-index:1;}
.ref-tile{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:999px;background:#eff6ff;border:1px solid #bfdbfe;color:#1d4ed8;font-size:11.5px;font-weight:700;line-height:1;cursor:help;white-space:nowrap;position:relative;z-index:2;}
.ref-tip{position:absolute;left:50%;transform:translateX(-50%);top:calc(100% + 10px);width:min(520px,calc(100vw - 24px));box-sizing:border-box;border:1px solid var(--border-strong);border-radius:20px;box-shadow:var(--shadow-lg);z-index:2147483600!important;opacity:0;visibility:hidden;pointer-events:auto;transition:opacity .12s ease, visibility .12s ease;white-space:normal;overflow:hidden;background:#fff;backdrop-filter:blur(6px);overflow-wrap:anywhere;word-break:break-word;isolation:isolate;contain:layout paint;}
.ref-tip::before{content:"";position:absolute;inset:0;background:#fff;border-radius:inherit;z-index:0;}
.ref-tip-inner{position:relative;z-index:2;max-height:min(400px,70vh);overflow:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;padding:18px 22px 20px 22px;background:#fff;scrollbar-gutter:stable;box-sizing:border-box;}
.ref-wrap:hover .ref-tip,.ref-wrap:focus-within .ref-tip,.ref-tip:hover{opacity:1;visibility:visible;}
.ref-tip-inner::-webkit-scrollbar{width:10px;}
.ref-tip-inner::-webkit-scrollbar-thumb{background:rgba(100,116,139,.35);border-radius:999px;}
.ref-item{padding:12px 6px 13px 6px;border-bottom:1px solid var(--border);position:relative;z-index:2;background:#fff;border-radius:12px;}
.ref-item:last-child{border-bottom:none;padding-bottom:6px;}
.ref-item:first-child{padding-top:6px;}
.ref-meta{font-size:10.5px;text-transform:uppercase;letter-spacing:.07em;color:var(--slate-500);font-weight:700;margin-bottom:5px;padding:0 1px;}
.ref-title{font-size:12px;font-weight:700;color:var(--navy);margin-bottom:6px;line-height:1.4;overflow-wrap:anywhere;word-break:break-word;padding:0 1px;}
.ref-snippet{font-size:11.5px;line-height:1.52;color:var(--slate-600);white-space:normal;overflow-wrap:anywhere;word-break:break-word;padding:0 1px;}
.ref-empty{font-size:11.5px;color:var(--slate-500);line-height:1.5;overflow-wrap:anywhere;word-break:break-word;padding:2px 1px;}
[data-testid="stChatMessage"],[data-testid="stChatMessageContent"]{overflow:visible!important;position:relative!important;z-index:0!important;}
[data-testid="stMarkdownContainer"]{overflow:visible!important;overflow-wrap:anywhere!important;word-break:break-word!important;}
[data-testid="stChatMessageContent"] p,[data-testid="stChatMessageContent"] li{font-size:12.35px;line-height:1.58;}
[data-testid="stChatMessageContent"] h1,[data-testid="stChatMessageContent"] h2,[data-testid="stChatMessageContent"] h3,[data-testid="stChatMessageContent"] h4{font-size:12.45px;line-height:1.45;font-weight:700;margin:.42rem 0 .18rem;letter-spacing:-.005em;}
[data-testid="stChatMessageContent"] ul,[data-testid="stChatMessageContent"] ol{margin:.2rem 0 .55rem 1rem;}
.workspace-nav-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xl);padding:12px 14px;box-shadow:var(--shadow-sm);margin:1.05rem 0 1.25rem;}
.builder-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xl);padding:16px 18px;box-shadow:var(--shadow-sm);margin:.3rem 0 1rem;}
.builder-kicker{font-size:10.5px;text-transform:uppercase;letter-spacing:.1em;color:var(--accent);font-weight:700;margin-bottom:4px;}
.builder-title{font-size:18px;font-weight:800;letter-spacing:-.02em;color:var(--navy);margin-bottom:4px;}
.builder-sub{font-size:13px;color:var(--slate-500);line-height:1.5;margin-bottom:10px;}
.summary-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:.2rem 0 .85rem;}
.summary-item{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-md);padding:12px 13px;box-shadow:var(--shadow-xs);min-height:96px;}
.summary-item .label{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--slate-500);font-weight:700;margin-bottom:4px;}
.summary-item .value{font-size:18px;font-weight:800;letter-spacing:-.025em;color:var(--navy);line-height:1.2;}
.summary-item .sub{font-size:12px;color:var(--slate-500);line-height:1.45;margin-top:5px;}
.sidebar-scope-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-md);padding:10px 12px;margin:0 0 .65rem;box-shadow:var(--shadow-xs);}
.sidebar-scope-title{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--slate-500);font-weight:700;margin-bottom:4px;}
.sidebar-scope-value{font-size:13px;font-weight:700;color:var(--navy);line-height:1.35;}
.workspace-nav-sub{font-size:12px;color:var(--slate-500);margin:-2px 0 8px;line-height:1.45;}
.section-note{font-size:12.5px;color:var(--slate-500);line-height:1.5;margin:0 0 10px;}
.helper-chip-row{display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-top:8px;}
.helper-chip{display:inline-flex;align-items:center;padding:4px 9px;border-radius:999px;background:var(--slate-50);border:1px solid var(--border);font-size:11px;font-weight:700;color:var(--slate-600);}
@keyframes tw-spin{to{transform:rotate(360deg);}}
@media(max-width:1100px){.hero-grid{grid-template-columns:repeat(2,minmax(0,1fr));}.summary-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}
@media(max-width:768px){
  .hero-grid{grid-template-columns:1fr;}
  .summary-grid{grid-template-columns:1fr;}
  .builder-card{padding:14px 14px 15px;}
  .hero-title{font-size:20px;}
  .metric-card{min-height:94px;}
  .ref-wrap{display:inline-block;max-width:100%;}
  .ref-tip{left:0;transform:none;width:min(360px,calc(100vw - 20px));max-height:min(56vh,360px);}
  .ref-tip-inner{padding:15px 16px 17px 16px;}
  .ref-item{padding:10px 3px 11px 3px;}
  .block-container{
    padding-top:1.25rem!important;
    padding-bottom:2.25rem!important;
    padding-left:1rem!important;
    padding-right:1rem!important;
  }
}

.lava-lamp{
  position:fixed;
  inset:0;
  pointer-events:none;
  overflow:hidden;
  z-index:0;
  opacity:.72;
}
.lava-blob{position:absolute;border-radius:50%;filter:blur(60px);mix-blend-mode:screen;opacity:.28;animation:lava-float 24s ease-in-out infinite alternate;}
.lava-blob.a{width:340px;height:340px;background:rgba(249,115,22,.40);top:-70px;left:-40px;animation-duration:23s;}
.lava-blob.b{width:420px;height:420px;background:rgba(99,102,241,.26);top:16%;right:-120px;animation-duration:28s;animation-delay:-6s;}
.lava-blob.c{width:360px;height:360px;background:rgba(236,72,153,.20);bottom:-110px;left:18%;animation-duration:31s;animation-delay:-9s;}
.lava-blob.d{width:300px;height:300px;background:rgba(34,197,94,.18);bottom:10%;right:16%;animation-duration:26s;animation-delay:-11s;}
[data-testid="stAppViewContainer"]>.main,
.block-container,
[data-testid="stSidebar"],
header{
  position:relative;
  z-index:1;
}
.beta-chip{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;background:#fef3c7;border:1px solid #f59e0b;color:#92400e;font-size:11px;font-weight:800;letter-spacing:.03em;text-transform:uppercase;}
.social-placeholder-card{background:linear-gradient(145deg,rgba(255,255,255,.96),rgba(241,245,249,.94));border:1px solid rgba(148,163,184,.24);border-radius:18px;padding:16px 16px 14px;box-shadow:var(--shadow-sm);min-height:190px;}
.social-placeholder-title{font-size:16px;font-weight:800;color:var(--navy);margin-bottom:6px;letter-spacing:-.02em;}
.social-placeholder-sub{font-size:12.5px;color:var(--slate-500);line-height:1.55;}
.social-placeholder-stat{font-size:11px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;}
.beta-banner{background:linear-gradient(135deg,rgba(15,23,42,.98),rgba(30,41,59,.96));border:1px solid rgba(99,102,241,.28);border-radius:22px;padding:18px 20px;box-shadow:var(--shadow-lg);color:#f8fafc;margin:.25rem 0 1rem;position:relative;overflow:hidden;}
.beta-banner::after{content:"";position:absolute;inset:auto -40px -60px auto;width:180px;height:180px;background:radial-gradient(circle,rgba(249,115,22,.22),rgba(249,115,22,0));filter:blur(8px);}
.beta-banner .title{font-size:18px;font-weight:800;letter-spacing:-.025em;margin-bottom:6px;position:relative;z-index:1;}
.beta-banner .sub{font-size:13px;line-height:1.6;color:rgba(241,245,249,.86);max-width:780px;position:relative;z-index:1;}
.beta-banner .pill-row{position:relative;z-index:1;}
@keyframes lava-float{0%{transform:translate3d(0,0,0) scale(1);}50%{transform:translate3d(70px,42px,0) scale(1.16);}100%{transform:translate3d(-55px,96px,0) scale(.92);}}

/* Bottom chat bar */
.chat-bar-header{display:flex;align-items:center;gap:8px;margin-bottom:8px;}
.chat-bar-header .icon{font-size:15px;}
.chat-bar-header .label{font-size:13px;font-weight:600;color:var(--slate-500);}
.chat-quick-actions{display:flex;gap:6px;flex-wrap:wrap;margin:6px 0 12px;}
.chat-quick-btn{display:inline-flex;align-items:center;gap:4px;padding:6px 12px;border-radius:999px;background:var(--slate-50);border:1px solid var(--border);font-size:11.5px;font-weight:500;color:var(--slate-600);cursor:pointer;transition:all .12s ease;}
.chat-quick-btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(99,102,241,.05);}

/* Symptomizer insights card for dashboard */
.sym-insights-card{background:linear-gradient(145deg,rgba(99,102,241,.04),var(--surface));border:1px solid rgba(99,102,241,.18);border-radius:var(--radius-lg);padding:14px 16px;box-shadow:var(--shadow-xs);margin:.5rem 0;}
.sym-insights-title{font-size:11px;text-transform:uppercase;letter-spacing:.09em;color:var(--accent);font-weight:700;margin-bottom:8px;}
.sym-bar{display:flex;align-items:center;gap:8px;margin:3px 0;}
.sym-bar-label{font-size:11.5px;font-weight:600;color:var(--navy);min-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.sym-bar-track{flex:1;height:6px;background:var(--slate-100);border-radius:3px;overflow:hidden;}
.sym-bar-fill{height:100%;border-radius:3px;transition:width .3s ease;}
.sym-bar-fill.det{background:var(--danger);}
.sym-bar-fill.del{background:var(--success);}
.sym-bar-count{font-size:11px;font-weight:700;color:var(--slate-500);min-width:28px;text-align:right;}

/* Time estimate badge */
.time-estimate{display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:999px;background:#f0fdf4;border:1px solid #86efac;font-size:11px;font-weight:600;color:#15803d;margin-top:4px;}

/* Chip hover effects */
.chip{transition:all .12s ease;cursor:default;}
.chip:hover{filter:brightness(.95);transform:translateY(-1px);box-shadow:0 2px 6px rgba(0,0,0,.08);}

/* Dark mode support */
@media (prefers-color-scheme: dark) {
  :root {
    --navy:#e2e8f0; --navy-mid:#cbd5e1; --navy-soft:#94a3b8;
    --slate-600:#94a3b8; --slate-500:#94a3b8; --slate-400:#64748b;
    --slate-200:#334155; --slate-100:#1e293b; --slate-50:#0f172a; --white:#0f172a;
    --page-bg:#0c1222; --surface:#1a2332; --border:#2d3a4f; --border-strong:#3d4f6a;
    --shadow-xs:0 1px 2px rgba(0,0,0,.2);
    --shadow-sm:0 1px 4px rgba(0,0,0,.25);
    --shadow-md:0 4px 12px rgba(0,0,0,.3);
    --shadow-lg:0 8px 28px rgba(0,0,0,.4);
  }
  .chip.blue{background:rgba(37,99,235,.15);border-color:rgba(37,99,235,.3);color:#60a5fa;}
  .chip.green{background:rgba(5,150,105,.15);border-color:rgba(5,150,105,.3);color:#34d399;}
  .chip.red{background:rgba(220,38,38,.15);border-color:rgba(220,38,38,.3);color:#f87171;}
  .chip.yellow{background:rgba(217,119,6,.15);border-color:rgba(217,119,6,.3);color:#fbbf24;}
  .chip.indigo{background:rgba(99,102,241,.15);border-color:rgba(99,102,241,.3);color:#a5b4fc;}
  .chip.gray{background:rgba(100,116,139,.15);border-color:rgba(100,116,139,.3);color:#94a3b8;}
  .ev-highlight{background:rgba(254,240,138,.2);}
  .ev-highlight.ev-det{background:rgba(239,68,68,.18);}
  .ev-highlight.ev-del{background:rgba(16,185,129,.18);}
  .review-body{color:var(--navy);}
  .sw-table tbody tr:hover{background:rgba(99,102,241,.06);}
  .thinking-card{background:var(--surface);border-color:var(--border);}
  .ref-tip,.ref-tip::before,.ref-item{background:var(--surface)!important;}
  [data-testid="stSidebar"]{background:#111827!important;border-right-color:var(--border)!important;}
  .metric-card,.hero-card,.hero-stat,[data-testid="stMetric"]{background:var(--surface);border-color:var(--border);}
  .metric-card.accent{background:linear-gradient(145deg,rgba(99,102,241,.08),var(--surface));}
  .beta-banner{background:linear-gradient(135deg,rgba(15,23,42,.95),rgba(30,41,59,.9));}
}

/* Streamlit dark mode detection (Streamlit uses data-theme attribute) */
[data-theme="dark"] {
  --navy:#e2e8f0; --navy-mid:#cbd5e1; --navy-soft:#94a3b8;
  --page-bg:#0c1222; --surface:#1a2332; --border:#2d3a4f; --border-strong:#3d4f6a;
  --slate-50:#0f172a; --slate-100:#1e293b;
}

</style>
"""
