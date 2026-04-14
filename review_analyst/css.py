"""Clean, light-only app theme for the CSAT dashboard."""
from __future__ import annotations

APP_CSS = """
<style>
:root,
[data-theme="light"],
[data-theme="dark"] {
  color-scheme: light !important;
  --navy: #111827;
  --navy-mid: #1f2937;
  --navy-soft: #374151;
  --slate-600: #4b5563;
  --slate-500: #6b7280;
  --slate-400: #9ca3af;
  --slate-200: #e5e7eb;
  --slate-100: #f3f4f6;
  --slate-50: #f9fafb;
  --white: #ffffff;
  --accent: #2563eb;
  --accent-bg: #eff6ff;
  --success: #15803d;
  --danger: #b91c1c;
  --warning: #b45309;
  --info: #2563eb;
  --page-bg: #f5f7fb;
  --surface: #ffffff;
  --surface-soft: #fafbfc;
  --border: #e5e7eb;
  --border-strong: #d1d5db;
  --shadow-xs: 0 1px 2px rgba(15, 23, 42, 0.04);
  --shadow-sm: 0 8px 24px rgba(15, 23, 42, 0.05);
  --shadow-md: 0 14px 30px rgba(15, 23, 42, 0.08);
  --shadow-lg: 0 18px 40px rgba(15, 23, 42, 0.10);
  --radius-sm: 10px;
  --radius-md: 14px;
  --radius-lg: 18px;
  --radius-xl: 22px;
}

html,
body,
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
[data-testid="stMain"],
.main {
  color-scheme: light !important;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
  color: var(--navy) !important;
  background: var(--page-bg) !important;
}

* {
  box-sizing: border-box;
}

body,
p,
span,
label,
small,
li,
div {
  color: inherit;
}

[data-testid="stHeader"] {
  background: rgba(245, 247, 251, 0.96) !important;
  border-bottom: 1px solid var(--border) !important;
}

[data-testid="stToolbar"] {
  right: 0.75rem;
}

.block-container {
  max-width: 1500px !important;
  padding-top: 1.4rem !important;
  padding-bottom: 2.5rem !important;
}

[data-testid="stSidebar"],
[data-testid="stSidebar"] > div {
  background: var(--surface) !important;
  color: var(--navy) !important;
}

[data-testid="stSidebar"] {
  border-right: 1px solid var(--border) !important;
}

[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] div,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span {
  color: var(--navy) !important;
}

.stButton > button,
[data-testid="baseButton-secondary"],
[data-testid="baseButton-headerNoPadding"] {
  min-height: 38px !important;
  border-radius: var(--radius-sm) !important;
  border: 1px solid var(--border-strong) !important;
  background: var(--surface) !important;
  color: var(--navy) !important;
  box-shadow: var(--shadow-xs) !important;
  font-weight: 600 !important;
  letter-spacing: -0.01em !important;
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background 0.14s ease !important;
}

.stButton > button:hover,
[data-testid="baseButton-secondary"]:hover,
[data-testid="baseButton-headerNoPadding"]:hover {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.10) !important;
  color: var(--accent) !important;
}

[data-testid="baseButton-primary"],
[data-testid="baseButton-primary"]:hover,
.stDownloadButton > button {
  background: var(--accent) !important;
  color: #ffffff !important;
  border-color: var(--accent) !important;
}

[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea,
[data-testid="stNumberInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stSelectbox"] > div > div,
[data-testid="stMultiselect"] > div > div,
[data-testid="stFileUploader"] section {
  border-radius: var(--radius-sm) !important;
  border-color: var(--border-strong) !important;
  background: var(--surface) !important;
  color: var(--navy) !important;
  box-shadow: none !important;
}

[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus,
[data-testid="stNumberInput"] input:focus,
[data-testid="stDateInput"] input:focus {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.10) !important;
}

[data-testid="stExpander"],
[data-testid="stMetric"],
[data-testid="stDataFrame"],
[data-testid="stContainer"][data-border="true"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
  box-shadow: var(--shadow-xs) !important;
}

[data-testid="stMetric"] [data-testid="stMetricValue"] {
  color: var(--navy) !important;
}

[data-testid="stMetric"] [data-testid="stMetricLabel"] {
  color: var(--slate-500) !important;
}

[data-testid="stProgressBar"] > div > div {
  background: var(--accent) !important;
}

.hero-card,
.metric-card,
.hero-stat,
.ws-status-bar,
.soft-panel,
.workspace-nav-card,
.builder-card,
.summary-item,
.sidebar-scope-card,
.social-placeholder-card,
.beta-banner,
.sym-insights-card,
.social-demo-hero,
.social-demo-note,
.social-demo-stat,
.social-voice-card,
.social-comment-card,
.social-signal-card {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-lg) !important;
  box-shadow: var(--shadow-xs) !important;
}

.hero-card,
.builder-card,
.workspace-nav-card,
.beta-banner,
.social-demo-hero {
  padding: 16px 18px !important;
}

.metric-card,
.hero-stat,
.summary-item,
.social-demo-stat,
.social-signal-card,
.social-voice-card,
.social-comment-card,
.social-placeholder-card,
.sym-insights-card {
  padding: 14px 16px !important;
}

.metric-card.accent,
.hero-stat.accent,
.social-demo-stat.indigo,
.social-demo-stat.green,
.social-demo-stat.orange,
.social-demo-stat.red,
.sym-insights-card,
.social-signal-card {
  background: var(--surface) !important;
}

.metric-card.accent,
.hero-stat.accent,
.social-demo-stat.indigo {
  border-left: 4px solid var(--accent) !important;
}

.social-demo-stat.green {
  border-left: 4px solid var(--success) !important;
}

.social-demo-stat.orange,
.social-signal-card {
  border-left: 4px solid var(--warning) !important;
}

.social-demo-stat.red {
  border-left: 4px solid var(--danger) !important;
}

.hero-kicker,
.builder-kicker,
.sym-insights-title,
.social-kicker,
.metric-label,
.sidebar-scope-title,
.summary-item .label,
.hero-stat .label,
.social-demo-stat .label,
.ref-meta,
.nav-tabs-label {
  color: var(--slate-500) !important;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 10.5px;
  font-weight: 700;
}

.hero-title,
.builder-title,
.section-title,
.social-demo-title,
.social-comment-title,
.social-placeholder-title,
.thinking-title,
.sym-state-banner .title,
.summary-item .value,
.hero-stat .value,
.metric-value,
.social-demo-stat .value,
.social-signal-score {
  color: var(--navy) !important;
  letter-spacing: -0.03em;
  font-weight: 800;
}

.hero-title,
.builder-title,
.section-title,
.social-demo-title {
  font-size: 20px;
}

.metric-value,
.hero-stat .value,
.social-demo-stat .value,
.social-signal-score {
  font-size: 24px;
}

.metric-sub,
.section-sub,
.section-note,
.builder-sub,
.workspace-nav-sub,
.summary-item .sub,
.sidebar-scope-value,
.review-body,
.social-demo-sub,
.social-demo-note,
.social-comment-meta,
.social-snippet,
.social-placeholder-sub,
.small-muted,
.compact-pager-sub,
.thinking-sub,
.sym-state-banner .sub,
.ref-snippet,
.ref-empty,
.social-demo-stat .sub {
  color: var(--slate-600) !important;
}

.section-sub,
.section-note,
.builder-sub,
.workspace-nav-sub,
.social-demo-sub,
.review-body,
.social-snippet,
.ref-snippet,
.thinking-sub,
.sym-state-banner .sub {
  line-height: 1.55;
}

.badge-row,
.chip-wrap,
.helper-chip-row,
.pill-row,
.chat-quick-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
}

.chip,
.pill,
.helper-chip,
.ws-filter-pill,
.beta-chip,
.social-platform-chip,
.ref-tile,
.chat-quick-btn,
.time-estimate {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 10px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 700;
  border: 1px solid var(--border);
  background: var(--slate-50);
  color: var(--navy);
  box-shadow: none;
}

.chip.blue,
.ref-tile,
.beta-chip,
.time-estimate,
.social-platform-chip.generic {
  background: var(--accent-bg);
  border-color: #bfdbfe;
  color: var(--accent);
}

.chip.green {
  background: #f0fdf4;
  border-color: #bbf7d0;
  color: var(--success);
}

.chip.red {
  background: #fef2f2;
  border-color: #fecaca;
  color: var(--danger);
}

.chip.yellow,
.social-platform-chip.reddit {
  background: #fff7ed;
  border-color: #fed7aa;
  color: var(--warning);
}

.chip.indigo,
.social-platform-chip.instagram {
  background: #eef2ff;
  border-color: #c7d2fe;
  color: #4f46e5;
}

.chip.gray,
.social-platform-chip.youtube,
.helper-chip,
.chat-quick-btn,
.ws-filter-pill,
.pill,
.social-comment-quote {
  background: var(--slate-50);
  border-color: var(--border);
  color: var(--slate-600);
}

.chip:hover,
.chat-quick-btn:hover {
  transform: none;
  filter: none;
  border-color: var(--accent);
  color: var(--accent);
  box-shadow: none;
}

.ws-status-bar {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  padding: 10px 14px !important;
}

.ws-status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--success);
  display: inline-block;
  margin-right: 6px;
}

.hero-grid,
.summary-grid {
  display: grid;
  gap: 10px;
}

.hero-grid {
  grid-template-columns: repeat(5, minmax(0, 1fr));
  margin-top: 12px;
}

.summary-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 0.25rem 0 0.9rem;
}

.review-body {
  font-size: 13.5px;
  white-space: pre-wrap;
  word-break: break-word;
}

.ev-highlight {
  background: #fef3c7;
  border-radius: 4px;
  padding: 0 0.18em;
  position: relative;
}

.ev-highlight.ev-det {
  background: #fee2e2;
}

.ev-highlight.ev-del {
  background: #dcfce7;
}

.ev-highlight::after {
  content: attr(data-tag);
  position: absolute;
  left: 50%;
  top: calc(100% + 6px);
  transform: translateX(-50%);
  width: min(260px, 60vw);
  background: #ffffff;
  color: var(--navy);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 0.5rem 0.65rem;
  font-size: 0.72rem;
  line-height: 1.35;
  box-shadow: var(--shadow-md);
  white-space: normal;
  z-index: 1000;
  pointer-events: none;
  opacity: 0;
  transition: opacity 0.12s ease;
}

.ev-highlight:hover::after {
  opacity: 1;
}

.sw-table-wrap,
.cohort-table {
  width: 100%;
  border-radius: var(--radius-md);
  border: 1px solid var(--border);
  overflow: hidden;
}

.sw-table,
.cohort-table {
  border-collapse: collapse;
  font-size: 12.5px;
}

.sw-table thead tr,
.cohort-table th {
  background: var(--slate-50);
}

.sw-table thead th,
.cohort-table th {
  padding: 8px 12px;
  text-align: left;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  font-size: 10.5px;
  color: var(--slate-500);
  border-bottom: 1px solid var(--border);
}

.sw-table tbody tr,
.cohort-table td {
  border-bottom: 1px solid var(--border);
}

.sw-table tbody tr:hover {
  background: var(--slate-50);
}

.sw-table tbody td,
.cohort-table td {
  padding: 8px 12px;
  color: var(--navy);
}

.sw-td-right {
  text-align: right !important;
  font-variant-numeric: tabular-nums;
}

.sw-star-good {
  color: var(--success);
  font-weight: 700;
}

.sw-star-bad {
  color: var(--danger);
  font-weight: 700;
}

.sw-divider {
  border: 0;
  border-top: 1px solid var(--border);
  margin: 1.3rem 0 1rem;
}

.compact-pager-status {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 38px;
  font-size: 13px;
  font-weight: 700;
  color: var(--navy);
}

.sym-state-banner,
.thinking-card,
.ref-tip {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-lg) !important;
  box-shadow: var(--shadow-sm) !important;
}

.sym-state-banner {
  padding: 2rem;
  text-align: center;
  margin: 1rem 0;
  border-style: dashed !important;
}

.thinking-overlay {
  position: fixed;
  inset: 0;
  background: rgba(245, 247, 251, 0.76);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 99999;
}

.thinking-card {
  width: min(400px, 92vw);
  padding: 1.5rem;
  text-align: center;
}

.thinking-spinner {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: 3px solid var(--slate-200);
  border-top-color: var(--accent);
  margin: 0 auto 1rem;
  animation: tw-spin 0.8s linear infinite;
}

.nav-tabs-wrap {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-xs);
  padding: 8px 10px;
  margin: 1rem 0 1.2rem;
}

.soft-panel {
  padding: 12px 14px;
  margin: 0.55rem 0 0.95rem;
}

.ref-wrap {
  display: inline-flex;
  position: relative;
  vertical-align: middle;
  z-index: 40;
}

.ref-wrap:hover,
.ref-wrap:focus-within {
  z-index: 2147483000 !important;
}

.ref-tip {
  position: absolute;
  left: 50%;
  top: calc(100% + 10px);
  transform: translateX(-50%);
  width: min(520px, calc(100vw - 24px));
  opacity: 0;
  visibility: hidden;
  overflow: hidden;
}

.ref-tip::before,
.ref-tip-inner,
.ref-item {
  background: var(--surface) !important;
}

.ref-tip-inner {
  max-height: min(400px, 70vh);
  overflow: auto;
  padding: 18px 20px;
}

.ref-item {
  padding: 12px 6px;
  border-bottom: 1px solid var(--border);
}

.ref-item:last-child {
  border-bottom: 0;
}

.ref-title {
  font-size: 12px;
  font-weight: 700;
  color: var(--navy);
  margin-bottom: 6px;
  line-height: 1.4;
}

.ref-wrap:hover .ref-tip,
.ref-wrap:focus-within .ref-tip,
.ref-tip:hover {
  opacity: 1;
  visibility: visible;
}

[data-testid="stChatMessage"],
[data-testid="stChatMessageContent"],
[data-testid="stMarkdownContainer"] {
  overflow: visible !important;
}

[data-testid="stChatMessageContent"] p,
[data-testid="stChatMessageContent"] li {
  font-size: 12.35px;
  line-height: 1.58;
}

[data-testid="stChatMessageContent"] h1,
[data-testid="stChatMessageContent"] h2,
[data-testid="stChatMessageContent"] h3,
[data-testid="stChatMessageContent"] h4 {
  font-size: 12.45px;
  line-height: 1.45;
  font-weight: 700;
  margin: 0.42rem 0 0.18rem;
}

.sidebar-scope-value,
.workspace-nav-sub,
.builder-sub {
  font-size: 12.5px;
}

.chat-bar-header .icon {
  font-size: 15px;
}

.chat-bar-header .label {
  font-size: 13px;
  font-weight: 600;
  color: var(--slate-500);
}

.social-comment-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-top: 10px;
  font-size: 12.2px;
  color: var(--slate-600);
}

.social-comment-quote {
  margin-top: 10px;
  padding: 10px 12px;
  border-radius: var(--radius-md);
}

.social-signal-score {
  color: var(--warning) !important;
}

.sym-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 4px 0;
}

.sym-bar-label {
  min-width: 130px;
  font-size: 11.5px;
  font-weight: 600;
  color: var(--navy);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sym-bar-track {
  flex: 1;
  height: 6px;
  background: var(--slate-100);
  border-radius: 999px;
  overflow: hidden;
}

.sym-bar-fill {
  height: 100%;
  border-radius: 999px;
}

.sym-bar-fill.det {
  background: #dc2626;
}

.sym-bar-fill.del {
  background: #16a34a;
}

.sym-bar-count {
  min-width: 28px;
  text-align: right;
  font-size: 11px;
  font-weight: 700;
  color: var(--slate-500);
}

.lava-lamp,
.lava-blob {
  display: none !important;
}

.beta-banner::after,
.social-demo-hero::before,
.social-demo-hero::after {
  display: none !important;
}

@keyframes tw-spin {
  to { transform: rotate(360deg); }
}

@media (max-width: 1100px) {
  .hero-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .summary-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 768px) {
  .hero-grid,
  .summary-grid,
  .social-comment-grid {
    grid-template-columns: 1fr;
  }

  .block-container {
    padding-left: 1rem !important;
    padding-right: 1rem !important;
  }

  .ref-wrap {
    display: inline-block;
    max-width: 100%;
  }

  .ref-tip {
    left: 0;
    transform: none;
    width: min(360px, calc(100vw - 20px));
  }
}
</style>
"""
