"""Social Listening Beta tab — mocked Meltwater-style experience.
Extracted from app.py for organization. Fully self-contained.
"""
from __future__ import annotations
import html as _html_mod
import re
from datetime import date, timedelta
from typing import Any
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

def _esc(s): return _html_mod.escape(str(s or ""))
def _safe_text(v, default=""):
    if v is None: return default
    try: m = pd.isna(v)
    except: m = False
    if isinstance(m, bool) and m: return default
    t = str(v).strip()
    return default if t.lower() in {"nan","none","null","<na>"} else t
def _slugify(text, fallback="custom"):
    c = re.sub(r"[^a-zA-Z0-9]+", "_", _safe_text(text).lower())
    c = re.sub(r"_+", "_", c).strip("_") or fallback
    return ("prompt_" + c if c[0].isdigit() else c)[:64]
def _sw_style_fig(fig):
    GRID = "rgba(148,163,184,0.18)"
    tc = len(getattr(fig, "data", []) or [])
    lcfg = dict(orientation="v",y=1.0,x=1.01,xanchor="left",yanchor="top",bgcolor="rgba(255,255,255,0.86)",bordercolor="rgba(148,163,184,0.22)",borderwidth=1,font=dict(size=11)) if tc > 3 else dict(orientation="h",y=1.12,x=0,xanchor="left",yanchor="bottom",bgcolor="rgba(255,255,255,0.84)",bordercolor="rgba(148,163,184,0.18)",borderwidth=1,font=dict(size=11))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Inter, system-ui, sans-serif",size=12),margin=dict(l=26,r=108 if tc>3 else 18,t=56 if tc>3 else 64,b=44),title=dict(x=0,xanchor="left",font=dict(size=15)),legend=lcfg,hoverlabel=dict(font=dict(family="Inter, system-ui, sans-serif",size=12)))
    fig.update_xaxes(gridcolor=GRID,zerolinecolor=GRID,automargin=True,title_standoff=10)
    fig.update_yaxes(gridcolor=GRID,zerolinecolor=GRID,automargin=True,title_standoff=10)
    return fig
def _show_plotly(fig):
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo":False,"displayModeBar":False,"responsive":True,"modeBarButtonsToRemove":["lasso2d","select2d","autoScale2d","toggleSpikelines"]})

def _build_social_beta_query(raw_query: str) -> str:
    text = _safe_text(raw_query).strip() or "Shark FlexBreeze"
    compact = re.sub(r"[^A-Za-z0-9]+", "", text)
    parts = [f'"{text}"']
    if compact and compact.lower() != re.sub(r"\s+", "", text).lower():
        parts.append(f'"{compact}"')
    if compact:
        parts.append(f"#{compact}")
    lowered = text.lower()
    if "shark" not in lowered:
        parts.append(f'"Shark {text}"')
    if "ninja" not in lowered:
        parts.append(f'"Ninja {text}"')
    return " OR ".join(dict.fromkeys(parts))


def _social_demo_payload():
    posts = pd.DataFrame([
        dict(rank=1, platform="Reddit", content_type="Thread", headline="FlexStyle filter door is way harder to remove than it should be", author="u/blowoutscience", source="r/HaircareScience", submission_date="2026-04-06", sentiment="Negative", views=86000, likes=1430, comments=243, shares=114, engagement=1787, velocity_pct=189, viral_status="New viral complaint", theme="Filter Door / Cleaning", voc_takeaway="Consumers understand there is a filter, but not the door motion or cleaning cadence.", snippet="I love the styling results, but the filter door feels like I'm going to break it every time I try to open it.", top_comment="I thought I was going to snap the door the first week — it's not intuitive at all and the arrows are too subtle.", top_comment_author="u/heatprotectplease", top_comment_likes=612),
        dict(rank=2, platform="YouTube", content_type="Video", headline="FlexStyle vs Dyson Airwrap after 60 days", author="Blowout Lab", source="YouTube · Blowout Lab", submission_date="2026-04-05", sentiment="Mixed", views=186000, likes=9800, comments=1304, shares=640, engagement=11744, velocity_pct=121, viral_status="Rising comparison video", theme="Value vs Dyson", voc_takeaway="Value wins the click, but premium feel and maintenance clarity still favor Dyson in the comments.", snippet="After two months I still think FlexStyle wins on value, but Dyson feels more premium and polished.", top_comment="FlexStyle is the better value, but Dyson still wins on polish and attachments. Cleaning the FlexStyle filter is way less intuitive.", top_comment_author="@styledbyjen", top_comment_likes=1904),
        dict(rank=3, platform="YouTube", content_type="Tutorial", headline="Why your FlexStyle is flashing red — clean the filter first", author="Clean Girl Routine", source="YouTube · Clean Girl Routine", submission_date="2026-04-04", sentiment="Mixed", views=94000, likes=4200, comments=487, shares=211, engagement=4898, velocity_pct=96, viral_status="Service moment", theme="Filter Cleaning", voc_takeaway="Education resolves the issue quickly, but customers think the warning means something is broken.", snippet="Most of the panic comments were actually clogged filter issues, not a dead unit.", top_comment="Why is the filter door so hard to remove? I had to pause and rewind twice.", top_comment_author="@curlsbeforecoffee", top_comment_likes=843),
        dict(rank=4, platform="Instagram", content_type="Reel", headline="FlexStyle curls still held better than expected", author="BeautyByMia", source="Instagram · @beautybymia", submission_date="2026-04-04", sentiment="Positive", views=312000, likes=21000, comments=604, shares=1700, engagement=23304, velocity_pct=78, viral_status="Positive creator proof", theme="Results / Value", voc_takeaway="When results show up visually, the price-value story lands immediately.", snippet="I switched from Dyson because the curls still hold and the price makes way more sense.", top_comment="The results are giving Airwrap but the price is way easier to justify.", top_comment_author="@glowmode", top_comment_likes=2455),
        dict(rank=5, platform="Reddit", content_type="Thread", headline="FlexStyle owners — worth switching from Dyson?", author="u/volumequest", source="r/DysonAirwrap", submission_date="2026-04-03", sentiment="Mixed", views=64000, likes=978, comments=189, shares=92, engagement=1259, velocity_pct=64, viral_status="Comparison debate", theme="Premium feel vs Value", voc_takeaway="Consumers do not say FlexStyle is bad — they say it needs clearer maintenance and a more premium-feeling experience.", snippet="Performance is surprisingly close, but Dyson feels more premium and the FlexStyle maintenance steps are not obvious.", top_comment="Performance is surprisingly close, but Dyson feels more premium and the FlexStyle maintenance steps are not obvious.", top_comment_author="u/hottoolhedge", top_comment_likes=521),
        dict(rank=6, platform="YouTube", content_type="Short", headline="Late-night blowout test: FlexStyle noise + filter clean reaction", author="Late Night Blowout", source="YouTube · Late Night Blowout", submission_date="2026-04-02", sentiment="Mixed", views=411000, likes=18000, comments=1827, shares=2200, engagement=22027, velocity_pct=214, viral_status="New viral video", theme="Noise / Filter confusion", voc_takeaway="The post is spreading because the comments became a community troubleshooting thread, not because the creator disliked the product.", snippet="This short is blowing up because everyone in the comments is asking why the filter light came on so fast.", top_comment="Every comment is about the filter door because nobody realizes you have to twist and lift in one motion.", top_comment_author="@heatwavesarah", top_comment_likes=3288),
        dict(rank=7, platform="Instagram", content_type="Carousel", headline="FlexStyle cleaning checklist everyone should save", author="The Blowout Edit", source="Instagram · @theblowoutedit", submission_date="2026-04-01", sentiment="Positive", views=129000, likes=8200, comments=212, shares=540, engagement=8952, velocity_pct=55, viral_status="Helpful maintenance post", theme="Maintenance education", voc_takeaway="Once customers see the steps, the maintenance story feels manageable instead of scary.", snippet="Wish this came in the box — the cleaning step is easy once you see it, but not before.", top_comment="Wish this came in the box — the cleaning step is easy once you see it, but not before.", top_comment_author="@blowdryclub", top_comment_likes=611),
        dict(rank=8, platform="Reddit", content_type="Thread", headline="FlexStyle red light after three weeks?", author="u/bouncylayers", source="r/FlexStyle", submission_date="2026-03-31", sentiment="Negative", views=58000, likes=702, comments=154, shares=60, engagement=916, velocity_pct=88, viral_status="Emerging maintenance confusion", theme="Filter warning / care", voc_takeaway="Customers can self-resolve this, but the first read is still 'my tool is failing'.", snippet="Mine was fine after cleaning, but the filter door still feels fiddly every single time.", top_comment="Mine was fine after cleaning, but the filter door still feels fiddly every single time.", top_comment_author="u/hairdaypanic", top_comment_likes=374),
    ])
    top_comments = (
        posts[["platform", "content_type", "headline", "source", "top_comment_author", "top_comment", "top_comment_likes", "engagement", "theme", "voc_takeaway"]]
        .rename(columns={"top_comment_author": "Author", "top_comment": "Comment", "top_comment_likes": "Comment Likes", "engagement": "Post Engagement", "theme": "Theme", "voc_takeaway": "VOC Takeaway", "headline": "Post / Video"})
        .sort_values(["Comment Likes", "Post Engagement"], ascending=[False, False])
        .reset_index(drop=True)
    )
    detractors = pd.DataFrame([
        dict(Theme="Filter door too hard to remove", Mentions=412, Share="29%", VOC="Customers feel like they might break the door when they try to open it."),
        dict(Theme="Filter cleaning not intuitive", Mentions=286, Share="20%", VOC="The steps are simple once shown, but not obvious from first use or pack-in guidance."),
        dict(Theme="Flashing red warning confusion", Mentions=173, Share="12%", VOC="Warning lights are being interpreted as a defect before customers learn it is a maintenance reminder."),
        dict(Theme="Dyson still feels more premium", Mentions=161, Share="11%", VOC="Comparison shoppers frame FlexStyle as stronger on value but weaker on polish and premium feel."),
        dict(Theme="High-speed airflow can feel loud", Mentions=118, Share="8%", VOC="Noise complaints spike in videos filmed indoors or at night."),
    ])
    delighters = pd.DataFrame([
        dict(Theme="Better value than Dyson", Mentions=407, Share="28%", VOC="Shoppers repeatedly say the price-to-results ratio is what gets them over the line."),
        dict(Theme="Strong styling results", Mentions=521, Share="36%", VOC="Curls, smooth blowouts, and quick dry time are the dominant visual proof points."),
        dict(Theme="Attachments feel versatile once learned", Mentions=198, Share="14%", VOC="Owners who get past the learning curve talk about replacing multiple tools with one system."),
        dict(Theme="Fast dry time", Mentions=176, Share="12%", VOC="Performance speed shows up in both creator content and organic comments."),
        dict(Theme="Less intimidating than Dyson", Mentions=133, Share="9%", VOC="Some first-time hot-tool users describe FlexStyle as easier to approach from a price and usage perspective."),
    ])
    viral = posts.sort_values(["velocity_pct", "engagement"], ascending=[False, False]).head(4).reset_index(drop=True)
    compare = pd.DataFrame([
        dict(Dimension="Value", FlexStyle="Wins in comments due to price-to-performance story", Dyson="Still seen as the prestige benchmark"),
        dict(Dimension="Premium feel", FlexStyle="Praised for results but described as less polished", Dyson="More premium / refined feel in creator comparisons"),
        dict(Dimension="Maintenance clarity", FlexStyle="Filter cleaning and door motion repeatedly called confusing", Dyson="Fewer comments about maintenance confusion"),
        dict(Dimension="Tutorial sentiment", FlexStyle="Consumers actively share fix-it comments and cleaning walkthroughs", Dyson="Less troubleshooting in comments, more styling technique discussion"),
    ])
    metrics = dict(mentions="4,230", positive="61%", viral_posts="4", risk="Medium", top_platform="YouTube reach · Reddit complaint density")
    return {"posts": posts, "top_comments": top_comments, "detractors": detractors, "delighters": delighters, "viral": viral, "compare": compare, "metrics": metrics}


def _social_demo_query(product_name: str) -> str:
    raw = _safe_text(product_name) or "FlexStyle"
    compact = re.sub(r"[^A-Za-z0-9]+", "", raw)
    return f'("{raw}" OR "{compact}" OR "Shark {raw}" OR "Shark {compact}" OR "#' + compact + '") AND (filter OR cleaning OR door OR Dyson OR Airwrap OR tutorial OR comments OR review)'


def _social_demo_trend(start_date, end_date):
    total_days = max((end_date - start_date).days, 1)
    periods = 8 if total_days >= 14 else max(4, total_days + 1)
    dates = pd.date_range(start=start_date, end=end_date, periods=periods)
    base_mentions = [180, 220, 245, 310, 295, 340, 415, 520]
    base_sentiment = [0.69, 0.67, 0.66, 0.63, 0.61, 0.60, 0.62, 0.64]
    mentions = (base_mentions[:periods] if periods <= len(base_mentions) else (base_mentions + [base_mentions[-1]] * (periods - len(base_mentions))))
    sentiment = (base_sentiment[:periods] if periods <= len(base_sentiment) else (base_sentiment + [base_sentiment[-1]] * (periods - len(base_sentiment))))
    df = pd.DataFrame({"date": dates, "mentions": mentions, "sentiment": sentiment})
    df["negative_share"] = (1 - df["sentiment"]).clip(lower=0) * 100
    return df


def _render_social_metric_card(label: str, value: str, sub: str, accent: str = "indigo"):
    st.markdown(f"""<div class='social-demo-stat {accent}'>
      <div class='label'>{_esc(label)}</div>
      <div class='value'>{_esc(value)}</div>
      <div class='sub'>{_esc(sub)}</div>
    </div>""", unsafe_allow_html=True)


def _render_social_post_card(row: pd.Series, *, highlight: str = ""):
    platform_cls = {
        "Reddit": "reddit",
        "YouTube": "youtube",
        "Instagram": "instagram",
    }.get(_safe_text(row.get("platform")), "generic")
    engagement_bits = [
        f"{int(pd.to_numeric(row.get('views'), errors='coerce') or 0):,} views",
        f"{int(pd.to_numeric(row.get('likes'), errors='coerce') or 0):,} likes",
        f"{int(pd.to_numeric(row.get('comments'), errors='coerce') or 0):,} comments",
        f"velocity +{int(pd.to_numeric(row.get('velocity_pct'), errors='coerce') or 0)}%",
    ]
    tag_html = f"<span class='social-platform-chip {platform_cls}'>{_esc(row.get('platform'))}</span>"
    if _safe_text(row.get("viral_status")):
        tag_html += f" <span class='social-platform-chip generic'>{_esc(row.get('viral_status'))}</span>"
    if highlight:
        tag_html += f" <span class='social-platform-chip generic'>{_esc(highlight)}</span>"
    st.markdown(f"""<div class='social-comment-card'>
      <div style='display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap;'>
        <div>
          <div class='social-kicker'>{tag_html}</div>
          <div class='social-comment-title'>{_esc(row.get('headline'))}</div>
          <div class='social-comment-meta'>{_esc(row.get('source'))} · {_esc(row.get('submission_date'))} · {_esc(row.get('content_type'))}</div>
        </div>
        <div class='social-comment-meta'>{_esc(row.get('sentiment'))}</div>
      </div>
      <div class='social-snippet'>{_esc(row.get('snippet'))}</div>
      <div class='social-comment-quote'>Top comment · {_esc(row.get('top_comment_author'))}: “{_esc(row.get('top_comment'))}”</div>
      <div class='social-comment-grid'>
        <div><b>Engagement</b><br>{_esc(' · '.join(engagement_bits))}</div>
        <div><b>VOC takeaway</b><br>{_esc(row.get('voc_takeaway'))}</div>
      </div>
    </div>""", unsafe_allow_html=True)


def _social_demo_answer(question: str) -> str:
    q = _safe_text(question).lower()
    if any(tok in q for tok in ["filter", "door", "clean", "maintenance", "red light"]):
        return (
            "**Demo VOC readout:** Filter maintenance is the single clearest friction point in the mocked FlexStyle social set. "
            "The strongest negative posts are not saying the tool performs badly — they are saying the **filter door is hard to remove** and the **cleaning motion is not intuitive**. "
            "That turns a solvable maintenance step into a perceived reliability problem. The highest-signal quote is: “Every comment is about the filter door because nobody realizes you have to twist and lift in one motion.”"
        )
    if any(tok in q for tok in ["dyson", "airwrap", "compare", "comparison"]):
        return (
            "**Demo VOC readout:** In the mocked YouTube comparison set, FlexStyle wins on **value** and often gets described as “close enough on results.” "
            "Dyson still wins on **premium feel**, polish, and attachment perception. The actionable gap is not styling performance alone — it is the combination of **maintenance clarity + premium experience cues**."
        )
    if any(tok in q for tok in ["viral", "trend", "spike", "blowing up"]):
        return (
            "**Demo VOC readout:** The newest viral surge is being driven by a YouTube Short plus a Reddit thread that both center on the same theme: **filter-door friction and cleaning confusion**. "
            "The posts are spreading because the comments became troubleshooting hubs. That means the opportunity is a fast education fix: packaging, quick-start maintenance visuals, and creator-safe how-to content."
        )
    return (
        "**Demo VOC readout:** FlexStyle is winning the social conversation on **value and styling results**, but it is leaking satisfaction on **filter cleaning clarity, filter-door removal, and premium feel versus Dyson**. "
        "The mocked experience is designed to show how Meltwater retrieval plus your own UI could turn raw comments into product-ready decisions."
    )


def _render_social_listening_tab():
    demo = _social_demo_payload()
    posts = demo["posts"]
    detractors = demo["detractors"]
    delighters = demo["delighters"]
    viral = demo["viral"]
    top_comments = demo["top_comments"]
    compare = demo["compare"]
    metrics = demo["metrics"]
    st.markdown("""
    <style>
    .social-demo-hero{position:relative;overflow:hidden;border:1px solid rgba(79,70,229,.18);border-radius:24px;padding:22px 24px 20px;background:linear-gradient(180deg,#ffffff 0%,#f8faff 100%);color:var(--navy);box-shadow:var(--shadow-sm);margin-bottom:14px;}
    .social-demo-hero::before{content:"";position:absolute;inset:auto -48px -68px auto;width:220px;height:220px;background:radial-gradient(circle,rgba(79,70,229,.10),rgba(79,70,229,0));filter:blur(10px);}
    .social-demo-title{font-size:24px;font-weight:900;letter-spacing:-.03em;position:relative;z-index:1;color:var(--navy);}
    .social-demo-sub{font-size:13px;line-height:1.6;color:var(--slate-600);max-width:900px;position:relative;z-index:1;margin-top:5px;}
    .social-demo-note{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:14px 16px;box-shadow:var(--shadow-xs);margin-bottom:12px;}
    .social-demo-note b{color:var(--navy);}
    .social-demo-stat{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:14px 16px;box-shadow:var(--shadow-xs);min-height:104px;}
    .social-demo-stat .label{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--slate-500);font-weight:700;}
    .social-demo-stat .value{font-size:26px;font-weight:900;letter-spacing:-.04em;color:var(--navy);margin-top:4px;}
    .social-demo-stat .sub{font-size:12px;color:var(--slate-600);line-height:1.45;margin-top:3px;}
    .social-demo-stat.orange{border-color:rgba(249,115,22,.22);background:linear-gradient(180deg,#fff8f1 0%,#ffffff 100%);}
    .social-demo-stat.indigo{border-color:rgba(79,70,229,.18);background:linear-gradient(180deg,#f8f7ff 0%,#ffffff 100%);}
    .social-demo-stat.green{border-color:rgba(5,150,105,.18);background:linear-gradient(180deg,#f2fcf7 0%,#ffffff 100%);}
    .social-demo-stat.red{border-color:rgba(220,38,38,.18);background:linear-gradient(180deg,#fff5f5 0%,#ffffff 100%);}
    .social-voice-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:16px 16px 14px;box-shadow:var(--shadow-xs);height:100%;}
    .social-kicker{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;font-weight:800;color:var(--accent-strong);margin-bottom:5px;}
    .social-comment-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:16px 16px 14px;box-shadow:var(--shadow-xs);margin-bottom:10px;}
    .social-comment-title{font-size:15px;font-weight:800;color:var(--navy);line-height:1.35;letter-spacing:-.02em;}
    .social-comment-meta{font-size:11.5px;color:var(--slate-500);margin-top:4px;}
    .social-snippet{font-size:13.5px;color:var(--navy);line-height:1.55;margin-top:10px;}
    .social-comment-quote{margin-top:10px;padding:10px 12px;border-radius:14px;background:rgba(79,70,229,.06);border:1px solid rgba(79,70,229,.14);font-size:12.8px;line-height:1.55;color:var(--navy);}
    .social-comment-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:11px;font-size:12.2px;color:var(--slate-600);}
    .social-platform-chip{display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:999px;font-size:11px;font-weight:800;letter-spacing:.03em;text-transform:uppercase;border:1px solid rgba(148,163,184,.18);margin-right:4px;margin-bottom:4px;}
    .social-platform-chip.reddit{background:#fff7ed;border-color:#fdba74;color:#9a3412;}
    .social-platform-chip.youtube{background:#fef2f2;border-color:#fca5a5;color:#b91c1c;}
    .social-platform-chip.instagram{background:#fdf4ff;border-color:#e9d5ff;color:#7c3aed;}
    .social-platform-chip.generic{background:#f8fafc;border-color:var(--border);color:var(--slate-600);}
    .social-signal-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid rgba(249,115,22,.18);border-radius:18px;padding:15px 16px 14px;box-shadow:var(--shadow-xs);height:100%;}
    .social-signal-score{font-size:26px;font-weight:900;letter-spacing:-.04em;color:#ea580c;margin-top:4px;}
    @media(max-width:900px){.social-comment-grid{grid-template-columns:1fr;}}
    </style>
    """, unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Social Listening Beta</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Mocked Meltwater-style experience for external voice-of-consumer. Built so the team can access social insights even before any reviews are uploaded.</div>", unsafe_allow_html=True)
    st.markdown("""<div class='social-demo-hero'>
      <span class='beta-chip'>Beta feature</span>
      <span class='beta-chip'>No reviews required</span>
      <span class='beta-chip'>FlexStyle demo</span>
      <div class='social-demo-title'>📣 FlexStyle Social Listening · Mocked Meltwater Draft</div>
      <div class='social-demo-sub'>This view is intentionally preloaded with a <b>FlexStyle</b> demo scenario so a teammate can open the social feature before there is any review file to upload. The comments, engagement, and viral posts below are <b>mocked</b> to demonstrate the future Meltwater-powered UX: top comments, VOC themes, viral detection, YouTube vs Dyson insights, and maintenance friction around filter cleaning.</div>
    </div>""", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns([2.3, 1.25, 1.0, 1.0])
    product_name = c1.text_input("Product name or question", value=st.session_state.get("social_demo_product", "FlexStyle"), key="social_demo_product", placeholder="e.g. FlexStyle")
    sources = c2.multiselect("Sources", ["Reddit", "YouTube", "Instagram"], default=["Reddit", "YouTube", "Instagram"], key="social_demo_sources")
    range_choice = c3.selectbox("Date range", ["7d", "30d", "90d", "Custom"], index=1, key="social_demo_range")
    region = c4.selectbox("Region", ["Global", "US", "UK", "EU"], index=0, key="social_demo_region")
    if range_choice == "Custom":
        d1, d2 = st.columns(2)
        start_date = d1.date_input("Start date", value=date.today() - timedelta(days=30), key="social_demo_start")
        end_date = d2.date_input("End date", value=date.today(), key="social_demo_end")
    else:
        start_date = date.today() - timedelta(days={"7d": 7, "30d": 30, "90d": 90}[range_choice])
        end_date = date.today()
    run_cols = st.columns([1.1, 4.2])
    if run_cols[0].button("Analyze demo", type="primary", use_container_width=True, key="social_demo_refresh"):
        st.toast("FlexStyle mocked social listening refreshed.")
    run_cols[1].caption("Current Beta shows a mocked FlexStyle scenario so the social tab is usable before live Meltwater wiring is complete.")

    query_text = _social_demo_query(product_name or "FlexStyle")
    st.markdown(f"""<div class='social-demo-note'>
      <b>Meltwater query preview</b><br>
      Product / question: <b>{_esc(product_name or 'FlexStyle')}</b><br>
      Query used: <code>{_esc(query_text)}</code><br>
      Sources: <b>{_esc(', '.join(sources) if sources else 'All')}</b> · Date range: <b>{_esc(f'{start_date} → {end_date}')}</b> · Region: <b>{_esc(region)}</b><br>
      <span class='small-muted'>Demo note: the charts and comments below are mocked around FlexStyle so the team can preview the exact UX, VOC story, and engagement framing before live API integration.</span>
    </div>""", unsafe_allow_html=True)

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        _render_social_metric_card("Mentions in scope", metrics["mentions"], "Mocked consumer mentions across Reddit, YouTube, and Instagram.", accent="indigo")
    with m2:
        _render_social_metric_card("Positive / mixed-positive", metrics["positive"], "Value + styling results carry the positive side of the conversation.", accent="green")
    with m3:
        _render_social_metric_card("New viral posts", metrics["viral_posts"], "Two videos and two threads are driving the newest discussion spike.", accent="orange")
    with m4:
        _render_social_metric_card("Risk level", metrics["risk"], "Complaint density is highest around filter cleaning and filter-door removal.", accent="red")

    insight_cols = st.columns(4)
    insight_cards = [
        ("Key Insight", "FlexStyle is winning on **value + visible styling results**, but the social conversation keeps turning into a maintenance tutorial whenever the filter light appears."),
        ("Biggest Risk", "The phrase cluster around **‘filter door is too hard to remove’** is the strongest negative signal. People do not frame it as a defect at first — then it becomes one in perception."),
        ("Biggest Driver", "The strongest proof point is still **results close to Dyson for less money**. That theme dominates creator videos and positive Instagram comments."),
        ("Immediate Opportunity", "A sharper education layer — pack-in visual, quick-start maintenance card, creator-safe tutorial, or QR-linked filter clean video — would likely neutralize a meaningful share of negative commentary."),
    ]
    for col, (title, body) in zip(insight_cols, insight_cards):
        with col:
            st.markdown(f"""<div class='social-voice-card'>
              <div class='social-kicker'>{_esc(title)}</div>
              <div style='font-size:13px;line-height:1.58;color:var(--navy);'>{body}</div>
            </div>""", unsafe_allow_html=True)

    tabs = st.tabs(["Executive VOC", "Top Comments", "New Viral Posts", "FlexStyle vs Dyson", "Explorer + Chat"])

    with tabs[0]:
        v1, v2 = st.columns([1.3, 1])
        with v1:
            trend_df = _social_demo_trend(start_date, end_date)
            fig_mentions = go.Figure()
            fig_mentions.add_trace(go.Scatter(x=trend_df["date"], y=trend_df["mentions"], mode="lines+markers", name="Mentions", line=dict(width=3, color="#6366f1"), marker=dict(size=7)))
            fig_mentions.add_trace(go.Bar(x=trend_df["date"], y=trend_df["negative_share"], name="Negative share %", opacity=0.30, marker_color="#f97316", yaxis="y2"))
            fig_mentions.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter", yaxis_title="Mentions", yaxis2=dict(title="Negative share %", overlaying="y", side="right", rangemode="tozero"), legend=dict(orientation="h", y=1.06, x=0))
            fig_mentions = _sw_style_fig(fig_mentions)
            _show_plotly(fig_mentions)
        with v2:
            platform_breakdown = posts.groupby("platform", as_index=False).agg(Posts=("headline", "count"), Engagement=("engagement", "sum")).sort_values("Engagement", ascending=False)
            fig_plat = px.bar(platform_breakdown, x="platform", y="Engagement", text="Posts")
            fig_plat.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter")
            fig_plat = _sw_style_fig(fig_plat)
            _show_plotly(fig_plat)
        t1, t2 = st.columns(2)
        with t1:
            st.markdown("**🔴 Top detractors in the mocked VOC**")
            st.dataframe(detractors, use_container_width=True, hide_index=True, height=250)
        with t2:
            st.markdown("**🟢 Top delighters in the mocked VOC**")
            st.dataframe(delighters, use_container_width=True, hide_index=True, height=250)
        st.markdown("""<div class='social-demo-note'>
          <b>Voice of the consumer summary</b><br>
          1. People do not come to social first to complain about styling results — they come to compare <b>FlexStyle vs Dyson</b> and then stumble into a maintenance conversation.<br>
          2. The highest-signal negative phrase is not “bad product,” it is <b>“the filter door is too hard to remove”</b> or <b>“cleaning the filter is not intuitive.”</b><br>
          3. The best creator proof is still <b>Dyson-like results at a friendlier price</b>, which means the product story is strong enough — the education layer just needs to catch up.
        </div>""", unsafe_allow_html=True)

    with tabs[1]:
        st.markdown("**Top comments with engagement**")
        highlight_rows = posts.sort_values(["top_comment_likes", "engagement"], ascending=[False, False]).head(4)
        for _, row in highlight_rows.iterrows():
            _render_social_post_card(row, highlight="Top comment")
        st.markdown("**Top comments table**")
        st.dataframe(top_comments, use_container_width=True, hide_index=True, height=320)

    with tabs[2]:
        st.markdown("**Newest viral posts spotted in the mocked social set**")
        vcols = st.columns(2)
        for col, (_, row) in zip(vcols * 2, viral.iterrows()):
            with col:
                st.markdown(f"""<div class='social-signal-card'>
                  <div class='social-kicker'>{_esc(row.get('viral_status'))}</div>
                  <div style='font-size:15px;font-weight:800;color:var(--navy);line-height:1.35;'>{_esc(row.get('headline'))}</div>
                  <div class='social-signal-score'>+{int(row.get('velocity_pct') or 0)}%</div>
                  <div style='font-size:12px;color:var(--slate-500);line-height:1.5;'>
                    {_esc(row.get('platform'))} · {_esc(row.get('source'))}<br>
                    {int(row.get('engagement') or 0):,} engagement · {int(row.get('comments') or 0):,} comments<br>
                    Theme: <b>{_esc(row.get('theme'))}</b>
                  </div>
                </div>""", unsafe_allow_html=True)
        st.markdown("<div style='height:.35rem'></div>", unsafe_allow_html=True)
        st.markdown("**Videos with high-signal top comments**")
        video_rows = posts[posts["platform"].eq("YouTube")].sort_values(["views", "top_comment_likes"], ascending=[False, False]).head(3)
        for _, row in video_rows.iterrows():
            _render_social_post_card(row, highlight="Video signal")

    with tabs[3]:
        st.markdown("**FlexStyle vs Dyson — mocked YouTube VOC**")
        st.dataframe(compare, use_container_width=True, hide_index=True, height=220)
        c_left, c_right = st.columns(2)
        with c_left:
            st.markdown("""<div class='social-voice-card'>
              <div class='social-kicker'>What FlexStyle wins</div>
              <div style='font-size:13px;line-height:1.58;color:var(--navy);'>
                • “Closer to Dyson than I expected for the price.”<br>
                • “Still gives the blowout look without the Dyson tax.”<br>
                • “Once I figured out the attachments, it replaced multiple tools for me.”
              </div>
            </div>""", unsafe_allow_html=True)
        with c_right:
            st.markdown("""<div class='social-voice-card'>
              <div class='social-kicker'>Where Dyson still leads</div>
              <div style='font-size:13px;line-height:1.58;color:var(--navy);'>
                • “Dyson still feels more polished and premium.”<br>
                • “The FlexStyle filter clean story is harder to understand from the comments.”<br>
                • “Maintenance friction is louder in FlexStyle tutorials than in Dyson tutorials.”
              </div>
            </div>""", unsafe_allow_html=True)
        st.markdown("""<div class='social-demo-note'>
          <b>VOC-backed takeaway</b><br>
          In this mocked draft, FlexStyle does <b>not</b> lose on visible styling performance. It loses emotional ground when comparison shoppers hit the maintenance story and read comments about the filter door being hard to remove. That is a fixable gap because it sounds like an education + usability issue, not a fundamental results issue.
        </div>""", unsafe_allow_html=True)

    with tabs[4]:
        st.markdown("**Explorer**")
        ec1, ec2 = st.columns([1.2, 1.2])
        platform_filter = ec1.multiselect("Filter platform", sorted(posts["platform"].unique().tolist()), default=sorted(posts["platform"].unique().tolist()), key="social_explorer_platform")
        sentiment_filter = ec2.multiselect("Filter sentiment", sorted(posts["sentiment"].unique().tolist()), default=sorted(posts["sentiment"].unique().tolist()), key="social_explorer_sentiment")
        view_df = posts[posts["platform"].isin(platform_filter) & posts["sentiment"].isin(sentiment_filter)].copy()
        if view_df.empty:
            st.info("No mocked posts match the current explorer filters.")
        else:
            preview = view_df[["platform", "content_type", "headline", "source", "submission_date", "sentiment", "views", "likes", "comments", "engagement", "theme"]].rename(columns={"platform": "Platform", "content_type": "Type", "headline": "Post / Video", "source": "Source", "submission_date": "Date", "sentiment": "Sentiment", "views": "Views", "likes": "Likes", "comments": "Comments", "engagement": "Engagement", "theme": "Theme"})
            st.dataframe(preview, use_container_width=True, hide_index=True, height=280)
        st.markdown("**Ask the mocked social assistant**")
        st.caption("This is a demo-only answer layer. It reads like the future Meltwater + AI experience, but the responses are intentionally mocked to match the FlexStyle data above.")
        st.session_state.setdefault("social_demo_chat_history", [{"role": "assistant", "content": "Ask about filter cleaning, viral posts, FlexStyle vs Dyson, or what the voice of the consumer is saying."}])
        qcols = st.columns(3)
        prompt_map = {
            "Why is FlexStyle trending?": "Why is FlexStyle trending right now?",
            "What is the filter-door issue?": "What is the filter door issue?",
            "How does FlexStyle compare vs Dyson on YouTube?": "How does FlexStyle compare vs Dyson on YouTube?",
        }
        clicked_prompt = None
        for col, label in zip(qcols, prompt_map.keys()):
            if col.button(label, use_container_width=True, key=f"social_prompt_{_slugify(label)}"):
                clicked_prompt = prompt_map[label]
        user_q = st.chat_input("Ask about the mocked social VOC…", key="social_demo_chat_input")
        social_prompt = clicked_prompt or user_q
        if social_prompt:
            st.session_state["social_demo_chat_history"].append({"role": "user", "content": social_prompt})
            st.session_state["social_demo_chat_history"].append({"role": "assistant", "content": _social_demo_answer(social_prompt)})
        for msg in st.session_state.get("social_demo_chat_history", [])[-6:]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════