"""Social Listening Beta tab — placeholder Meltwater-style experience.

This module mirrors the future Meltwater/OpenAI flow while keeping the actual
content in polished demo mode for now.
"""
from __future__ import annotations

import html as _html_mod
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

MELTWATER_BASE = "https://api.meltwater.com/v3"
_DEMO_SEARCHES: dict[int, str] = {}
_NEXT_SEARCH_ID = 1000


def _esc(value: Any) -> str:
    return _html_mod.escape(str(value or ""))


def _safe_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        missing = pd.isna(value)
    except Exception:
        missing = False
    if isinstance(missing, bool) and missing:
        return default
    text = str(value).strip()
    return default if text.lower() in {"nan", "none", "null", "<na>"} else text


def _slugify(text: str, fallback: str = "custom") -> str:
    compact = re.sub(r"[^a-zA-Z0-9]+", "_", _safe_text(text).lower())
    compact = re.sub(r"_+", "_", compact).strip("_") or fallback
    return ("prompt_" + compact if compact[0].isdigit() else compact)[:64]


def _sw_style_fig(fig: go.Figure) -> go.Figure:
    grid = "rgba(148,163,184,0.18)"
    trace_count = len(getattr(fig, "data", []) or [])
    legend_cfg = (
        dict(orientation="v", y=1.0, x=1.01, xanchor="left", yanchor="top",
             bgcolor="rgba(255,255,255,0.86)", bordercolor="rgba(148,163,184,0.22)", borderwidth=1,
             font=dict(size=11))
        if trace_count > 3 else
        dict(orientation="h", y=1.12, x=0, xanchor="left", yanchor="bottom",
             bgcolor="rgba(255,255,255,0.84)", bordercolor="rgba(148,163,184,0.18)", borderwidth=1,
             font=dict(size=11))
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, system-ui, sans-serif", size=12),
        margin=dict(l=26, r=108 if trace_count > 3 else 18, t=56 if trace_count > 3 else 64, b=44),
        title=dict(x=0, xanchor="left", font=dict(size=15)),
        legend=legend_cfg,
        hoverlabel=dict(font=dict(family="Inter, system-ui, sans-serif", size=12)),
    )
    fig.update_xaxes(gridcolor=grid, zerolinecolor=grid, automargin=True, title_standoff=10)
    fig.update_yaxes(gridcolor=grid, zerolinecolor=grid, automargin=True, title_standoff=10)
    return fig


def _show_plotly(fig: go.Figure) -> None:
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
            "displayModeBar": False,
            "responsive": True,
            "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d", "toggleSpikelines"],
        },
    )


def _build_social_beta_query(raw_query: str) -> str:
    text = _safe_text(raw_query).strip() or "Shark FlexStyle"
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


def _social_demo_payload() -> dict[str, Any]:
    posts = pd.DataFrame([
        dict(rank=1, platform="Reddit", content_type="Thread", headline="FlexStyle filter door is way harder to remove than it should be", author="u/blowoutscience", source="r/HaircareScience", submission_date="2026-04-06", sentiment="Negative", views=86000, likes=1430, comments=243, shares=114, engagement=1787, velocity_pct=189, viral_status="New viral complaint", theme="Filter door / cleaning", voc_takeaway="Consumers understand there is a filter, but not the door motion or cleaning cadence.", snippet="I love the styling results, but the filter door feels like I'm going to break it every time I try to open it.", top_comment="I thought I was going to snap the door the first week — it's not intuitive at all and the arrows are too subtle.", top_comment_author="u/heatprotectplease", top_comment_likes=612, classification="ORGANIC", cause="Complaint Cluster", improvement_theme="Clearer cleaning visuals", improvement_priority="high", improvement_trend="rising", praise_theme="", hack_use_case=""),
        dict(rank=2, platform="YouTube", content_type="Video", headline="FlexStyle vs Dyson Airwrap after 60 days", author="Blowout Lab", source="YouTube · Blowout Lab", submission_date="2026-04-05", sentiment="Mixed", views=186000, likes=9800, comments=1304, shares=640, engagement=11744, velocity_pct=121, viral_status="Rising comparison video", theme="Value vs Dyson", voc_takeaway="Value wins the click, but premium feel and maintenance clarity still favor Dyson in the comments.", snippet="After two months I still think FlexStyle wins on value, but Dyson feels more premium and polished.", top_comment="FlexStyle is the better value, but Dyson still wins on polish and attachments. Cleaning the FlexStyle filter is way less intuitive.", top_comment_author="@styledbyjen", top_comment_likes=1904, classification="ORGANIC", cause="Creator Organic", improvement_theme="More premium-feeling attachment fit", improvement_priority="medium", improvement_trend="stable", praise_theme="Value + visible payoff", hack_use_case=""),
        dict(rank=3, platform="YouTube", content_type="Tutorial", headline="Why your FlexStyle is flashing red — clean the filter first", author="Clean Girl Routine", source="YouTube · Clean Girl Routine", submission_date="2026-04-04", sentiment="Mixed", views=94000, likes=4200, comments=487, shares=211, engagement=4898, velocity_pct=96, viral_status="Service moment", theme="Filter cleaning", voc_takeaway="Education resolves the issue quickly, but customers think the warning means something is broken.", snippet="Most of the panic comments were actually clogged filter issues, not a dead unit.", top_comment="Why is the filter door so hard to remove? I had to pause and rewind twice.", top_comment_author="@curlsbeforecoffee", top_comment_likes=843, classification="ORGANIC", cause="Creator Organic", improvement_theme="Explain the maintenance warning sooner", improvement_priority="high", improvement_trend="rising", praise_theme="", hack_use_case=""),
        dict(rank=4, platform="Instagram", content_type="Reel", headline="FlexStyle curls still held better than expected", author="BeautyByMia", source="Instagram · @beautybymia", submission_date="2026-04-04", sentiment="Positive", views=312000, likes=21000, comments=604, shares=1700, engagement=23304, velocity_pct=78, viral_status="Positive creator proof", theme="Results / value", voc_takeaway="When results show up visually, the price-value story lands immediately.", snippet="I switched from Dyson because the curls still hold and the price makes way more sense.", top_comment="The results are giving Airwrap but the price is way easier to justify.", top_comment_author="@glowmode", top_comment_likes=2455, classification="ORGANIC", cause="Creator Organic", improvement_theme="", improvement_priority="medium", improvement_trend="stable", praise_theme="Visible results", hack_use_case=""),
        dict(rank=5, platform="Reddit", content_type="Thread", headline="FlexStyle owners — worth switching from Dyson?", author="u/volumequest", source="r/DysonAirwrap", submission_date="2026-04-03", sentiment="Mixed", views=64000, likes=978, comments=189, shares=92, engagement=1259, velocity_pct=64, viral_status="Comparison debate", theme="Premium feel vs value", voc_takeaway="Consumers do not say FlexStyle is bad — they say it needs clearer maintenance and a more premium-feeling experience.", snippet="Performance is surprisingly close, but Dyson feels more premium and the FlexStyle maintenance steps are not obvious.", top_comment="Performance is surprisingly close, but Dyson feels more premium and the FlexStyle maintenance steps are not obvious.", top_comment_author="u/hottoolhedge", top_comment_likes=521, classification="ORGANIC", cause="Creator Organic", improvement_theme="More premium-feeling attachment fit", improvement_priority="medium", improvement_trend="stable", praise_theme="Closer to premium on outcome", hack_use_case=""),
        dict(rank=6, platform="YouTube", content_type="Short", headline="Late-night blowout test: FlexStyle noise + filter clean reaction", author="Late Night Blowout", source="YouTube · Late Night Blowout", submission_date="2026-04-02", sentiment="Mixed", views=411000, likes=18000, comments=1827, shares=2200, engagement=22027, velocity_pct=214, viral_status="New viral video", theme="Comment-led virality", voc_takeaway="The post is spreading because the comments became a community troubleshooting thread, not because the creator disliked the product.", snippet="This short is blowing up because everyone in the comments is asking why the filter light came on so fast.", top_comment="Every comment is about the filter door because nobody realizes you have to twist and lift in one motion.", top_comment_author="@heatwavesarah", top_comment_likes=3288, classification="ORGANIC", cause="Complaint Cluster", improvement_theme="Explain the maintenance warning sooner", improvement_priority="high", improvement_trend="rising", praise_theme="", hack_use_case=""),
        dict(rank=7, platform="Instagram", content_type="Carousel", headline="FlexStyle cleaning checklist everyone should save", author="The Blowout Edit", source="Instagram · @theblowoutedit", submission_date="2026-04-01", sentiment="Positive", views=129000, likes=8200, comments=212, shares=540, engagement=8952, velocity_pct=55, viral_status="Helpful maintenance post", theme="Maintenance education", voc_takeaway="Once customers see the steps, the maintenance story feels manageable instead of scary.", snippet="Wish this came in the box — the cleaning step is easy once you see it, but not before.", top_comment="Wish this came in the box — the cleaning step is easy once you see it, but not before.", top_comment_author="@blowdryclub", top_comment_likes=611, classification="ORGANIC", cause="Creator Organic", improvement_theme="", improvement_priority="medium", improvement_trend="stable", praise_theme="Guided confidence", hack_use_case=""),
        dict(rank=8, platform="Reddit", content_type="Thread", headline="FlexStyle red light after three weeks?", author="u/bouncylayers", source="r/FlexStyle", submission_date="2026-03-31", sentiment="Negative", views=58000, likes=702, comments=154, shares=60, engagement=916, velocity_pct=88, viral_status="Emerging maintenance confusion", theme="Filter warning / care", voc_takeaway="Customers can self-resolve this, but the first read is still 'my tool is failing'.", snippet="Mine was fine after cleaning, but the filter door still feels fiddly every single time.", top_comment="Mine was fine after cleaning, but the filter door still feels fiddly every single time.", top_comment_author="u/hairdaypanic", top_comment_likes=374, classification="ORGANIC", cause="Complaint Cluster", improvement_theme="Explain the maintenance warning sooner", improvement_priority="high", improvement_trend="rising", praise_theme="", hack_use_case=""),
        dict(rank=9, platform="TikTok", content_type="Video", headline="#ad first week with FlexStyle", author="@creatorontherise", source="TikTok · @creatorontherise", submission_date="2026-04-05", sentiment="Positive", views=160000, likes=7600, comments=408, shares=812, engagement=9210, velocity_pct=71, viral_status="Seeded creator post", theme="Seeded creator proof", voc_takeaway="Seeded content helps awareness, but unscripted social posts still teach the team more about product reality.", snippet="Gifted by the brand, but I genuinely like how quickly it gets me to a salon-looking blowout.", top_comment="Thanks for saying it was gifted — the result still looks good.", top_comment_author="@honestviewer", top_comment_likes=460, classification="INCENTIVIZED_EXPLICIT", cause="Paid/Seeded", improvement_theme="", improvement_priority="medium", improvement_trend="stable", praise_theme="Fast first impression", hack_use_case=""),
        dict(rank=10, platform="Instagram", content_type="Reel", headline="Use my code if you're considering FlexStyle", author="@dealdrivenbeauty", source="Instagram · @dealdrivenbeauty", submission_date="2026-04-05", sentiment="Positive", views=128000, likes=7100, comments=362, shares=414, engagement=8640, velocity_pct=58, viral_status="Creator wave", theme="Inferred seeding", voc_takeaway="The audience response is decent, but the comment quality is shallower than organic tutorials or Reddit comparisons.", snippet="No formal disclosure in the caption, but the post reads like the rest of the seeding wave and ends with a promo code.", top_comment="This sounds exactly like the other three videos I saw today.", top_comment_author="@skepticalviewer", top_comment_likes=318, classification="INCENTIVIZED_INFERRED", cause="Paid/Seeded", improvement_theme="", improvement_priority="medium", improvement_trend="stable", praise_theme="Creator promise", hack_use_case=""),
        dict(rank=11, platform="Reddit", content_type="Thread", headline="Wish FlexStyle shipped with a clearer quick-start guide", author="u/setupwish", source="r/BuyItForLifeMaybe", submission_date="2026-04-01", sentiment="Negative", views=42000, likes=812, comments=117, shares=41, engagement=1105, velocity_pct=55, viral_status="Feature request thread", theme="Onboarding clarity", voc_takeaway="Users are not rejecting the product; they are asking for a better first-run learning curve.", snippet="The product is good once you get it, but the pack-in guidance should have made the maintenance loop obvious.", top_comment="A single quick-start card would have solved 80% of my confusion.", top_comment_author="u/visualinstructions", top_comment_likes=287, classification="ORGANIC", cause="Complaint Cluster", improvement_theme="Onboarding clarity", improvement_priority="high", improvement_trend="rising", praise_theme="", hack_use_case=""),
        dict(rank=12, platform="TikTok", content_type="Video", headline="Wait, FlexStyle also works for second-day bang refresh?", author="@nobodytoldme", source="TikTok · @nobodytoldme", submission_date="2026-04-01", sentiment="Positive", views=98000, likes=5200, comments=310, shares=504, engagement=6130, velocity_pct=74, viral_status="Discovery post", theme="Versatility discovery", voc_takeaway="Consumers love saving unexpected use cases because they make the product feel smarter and more versatile.", snippet="The concentrator plus cool shot is my second-day bang rescue move now.", top_comment="Okay this is the first tip that made me save the video.", top_comment_author="@saveditlater", top_comment_likes=562, classification="ORGANIC", cause="Creator Organic", improvement_theme="", improvement_priority="medium", improvement_trend="stable", praise_theme="Versatile use case", hack_use_case="Second-day bang refresh"),
    ])
    top_comments = (
        posts[["platform", "content_type", "headline", "source", "top_comment_author", "top_comment", "top_comment_likes", "engagement", "theme", "voc_takeaway"]]
        .rename(columns={"top_comment_author": "Author", "top_comment": "Comment", "top_comment_likes": "Comment Likes", "engagement": "Post Engagement", "theme": "Theme", "voc_takeaway": "VOC Takeaway", "headline": "Post / Video"})
        .sort_values(["Comment Likes", "Post Engagement"], ascending=[False, False])
        .reset_index(drop=True)
    )
    detractors = pd.DataFrame([
        dict(Theme="Clearer cleaning visuals", Mentions=3, Trend="rising", VOC="Consumers feel like they might break the door when they try to open it."),
        dict(Theme="Explain the maintenance warning sooner", Mentions=3, Trend="rising", VOC="The warning light reads like failure before it reads like maintenance."),
        dict(Theme="Onboarding clarity", Mentions=1, Trend="rising", VOC="A better first-run guide would cut a lot of confusion."),
        dict(Theme="More premium-feeling attachment fit", Mentions=2, Trend="stable", VOC="Comparison shoppers still use Dyson as the benchmark for polish."),
    ])
    delighters = pd.DataFrame([
        dict(Theme="Visible results", Mentions=1, Share="17%", VOC="The results are giving Airwrap but the price is way easier to justify."),
        dict(Theme="Value + visible payoff", Mentions=1, Share="17%", VOC="FlexStyle is the better value, but still delivers a real styling payoff."),
        dict(Theme="Guided confidence", Mentions=1, Share="17%", VOC="Once customers see the steps, the maintenance story feels manageable instead of scary."),
        dict(Theme="Fast first impression", Mentions=1, Share="17%", VOC="Gifted content helps awareness, especially when the results are immediate."),
        dict(Theme="Versatile use case", Mentions=1, Share="17%", VOC="Unexpected use cases make the product feel more ownable."),
    ])
    compare = pd.DataFrame([
        dict(Angle="Value / affordability", FlexStyle="Stronger", Dyson_Airwrap="Weaker", What_social_says="FlexStyle wins when creators show visible payoff and price in the same frame."),
        dict(Angle="Premium polish", FlexStyle="Needs work", Dyson_Airwrap="Leads", What_social_says="Dyson still owns the emotional polish conversation."),
        dict(Angle="Onboarding clarity", FlexStyle="Needs work", Dyson_Airwrap="Less friction", What_social_says="Comment sections keep teaching people the maintenance flow."),
    ])
    viral = posts.sort_values(["velocity_pct", "engagement"], ascending=[False, False]).head(4).reset_index(drop=True)
    metrics = {"mentions": f"{len(posts):,}", "positive": "50%", "viral_posts": "4", "risk": "Medium", "organic_share": "83%"}
    return {"posts": posts, "top_comments": top_comments, "detractors": detractors, "delighters": delighters, "viral": viral, "compare": compare, "metrics": metrics}


def _social_demo_query(product_name: str) -> str:
    raw = _safe_text(product_name) or "FlexStyle"
    compact = re.sub(r"[^A-Za-z0-9]+", "", raw)
    return f'(\"{raw}\" OR \"{compact}\" OR \"Shark {raw}\" OR \"#{compact}\") AND (review OR comments OR tutorial OR comparison OR complaint OR tip OR creator)'


def _social_demo_trend(start_date: date, end_date: date) -> pd.DataFrame:
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


def _render_social_metric_card(label: str, value: str, sub: str, accent: str = "indigo") -> None:
    st.markdown(f"""<div class='social-demo-stat {accent}'>
      <div class='label'>{_esc(label)}</div>
      <div class='value'>{_esc(value)}</div>
      <div class='sub'>{_esc(sub)}</div>
    </div>""", unsafe_allow_html=True)


def _render_social_post_card(row: pd.Series, *, highlight: str = "") -> None:
    platform_cls = {"Reddit": "reddit", "YouTube": "youtube", "Instagram": "instagram", "TikTok": "tiktok"}.get(_safe_text(row.get("platform")), "generic")
    engagement_bits = [
        f"{int(pd.to_numeric(row.get('views'), errors='coerce') or 0):,} reach",
        f"{int(pd.to_numeric(row.get('likes'), errors='coerce') or 0):,} likes",
        f"{int(pd.to_numeric(row.get('comments'), errors='coerce') or 0):,} comments",
        f"velocity +{int(pd.to_numeric(row.get('velocity_pct'), errors='coerce') or 0)}%",
    ]
    tag_html = f"<span class='social-platform-chip {platform_cls}'>{_esc(row.get('platform'))}</span>"
    if _safe_text(row.get("classification")):
        tag_html += f" <span class='social-platform-chip generic'>{_esc(row.get('classification'))}</span>"
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


def _demo_docs(product_name: str) -> list[dict[str, Any]]:
    demo = _social_demo_payload()
    posts = demo["posts"].copy()
    product = _safe_text(product_name) or "FlexStyle"
    docs = []
    for _, row in posts.iterrows():
        sentiment = _safe_text(row["sentiment"]).lower()
        if sentiment == "mixed":
            sentiment = "neutral"
        docs.append({
            "source": {"source_type": row["platform"], "source_name": row["source"]},
            "sentiment": sentiment,
            "metrics": {"engagement": int(row["engagement"]), "reach": int(row["views"]), "likes": int(row["likes"]), "comments": int(row["comments"]), "shares": int(row["shares"]), "velocity_pct": int(row["velocity_pct"])},
            "published": f"{row['submission_date']}T12:00:00Z",
            "content": {"title": _safe_text(row['headline']).replace('FlexStyle', product), "body": _safe_text(row['snippet']).replace('FlexStyle', product)},
            "content_type": row["content_type"],
            "author": row["author"],
            "url": f"https://demo.social/{_slugify(product)}/{_slugify(row['headline'])}",
            "classification": row["classification"],
            "cause": row["cause"],
            "theme": row["theme"],
            "voc_takeaway": row["voc_takeaway"].replace('FlexStyle', product),
            "top_comment": _safe_text(row["top_comment"]).replace('FlexStyle', product),
            "top_comment_author": row["top_comment_author"],
            "top_comment_likes": int(row["top_comment_likes"]),
            "viral_status": row["viral_status"],
            "improvement_theme": row["improvement_theme"],
            "improvement_priority": row["improvement_priority"],
            "improvement_trend": row["improvement_trend"],
            "praise_theme": row["praise_theme"],
            "hack_use_case": row["hack_use_case"],
        })
    return docs


def mw_headers(key: str) -> dict[str, str]:
    return {"apikey": key, "Accept": "application/json", "Content-Type": "application/json"}


def create_temp_search(key: str, term: str) -> Optional[dict[str, Any]]:
    del key
    global _NEXT_SEARCH_ID
    _NEXT_SEARCH_ID += 1
    _DEMO_SEARCHES[_NEXT_SEARCH_ID] = _safe_text(term) or "FlexStyle"
    return {"id": _NEXT_SEARCH_ID, "name": f"[TMP] {_safe_text(term) or 'FlexStyle'} {datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"}


def delete_search(key: str, search_id: int) -> None:
    del key
    _DEMO_SEARCHES.pop(int(search_id), None)


def fetch_mentions(key: str, search_id: int, days: int, sort_by: str, sources: list[str], page_size: int = 50, sentiments: Optional[list[str]] = None) -> list[dict[str, Any]]:
    del key, days
    term = _DEMO_SEARCHES.get(int(search_id), "FlexStyle")
    docs = _demo_docs(term)
    source_filter = {s.lower() for s in sources or []}
    if source_filter:
        docs = [doc for doc in docs if _safe_text(doc["source"]["source_type"]).lower() in source_filter]
    sentiment_filter = {s.lower() for s in sentiments or []}
    if sentiment_filter:
        docs = [doc for doc in docs if _safe_text(doc["sentiment"]).lower() in sentiment_filter]
    if sort_by == "reach":
        docs.sort(key=lambda d: int(d["metrics"].get("reach", 0)), reverse=True)
    elif sort_by == "date":
        docs.sort(key=lambda d: _safe_text(d.get("published")), reverse=True)
    else:
        docs.sort(key=lambda d: int(d["metrics"].get("engagement", 0)), reverse=True)
    return docs[:page_size]


def doc_to_snippet(doc: dict[str, Any], max_body: int = 400) -> str:
    content = doc.get("content", {})
    metrics = doc.get("metrics", {})
    return (
        f"SOURCE: {doc.get('source', {}).get('source_type', 'unknown')} | "
        f"SENTIMENT: {doc.get('sentiment', 'unknown')} | "
        f"ENGAGEMENT: {metrics.get('engagement', 0)} | "
        f"REACH: {metrics.get('reach', 0)} | "
        f"DATE: {(doc.get('published') or '')[:10]}\n"
        f"TITLE: {content.get('title', '')}\n"
        f"BODY: {(content.get('body') or '')[:max_body]}\n"
        f"URL: {doc.get('url', '')}"
    )


def run_ai(openai_key: str, system: str, user: str, max_tokens: int = 2000) -> dict[str, Any]:
    del openai_key, system, user, max_tokens
    return {"mode": "demo_placeholder"}


_VIRAL_SYSTEM = """
You are a social media analyst. Given engagement-sorted mentions, identify spike events, likely cause of each spike, the cause breakdown of virality, and whether any spike is a negative cluster product insights should review.
Return only structured JSON in the future live path.
""".strip()

_PAID_SYSTEM = """
You are a media transparency analyst. Separate organic posts from explicitly and implicitly incentivized creator content.
Return only structured JSON in the future live path.
""".strip()

_LOVE_SYSTEM = """
You are a voice-of-customer analyst. Extract the specific features, attributes, or outcomes people praise most in positive social mentions.
Return only structured JSON in the future live path.
""".strip()

_IDEAS_SYSTEM = """
You are a product development analyst. Pull out feature requests, wish phrasing, comparative gaps, workarounds, and frustrations.
Return only structured JSON in the future live path.
""".strip()

_HACKS_SYSTEM = """
You are an innovation analyst scanning for unintended but positive product uses.
Classify each hack as a content opportunity, NPI signal, competitive edge, or safety flag.
Return only structured JSON in the future live path.
""".strip()


def analyze_viral(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    del key
    top = sorted(docs, key=lambda d: int(d["metrics"]["engagement"]), reverse=True)[:4]
    total = sum(int(d["metrics"]["engagement"]) for d in top) or 1
    causes = {label: 0 for label in ["Creator Organic", "Press/Editorial", "Complaint Cluster", "Paid/Seeded", "Unknown"]}
    events = []
    for doc in top:
        cause = _safe_text(doc.get("cause"), "Unknown")
        causes[cause] = causes.get(cause, 0) + int(doc["metrics"]["engagement"])
        events.append({
            "date": _safe_text(doc.get("published"))[:10],
            "title": " ".join(_safe_text(doc["content"]["title"]).split()[:12]),
            "cause": cause,
            "engagement": int(doc["metrics"]["engagement"]),
            "reach": int(doc["metrics"]["reach"]),
            "summary": _safe_text(doc.get("voc_takeaway")),
            "pi_flag": bool(cause == "Complaint Cluster"),
        })
    breakdown = {label: int(round((value / total) * 100)) for label, value in causes.items()}
    return {"spike_events": events, "cause_breakdown": breakdown, "overall_summary": f"{_safe_text(term) or 'FlexStyle'} is trending because creator proof and complaint-led troubleshooting are both pulling attention. The sharpest risk cluster is still the maintenance story, especially filter-door removal and the warning-light explanation. The opportunity is to turn that same social energy into clearer education content and cleaner onboarding."}


def analyze_paid(key: str, docs: list[dict[str, Any]]) -> dict[str, Any]:
    del key
    posts = []
    organic = []
    incentivized = []
    for doc in docs:
        classification = _safe_text(doc.get("classification"), "ORGANIC")
        posts.append({
            "url": _safe_text(doc.get("url")),
            "title": _safe_text(doc.get("content", {}).get("title"))[:60],
            "classification": classification,
            "reason": "Clear disclosure language" if classification == "INCENTIVIZED_EXPLICIT" else "Affiliate / wave-like creator language" if classification == "INCENTIVIZED_INFERRED" else "Reads like organic consumer or editorial content",
            "engagement": int(doc.get("metrics", {}).get("engagement", 0)),
            "source": _safe_text(doc.get("source", {}).get("source_type")),
        })
        (organic if classification == "ORGANIC" else incentivized).append(doc)
    organic_avg = int(round(sum(int(d["metrics"]["engagement"]) for d in organic) / max(len(organic), 1)))
    incent_avg = int(round(sum(int(d["metrics"]["engagement"]) for d in incentivized) / max(len(incentivized), 1)))
    top_organic = [{"url": d["url"], "title": d["content"]["title"], "engagement": int(d["metrics"]["engagement"]), "why_notable": d["voc_takeaway"]} for d in organic[:3]]
    return {"posts": posts, "stats": {"total": len(posts), "organic": len(organic), "incentivized_explicit": sum(1 for d in docs if d.get("classification") == "INCENTIVIZED_EXPLICIT"), "incentivized_inferred": sum(1 for d in docs if d.get("classification") == "INCENTIVIZED_INFERRED"), "organic_avg_engagement": organic_avg, "incentivized_avg_engagement": incent_avg}, "top_organic": top_organic, "summary": "Organic posts are still where the highest-signal product learning lives. Seeded content is helpful for awareness, but it teaches less than creator comparisons, troubleshooting threads, and honest saves / comments."}


def analyze_love(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    del key
    positive = [d for d in docs if _safe_text(d.get("sentiment")).lower() == "positive"]
    theme_map: dict[str, list[dict[str, Any]]] = {}
    for doc in positive:
        theme = _safe_text(doc.get("praise_theme"), "General satisfaction")
        theme_map.setdefault(theme, []).append(doc)
    praise = []
    hero_claims = []
    for theme, items in theme_map.items():
        best = max(items, key=lambda d: int(d["metrics"]["engagement"]))
        praise.append({"theme": theme, "mention_count": len(items), "share_of_positive_posts": int(round((len(items) / max(len(positive), 1)) * 100)), "best_quote": _safe_text(best.get("top_comment")) or _safe_text(best.get("content", {}).get("body"))[:100], "quote_source": _safe_text(best.get("source", {}).get("source_type")), "quote_engagement": int(best["metrics"]["engagement"])})
    for row in sorted(praise, key=lambda r: (-r["mention_count"], -r["quote_engagement"]))[:3]:
        hero_claims.append({"claim": row["theme"] if row["theme"] != "Visible results" else f"{_safe_text(term) or 'FlexStyle'} delivers visible payoff", "evidence": f"{row['theme']} shows up across {row['mention_count']} positive posts in the demo social set.", "engagement_weight": "high" if row["quote_engagement"] >= 9000 else "medium"})
    return {"praise_themes": sorted(praise, key=lambda r: (-r["mention_count"], -r["quote_engagement"])), "hero_claims": hero_claims, "summary": f"The positive conversation around {_safe_text(term) or 'FlexStyle'} is led by visible results, value, and confidence once consumers understand the maintenance steps. The best marketing territory is still outcome-led rather than spec-led. Social proof is strongest when the before / after payoff is obvious."}


def analyze_ideas(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    del key, term
    grouped: dict[str, list[dict[str, Any]]] = {}
    for doc in docs:
        theme = _safe_text(doc.get("improvement_theme"))
        if theme:
            grouped.setdefault(theme, []).append(doc)
    rows = []
    for theme, items in grouped.items():
        best = max(items, key=lambda d: int(d["metrics"]["engagement"]))
        rows.append({"theme": theme, "post_count": len(items), "trend": _safe_text(best.get("improvement_trend"), "stable"), "representative_quote": _safe_text(best.get("top_comment")), "quote_source": _safe_text(best.get("source", {}).get("source_type")), "quote_engagement": int(best["metrics"]["engagement"]), "product_implication": _safe_text(best.get("voc_takeaway")), "priority": _safe_text(best.get("improvement_priority"), "medium")})
    rows.sort(key=lambda r: ({"high": 0, "medium": 1, "low": 2}.get(r["priority"], 9), -r["post_count"], -r["quote_engagement"]))
    jira = [r["theme"] for r in rows[:3]]
    return {"improvement_themes": rows, "unmet_need_summary": "The social feedback says the biggest opportunity is not a dramatic new feature. It is a cleaner path from unboxing to confidence: better cleaning visuals, a clearer maintenance-warning story, and less emotional drag in comparison shopping.", "jira_candidates": jira}


def analyze_hacks(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    del key, term
    rows = []
    for doc in docs:
        use_case = _safe_text(doc.get("hack_use_case"))
        if not use_case:
            continue
        rows.append({"use_case": use_case, "description": "Consumers are discovering a save-worthy secondary use case that makes the product feel more versatile.", "representative_quote": _safe_text(doc.get("top_comment")) or _safe_text(doc.get("content", {}).get("body"))[:100], "quote_source": _safe_text(doc.get("source", {}).get("source_type")), "quote_engagement": int(doc["metrics"]["engagement"]), "post_count": 1, "novelty": "medium", "implications": ["CONTENT_OPPORTUNITY", "COMPETITIVE_EDGE"], "action_note": "Turn this into creator-ready content and test whether it suggests a bundle or accessory angle."})
    return {"hacks": rows, "summary": "Even in placeholder mode, the most interesting hack signal is the second-day refresh routine. These discoveries matter because they expand perceived utility and usually make for strong short-form content."}


def run_full_analysis(mw_key: str, openai_key: str, term: str, days: int = 30, sources: Optional[list[str]] = None, fetch_count: int = 50) -> dict[str, Any]:
    sources = sources or []
    temp = create_temp_search(mw_key, term)
    sid = int(temp["id"]) if temp else 0
    try:
        docs_by_eng = fetch_mentions(mw_key, sid, days, "engagement", sources, fetch_count)
        docs_positive = fetch_mentions(mw_key, sid, days, "engagement", sources, fetch_count, sentiments=["positive"])
        docs_all = fetch_mentions(mw_key, sid, days, "date", sources, fetch_count)
        return {
            "viral": analyze_viral(openai_key, docs_by_eng, term),
            "paid": analyze_paid(openai_key, docs_by_eng),
            "love": analyze_love(openai_key, docs_positive, term),
            "ideas": analyze_ideas(openai_key, docs_all, term),
            "hacks": analyze_hacks(openai_key, docs_all, term),
            "docs": docs_all,
            "query": _social_demo_query(term),
        }
    finally:
        if sid:
            delete_search(mw_key, sid)


def _social_demo_answer(question: str) -> str:
    analysis = st.session_state.get("social_demo_analysis") or run_full_analysis("demo", "demo", "FlexStyle")
    q = _safe_text(question).lower()
    if any(tok in q for tok in ["paid", "organic", "sponsored", "gifted", "seeded"]):
        stats = analysis["paid"]["stats"]
        return f"**Demo social readout:** In the placeholder mix, **{stats['organic']} of {stats['total']}** high-signal mentions read as organic. There are **{stats['incentivized_explicit']} explicitly disclosed** seeded posts and **{stats['incentivized_inferred']} inferred** seeding-style posts. The most useful product learning still comes from organic comparison videos, tutorials, and troubleshooting threads."
    if any(tok in q for tok in ["hack", "use case", "unexpected"]):
        hacks = analysis["hacks"].get("hacks", [])
        if hacks:
            first = hacks[0]
            return f"**Demo social readout:** The strongest exploratory signal is **{first['use_case']}**. It matters because it makes the product feel more versatile and share-worthy. The best next step is to turn that into creator-ready content, then decide whether it deserves a stronger product or accessory story."
    if any(tok in q for tok in ["improve", "fix", "issue", "friction", "complaint"]):
        rows = analysis["ideas"].get("improvement_themes", [])[:3]
        bullets = "\n".join(f"- **{r['theme']}** · {r['priority']} priority · {r['product_implication']}" for r in rows)
        return f"**Demo social readout:** The biggest opportunity is to make the first-run experience feel cleaner and more obvious.\n\n{bullets}\n\nThe fastest reputational win is better maintenance education, not a bigger new feature announcement."
    if any(tok in q for tok in ["viral", "trend", "spike", "trending"]):
        first = analysis["viral"]["spike_events"][0]
        return f"**Demo social readout:** The biggest spike in the placeholder dataset is **{first['title']}**, driven by **{first['cause']}**. The post is spreading because the comments turned into a live troubleshooting thread. That is a signal that creator content and pack-in education should work together, not separately."
    if any(tok in q for tok in ["love", "positive", "praise", "hero"]):
        top = analysis["love"]["praise_themes"][:3]
        bullets = "\n".join(f"- **{row['theme']}** · {row['mention_count']} positive mentions" for row in top)
        return f"**Demo social readout:** The positive side of the conversation is led by visible results, value, and confidence once consumers see the maintenance steps.\n\n{bullets}\n\nThe product story is strongest when the outcome is obvious on camera."
    product = _safe_text(st.session_state.get("social_demo_product"), "FlexStyle")
    return f"**Demo social readout:** The placeholder social story says {product} wins on visible payoff and value, but still leaks trust on maintenance clarity, the warning-light story, and premium-feel comparison shopping. The fastest win is cleaner education, not more volume of marketing."


def _render_social_css() -> None:
    st.markdown("""
    <style>
    .social-demo-hero{position:relative;overflow:hidden;border:1px solid rgba(79,70,229,.18);border-radius:24px;padding:22px 24px 20px;background:linear-gradient(180deg,#ffffff 0%,#f8faff 100%);color:var(--navy);box-shadow:var(--shadow-sm);margin-bottom:14px;}
    .social-demo-hero::before{content:"";position:absolute;inset:auto -48px -68px auto;width:220px;height:220px;background:radial-gradient(circle,rgba(79,70,229,.10),rgba(79,70,229,0));filter:blur(10px);}
    .social-demo-title{font-size:24px;font-weight:900;letter-spacing:-.03em;position:relative;z-index:1;color:var(--navy);}
    .social-demo-sub{font-size:13px;line-height:1.6;color:var(--slate-600);max-width:960px;position:relative;z-index:1;margin-top:5px;}
    .social-demo-note{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:14px 16px;box-shadow:var(--shadow-xs);margin-bottom:12px;}
    .social-demo-stat{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:14px 16px;box-shadow:var(--shadow-xs);min-height:104px;}
    .social-demo-stat .label{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--slate-500);font-weight:700;}
    .social-demo-stat .value{font-size:26px;font-weight:900;letter-spacing:-.04em;color:var(--navy);margin-top:4px;}
    .social-demo-stat .sub{font-size:12px;color:var(--slate-600);line-height:1.45;margin-top:3px;}
    .social-demo-stat.orange{border-color:rgba(249,115,22,.22);background:linear-gradient(180deg,#fff8f1 0%,#ffffff 100%);} .social-demo-stat.indigo{border-color:rgba(79,70,229,.18);background:linear-gradient(180deg,#f8f7ff 0%,#ffffff 100%);} .social-demo-stat.green{border-color:rgba(5,150,105,.18);background:linear-gradient(180deg,#f2fcf7 0%,#ffffff 100%);} .social-demo-stat.red{border-color:rgba(220,38,38,.18);background:linear-gradient(180deg,#fff5f5 0%,#ffffff 100%);} .social-demo-stat.blue{border-color:rgba(14,165,233,.18);background:linear-gradient(180deg,#f0f9ff 0%,#ffffff 100%);}
    .social-voice-card,.social-module-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:16px 16px 14px;box-shadow:var(--shadow-xs);height:100%;}
    .social-kicker{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;font-weight:800;color:var(--accent-strong);margin-bottom:5px;}
    .social-comment-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:16px 16px 14px;box-shadow:var(--shadow-xs);margin-bottom:10px;}
    .social-comment-title{font-size:15px;font-weight:800;color:var(--navy);line-height:1.35;letter-spacing:-.02em;} .social-comment-meta{font-size:11.5px;color:var(--slate-500);margin-top:4px;} .social-snippet{font-size:13.5px;color:var(--navy);line-height:1.55;margin-top:10px;}
    .social-comment-quote{margin-top:10px;padding:10px 12px;border-radius:14px;background:rgba(79,70,229,.06);border:1px solid rgba(79,70,229,.14);font-size:12.8px;line-height:1.55;color:var(--navy);} .social-comment-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:11px;font-size:12.2px;color:var(--slate-600);}
    .social-platform-chip{display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:999px;font-size:11px;font-weight:800;letter-spacing:.03em;text-transform:uppercase;border:1px solid rgba(148,163,184,.18);margin-right:4px;margin-bottom:4px;} .social-platform-chip.reddit{background:#fff7ed;border-color:#fdba74;color:#9a3412;} .social-platform-chip.youtube{background:#fef2f2;border-color:#fca5a5;color:#b91c1c;} .social-platform-chip.instagram{background:#fdf4ff;border-color:#e9d5ff;color:#7c3aed;} .social-platform-chip.tiktok{background:#eef2ff;border-color:#c7d2fe;color:#3730a3;} .social-platform-chip.generic{background:#f8fafc;border-color:var(--border);color:var(--slate-600);}
    .social-signal-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid rgba(249,115,22,.18);border-radius:18px;padding:15px 16px 14px;box-shadow:var(--shadow-xs);height:100%;} .social-signal-score{font-size:26px;font-weight:900;letter-spacing:-.04em;color:#ea580c;margin-top:4px;} .social-chip{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:#f8fafc;border:1px solid var(--border);font-size:11px;font-weight:700;color:var(--slate-600);margin-right:6px;margin-bottom:6px;}
    @media(max-width:900px){.social-comment-grid{grid-template-columns:1fr;}}
    </style>
    """, unsafe_allow_html=True)


def _render_social_listening_tab() -> None:
    _render_social_css()
    st.markdown("<div class='section-title'>Social Listening</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Placeholder Meltwater-style social intelligence studio with structured module outputs and demo content for now.</div>", unsafe_allow_html=True)
    product_name = st.session_state.get("social_demo_product", "FlexStyle")
    _ = _social_demo_payload()
    st.markdown("""<div class='social-demo-hero'>
      <span class='beta-chip'>Beta feature</span>
      <span class='beta-chip'>Demo mode</span>
      <span class='beta-chip'>Placeholder content</span>
      <div class='social-demo-title'>📣 Meltwater-style Social Listening Studio</div>
      <div class='social-demo-sub'>This tab now follows the future architecture more closely: create a temp search, fetch tuned mention sets, then run five structured modules — <b>Viral Moments</b>, <b>Paid vs Organic</b>, <b>What They Love</b>, <b>Improvement Ideas</b>, and <b>Product Hacks</b>. The content is still intentionally placeholder, but the experience is designed to feel like the real product.</div>
    </div>""", unsafe_allow_html=True)

    c1, c2, c3 = st.columns([2.4, 1.8, 1.2])
    product_name = c1.text_input("Product or topic", value=product_name, key="social_demo_product", placeholder="e.g. FlexStyle")
    sources = c2.multiselect("Sources", ["Reddit", "YouTube", "Instagram", "TikTok"], default=st.session_state.get("social_demo_sources", ["Reddit", "YouTube", "Instagram", "TikTok"]), key="social_demo_sources")
    days = c3.selectbox("Window", [7, 30, 90], index=1, key="social_demo_days")
    run_cols = st.columns([1.1, 4.0])
    if run_cols[0].button("Run demo analysis", type="primary", use_container_width=True, key="social_demo_refresh"):
        if hasattr(st, "status"):
            with st.status("Running placeholder Meltwater + AI flow", expanded=False) as status:
                status.write(f"Create temp search for **{product_name or 'FlexStyle'}**")
                status.write("Fetch engagement, positive-only, and date-sorted mention sets")
                status.write("Run Viral, Paid, Love, Ideas, and Hacks modules")
                status.update(label="Demo social analysis ready", state="complete", expanded=False)
        else:
            st.toast("Demo social analysis refreshed.")
    run_cols[1].caption("The UI is real; the content is intentionally placeholder until the live social taxonomy, prompts, and source rules are finalized.")

    analysis = run_full_analysis("demo", "demo", product_name or "FlexStyle", days=days, sources=sources, fetch_count=50)
    st.session_state["social_demo_analysis"] = analysis
    docs = analysis["docs"]
    posts = pd.DataFrame([{
        "platform": d["source"]["source_type"], "content_type": d["content_type"], "headline": d["content"]["title"], "source": d["source"]["source_name"], "submission_date": d["published"][:10], "sentiment": d["sentiment"].title(), "views": d["metrics"]["reach"], "likes": d["metrics"]["likes"], "comments": d["metrics"]["comments"], "shares": d["metrics"]["shares"], "engagement": d["metrics"]["engagement"], "velocity_pct": d["metrics"]["velocity_pct"], "viral_status": d["viral_status"], "theme": d["theme"], "voc_takeaway": d["voc_takeaway"], "snippet": d["content"]["body"], "top_comment": d["top_comment"], "top_comment_author": d["top_comment_author"], "classification": d["classification"]
    } for d in docs])

    st.markdown(f"""<div class='social-demo-note'>
      <b>Search + pipeline preview</b><br>
      Product / topic: <b>{_esc(product_name or 'FlexStyle')}</b><br>
      Query used: <code>{_esc(analysis['query'])}</code><br>
      Sources: <b>{_esc(', '.join(sources) if sources else 'All')}</b> · Window: <b>{days} days</b><br>
      <span class='small-muted'>The placeholder layer already uses the same function names and module splits as the intended Meltwater + OpenAI implementation, so the UX can stay stable while the live content gets ironed out.</span>
    </div>""", unsafe_allow_html=True)

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1: _render_social_metric_card("Mentions in sample", f"{len(docs):,}", "High-signal placeholder mentions returned by the demo fetch layer.", accent="indigo")
    with k2: _render_social_metric_card("Positive share", f"{int(round(sum(1 for d in docs if d['sentiment']=='positive') / max(len(docs), 1) * 100))}%", "Visible results and value still lead the positive side.", accent="green")
    with k3: _render_social_metric_card("Organic share", f"{analysis['paid']['stats']['organic']}/{analysis['paid']['stats']['total']}", "Most of the real learning still lives in organic content.", accent="blue")
    with k4: _render_social_metric_card("New viral posts", f"{len(analysis['viral']['spike_events'])}", "Complaint clusters and creator proof are both moving attention.", accent="orange")
    with k5: _render_social_metric_card("Risk level", "Medium", "The risk is concentrated in maintenance clarity, not core product payoff.", accent="red")

    module_cols = st.columns(3)
    module_cards = [
        ("Pipeline", "Temp search → mention retrieval → structured module outputs → executive readout."),
        ("Why this is useful now", "The team can pressure-test information architecture, charts, cards, and answer quality before going live."),
        ("What changes later", "The placeholder docs and summaries can swap to live Meltwater + AI with the same top-level UX."),
    ]
    for col, (title, body) in zip(module_cols, module_cards):
        with col:
            st.markdown(f"""<div class='social-module-card'><div class='social-kicker'>{_esc(title)}</div><div style='font-size:13px;line-height:1.58;color:var(--navy);'>{_esc(body)}</div></div>""", unsafe_allow_html=True)

    tabs = st.tabs(["Overview", "Viral Moments", "Paid vs Organic", "What They Love", "Improvement Ideas", "Product Hacks", "Explorer + Ask AI"])

    with tabs[0]:
        c_left, c_right = st.columns([1.3, 1.0])
        with c_left:
            trend_df = _social_demo_trend(date.today() - timedelta(days=days), date.today())
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=trend_df["date"], y=trend_df["mentions"], mode="lines+markers", name="Mentions", line=dict(width=3, color="#6366f1"), marker=dict(size=7)))
            fig.add_trace(go.Bar(x=trend_df["date"], y=trend_df["negative_share"], name="Negative share %", opacity=0.30, marker_color="#f97316", yaxis="y2"))
            fig.update_layout(height=360, yaxis_title="Mentions", yaxis2=dict(title="Negative share %", overlaying="y", side="right", rangemode="tozero"), legend=dict(orientation="h", y=1.06, x=0))
            _show_plotly(_sw_style_fig(fig))
        with c_right:
            platform_breakdown = posts.groupby("platform", as_index=False).agg(Posts=("headline", "count"), Engagement=("engagement", "sum")).sort_values("Engagement", ascending=False)
            fig_bar = px.bar(platform_breakdown, x="platform", y="Engagement", text="Posts")
            fig_bar.update_layout(height=360)
            _show_plotly(_sw_style_fig(fig_bar))
        d1, d2 = st.columns(2)
        with d1:
            st.markdown("**Top detractors in the placeholder social VOC**")
            st.dataframe(_social_demo_payload()["detractors"], use_container_width=True, hide_index=True, height=220)
        with d2:
            st.markdown("**Top delighters in the placeholder social VOC**")
            st.dataframe(_social_demo_payload()["delighters"], use_container_width=True, hide_index=True, height=220)
        st.markdown(f"""<div class='social-demo-note'><b>Executive VOC summary</b><br>{_esc(analysis['viral']['overall_summary'])}<br><br>{_esc(analysis['love']['summary'])}</div>""", unsafe_allow_html=True)

    with tabs[1]:
        st.markdown("**Viral Moments module**")
        vcols = st.columns(2)
        for col, event in zip(vcols * 2, analysis["viral"]["spike_events"]):
            with col:
                st.markdown(f"""<div class='social-signal-card'><div class='social-kicker'>{_esc(event['cause'])}</div><div style='font-size:15px;font-weight:800;color:var(--navy);line-height:1.35;'>{_esc(event['title'])}</div><div class='social-signal-score'>+{int(round((event['engagement'] / max(1, analysis['viral']['spike_events'][0]['engagement'])) * 100))}%</div><div style='font-size:12px;color:var(--slate-500);line-height:1.5;'>{_esc(event['date'])}<br>{int(event['engagement']):,} engagement · {int(event['reach']):,} reach<br>{_esc(event['summary'])}</div></div>""", unsafe_allow_html=True)
        cause_df = pd.DataFrame({"Cause": list(analysis["viral"]["cause_breakdown"].keys()), "Share": list(analysis["viral"]["cause_breakdown"].values())})
        fig_cause = px.pie(cause_df, names="Cause", values="Share", hole=0.45)
        fig_cause.update_layout(height=320)
        _show_plotly(_sw_style_fig(fig_cause))

    with tabs[2]:
        st.markdown("**Paid vs Organic module**")
        p1, p2, p3 = st.columns(3)
        stats = analysis["paid"]["stats"]
        with p1: _render_social_metric_card("Organic posts", str(stats["organic"]), "Unscripted consumer / editorial content", accent="green")
        with p2: _render_social_metric_card("Explicitly incentivized", str(stats["incentivized_explicit"]), "Clear #ad / gifted disclosure", accent="orange")
        with p3: _render_social_metric_card("Inferred incentivized", str(stats["incentivized_inferred"]), "Creator-wave / promo-code style content", accent="indigo")
        paid_df = pd.DataFrame(analysis["paid"]["posts"])
        st.dataframe(paid_df, use_container_width=True, hide_index=True, height=280)
        st.markdown(f"""<div class='social-demo-note'><b>Module summary</b><br>{_esc(analysis['paid']['summary'])}</div>""", unsafe_allow_html=True)

    with tabs[3]:
        st.markdown("**What They Love module**")
        love_df = pd.DataFrame(analysis["love"]["praise_themes"])
        st.dataframe(love_df, use_container_width=True, hide_index=True, height=260)
        claim_cols = st.columns(max(1, min(3, len(analysis['love']['hero_claims']) or 1)))
        for col, claim in zip(claim_cols, analysis["love"]["hero_claims"] or [{"claim": "Visible payoff", "evidence": "Placeholder evidence", "engagement_weight": "medium"}]):
            with col:
                st.markdown(f"""<div class='social-module-card'><div class='social-kicker'>Hero claim</div><div style='font-size:15px;font-weight:800;color:var(--navy);line-height:1.35;'>{_esc(claim['claim'])}</div><div style='font-size:12px;color:var(--slate-500);margin-top:6px;'>{_esc(claim['evidence'])}</div><div style='margin-top:10px;'><span class='social-chip'>{_esc(claim['engagement_weight'])} engagement weight</span></div></div>""", unsafe_allow_html=True)
        st.markdown(f"""<div class='social-demo-note'><b>Module summary</b><br>{_esc(analysis['love']['summary'])}</div>""", unsafe_allow_html=True)

    with tabs[4]:
        st.markdown("**Improvement Ideas module**")
        ideas_df = pd.DataFrame(analysis["ideas"]["improvement_themes"])
        st.dataframe(ideas_df, use_container_width=True, hide_index=True, height=280)
        st.markdown(f"""<div class='social-demo-note'><b>Unmet need summary</b><br>{_esc(analysis['ideas']['unmet_need_summary'])}<br><br><b>Jira candidates</b><br><span class='social-chip'>{'</span><span class=\'social-chip\'>'.join(_esc(x) for x in analysis['ideas']['jira_candidates'])}</span></div>""", unsafe_allow_html=True)

    with tabs[5]:
        st.markdown("**Product Hacks module**")
        hacks_df = pd.DataFrame(analysis["hacks"]["hacks"])
        if hacks_df.empty:
            st.info("No placeholder hacks are loaded for the current demo filters.")
        else:
            st.dataframe(hacks_df, use_container_width=True, hide_index=True, height=240)
        st.markdown(f"""<div class='social-demo-note'><b>Module summary</b><br>{_esc(analysis['hacks']['summary'])}</div>""", unsafe_allow_html=True)

    with tabs[6]:
        st.markdown("**Explorer**")
        f1, f2 = st.columns(2)
        platform_filter = f1.multiselect("Filter platform", sorted(posts["platform"].unique().tolist()), default=sorted(posts["platform"].unique().tolist()), key="social_explorer_platform")
        sentiment_filter = f2.multiselect("Filter sentiment", sorted(posts["sentiment"].unique().tolist()), default=sorted(posts["sentiment"].unique().tolist()), key="social_explorer_sentiment")
        filtered = posts[posts["platform"].isin(platform_filter) & posts["sentiment"].isin(sentiment_filter)]
        if filtered.empty:
            st.info("No placeholder posts match the current explorer filters.")
        else:
            st.dataframe(filtered[["platform", "content_type", "headline", "source", "submission_date", "sentiment", "engagement", "theme", "classification"]], use_container_width=True, hide_index=True, height=250)
            for _, row in filtered.head(3).iterrows():
                _render_social_post_card(row, highlight="Explorer")
        st.markdown("**Ask the placeholder social assistant**")
        st.caption("This is still demo mode, but the answer layer now uses the structured module outputs above instead of a single canned response.")
        st.session_state.setdefault("social_demo_chat_history", [{"role": "assistant", "content": "Ask about viral moments, paid vs organic, what people love, improvement ideas, or product hacks."}])
        quick_map = {"Why is it trending?": "Why is this trending right now?", "What should product fix first?": "What should product fix first?", "What are people loving?": "What are people loving most?"}
        qcols = st.columns(3)
        clicked = None
        for col, label in zip(qcols, quick_map):
            if col.button(label, use_container_width=True, key=f"social_prompt_{_slugify(label)}"):
                clicked = quick_map[label]
        user_q = st.chat_input("Ask about the placeholder social VOC…", key="social_demo_chat_input")
        prompt = clicked or user_q
        if prompt:
            st.session_state["social_demo_chat_history"].append({"role": "user", "content": prompt})
            st.session_state["social_demo_chat_history"].append({"role": "assistant", "content": _social_demo_answer(prompt)})
        for msg in st.session_state["social_demo_chat_history"][-6:]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
