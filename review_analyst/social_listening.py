"""Social Listening Beta tab with placeholder Meltwater-style workflow.

This module mirrors the future live architecture without requiring external APIs:
1. Create a temporary search
2. Fetch mention documents in Meltwater-like shapes
3. Serialize docs into model-ready snippets
4. Run five analysis modules
5. Render a polished Streamlit experience

The current implementation is intentionally deterministic and demo-friendly so the
UX can be reviewed before live content, prompts, and API wiring are finalized.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
import html as _html_mod
import json
import re
import time
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
try:
    import streamlit as st
except Exception:  # pragma: no cover - fallback for test/container environments without Streamlit
    class _StreamlitStub:
        def __init__(self) -> None:
            self.session_state: dict[str, Any] = {}

        def __getattr__(self, name: str) -> Any:
            raise RuntimeError(f"Streamlit is required for UI rendering: {name}")

    st = _StreamlitStub()

MELTWATER_BASE = "https://api.meltwater.com/v3"
_DEMO_NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)
_DEMO_SEARCH_REGISTRY: dict[int, str] = {}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

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
    if text.lower() in {"nan", "none", "null", "<na>"}:
        return default
    return text or default


def _slugify(text: str, fallback: str = "custom") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", _safe_text(text).lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_") or fallback
    return ("prompt_" + cleaned if cleaned[0].isdigit() else cleaned)[:64]


def _clip_words(text: str, max_words: int) -> str:
    words = _safe_text(text).split()
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words]) + "…"


def _pct(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return int(round((numerator / denominator) * 100))


def _iso_days_ago(days_ago: int, *, hour: int = 12) -> str:
    when = (_DEMO_NOW - timedelta(days=days_ago)).replace(hour=hour, minute=0, second=0, microsecond=0)
    return when.isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(_safe_text(value).replace("Z", "+00:00"))


def _termize(text: str, term: str) -> str:
    term_clean = _safe_text(term) or "FlexStyle"
    return (
        _safe_text(text)
        .replace("Shark FlexStyle", f"Shark {term_clean}")
        .replace("FlexStyle", term_clean)
    )


def _term_url(term: str, slug_suffix: str) -> str:
    term_slug = _slugify(term or "flexstyle", fallback="flexstyle").replace("prompt_", "")
    return f"https://demo.social.local/{term_slug}/{slug_suffix}"


def _sw_style_fig(fig: go.Figure) -> go.Figure:
    grid = "rgba(148,163,184,0.18)"
    trace_count = len(getattr(fig, "data", []) or [])
    legend_cfg = (
        dict(
            orientation="v",
            y=1.0,
            x=1.01,
            xanchor="left",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.86)",
            bordercolor="rgba(148,163,184,0.22)",
            borderwidth=1,
            font=dict(size=11),
        )
        if trace_count > 3
        else dict(
            orientation="h",
            y=1.12,
            x=0,
            xanchor="left",
            yanchor="bottom",
            bgcolor="rgba(255,255,255,0.84)",
            bordercolor="rgba(148,163,184,0.18)",
            borderwidth=1,
            font=dict(size=11),
        )
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


# ---------------------------------------------------------------------------
# Demo mention corpus
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DemoMention:
    """Single social mention used to power the placeholder workflow."""

    doc_id: str
    platform: str
    content_type: str
    title: str
    author: str
    source_name: str
    sentiment: str
    reach: int
    likes: int
    comments: int
    shares: int
    engagement: int
    velocity_pct: int
    viral_status: str
    theme: str
    voc_takeaway: str
    body: str
    top_comment: str
    top_comment_author: str
    top_comment_likes: int
    published_days_ago: int
    cause: str
    classification: str
    pi_flag: bool = False
    positive_themes: tuple[str, ...] = field(default_factory=tuple)
    improvement_themes: tuple[str, ...] = field(default_factory=tuple)
    hack_name: str = ""
    hack_description: str = ""
    hack_implications: tuple[str, ...] = field(default_factory=tuple)
    why_notable: str = ""

    def to_doc(self, term: str) -> dict[str, Any]:
        """Convert the demo row into a Meltwater-style document."""
        doc_term = _safe_text(term) or "FlexStyle"
        return {
            "id": self.doc_id,
            "author": self.author,
            "content_type": self.content_type,
            "source": {
                "source_type": self.platform,
                "name": self.source_name.replace("FlexStyle", doc_term),
            },
            "content": {
                "title": _termize(self.title, doc_term),
                "body": _termize(self.body, doc_term),
            },
            "metrics": {
                "engagement": self.engagement,
                "reach": self.reach,
                "likes": self.likes,
                "comments": self.comments,
                "shares": self.shares,
            },
            "sentiment": self.sentiment,
            "published": _iso_days_ago(self.published_days_ago),
            "url": _term_url(doc_term, self.doc_id),
            "velocity_pct": self.velocity_pct,
            "viral_status": self.viral_status,
            "theme": self.theme,
            "voc_takeaway": _termize(self.voc_takeaway, doc_term),
            "top_comment": _termize(self.top_comment, doc_term),
            "top_comment_author": self.top_comment_author,
            "top_comment_likes": self.top_comment_likes,
            "cause": self.cause,
            "classification": self.classification,
            "pi_flag": self.pi_flag,
            "positive_themes": list(self.positive_themes),
            "improvement_themes": list(self.improvement_themes),
            "hack_name": self.hack_name,
            "hack_description": _termize(self.hack_description, doc_term),
            "hack_implications": list(self.hack_implications),
            "representative_quote": _termize(self.top_comment or self.body, doc_term),
            "why_notable": _termize(self.why_notable, doc_term),
        }


_DEMO_MENTIONS: tuple[DemoMention, ...] = (
    DemoMention(
        doc_id="reddit-filter-door",
        platform="Reddit",
        content_type="Thread",
        title="FlexStyle filter door is way harder to remove than it should be",
        author="u/blowoutscience",
        source_name="r/HaircareScience",
        sentiment="negative",
        reach=86000,
        likes=1430,
        comments=243,
        shares=114,
        engagement=1787,
        velocity_pct=189,
        viral_status="New viral complaint",
        theme="Filter Door / Cleaning",
        voc_takeaway="Consumers understand there is a filter, but not the door motion or cleaning cadence.",
        body="I love the styling results, but the filter door feels like I'm going to break it every time I try to open it. Once it is open, the cleanup is simple. Getting there is the annoying part.",
        top_comment="I thought I was going to snap the door the first week — it's not intuitive at all and the arrows are too subtle.",
        top_comment_author="u/heatprotectplease",
        top_comment_likes=612,
        published_days_ago=8,
        cause="Complaint Cluster",
        classification="ORGANIC",
        pi_flag=True,
        improvement_themes=("Filter door removal", "Filter cleaning clarity", "Pack-in quick start"),
        why_notable="High-signal complaint language with unusually strong comment agreement around the same motion problem.",
    ),
    DemoMention(
        doc_id="youtube-dyson-60-days",
        platform="YouTube",
        content_type="Video",
        title="FlexStyle vs Dyson Airwrap after 60 days",
        author="Blowout Lab",
        source_name="YouTube · Blowout Lab",
        sentiment="mixed",
        reach=186000,
        likes=9800,
        comments=1304,
        shares=640,
        engagement=11744,
        velocity_pct=121,
        viral_status="Rising comparison video",
        theme="Value vs Dyson",
        voc_takeaway="Value wins the click, but premium feel and maintenance clarity still favor Dyson in the comments.",
        body="After two months I still think FlexStyle wins on value, but Dyson feels more premium and polished. The performance is close enough that the price gap matters.",
        top_comment="FlexStyle is the better value, but Dyson still wins on polish and attachments. Cleaning the FlexStyle filter is way less intuitive.",
        top_comment_author="@styledbyjen",
        top_comment_likes=1904,
        published_days_ago=9,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Value vs Dyson", "Visible styling results"),
        improvement_themes=("Premium feel gap", "Filter cleaning clarity"),
        why_notable="Strong comparison reach with balanced praise and friction, making it one of the best decision-stage reads.",
    ),
    DemoMention(
        doc_id="youtube-red-light",
        platform="YouTube",
        content_type="Tutorial",
        title="Why your FlexStyle is flashing red — clean the filter first",
        author="Clean Girl Routine",
        source_name="YouTube · Clean Girl Routine",
        sentiment="mixed",
        reach=94000,
        likes=4200,
        comments=487,
        shares=211,
        engagement=4898,
        velocity_pct=96,
        viral_status="Service moment",
        theme="Filter Cleaning",
        voc_takeaway="Education resolves the issue quickly, but customers think the warning means something is broken.",
        body="Most of the panic comments were actually clogged filter issues, not a dead unit. Once people see the steps, the concern disappears fast.",
        top_comment="Why is the filter door so hard to remove? I had to pause and rewind twice.",
        top_comment_author="@curlsbeforecoffee",
        top_comment_likes=843,
        published_days_ago=10,
        cause="Complaint Cluster",
        classification="ORGANIC",
        improvement_themes=("Warning light meaning", "Filter cleaning clarity", "Filter door removal"),
        positive_themes=("Maintenance gets easier after one demo",),
        why_notable="Turns a reliability scare into a solvable education gap, which is exactly the kind of issue social can neutralize quickly.",
    ),
    DemoMention(
        doc_id="instagram-curls-held",
        platform="Instagram",
        content_type="Reel",
        title="FlexStyle curls still held better than expected",
        author="BeautyByMia",
        source_name="Instagram · @beautybymia",
        sentiment="positive",
        reach=312000,
        likes=21000,
        comments=604,
        shares=1700,
        engagement=23304,
        velocity_pct=78,
        viral_status="Positive creator proof",
        theme="Results / Value",
        voc_takeaway="When results show up visually, the price-value story lands immediately.",
        body="I switched from Dyson because the curls still hold and the price makes way more sense. It looks salon-level without the Dyson tax.",
        top_comment="The results are giving Airwrap but the price is way easier to justify.",
        top_comment_author="@glowmode",
        top_comment_likes=2455,
        published_days_ago=10,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Visible styling results", "Value vs Dyson", "Hair holds shape"),
        why_notable="Clear visual proof and high share counts make this the strongest positive proof asset in the demo set.",
    ),
    DemoMention(
        doc_id="reddit-switch-dyson",
        platform="Reddit",
        content_type="Thread",
        title="FlexStyle owners — worth switching from Dyson?",
        author="u/volumequest",
        source_name="r/DysonAirwrap",
        sentiment="mixed",
        reach=64000,
        likes=978,
        comments=189,
        shares=92,
        engagement=1259,
        velocity_pct=64,
        viral_status="Comparison debate",
        theme="Premium feel vs Value",
        voc_takeaway="Consumers do not say FlexStyle is bad — they say it needs clearer maintenance and a more premium-feeling experience.",
        body="Performance is surprisingly close, but Dyson feels more premium and the FlexStyle maintenance steps are not obvious.",
        top_comment="Performance is surprisingly close, but Dyson feels more premium and the maintenance steps are not obvious.",
        top_comment_author="u/hottoolhedge",
        top_comment_likes=521,
        published_days_ago=11,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Value vs Dyson",),
        improvement_themes=("Premium feel gap", "Filter cleaning clarity"),
        why_notable="Pure mid-funnel comparison behavior with strong category-language that product and brand teams can reuse.",
    ),
    DemoMention(
        doc_id="youtube-night-short",
        platform="YouTube",
        content_type="Short",
        title="Late-night blowout test: FlexStyle noise + filter clean reaction",
        author="Late Night Blowout",
        source_name="YouTube · Late Night Blowout",
        sentiment="mixed",
        reach=411000,
        likes=18000,
        comments=1827,
        shares=2200,
        engagement=22027,
        velocity_pct=214,
        viral_status="New viral video",
        theme="Noise / Filter confusion",
        voc_takeaway="The post is spreading because the comments became a community troubleshooting thread, not because the creator disliked the product.",
        body="This short is blowing up because everyone in the comments is asking why the filter light came on so fast and whether the fan is supposed to sound this loud indoors.",
        top_comment="Every comment is about the filter door because nobody realizes you have to twist and lift in one motion.",
        top_comment_author="@heatwavesarah",
        top_comment_likes=3288,
        published_days_ago=12,
        cause="Complaint Cluster",
        classification="ORGANIC",
        pi_flag=True,
        improvement_themes=("Filter door removal", "Warning light meaning", "Noise at high speed"),
        why_notable="Highest-velocity asset in the dataset and the clearest example of comments turning into a troubleshooting forum.",
    ),
    DemoMention(
        doc_id="instagram-cleaning-checklist",
        platform="Instagram",
        content_type="Carousel",
        title="FlexStyle cleaning checklist everyone should save",
        author="The Blowout Edit",
        source_name="Instagram · @theblowoutedit",
        sentiment="positive",
        reach=129000,
        likes=8200,
        comments=212,
        shares=540,
        engagement=8952,
        velocity_pct=55,
        viral_status="Helpful maintenance post",
        theme="Maintenance education",
        voc_takeaway="Once customers see the steps, the maintenance story feels manageable instead of scary.",
        body="Wish this came in the box — the cleaning step is easy once you see it, but not before. The save rate is huge because it solves a real question.",
        top_comment="Wish this came in the box — the cleaning step is easy once you see it, but not before.",
        top_comment_author="@blowdryclub",
        top_comment_likes=611,
        published_days_ago=13,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Maintenance gets easier after one demo",),
        improvement_themes=("Pack-in quick start", "Filter cleaning clarity"),
        why_notable="High save/share behavior suggests the problem is fixable with clearer education.",
    ),
    DemoMention(
        doc_id="reddit-red-light",
        platform="Reddit",
        content_type="Thread",
        title="FlexStyle red light after three weeks?",
        author="u/bouncylayers",
        source_name="r/FlexStyle",
        sentiment="negative",
        reach=58000,
        likes=702,
        comments=154,
        shares=60,
        engagement=916,
        velocity_pct=88,
        viral_status="Emerging maintenance confusion",
        theme="Filter warning / care",
        voc_takeaway="Customers can self-resolve this, but the first read is still 'my tool is failing'.",
        body="Mine was fine after cleaning, but the filter door still feels fiddly every single time. The warning light made me think the whole thing was breaking.",
        top_comment="Mine was fine after cleaning, but the filter door still feels fiddly every single time.",
        top_comment_author="u/hairdaypanic",
        top_comment_likes=374,
        published_days_ago=14,
        cause="Complaint Cluster",
        classification="ORGANIC",
        pi_flag=True,
        improvement_themes=("Warning light meaning", "Filter door removal"),
        why_notable="Strong reliability language despite an ultimately fixable issue, which is why PI should still watch it.",
    ),
    DemoMention(
        doc_id="tiktok-gifted-unboxing",
        platform="TikTok",
        content_type="Video",
        title="Gifted FlexStyle unboxing + first blowout",
        author="@hairtokhome",
        source_name="TikTok · @hairtokhome",
        sentiment="positive",
        reach=178000,
        likes=11200,
        comments=418,
        shares=960,
        engagement=12578,
        velocity_pct=102,
        viral_status="Seeded creator moment",
        theme="Creator try-on",
        voc_takeaway="The content performs because the before/after is strong, but the post is clearly disclosed and should not be counted as organic advocacy.",
        body="Gifted by Shark and honestly impressed by how fast this dried my roots. #gifted #ad I still need to practice with the round brush.",
        top_comment="The before and after is crazy but I wish creators showed the cleanup too.",
        top_comment_author="@shineandset",
        top_comment_likes=584,
        published_days_ago=7,
        cause="Paid/Seeded",
        classification="INCENTIVIZED_EXPLICIT",
        positive_themes=("Fast dry time", "Visible styling results"),
        why_notable="Clear disclosure plus high reach makes it useful for separating paid proof from true organic pull.",
    ),
    DemoMention(
        doc_id="tiktok-affiliate-code",
        platform="TikTok",
        content_type="Video",
        title="The FlexStyle attachments I actually use every week",
        author="@blowoutbrooke",
        source_name="TikTok · @blowoutbrooke",
        sentiment="mixed",
        reach=146000,
        likes=9800,
        comments=310,
        shares=720,
        engagement=10830,
        velocity_pct=73,
        viral_status="Creator conversion wave",
        theme="Attachment ranking",
        voc_takeaway="Great reach, but the 'use my code' CTA means the post is not a clean read on organic consumer language.",
        body="These are the attachments I actually use every week — use my code FLEX20 if you want the exact setup. I still wish the filter-clean step were easier to understand for first-timers.",
        top_comment="Love the results, but please show how you clean the filter because that part still confuses me.",
        top_comment_author="@rootliftclub",
        top_comment_likes=301,
        published_days_ago=6,
        cause="Paid/Seeded",
        classification="INCENTIVIZED_INFERRED",
        positive_themes=("Versatile attachments", "Visible styling results"),
        improvement_themes=("Filter cleaning clarity",),
        why_notable="Useful for promo-vs-organic split because it looks natural at first glance but still carries conversion language.",
    ),
    DemoMention(
        doc_id="editorial-roundup",
        platform="Editorial",
        content_type="Article",
        title="Best blowout tools of the year: why FlexStyle keeps getting shortlisted",
        author="Beauty Desk",
        source_name="Editorial · The Glow Report",
        sentiment="positive",
        reach=98000,
        likes=0,
        comments=0,
        shares=210,
        engagement=210,
        velocity_pct=34,
        viral_status="Editorial pickup",
        theme="Press validation",
        voc_takeaway="Editorial mentions validate quality and value, but they do not surface maintenance friction as clearly as social comments do.",
        body="Editors keep recommending FlexStyle because it offers visible results, multiple attachments, and a gentler price point than Dyson. The article barely mentions maintenance.",
        top_comment="Saved because I keep hearing this is the best value alternative to Dyson.",
        top_comment_author="site comment",
        top_comment_likes=43,
        published_days_ago=5,
        cause="Press/Editorial",
        classification="ORGANIC",
        positive_themes=("Value vs Dyson", "Versatile attachments"),
        why_notable="Helps distinguish press fuel from creator or complaint spikes in the viral module.",
    ),
    DemoMention(
        doc_id="instagram-extension-hack",
        platform="Instagram",
        content_type="Reel",
        title="Unexpected FlexStyle win: styling clip-in extensions faster",
        author="@studio.sadie",
        source_name="Instagram · @studio.sadie",
        sentiment="positive",
        reach=156000,
        likes=9200,
        comments=177,
        shares=690,
        engagement=10067,
        velocity_pct=91,
        viral_status="Niche hack gaining saves",
        theme="Unexpected use case",
        voc_takeaway="Consumers are discovering adjacent use cases that could become content or accessory opportunities.",
        body="Nobody told me FlexStyle is this good for styling clip-in extensions on a stand. It saves me from juggling a dryer and brush separately.",
        top_comment="Wait, using it on extensions on a stand is genius — I never thought about that.",
        top_comment_author="@volumevault",
        top_comment_likes=407,
        published_days_ago=4,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Versatile attachments",),
        hack_name="Style clip-in extensions",
        hack_description="Consumers are using the tool to smooth and shape clip-in extensions on a stand before putting them on.",
        hack_implications=("CONTENT_OPPORTUNITY", "COMPETITIVE_EDGE"),
        why_notable="High save/comment ratio makes it a strong candidate for official creator content or an accessory brief.",
    ),
    DemoMention(
        doc_id="reddit-wish-arrow",
        platform="Reddit",
        content_type="Thread",
        title="Small wish: the filter door needs a clearer open arrow",
        author="u/softwavesonly",
        source_name="r/FlexStyle",
        sentiment="negative",
        reach=42000,
        likes=488,
        comments=92,
        shares=28,
        engagement=608,
        velocity_pct=47,
        viral_status="Low-volume repeated ask",
        theme="Instruction clarity",
        voc_takeaway="Consumers are already prescribing the fix: clearer guidance and a more obvious motion cue on the product itself.",
        body="The tool works great, but it would be better if the filter door had a clearer arrow or tactile cue. I keep second-guessing the motion every time.",
        top_comment="Literally just add a bigger arrow and this entire Reddit thread disappears.",
        top_comment_author="u/blowoutfixer",
        top_comment_likes=211,
        published_days_ago=3,
        cause="Complaint Cluster",
        classification="ORGANIC",
        improvement_themes=("Filter door removal", "Pack-in quick start"),
        why_notable="Low volume but unusually precise design feedback; great candidate for packaging or industrial design follow-up.",
    ),
    DemoMention(
        doc_id="youtube-travel-case",
        platform="YouTube",
        content_type="Video",
        title="What I wish my FlexStyle kit included after a month of travel",
        author="Travel Hair Lab",
        source_name="YouTube · Travel Hair Lab",
        sentiment="mixed",
        reach=78000,
        likes=3600,
        comments=147,
        shares=208,
        engagement=3955,
        velocity_pct=58,
        viral_status="Improvement request",
        theme="Accessory / storage",
        voc_takeaway="The core tool wins, but storage and packability still create light unmet-need chatter.",
        body="The tool works really well, but I wish it came with a more travel-friendly case and a quicker guide for the attachment order when I am rushed.",
        top_comment="A real travel case plus quick-start guide would make this a no-brainer for me.",
        top_comment_author="@carryoncurl",
        top_comment_likes=266,
        published_days_ago=2,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Fast dry time",),
        improvement_themes=("Travel case included", "Pack-in quick start"),
        why_notable="Useful line extension and merchandising signal without sounding like a core product failure.",
    ),
    DemoMention(
        doc_id="youtube-second-day-bangs",
        platform="YouTube",
        content_type="Short",
        title="FlexStyle hack for refreshing second-day curtain bangs",
        author="@blowoutbits",
        source_name="YouTube · Blowout Bits",
        sentiment="positive",
        reach=119000,
        likes=6100,
        comments=133,
        shares=520,
        engagement=6753,
        velocity_pct=82,
        viral_status="Useful creator hack",
        theme="Unexpected use case",
        voc_takeaway="The product is showing up in quick-refresh routines, not just full styling sessions.",
        body="I figured out the concentrator plus cool shot combo is perfect for refreshing second-day curtain bangs in under two minutes.",
        top_comment="Okay this makes me want one just for second-day bangs and quick touchups.",
        top_comment_author="@partedperfect",
        top_comment_likes=389,
        published_days_ago=1,
        cause="Creator Organic",
        classification="ORGANIC",
        positive_themes=("Fast dry time", "Beginner-friendly after learning"),
        hack_name="Refresh second-day bangs",
        hack_description="Consumers are using the tool as a fast refresh system for second-day styles rather than only full blowouts.",
        hack_implications=("CONTENT_OPPORTUNITY", "NPI_SIGNAL"),
        why_notable="High intent in comments and strong 'I would buy for this alone' language.",
    ),
)


def _demo_documents(term: str) -> list[dict[str, Any]]:
    """Return Meltwater-style documents for the requested term."""
    return [spec.to_doc(term or "FlexStyle") for spec in _DEMO_MENTIONS]


# ---------------------------------------------------------------------------
# Meltwater layer (placeholder implementation)
# ---------------------------------------------------------------------------

def mw_headers(key: str) -> dict[str, str]:
    """Return headers matching the future Meltwater integration shape."""
    return {"apikey": key, "Accept": "application/json", "Content-Type": "application/json"}


def create_temp_search(key: str, term: str) -> Optional[dict[str, Any]]:
    """Create a deterministic temp search object for demo mode."""
    del key
    clean_term = _safe_text(term) or "FlexStyle"
    search_id = abs(hash((clean_term.lower(), "demo-search"))) % 10_000_000
    _DEMO_SEARCH_REGISTRY[search_id] = clean_term
    return {
        "id": search_id,
        "name": f"[DEMO] {clean_term} {_DEMO_NOW.strftime('%Y%m%d%H%M%S')}",
        "query": {
            "type": "boolean",
            "boolean": _build_social_beta_query(clean_term),
            "case_sensitivity": "no",
        },
        "demo": True,
    }


def delete_search(key: str, search_id: int) -> None:
    """Delete a temp search from the in-memory demo registry."""
    del key
    _DEMO_SEARCH_REGISTRY.pop(int(search_id), None)


def fetch_mentions(
    key: str,
    search_id: int,
    days: int,
    sort_by: str,
    sources: list[str],
    page_size: int = 50,
    sentiments: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """Fetch demo mentions in a Meltwater-like shape.

    The implementation is intentionally local and deterministic for placeholder
    UX review, but it honors the same inputs the live integration will use.
    """
    del key
    term = _DEMO_SEARCH_REGISTRY.get(int(search_id), "FlexStyle")
    docs = _demo_documents(term)
    cutoff = _DEMO_NOW - timedelta(days=max(int(days), 1))
    normalized_sources = {s.lower() for s in (sources or [])}
    normalized_sentiments = {s.lower() for s in (sentiments or [])}
    filtered: list[dict[str, Any]] = []
    for doc in docs:
        published = _parse_iso(doc.get("published", _DEMO_NOW.isoformat()))
        if published < cutoff:
            continue
        source_name = _safe_text(doc.get("source", {}).get("source_type")).lower()
        if normalized_sources and source_name not in normalized_sources:
            continue
        sentiment_name = _safe_text(doc.get("sentiment")).lower()
        if normalized_sentiments and sentiment_name not in normalized_sentiments:
            continue
        filtered.append(doc)

    if sort_by == "reach":
        filtered.sort(key=lambda d: int(d.get("metrics", {}).get("reach", 0)), reverse=True)
    elif sort_by == "date":
        filtered.sort(key=lambda d: _parse_iso(d.get("published", _DEMO_NOW.isoformat())), reverse=True)
    else:
        filtered.sort(key=lambda d: int(d.get("metrics", {}).get("engagement", 0)), reverse=True)
    return filtered[: max(1, int(page_size))]


def doc_to_snippet(doc: Mapping[str, Any], max_body: int = 400) -> str:
    """Serialize a mention doc into the future LLM context-window format."""
    content = doc.get("content", {}) if isinstance(doc, Mapping) else {}
    metrics = doc.get("metrics", {}) if isinstance(doc, Mapping) else {}
    return (
        f"SOURCE: {doc.get('source', {}).get('source_type', 'unknown')} | "
        f"SENTIMENT: {doc.get('sentiment', 'unknown')} | "
        f"ENGAGEMENT: {metrics.get('engagement', 0)} | "
        f"REACH: {metrics.get('reach', 0)} | "
        f"DATE: {_safe_text(doc.get('published'))[:10]}\n"
        f"TITLE: {_safe_text(content.get('title'))}\n"
        f"BODY: {_safe_text(content.get('body'))[:max_body]}\n"
        f"URL: {_safe_text(doc.get('url'))}"
    )


# ---------------------------------------------------------------------------
# AI layer (placeholder executor)
# ---------------------------------------------------------------------------

def run_ai(openai_key: str, system: str, user: str, max_tokens: int = 2000) -> dict[str, Any]:
    """Placeholder AI executor.

    The UI currently uses deterministic local analyzers so the demo stays
    reliable without external keys. This function is kept in place so the
    future live OpenAI call can drop into the same architecture.
    """
    del openai_key, system, user, max_tokens
    return {"mode": "demo", "note": "Placeholder AI executor is active."}


# ---------------------------------------------------------------------------
# Module prompts + analysis functions
# ---------------------------------------------------------------------------

_VIRAL_SYSTEM = """
You are a social media analyst. Given engagement-sorted mentions, identify:
1. Spike events — posts that likely triggered a wave of engagement
2. Cause of each spike: Creator Organic | Press/Editorial | Complaint Cluster | Paid/Seeded | Unknown
3. % breakdown of virality by cause type
4. Whether any spike is a negative cluster PI should review (pi_flag: true)

Return ONLY valid JSON:
{
  "spike_events": [
    {
      "date": "YYYY-MM-DD",
      "title": "≤12 word headline",
      "cause": "Creator Organic|Press/Editorial|Complaint Cluster|Paid/Seeded|Unknown",
      "engagement": 0,
      "reach": 0,
      "summary": "2-sentence explanation",
      "pi_flag": false
    }
  ],
  "cause_breakdown": {
    "Creator Organic": 0, "Press/Editorial": 0,
    "Complaint Cluster": 0, "Paid/Seeded": 0, "Unknown": 0
  },
  "overall_summary": "3-sentence paragraph"
}
""".strip()

_PAID_SYSTEM = """
You are a media transparency analyst. Classify each post as:
- ORGANIC: genuine consumer post, no commercial disclosure
- INCENTIVIZED_EXPLICIT: contains #ad #gifted #sponsored #partner "c/o" "gifted by"
- INCENTIVIZED_INFERRED: no disclosure but shows signals — uniform praise, identical
  phrasing across posts, affiliate language ("use my code"), seeding wave timing

Return ONLY valid JSON:
{
  "posts": [
    {
      "url": "...", "title": "≤60 chars",
      "classification": "ORGANIC|INCENTIVIZED_EXPLICIT|INCENTIVIZED_INFERRED",
      "reason": "1-sentence",
      "engagement": 0, "source": "..."
    }
  ],
  "stats": {
    "total": 0, "organic": 0,
    "incentivized_explicit": 0, "incentivized_inferred": 0,
    "organic_avg_engagement": 0, "incentivized_avg_engagement": 0
  },
  "top_organic": [
    {"url": "...", "title": "...", "engagement": 0, "why_notable": "1-sentence"}
  ],
  "summary": "3-sentence paragraph"
}
""".strip()

_LOVE_SYSTEM = """
You are a voice-of-customer analyst. Given positive-sentiment mentions, extract
what specific features, attributes, or outcomes people praise most.

Return ONLY valid JSON:
{
  "praise_themes": [
    {
      "theme": "≤5 word feature/outcome name",
      "mention_count": 0,
      "share_of_positive_posts": 0,
      "best_quote": "verbatim ≤40 words",
      "quote_source": "...", "quote_engagement": 0
    }
  ],
  "hero_claims": [
    {
      "claim": "marketing-ready claim ≤10 words",
      "evidence": "1-sentence social proof summary",
      "engagement_weight": "high|medium|low"
    }
  ],
  "summary": "3-sentence paragraph"
}
""".strip()

_IDEAS_SYSTEM = """
You are a product development analyst. Scan mentions for improvement signals:
wish phrasing ("I wish it had", "would be better if"), comparative gaps
("Dyson does X but this doesn't"), workarounds, feature requests, frustrations.

Return ONLY valid JSON:
{
  "improvement_themes": [
    {
      "theme": "≤6 word improvement theme",
      "post_count": 0,
      "trend": "rising|stable|declining",
      "representative_quote": "verbatim ≤40 words",
      "quote_source": "...", "quote_engagement": 0,
      "product_implication": "1-sentence for product team",
      "priority": "high|medium|low"
    }
  ],
  "unmet_need_summary": "2-3 sentence paragraph for NPI consideration",
  "jira_candidates": ["ticket title ≤10 words", "...", "..."]
}
""".strip()

_HACKS_SYSTEM = """
You are an innovation analyst scanning for unintended but positive product uses.
Look for posts where consumers use the product for a different purpose, discover
unexpected capabilities, or apply it to different materials/subjects.

Linguistic signals: "figured out", "hack", "tip", "discovered", "nobody told me",
"wait it does", "you can also use", "use it for", "instead of", "works on/for".

Classify implications:
- NPI_SIGNAL: latent market or accessory opportunity
- SAFETY_FLAG: could be unsafe or void warranty
- CONTENT_OPPORTUNITY: high-engagement discovery to amplify officially
- COMPETITIVE_EDGE: differentiating capability vs competitors

Return ONLY valid JSON:
{
  "hacks": [
    {
      "use_case": "≤6 word unintended use name",
      "description": "1-2 sentences",
      "representative_quote": "verbatim ≤40 words",
      "quote_source": "...", "quote_engagement": 0,
      "post_count": 0,
      "novelty": "high|medium|low",
      "implications": ["NPI_SIGNAL"],
      "action_note": "1-sentence recommendation"
    }
  ],
  "summary": "3-sentence paragraph for PI and NPI teams"
}
""".strip()


def _best_quote(theme: str, docs: Sequence[Mapping[str, Any]], *, field_name: str) -> tuple[str, Mapping[str, Any]]:
    candidates: list[Mapping[str, Any]] = []
    for doc in docs:
        values = {str(v) for v in doc.get(field_name, [])}
        if theme in values:
            candidates.append(doc)
    if not candidates:
        return ("", {})
    chosen = max(candidates, key=lambda d: int(d.get("metrics", {}).get("engagement", 0)))
    quote = _safe_text(chosen.get("top_comment") or chosen.get("representative_quote") or chosen.get("content", {}).get("body"))
    return (quote[:160], chosen)


def analyze_viral(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    """Identify the highest-velocity spike events in demo mode."""
    del key
    if not docs:
        return {"spike_events": [], "cause_breakdown": {}, "overall_summary": f"No mentions were found for {term}."}
    ranked = sorted(docs, key=lambda d: (int(d.get("velocity_pct", 0)), int(d.get("metrics", {}).get("engagement", 0))), reverse=True)
    spike_events: list[dict[str, Any]] = []
    cause_counts = {
        "Creator Organic": 0,
        "Press/Editorial": 0,
        "Complaint Cluster": 0,
        "Paid/Seeded": 0,
        "Unknown": 0,
    }
    for doc in ranked[:4]:
        cause = _safe_text(doc.get("cause"), "Unknown")
        if cause not in cause_counts:
            cause = "Unknown"
        cause_counts[cause] += 1
        spike_events.append(
            {
                "date": _safe_text(doc.get("published"))[:10],
                "title": _clip_words(_safe_text(doc.get("content", {}).get("title")), 12),
                "cause": cause,
                "engagement": int(doc.get("metrics", {}).get("engagement", 0)),
                "reach": int(doc.get("metrics", {}).get("reach", 0)),
                "summary": _safe_text(doc.get("voc_takeaway")),
                "pi_flag": bool(doc.get("pi_flag")),
            }
        )
    total = max(len(spike_events), 1)
    breakdown = {name: _pct(count, total) for name, count in cause_counts.items()}
    primary_cause = max(breakdown, key=breakdown.get) if breakdown else "Unknown"
    flagged = [event for event in spike_events if event.get("pi_flag")]
    overall_summary = (
        f"In demo mode, the loudest {term} spikes are being driven by {primary_cause.lower()} moments. "
        f"The highest-velocity posts are not generic buzz: they split between creator proof on value/results and complaint clusters around filter cleaning clarity. "
        f"{len(flagged)} of the top spike events are flagged for PI follow-up because the comments start framing a solvable maintenance issue like a reliability issue."
    )
    return {"spike_events": spike_events, "cause_breakdown": breakdown, "overall_summary": overall_summary}


def analyze_paid(key: str, docs: list[dict[str, Any]]) -> dict[str, Any]:
    """Split organic content from explicit or inferred seeding in demo mode."""
    del key
    if not docs:
        return {"posts": [], "stats": {}, "top_organic": [], "summary": "No mentions available."}
    posts: list[dict[str, Any]] = []
    org_total = 0
    org_engagement = 0
    inc_total = 0
    inc_engagement = 0
    explicit = 0
    inferred = 0
    for doc in docs[:50]:
        classification = _safe_text(doc.get("classification"), "ORGANIC")
        title = _clip_words(_safe_text(doc.get("content", {}).get("title")), 10)
        reason = _safe_text(doc.get("why_notable"))
        if classification == "INCENTIVIZED_EXPLICIT":
            explicit += 1
            inc_total += 1
            inc_engagement += int(doc.get("metrics", {}).get("engagement", 0))
            reason = reason or "Explicit disclosure language signals this is seeded content, not pure organic advocacy."
        elif classification == "INCENTIVIZED_INFERRED":
            inferred += 1
            inc_total += 1
            inc_engagement += int(doc.get("metrics", {}).get("engagement", 0))
            reason = reason or "Affiliate or conversion language suggests this post is at least partially incentivized."
        else:
            org_total += 1
            org_engagement += int(doc.get("metrics", {}).get("engagement", 0))
            reason = reason or "The post reads like a genuine consumer or editorial mention without commercial disclosure."
        posts.append(
            {
                "url": _safe_text(doc.get("url")),
                "title": title[:60],
                "classification": classification,
                "reason": reason,
                "engagement": int(doc.get("metrics", {}).get("engagement", 0)),
                "source": _safe_text(doc.get("source", {}).get("name")),
            }
        )
    top_organic_docs = [doc for doc in docs if _safe_text(doc.get("classification"), "ORGANIC") == "ORGANIC"]
    top_organic_docs.sort(key=lambda d: int(d.get("metrics", {}).get("engagement", 0)), reverse=True)
    top_organic = [
        {
            "url": _safe_text(doc.get("url")),
            "title": _clip_words(_safe_text(doc.get("content", {}).get("title")), 10),
            "engagement": int(doc.get("metrics", {}).get("engagement", 0)),
            "why_notable": _safe_text(doc.get("why_notable")) or _safe_text(doc.get("voc_takeaway")),
        }
        for doc in top_organic_docs[:3]
    ]
    stats = {
        "total": len(posts),
        "organic": org_total,
        "incentivized_explicit": explicit,
        "incentivized_inferred": inferred,
        "organic_avg_engagement": int(round(org_engagement / org_total)) if org_total else 0,
        "incentivized_avg_engagement": int(round(inc_engagement / inc_total)) if inc_total else 0,
    }
    summary = (
        "The placeholder split keeps paid and organic signals separate so the team can read real consumer language without over-crediting creator seeding. "
        f"In this demo set, {stats['organic']} of {stats['total']} posts are organic or editorial, while {explicit + inferred} are seeded or conversion-oriented. "
        "That makes the organic proof stronger than the paid layer, which is a healthy place to be before launch content is fully tuned."
    )
    return {"posts": posts, "stats": stats, "top_organic": top_organic, "summary": summary}


def analyze_love(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    """Aggregate the strongest positive themes in the demo dataset."""
    del key
    if not docs:
        return {"praise_themes": [], "hero_claims": [], "summary": f"No positive mentions were found for {term}."}
    theme_counts: dict[str, int] = {}
    for doc in docs:
        for theme in doc.get("positive_themes", []):
            theme_counts[theme] = theme_counts.get(theme, 0) + 1
    total_positive_posts = max(len(docs), 1)
    praise_themes: list[dict[str, Any]] = []
    for theme, count in sorted(theme_counts.items(), key=lambda item: (-item[1], item[0]))[:6]:
        quote, best_doc = _best_quote(theme, docs, field_name="positive_themes")
        praise_themes.append(
            {
                "theme": theme,
                "mention_count": count,
                "share_of_positive_posts": _pct(count, total_positive_posts),
                "best_quote": quote or _safe_text(best_doc.get("content", {}).get("body"))[:120],
                "quote_source": _safe_text(best_doc.get("source", {}).get("name")),
                "quote_engagement": int(best_doc.get("metrics", {}).get("engagement", 0) or 0),
            }
        )
    hero_claims: list[dict[str, Any]] = []
    hero_mappings = {
        "Visible styling results": "Salon-looking blowouts without the Dyson tax",
        "Value vs Dyson": "Closer to Dyson than the price suggests",
        "Fast dry time": "Fast roots-to-finish dry time",
        "Versatile attachments": "One system replaces multiple styling tools",
        "Hair holds shape": "Curls hold better than expected",
        "Beginner-friendly after learning": "Gets easier after one guided session",
        "Maintenance gets easier after one demo": "Simple once the maintenance step is shown",
    }
    for row in praise_themes[:3]:
        evidence = f"{row['mention_count']} high-signal positive posts and top quote engagement of {row['quote_engagement']:,}."
        weight = "high" if row["quote_engagement"] >= 10000 else "medium" if row["quote_engagement"] >= 4000 else "low"
        hero_claims.append(
            {
                "claim": hero_mappings.get(row["theme"], row["theme"]),
                "evidence": evidence,
                "engagement_weight": weight,
            }
        )
    summary = (
        f"The strongest positive story for {term} is still value plus visible results. "
        "When creators show the finished hair, the conversation immediately turns favorable and shoppers compare the output to Dyson. "
        "The next-best theme is versatility: once people learn the attachments, they start talking about replacing multiple tools with one system."
    )
    return {"praise_themes": praise_themes, "hero_claims": hero_claims, "summary": summary}


def analyze_ideas(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    """Turn improvement chatter into product-ready opportunities."""
    del key
    if not docs:
        return {"improvement_themes": [], "unmet_need_summary": f"No improvement signals were found for {term}.", "jira_candidates": []}
    trend_map = {
        "Filter door removal": "rising",
        "Filter cleaning clarity": "rising",
        "Warning light meaning": "rising",
        "Premium feel gap": "stable",
        "Noise at high speed": "stable",
        "Pack-in quick start": "rising",
        "Travel case included": "stable",
    }
    implication_map = {
        "Filter door removal": "Simplify the door motion or make the physical cue more obvious so maintenance does not feel risky.",
        "Filter cleaning clarity": "Tighten quick-start education with one visual that shows exactly how and when to clean the filter.",
        "Warning light meaning": "Reframe the warning as maintenance-not-failure in pack-in, onboarding, and creator content.",
        "Premium feel gap": "Tune fit-and-finish messaging and premium cues where comparison shoppers evaluate the system against Dyson.",
        "Noise at high speed": "Clarify expected sound profile and test whether nighttime or indoor use needs different guidance.",
        "Pack-in quick start": "Ship a more visual setup and care card so customers do not have to search for the first-use motion.",
        "Travel case included": "Consider bundle or accessory options for portability rather than treating it like a core performance issue.",
    }
    priority_map = {
        "Filter door removal": "high",
        "Filter cleaning clarity": "high",
        "Warning light meaning": "high",
        "Pack-in quick start": "high",
        "Premium feel gap": "medium",
        "Noise at high speed": "medium",
        "Travel case included": "low",
    }
    counts: dict[str, int] = {}
    for doc in docs:
        for theme in doc.get("improvement_themes", []):
            counts[theme] = counts.get(theme, 0) + 1
    improvement_themes: list[dict[str, Any]] = []
    for theme, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:7]:
        quote, best_doc = _best_quote(theme, docs, field_name="improvement_themes")
        improvement_themes.append(
            {
                "theme": theme,
                "post_count": count,
                "trend": trend_map.get(theme, "stable"),
                "representative_quote": quote or _safe_text(best_doc.get("content", {}).get("body"))[:120],
                "quote_source": _safe_text(best_doc.get("source", {}).get("name")),
                "quote_engagement": int(best_doc.get("metrics", {}).get("engagement", 0) or 0),
                "product_implication": implication_map.get(theme, "Review this signal with product and support."),
                "priority": priority_map.get(theme, "medium"),
            }
        )
    jira_candidates = []
    jira_map = {
        "Filter door removal": "Clarify filter-door motion",
        "Filter cleaning clarity": "Design care quick-start",
        "Warning light meaning": "Reword maintenance warning",
        "Pack-in quick start": "Create pack-in setup card",
        "Premium feel gap": "Audit premium experience cues",
        "Travel case included": "Evaluate travel bundle",
    }
    for item in improvement_themes[:3]:
        jira_candidates.append(jira_map.get(item["theme"], item["theme"]))
    unmet_need_summary = (
        f"The biggest unmet need for {term} is not a new styling result. It is confidence and clarity around maintenance. "
        "Social users are already describing the fixes: clearer motion cues, a better quick-start care visual, and language that keeps the red warning light from feeling like a failure state. "
        "The secondary opportunity is experience polish for comparison shoppers who still describe Dyson as the more premium-feeling system."
    )
    return {
        "improvement_themes": improvement_themes,
        "unmet_need_summary": unmet_need_summary,
        "jira_candidates": jira_candidates,
    }


def analyze_hacks(key: str, docs: list[dict[str, Any]], term: str) -> dict[str, Any]:
    """Capture unintended but positive use cases in the demo data."""
    del key
    hack_docs = [doc for doc in docs if _safe_text(doc.get("hack_name"))]
    if not hack_docs:
        return {"hacks": [], "summary": f"No product hacks were found for {term}."}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for doc in hack_docs:
        grouped.setdefault(_safe_text(doc.get("hack_name")), []).append(doc)
    hacks: list[dict[str, Any]] = []
    for use_case, rows in grouped.items():
        rows.sort(key=lambda d: int(d.get("metrics", {}).get("engagement", 0)), reverse=True)
        best = rows[0]
        novelty = "high" if int(best.get("metrics", {}).get("engagement", 0)) >= 9000 else "medium"
        implications = list(best.get("hack_implications", [])) or ["CONTENT_OPPORTUNITY"]
        action_note = (
            "Consider converting this into official creator guidance and testing whether an accessory, bundle, or content hook should support it."
            if "NPI_SIGNAL" in implications
            else "This is strong fodder for organic creator content because the use case feels discovered, not scripted."
        )
        hacks.append(
            {
                "use_case": use_case,
                "description": _safe_text(best.get("hack_description")),
                "representative_quote": _safe_text(best.get("representative_quote"))[:160],
                "quote_source": _safe_text(best.get("source", {}).get("name")),
                "quote_engagement": int(best.get("metrics", {}).get("engagement", 0)),
                "post_count": len(rows),
                "novelty": novelty,
                "implications": implications,
                "action_note": action_note,
            }
        )
    summary = (
        f"The hack module suggests {term} is stretching beyond its obvious use cases. "
        "That is good news: discovered workflows usually surface either content opportunities or line-extension signals before formal research catches up. "
        "Nothing in the current placeholder set looks unsafe, so this demo leans more toward growth opportunity than safety risk."
    )
    return {"hacks": hacks, "summary": summary}


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_full_analysis(
    mw_key: str,
    openai_key: str,
    term: str,
    days: int = 30,
    sources: Optional[list[str]] = None,
    fetch_count: int = 50,
) -> dict[str, Any]:
    """Create a temp search, run all five placeholder modules, and clean up."""
    del mw_key, openai_key
    sources = sources or []
    temp = create_temp_search("", term)
    if not temp:
        return {"viral": {}, "paid": {}, "love": {}, "ideas": {}, "hacks": {}}
    search_id = int(temp["id"])
    try:
        docs_by_eng = fetch_mentions("", search_id, days, "engagement", sources, fetch_count)
        docs_positive = fetch_mentions("", search_id, days, "engagement", sources, fetch_count, sentiments=["positive"])
        docs_all = fetch_mentions("", search_id, days, "date", sources, fetch_count)
        return {
            "viral": analyze_viral("", docs_by_eng, term),
            "paid": analyze_paid("", docs_by_eng),
            "love": analyze_love("", docs_positive, term),
            "ideas": analyze_ideas("", docs_all, term),
            "hacks": analyze_hacks("", docs_all, term),
        }
    finally:
        delete_search("", search_id)


# ---------------------------------------------------------------------------
# Backward-compatible payload helpers for app.py imports
# ---------------------------------------------------------------------------

def _build_social_beta_query(raw_query: str) -> str:
    """Build the Meltwater-style boolean query preview string."""
    text = _safe_text(raw_query).strip() or "FlexStyle"
    compact = re.sub(r"[^A-Za-z0-9]+", "", text)
    parts = [f'"{text}"']
    if compact and compact.lower() != re.sub(r"\s+", "", text).lower():
        parts.append(f'"{compact}"')
    if compact:
        parts.append(f'#{compact}')
    lowered = text.lower()
    if "shark" not in lowered:
        parts.append(f'"Shark {text}"')
    if "ninja" not in lowered:
        parts.append(f'"Ninja {text}"')
    core = " OR ".join(dict.fromkeys(parts))
    return (
        f"({core}) AND "
        "(review OR tutorial OR filter OR cleaning OR Dyson OR Airwrap OR value OR complaint OR comparison OR creator)"
    )


def _social_demo_query(product_name: str) -> str:
    """Backward-compatible alias for the query preview."""
    return _build_social_beta_query(product_name)


def _social_demo_trend(start_date: date, end_date: date) -> pd.DataFrame:
    """Generate a smooth placeholder trend curve for the selected date range."""
    total_days = max((end_date - start_date).days, 1)
    periods = 8 if total_days >= 14 else max(4, total_days + 1)
    dates = pd.date_range(start=start_date, end=end_date, periods=periods)
    base_mentions = [180, 214, 236, 281, 301, 344, 401, 476]
    base_sentiment = [0.69, 0.67, 0.66, 0.63, 0.61, 0.60, 0.62, 0.64]
    mentions = base_mentions[:periods] if periods <= len(base_mentions) else base_mentions + [base_mentions[-1]] * (periods - len(base_mentions))
    sentiment = base_sentiment[:periods] if periods <= len(base_sentiment) else base_sentiment + [base_sentiment[-1]] * (periods - len(base_sentiment))
    df = pd.DataFrame({"date": dates, "mentions": mentions, "sentiment": sentiment})
    df["negative_share"] = (1 - df["sentiment"]).clip(lower=0) * 100
    return df


def _docs_to_posts_df(docs: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for idx, doc in enumerate(docs, start=1):
        metrics = doc.get("metrics", {}) if isinstance(doc, Mapping) else {}
        rows.append(
            {
                "rank": idx,
                "platform": _safe_text(doc.get("source", {}).get("source_type")),
                "content_type": _safe_text(doc.get("content_type"), "Post"),
                "headline": _safe_text(doc.get("content", {}).get("title")),
                "author": _safe_text(doc.get("author")),
                "source": _safe_text(doc.get("source", {}).get("name")),
                "submission_date": _safe_text(doc.get("published"))[:10],
                "sentiment": _safe_text(doc.get("sentiment")).title(),
                "views": int(metrics.get("reach", 0)),
                "likes": int(metrics.get("likes", 0)),
                "comments": int(metrics.get("comments", 0)),
                "shares": int(metrics.get("shares", 0)),
                "engagement": int(metrics.get("engagement", 0)),
                "velocity_pct": int(doc.get("velocity_pct", 0)),
                "viral_status": _safe_text(doc.get("viral_status")),
                "theme": _safe_text(doc.get("theme")),
                "voc_takeaway": _safe_text(doc.get("voc_takeaway")),
                "snippet": _safe_text(doc.get("content", {}).get("body"))[:220],
                "top_comment": _safe_text(doc.get("top_comment")),
                "top_comment_author": _safe_text(doc.get("top_comment_author")),
                "top_comment_likes": int(doc.get("top_comment_likes", 0)),
                "classification": _safe_text(doc.get("classification"), "ORGANIC"),
                "cause": _safe_text(doc.get("cause"), "Unknown"),
                "url": _safe_text(doc.get("url")),
            }
        )
    return pd.DataFrame(rows)


def _comparison_table(term: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Dimension": "Value", term: "Wins in comments due to price-to-performance story", "Dyson": "Still seen as the prestige benchmark"},
            {"Dimension": "Premium feel", term: "Praised for results but described as less polished", "Dyson": "More refined feel in creator comparisons"},
            {"Dimension": "Maintenance clarity", term: "Filter cleaning and door motion repeatedly called confusing", "Dyson": "Fewer comments about maintenance confusion"},
            {"Dimension": "Tutorial sentiment", term: "Consumers actively share fix-it comments and care walkthroughs", "Dyson": "More styling-technique discussion than troubleshooting"},
        ]
    )


def _social_demo_payload(term: str = "FlexStyle", days: int = 30, sources: Optional[Sequence[str]] = None) -> dict[str, Any]:
    """Return the enriched payload used by the Streamlit social tab."""
    clean_term = _safe_text(term) or "FlexStyle"
    source_list = list(sources or [])
    temp = create_temp_search("", clean_term)
    search_id = int(temp["id"] if temp else 0)
    docs_all = fetch_mentions("", search_id, days, "date", source_list, 80)
    docs_by_eng = fetch_mentions("", search_id, days, "engagement", source_list, 80)
    docs_positive = fetch_mentions("", search_id, days, "engagement", source_list, 80, sentiments=["positive"])
    analysis = {
        "viral": analyze_viral("", docs_by_eng, clean_term),
        "paid": analyze_paid("", docs_by_eng),
        "love": analyze_love("", docs_positive, clean_term),
        "ideas": analyze_ideas("", docs_all, clean_term),
        "hacks": analyze_hacks("", docs_all, clean_term),
    }
    delete_search("", search_id)
    posts = _docs_to_posts_df(docs_all)
    top_comments = (
        posts[
            [
                "platform",
                "content_type",
                "headline",
                "source",
                "top_comment_author",
                "top_comment",
                "top_comment_likes",
                "engagement",
                "theme",
                "voc_takeaway",
            ]
        ]
        .rename(
            columns={
                "top_comment_author": "Author",
                "top_comment": "Comment",
                "top_comment_likes": "Comment Likes",
                "engagement": "Post Engagement",
                "theme": "Theme",
                "voc_takeaway": "VOC Takeaway",
                "headline": "Post / Video",
            }
        )
        .sort_values(["Comment Likes", "Post Engagement"], ascending=[False, False])
        .reset_index(drop=True)
    )
    detractors = pd.DataFrame(analysis["ideas"].get("improvement_themes", [])).rename(
        columns={"theme": "Theme", "post_count": "Mentions", "priority": "Priority", "product_implication": "VOC"}
    )
    if not detractors.empty:
        detractors = detractors[["Theme", "Mentions", "Priority", "VOC"]]
    delighters = pd.DataFrame(analysis["love"].get("praise_themes", [])).rename(
        columns={"theme": "Theme", "mention_count": "Mentions", "share_of_positive_posts": "Share", "best_quote": "VOC"}
    )
    if not delighters.empty:
        delighters["Share"] = delighters["Share"].astype(str) + "%"
        delighters = delighters[["Theme", "Mentions", "Share", "VOC"]]
    viral_df = pd.DataFrame(analysis["viral"].get("spike_events", []))
    positive_posts = int((posts["sentiment"].str.lower() == "positive").sum()) if not posts.empty else 0
    risk = "High" if any(bool(event.get("pi_flag")) for event in analysis["viral"].get("spike_events", [])) else "Medium"
    stats = analysis["paid"].get("stats", {})
    metrics = {
        "mentions": f"{len(posts):,}",
        "positive": f"{_pct(positive_posts, len(posts))}%",
        "viral_posts": str(len(analysis["viral"].get("spike_events", []))),
        "risk": risk,
        "organic_share": f"{_pct(int(stats.get('organic', 0)), int(stats.get('total', 0) or 1))}%",
        "top_platform": posts.groupby("platform")["engagement"].sum().sort_values(ascending=False).index[0] if not posts.empty else "N/A",
    }
    return {
        "posts": posts,
        "top_comments": top_comments,
        "detractors": detractors,
        "delighters": delighters,
        "viral": viral_df,
        "compare": _comparison_table(clean_term),
        "metrics": metrics,
        "analysis": analysis,
        "docs_all": docs_all,
        "docs_by_eng": docs_by_eng,
        "docs_positive": docs_positive,
        "query": _build_social_beta_query(clean_term),
        "term": clean_term,
    }


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _render_social_metric_card(label: str, value: str, sub: str, accent: str = "indigo") -> None:
    st.markdown(
        f"""<div class='social-demo-stat {accent}'>
          <div class='label'>{_esc(label)}</div>
          <div class='value'>{_esc(value)}</div>
          <div class='sub'>{_esc(sub)}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_social_post_card(row: pd.Series, *, highlight: str = "") -> None:
    platform_cls = {
        "Reddit": "reddit",
        "YouTube": "youtube",
        "Instagram": "instagram",
        "TikTok": "tiktok",
        "Editorial": "editorial",
    }.get(_safe_text(row.get("platform")), "generic")
    engagement_bits = [
        f"{int(pd.to_numeric(row.get('views'), errors='coerce') or 0):,} reach",
        f"{int(pd.to_numeric(row.get('likes'), errors='coerce') or 0):,} likes",
        f"{int(pd.to_numeric(row.get('comments'), errors='coerce') or 0):,} comments",
        f"{int(pd.to_numeric(row.get('engagement'), errors='coerce') or 0):,} engagement",
    ]
    st.markdown(
        f"""<div class='social-comment-card'>
          <div style='display:flex;justify-content:space-between;gap:12px;align-items:flex-start;'>
            <div>
              <span class='social-platform-chip {platform_cls}'>{_esc(row.get('platform'))}</span>
              <span class='social-platform-chip generic'>{_esc(row.get('content_type'))}</span>
              {f"<span class='social-platform-chip generic'>{_esc(highlight)}</span>" if highlight else ""}
            </div>
            <div class='small-muted'>{_esc(row.get('submission_date'))}</div>
          </div>
          <div class='social-comment-title'>{_esc(row.get('headline'))}</div>
          <div class='social-comment-meta'>{_esc(row.get('source'))} · {_esc(row.get('author'))} · {_esc(row.get('sentiment'))}</div>
          <div class='social-snippet'>{_esc(row.get('snippet'))}</div>
          <div class='social-comment-quote'><b>Top comment</b><br>{_esc(row.get('top_comment'))}</div>
          <div class='social-comment-grid'>
            <div><b>Top-comment likes</b><br>{int(pd.to_numeric(row.get('top_comment_likes'), errors='coerce') or 0):,}</div>
            <div><b>Theme</b><br>{_esc(row.get('theme'))}</div>
            <div><b>Engagement</b><br>{_esc(' · '.join(engagement_bits))}</div>
            <div><b>VOC takeaway</b><br>{_esc(row.get('voc_takeaway'))}</div>
          </div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_social_styles() -> None:
    st.markdown(
        """
        <style>
        .social-demo-hero{position:relative;overflow:hidden;border:1px solid rgba(79,70,229,.18);border-radius:24px;padding:22px 24px 20px;background:linear-gradient(180deg,#ffffff 0%,#f8faff 100%);color:var(--navy);box-shadow:var(--shadow-sm);margin-bottom:14px;}
        .social-demo-hero::before{content:"";position:absolute;inset:auto -48px -68px auto;width:240px;height:240px;background:radial-gradient(circle,rgba(79,70,229,.10),rgba(79,70,229,0));filter:blur(10px);}
        .social-demo-title{font-size:24px;font-weight:900;letter-spacing:-.03em;position:relative;z-index:1;color:var(--navy);}
        .social-demo-sub{font-size:13px;line-height:1.65;color:var(--slate-600);max-width:980px;position:relative;z-index:1;margin-top:6px;}
        .social-demo-note{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:14px 16px;box-shadow:var(--shadow-xs);margin-bottom:12px;}
        .social-demo-note b{color:var(--navy);}
        .social-demo-stat{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:14px 16px;box-shadow:var(--shadow-xs);min-height:108px;}
        .social-demo-stat .label{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--slate-500);font-weight:700;}
        .social-demo-stat .value{font-size:26px;font-weight:900;letter-spacing:-.04em;color:var(--navy);margin-top:4px;}
        .social-demo-stat .sub{font-size:12px;color:var(--slate-600);line-height:1.45;margin-top:3px;}
        .social-demo-stat.orange{border-color:rgba(249,115,22,.22);background:linear-gradient(180deg,#fff8f1 0%,#ffffff 100%);}
        .social-demo-stat.indigo{border-color:rgba(79,70,229,.18);background:linear-gradient(180deg,#f8f7ff 0%,#ffffff 100%);}
        .social-demo-stat.green{border-color:rgba(5,150,105,.18);background:linear-gradient(180deg,#f2fcf7 0%,#ffffff 100%);}
        .social-demo-stat.red{border-color:rgba(220,38,38,.18);background:linear-gradient(180deg,#fff5f5 0%,#ffffff 100%);}
        .social-voice-card,.social-module-card,.social-summary-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid var(--border);border-radius:18px;padding:16px 16px 14px;box-shadow:var(--shadow-xs);height:100%;}
        .social-module-card{padding:18px 18px 16px;}
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
        .social-platform-chip.tiktok{background:#eefcff;border-color:#a5f3fc;color:#155e75;}
        .social-platform-chip.editorial{background:#f8fafc;border-color:#cbd5e1;color:#334155;}
        .social-platform-chip.generic{background:#f8fafc;border-color:var(--border);color:var(--slate-600);}
        .social-signal-card{background:linear-gradient(180deg,#ffffff 0%,#fbfcff 100%);border:1px solid rgba(249,115,22,.18);border-radius:18px;padding:15px 16px 14px;box-shadow:var(--shadow-xs);height:100%;}
        .social-signal-score{font-size:26px;font-weight:900;letter-spacing:-.04em;color:#ea580c;margin-top:4px;}
        .social-mini-list{display:grid;gap:10px;}
        .social-mini-row{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;padding:10px 0;border-bottom:1px solid rgba(226,232,240,.8);}
        .social-mini-row:last-child{border-bottom:none;padding-bottom:0;}
        .social-mini-row:first-child{padding-top:0;}
        .social-badge{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:999px;background:#eff6ff;border:1px solid #bfdbfe;font-size:11px;font-weight:700;color:#1d4ed8;}
        .social-json{background:#0f172a;color:#e2e8f0;border-radius:16px;padding:14px 16px;font-size:12px;line-height:1.55;overflow:auto;border:1px solid rgba(148,163,184,.18);}
        @media(max-width:900px){.social-comment-grid{grid-template-columns:1fr;}}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _module_count_badges(payload: Mapping[str, Any]) -> str:
    analysis = payload.get("analysis", {}) if isinstance(payload, Mapping) else {}
    viral_count = len(analysis.get("viral", {}).get("spike_events", []))
    organic_share = payload.get("metrics", {}).get("organic_share", "0%") if isinstance(payload, Mapping) else "0%"
    love_count = len(analysis.get("love", {}).get("praise_themes", []))
    ideas_count = len(analysis.get("ideas", {}).get("improvement_themes", []))
    hacks_count = len(analysis.get("hacks", {}).get("hacks", []))
    badges = [
        f"<span class='social-badge'>Viral moments · {viral_count}</span>",
        f"<span class='social-badge'>Organic share · {organic_share}</span>",
        f"<span class='social-badge'>Love themes · {love_count}</span>",
        f"<span class='social-badge'>Improvement ideas · {ideas_count}</span>",
        f"<span class='social-badge'>Hacks · {hacks_count}</span>",
    ]
    return "".join(badges)


def _run_demo_pipeline(term: str, days: int, sources: Sequence[str], *, show_feedback: bool) -> dict[str, Any]:
    """Execute the local placeholder workflow and optionally show progress."""
    clean_term = _safe_text(term) or "FlexStyle"
    source_list = list(sources)
    if show_feedback:
        progress = st.progress(0)
        status = st.empty()
        steps = [
            (10, "Creating temporary search…"),
            (30, "Fetching engagement-sorted mentions…"),
            (45, "Fetching positive mentions…"),
            (58, "Fetching timeline mentions…"),
            (72, "Running viral module…"),
            (82, "Running paid vs organic module…"),
            (90, "Running love + ideas modules…"),
            (97, "Running product hacks module…"),
            (100, "Finalizing placeholder social workspace…"),
        ]
        for pct, label in steps:
            status.markdown(f"<div class='small-muted'>{_esc(label)}</div>", unsafe_allow_html=True)
            progress.progress(pct)
            time.sleep(0.04)
    payload = _social_demo_payload(clean_term, days=days, sources=source_list)
    if show_feedback:
        st.toast("Social Listening demo refreshed.")
    return payload


def _render_overview_tab(payload: Mapping[str, Any], start_date: date, end_date: date) -> None:
    posts = payload["posts"]
    trend_df = _social_demo_trend(start_date, end_date)
    left, right = st.columns([1.35, 1.0])
    with left:
        fig_mentions = go.Figure()
        fig_mentions.add_trace(
            go.Scatter(
                x=trend_df["date"],
                y=trend_df["mentions"],
                mode="lines+markers",
                name="Mentions",
                line=dict(width=3, color="#6366f1"),
                marker=dict(size=7),
            )
        )
        fig_mentions.add_trace(
            go.Bar(
                x=trend_df["date"],
                y=trend_df["negative_share"],
                name="Negative share %",
                opacity=0.30,
                marker_color="#f97316",
                yaxis="y2",
            )
        )
        fig_mentions.update_layout(
            height=360,
            margin=dict(l=20, r=20, t=20, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_family="Inter",
            yaxis_title="Mentions",
            yaxis2=dict(title="Negative share %", overlaying="y", side="right", rangemode="tozero"),
            legend=dict(orientation="h", y=1.06, x=0),
        )
        _show_plotly(_sw_style_fig(fig_mentions))
    with right:
        platform_breakdown = posts.groupby("platform", as_index=False).agg(Posts=("headline", "count"), Engagement=("engagement", "sum")).sort_values("Engagement", ascending=False)
        fig_plat = px.bar(platform_breakdown, x="platform", y="Engagement", text="Posts")
        fig_plat.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter")
        _show_plotly(_sw_style_fig(fig_plat))

    st.markdown("**Module readout**")
    cards = st.columns(4)
    summaries = [
        ("Viral driver", payload["analysis"]["viral"]["overall_summary"]),
        ("Paid vs organic", payload["analysis"]["paid"]["summary"]),
        ("What people love", payload["analysis"]["love"]["summary"]),
        ("What to improve", payload["analysis"]["ideas"]["unmet_need_summary"]),
    ]
    for col, (title, body) in zip(cards, summaries):
        with col:
            st.markdown(
                f"""<div class='social-voice-card'>
                <div class='social-kicker'>{_esc(title)}</div>
                <div style='font-size:13px;line-height:1.58;color:var(--navy);'>{_esc(body)}</div>
                </div>""",
                unsafe_allow_html=True,
            )

    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**🔴 Product/team watchouts**")
        st.dataframe(payload["detractors"], use_container_width=True, hide_index=True, height=260)
    with t2:
        st.markdown("**🟢 What consumers already love**")
        st.dataframe(payload["delighters"], use_container_width=True, hide_index=True, height=260)

    with st.expander("Preview the future AI context window", expanded=False):
        snippets = [doc_to_snippet(doc, max_body=220) for doc in payload.get("docs_by_eng", [])[:4]]
        st.markdown(f"<div class='social-json'>{_esc(chr(10).join([f'--- {i+1} ---\n{snip}' for i, snip in enumerate(snippets)]))}</div>", unsafe_allow_html=True)


def _render_viral_tab(payload: Mapping[str, Any]) -> None:
    analysis = payload["analysis"]["viral"]
    spike_df = pd.DataFrame(analysis.get("spike_events", []))
    left, right = st.columns([1.4, 1.0])
    with left:
        st.markdown("**Spike events**")
        if spike_df.empty:
            st.info("No spike events in scope.")
        else:
            card_cols = st.columns(2)
            for idx, (_, row) in enumerate(spike_df.iterrows()):
                with card_cols[idx % 2]:
                    st.markdown(
                        f"""<div class='social-signal-card'>
                        <div class='social-kicker'>{_esc(row.get('cause'))}</div>
                        <div style='font-size:15px;font-weight:800;color:var(--navy);line-height:1.35;'>{_esc(row.get('title'))}</div>
                        <div class='social-signal-score'>{int(row.get('engagement') or 0):,}</div>
                        <div style='font-size:12px;color:var(--slate-500);line-height:1.5;'>
                          {_esc(row.get('date'))} · Reach {int(row.get('reach') or 0):,}<br>
                          PI flag: <b>{'Yes' if bool(row.get('pi_flag')) else 'No'}</b><br>
                          {_esc(row.get('summary'))}
                        </div>
                        </div>""",
                        unsafe_allow_html=True,
                    )
    with right:
        breakdown = analysis.get("cause_breakdown", {})
        cause_df = pd.DataFrame({"Cause": list(breakdown.keys()), "Share": list(breakdown.values())})
        fig = px.pie(cause_df, names="Cause", values="Share", hole=0.55)
        fig.update_layout(height=340, margin=dict(l=20, r=20, t=12, b=20), showlegend=True)
        _show_plotly(_sw_style_fig(fig))
        st.markdown(
            f"""<div class='social-demo-note'>
            <b>Viral readout</b><br>
            {_esc(analysis.get('overall_summary'))}
            </div>""",
            unsafe_allow_html=True,
        )


def _render_paid_tab(payload: Mapping[str, Any]) -> None:
    analysis = payload["analysis"]["paid"]
    stats = analysis.get("stats", {})
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        _render_social_metric_card("Organic posts", str(stats.get("organic", 0)), "Organic + editorial posts in current demo scope.", accent="green")
    with p2:
        _render_social_metric_card("Explicitly incentivized", str(stats.get("incentivized_explicit", 0)), "Posts with clear disclosure like #ad or gifted.", accent="orange")
    with p3:
        _render_social_metric_card("Inferred incentivized", str(stats.get("incentivized_inferred", 0)), "Posts that read like affiliate / seeding content.", accent="indigo")
    with p4:
        _render_social_metric_card("Organic avg engagement", f"{int(stats.get('organic_avg_engagement', 0)):,}", "Average engagement on cleaner consumer or editorial posts.", accent="red")

    left, right = st.columns([1.45, 1.0])
    with left:
        table = pd.DataFrame(analysis.get("posts", []))
        if not table.empty:
            table = table.rename(columns={"url": "URL", "title": "Post", "classification": "Classification", "reason": "Why", "engagement": "Engagement", "source": "Source"})
        st.markdown("**Classification table**")
        st.dataframe(table, use_container_width=True, hide_index=True, height=320)
    with right:
        st.markdown("**Top organic proof**")
        for item in analysis.get("top_organic", []):
            st.markdown(
                f"""<div class='social-module-card'>
                <div class='social-kicker'>Organic highlight</div>
                <div style='font-size:14px;font-weight:800;color:var(--navy);'>{_esc(item.get('title'))}</div>
                <div style='font-size:12px;color:var(--slate-500);margin-top:4px;'>Engagement {_esc(f"{int(item.get('engagement', 0)):,}")}</div>
                <div style='font-size:13px;line-height:1.58;color:var(--navy);margin-top:8px;'>{_esc(item.get('why_notable'))}</div>
                </div>""",
                unsafe_allow_html=True,
            )
        st.markdown(
            f"""<div class='social-demo-note'>
            <b>Paid vs organic summary</b><br>
            {_esc(analysis.get('summary'))}
            </div>""",
            unsafe_allow_html=True,
        )


def _render_love_tab(payload: Mapping[str, Any]) -> None:
    analysis = payload["analysis"]["love"]
    praise_df = pd.DataFrame(analysis.get("praise_themes", []))
    left, right = st.columns([1.3, 1.0])
    with left:
        st.markdown("**Top praise themes**")
        if not praise_df.empty:
            fig = px.bar(praise_df, x="theme", y="mention_count", text="share_of_positive_posts")
            fig.update_layout(height=340, margin=dict(l=20, r=20, t=20, b=60), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            fig.update_xaxes(title="", tickangle=-22)
            fig.update_yaxes(title="Mentions")
            _show_plotly(_sw_style_fig(fig))
        st.dataframe(praise_df.rename(columns={"theme": "Theme", "mention_count": "Mentions", "share_of_positive_posts": "Share %", "best_quote": "Best Quote", "quote_source": "Source", "quote_engagement": "Engagement"}), use_container_width=True, hide_index=True, height=260)
    with right:
        st.markdown("**Hero claims from social**")
        for claim in analysis.get("hero_claims", []):
            st.markdown(
                f"""<div class='social-module-card'>
                <div class='social-kicker'>{_esc(claim.get('engagement_weight'))} confidence</div>
                <div style='font-size:14px;font-weight:800;color:var(--navy);'>{_esc(claim.get('claim'))}</div>
                <div style='font-size:13px;line-height:1.58;color:var(--navy);margin-top:8px;'>{_esc(claim.get('evidence'))}</div>
                </div>""",
                unsafe_allow_html=True,
            )
        st.markdown(
            f"""<div class='social-demo-note'>
            <b>What they love summary</b><br>
            {_esc(analysis.get('summary'))}
            </div>""",
            unsafe_allow_html=True,
        )


def _render_ideas_tab(payload: Mapping[str, Any]) -> None:
    analysis = payload["analysis"]["ideas"]
    ideas_df = pd.DataFrame(analysis.get("improvement_themes", []))
    left, right = st.columns([1.45, 1.0])
    with left:
        if not ideas_df.empty:
            fig = px.scatter(ideas_df, x="post_count", y="priority", size="quote_engagement", hover_name="theme")
            fig.update_layout(height=330, margin=dict(l=20, r=20, t=12, b=20), xaxis_title="Post count", yaxis_title="Priority")
            _show_plotly(_sw_style_fig(fig))
        st.dataframe(ideas_df.rename(columns={"theme": "Theme", "post_count": "Posts", "trend": "Trend", "representative_quote": "Representative Quote", "quote_source": "Source", "quote_engagement": "Engagement", "product_implication": "Product implication", "priority": "Priority"}), use_container_width=True, hide_index=True, height=310)
    with right:
        st.markdown(
            f"""<div class='social-demo-note'>
            <b>Unmet need summary</b><br>
            {_esc(analysis.get('unmet_need_summary'))}
            </div>""",
            unsafe_allow_html=True,
        )
        st.markdown("**Suggested JIRA candidates**")
        for title in analysis.get("jira_candidates", []):
            st.markdown(f"<div class='social-module-card' style='margin-bottom:10px;'><div style='font-size:14px;font-weight:800;color:var(--navy);'>{_esc(title)}</div></div>", unsafe_allow_html=True)


def _render_hacks_tab(payload: Mapping[str, Any]) -> None:
    analysis = payload["analysis"]["hacks"]
    hacks = analysis.get("hacks", [])
    if not hacks:
        st.info("No hack opportunities in scope.")
        return
    cols = st.columns(2)
    for idx, hack in enumerate(hacks):
        with cols[idx % 2]:
            implications = ", ".join(hack.get("implications", []))
            st.markdown(
                f"""<div class='social-module-card'>
                <div class='social-kicker'>{_esc(hack.get('novelty'))} novelty</div>
                <div style='font-size:15px;font-weight:800;color:var(--navy);'>{_esc(hack.get('use_case'))}</div>
                <div style='font-size:13px;line-height:1.58;color:var(--navy);margin-top:8px;'>{_esc(hack.get('description'))}</div>
                <div class='social-comment-quote' style='margin-top:10px;'>{_esc(hack.get('representative_quote'))}</div>
                <div class='social-mini-list' style='margin-top:12px;'>
                  <div class='social-mini-row'><span>Source</span><b>{_esc(hack.get('quote_source'))}</b></div>
                  <div class='social-mini-row'><span>Engagement</span><b>{int(hack.get('quote_engagement', 0)):,}</b></div>
                  <div class='social-mini-row'><span>Implications</span><b>{_esc(implications)}</b></div>
                </div>
                <div style='font-size:12px;color:var(--slate-600);line-height:1.55;margin-top:10px;'>{_esc(hack.get('action_note'))}</div>
                </div>""",
                unsafe_allow_html=True,
            )
    st.markdown(
        f"""<div class='social-demo-note' style='margin-top:10px;'>
        <b>Product hacks summary</b><br>
        {_esc(analysis.get('summary'))}
        </div>""",
        unsafe_allow_html=True,
    )


def _social_demo_answer(question: str, *, mode: str = "General") -> str:
    """Return a richer demo assistant answer grounded in the current social payload."""
    payload = st.session_state.get("social_listening_payload") or _social_demo_payload()
    analysis = payload.get("analysis", {})
    term = _safe_text(payload.get("term"), "FlexStyle")
    q = _safe_text(question).lower()
    mode = _safe_text(mode, "General")
    viral = analysis.get("viral", {})
    paid = analysis.get("paid", {})
    love = analysis.get("love", {})
    ideas = analysis.get("ideas", {})
    hacks = analysis.get("hacks", {})
    metrics = payload.get("metrics", {})
    posts = payload.get("posts")
    if isinstance(posts, pd.DataFrame) and not posts.empty:
        top_sources = posts["platform"].value_counts().head(3).index.tolist()
        top_themes = posts["theme"].value_counts().head(3).index.tolist()
    else:
        top_sources = []
        top_themes = []

    if mode == "Viral moments" or any(tok in q for tok in ["viral", "trend", "spike", "blowing up"]):
        top_event = (viral.get("spike_events") or [{}])[0]
        return (
            f"The main spike for **{term}** is **{_safe_text(top_event.get('title'))}**, driven by **{_safe_text(top_event.get('cause'))}**. "
            f"In the demo data, the PI-sensitive angle is mostly around **filter-door removal, warning-light meaning, and maintenance clarity** rather than a catastrophic product issue. "
            f"That makes the likely fix path more about education, creator proof, and support content than a defensive brand response."
        )
    if mode == "Paid vs organic" or any(tok in q for tok in ["paid", "organic", "gifted", "seeded", "sponsored"]):
        stats = paid.get("stats", {})
        return (
            f"For **{term}**, the placeholder split shows **{stats.get('organic', 0)} organic/editorial posts**, **{stats.get('incentivized_explicit', 0)} explicit incentivized posts**, and **{stats.get('incentivized_inferred', 0)} inferred incentivized posts**. "
            "The important read is that the strongest value and maintenance language is still surfacing organically, which suggests the story has real consumer pull beyond seeded creator content. "
            "I would keep paid and organic separate in reporting so leadership can see both reach and authenticity."
        )
    if mode == "What they love" or any(tok in q for tok in ["love", "positive", "why do people like", "what do people love"]):
        top_theme = (love.get("praise_themes") or [{}])[0]
        return (
            f"The strongest positive theme for **{term}** is **{_safe_text(top_theme.get('theme'))}**. "
            "Consumers are not just saying they like the product; they are using language around visible styling payoff, near-premium results, and strong value for the price. "
            "That usually means before-and-after proof, quick routines, and comparison content will outperform generic benefit claims."
        )
    if mode == "Improvement ideas" or any(tok in q for tok in ["improve", "fix", "wish", "complaint", "product team", "npi"]):
        top_issue = (ideas.get("improvement_themes") or [{}])[0]
        jira_candidates = ideas.get("jira_candidates", [])[:3]
        return (
            f"The top improvement signal for **{term}** is **{_safe_text(top_issue.get('theme'))}**. "
            "This reads more like a confidence-and-clarity gap than a total product failure, which is useful because it can be attacked through design cues, pack-in guidance, onboarding content, and support language. "
            f"The cleanest placeholder next moves are: **{', '.join(jira_candidates)}**."
        )
    if mode == "Product hacks" or any(tok in q for tok in ["hack", "unexpected", "use case", "innovation"]):
        top_hack = (hacks.get("hacks") or [{}])[0]
        return (
            f"The most interesting discovered use case for **{term}** is **{_safe_text(top_hack.get('use_case'))}**. "
            "Hack behavior is valuable because it often surfaces new content angles or accessory opportunities before formal research catches up. "
            "In this placeholder demo, the best next step would be to test a lightweight creator asset around the use case and see whether save and share rates stay strong."
        )

    if any(tok in q for tok in ["how many", "volume", "mentions", "count"]):
        return (
            f"The current demo scope for **{term}** includes **{_safe_text(metrics.get('mentions', '0'))} mentions** across **{', '.join(top_sources) if top_sources else 'the selected sources'}**. "
            f"The placeholder positive share is **{_safe_text(metrics.get('positive', 'n/a'))}**, with **{_safe_text(metrics.get('viral_posts', '0'))} viral moments** highlighted so far."
        )
    if any(tok in q for tok in ["where", "source", "platform", "channel"]):
        return (
            f"The heaviest placeholder signal for **{term}** is coming from **{', '.join(top_sources) if top_sources else 'the selected platforms'}**. "
            "That matters because creator-led channels tend to surface proof, routine content, and comparison language, while forum or editorial channels usually expose more nuanced maintenance and reliability discussion."
        )
    if any(tok in q for tok in ["risk", "pi", "issue", "problem", "watch out"]):
        flagged = [event for event in (viral.get("spike_events") or []) if event.get("pi_flag")]
        return (
            f"The current placeholder risk level for **{term}** is **{_safe_text(metrics.get('risk', 'Moderate'))}**, and **{len(flagged)} spike event(s)** are flagged for PI follow-up. "
            "The common thread is not broad product rejection; it is that solvable maintenance friction can start to look like a reliability issue when creators and commenters reinforce it."
        )

    overview_bits = [
        f"For **{term}**, the general story is that social is rewarding **value + visible results** while still surfacing friction around **maintenance clarity, filter-door handling, and premium-feel comparisons**.",
        f"The strongest current strengths are tied to **{', '.join(top_themes[:2]) if top_themes else 'visible styling payoff and value'}**, while the most important risk is that education gaps can snowball into negative perception.",
        "This assistant is in **General** mode right now, so you can ask basic questions naturally instead of forcing a module-specific template.",
    ]
    return " ".join(bit for bit in overview_bits if bit)


def _render_explorer_tab(payload: Mapping[str, Any]) -> None:
    posts = payload["posts"]
    st.markdown("**Explorer**")
    e1, e2, e3 = st.columns([1.1, 1.1, 1.0])
    platform_filter = e1.multiselect("Filter platform", sorted(posts["platform"].unique().tolist()), default=sorted(posts["platform"].unique().tolist()), key="social_explorer_platform")
    sentiment_filter = e2.multiselect("Filter sentiment", sorted(posts["sentiment"].unique().tolist()), default=sorted(posts["sentiment"].unique().tolist()), key="social_explorer_sentiment")
    classification_filter = e3.multiselect("Filter classification", sorted(posts["classification"].unique().tolist()), default=sorted(posts["classification"].unique().tolist()), key="social_explorer_classification")
    view_df = posts[
        posts["platform"].isin(platform_filter)
        & posts["sentiment"].isin(sentiment_filter)
        & posts["classification"].isin(classification_filter)
    ].copy()
    if view_df.empty:
        st.info("No demo posts match the current explorer filters.")
    else:
        preview = view_df[
            [
                "platform",
                "content_type",
                "headline",
                "source",
                "submission_date",
                "sentiment",
                "classification",
                "views",
                "likes",
                "comments",
                "engagement",
                "theme",
                "url",
            ]
        ].rename(
            columns={
                "platform": "Platform",
                "content_type": "Type",
                "headline": "Post / Video",
                "source": "Source",
                "submission_date": "Date",
                "sentiment": "Sentiment",
                "classification": "Classification",
                "views": "Reach",
                "likes": "Likes",
                "comments": "Comments",
                "engagement": "Engagement",
                "theme": "Theme",
                "url": "URL",
            }
        )
        st.dataframe(preview, use_container_width=True, hide_index=True, height=320)
    with st.expander("How this social engine is planned to work", expanded=False):
        st.markdown(
            """
            1. Build a short-lived boolean search for the product or topic.
            2. Pull three mention sets tuned for the right job: engagement-sorted, positive-only, and date-sorted.
            3. Serialize mention snippets into a compact AI context window.
            4. Run five modules: Viral Moments, Paid vs Organic, What They Love, Improvement Ideas, and Product Hacks.
            5. Return JSON that can be rendered in the UI, exported, or pushed into downstream workflows.

            For now the content is placeholder-only, but the code paths and JSON shapes are aligned to that future workflow.
            """
        )
        st.markdown(f"<div class='social-json'>{_esc(json.dumps(payload.get('analysis', {}), indent=2))}</div>", unsafe_allow_html=True)

    st.markdown("**Ask the social assistant**")
    st.caption("This answer layer is still demo-only, but it is now grounded in the current placeholder module outputs instead of a generic canned response.")
    st.session_state.setdefault(
        "social_demo_chat_history",
        [
            {
                "role": "assistant",
                "content": "General mode is the default here. Ask basic questions naturally, or switch modes if you want a more opinionated read on viral moments, paid vs organic, what people love, improvement ideas, or product hacks.",
            }
        ],
    )
    mode_options = ["General", "Viral moments", "Paid vs organic", "What they love", "Improvement ideas", "Product hacks"]
    if st.session_state.get("social_demo_ai_mode") not in mode_options:
        st.session_state["social_demo_ai_mode"] = "General"
    assistant_mode = st.selectbox(
        "Assistant mode",
        options=mode_options,
        key="social_demo_ai_mode",
        help="General is the default for freeform questions. Switch modes only when you want the placeholder assistant to lean harder into one module.",
    )
    st.caption("General is the default. It answers basic questions without forcing a preset module format.")
    prompt_map = {
        "General overview": "What is the general story here?",
        "Why is this product trending?": "Why is this product trending right now?",
        "What should product fix first?": "What should product fix first?",
        "What do people genuinely love?": "What do people genuinely love?",
        "Any hack or content opportunities?": "Any hack or content opportunities?",
    }
    prompt_cols = st.columns(len(prompt_map))
    clicked_prompt = None
    for col, label in zip(prompt_cols, prompt_map):
        if col.button(label, use_container_width=True, key=f"social_prompt_{_slugify(label)}"):
            clicked_prompt = prompt_map[label]
    user_q = st.chat_input("Ask about the social listening demo…", key="social_demo_chat_input")
    social_prompt = clicked_prompt or user_q
    if social_prompt and social_prompt != st.session_state.get("social_demo_last_prompt"):
        st.session_state["social_demo_last_prompt"] = social_prompt
        st.session_state["social_demo_chat_history"].append({"role": "user", "content": social_prompt})
        st.session_state["social_demo_chat_history"].append({"role": "assistant", "content": _social_demo_answer(social_prompt, mode=assistant_mode)})
    for msg in st.session_state.get("social_demo_chat_history", [])[-8:]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])


# ---------------------------------------------------------------------------
# Main tab renderer
# ---------------------------------------------------------------------------

def _render_social_listening_tab() -> None:
    """Render the placeholder social listening experience."""
    _render_social_styles()
    st.markdown("<div class='section-title'>Social Listening Beta</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Placeholder Meltwater-style workflow for external voice of consumer. This demo is intentionally polished even though the live content and prompt tuning are still being finalized.</div>", unsafe_allow_html=True)

    st.markdown(
        """<div class='social-demo-hero'>
          <span class='beta-chip'>Beta feature</span>
          <span class='beta-chip'>Placeholder data</span>
          <span class='beta-chip'>No reviews required</span>
          <div class='social-demo-title'>📣 Social Listening Workflow Preview</div>
          <div class='social-demo-sub'>This tab now mirrors the future <b>Meltwater + AI</b> workflow much more closely: create a temporary search, fetch mentions, rank by engagement/date, and run five analysis modules — <b>Viral Moments</b>, <b>Paid vs Organic</b>, <b>What They Love</b>, <b>Improvement Ideas</b>, and <b>Product Hacks</b>. The content is still placeholder/demo content for now, but the experience is intentionally close to the intended product.</div>
        </div>""",
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns([2.4, 1.35, 1.0, 1.0])
    product_name = c1.text_input("Product name or question", value=st.session_state.get("social_demo_product", "FlexStyle"), key="social_demo_product", placeholder="e.g. FlexStyle")
    source_options = ["Reddit", "YouTube", "Instagram", "TikTok", "Editorial"]
    sources = c2.multiselect("Sources", source_options, default=st.session_state.get("social_demo_sources", ["Reddit", "YouTube", "Instagram", "TikTok", "Editorial"]), key="social_demo_sources")
    range_choice = c3.selectbox("Date range", ["7d", "30d", "90d", "Custom"], index=1, key="social_demo_range")
    region = c4.selectbox("Region", ["Global", "US", "UK", "EU"], index=0, key="social_demo_region")

    if range_choice == "Custom":
        d1, d2 = st.columns(2)
        start_date = d1.date_input("Start date", value=date(2026, 3, 15), key="social_demo_start")
        end_date = d2.date_input("End date", value=date(2026, 4, 14), key="social_demo_end")
    else:
        day_window = {"7d": 7, "30d": 30, "90d": 90}[range_choice]
        end_date = date(2026, 4, 14)
        start_date = end_date - timedelta(days=day_window)
    day_window = max((end_date - start_date).days, 1)

    run_cols = st.columns([1.1, 5.0])
    rerun = run_cols[0].button("Analyze demo", type="primary", use_container_width=True, key="social_demo_refresh")
    run_cols[1].caption("Current beta uses deterministic demo content with live-looking module outputs so the experience can be reviewed before APIs and final prompts are locked.")

    signature = json.dumps(
        {
            "product": _safe_text(product_name),
            "sources": list(sorted(sources)),
            "range": range_choice,
            "start": str(start_date),
            "end": str(end_date),
            "region": region,
        },
        sort_keys=True,
    )
    if rerun or st.session_state.get("social_listening_signature") != signature or "social_listening_payload" not in st.session_state:
        st.session_state["social_listening_payload"] = _run_demo_pipeline(product_name, day_window, sources, show_feedback=rerun)
        st.session_state["social_listening_signature"] = signature
    payload = st.session_state.get("social_listening_payload") or _social_demo_payload(product_name, day_window, sources)

    query_text = payload.get("query") or _social_demo_query(product_name or "FlexStyle")
    st.markdown(
        f"""<div class='social-demo-note'>
          <b>Meltwater query preview</b><br>
          Product / question: <b>{_esc(product_name or 'FlexStyle')}</b><br>
          Query used: <code>{_esc(query_text)}</code><br>
          Sources: <b>{_esc(', '.join(sources) if sources else 'All')}</b> · Date range: <b>{_esc(f'{start_date} → {end_date}')}</b> · Region: <b>{_esc(region)}</b><br>
          <span class='small-muted'>This is still placeholder content, but the query string, module shapes, and orchestration are aligned to the intended live integration.</span>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown(f"<div style='display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;'>{_module_count_badges(payload)}</div>", unsafe_allow_html=True)

    metrics = payload.get("metrics", {})
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        _render_social_metric_card("Mentions in scope", _safe_text(metrics.get("mentions")), "Placeholder mentions across selected sources.", accent="indigo")
    with m2:
        _render_social_metric_card("Positive share", _safe_text(metrics.get("positive")), "Share of currently positive demo posts.", accent="green")
    with m3:
        _render_social_metric_card("Viral moments", _safe_text(metrics.get("viral_posts")), "Top spikes surfaced by the viral module.", accent="orange")
    with m4:
        _render_social_metric_card("Organic share", _safe_text(metrics.get("organic_share")), "Organic/editorial portion of the current scope.", accent="indigo")
    with m5:
        _render_social_metric_card("Risk level", _safe_text(metrics.get("risk")), "PI watch level from complaint cluster density.", accent="red")

    tabs = st.tabs([
        "Overview",
        "Viral Moments",
        "Paid vs Organic",
        "What They Love",
        "Improvement Ideas",
        "Product Hacks",
        "Explorer + Ask AI",
    ])
    with tabs[0]:
        _render_overview_tab(payload, start_date, end_date)
    with tabs[1]:
        _render_viral_tab(payload)
    with tabs[2]:
        _render_paid_tab(payload)
    with tabs[3]:
        _render_love_tab(payload)
    with tabs[4]:
        _render_ideas_tab(payload)
    with tabs[5]:
        _render_hacks_tab(payload)
    with tabs[6]:
        _render_explorer_tab(payload)
