from __future__ import annotations

APP_TITLE = "SharkNinja Review Analyst"

DEFAULT_PASSKEY = "caC6wVBHos09eVeBkLIniLUTzrNMMH2XMADEhpHe1ewUw"
DEFAULT_DISPLAYCODE = "15973_3_0-en_us"
DEFAULT_API_VERSION = "5.5"
DEFAULT_PAGE_SIZE = 100
DEFAULT_SORT = "SubmissionTime:desc"
DEFAULT_CONTENT_LOCALES = (
    "en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,"
    "id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,"
    "tr*,vi*,en_AU,en_CA,en_GB"
)

BAZAARVOICE_ENDPOINT = "https://api.bazaarvoice.com/data/reviews.json"
POWERREVIEWS_ENDPOINT = "https://display.powerreviews.com"
POWERREVIEWS_ENDPOINT_TEMPLATE = (
    "https://display.powerreviews.com/m/{merchant_id}/l/{locale}/product/{product_id}/reviews"
)
POWERREVIEWS_MAX_PAGE_SIZE = 25
OKENDO_API_ROOT = "https://api.okendo.io/v1"
OKENDO_API_VERSION = "2025-02-01"
OKENDO_MAX_PAGE_SIZE = 25
CURRENTBODY_OKENDO_STORE_ID = "37e09e6e-c267-4f51-9ec1-d136d8570631"

UK_EU_BV_PASSKEY = "capxzF3xnCmhSCHhkomxF1sQkZmh2zK2fNb8D1VDNl3hY"
COSTCO_BV_PASSKEY = "bai25xto36hkl5erybga10t99"
SEPHORA_BV_PASSKEY = "calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus"
ULTA_POWERREVIEWS_API_KEY = "daa0f241-c242-4483-afb7-4449942d1a2b"
HOKA_POWERREVIEWS_API_KEY = "ea283fa2-3fdc-4127-863c-b1e2397f7a77"

DEFAULT_PRODUCT_URL = "https://www.sharkninja.com/ninja-air-fryer-pro-xl-6-in-1/AF181.html"
SOURCE_MODE_URL = "Product / review URL"
SOURCE_MODE_FILE = "Uploaded review file"

TAB_DASHBOARD = "\U0001F4CA  Dashboard"
TAB_REVIEW_EXPLORER = "\U0001F50D  Review Explorer"
TAB_AI_ANALYST = "\U0001F916  AI Analyst"
TAB_REVIEW_PROMPT = "\U0001F3F7\ufe0f  Review Prompt"
TAB_SYMPTOMIZER = "\U0001F48A  Symptomizer"
WORKSPACE_TABS = [
    TAB_DASHBOARD,
    TAB_REVIEW_EXPLORER,
    TAB_AI_ANALYST,
    TAB_REVIEW_PROMPT,
    TAB_SYMPTOMIZER,
]

MODEL_OPTIONS = [
    "gpt-5.4-mini",
    "gpt-5.4",
    "gpt-5.4-pro",
    "gpt-5.4-nano",
    "gpt-5-chat-latest",
    "gpt-5-mini",
    "gpt-5",
    "gpt-5-nano",
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4.1",
]
DEFAULT_MODEL = "gpt-5.4-mini"
DEFAULT_REASONING = "none"
STRUCTURED_FALLBACK_MODEL = "gpt-5.4-mini"

STOPWORDS = {
    "a","about","after","again","all","also","am","an","and","any","are","as","at",
    "be","because","been","before","being","best","better","but","by","can","could",
    "did","do","does","don","down","even","every","for","from","get","got","great",
    "had","has","have","he","her","here","hers","him","his","how","i","if","in",
    "into","is","it","its","just","like","love","made","make","many","me","more",
    "most","much","my","new","no","not","now","of","on","one","only","or","other",
    "our","out","over","product","really","so","some","than","that","the","their",
    "them","then","there","these","they","this","to","too","use","used","using",
    "very","was","we","well","were","what","when","which","while","with","would",
    "you","your",
}

PERSONAS = {
    "Product Development": {
        "blurb": "Translates reviews into product and feature decisions.",
        "prompt": "Create a report for the product development team. Highlight what customers love, unmet needs, feature gaps, usability friction, and concrete roadmap opportunities. End with the top 5 product actions ranked by impact.",
    },
    "Quality Engineer": {
        "blurb": "Focuses on failure modes, defects, durability, and root-cause signals.",
        "prompt": "Create a report for a quality engineer. Identify defect patterns, reliability risks, cleaning issues, performance inconsistencies, and probable root-cause hypotheses. Separate confirmed evidence from inference.",
    },
    "Consumer Insights": {
        "blurb": "Extracts sentiment drivers, purchase motivations, and voice-of-customer insights.",
        "prompt": "Create a report for the consumer insights team. Summarize key sentiment drivers, barriers to adoption, purchase motivations, key use cases, and how tone changes across star ratings and incentivized vs non-incentivized reviews.",
    },
}

SITE_REVIEW_CONFIGS = [
    {
        "key": "sharkninja_us",
        "provider": "bazaarvoice",
        "bv_style": "revstats",
        "label": "SharkNinja US",
        "domains": [
            "sharkninja.com", "www.sharkninja.com",
            "sharkclean.com", "www.sharkclean.com",
            "ninjakitchen.com", "www.ninjakitchen.com",
        ],
        "passkey": DEFAULT_PASSKEY,
        "displaycode": DEFAULT_DISPLAYCODE,
        "api_version": DEFAULT_API_VERSION,
        "content_locales": DEFAULT_CONTENT_LOCALES,
        "sort": DEFAULT_SORT,
        "retailer": "SharkNinja US",
    },
    {
        "key": "sharkninja_uk_eu",
        "provider": "bazaarvoice",
        "bv_style": "simple",
        "label": "SharkNinja UK/EU",
        "domains": [
            "sharkninja.co.uk", "www.sharkninja.co.uk",
            "sharkninja.eu", "www.sharkninja.eu",
            "sharkninja.de", "www.sharkninja.de",
            "sharkninja.fr", "www.sharkninja.fr",
            "sharkninja.es", "www.sharkninja.es",
            "sharkninja.it", "www.sharkninja.it",
            "sharkninja.nl", "www.sharkninja.nl",
            "sharkclean.co.uk", "www.sharkclean.co.uk", "ninjakitchen.co.uk", "www.ninjakitchen.co.uk",
            "sharkclean.eu", "www.sharkclean.eu", "ninjakitchen.eu", "www.ninjakitchen.eu",
            "sharkclean.de", "www.sharkclean.de", "ninjakitchen.de", "www.ninjakitchen.de",
            "sharkclean.fr", "www.sharkclean.fr", "ninjakitchen.fr", "www.ninjakitchen.fr",
            "sharkclean.es", "www.sharkclean.es", "ninjakitchen.es", "www.ninjakitchen.es",
            "sharkclean.it", "www.sharkclean.it", "ninjakitchen.it", "www.ninjakitchen.it",
            "sharkclean.nl", "www.sharkclean.nl", "ninjakitchen.nl", "www.ninjakitchen.nl",
        ],
        "passkey": UK_EU_BV_PASSKEY,
        "api_version": "5.4",
        "sort": "SubmissionTime:desc",
        "locale": "en_GB",
        "retailer": "SharkNinja UK/EU",
    },
    {
        "key": "costco",
        "provider": "bazaarvoice",
        "bv_style": "revstats",
        "label": "Costco",
        "domains": ["costco.com", "www.costco.com"],
        "passkey": COSTCO_BV_PASSKEY,
        "displaycode": "2070_2_0-en_us",
        "api_version": "5.5",
        "content_locales": "en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,tr*,vi*",
        "sort": "SubmissionTime:desc",
        "retailer": "Costco",
    },
    {
        "key": "sephora",
        "provider": "bazaarvoice",
        "bv_style": "simple",
        "label": "Sephora",
        "domains": ["sephora.com", "www.sephora.com"],
        "passkey": SEPHORA_BV_PASSKEY,
        "api_version": "5.4",
        "sort": "SubmissionTime:desc",
        "locale": "en_US",
        "retailer": "Sephora",
        "extra_filters": ["contentlocale:en*"],
    },
    {
        "key": "ulta",
        "provider": "powerreviews",
        "label": "Ulta",
        "domains": ["ulta.com", "www.ulta.com"],
        "merchant_id": "6406",
        "locale": "en_US",
        "page_locale": "en_US",
        "api_key": ULTA_POWERREVIEWS_API_KEY,
        "sort": "Newest",
        "retailer": "Ulta",
    },
    {
        "key": "hoka",
        "provider": "powerreviews",
        "label": "Hoka",
        "domains": ["hoka.com", "www.hoka.com"],
        "merchant_id": "437772",
        "locale": "en_US",
        "page_locale": "en_US",
        "api_key": HOKA_POWERREVIEWS_API_KEY,
        "sort": "Newest",
        "retailer": "Hoka",
    },
    {
        "key": "currentbody",
        "provider": "okendo",
        "label": "CurrentBody",
        "domains": ["currentbody.com", "www.currentbody.com", "us.currentbody.com"],
        "okendo_user_id": CURRENTBODY_OKENDO_STORE_ID,
        "locale": "en",
        "order_by": "date desc",
        "page_size": OKENDO_MAX_PAGE_SIZE,
        "retailer": "CurrentBody",
    },
]
