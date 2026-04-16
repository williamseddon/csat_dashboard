from review_analyst.taxonomy import (
    bucket_symptom_label,
    build_structured_taxonomy_rows,
    canonical_theme_name,
    canonicalize_symptom_catalog,
    infer_category,
    infer_l1_theme,
    prioritize_ai_taxonomy_items,
    select_supported_category_pack,
    standardize_symptom_label,
    suggest_taxonomy_merges,
    taxonomy_prompt_context,
)


def test_standardize_symptom_label_canonicalizes_common_variants():
    assert standardize_symptom_label("Easy Cleanup", side="delighter") == "Easy To Clean"
    assert standardize_symptom_label("Fast Charge", side="delighter") == "Fast Charging"
    assert standardize_symptom_label("Great Value", side="delighter") == "Good Value"
    assert standardize_symptom_label("Noise Issue", side="detractor") == "Loud"
    assert standardize_symptom_label("Bad Smell", side="detractor") == "Unpleasant Scent"



def test_canonicalize_symptom_catalog_dedupes_and_builds_aliases():
    dels, dets, aliases = canonicalize_symptom_catalog(
        ["Easy Cleanup", "Quiet Operation", "Good Value", "Great Value"],
        ["Noise Issue", "Noisy", "Over Priced", "Bad Smell"],
    )

    assert dels == ["Easy To Clean", "Quiet", "Good Value"]
    assert dets == ["Loud", "Overpriced", "Unpleasant Scent"]
    assert "Easy Cleanup" in aliases["Easy To Clean"]
    assert "Quiet Operation" in aliases["Quiet"]
    assert "Noise Issue" in aliases["Loud"]
    assert "Over Priced" in aliases["Overpriced"]



def test_infer_category_and_supported_pack_are_category_aware():
    reviews = [
        "Battery dies fast and bluetooth pairing keeps dropping.",
        "Pairs quickly with my phone and charges quickly.",
        "The battery life is much better than my last headphones.",
    ]
    info = infer_category("Wireless headphones with bluetooth and charging case", reviews)
    pack = select_supported_category_pack(info["category"], reviews, min_hits=1)

    assert info["category"] == "electronics"
    assert "Fast Charging" in pack["delighters"]
    assert "Easy Connectivity" in pack["delighters"]
    assert "Short Battery Life" in pack["detractors"]



def test_taxonomy_prompt_context_mentions_category_and_systematic_labeling():
    context = taxonomy_prompt_context("beauty_personal_care")

    assert "Beauty / Personal Care" in context
    assert "systematic label naming" in context.lower()



def test_bucket_symptom_label_separates_universal_category_and_product_specific():
    assert bucket_symptom_label("Good Value", side="delighter", category="kitchen_appliance") == "Universal Neutral"
    assert bucket_symptom_label("Quiet", side="delighter", category="kitchen_appliance") == "Category Driver"
    assert bucket_symptom_label("Filter Door Hard To Open", side="detractor", category="beauty_personal_care") == "Product Specific"



def test_prioritize_ai_taxonomy_items_prefers_supported_systematic_labels():
    reviews = [
        "Great value and it works well every time.",
        "The filter door is hard to open and cleaning it is awkward.",
        "I love the blowouts, but the filter door still feels stiff.",
        "Easy attachment switching and powerful airflow.",
    ]
    items = [
        {"label": "Great Value", "aliases": ["worth the price"], "bucket": "Category Driver"},
        {"label": "Filter Door Hard To Open", "aliases": ["filter door is hard to open"], "bucket": "Product Specific"},
        {"label": "Bad Experience", "aliases": [], "bucket": "Product Specific"},
    ]

    prioritized = prioritize_ai_taxonomy_items(items, side="detractor", sample_reviews=reviews, category="beauty_personal_care", min_review_hits=1)
    labels = [item["label"] for item in prioritized]

    assert "Filter Door Hard To Open" in labels
    assert "Bad Experience" not in labels


def test_prioritize_ai_taxonomy_items_drops_seeded_category_driver_without_review_support():
    reviews = [
        "The filter door is hard to open and cleaning it is awkward.",
        "The filter still feels stiff after a week.",
    ]
    items = [
        {"label": "Quiet", "aliases": ["runs quietly"], "bucket": "Category Driver", "seeded": True},
        {"label": "Filter Door Hard To Open", "aliases": ["filter door is hard to open"], "bucket": "Product Specific"},
    ]

    prioritized = prioritize_ai_taxonomy_items(items, side="detractor", sample_reviews=reviews, category="beauty_personal_care", min_review_hits=1)
    labels = [item["label"] for item in prioritized]

    assert "Filter Door Hard To Open" in labels
    assert "Quiet" not in labels



def test_theme_normalization_and_inference_keep_taxonomy_systematic():
    assert canonical_theme_name("usability") == "Ease Of Use"
    assert canonical_theme_name("build quality") == "Quality & Durability"
    assert infer_l1_theme("Filter Door Hard To Open", side="detractor") == "Ease Of Use"
    assert infer_l1_theme("Battery Dies Fast", side="detractor") == "Power & Battery"
    assert infer_l1_theme("Great Value", side="delighter") == "Value"



def test_build_structured_taxonomy_rows_assigns_l1_l2_and_keeps_preview_metadata():
    preview_items = [
        {
            "label": "Filter Door Hard To Open",
            "side": "detractor",
            "bucket": "Product Specific",
            "theme": "Ease Of Use",
            "review_hits": 4,
            "aliases": ["door is hard to open"],
        },
        {
            "label": "Good Value",
            "side": "delighter",
            "bucket": "Universal Neutral",
            "theme": "Value",
            "review_hits": 3,
            "aliases": ["worth the price"],
        },
    ]
    rows = build_structured_taxonomy_rows(
        delighters=["Good Value"],
        detractors=["Filter Door Hard To Open"],
        aliases={"Filter Door Hard To Open": ["stiff filter door"]},
        category="beauty_personal_care",
        preview_items=preview_items,
    )
    by_label = {row["L2 Symptom"]: row for row in rows}

    assert by_label["Good Value"]["L1 Theme"] == "Value"
    assert by_label["Good Value"]["Bucket"] == "Universal Neutral"
    assert by_label["Filter Door Hard To Open"]["L1 Theme"] == "Ease Of Use"
    assert by_label["Filter Door Hard To Open"]["Review Hits"] == 4
    assert "Stiff Filter Door" in by_label["Filter Door Hard To Open"]["Aliases"]



def test_suggest_taxonomy_merges_flags_similar_labels_inside_same_theme():
    rows = [
        {"L1 Theme": "Noise", "L2 Symptom": "Loud", "Side": "Detractor", "side_key": "detractor", "Review Hits": 8},
        {"L1 Theme": "Noise", "L2 Symptom": "Too Loud", "Side": "Detractor", "side_key": "detractor", "Review Hits": 3},
        {"L1 Theme": "Cleaning & Maintenance", "L2 Symptom": "Hard To Clean", "Side": "Detractor", "side_key": "detractor", "Review Hits": 5},
    ]
    suggestions = suggest_taxonomy_merges(rows)

    assert suggestions
    assert suggestions[0]["L1 Theme"] == "Noise"
    assert {suggestions[0]["Keep"], suggestions[0]["Merge"]} == {"Loud", "Too Loud"}
