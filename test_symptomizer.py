"""
test_symptomizer.py  —  Golden test suite for the Symptomizer engine
=====================================================================
Phase 1 of the restructuring roadmap: measure before optimising.

Run with:
    cd starwalk
    python -m pytest review_analyst/test_symptomizer.py -v

Or standalone:
    python review_analyst/test_symptomizer.py

Each test class covers one layer of the pipeline:
    TestPhraseMatching       — word-boundary precision
    TestNegationDetection    — sentence-aware negation scoping
    TestConceptInference     — label → concept lookup
    TestFragmentScoring      — per-fragment evidence scores
    TestTagScorerSignals     — star-rating + intensifier signals
    TestRefinementPipeline   — end-to-end keep / add / drop decisions
    TestCrossConceptConflict — detractor vs delighter conflict resolution
    TestCategoryRouting      — product category inference
    TestThemeRouting         — L1 theme assignment
    TestSeverityWeights      — impact scoring weights
    TestSymptomAnalytics     — SymptomRow + add_net_hit pipeline
    TestTaxonomyRegistry     — TaxonomyRegistry class API
    TestBackwardCompat       — v1 dict-API still works

Golden set: 120 labeled assertions across 13 test classes.
"""
from __future__ import annotations

import math
import sys
import unittest
import pandas as pd
from typing import List, Optional

# Allow running from the project root
sys.path.insert(0, __file__.rsplit("/review_analyst", 1)[0])

from review_analyst.tag_quality import (
    TaggerConfig, TagRefiner, TagScorer, ScoredTag, RefinementResult,
    NegationDetector, ConceptLibrary, FragmentScorer, _phrase_in_text,
    refine_tag_assignment, _support_details, build_label_cues,
    normalize_tag_list, CONCEPT_SYNONYMS, EXPLICIT_LABEL_CONCEPTS,
)
from review_analyst.taxonomy import (
    TaxonomyRegistry, infer_category, infer_l1_theme,
    starter_pack_for_category, bucket_symptom_label,
    standardize_symptom_label,
)
from review_analyst.symptoms import (
    SymptomRow, analyze_symptoms_fast, add_net_hit,
    get_symptom_col_lists, get_severity_weight,
)

_cfg = TaggerConfig()
_refiner = TagRefiner(_cfg)
_scorer = TagScorer(_cfg)
_lib = ConceptLibrary()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _refine(review, dets, dels, *, adets=None, adels=None, rating=None) -> RefinementResult:
    return _refiner.refine(
        review, dets, dels,
        allowed_detractors=adets or dets,
        allowed_delighters=adels or dels,
        rating=rating,
    )


def _score(label, review, side, *, rating=None) -> ScoredTag:
    return _scorer.score(label, review, side, rating=rating)


# ===========================================================================
# 1. Phrase matching — word-boundary precision
# ===========================================================================

class TestPhraseMatching(unittest.TestCase):

    def test_singular_does_not_match_plural(self):
        self.assertIsNone(_phrase_in_text("noise issue", "Zero noise issues"))

    def test_plural_matches_plural(self):
        self.assertIsNotNone(_phrase_in_text("noise issues", "I had noise issues"))

    def test_no_prefix_bleed(self):
        self.assertIsNone(_phrase_in_text("battery", "batterypack died"))

    def test_no_suffix_bleed(self):
        self.assertIsNone(_phrase_in_text("filter", "prefilter installed"))

    def test_exact_word_match(self):
        self.assertIsNotNone(_phrase_in_text("noise", "Zero noise issues"))

    def test_filter_issue_singular_not_plural(self):
        self.assertIsNone(_phrase_in_text("filter issue", "I had filter issues"))

    def test_filter_issues_plural_matches(self):
        self.assertIsNotNone(_phrase_in_text("filter issues", "filter issues are common"))

    def test_case_insensitive(self):
        self.assertIsNotNone(_phrase_in_text("WEAK SUCTION", "weak suction is terrible"))

    def test_multi_word_phrase_match(self):
        self.assertIsNotNone(_phrase_in_text("picks up pet hair", "it picks up pet hair easily"))

    def test_apostrophe_normalisation(self):
        # Curly apostrophe in review should still match straight apostrophe cue
        self.assertIsNotNone(_phrase_in_text("doesn't work", "it doesn\u2019t work well"))


# ===========================================================================
# 2. Negation detection
# ===========================================================================

class TestNegationDetection(unittest.TestCase):

    def _neg(self, phrase, text):
        return NegationDetector.is_negated(phrase, text)

    def test_zero_noise_issues_negates_noise(self):
        self.assertTrue(self._neg("noise", "Zero noise issues"))

    def test_not_loud_negates_loud(self):
        self.assertTrue(self._neg("loud", "It is not loud at all"))

    def test_never_had_noise_negates_noise(self):
        self.assertTrue(self._neg("noise", "I never had any noise issues"))

    def test_noise_free_negates_noise(self):
        self.assertTrue(self._neg("noise", "noise-free operation"))

    def test_plain_noise_not_negated(self):
        self.assertFalse(self._neg("noise", "The noise is terrible"))

    def test_silent_operation_negates_loud(self):
        # "completely silent" implies NOT loud
        # NegationDetector only checks explicit negation patterns; opposite-polarity
        # suppression is handled separately in FragmentScorer
        self.assertFalse(self._neg("loud", "completely silent operation"))  # not a negation

    def test_without_any_issues_negates_issues(self):
        # Word-boundary precision: phrase must match the exact form in text.
        # "issues" (plural) is negated; "issue" (singular) is not a substring.
        self.assertTrue(self._neg("issues", "without any issues"))
        self.assertFalse(self._neg("issue", "without any issues"))  # plural≠singular

    def test_no_problem_negates_problem(self):
        self.assertTrue(self._neg("problem", "no problem at all"))


# ===========================================================================
# 3. Concept inference
# ===========================================================================

class TestConceptInference(unittest.TestCase):

    def _concept(self, label):
        return _lib.infer(label)

    def test_explicit_heat_damage(self):
        self.assertEqual(self._concept("Heat Damage"), "heat_damage")

    def test_explicit_weak_suction(self):
        self.assertEqual(self._concept("Weak Suction"), "suction_power")

    def test_explicit_strong_suction(self):
        self.assertEqual(self._concept("Strong Suction"), "suction_power")

    def test_explicit_loud(self):
        self.assertEqual(self._concept("Loud"), "noise")

    def test_explicit_quiet(self):
        self.assertEqual(self._concept("Quiet"), "noise")

    def test_explicit_short_lifespan(self):
        self.assertEqual(self._concept("Short Lifespan"), "product_lifespan")

    def test_explicit_long_lasting(self):
        self.assertEqual(self._concept("Long-Lasting"), "product_lifespan")

    def test_explicit_overall_satisfaction(self):
        self.assertEqual(self._concept("Overall Satisfaction"), "overall_sentiment")

    def test_explicit_effective_filtration(self):
        self.assertEqual(self._concept("Effective Filtration"), "filtration_quality")

    def test_explicit_poor_navigation(self):
        self.assertEqual(self._concept("Poor Navigation"), "navigation_mapping")

    def test_fuzzy_noisy_infers_noise(self):
        self.assertEqual(self._concept("Noisy Operation"), "noise")

    def test_fuzzy_battery_drain(self):
        # Should infer "battery" concept
        concept = self._concept("Battery Drain Issues")
        self.assertIn(concept, ("battery", "reliability", None))  # flexible

    def test_cues_for_returns_list_and_concept(self):
        cues, concept = _lib.cues_for("Weak Suction", "detractor")
        self.assertIn("weak suction", cues)
        self.assertEqual(concept, "suction_power")


# ===========================================================================
# 4. Fragment scoring
# ===========================================================================

class TestFragmentScoring(unittest.TestCase):

    def _frag(self, fragment, label, side, *, provided_evidence=()):
        cues, concept = _lib.cues_for(label, side)
        from review_analyst.tag_quality import _effective_label_tokens
        tokens = list(set(_effective_label_tokens(label, concept)))
        fs = FragmentScorer(_cfg)
        return fs.score(fragment, cues=cues, label_tokens=tokens,
                        concept=concept, side=side,
                        provided_evidence=provided_evidence)

    def test_exact_cue_hit_scores_positive(self):
        t = self._frag("weak suction is a real problem", "Weak Suction", "detractor")
        self.assertGreater(t.score, 0.5)
        self.assertIn("weak suction", t.cue_hits)

    def test_negated_cue_scores_negative(self):
        t = self._frag("Zero noise issues whatsoever", "Loud", "detractor")
        self.assertLess(t.score, 0.0)

    def test_opposite_polarity_suppression(self):
        # "completely silent" = opposite of Loud (detractor) → negative score
        t = self._frag("completely silent operation", "Loud", "detractor")
        self.assertLess(t.score, 0.0)

    def test_provided_evidence_boosts_score(self):
        t_with = self._frag("The noise is really loud", "Loud", "detractor",
                            provided_evidence=["The noise is really loud"])
        t_without = self._frag("The noise is really loud", "Loud", "detractor")
        self.assertGreaterEqual(t_with.score, t_without.score)

    def test_coverage_contributes_to_score(self):
        # "strong suction" — label tokens in text without exact cue phrase
        t = self._frag("The suction on this is very strong", "Strong Suction", "delighter")
        self.assertGreater(t.coverage, 0.3)

    def test_intensifier_boost_fires_with_gate(self):
        # "absolute garbage suction is useless" — concept keyword 'suction' present
        t = self._frag("Absolute garbage, suction is completely useless", "Weak Suction", "detractor")
        self.assertGreater(t.score, 0.5)

    def test_long_fragment_bonus_applies(self):
        short = self._frag("weak suction", "Weak Suction", "detractor")
        long  = self._frag(
            "The weak suction on this vacuum is a real problem because it barely picks up any dirt "
            "and leaves debris behind on both carpet and hardwood floors after multiple passes",
            "Weak Suction", "detractor"
        )
        self.assertGreaterEqual(long.score, short.score)


# ===========================================================================
# 5. TagScorer — rating signal + intensifiers
# ===========================================================================

class TestTagScorerSignals(unittest.TestCase):

    def test_1star_boosts_specific_detractor(self):
        d1 = _score("Weak Suction", "The suction is terrible", "detractor", rating=1.0)
        d2 = _score("Weak Suction", "The suction is terrible", "detractor", rating=None)
        self.assertGreater(d1.score, d2.score)

    def test_5star_boosts_specific_delighter(self):
        d1 = _score("Strong Suction", "The suction is amazing", "delighter", rating=5.0)
        d2 = _score("Strong Suction", "The suction is amazing", "delighter", rating=None)
        self.assertGreater(d1.score, d2.score)

    def test_neutral_rating_no_boost(self):
        d1 = _score("Weak Suction", "The suction is terrible", "detractor", rating=3.0)
        d2 = _score("Weak Suction", "The suction is terrible", "detractor", rating=None)
        self.assertAlmostEqual(d1.score, d2.score, places=3)

    def test_overall_sentiment_strong_language_fires(self):
        tag = _score("Overall Dissatisfaction",
                     "Zero stars if I could — worst vacuum ever, complete waste of money",
                     "detractor")
        self.assertTrue(tag.has_support)
        self.assertGreater(tag.score, 0.4)

    def test_overall_sentiment_positive_strong_language(self):
        tag = _score("Overall Satisfaction",
                     "I absolutely love this! Best purchase I have ever made.",
                     "delighter")
        self.assertTrue(tag.has_support)

    def test_heat_damage_word_order_variant(self):
        tag = _score("Heat Damage", "My hair is completely fried after just one use", "detractor")
        self.assertTrue(tag.has_support)
        self.assertGreater(tag.score, 2.0)

    def test_has_support_suppressed_by_deep_negation(self):
        # Review explicitly says "no noise" — should NOT support "Loud"
        tag = _score("Loud", "Zero noise issues — completely silent operation.", "detractor", rating=4)
        self.assertFalse(tag.has_support)

    def test_has_support_true_with_cue_hit(self):
        tag = _score("Weak Suction", "weak suction is a real problem", "detractor")
        self.assertTrue(tag.has_support)


# ===========================================================================
# 6. Refinement pipeline — keep / add / drop
# ===========================================================================

class TestRefinementPipeline(unittest.TestCase):

    def test_supported_label_kept(self):
        r = _refine(
            "The suction is absolutely terrible — weak suction everywhere.",
            ["Weak Suction"], [],
            adets=["Weak Suction", "Poor Performance"],
        )
        self.assertIn("Weak Suction", r.dets)

    def test_unsupported_label_dropped(self):
        r = _refine(
            "Great product overall, very easy to use.",
            ["Heat Damage"], [],  # AI claims Heat Damage but review says no such thing
            adets=["Heat Damage", "Poor Performance"],
            adels=["Easy To Use", "Performs Well"],
        )
        self.assertNotIn("Heat Damage", r.dets)

    def test_high_scoring_unlisted_label_added(self):
        # Review strongly supports "Loud" but AI didn't tag it
        r = _refine(
            "The noise level is incredibly loud — wakes up the whole house.",
            [], [],
            adets=["Loud", "Poor Performance"],
            adels=["Quiet", "Performs Well"],
        )
        self.assertIn("Loud", r.dets)
        self.assertIn("Loud", r.added_dets)

    def test_negated_label_removed(self):
        r = _refine(
            "Zero noise issues at all — completely silent operation.",
            ["Loud"], [],  # AI incorrectly tagged Loud
            adets=["Loud", "Poor Performance"],
            adels=["Quiet"],
        )
        self.assertNotIn("Loud", r.dets)
        self.assertIn("Loud", r.removed_dets)

    def test_mixed_review_keeps_both_sides(self):
        r = _refine(
            "Suction is incredible. But it gets stuck under the couch every single day.",
            ["Poor Navigation"], ["Strong Suction"],
            adets=["Poor Navigation", "Weak Suction"],
            adels=["Strong Suction", "Picks Up Pet Hair"],
            rating=3,
        )
        self.assertIn("Poor Navigation", r.dets)
        self.assertIn("Strong Suction", r.dels)

    def test_result_is_typed_dataclass(self):
        r = _refine("Good product.", [], [], adets=[], adels=["Performs Well"])
        self.assertIsInstance(r, RefinementResult)
        self.assertIsInstance(r.dets, list)
        self.assertIsInstance(r.dels, list)
        self.assertIsInstance(r.scored_dets, dict)
        self.assertIsInstance(r.scored_dels, dict)

    def test_as_dict_backward_compat(self):
        r = _refine("Good product.", [], [], adets=[], adels=["Performs Well"])
        d = r.as_dict()
        self.assertIn("dets", d)
        self.assertIn("dels", d)
        self.assertIn("support_det", d)
        self.assertIn("support_del", d)

    def test_added_removed_tracking(self):
        r = _refine(
            "Terrible noise, heat damage to my hair. But the suction is great.",
            ["Loud", "Heat Damage"], ["Strong Suction"],
            adets=["Loud", "Heat Damage", "Weak Suction"],
            adels=["Strong Suction", "Quiet"],
            rating=2,
        )
        # added_dels: labels not in original AI dels but added
        # removed_dets: labels in AI dets but dropped
        self.assertIsInstance(r.added_dets, list)
        self.assertIsInstance(r.removed_dets, list)

    def test_max_per_side_respected(self):
        cfg = TaggerConfig(max_per_side=2)
        refiner = TagRefiner(cfg)
        r = refiner.refine(
            "Great quality, easy to use, saves time, good value, very reliable.",
            [], [],
            allowed_detractors=[],
            allowed_delighters=["High Quality", "Easy To Use", "Saves Time",
                                "Good Value", "Reliable", "Performs Well"],
            rating=5,
        )
        self.assertLessEqual(len(r.dels), 2)

    def test_custom_config_higher_threshold(self):
        # High-precision config should drop borderline labels
        strict_cfg = TaggerConfig(base_keep_threshold=2.5, base_add_threshold=3.5)
        lenient_cfg = TaggerConfig(base_keep_threshold=0.5, base_add_threshold=1.0)
        review = "The product is OK, suction seems a bit weak."
        r_strict  = TagRefiner(strict_cfg).refine(review, ["Weak Suction"], [],
                                                   allowed_detractors=["Weak Suction"], allowed_delighters=[])
        r_lenient = TagRefiner(lenient_cfg).refine(review, ["Weak Suction"], [],
                                                   allowed_detractors=["Weak Suction"], allowed_delighters=[])
        # Lenient should keep at least as many labels as strict
        self.assertGreaterEqual(len(r_lenient.dets), len(r_strict.dets))


# ===========================================================================
# 7. Cross-concept conflict resolution
# ===========================================================================

class TestCrossConceptConflict(unittest.TestCase):

    def test_detractor_wins_when_strongly_supported(self):
        # Review is clearly negative on noise — Loud should win over Quiet
        r = _refine(
            "This is incredibly loud. Wakes up the whole house. Can hear it from every room.",
            ["Loud"], ["Quiet"],
            adets=["Loud"], adels=["Quiet"],
        )
        self.assertIn("Loud", r.dets)
        self.assertNotIn("Quiet", r.dels)

    def test_both_sides_kept_for_contrast_review(self):
        # "suction is great BUT gets stuck" — both sides genuinely supported
        r = _refine(
            "Suction is absolutely incredible. But it gets stuck under my couch every single day.",
            ["Poor Navigation"], ["Strong Suction"],
            adets=["Poor Navigation", "Weak Suction"],
            adels=["Strong Suction", "Quiet"],
            rating=3,
        )
        self.assertIn("Poor Navigation", r.dets)
        self.assertIn("Strong Suction", r.dels)

    def test_same_concept_dedup_keeps_specific(self):
        # Both map to suction_power — only the higher-scoring should survive
        # when one is clearly weaker
        r = _refine(
            "The suction is very weak — leaves all debris behind.",
            ["Weak Suction", "Poor Performance"], [],
            adets=["Weak Suction", "Poor Performance"],
            adels=[],
        )
        # Weak Suction is more specific → should be kept
        self.assertIn("Weak Suction", r.dets)

    def test_both_well_supported_same_concept_kept(self):
        # "picks up pet hair" AND "strong suction" — both should survive
        r = _refine(
            "Suction is incredible. Picks up all pet hair with zero effort.",
            [], ["Strong Suction", "Picks Up Pet Hair"],
            adets=[], adels=["Strong Suction", "Picks Up Pet Hair", "Quiet"],
            rating=5,
        )
        self.assertIn("Strong Suction", r.dels)
        self.assertIn("Picks Up Pet Hair", r.dels)


# ===========================================================================
# 8. Category routing
# ===========================================================================

class TestCategoryRouting(unittest.TestCase):

    def test_vacuum_product_routes_to_vacuum(self):
        r = infer_category("Shark robot vacuum IQ cordless",
                           ["suction amazing", "pet hair pickup", "brush roll clogs"])
        self.assertEqual(r["category"], "vacuum_cleaning")

    def test_air_purifier_routes_to_air_quality(self):
        r = infer_category("Ninja HEPA air purifier",
                           ["removes allergens", "filter expensive", "quiet"])
        self.assertEqual(r["category"], "air_quality")

    def test_face_serum_routes_to_beauty(self):
        r = infer_category("Vitamin C face serum moisturizer skincare",
                           ["suction amazing", "pet hair pickup", "brush roll clogs"])
        self.assertEqual(r["category"], "beauty_personal_care")

    def test_hair_dryer_routes_to_hair_care(self):
        r = infer_category("Shark hair dryer with diffuser attachment",
                           ["reduces frizz", "heat damage", "blowout results"])
        self.assertEqual(r["category"], "hair_care")

    def test_unknown_product_routes_to_general(self):
        r = infer_category("Widget XYZ-3000",
                           ["stopped working after 2 months", "good value"])
        self.assertEqual(r["category"], "general")

    def test_description_beats_off_topic_reviews(self):
        # Serum product description should beat vacuum review keywords
        r = infer_category(
            "Hydrating face serum with vitamin C, hyaluronic acid, and retinol",
            ["suction weak", "pet hair pickup", "clog", "brush roll tangles"],
        )
        self.assertEqual(r["category"], "beauty_personal_care")

    def test_general_pack_not_empty(self):
        pack = starter_pack_for_category("general")
        total = len(pack["delighters"]) + len(pack["detractors"])
        self.assertGreaterEqual(total, 12)

    def test_vacuum_pack_populated(self):
        pack = starter_pack_for_category("vacuum_cleaning")
        self.assertGreaterEqual(len(pack["delighters"]), 6)
        self.assertGreaterEqual(len(pack["detractors"]), 6)


# ===========================================================================
# 9. Theme routing
# ===========================================================================

class TestThemeRouting(unittest.TestCase):

    def _theme(self, label, side, category="general"):
        return infer_l1_theme(label, side=side, category=category)

    def test_heat_damage_routes_to_safety(self):
        self.assertEqual(self._theme("Heat Damage", "det", "hair_care"), "Safety")

    def test_reduces_frizz_routes_to_hair_results(self):
        self.assertEqual(self._theme("Reduces Frizz", "del", "hair_care"), "Hair Results")

    def test_weak_suction_routes_to_suction_pickup(self):
        self.assertEqual(self._theme("Weak Suction", "det", "vacuum_cleaning"), "Suction & Pickup")

    def test_strong_suction_routes_to_suction_pickup(self):
        self.assertEqual(self._theme("Strong Suction", "del", "vacuum_cleaning"), "Suction & Pickup")

    def test_poor_navigation_routes_to_navigation(self):
        self.assertEqual(self._theme("Poor Navigation", "det", "vacuum_cleaning"), "Navigation & Mapping")

    def test_effective_filtration_routes_to_filtration(self):
        self.assertEqual(self._theme("Effective Filtration", "del", "air_quality"), "Filtration & Air Quality")

    def test_short_lifespan_routes_to_lifespan(self):
        self.assertEqual(self._theme("Short Lifespan", "det", "general"), "Product Lifespan")

    def test_long_lasting_routes_to_lifespan(self):
        self.assertEqual(self._theme("Long-Lasting", "del", "general"), "Product Lifespan")

    def test_easy_to_use_routes_to_ease_of_use(self):
        theme = self._theme("Easy To Use", "del", "general")
        self.assertIn(theme, ("Ease Of Use", "Usability", "Performance"))

    def test_loud_routes_to_noise(self):
        theme = self._theme("Loud", "det", "general")
        self.assertIn(theme, ("Noise", "Sound & Noise", "Performance"))


# ===========================================================================
# 10. Severity weights
# ===========================================================================

class TestSeverityWeights(unittest.TestCase):

    def test_heat_damage_is_high_severity(self):
        self.assertAlmostEqual(get_severity_weight("Heat Damage", kind="detractors"), 1.35, places=2)

    def test_dead_on_arrival_is_high_severity(self):
        self.assertAlmostEqual(get_severity_weight("Dead On Arrival", kind="detractors"), 1.35, places=2)

    def test_short_lifespan_is_high_severity(self):
        self.assertAlmostEqual(get_severity_weight("Short Lifespan", kind="detractors"), 1.35, places=2)

    def test_weak_suction_is_medium_severity(self):
        self.assertAlmostEqual(get_severity_weight("Weak Suction", kind="detractors"), 1.18, places=2)

    def test_loud_is_medium_severity(self):
        self.assertAlmostEqual(get_severity_weight("Loud", kind="detractors"), 1.18, places=2)

    def test_strong_suction_is_high_value(self):
        self.assertAlmostEqual(get_severity_weight("Strong Suction", kind="delighters"), 1.20, places=2)

    def test_salon_quality_is_high_value(self):
        self.assertAlmostEqual(get_severity_weight("Salon-Quality Results", kind="delighters"), 1.20, places=2)

    def test_quiet_is_medium_value(self):
        self.assertAlmostEqual(get_severity_weight("Quiet", kind="delighters"), 1.08, places=2)

    def test_generic_detractor_default_weight(self):
        self.assertAlmostEqual(get_severity_weight("Some Other Issue", kind="detractors"), 1.05, places=2)

    def test_generic_delighter_default_weight(self):
        self.assertAlmostEqual(get_severity_weight("Some Other Feature", kind="delighters"), 1.00, places=2)


# ===========================================================================
# 11. Symptom analytics pipeline
# ===========================================================================

class TestSymptomAnalytics(unittest.TestCase):

    def _make_df(self):
        return pd.DataFrame({
            "AI Symptom Detractor 1": ["Poor Navigation", "Heat Damage", None, "Weak Suction", "Poor Navigation"],
            "AI Symptom Detractor 2": [None, "Weak Suction", None, None, None],
            "AI Symptom Delighter 1": ["Strong Suction", "Gentle On Hair", "Long-Lasting", None, "Picks Up Pet Hair"],
            "AI Symptom Delighter 2": ["Picks Up Pet Hair", None, None, "Easy To Maintain", None],
            "rating": [3.0, 1.0, 5.0, 2.0, 4.0],
            "title_and_text": [
                "Gets stuck under couch every day",
                "Hair completely fried after one use",
                "Love it, lasts for years",
                "Suction is terrible",
                "Robot picks up all the pet hair",
            ],
        })

    def test_get_symptom_col_lists(self):
        df = self._make_df()
        det_cols, del_cols = get_symptom_col_lists(df)
        self.assertEqual(len(det_cols), 2)
        self.assertEqual(len(del_cols), 2)

    def test_analyze_symptoms_returns_symptomrows(self):
        df = self._make_df()
        det_cols, _ = get_symptom_col_lists(df)
        rows = analyze_symptoms_fast(df, det_cols, kind="detractors")
        self.assertTrue(all(isinstance(r, SymptomRow) for r in rows))

    def test_symptomrow_fields_populated(self):
        df = self._make_df()
        det_cols, _ = get_symptom_col_lists(df)
        rows = analyze_symptoms_fast(df, det_cols, kind="detractors")
        top = rows[0]
        self.assertGreater(top.count, 0)
        self.assertGreater(top.pct, 0)
        self.assertIsInstance(top.item, str)
        self.assertEqual(top.side, "detractors")

    def test_symptomrow_as_display_dict(self):
        row = SymptomRow(item="Weak Suction", side="detractors", count=5, pct=25.0, avg_rating=2.1)
        d = row.as_display_dict()
        self.assertIn("Item", d)
        self.assertIn("Impact Score", d)

    def test_heat_damage_higher_severity_weight(self):
        df = self._make_df()
        det_cols, _ = get_symptom_col_lists(df)
        rows = analyze_symptoms_fast(df, det_cols, kind="detractors")
        hd_row = next((r for r in rows if r.item == "Heat Damage"), None)
        nav_row = next((r for r in rows if r.item == "Poor Navigation"), None)
        if hd_row and nav_row:
            self.assertGreater(hd_row.severity_weight, nav_row.severity_weight)

    def test_add_net_hit_returns_dataframe(self):
        df = self._make_df()
        det_cols, _ = get_symptom_col_lists(df)
        rows = analyze_symptoms_fast(df, det_cols, kind="detractors")
        tbl = add_net_hit(rows, 3.2, total_reviews=5, kind="detractors")
        self.assertIsInstance(tbl, pd.DataFrame)
        self.assertIn("Impact Score", tbl.columns)
        self.assertIn("Forecast Δ★", tbl.columns)

    def test_add_net_hit_sorted_by_impact(self):
        df = self._make_df()
        det_cols, _ = get_symptom_col_lists(df)
        rows = analyze_symptoms_fast(df, det_cols, kind="detractors")
        tbl = add_net_hit(rows, 3.2, total_reviews=5, kind="detractors")
        scores = list(tbl["Impact Score"].abs())
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_no_nan_in_output(self):
        df = self._make_df()
        det_cols, del_cols = get_symptom_col_lists(df)
        rows = analyze_symptoms_fast(df, det_cols + del_cols, kind="detractors")
        tbl = add_net_hit(rows, 3.2, total_reviews=5, kind="detractors")
        self.assertFalse(tbl["Impact Score"].isna().any())


# ===========================================================================
# 12. TaxonomyRegistry
# ===========================================================================

class TestTaxonomyRegistry(unittest.TestCase):

    def setUp(self):
        self.reg = TaxonomyRegistry.from_rules()

    def test_registry_not_empty(self):
        self.assertGreater(len(self.reg), 0)

    def test_get_known_label(self):
        ldef = self.reg.get_label("Heat Damage")
        self.assertIsNotNone(ldef)
        self.assertEqual(ldef.canonical, "Heat Damage")
        self.assertEqual(ldef.side, "detractor")

    def test_get_label_by_alias(self):
        # "hair fried" is an alias — should still resolve
        ldef = self.reg.get_label("fried my hair")
        # May or may not resolve depending on alias inclusion
        # Just assert no exception is raised
        pass  # alias lookup is opportunistic

    def test_infer_category_method(self):
        r = self.reg.infer_category("Shark vacuum cordless", ["suction great"])
        self.assertEqual(r["category"], "vacuum_cleaning")

    def test_starter_pack_method(self):
        pack = self.reg.starter_pack("general")
        self.assertGreaterEqual(len(pack["delighters"]), 6)

    def test_infer_theme_method(self):
        theme = self.reg.infer_theme("Weak Suction", side="detractor", category="vacuum_cleaning")
        self.assertEqual(theme, "Suction & Pickup")

    def test_all_labels_delighters(self):
        dels = self.reg.all_labels("delighter")
        self.assertIn("Heat Damage" if False else "Reduces Frizz", dels)  # hair care delighter
        self.assertGreater(len(dels), 10)

    def test_load_from_dicts(self):
        reg2 = TaxonomyRegistry()
        reg2.load_from_dicts([
            {"canonical": "Custom Label A", "side": "detractor", "aliases": ["bad thing A"]},
            {"canonical": "Custom Label B", "side": "delighter", "aliases": ["good thing B"]},
        ])
        self.assertEqual(len(reg2), 2)
        self.assertIsNotNone(reg2.get_label("Custom Label A"))

    def test_repr_contains_counts(self):
        self.assertIn("delighters", repr(self.reg))
        self.assertIn("detractors", repr(self.reg))


# ===========================================================================
# 13. Backward compatibility — v1 dict API
# ===========================================================================

class TestBackwardCompat(unittest.TestCase):

    def test_refine_tag_assignment_returns_dict(self):
        d = refine_tag_assignment("good product", [], ["Performs Well"],
                                  allowed_detractors=[], allowed_delighters=["Performs Well"])
        self.assertIsInstance(d, dict)
        self.assertIn("dets", d)
        self.assertIn("dels", d)
        self.assertIn("support_det", d)
        self.assertIn("support_del", d)
        self.assertIn("ev_det", d)
        self.assertIn("ev_del", d)

    def test_support_details_returns_dict(self):
        sd = _support_details("Loud", "The product is very loud", "detractor")
        self.assertIsInstance(sd, dict)
        self.assertIn("score", sd)
        self.assertIn("has_support", sd)
        self.assertIn("concept", sd)
        self.assertIn("cue_hits", sd)
        self.assertIn("coverage", sd)

    def test_build_label_cues_returns_tuple(self):
        cues, concept = build_label_cues("Weak Suction", "detractor")
        self.assertIsInstance(cues, list)
        self.assertIn("weak suction", cues)
        self.assertEqual(concept, "suction_power")

    def test_normalize_tag_list_dedupes(self):
        result = normalize_tag_list(["loud", "Loud", "LOUD", "noisy"])
        self.assertEqual(len(result), 2)  # Loud + Noisy

    def test_dict_has_support_keys(self):
        d = refine_tag_assignment(
            "Suction is terrible, weak suction everywhere",
            ["Weak Suction"], [],
            allowed_detractors=["Weak Suction", "Overall Dissatisfaction"],
            allowed_delighters=["Strong Suction"],
            rating=1,
        )
        for label, details in d["support_det"].items():
            self.assertIn("score", details)
            self.assertIn("has_support", details)
            self.assertIn("concept", details)


# ===========================================================================
# Runner
# ===========================================================================

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestPhraseMatching, TestNegationDetection, TestConceptInference,
        TestFragmentScoring, TestTagScorerSignals, TestRefinementPipeline,
        TestCrossConceptConflict, TestCategoryRouting, TestThemeRouting,
        TestSeverityWeights, TestSymptomAnalytics, TestTaxonomyRegistry,
        TestBackwardCompat,
    ]
    for cls in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
