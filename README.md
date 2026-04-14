"""review_analyst — Symptomizer voice-of-customer analysis engine.

Package layout
--------------
tag_quality.py    Core label refinement engine (TaggerConfig, TagRefiner, …)
taxonomy.py       Label catalog, category inference, theme routing (TaxonomyRegistry)
symptoms.py       Analytics layer: frequency tables, impact scoring (SymptomRow)
normalization.py  Review ingestion for BV, PowerReviews, Okendo, uploaded files
analytics.py      Review-level metrics: rating distribution, trends, cohorts
"""
