# StarWalk single-file Beta entry point

`app.py` is now a consolidated single-file Streamlit entry point that restores the richer Review Prompt and Symptomizer flows from the reference UI, adds filtered-view export criteria, collapses sidebar filter groups by default, includes a richer Social Listening Beta demo with mocked FlexStyle VOC, viral posts, top comments, and Meltwater-style query framing, applies a lava-lamp style animated background, defaults runtime data loading back to the single-file path, hardens Excel export sizing, and recognizes the new `sharkninja.co.uk` UK/EU host pattern.

`app_split_backup.py` keeps the previous split entry point as a safety copy.

---

# SharkNinja Review Analyst - split package refactor

This package keeps the scraper and data layer split into dedicated modules while restoring much more of the original monolith's look and feel. The single-file app now also includes a social-only Beta route that can be opened without any review uploads.

## What moved where

- `app.py`
  - Streamlit entry point and restored UI shell
  - workspace builder, hero header, active filter summary, dashboard, explorer, AI tab routing
- `review_analyst/connectors.py`
  - Bazaarvoice, PowerReviews, Okendo / CurrentBody, Ulta, Hoka, and file-loading logic
  - this is the main file to edit when retailer scraping or review endpoint handling changes
- `review_analyst/normalization.py`
  - review flattening and dataframe normalization
- `review_analyst/analytics.py`
  - metrics, rating distributions, cohorts, trends, cumulative regional rating trend helpers
- `review_analyst/ui.py`
  - shared styling, cards, nav, review cards, chart styling helpers
- `review_analyst/symptoms.py`
  - symptom column detection and basic detractor / delighter analytics helpers
- `review_analyst/openai_service.py`
  - OpenAI client and AI analyst helper
- `review_analyst/export.py`
  - workbook export for the current workspace
- `review_analyst/repository.py`
  - in-memory repo plus SQLAlchemy repo for future database integration
- `review_analyst/state.py`
  - Streamlit session defaults and reset helpers
- `review_analyst/config.py`
  - constants and site/provider configuration

## Ulta + export hardening included

The Ulta / PowerReviews path is now safer in both the runtime and export layers:


- preferring PowerReviews page ids like `xlsImpprod...` and `pimprod...`
- passing `page_locale` correctly
- handling more PowerReviews payload shapes
- paging from PowerReviews server metadata more consistently
- hardening workbook autosizing so duplicate column labels or nested values no longer crash the current-view export


## SharkNinja UK/EU host fix included

The newer `sharkninja.co.uk` host is now treated as a SharkNinja UK/EU Bazaarvoice site, so the app will use the UK/EU review path instead of falling back to the US connector logic.

## CurrentBody / Okendo support included

The CurrentBody support is handled in `review_analyst/connectors.py` and `review_analyst/normalization.py` by:

- accepting direct Okendo review API URLs
- detecting embedded Okendo endpoints on product pages when present
- converting CurrentBody Shopify product handles into product ids through the Shopify `.js` product endpoint
- paging through Okendo `nextUrl` cursors until all reviews are loaded
- flattening Okendo review payloads into the same dataframe shape used elsewhere in the app

## Social Listening Beta demo included

The single-file `app.py` now includes a mocked **FlexStyle social listening** experience designed to preview a future Meltwater-backed workflow before live social data is connected. The demo includes:

- a social-only entry path that works without uploaded reviews
- mocked FlexStyle viral posts and top comments with engagement
- Voice-of-Consumer callouts for filter cleaning, filter-door friction, and FlexStyle vs Dyson comparison themes
- a chat-style demo response area for social follow-up questions

## What was restored to feel closer to the original

- original-style Inter-based visual theme and card system
- richer workspace builder and hero header
- active filter summary pills
- button-based workspace navigation instead of plain tabs
- cumulative average rating over time chart with regional lines and optional volume bars
- expanded secondary dashboard views for rating mix, cohorts, sentiment, markets, and review depth
- richer review explorer cards and compact pagination
- improved AI Analyst layout and quick actions

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Database-ready seam

`review_analyst/repository.py` contains:

- `DataFrameReviewRepository`
- `SQLAlchemyReviewRepository`

That makes it easier to move from an in-memory dataframe to SQLite or Postgres later.

## Still worth porting next

The split package now looks much closer to the original across the main workspace, but the full original execution flows for Review Prompt and Symptomizer still need their own module pass.

Suggested next files:

- `review_analyst/prompting.py`
- `review_analyst/symptomizer_runtime.py`
- `review_analyst/ui_prompt.py`
- `review_analyst/ui_symptomizer.py`
