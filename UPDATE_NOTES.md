# Starwalk Review Analyst — Speed, Accuracy, and Compare Update

This build keeps the local database workflow but sharpens the app around four priorities: faster repeated loads, more informative loading states, less AI guessing, and cleaner executive compare views.

## What changed in this build

### Faster local database workflow
- Local-database selections now save to a persistent **workspace extract cache** on disk after the first successful load.
- Re-opening the same base model / category / SKU selection can now reuse that cached extract instead of hydrating everything from SQLite again.
- The builder shows a more explicit **speed plan** and database footprint, including:
  - SQLite file size
  - warm extract count
  - warm extract cache size
- Local SQLite now maintains stronger query indexes plus `ANALYZE` / `PRAGMA optimize` so the synced database stays responsive as it grows.
- Workspace load progress now reports whether the app reused a warm cache or loaded directly from SQLite, along with approximate row throughput.

### More informative AI retrieval
- AI now uses entity grounding before answering so it is better at:
  - base model numbers
  - SKU / product IDs
  - product names
  - likely misspellings
- When the workspace is backed by the local database, AI now gets extra context about whether the workspace is a loaded slice of a larger synced selection.
- When a strong entity match is found, AI can pull a small amount of extra supporting evidence from the synced local database instead of guessing.
- Added a subtle in-product note that AI can make mistakes and should be verified against source reviews.

### New Action Center compare view
- Added a new **Brands** board to Action Center.
- It includes:
  - brand scale vs satisfaction view
  - brand rating comparison
  - brand trend line
  - brand × category comparison matrix
  - clean brand performance table for executive review

### UI polish
- Plot legends are pushed away from chart titles more aggressively.
- AI response area now has more bottom spacing and cleaner message padding.
- The local-database area is more explicit about how to keep the app fast at scale.

# Starwalk Review Analyst — Action Center Update

This build upgrades the app from a file analyzer into a local-database-native review command center.

## What is new

### Action Center
- New workspace tab for:
  - category comparison across Shark and Ninja
  - country comparison
  - base-model family mapping
  - alert board with recommended actions
- Supports:
  - review counts
  - average rating
  - low-star share
  - organic share
  - trend lines
  - action segments: Fix now, Protect, Scale, Watch, Stable

### Better source mapping
- `IncentivizedReview (CDV)` is now mapped correctly.
- Reviews now roll into:
  - `Seeded / Incentivized`
  - `Organic`
  - `Syndicated`
- `Campaign ID` is now translated into a more useful acquisition channel field.
- `Moderation Status` is normalized into buckets like:
  - Approved
  - Pending
  - Rejected
  - Removed

### Smarter defaults
- Moderation defaults to **Approved + Pending**.
- Rejected / Removed reviews stay hidden unless you opt in.

### Better product selection UX
- The local database builder now includes **Quick product search**.
- Search works across:
  - base model number
  - product / SKU ID
  - product name
  - mapped category
- You can apply a search hit directly to the builder filters.

### More useful symptom drilldowns
- Detractor / delighter tables now support drilldown by:
  - country
  - locale
  - reviewer location
  - base model
  - product / SKU
  - category / subcategory
  - acquisition channel
  - moderation
  - review type

### Review Explorer trend detector
- New trend detector for spotting movers before reading row-by-row.
- Compare recent periods vs. a baseline by:
  - category
  - country
  - base model
  - review origin

## Recommended workflow
1. Drop the latest merged reviews file into the local database `incoming/reviews` folder.
2. Drop the latest SKU mapping workbook into `incoming/sku_mapping`.
3. Sync the local database.
4. Use the **Quick product search** or the base-model selector.
5. Open **Action Center** for cross-category and cross-country comparisons.
6. Use **Review Explorer** for row-level review investigation.
7. Use **Symptomizer** when you want structured theme analysis.

## Performance notes
- Default local-database workspace load was reduced to **25,000 newest reviews** to keep the interactive workspace faster.
- Action Center can query the synced local SQLite database directly, so it can compare larger scopes without requiring every row to be loaded into the workspace first.
