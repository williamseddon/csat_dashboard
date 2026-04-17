# Starwalk Review Analyst — Full-Corpus + Dyson Mapping Update

This build focuses on making the local database flow smoother, faster, and less restrictive when you want to work from the full synced corpus.

It also adds native support for Dyson review workbooks and Dyson product mapping files so they can land in the same central SQLite database as the merged Bazaarvoice / MDM flow.

## What changed in this build

### Faster local database setup and selection checks
- Local database readiness is now stricter: the app checks for the required synced tables before treating the SQLite file as usable.
- Selection counts now read from the much smaller **product directory** tables instead of scanning the full review table, so the builder updates more quickly.
- Added stronger SQLite tuning and extra indexes for the filters the app uses most often.
- Workspace loads now read from a slimmer SQLite projection that drops the heaviest raw JSON fields before hydrating the interactive workspace.
- Review ingestion writes larger batches during sync for a smoother rebuild.

### Multi-select builder filters
- The local database builder now supports **multi-select** for:
  - mapped brand
  - mapped category
  - mapped subcategory
  - base model number
  - specific product / SKU
- Filter options stay context-aware and update off the current intersection.
- Added a **Clear filters** action plus a compact chip summary of the active selection.

### Full-corpus workspace loading
- The default local-database load profile is now **All matched**.
- You can still switch to:
  - Fast
  - Balanced
  - Deep
  - Custom
- This removes the old 25,000-review default cap in the builder and makes it much easier to load the entire filtered selection when that is what you want.

### Better preload behavior
- Preload now works with the new multi-select selections and full-corpus mode.
- Warmed selections continue to save to disk so repeat opens can reuse cached extracts.

### Dyson review + mapping support
- The local database sync now scans `incoming/reviews/dyson/` for Dyson review workbooks named like `dyson__tp07-whsv.xlsx`.
- Dyson mapping workbooks placed in `incoming/sku_mapping/dyson/` are normalized into the same SKU catalog as the MDM mapping file.
- Dyson filenames are treated as product IDs and mapped to Dyson model codes (for example `TP07`, `SV53`) as the base model numbers in SQLite.
- Dyson review rows default to **Approved** moderation so they work naturally with the app's moderation defaults and dashboards.
- The UI now shows Dyson source counts in the local database setup area so it is clearer what will be included in the next sync.

## Recommended workflow
1. Drop the latest merged Bazaarvoice review export into `incoming/reviews`.
2. Drop the latest MDM / SKU mapping workbook into `incoming/sku_mapping`.
3. Add any Dyson review workbooks into `incoming/reviews/dyson/`.
4. Add the Dyson mapping workbook into `incoming/sku_mapping/dyson/`.
5. Sync the local database.
6. Use quick product search and/or the new multi-select filters.
7. Leave the builder on **All matched** when you want the full selection in the workspace.
8. Switch to Fast or Balanced when you only need a lighter slice for quicker interactive work.
