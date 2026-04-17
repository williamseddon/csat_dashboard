# Local Central Review Database

This build can turn your latest review export and master SKU mapping file into a local SQLite database that the app can filter by brand, category, subcategory, base model number, and product/SKU.

## Recommended setup

Use Excel and CSV as the input/source files, and use SQLite as the actual runtime database.

- **Input files (easy to replace):**
  - `incoming/reviews/` → latest merged review export (`.csv`, `.xlsx`, `.xlsm`, `.xls`)
  - `incoming/sku_mapping/` → latest master SKU mapping file (`.xlsx`, `.xlsm`, `.xls`, `.csv`)
- **Runtime database (fast filtering in the app):**
  - `central_review_database.sqlite3`
- **Optional Excel snapshot for sharing/checking:**
  - `exports/central_review_database_snapshot.xlsx`

## Why not use one giant Excel file as the database?

A combined Excel workbook is a good **input/staging file**, but it is not the best runtime database for this app.

SQLite is better for the live database because it is:

- much faster for filtering and joins
- more stable for hundreds of thousands of reviews
- easy to rebuild from the newest files
- easier to keep clean than a constantly overwritten workbook

The app still accepts Excel as an input source. The recommended pattern is:

1. Drop the newest review export into `incoming/reviews/`
2. Drop the newest SKU mapping file into `incoming/sku_mapping/`
3. Let the app sync those files into SQLite
4. Use the app UI to filter by base model, category, subcategory, or product ID

## Base model linking

The importer enriches each review with catalog fields such as:

- `base_model_number`
- `master_item`
- `mapped_brand`
- `mapped_category`
- `mapped_subcategory`
- `mapped_subsub_category`
- `mapped_region`
- `mapped_item_status`
- `mapped_lifecycle_phase`

This means a family like **HD400** can link child SKUs and accessories while still preserving the exact product ID for each review.

## App workflow

In the workspace builder, open the **Local central review database** panel.

From there you can:

- point the app at a local database folder
- auto-sync when newer files are detected
- manually force a rebuild
- export the summary workbook snapshot
- filter the database by:
  - mapped brand
  - mapped category
  - mapped subcategory
  - base model number
  - product/SKU

## Folder structure

The app creates this structure automatically:

```text
.starwalk_data/
  local_review_database/
    incoming/
      reviews/
      sku_mapping/
    exports/
    central_review_database.sqlite3
```

You can also choose a custom local database root inside the app.

## Optional CLI sync

You can rebuild from the command line too:

```bash
python scripts/sync_local_review_database.py --export-snapshot
```

Optional flags:

```bash
python scripts/sync_local_review_database.py --root /path/to/local_review_database --force --chunk-size 50000 --export-snapshot
```
