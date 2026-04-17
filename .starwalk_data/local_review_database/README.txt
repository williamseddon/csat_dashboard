Drop your latest merged review export into incoming/reviews/
Drop your latest master SKU mapping workbook into incoming/sku_mapping/

Then either:
1. Open the app and use the Local central review database panel to sync, or
2. Run:

   python scripts/sync_local_review_database.py --export-snapshot

New in this build:
- Quick product search for base model numbers, SKU IDs, and product names
- Base-model-first workflow support
- Action Center dashboards for categories, countries, base models, and alerts
- Moderation defaults to Approved + Pending
