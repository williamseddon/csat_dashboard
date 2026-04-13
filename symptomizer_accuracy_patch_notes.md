# Symptomizer accuracy patch notes

This patch focuses on reducing incorrect tags and false positives.

## What changed

### Tag accuracy
- Improved negation handling for phrases like:
  - `no issues with noise`
  - `not as quiet as expected`
  - `not durable`
- Added support for polarity flips where a negated positive phrase should become a detractor.
- Reduced concept bleed between similar labels such as:
  - `Easy To Use`
  - `Easy To Clean`
  - `Easy Setup`
- Tightened broad overall-sentiment fallback so `Overall Satisfaction` is not added as easily in mixed reviews.
- Added specific handling for cleanup/setup friction patterns like:
  - `cleanup takes forever`
  - `once you figure out the setup`

### Taxonomy quality
- Seeded category-driver labels now need actual review support.
- Zero-support product-specific fallback labels are no longer kept just because the bucket is empty.

### Stability on reruns
- Partial reruns now preserve the untouched side instead of clearing all AI symptom columns.
- Batch writes respect `Needs_Delighters` and `Needs_Detractors` when saving results.

### Review context
- The symptomizer now assembles richer review context from additional useful text fields instead of relying only on `title_and_text` or `title + review_text`.

## Validation
- Full automated test suite passed after the patch.
- Added targeted tests for the main bad-tag cases above.
