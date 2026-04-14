# Symptomizer deep dive

## What the engine is today

The uploaded package is not a single “human-like classifier.” It is a chain:

1. **Taxonomy generation** from a sampled subset of reviews plus seeded category and product-knowledge labels.
2. **Batch OpenAI tagging** against the allowed taxonomy with evidence snippets.
3. **Local rule-based refinement** that canonicalizes labels, adds/removes tags, resolves conflicts, injects broad universal labels, and computes QA/edit metrics.

That architecture can work, but the current chain is flattening nuance in the middle and then masking misses at the end.

## Highest-impact blockers

### 1) The taxonomy is sample-driven and seed-driven, not corpus-driven

Relevant code:
- `app.py:5996-6003`
- `app.py:6133-6169`
- `review_analyst/taxonomy.py:432-448`

Observed behavior:
- Taxonomy building uses at most **60 truncated reviews**.
- It mixes in **generic seeded labels** from product knowledge.
- Seeded category-driver labels are kept even when they have **0 review hits**.
- If no product-specific labels survive, the fallback path still keeps some product-specific labels with **0 review hits**.

Why this hurts “calculated human” behavior:
- A careful analyst starts from what the corpus actually says.
- This engine starts from what it thinks the product/category *should* say.
- That leads to bloated or misaligned catalogs, which then forces the classifier to choose from a noisy menu.

### 2) The engine only reads title + body, not the full review context

Relevant code:
- `app.py:5299-5308`

Observed behavior:
- `_symptomizer_review_text()` uses only `title_and_text`, or `title + review_text`.
- It does not directly feed structured fields like pros/cons, variant/size, reviewer attributes, or other context into classification.

Why this hurts:
- Human classifiers use every available clue.
- Variant-sensitive issues like fit, accessories, attachment swaps, battery pack version, or “used once vs 3 months” are easier to classify when those fields are explicit.

### 3) Incremental runs can overwrite good AI tags

Relevant code:
- `app.py:5911`
- `app.py:5047-5054`

Observed behavior:
- The batch payload includes `needs_delighters` and `needs_detractors`, but the prompt/runtime does not actually enforce them.
- `_write_ai_symptom_row()` clears **all** AI symptom columns for the row before writing the new result.

Why this hurts:
- If you rerun only because one side is missing, the other AI side can still be reauthored or wiped.
- That makes the engine unstable instead of “calculated.”

### 4) The local refiner is lexical, not semantic

Relevant code:
- `review_analyst/tag_quality.py:392-414`
- `review_analyst/tag_quality.py:474-575`
- `review_analyst/tag_quality.py:638-688`

Observed behavior:
- Support is scored from substring matches, token overlap, simple fragment splitting, and concept synonym bundles.
- It is not doing true semantic grounding or span classification.

Why this hurts:
- Humans handle paraphrase, negation, comparison, exception clauses, and absence-of-complaint language.
- Lexical scoring does not.

### 5) Concept inference is order-sensitive and cross-contaminates labels

Relevant code:
- `review_analyst/tag_quality.py:429-450`

Observed behavior:
- `_infer_concept()` chooses the first concept whose keyword appears in the label or aliases.
- `build_label_cues()` then imports the synonym bundle for that concept.

Consequence:
- Labels containing generic words like **easy** or **hard** can inherit cues from the wrong concept.
- Example: `Easy To Clean` can inherit generic ease-of-use cues instead of cleaning-only cues.

### 6) Negation handling is brittle and under-penalized

Relevant code:
- `review_analyst/tag_quality.py:410-414`
- `review_analyst/tag_quality.py:519-541`

Observed behavior:
- Negation only catches direct patterns like `not durable`.
- It misses forms like `not as quiet as expected` or `no issues with noise`.
- Even when negation is detected, the penalty is small enough that positive cues can still win.

Why this hurts:
- This is exactly where humans outperform naive taggers.
- Many review sentences are mixed or comparative.

### 7) Broad universal labels can hide missed specifics

Relevant code:
- `review_analyst/tag_quality.py:656-703`

Observed behavior:
- The refiner injects universal-neutral labels like `Overall Satisfaction`, `Good Value`, `Easy To Use`, `Reliable`, etc.

Why this hurts:
- The result often looks superficially plausible even when the specific symptom was missed.
- That makes the output feel broad and safe, not precise and calculated.

### 8) “Accuracy” is edit distance from the model’s own baseline, not human truth

Relevant code:
- `app.py:8277-8278`
- `review_analyst/tag_quality.py:791-825`

Observed behavior:
- QA baseline is created from the engine’s first pass.
- Accuracy starts at 100% because it is comparing the baseline to itself.
- Later human edits lower “accuracy,” even if the human made the output more correct.

Why this hurts:
- This metric cannot tell you whether the engine is human-like.
- It only tells you how much the current state differs from the original model output.

## Controlled examples from the local refiner

I ran the deterministic refiner on a few targeted review sentences. These are the exact kinds of cases that separate a careful human reader from a lexical tagger.

1. `It's quieter than my old one, but still loud on high.`
   - Expected: `Quiet` + `Loud`
   - Actual: `Quiet` + `Loud`
   - This is a case the current logic handles reasonably well.

2. `No issues with noise.`
   - Expected: no noise complaint
   - Actual: `Loud` + `Overall Satisfaction`
   - Failure mode: absence-of-complaint phrasing becomes a complaint.

3. `Not as quiet as expected.`
   - Expected: noise complaint / not `Quiet`
   - Actual: `Quiet`
   - Failure mode: comparative negation flips polarity.

4. `Great value, but not durable.`
   - Expected: `Good Value` + `Unreliable`
   - Actual: `Good Value` + `Reliable`
   - Failure mode: negated positive cue still scores as positive.

5. `The battery does not last and the app will not connect.`
   - Expected: `Short Battery Life` + `Connectivity Issues`
   - Actual: no tags
   - Failure mode: lexical recall gap on common paraphrases.

6. `Easy to use once you figure out the setup.`
   - Expected: `Easy To Use` plus maybe setup friction nuance
   - Actual: `Easy To Use` + `Easy To Clean` + `Easy Setup`
   - Failure mode: cue contamination across concepts.

7. `Works great but cleanup takes forever.`
   - Expected: `Overall Satisfaction` + `Hard To Clean`
   - Actual: `Overall Satisfaction`
   - Failure mode: broad fallback hides the missed specific symptom.

## What to change first

### Immediate fixes

1. **Stop wiping all AI columns on every rewrite.**
   Merge per-side updates instead of clearing both sides.

2. **Respect `needs_delighters` / `needs_detractors` in the runtime.**
   If a side is not being processed, preserve it.

3. **Remove zero-hit seeded labels from the active taxonomy.**
   Seed candidates can be suggestions, but not active labels unless supported.

4. **Turn concept inference from substring heuristics into explicit mapping.**
   Do not let `easy` route `Easy To Clean` into ease-of-use logic.

5. **Harden negation/comparison rules.**
   Add patterns for:
   - `no issues with X`
   - `not as X as expected`
   - `less X than`
   - `only on high`
   - `once it connects`
   - `works great but ...`

### Medium-term fixes

6. **Move to span-first classification.**
   Extract supporting spans or clauses first, then map spans to taxonomy labels.

7. **Build the taxonomy from corpus mining, not only samples.**
   Use the full review set for candidate phrase mining and support counts.

8. **Separate broad sentiment from symptom tagging.**
   Overall satisfaction/dissatisfaction should be a separate output channel, not mixed into symptom labels.

9. **Add abstention and confidence.**
   A careful human sometimes says “not enough evidence.” The engine should too.

### Measurement fixes

10. **Create a human-labeled gold set.**
    Measure precision, recall, polarity confusion, and span support against human consensus.

11. **Use human edits as supervised feedback, not as a penalty against model “accuracy.”**

## Bottom line

The biggest hidden problem is not the OpenAI call itself. It is the deterministic layer wrapped around it:
- taxonomy seeding that can keep unsupported labels,
- lexical post-processing that confuses concepts,
- weak negation/comparison handling,
- and a QA metric that does not measure truth.

That is why the engine can look organized and still fail to read like a calculated human.
