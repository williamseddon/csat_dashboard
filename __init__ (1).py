import io

import pandas as pd

from review_analyst.normalization import normalize_uploaded_df, read_uploaded_file


class FakeUpload:
    def __init__(self, name: str, raw: bytes):
        self.name = name
        self._raw = raw

    def getvalue(self):
        return self._raw


def test_normalize_uploaded_df_preserves_local_symptoms_and_meta_when_enabled():
    raw = pd.DataFrame(
        {
            "Review ID": ["r1", "r2"],
            "Review Text": ["Loud but easy to clean", "Love it"],
            "Rating": [2, 5],
            "Symptom 1": ["noise issue", "NOT MENTIONED"],
            "AI Symptom Delighter 1": ["easy cleanup", "great performance"],
            "Safety": ["Concern", "Not Mentioned"],
            "Reliability": ["Negative", "Positive"],
            "# of Sessions": ["2–3", "10+"],
        }
    )

    excluded = normalize_uploaded_df(raw, include_local_symptomization=False)
    included = normalize_uploaded_df(raw, include_local_symptomization=True)

    assert "Symptom 1" not in excluded.columns
    assert "AI Symptom Delighter 1" not in excluded.columns

    assert "Symptom 1" in included.columns
    assert "AI Symptom Delighter 1" in included.columns
    included_by_id = included.set_index("review_id")
    assert included_by_id.loc["r1", "Symptom 1"] == "Noise Issue"
    assert pd.isna(included_by_id.loc["r2", "Symptom 1"])
    assert included_by_id.loc["r1", "AI Symptom Delighter 1"] == "Easy Cleanup"
    assert included_by_id.loc["r1", "AI Safety"] == "Concern"
    assert included_by_id.loc["r2", "AI Reliability"] == "Positive"
    assert included_by_id.loc["r1", "AI # of Sessions"] == "2–3"


def test_read_uploaded_file_selects_review_sheet_and_keeps_local_symptoms():
    summary = pd.DataFrame({"Metric": ["Rows"], "Value": [2]})
    reviews = pd.DataFrame(
        {
            "Review ID": ["r1", "r2"],
            "Review Text": ["Too loud", "Worth it"],
            "Rating": [2, 5],
            "Symptom 1": ["noise", None],
            "AI Symptom Delighter 1": [None, "easy setup"],
            "Safety": ["Concern", "Not Mentioned"],
        }
    )
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        reviews.to_excel(writer, sheet_name="Reviews", index=False)
    upload = FakeUpload("local_symptoms.xlsx", bio.getvalue())

    normalized = read_uploaded_file(upload, include_local_symptomization=True)
    normalized_by_id = normalized.set_index("review_id")

    assert normalized.attrs.get("source_sheet_name") == "Reviews"
    assert "Symptom 1" in normalized.columns
    assert "AI Symptom Delighter 1" in normalized.columns
    assert normalized_by_id.loc["r1", "review_text"] == "Too loud"
    assert normalized_by_id.loc["r2", "review_text"] == "Worth it"
    assert normalized_by_id.loc["r1", "Symptom 1"] == "Noise"
    assert normalized_by_id.loc["r2", "AI Symptom Delighter 1"] == "Easy Setup"
    assert normalized_by_id.loc["r1", "AI Safety"] == "Concern"
