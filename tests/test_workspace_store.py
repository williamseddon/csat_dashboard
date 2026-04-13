import pandas as pd

from review_analyst.workspace_store import (
    count_workspace_records,
    delete_workspace_record,
    list_workspace_records,
    load_workspace_record,
    rename_workspace_record,
    save_workspace_record,
)


def test_workspace_store_round_trip(tmp_path):
    db_path = tmp_path / "workspace_store.sqlite3"
    reviews = pd.DataFrame(
        {
            "review_id": ["r1", "r2"],
            "review_text": ["Too loud", "Easy to clean"],
            "rating": [2, 5],
            "content_locale": ["en_US", "en_GB"],
            "AI Symptom Detractor 1": ["Noise", None],
            "AI Symptom Delighter 1": [None, "Easy Cleanup"],
        }
    )
    dataset_payload = {
        "source_type": "uploaded",
        "source_label": "demo.xlsx",
        "source_urls": [],
        "summary": {
            "product_url": "",
            "product_id": "UPLOAD_TEST",
            "total_reviews": 2,
            "page_size": 2,
            "requests_needed": 1,
            "reviews_downloaded": 2,
        },
    }
    state_payload = {
        "sym_delighters": ["Easy Cleanup"],
        "sym_detractors": ["Noise"],
        "sym_processed_rows": [{"idx": 0, "wrote_dets": ["Noise"], "wrote_dels": []}],
    }

    workspace_id = save_workspace_record(
        workspace_name="Demo workspace",
        source_type="uploaded",
        source_label="demo.xlsx",
        source_key="uploaded::demo.xlsx",
        reviews_df=reviews,
        dataset_payload=dataset_payload,
        state_payload=state_payload,
        metadata={"note": "first save"},
        product_id="UPLOAD_TEST",
        product_url="",
        latest_review_date="2026-04-10",
        symptomized=True,
        symptom_engine="gpt-5.4-mini",
        symptomized_at="2026-04-11 01:10:00 UTC",
        db_path=str(db_path),
    )

    assert workspace_id
    assert count_workspace_records(str(db_path)) == 1

    rows = list_workspace_records(db_path=str(db_path))
    assert len(rows) == 1
    assert rows[0]["workspace_name"] == "Demo workspace"
    assert rows[0]["symptomized"] is True
    assert rows[0]["symptom_engine"] == "gpt-5.4-mini"
    assert rows[0]["review_count"] == 2
    assert rows[0]["symptomized_count"] == 2
    assert rows[0]["region_count"] == 2
    assert abs(float(rows[0]["symptomized_pct"]) - 1.0) < 1e-9
    assert abs(float(rows[0]["avg_rating"]) - 3.5) < 1e-9

    loaded = load_workspace_record(workspace_id, db_path=str(db_path))
    assert loaded["workspace_name"] == "Demo workspace"
    assert loaded["dataset_payload"]["summary"]["product_id"] == "UPLOAD_TEST"
    assert loaded["state_payload"]["sym_delighters"] == ["Easy Cleanup"]
    assert loaded["metadata"]["region_count"] == 2
    assert loaded["metadata"]["region_labels"] == ["UK", "USA"]
    assert list(loaded["reviews_df"]["review_id"].astype(str)) == ["r1", "r2"]
    assert loaded["reviews_df"].set_index("review_id").loc["r1", "AI Symptom Detractor 1"] == "Noise"

    delete_workspace_record(workspace_id, db_path=str(db_path))
    assert count_workspace_records(str(db_path)) == 0


def test_workspace_store_source_upsert_updates_existing_record(tmp_path):
    db_path = tmp_path / "workspace_store.sqlite3"
    reviews = pd.DataFrame({"review_id": ["r1"], "review_text": ["Love it"], "rating": [5]})
    workspace_id_1 = save_workspace_record(
        workspace_name="Demo workspace",
        source_type="uploaded",
        source_label="demo.xlsx",
        source_key="uploaded::demo.xlsx",
        reviews_df=reviews,
        dataset_payload={"summary": {"product_id": "UPLOAD_TEST"}},
        db_path=str(db_path),
        allow_source_upsert=True,
    )
    workspace_id_2 = save_workspace_record(
        workspace_name="Demo workspace updated",
        source_type="uploaded",
        source_label="demo.xlsx",
        source_key="uploaded::demo.xlsx",
        reviews_df=reviews.assign(rating=[4]),
        dataset_payload={"summary": {"product_id": "UPLOAD_TEST"}},
        db_path=str(db_path),
        allow_source_upsert=True,
    )

    assert workspace_id_1 == workspace_id_2
    rows = list_workspace_records(db_path=str(db_path))
    assert len(rows) == 1
    assert rows[0]["workspace_name"] == "Demo workspace updated"
    loaded = load_workspace_record(workspace_id_2, db_path=str(db_path))
    assert int(loaded["reviews_df"].iloc[0]["rating"]) == 4


def test_workspace_store_search_and_filters_include_engine(tmp_path):
    db_path = tmp_path / "workspace_store.sqlite3"
    reviews = pd.DataFrame({"review_id": ["r1"], "review_text": ["Solid"], "rating": [4]})
    save_workspace_record(
        workspace_name="Alpha batch",
        source_type="uploaded",
        source_label="alpha.xlsx",
        source_key="uploaded::alpha.xlsx",
        reviews_df=reviews,
        dataset_payload={"summary": {"product_id": "ALPHA_1"}},
        symptomized=True,
        symptom_engine="gpt-5.4-mini",
        latest_review_date="2026-04-11",
        db_path=str(db_path),
    )
    save_workspace_record(
        workspace_name="Beta batch",
        source_type="bazaarvoice",
        source_label="https://example.com/product",
        source_key="bazaarvoice::beta",
        reviews_df=reviews,
        dataset_payload={"summary": {"product_id": "BETA_2"}},
        symptomized=False,
        db_path=str(db_path),
    )

    engine_rows = list_workspace_records(search="gpt-5.4-mini", db_path=str(db_path))
    assert [row["workspace_name"] for row in engine_rows] == ["Alpha batch"]

    unsymptomized_rows = list_workspace_records(symptom_filter="Needs symptomizer", db_path=str(db_path))
    assert [row["workspace_name"] for row in unsymptomized_rows] == ["Beta batch"]


def test_workspace_store_rename_updates_existing_entry(tmp_path):
    db_path = tmp_path / "workspace_store.sqlite3"
    reviews = pd.DataFrame({"review_id": ["r1"], "review_text": ["Solid"], "rating": [4]})
    workspace_id = save_workspace_record(
        workspace_name="Original name",
        source_type="uploaded",
        source_label="rename.xlsx",
        source_key="uploaded::rename.xlsx",
        reviews_df=reviews,
        dataset_payload={"summary": {"product_id": "REN_1"}},
        db_path=str(db_path),
    )

    rename_workspace_record(workspace_id, "Renamed workspace", db_path=str(db_path))

    rows = list_workspace_records(db_path=str(db_path))
    assert [row["workspace_name"] for row in rows] == ["Renamed workspace"]
    renamed_search_rows = list_workspace_records(search="Renamed workspace", db_path=str(db_path))
    assert [row["workspace_name"] for row in renamed_search_rows] == ["Renamed workspace"]

def test_workspace_store_sort_options_cover_rating_and_symptomization(tmp_path):
    db_path = tmp_path / "workspace_store.sqlite3"
    alpha_reviews = pd.DataFrame({"review_id": ["a1", "a2"], "review_text": ["A", "B"], "rating": [5, 4], "AI Symptom Detractor 1": [None, None]})
    beta_reviews = pd.DataFrame({"review_id": ["b1", "b2"], "review_text": ["C", "D"], "rating": [2, 2], "AI Symptom Detractor 1": ["Noise", "Leak"]})
    gamma_reviews = pd.DataFrame({"review_id": ["c1", "c2"], "review_text": ["E", "F"], "rating": [3, 4], "AI Symptom Detractor 1": ["Noise", None]})

    save_workspace_record(
        workspace_name="Alpha",
        source_type="uploaded",
        source_label="alpha.xlsx",
        source_key="uploaded::alpha.xlsx",
        reviews_df=alpha_reviews,
        dataset_payload={"summary": {"product_id": "ALPHA"}},
        db_path=str(db_path),
    )
    save_workspace_record(
        workspace_name="Beta",
        source_type="uploaded",
        source_label="beta.xlsx",
        source_key="uploaded::beta.xlsx",
        reviews_df=beta_reviews,
        dataset_payload={"summary": {"product_id": "BETA"}},
        db_path=str(db_path),
    )
    save_workspace_record(
        workspace_name="Gamma",
        source_type="uploaded",
        source_label="gamma.xlsx",
        source_key="uploaded::gamma.xlsx",
        reviews_df=gamma_reviews,
        dataset_payload={"summary": {"product_id": "GAMMA"}},
        db_path=str(db_path),
    )

    highest_rating = list_workspace_records(sort_by="Highest rating", db_path=str(db_path))
    assert [row["workspace_name"] for row in highest_rating][:3] == ["Alpha", "Gamma", "Beta"]

    lowest_rating = list_workspace_records(sort_by="Lowest rating", db_path=str(db_path))
    assert [row["workspace_name"] for row in lowest_rating][:3] == ["Beta", "Gamma", "Alpha"]

    most_symptomized = list_workspace_records(sort_by="Most symptomized", db_path=str(db_path))
    assert [row["workspace_name"] for row in most_symptomized][:3] == ["Beta", "Gamma", "Alpha"]




def test_workspace_store_refreshes_tagged_counts_from_state_payload(tmp_path):
    db_path = tmp_path / "workspace_store.sqlite3"
    reviews = pd.DataFrame(
        {
            "review_id": ["r1", "r2", "r3"],
            "review_text": ["Leaks after each use", "Works fine", "Button is stiff"],
            "rating": [1, 4, 2],
            "content_locale": ["en_US", "en_US", "en_GB"],
        }
    )
    state_payload = {
        "sym_processed_rows": [
            {"idx": 0, "wrote_dets": ["Tank Leaks At Seam"], "wrote_dels": []},
            {"idx": 2, "wrote_dets": ["Button Hard To Press"], "wrote_dels": []},
        ]
    }

    workspace_id = save_workspace_record(
        workspace_name="State-backed workspace",
        source_type="uploaded",
        source_label="state.xlsx",
        source_key="uploaded::state.xlsx",
        reviews_df=reviews,
        dataset_payload={"summary": {"product_id": "STATE_1"}},
        state_payload=state_payload,
        symptomized=True,
        db_path=str(db_path),
    )

    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE saved_workspaces SET symptomized_count = 0, symptomized_pct = 0, review_count = 0, avg_rating = NULL, region_count = 0 WHERE workspace_id = ?",
            (workspace_id,),
        )

    rows = list_workspace_records(db_path=str(db_path))
    assert len(rows) == 1
    row = rows[0]
    assert row["workspace_id"] == workspace_id
    assert row["review_count"] == 3
    assert row["symptomized_count"] == 2
    assert row["symptomized"] is True
    assert abs(float(row["symptomized_pct"]) - (2 / 3)) < 1e-9
    assert row["region_count"] == 2
    assert abs(float(row["avg_rating"]) - ((1 + 4 + 2) / 3)) < 1e-9
