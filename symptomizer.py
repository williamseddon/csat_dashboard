from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class ReviewRepository(ABC):
    @abstractmethod
    def upsert_reviews(self, reviews: pd.DataFrame) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_reviews(self, filters: Optional[dict] = None) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_review_by_id(self, review_id: str) -> pd.DataFrame:
        raise NotImplementedError


class DataFrameReviewRepository(ReviewRepository):
    def __init__(self, initial_df: Optional[pd.DataFrame] = None):
        self._df = initial_df.copy() if initial_df is not None else pd.DataFrame()

    def upsert_reviews(self, reviews: pd.DataFrame) -> int:
        if reviews.empty:
            return 0
        if self._df.empty:
            self._df = reviews.copy()
            return len(reviews)
        combined = pd.concat([self._df, reviews], ignore_index=True)
        if "review_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["review_id"], keep="last")
        self._df = combined.reset_index(drop=True)
        return len(reviews)

    def get_reviews(self, filters: Optional[dict] = None) -> pd.DataFrame:
        df = self._df.copy()
        filters = filters or {}
        for col, value in filters.items():
            if col in df.columns:
                df = df[df[col] == value]
        return df.reset_index(drop=True)

    def get_review_by_id(self, review_id: str) -> pd.DataFrame:
        if "review_id" not in self._df.columns:
            return pd.DataFrame()
        return self._df[self._df["review_id"].astype(str) == str(review_id)].copy()


class SQLAlchemyReviewRepository(ReviewRepository):
    def __init__(self, db_url: str, table_name: str = "reviews"):
        from sqlalchemy import create_engine

        self.engine = create_engine(db_url, future=True)
        self.table_name = table_name

    def upsert_reviews(self, reviews: pd.DataFrame) -> int:
        from sqlalchemy import text

        if reviews.empty:
            return 0
        temp_table = f"{self.table_name}_staging"
        reviews.to_sql(temp_table, self.engine, if_exists="replace", index=False)
        with self.engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM {temp_table} WHERE 1=0"))
            conn.execute(text(f"DELETE FROM {self.table_name} WHERE review_id IN (SELECT review_id FROM {temp_table})"))
            conn.execute(text(f"INSERT INTO {self.table_name} SELECT * FROM {temp_table}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        return len(reviews)

    def get_reviews(self, filters: Optional[dict] = None) -> pd.DataFrame:
        from sqlalchemy import text

        filters = filters or {}
        where_parts = []
        params = {}
        for index, (col, value) in enumerate(filters.items()):
            key = f"p{index}"
            where_parts.append(f"{col} = :{key}")
            params[key] = value
        where_sql = " AND ".join(where_parts)
        sql = f"SELECT * FROM {self.table_name}" + (f" WHERE {where_sql}" if where_sql else "")
        return pd.read_sql(text(sql), self.engine, params=params)

    def get_review_by_id(self, review_id: str) -> pd.DataFrame:
        from sqlalchemy import text

        return pd.read_sql(
            text(f"SELECT * FROM {self.table_name} WHERE review_id = :review_id"),
            self.engine,
            params={"review_id": review_id},
        )
