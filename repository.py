from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


class ReviewDownloaderError(Exception):
    pass


@dataclass
class ReviewBatchSummary:
    product_url: str
    product_id: str
    total_reviews: int
    page_size: int
    requests_needed: int
    reviews_downloaded: int


@dataclass
class LoadedWorkspace:
    summary: ReviewBatchSummary
    reviews_df: pd.DataFrame
    source_type: str
    source_label: str
    source_urls: List[str] = field(default_factory=list)
    source_failures: List[tuple[str, str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary": self.summary,
            "reviews_df": self.reviews_df,
            "source_type": self.source_type,
            "source_label": self.source_label,
            "source_urls": list(self.source_urls),
            "source_failures": list(self.source_failures),
        }
