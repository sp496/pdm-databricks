import pytest
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from curated.data_curator import DataCurator, Constants


class TestConstants:
    def test_date_folder_format(self):
        assert Constants.DATE_FOLDER_FORMAT == "%Y%m%d"


class TestDataCurator:
    def test_instantiation_with_empty_mappings(self):
        empty_df = pd.DataFrame(columns=["source_col", "target_col", "study_protocol"])
        curator = DataCurator(
            subject_mapping_df=empty_df,
            depot_mapping_df=empty_df,
            site_mapping_df=empty_df,
        )
        assert curator is not None
