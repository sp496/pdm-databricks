import pytest
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lib"))

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


class TestResolveColumns:
    def test_first_matching_source_wins(self):
        df = pd.DataFrame({"Visit Date (Suvoda)": ["01-Jan-2024"], "Other": ["x"]})
        result = DataCurator._resolve_columns(df, {"Visit Date": ["Visit Date", "Visit Date (Suvoda)"]})
        assert "Visit Date" in result.columns
        assert "Visit Date (Suvoda)" not in result.columns

    def test_canonical_name_already_present_is_not_double_renamed(self):
        df = pd.DataFrame({"Visit Date": ["01-Jan-2024"]})
        result = DataCurator._resolve_columns(df, {"Visit Date": ["Visit Date", "Visit Date (Suvoda)"]})
        assert list(result.columns) == ["Visit Date"]

    def test_no_matching_source_leaves_df_unchanged(self):
        df = pd.DataFrame({"UnknownCol": ["val"]})
        result = DataCurator._resolve_columns(df, {"Visit Date": ["Visit Date", "Visit Date (Suvoda)"]})
        assert "Visit Date" not in result.columns
        assert "UnknownCol" in result.columns

    def test_multiple_targets_resolved_independently(self):
        df = pd.DataFrame({"SubjNo": ["S001"], "DrugDesc": ["Drug A"]})
        col_map = {
            "Subject Number": ["Subject Number", "SubjNo"],
            "Drug Description": ["Drug Description", "DrugDesc"],
        }
        result = DataCurator._resolve_columns(df, col_map)
        assert "Subject Number" in result.columns
        assert "Drug Description" in result.columns
        assert "SubjNo" not in result.columns
        assert "DrugDesc" not in result.columns

    def test_empty_col_map_returns_df_unchanged(self):
        df = pd.DataFrame({"A": [1], "B": [2]})
        result = DataCurator._resolve_columns(df, {})
        assert list(result.columns) == ["A", "B"]


class TestAssembleWithColMap:
    @pytest.fixture
    def curator(self):
        return DataCurator()

    @pytest.fixture
    def site_depot_df(self):
        return pd.DataFrame({
            "Arcus Site": ["SITE001"],
            "Depot": ["DEPOT1"],
            "Depot Country": ["US"],
        })

    def test_assemble_subject_visit_data_with_visit_col_map(self, curator, site_depot_df):
        visit_df = pd.DataFrame({
            "Visit Date (Suvoda)": ["01-Jan-2024"],
            "Subject Number": ["S001"],
            "Drug Description": ["Drug A"],
            "Arcus Site ID": ["SITE001"],
        })
        subject_df = pd.DataFrame({
            "Subject Number": ["S001"],
            "Study Protocol": ["GS-US-123-4567"],
            "Date Randomized": ["01-Jan-2024"],
            "Date Discontinued": [None],
            "Gilead Site Number": ["GS001"],
        })
        visit_col_map = {"Visit Date": ["Visit Date", "Visit Date (Suvoda)"]}
        result = curator.assemble_subject_visit_data(visit_df, subject_df, site_depot_df, visit_col_map=visit_col_map)
        assert "Visit Date" in result.columns

    def test_assemble_subject_visit_data_without_col_map(self, curator, site_depot_df):
        visit_df = pd.DataFrame({
            "Visit Date": ["01-Jan-2024"],
            "Subject Number": ["S001"],
            "Drug Description": ["Drug A"],
            "Arcus Site ID": ["SITE001"],
        })
        subject_df = pd.DataFrame({
            "Subject Number": ["S001"],
            "Study Protocol": ["GS-US-123-4567"],
            "Date Randomized": ["01-Jan-2024"],
            "Date Discontinued": [None],
            "Gilead Site Number": ["GS001"],
        })
        result = curator.assemble_subject_visit_data(visit_df, subject_df, site_depot_df)
        assert "Visit Date" in result.columns

    def test_assemble_site_data_with_col_map(self, curator, site_depot_df):
        site_df = pd.DataFrame({
            "Site Number (Arcus)": ["SITE001"],
            "Gilead Site Number": ["GS001"],
            "PI Last Name": ["Smith"],
            "PCI Item Number Lot": ["LOT001"],
            "Drug Description": ["Drug A"],
            "Drug Code": ["DC001"],
            "Finished Lot": ["FL001"],
            "Expiration Date": ["31-Dec-2025"],
            "Quantity (Site Units)": ["10"],
            "Drug Status": ["Intact"],
        })
        col_map = {"Arcus Site Number": ["Arcus Site Number", "Site Number (Arcus)"]}
        result = curator.assemble_site_data(site_df, site_depot_df, col_map=col_map)
        assert not result.empty

    def test_assemble_depot_data_with_col_map(self, curator, site_depot_df):
        depot_df = pd.DataFrame({
            "Depot ID": ["DEPOT1"],
            "Depot Name": ["Main Depot"],
            "Drug Description": ["Drug A"],
            "Drug Code": ["DC001"],
            "PCI Item Number Lot": ["LOT001"],
            "Finished Lot": ["FL001"],
            "Expiration Date": ["31-Dec-2025"],
            "Quantity (Depot Units)": ["50"],
            "Drug Status": ["Intact"],
        })
        col_map = {"Depot Number": ["Depot Number", "Depot ID"]}
        result = curator.assemble_depot_data(depot_df, site_depot_df, col_map=col_map)
        assert not result.empty
