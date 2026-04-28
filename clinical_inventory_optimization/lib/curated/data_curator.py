"""
Data Curator Module

This module contains all pandas-based data processing logic for clinical inventory curation.
It accepts DataFrames as input, making it fully portable and testable locally or in Databricks.

Key Design:
- Primary methods accept DataFrames (not file paths)
- Databricks notebook handles file I/O and passes DataFrames
- Can be run locally by reading CSV files and passing DataFrames
"""

import os
import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Constants:
    """Constants used throughout the data curation process."""
    STUDY_PROTOCOL_PATTERN = r'GS-US-\d+-\d+(?:-\d+_\d+)?' #r'GS-US-\d+-\d+'
    DATE_FOLDER_FORMAT = "%Y%m%d"
    INPUT_DATE_FORMATS = ['%d-%b-%Y', '%d %b %Y']  # Support multiple date formats
    OUTPUT_DATE_FORMAT = '%Y-%m-%d'
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


class DataCurator:
    """
    Handles all pandas-based data processing for clinical inventory curation.

    This class works with DataFrames as input, making it portable and testable.
    It does NOT handle file I/O - that's the responsibility of the caller.
    """

    def __init__(self, subject_mapping_df: Optional[pd.DataFrame] = None,
                 site_mapping_df: Optional[pd.DataFrame] = None,
                 depot_mapping_df: Optional[pd.DataFrame] = None,
                 slsm_mapping_df: Optional[pd.DataFrame] = None,
                 clsm_mapping_df: Optional[pd.DataFrame] = None):

        """
        Initialize the DataCurator.

        Args:
            mapping_df: DataFrame containing column mappings for standardization
        """

        def dedupe(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
            return df.drop_duplicates() if df is not None else None

        self.subject_mapping_df = dedupe(subject_mapping_df)
        self.site_mapping_df = dedupe(site_mapping_df)
        self.depot_mapping_df = dedupe(depot_mapping_df)
        self.slsm_mapping_df = dedupe(slsm_mapping_df)
        self.clsm_mapping_df = dedupe(clsm_mapping_df)

        self.mapping_df_map = {
            'subject':       self.subject_mapping_df,
            'site':          self.site_mapping_df,
            'depot':         self.depot_mapping_df,
            'slsm':          self.slsm_mapping_df,
            'clsm':          self.clsm_mapping_df,
            'subject_visit': None,
        }

        logger.info("DataCurator initialized")

    # ========================================================================
    # Static Utility Methods (can be used without instance)
    # ========================================================================

    @staticmethod
    def extract_study_protocol(filename: str) -> str:
        """
        Extract Study Protocol from filename.

        Args:
            filename: Filename containing study protocol (e.g., "Gilead GS-US-592-6173_Subject Summary...")

        Returns:
            Study protocol string (e.g., "GS-US-592-6173")

        Raises:
            ValueError: If study protocol pattern not found
        """
        match = re.search(Constants.STUDY_PROTOCOL_PATTERN, filename)
        if not match:
            raise ValueError(f"Could not extract Study Protocol from filename: {filename}")

        study_protocol = match.group(0)
        logger.debug(f"Extracted study protocol: {study_protocol} from {filename}")
        return study_protocol

    @staticmethod
    def remove_rows_with_n_values(df: pd.DataFrame, n: int = 1) -> pd.DataFrame:
        """
        Remove rows that have N or fewer non-null values.

        Args:
            df: Input DataFrame
            n: Threshold for minimum non-null values

        Returns:
            DataFrame with sparse rows removed
        """
        non_null_counts = df.notna().sum(axis=1)
        df_filtered = df[non_null_counts > n].reset_index(drop=True)

        removed_count = len(df) - len(df_filtered)
        if removed_count > 0:
            logger.info(f"Removed {removed_count} rows with {n} or fewer values")

        return df_filtered

    @staticmethod
    def convert_date_columns(df: pd.DataFrame,
                            date_columns: List[str],
                            input_formats: List[str] = None) -> pd.DataFrame:
        """
        Convert date columns to datetime format, trying multiple formats.

        This method handles columns that may contain dates in different formats
        by trying each format and combining the results.

        Args:
            df: Input DataFrame
            date_columns: List of column names to convert
            input_formats: List of possible input date format strings. If None, uses Constants.INPUT_DATE_FORMATS

        Returns:
            DataFrame with converted date columns
        """
        if input_formats is None:
            input_formats = Constants.INPUT_DATE_FORMATS

        df = df.copy()

        for col in date_columns:
            if col not in df.columns:
                continue

            # Start with all NaT (Not a Time)
            result = pd.Series([pd.NaT] * len(df), index=df.index)
            total_converted = 0

            # Try each format and fill in successfully converted values
            for fmt in input_formats:
                try:
                    # Try converting with this format
                    converted = pd.to_datetime(df[col], format=fmt, errors='coerce')

                    # For rows that are still NaT in result, try to use this format's result
                    mask = result.isna() & converted.notna()
                    result[mask] = converted[mask]

                    newly_converted = mask.sum()
                    if newly_converted > 0:
                        logger.debug(f"Format '{fmt}' converted {newly_converted} values in column '{col}'")
                        total_converted += newly_converted

                except Exception as e:
                    logger.debug(f"Format {fmt} failed for column {col}: {str(e)}")
                    continue

            # Assign the result back to the dataframe
            df[col] = result

            if total_converted > 0:
                logger.info(f"Converted date column '{col}': {total_converted}/{len(df)} values successfully parsed")
            else:
                logger.warning(f"Could not convert any values in date column '{col}' with provided formats: {input_formats}")

        return df

    @staticmethod
    def add_metadata_columns(df: pd.DataFrame,
                           date_folder: str,
                           source_file: str) -> pd.DataFrame:
        """
        Add metadata columns to DataFrame.

        Args:
            df: Input DataFrame
            date_folder: Date folder string (e.g., "20251106")
            source_file: Source filename

        Returns:
            DataFrame with metadata columns added
        """
        df = df.copy()

        df['extract_date'] = pd.to_datetime(
            date_folder,
            format=Constants.DATE_FOLDER_FORMAT
        ).strftime(Constants.OUTPUT_DATE_FORMAT)

        df['processed_timestamp'] = datetime.now().strftime(Constants.TIMESTAMP_FORMAT)
        df['source_file'] = source_file

        logger.debug(f"Added metadata: extract_date={df['extract_date'].iloc[0]}, "
                    f"source_file={source_file}")

        return df

    @staticmethod
    def extract_year(x):
        # If it's already a 4-digit year
        if pd.notna(x) and str(x).isdigit() and len(str(x)) == 4:
            return int(x)
        # Otherwise try parsing as date
        try:
            return pd.to_datetime(x, dayfirst=True, errors='coerce').year
        except:
            return np.nan

    # ========================================================================
    # Column Mapping Methods
    # ========================================================================

    def create_column_mapping(self, study_protocol: str, file_type: str) -> Optional[Tuple[Dict[str, str], List[str]]]:
        """
        Create column mapping dictionary for a specific study protocol.

        Args:
            study_protocol: Study protocol identifier
            file_type: One of 'subject', 'site', 'depot'

        Returns:
            Tuple of (column_mapping, standard_columns) where column_mapping maps raw cell
            values from the mapping sheet to standardized column names. A cell value of the
            form "=<literal>" denotes a hard-coded override and is passed through as-is;
            standardize_data interprets these entries.

        Raises:
            ValueError: If mapping_df not set or study protocol not found
        """

        if file_type not in self.mapping_df_map:
            raise ValueError(f"Invalid file_type '{file_type}'. Expected one of: {list(self.mapping_df_map.keys())}")

        mapping_df = self.mapping_df_map[file_type]

        standard_columns = mapping_df['Column Header'].dropna().tolist()

        if file_type == 'subject' and study_protocol not in mapping_df.columns:
            raise ValueError(
                f"Study Protocol '{study_protocol}' not found in {file_type} mapping file. "
                f"Available protocols: {list(mapping_df.columns[1:])}"
            )

        elif study_protocol not in mapping_df.columns:
            logger.debug(f"No {file_type} column mapping for {study_protocol}")
            return {}, standard_columns

        column_mapping = {}

        for _, row in mapping_df.iterrows():
            std_col = row['Column Header']
            orig_col = row[study_protocol]
            if pd.notna(orig_col) and orig_col.strip():
                column_mapping[orig_col.strip()] = std_col

        logger.debug(f"Created {file_type} column mapping with {len(column_mapping)} entries for {study_protocol}")
        return column_mapping, standard_columns

    # ========================================================================
    # Core Processing Methods (accept DataFrames)
    # ========================================================================


    def assemble_subject_visit_data(
        self,
        visit_df: pd.DataFrame,
        subject_df: pd.DataFrame,
        site_depot_df: pd.DataFrame,
        drop_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Assemble a unified subject-visit DataFrame from three source DataFrames.

        1. Reduce visit_df to the latest visit per subject per drug.
        2. Join patient-level fields from subject_df.
        3. Join site-to-depot mapping from site_depot_df.
        4. Drop any specified unused columns.

        Args:
            visit_df:      Subject Visit Summary (one row per visit).
            subject_df:    Subject Summary (patient-level info).
            site_depot_df: Site-to-Depot mapping.
            drop_columns:  Raw column names to drop from the assembled result.

        Returns:
            Assembled DataFrame with one row per subject per drug.
        """
        visit_df = visit_df.copy()

        # Step 1: latest visit per subject per drug
        visit_df['Visit Date'] = pd.to_datetime(visit_df['Visit Date'], dayfirst=True, errors='coerce')
        latest = (
            visit_df
            .sort_values(['Subject Number', 'Drug Description', 'Visit Date'])
            .groupby(['Subject Number', 'Drug Description'], as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
        # Reformat Visit Date back to string so convert_date_columns isn't applied twice
        latest['Visit Date'] = latest['Visit Date'].dt.strftime('%Y-%m-%d')

        # Step 2: join patient-level info from subject_df
        subject_cols = ['Subject Number', 'Study Protocol', 'Date Randomized', 'Date Discontinued']
        subject_dedup = (
            subject_df[[c for c in subject_cols if c in subject_df.columns]]
            .drop_duplicates(subset=['Subject Number', 'Study Protocol'])
        )
        latest = latest.merge(subject_dedup, how='left', on=['Subject Number', 'Study Protocol'])

        # Step 3: join site-to-depot mapping
        latest = latest.merge(
            site_depot_df[['Arcus Site', 'Depot']],
            how='left',
            left_on='Arcus Site ID',
            right_on='Arcus Site',
        )
        latest = (
            latest
            .rename(columns={'Depot': 'Parent Depot'})
            .drop(columns=['Arcus Site'], errors='ignore')
        )

        # Step 4: drop unused columns
        if drop_columns:
            latest = latest.drop(columns=[c for c in drop_columns if c in latest.columns])

        logger.info(f"Assembled subject-visit data: {latest.shape[0]} rows, {latest.shape[1]} columns")
        return latest

    def type_specific_processing(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        if file_type == 'subject':
            if 'Year of Birth' in df.columns:
                df['Year of Birth'] = df['Year of Birth'].apply(self.extract_year)

        if file_type == 'depot':
            if 'Country' in df.columns and ('Approved Countries' not in df.columns
                                            or df['Approved Countries'].isna().all()):
                df['Approved Countries'] = df['Country']

        return df


    def standardize_data(self,
                         df: pd.DataFrame,
                         study_protocol: str,
                         file_type: str) -> pd.DataFrame:
        """
        Standardize a Subject Summary DataFrame based on header mapping.

        Args:
            df: Input DataFrame (already read from CSV)
            filename: Original filename to extract Study Protocol from

        Returns:
            Tuple of (standardized DataFrame, study_protocol)

        Raises:
            ValueError: If mapping cannot be applied
        """


        # Get column mapping
        column_mapping, standard_columns = self.create_column_mapping(study_protocol, file_type)

        # Skip standardization if no mapping found for the study
        if not column_mapping:
            return df

        # Invert to: standardized_col -> raw_val (source column name or "ColName=literal")
        inverted_mapping = {std_col: raw_val for raw_val, std_col in column_mapping.items()}

        df_standardized = pd.DataFrame(index=df.index)

        for std_col in standard_columns:
            raw_val = inverted_mapping.get(std_col)
            if raw_val is not None and '=' in raw_val:
                _, value = raw_val.split('=', 1)
                df_standardized[std_col] = value.strip()
            elif raw_val is not None and ',' in raw_val:
                source_cols = [c.strip() for c in raw_val.split(',')]
                existing = [c for c in source_cols if c in df.columns]
                if existing:
                    df_standardized[std_col] = df[existing].bfill(axis=1).iloc[:, 0]
                else:
                    df_standardized[std_col] = None
            elif raw_val is not None and raw_val in df.columns:
                df_standardized[std_col] = df[raw_val]
            elif std_col in df.columns:
                df_standardized[std_col] = df[std_col]
            else:
                df_standardized[std_col] = None

        logger.info(f"Standardized data: {df_standardized.shape[0]} rows, "
                   f"{df_standardized.shape[1]} columns")

        return df_standardized


    def add_study_protocol_column(self,
                                  df: pd.DataFrame,
                                  filename: str) -> Tuple[pd.DataFrame, str]:
        """
        Add Study Protocol column to a DataFrame (for depot, site, supply method files).

        Args:
            df: Input DataFrame
            filename: Filename to extract study protocol from

        Returns:
            Tuple of (DataFrame with Study Protocol column, study_protocol)
        """
        df = df.copy()

        # Extract study protocol
        study_protocol = self.extract_study_protocol(filename)

        # Add Study Protocol column at the beginning
        df.insert(0, 'Study Protocol', study_protocol)

        logger.info(f"Added Study Protocol '{study_protocol}' to DataFrame: {df.shape}")

        return df, study_protocol


    def process_data(self,
                     df: pd.DataFrame,
                     file_type,
                     filename: str,
                     date_folder: str,
                     table_column_mapping: Dict[str, str],
                     date_columns: List[str],
                     study_protocol: str = None) -> Optional[pd.DataFrame]:
        """
        Process a single Subject Summary DataFrame.

        Args:
            df: Input DataFrame
            filename: Source filename
            date_folder: Date folder string (e.g., "20251106")
            table_column_mapping: Dictionary to rename columns
            date_columns: List of date column names to convert
            study_protocol: Optional override — skips filename extraction when provided
                            (use for file types where protocol comes from the data itself)

        Returns:
            Processed DataFrame, or None if processing fails
        """
        try:

            # Use provided study protocol or extract from filename
            study_protocol = study_protocol or self.extract_study_protocol(filename)

            # Standardize the dataframe
            if self.mapping_df_map[file_type] is not None:
                standardized_df = self.standardize_data(df, study_protocol, file_type=file_type)
            else:
                standardized_df = df.copy()

            standardized_df = self.type_specific_processing(standardized_df, file_type)

            # Add Study Protocol column
            standardized_df.insert(0, 'Study Protocol', study_protocol)
            logger.debug(f"Added Study Protocol column: {study_protocol}")

            # Add metadata
            standardized_df = self.add_metadata_columns(standardized_df, date_folder, filename)

            # Rename columns to match database schema
            standardized_df = standardized_df.rename(columns=table_column_mapping)

            # Convert date columns
            standardized_df = self.convert_date_columns(standardized_df, date_columns)

            logger.info(f"Processed Subject Summary: {filename}")
            return standardized_df

        except Exception as e:
            logger.error(f"Error processing Subject Summary {filename}: {str(e)}", exc_info=True)
            return None


    # ========================================================================
    # Convenience Methods for Local Debugging (accept file paths)
    # ========================================================================

    def process_data_from_file(self,
                               file_path,
                               file_type,
                               date_folder: str,
                               table_column_mapping: Dict[str, str],
                               date_columns: List[str]) -> Optional[pd.DataFrame]:
        """
        Process a single Subject Summary DataFrame.

        Args:
            df: Input DataFrame
            filename: Source filename
            date_folder: Date folder string (e.g., "20251106")
            column_mapping: Dictionary to rename columns
            date_columns: List of date column names to convert

        Returns:
            Processed DataFrame, or None if processing fails
        """
        try:
            df = read_dynamic_csv(file_path)
            filename = file_path.split('/')[-1]  # Extract filename from path
            logger.info(f"✓ Loaded {filename}: {df.shape}")

            standardized_df = self.process_data(df, file_type, filename, date_folder, table_column_mapping, date_columns)

            return standardized_df

        except Exception as e:
            logger.error(f"Error processing Subject Summary {file_path}: {str(e)}", exc_info=True)
            return None


# ========================================================================
# Convenience Functions for Local File Reading
# ========================================================================

def read_dynamic_csv(filepath: str, max_rows: int = 10) -> pd.DataFrame:
    """
    Read CSV file with dynamic header row detection.

    Finds the first row where all cells are non-empty and uses it as the header.
    This is a convenience function for local testing.

    Args:
        filepath: Path to the CSV file
        max_rows: Maximum number of rows to search for header

    Returns:
        DataFrame with data

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If no valid header found
    """
    logger.info(f"Reading CSV with dynamic header detection: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= max_rows:
                raise ValueError(f"No valid header found in first {max_rows} rows of {filepath}")

            cells = [c.strip() for c in line.split(',')]

            # Check if all cells are non-empty and has multiple columns
            if len(cells) > 1 and all(cells):
                logger.info(f"Found header at line {i} in {filepath}")
                df = pd.read_csv(filepath, dtype=str, encoding='utf-8', skiprows=i)
                logger.info(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
                return df

    raise ValueError(f"No fully populated header line found in {filepath}")


def load_excel_mapping(excel_path: str, sheet_name: str = 'Header') -> pd.DataFrame | None:
    """
    Load column mapping from an Excel file.

    This is a convenience function for loading mapping files.

    Args:
        excel_path: Path to the Excel file containing mappings
        sheet_name: Name of the sheet to read

    Returns:
        DataFrame with mapping data or None if file does not exist
    """
    logger.info(f"Loading Excel mapping from: {excel_path}, sheet: {sheet_name}")

    # Check if file exists
    if not os.path.exists(excel_path):
        logger.warning(f"File not found: {excel_path}")
        return None

    mapping_df = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
        engine='openpyxl',
        dtype=str
    )

    # Verify 'Column Header' column exists
    if 'Column Header' not in mapping_df.columns:
        raise ValueError(
            f"'Column Header' column not found in mapping file. "
            f"Found columns: {list(mapping_df.columns)}"
        )

    logger.info(f"Mapping loaded: {len(mapping_df)} rows, "
                f"{len(mapping_df.columns)-1} study protocols")

    return mapping_df
 