import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import re
import logging
from dataclasses import dataclass
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA CLASSES FOR TYPE SAFETY
# ============================================================================

@dataclass
class CycleInfo:
    """Represents information about a patient's current cycle"""
    cycle_number: int
    day_number: int
    last_visit_date: pd.Timestamp


@dataclass
class ProjectedVisit:
    """Represents a projected future visit"""
    subject_number: int
    drug_dispensed: str
    cycle_number: int
    cycle_day: int
    visit_date: str
    medicine_quantity: float
    visit_description: str


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration settings for the demand planning system"""

    # File paths - Update these to match your local setup
    # SUBJECT_SUMMARY_FILE = "test_scenarios/01_simple_single_drug/subject_summary.csv"
    # TREATMENT_MAPPING_FILE = "test_scenarios/01_simple_single_drug/treatment_mapping.csv"
    # OUTPUT_FILE = "test_scenarios/01_simple_single_drug/demand_forecast.csv"

    SUBJECT_SUMMARY_FILE = "clinical_subject_summary.csv"
    TREATMENT_MAPPING_FILE = "treatment_group_mapping.csv"
    OUTPUT_FILE = "demand_forecast.csv"

    # Column mappings for standardization
    SUBJECT_COLUMNS = [
        'study_protocol', 'site_id', 'country', 'parent_depot', 'investigator',
        'subject_number', 'year_of_birth', 'gender', 'tpc', 'date_randomized',
        'date_crossover_enrolled', 'date_crossover_approved',
        'date_crossover_treatment_discontinued', 'subject_status',
        'randomized_treatment', 'last_study_visit_recorded',
        'last_study_visit_date', 'last_study_visit_number',
        'next_min_study_visit_date', 'next_max_study_visit_date',
        'additional_drug_status', 'last_additional_drug_visit_recorded',
        'last_additional_drug_visit_date', 'last_additional_drug_visit_number',
        'next_min_additional_drug_visit_date', 'next_max_additional_drug_visit_date',
        'extract_date'
    ]

    MAPPING_COLUMNS = [
        'study_protocol', 'randomized_treatment', 'subject_status', 'tpc',
        'study_drug_dispensed', 'additional_study_drug_dispensed',
        'additional_study_drug_prefix', 'country', 'visit_days',
        'dispensing_quantity', 'dispensing_frequency_days', 'max_cycles'
    ]

    # Statuses that indicate a patient has stopped treatment
    EXCLUDED_STATUSES = [
        "Screen Failed",
        "Pre-Screened Failed",
        "Treatment Discontinued",
        "Crossover Treatment Discontinued"
    ]

    # Default values
    DEFAULT_PROJECTION_DAYS = 1095  # Project visits for one year ahead (primary constraint)
    # Note: max_cycles from mapping is a secondary constraint (hard cap on cycle numbers)


# ============================================================================
# DATA LOADING AND CLEANING
# ============================================================================

class DataLoader:
    """Handles loading and initial cleaning of data files"""

    @staticmethod
    def prepare_subject_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare and clean subject summary data

        Args:
            df: Raw subject DataFrame

        Returns:
            Cleaned subject DataFrame
        """
        logger.info(f"Preparing subject data ({len(df)} records)")

        # Clean column names first
        df.columns = df.columns.str.strip()

        # Select only required columns if they exist
        available_cols = [col for col in Config.SUBJECT_COLUMNS if col in df.columns]
        df = df[available_cols]

        # Convert date columns
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        logger.info(f"Prepared {len(df)} subject records")
        return df

    @staticmethod
    def prepare_mapping_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare and clean treatment mapping data

        Args:
            df: Raw mapping DataFrame

        Returns:
            Cleaned mapping DataFrame
        """
        logger.info(f"Preparing mapping data ({len(df)} records)")

        # Clean column names first
        df.columns = df.columns.str.strip()

        # Select only required columns if they exist
        available_cols = [col for col in Config.MAPPING_COLUMNS if col in df.columns]
        df = df[available_cols]

        # Handle TPC column variations
        for col in df.columns:
            if "TPC" in col.upper():
                df.rename(columns={col: "tpc"}, inplace=True)
                break

        logger.info(f"Prepared {len(df)} mapping records")
        return df

    @staticmethod
    def load_subject_data(filepath: str) -> pd.DataFrame:
        """
        Load subject summary data from CSV file

        Args:
            filepath: Path to the subject summary CSV file

        Returns:
            Cleaned subject DataFrame
        """
        logger.info(f"Loading subject data from {filepath}")

        try:
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} subject records from file")
            return DataLoader.prepare_subject_data(df)

        except Exception as e:
            logger.error(f"Error loading subject data: {e}")
            raise

    @staticmethod
    def load_mapping_data(filepath: str) -> pd.DataFrame:
        """
        Load treatment mapping data from CSV file

        Args:
            filepath: Path to the treatment mapping CSV file

        Returns:
            Cleaned mapping DataFrame
        """
        logger.info(f"Loading treatment mapping data from {filepath}")

        try:
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} mapping records from file")
            return DataLoader.prepare_mapping_data(df)

        except Exception as e:
            logger.error(f"Error loading mapping data: {e}")
            raise


# ============================================================================
# TEXT PROCESSING UTILITIES
# ============================================================================

class TextProcessor:
    """Utilities for text normalization and parsing"""

    @staticmethod
    def normalize_text(text: Any) -> str:
        """
        Normalize text by cleaning quotes and whitespace while preserving case

        Args:
            text: Input text to normalize

        Returns:
            Normalized text with cleaned quotes and trimmed whitespace (case preserved)
        """
        if pd.isna(text):
            return 'nan'

        text = str(text)
        # Replace smart quotes with regular quotes
        text = text.replace("'", "'").replace(""", '"').replace(""", '"')
        # Strip whitespace (case is preserved)
        return text.strip()

    @staticmethod
    def parse_cycle_day(visit_string: str) -> int:
        """
        Parse the day number from visit strings like 'TPC C20D1' or 'Cycle 46 Day 8'

        Args:
            visit_string: String describing the visit

        Returns:
            Day number (defaults to 1 if unparsable)
        """
        if pd.isna(visit_string):
            return 1

        visit_str = str(visit_string)

        # Try to match patterns like D1, Day 1, Day1
        match = re.search(r'(?:D|Day\s?)(\d+)', visit_str, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 1

        # If it's a cycle string without a day, assume day 1
        if 'cycle' in visit_str.lower():
            return 1

        return 1

    @staticmethod
    def parse_cycle_number(visit_string: str) -> int:
        """
        Parse the cycle number from visit strings like 'TPC C20D1' or 'Cycle 46 Day 8'

        Args:
            visit_string: String describing the visit

        Returns:
            Cycle number (defaults to 0 if unparsable)
        """
        if pd.isna(visit_string):
            return 0

        visit_str = str(visit_string)

        # Try to match patterns like C20, Cycle 46, Cycle46
        match = re.search(r'(?:C|Cycle\s?)(\d+)', visit_str, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 0

        return 0

    @staticmethod
    def parse_visit_days(visit_days_str: str) -> List[int]:
        """
        Parse visit days from a comma-separated string

        Args:
            visit_days_str: String like "1,8,15" or "1, 8, 15"

        Returns:
            List of day numbers
        """
        if pd.isna(visit_days_str) or str(visit_days_str).lower() == 'nan':
            return []

        try:
            days = str(visit_days_str).split(',')
            return [int(day.strip()) for day in days if day.strip().isdigit()]
        except Exception as e:
            logger.warning(f"Could not parse visit days '{visit_days_str}': {e}")
            return []


# ============================================================================
# VISIT PROJECTION ENGINE
# ============================================================================

class VisitProjector:
    """Handles the projection of future visits for patients"""

    def __init__(self, text_processor: TextProcessor):
        self.text_processor = text_processor

    def project_future_visits(self, row: pd.Series) -> List[ProjectedVisit]:
        """
        Project future visits for a patient

        This implementation matches the Databricks logic:
        1. Projects remaining visits in the current cycle
        2. Projects future complete cycles
        3. Rolls forward cycles if they're in the past

        Args:
            row: Patient data row with merged treatment information

        Returns:
            List of ProjectedVisit objects
        """
        projected_visits = []

        try:
            # Get last visit date and validate
            last_visit_date = pd.to_datetime(row['last_study_visit_date'])
            if pd.isna(last_visit_date):
                return []

            # Get dispensing frequency and validate
            dispensing_frequency = row.get('dispensing_frequency_days', 28)
            if pd.isna(dispensing_frequency):
                dispensing_frequency = 28
            cycle_days = int(dispensing_frequency)

            # Get visit days pattern
            visit_days_str = row.get('visit_days', '')
            visit_days = self.text_processor.parse_visit_days(visit_days_str)
            if not visit_days:
                visit_days = [1]  # Default to day 1 only
            visit_days = sorted(visit_days)

            # Get cycle information
            last_day_number = row.get('parsed_last_visit_day', 1)
            current_cycle_number = row.get('parsed_last_visit_cycle', 0)
            is_crossover = row.get('is_crossover', False)
            is_tpc = row.get('is_tpc', False)

            # Get max cycles (optional constraint - hard cap on cycle numbers)
            max_cycles = row.get('max_cycles', None)
            if not pd.isna(max_cycles) and max_cycles >= 1:
                max_cycles = int(max_cycles)
            else:
                max_cycles = None  # No cycle limit, only time limit applies

            # Calculate Day 1 of the last recorded cycle (current cycle)
            time_to_subtract = timedelta(days=last_day_number - 1)
            last_cycle_day_1 = last_visit_date - time_to_subtract

            # Determine prefix for forecast string
            if is_crossover:
                prefix = "Crossover "
            elif is_tpc:
                prefix = "TPC "
            else:
                prefix = ""

            # Get today's date and projection horizon for time-based filtering
            TODAY = datetime.now().date()
            projection_horizon = TODAY + timedelta(days=Config.DEFAULT_PROJECTION_DAYS)

            # ============================================================================
            # A. PROJECT REMAINING VISITS IN CURRENT CYCLE
            # ============================================================================
            remaining_days = [day for day in visit_days if day > last_day_number]

            for day in remaining_days:
                visit_date = last_cycle_day_1 + timedelta(days=day - 1)

                # Only include if the projected date is:
                # 1. After the last recorded visit
                # 2. Within the projection horizon (next 365 days)
                # 3. Within max_cycles if defined
                if visit_date.date() > last_visit_date.date() and visit_date.date() <= projection_horizon:
                    # Check max_cycles constraint if defined
                    if max_cycles is not None and current_cycle_number > max_cycles:
                        continue

                    recorded_forecast_str = f"{prefix}Cycle {current_cycle_number} Day {day}"

                    projected_visit = ProjectedVisit(
                        subject_number=int(row['subject_number']),
                        drug_dispensed=row['drug_dispensed'],
                        cycle_number=current_cycle_number,
                        cycle_day=day,
                        visit_date=visit_date.strftime('%Y-%m-%d'),
                        medicine_quantity=row['total_medicines_required_per_cycle'],
                        visit_description=recorded_forecast_str
                    )

                    projected_visits.append(projected_visit)

            # ============================================================================
            # B. PROJECT NEXT FULL CYCLES (TIME-BASED WITH MAX_CYCLES CAP)
            # ============================================================================

            # Calculate Day 1 of the NEXT cycle
            cycle_duration = timedelta(days=cycle_days)
            forecast_start_day_1 = last_cycle_day_1 + cycle_duration

            # Roll forward if the calculated start is in the past
            if forecast_start_day_1.date() < TODAY:
                days_since_start = (TODAY - forecast_start_day_1.date()).days
                missed_cycles = days_since_start // cycle_days + 1
                cycle_day_1 = forecast_start_day_1 + missed_cycles * cycle_duration
            else:
                cycle_day_1 = forecast_start_day_1

            # Determine the cycle number to start the future projection from
            start_cycle_number = current_cycle_number + 1

            # Project cycles until we exceed the time horizon
            # Loop stops when visit dates exceed projection_horizon OR max_cycles is reached
            cycle_offset = 0
            while True:
                # Calculate Day 1 of the current future forecast cycle
                current_cycle_day_1 = cycle_day_1 + timedelta(days=cycle_offset * cycle_days)
                current_projected_cycle = start_cycle_number + cycle_offset

                # Check if we've exceeded max cycles (hard cap if defined)
                if max_cycles is not None and current_projected_cycle > max_cycles:
                    break

                # Check if the first day of this cycle exceeds our time horizon
                # If so, we still need to check individual visit days in case some are within horizon
                any_visit_in_horizon = False

                for day in visit_days:
                    visit_date = current_cycle_day_1 + timedelta(days=day - 1)

                    # Only include visits within the projection horizon
                    if visit_date.date() <= projection_horizon:
                        any_visit_in_horizon = True

                        recorded_forecast_str = f"{prefix}Cycle {current_projected_cycle} Day {day}"

                        projected_visit = ProjectedVisit(
                            subject_number=int(row['subject_number']),
                            drug_dispensed=row['drug_dispensed'],
                            cycle_number=current_projected_cycle,
                            cycle_day=day,
                            visit_date=visit_date.strftime('%Y-%m-%d'),
                            medicine_quantity=row['total_medicines_required_per_cycle'],
                            visit_description=recorded_forecast_str
                        )

                        projected_visits.append(projected_visit)

                # If no visits in this cycle were within the horizon, we're done
                if not any_visit_in_horizon:
                    break

                cycle_offset += 1

        except Exception as e:
            logger.error(f"Error projecting visits for subject {row.get('subject_number', 'unknown')}: {e}")

        return projected_visits


# ============================================================================
# MAIN DEMAND PLANNING PROCESSOR
# ============================================================================

class DemandPlanningProcessor:
    """Main processor that orchestrates the demand planning workflow"""

    def __init__(self):
        self.data_loader = DataLoader()
        self.text_processor = TextProcessor()
        self.visit_projector = VisitProjector(self.text_processor)

    def filter_active_subjects(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to only include active subjects

        Args:
            df: Subject DataFrame

        Returns:
            Filtered DataFrame with only active subjects
        """
        logger.info("Filtering to active subjects only")

        initial_count = len(df)
        df_filtered = df[~df["subject_status"].isin(Config.EXCLUDED_STATUSES)]
        final_count = len(df_filtered)

        logger.info(f"Filtered from {initial_count} to {final_count} active subjects")
        return df_filtered

    def normalize_data(self, df_subjects: pd.DataFrame, df_mapping: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Normalize text fields in both DataFrames for consistent matching

        Args:
            df_subjects: Subject DataFrame
            df_mapping: Treatment mapping DataFrame

        Returns:
            Tuple of normalized DataFrames
        """
        logger.info("Normalizing data for consistent matching")

        # Define columns to normalize
        normalize_columns = ["study_protocol", "randomized_treatment", "tpc", "country"]

        # Normalize subject data
        for col in normalize_columns:
            if col in df_subjects.columns:
                df_subjects[col] = df_subjects[col].apply(self.text_processor.normalize_text)

        # Normalize mapping data
        for col in normalize_columns:
            if col in df_mapping.columns:
                df_mapping[col] = df_mapping[col].apply(self.text_processor.normalize_text)

        # Also normalize drug columns in mapping
        drug_columns = ["study_drug_dispensed", "additional_study_drug_dispensed"]
        for col in drug_columns:
            if col in df_mapping.columns:
                df_mapping[col] = df_mapping[col].apply(self.text_processor.normalize_text)

        return df_subjects, df_mapping

    def merge_and_calculate(self, df_subjects: pd.DataFrame, df_mapping: pd.DataFrame) -> pd.DataFrame:
        """
        Merge subject and mapping data using hierarchical country-specific matching,
        then calculate medicine requirements.

        Merge Strategy:
        1. First priority: Match with country-specific protocols (country is specified in mapping)
        2. Fallback: Match with generic protocols (country is 'nan' string in mapping)

        Note: After normalization, null country values become the string 'nan'
        Case-insensitive matching is performed using temporary lowercase columns,
        while original case is preserved in the results.

        Args:
            df_subjects: Subject DataFrame
            df_mapping: Treatment mapping DataFrame

        Returns:
            Merged DataFrame with calculated medicine requirements
        """
        logger.info("Merging subject and treatment mapping data with hierarchical country matching")

        # Add temporary row identifier to track individual rows (not just subject numbers)
        # This is critical because same subject can have multiple rows with different protocols/status
        df_subjects = df_subjects.copy()
        df_subjects['_temp_row_id'] = range(len(df_subjects))

        df_mapping = df_mapping.copy()

        # Define base merge keys (without country)
        base_merge_keys = ["study_protocol", "randomized_treatment", "tpc", "subject_status"]

        # ========================================================================
        # Create temporary lowercase columns for case-insensitive matching
        # ========================================================================
        merge_cols_to_lower = base_merge_keys + ["country"]

        for col in merge_cols_to_lower:
            if col in df_subjects.columns:
                df_subjects[f'{col}_lower'] = df_subjects[col].astype(str).str.lower()
            if col in df_mapping.columns:
                df_mapping[f'{col}_lower'] = df_mapping[col].astype(str).str.lower()

        # Create lowercase versions of merge keys
        base_merge_keys_lower = [f'{col}_lower' for col in base_merge_keys]

        # ========================================================================
        # STEP 1: Try country-specific match first (highest priority)
        # ========================================================================

        # Filter mapping data to only country-specific protocols (where country is not 'nan' string)
        # Note: normalize_text() converts null values to the string 'nan'
        df_mapping_country_specific = df_mapping[df_mapping['country'] != 'nan'].copy()

        if len(df_mapping_country_specific) > 0:
            # Merge with country included (using lowercase columns)
            merge_keys_with_country_lower = base_merge_keys_lower + ["country_lower"]
            df_merged_country_specific = pd.merge(
                df_subjects,
                df_mapping_country_specific,
                on=merge_keys_with_country_lower,
                how="inner",
                suffixes=('', '_mapping')
            )
            logger.info(f"Country-specific merge resulted in {len(df_merged_country_specific)} records")
        else:
            df_merged_country_specific = pd.DataFrame()
            logger.info("No country-specific mappings found")

        # ========================================================================
        # STEP 2: For non-matched rows, try generic match (fallback)
        # ========================================================================

        # Identify ROWS (not just subjects) that were NOT matched in country-specific merge
        if len(df_merged_country_specific) > 0:
            matched_row_ids = set(df_merged_country_specific['_temp_row_id'].unique())
            df_subjects_remaining = df_subjects.copy()#[~df_subjects['_temp_row_id'].isin(matched_row_ids)].copy()
            logger.info(f"{len(df_subjects_remaining)} subject rows remaining for generic match "
                       f"(out of {len(df_subjects)} total rows)")
        else:
            df_subjects_remaining = df_subjects.copy()
            logger.info(f"All {len(df_subjects_remaining)} subject rows will attempt generic match")

        # Filter mapping data to only generic protocols (where country is 'nan' string)
        df_mapping_generic = df_mapping[df_mapping['country'] == 'nan'].copy()

        if len(df_subjects_remaining) > 0 and len(df_mapping_generic) > 0:
            # Merge without country (use base keys only, with lowercase columns)
            df_merged_generic = pd.merge(
                df_subjects_remaining,
                df_mapping_generic,
                on=base_merge_keys_lower,
                how="inner",
                suffixes=('', '_mapping')
            )
            logger.info(f"Generic merge resulted in {len(df_merged_generic)} records")
        else:
            df_merged_generic = pd.DataFrame()
            if len(df_mapping_generic) == 0:
                logger.warning("No generic mappings found (all mappings are country-specific)")

        # ========================================================================
        # STEP 3: Standardize columns before combining
        # ========================================================================

        # Handle duplicate columns from merge (due to suffixes)
        # We want to keep the original case-preserved columns from subjects dataframe

        if len(df_merged_generic) > 0:
            # For columns that appear in both dataframes, keep the subject version
            # and drop the mapping version (with _mapping suffix)
            for col in base_merge_keys + ['country']:
                if f'{col}_mapping' in df_merged_generic.columns:
                    df_merged_generic.drop(columns=[f'{col}_mapping'], inplace=True)

            # Add subject_country column from the original country column
            df_merged_generic['subject_country'] = df_merged_generic['country']

        if len(df_merged_country_specific) > 0:
            # For columns that appear in both dataframes, keep the subject version
            # and drop the mapping version (with _mapping suffix)
            for col in base_merge_keys + ['country']:
                if f'{col}_mapping' in df_merged_country_specific.columns:
                    df_merged_country_specific.drop(columns=[f'{col}_mapping'], inplace=True)

            # Add subject_country column (same as country for country-specific matches)
            df_merged_country_specific['subject_country'] = df_merged_country_specific['country']

        # ========================================================================
        # STEP 4: Combine both merge results
        # ========================================================================

        if len(df_merged_country_specific) > 0 and len(df_merged_generic) > 0:
            df_merged = pd.concat([df_merged_country_specific, df_merged_generic], ignore_index=True)
        elif len(df_merged_country_specific) > 0:
            df_merged = df_merged_country_specific
        elif len(df_merged_generic) > 0:
            df_merged = df_merged_generic
        else:
            logger.error("No matches found in either country-specific or generic merge")
            return pd.DataFrame()

        logger.info(f"Total merged records: {len(df_merged)} "
                   f"({len(df_merged_country_specific)} country-specific + {len(df_merged_generic)} generic)")

        # build country set (lowercase) from your existing list
        country_list = df_mapping_country_specific['country'].dropna().unique().tolist()
        country_set = {c.lower() for c in country_list}

        def extract_drug_base(val):
            if val == 'nan':
                return 'nan'  # preserve NaN

            s = str(val).strip()

            # 1) Remove dosage and everything after the first digit
            s = re.sub(r'\d.*', '', s).strip()

            # 2) Remove country names (case-insensitive) with optional hyphen
            for c in country_set:
                s = re.sub(rf'(?i)\b{re.escape(c)}\b-?', '', s)
                s = re.sub(rf'(?i)-\b{re.escape(c)}\b', '', s)

            # 3) Remove leftover brackets if any survived
            s = re.sub(r'[\(\)]', '', s)

            # 4) Clean leading/trailing hyphens or extra spaces
            s = re.sub(r'^[\s\-]+', '', s)
            s = re.sub(r'[\s\-]+$', '', s)
            s = re.sub(r'\s{2,}', ' ', s)

            return s.strip()

        df_mapping_country_specific['study_drug_dispensed_base'] = (
            df_mapping_country_specific['study_drug_dispensed'].apply(extract_drug_base)
        )

        key_cols = [
            'study_protocol_lower',
            'randomized_treatment_lower',
            'tpc_lower',
            'subject_status_lower',
            'country_lower',
            'study_drug_dispensed_base'
        ]

        df_mapping_country_specific["key_col"] = (
            df_mapping_country_specific[key_cols]
            .astype(str)
            .agg("+".join, axis=1)
        )

        country_specific_dispense_map = (
            df_mapping_country_specific
            .groupby("key_col")["study_drug_dispensed"]
            .apply(list)
            .to_dict()
        )

        df_merged['study_drug_dispensed_base'] = df_merged['study_drug_dispensed'].str.extract(r'^(.*?)\s\d', expand=False)

        df_merged["key_col"] = (
            df_merged[key_cols]
            .astype(str)
            .agg("+".join, axis=1)
        )

        df_merged = df_merged[
            df_merged.apply(
                lambda row: (
                                # If key does not exist → keep the row
                                    row["key_col"] not in country_specific_dispense_map
                            ) or (
                                # If key exists → check if value is allowed
                                    row["study_drug_dispensed"] in country_specific_dispense_map[row["key_col"]]
                            ),
                axis=1
            )
        ]

        # Remove temporary columns (row identifier and lowercase columns)
        cols_to_drop = ['_temp_row_id']
        for col in merge_cols_to_lower:
            cols_to_drop.append(f'{col}_lower')

        # Only drop columns that exist
        cols_to_drop = [col for col in cols_to_drop if col in df_merged.columns]
        if cols_to_drop:
            df_merged.drop(columns=cols_to_drop, inplace=True)

        # Calculate visit count per cycle
        def count_visits(visit_days_str):
            visits = self.text_processor.parse_visit_days(visit_days_str)
            return len(visits) if visits else 0

        df_merged["visit_count_per_cycle"] = df_merged["visit_days"].apply(count_visits)

        # Calculate total medicines required per cycle
        df_merged["total_medicines_required_per_cycle"] = (
                df_merged["visit_count_per_cycle"] * df_merged["dispensing_quantity"]
        )

        # Identify the specific medicine for each row
        df_merged["drug_dispensed"] = np.where(
            df_merged["study_drug_dispensed"] != "nan",
            df_merged["study_drug_dispensed"],
            df_merged["additional_study_drug_dispensed"]
        )

        # Filter out rows without a medicine
        df_result = df_merged[df_merged["drug_dispensed"] != "nan"].copy()
        logger.info(f"Identified {len(df_result)} records with valid medicines")

        # Add parsed visit information for easier processing
        df_result['parsed_last_visit_cycle'] = df_result['last_study_visit_recorded'].apply(
            self.text_processor.parse_cycle_number
        )
        df_result['parsed_last_visit_day'] = df_result['last_study_visit_recorded'].apply(
            self.text_processor.parse_cycle_day
        )

        return df_result

    def aggregate_by_patient_medicine(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate data by patient and medicine combination

        Args:
            df: DataFrame with calculated medicine requirements

        Returns:
            Aggregated DataFrame with one row per patient-medicine combination
        """
        logger.info("Aggregating by patient and medicine")

        # Define aggregation
        id_cols = ["study_protocol", "subject_number", "drug_dispensed"]

        # Columns to sum
        sum_cols = ["total_medicines_required_per_cycle"]

        # All other columns - take the first value
        other_cols = [col for col in df.columns if col not in id_cols + sum_cols]

        agg_dict = {col: 'first' for col in other_cols}
        agg_dict['last_study_visit_date'] = 'max'
        agg_dict.update({col: 'sum' for col in sum_cols})

        df_sorted = df.sort_values('last_study_visit_date', ascending=False, na_position='last')
        df_aggregated = df_sorted.groupby(id_cols, dropna=False).agg(agg_dict).reset_index()

        logger.info(f"Aggregated to {len(df_aggregated)} patient-medicine combinations")

        # Add flags for Crossover and TPC to help with prefix generation
        df_aggregated['is_crossover'] = df_aggregated['last_study_visit_recorded'].astype(str).str.contains('Crossover', case=False)
        df_aggregated['is_tpc'] = df_aggregated['last_study_visit_recorded'].astype(str).str.contains('tpc', case=False)

        return df_aggregated


    def project_all_visits(self, df_plan: pd.DataFrame) -> pd.DataFrame:
        """
        Project future visits for all patients

        Args:
            df_plan: Aggregated patient-medicine DataFrame

        Returns:
            DataFrame with all projected visits
        """
        logger.info("Projecting future visits for all patients")

        all_visits = []
        error_subjects = []

        total_rows = len(df_plan)

        for idx, row in df_plan.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing row {idx}/{total_rows}")

            try:
                visits = self.visit_projector.project_future_visits(row)

                # Convert ProjectedVisit objects to dictionaries with all necessary columns
                for visit in visits:
                    all_visits.append({
                        # Visit-specific columns
                        'predicted_study_visit': visit.visit_description,
                        'cycle': visit.cycle_number,
                        'day': visit.cycle_day,
                        'predicted_next_visit_date': visit.visit_date,
                        # Subject and study metadata columns
                        'study_protocol': row['study_protocol'],
                        'parent_depot': row.get('parent_depot'),
                        'site_id': row.get('site_id'),
                        'subject_number': visit.subject_number,
                        'subject_status': row.get('subject_status'),
                        'subject_country': row.get('subject_country'),
                        'randomized_treatment': row.get('randomized_treatment'),
                        'tpc': row.get('tpc'),
                        'drug_dispensed': visit.drug_dispensed,
                        'dispensing_quantity': row.get('dispensing_quantity'),
                        'extract_date': row.get('extract_date')
                    })

            except Exception as e:
                error_subjects.append(row['subject_number'])
                logger.error(f"Error projecting visits for subject {row['subject_number']}: {e}")

        if error_subjects:
            logger.warning(f"Encountered errors for {len(set(error_subjects))} subjects")

        df_visits = pd.DataFrame(all_visits)
        logger.info(f"Generated {len(df_visits)} projected visits")

        return df_visits

    def prepare_final_output(self, df_visits: pd.DataFrame, df_plan: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the final output by formatting the projected visits DataFrame

        Args:
            df_visits: DataFrame with projected visits (already contains all necessary columns)
            df_plan: Original patient-medicine plan DataFrame (not used, kept for backward compatibility)

        Returns:
            Final formatted DataFrame ready for output
        """
        logger.info("Preparing final output")

        # Make a copy to avoid modifying the original
        df_final = df_visits.copy()

        # Rename columns for final output
        df_final.rename(columns={
            'study_protocol': 'study_name'
        }, inplace=True)

        # Define final column order
        final_columns = [
            'study_name', 'parent_depot', 'site_id', 'subject_number',
            'subject_status', 'subject_country', 'randomized_treatment', 'tpc', 'drug_dispensed',
            'dispensing_quantity', 'predicted_study_visit', 'cycle', 'day',
            'predicted_next_visit_date', 'extract_date'
        ]

        # Keep only columns that exist
        final_columns = [col for col in final_columns if col in df_final.columns]
        df_final = df_final[final_columns]

        # Sort for better readability
        df_final = df_final.sort_values(
            by=['study_name', 'parent_depot', 'site_id', 'subject_number', 'cycle', 'day']
        )

        df_final['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        logger.info(f"Final output contains {len(df_final)} records")
        return df_final

    def run(self,
            subject_file: Optional[str] = None,
            mapping_file: Optional[str] = None,
            output_file: Optional[str] = None,
            df_subjects: Optional[pd.DataFrame] = None,
            df_mapping: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Run the complete demand planning process

        Args:
            subject_file: Path to subject summary CSV (used if df_subjects not provided)
            mapping_file: Path to treatment mapping CSV (used if df_mapping not provided)
            output_file: Path for output CSV (optional, if None will not save to file)
            df_subjects: Subject DataFrame (if provided, subject_file is ignored)
            df_mapping: Treatment mapping DataFrame (if provided, mapping_file is ignored)

        Returns:
            Final demand forecast DataFrame
        """
        logger.info("=" * 60)
        logger.info("Starting Demand Planning Process")
        logger.info("=" * 60)

        # Load data - either from dataframes or from files
        if df_subjects is None:
            if subject_file is None:
                raise ValueError("Either df_subjects or subject_file must be provided")
            logger.info(f"Loading subject data from file: {subject_file}")
            df_subjects = self.data_loader.load_subject_data(subject_file)
        else:
            logger.info(f"Using provided subject DataFrame with {len(df_subjects)} records")
            # Make a copy to avoid modifying the original
            df_subjects = df_subjects.copy()
            # Apply the same preparation logic that load_subject_data uses
            df_subjects = self.data_loader.prepare_subject_data(df_subjects)

        if df_mapping is None:
            if mapping_file is None:
                raise ValueError("Either df_mapping or mapping_file must be provided")
            logger.info(f"Loading mapping data from file: {mapping_file}")
            df_mapping = self.data_loader.load_mapping_data(mapping_file)
        else:
            logger.info(f"Using provided mapping DataFrame with {len(df_mapping)} records")
            # Make a copy to avoid modifying the original
            df_mapping = df_mapping.copy()
            # Apply the same preparation logic that load_mapping_data uses
            df_mapping = self.data_loader.prepare_mapping_data(df_mapping)

        # Filter to active subjects
        df_subjects = self.filter_active_subjects(df_subjects)

        # Normalize data
        df_subjects, df_mapping = self.normalize_data(df_subjects, df_mapping)

        # Merge and calculate requirements (includes hierarchical country matching)
        df_merged = self.merge_and_calculate(df_subjects, df_mapping)

        # Country columns are already handled in merge_and_calculate method

        # Aggregate by patient-medicine
        df_plan = self.aggregate_by_patient_medicine(df_merged)

        # Project future visits
        df_visits = self.project_all_visits(df_plan)

        # Prepare final output
        df_final = self.prepare_final_output(df_visits, df_plan)

        # Save to file (optional)
        if output_file is not None:
            logger.info(f"Saving results to {output_file}")
            df_final.to_csv(output_file, index=False)
        else:
            logger.info("Output file not specified, skipping file save")

        logger.info("=" * 60)
        logger.info("Demand Planning Process Complete")
        logger.info("=" * 60)

        return df_final


# ============================================================================
# CONVENIENCE FUNCTIONS FOR NOTEBOOK USAGE
# ============================================================================

def run_demand_planning(df_subjects: pd.DataFrame,
                        df_mapping: pd.DataFrame,
                        output_file: Optional[str] = None) -> pd.DataFrame:
    """
    Convenience function for running demand planning from a notebook with dataframes

    Usage in Databricks notebook:
    ```python
    from demand_planning import run_demand_planning

    # Read your data (from Delta tables, CSV, etc.)
    df_subjects = spark.table("your_subject_table").toPandas()
    df_mapping = spark.table("your_mapping_table").toPandas()

    # Run demand planning
    df_forecast = run_demand_planning(df_subjects, df_mapping)

    # Optionally save to CSV
    df_forecast = run_demand_planning(df_subjects, df_mapping, output_file="output.csv")
    ```

    Args:
        df_subjects: Subject summary DataFrame
        df_mapping: Treatment mapping DataFrame
        output_file: Optional path to save output CSV

    Returns:
        Final demand forecast DataFrame
    """
    processor = DemandPlanningProcessor()
    return processor.run(df_subjects=df_subjects, df_mapping=df_mapping, output_file=output_file)


# ============================================================================
# MAIN EXECUTION (FOR LOCAL DEBUGGING)
# ============================================================================

def main():
    """Main execution function for local debugging with CSV files"""

    # Initialize the processor
    processor = DemandPlanningProcessor()

    # Define file paths
    subject_file = Config.SUBJECT_SUMMARY_FILE
    mapping_file = Config.TREATMENT_MAPPING_FILE
    output_file = Config.OUTPUT_FILE

    try:
        # Run the process with file paths
        result_df = processor.run(subject_file=subject_file,
                                  mapping_file=mapping_file,
                                  output_file=output_file)

        # Display summary statistics
        print("\n" + "=" * 60)
        print("SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Total projected visits: {len(result_df)}")
        print(f"Unique patients: {result_df['subject_number'].nunique()}")
        print(f"Unique drugs: {result_df['drug_dispensed'].nunique()}")
        print(
            f"Date range: {result_df['predicted_next_visit_date'].min()} to {result_df['predicted_next_visit_date'].max()}")

        # Display first few rows
        print("\n" + "=" * 60)
        print("SAMPLE OUTPUT (First 10 rows)")
        print("=" * 60)
        print(result_df.head(10).to_string())

    except Exception as e:
        logger.error(f"Failed to complete demand planning: {e}")
        raise


if __name__ == "__main__":
    main()