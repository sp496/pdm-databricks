import os
import pandas as pd
from typing import Optional, Dict, Any, Callable

VALID_SOURCES = {"spark", "starburst", "file"}


class DataBackend:
    """
    Handles connection setup and query execution for a single data source.

    data_source="spark"      — prod: queries run via Spark SQL against Databricks tables.
    data_source="starburst"  — dev:  queries run via Starburst/Trino JDBC.
    data_source="file"       — local/testing: skips all connection attempts, loads from files directly.

    For "spark" and "starburst", load() falls back to a local file if the live query fails.
    """

    def __init__(self, data_source: str, starburst_config: Optional[Dict[str, Any]] = None):
        if data_source not in VALID_SOURCES:
            raise ValueError(f"data_source must be one of {VALID_SOURCES}, got '{data_source}'")
        if data_source == "starburst" and not starburst_config:
            raise ValueError("starburst_config is required when data_source='starburst'")

        self.data_source = data_source
        self.name = data_source.capitalize()
        self.available = False

        self._spark = None
        self._base_url = None
        self._properties = None
        self._default_catalog = None
        self._default_schema = None

        if data_source == "file":
            print("\tdata_source='file' — skipping connection setup, all data loaded from files")
        elif data_source == "spark":
            self._setup_spark()
            self.available = self._spark is not None
        elif data_source == "starburst":
            self._setup_spark()
            self._setup_starburst(starburst_config)

    def _setup_spark(self):
        try:
            import pyspark
            spark = pyspark.sql.SparkSession.getActiveSession()
            if spark:
                spark.sql("SELECT 1").collect()
                print("\tSpark session is available")
                self._spark = spark
            else:
                print("\tNo active Spark session found")
        except Exception as e:
            print(f"\tWarning: Could not access Spark session: {e}")

    def _setup_starburst(self, starburst_config: Dict[str, Any]):
        if not self._spark:
            print("\tStarburst requested but no Spark session available — falling back to files")
            return
        try:
            self._base_url = starburst_config.get('base_url', 'jdbc:trino://query.gilead.com:443')
            self._default_catalog = starburst_config.get('default_catalog', 'pdm')
            self._default_schema = starburst_config.get('default_schema', 'default')
            self._properties = {
                "user": starburst_config.get('username'),
                "password": starburst_config.get('password'),
                "SSL": "true",
                "driver": "io.trino.jdbc.TrinoDriver"
            }

            test_url = f"{self._base_url}/{self._default_catalog}/{self._default_schema}"
            test_df = self._spark.read.jdbc(
                url=test_url,
                table="(SELECT 1 as test) AS tmp",
                properties=self._properties
            )
            if test_df.collect()[0]['test'] == 1:
                print("\tStarburst/Trino connection is available")
                self.available = True
            else:
                print("\tStarburst/Trino connection test failed")
        except Exception as e:
            print(f"\tWarning: Could not establish Starburst connection: {e}")

    @staticmethod
    def _cast_temporal_to_string(spark_df):
        """Cast every timestamp/date column to string in Spark.

        Source systems (notably Oracle EBS) use sentinel dates such as
        4712-12-31 that sit far beyond pandas' nanosecond Timestamp range
        (max ~2262-04-11). Letting toPandas() convert those via Arrow fails with
        an "out of bounds timestamp" error. Casting to string in Spark first
        sidesteps the conversion entirely — and is behaviour-preserving, since
        _run_query stringifies the whole frame (.astype(str)) anyway.
        """
        from pyspark.sql import types as T
        from pyspark.sql.functions import col

        temporal_types = (T.TimestampType, T.DateType)
        if hasattr(T, "TimestampNTZType"):  # Spark 3.4+
            temporal_types = temporal_types + (T.TimestampNTZType,)

        for field in spark_df.schema.fields:
            if isinstance(field.dataType, temporal_types):
                spark_df = spark_df.withColumn(field.name, col(field.name).cast("string"))
        return spark_df

    def _run_query(self, query: str) -> pd.DataFrame:
        if self.data_source == "starburst":
            url = f"{self._base_url}/{self._default_catalog}/{self._default_schema}"
            # Wrap the query as a subquery for the JDBC reader. The closing paren
            # MUST sit on its own line: if the query's last line ends in a `--`
            # line comment (e.g. "WHERE x = 131  -- Master Org"), placing `) AS tmp`
            # on the same line would comment out the paren and produce a Trino
            # "mismatched input '<EOF>'" parse error. Leading/trailing newlines
            # make the wrapping robust to comments at either end.
            spark_df = self._spark.read.jdbc(url=url, table=f"(\n{query}\n) AS tmp", properties=self._properties)
            if spark_df.isEmpty():
                raise Exception("Query returned no results")
        else:
            spark_df = self._spark.sql(query)

        # Cast temporal columns to string before toPandas (see helper docstring).
        spark_df = self._cast_temporal_to_string(spark_df)
        return spark_df.toPandas().astype(str)

    def run_query(self, data_name: str, query: str) -> pd.DataFrame:
        """Execute a query against the live backend and return a pandas DataFrame.

        Unlike load(), this has no file fallback — use it for queries that have
        no natural CSV equivalent (e.g. one-off staging queries against a live
        source system). Raises if data_source='file' or the backend is unavailable.
        """
        if self.data_source == "file":
            raise RuntimeError(f"Cannot run_query for '{data_name}' when data_source='file'")
        if not self.available:
            raise RuntimeError(f"{self.name} backend not available for '{data_name}'")

        print(f"\t\tRunning {data_name} query against {self.name}...")
        indented_query = '\n'.join(['\t\t\t' + line for line in query.split('\n')])
        print(f"\t\tQuery:\n{indented_query}")
        df = self._run_query(query)
        print(f"\t\tSuccessfully loaded {data_name} from {self.name} ({len(df)} rows)")
        return df

    def load(self, data_name: str, query: str, file_path: str,
             file_loader_func: Callable, **loader_kwargs) -> pd.DataFrame:
        """
        Load data from the configured source, falling back to a local file on failure.
        When data_source='file', skips live query and loads from file directly.
        """
        if self.data_source != "file" and self.available:
            try:
                print(f"\t\tTrying to load {data_name} from {self.name}...")
                indented_query = '\n'.join(['\t\t\t' + line for line in query.split('\n')])
                print(f"\t\tQuery:\n{indented_query}")
                df = self._run_query(query)
                print(f"\t\tSuccessfully loaded {data_name} from {self.name} ({len(df)} rows)")
                return df
            except Exception as e:
                error_lines = str(e).split('\n')[:2]
                truncated = [l[:100] + "..." if len(l) > 100 else l for l in error_lines]
                print(f"\t\tFailed to load {data_name} from {self.name}: {chr(10).join(truncated)}")
                print(f"\t\tFalling back to file: {file_path}")
        elif self.data_source != "file" and not self.available:
            print(f"\t\t{self.name} not available for {data_name}, loading from file: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Required file not found: {data_name} at {file_path}")

        result_df = file_loader_func(file_path, **loader_kwargs)
        print(f"\t\tSuccessfully loaded {data_name} from file ({len(result_df)} rows)")
        return result_df
