import pytest
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lib"))

from processed.demand_planning_processor import Config


class TestConfig:
    def test_excluded_statuses_is_list(self):
        assert isinstance(Config.EXCLUDED_STATUSES, list)
        assert len(Config.EXCLUDED_STATUSES) > 0

    def test_projection_days_positive(self):
        assert Config.DEFAULT_PROJECTION_DAYS > 0
