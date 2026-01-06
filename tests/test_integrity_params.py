
import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add project root to path to allow importing src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src import integrity, config

class TestIntegrityParams(unittest.TestCase):
    def setUp(self):
        # Create a basic dataframe structure
        self.base_date = pd.Timestamp("2023-01-01 00:00")
        self.periods = 10
        self.dates = [self.base_date + pd.Timedelta(minutes=10*i) for i in range(self.periods)]
        
        data = {
            "TimeStamp": self.dates,
            "StationId": [1] * self.periods,
            # Create some dummy columns that are expected
            "met_WindSpeedRot_mean": [10.0] * self.periods, 
            "met_WindSpeedRot_min": [10.0] * self.periods,
            "met_WindSpeedRot_max": [10.0] * self.periods,
            "met_WindSpeedRot_stddev": [0.0] * self.periods,
        }
        self.df = pd.DataFrame(data)

    def test_custom_stuck_intervals(self):
        # Default is 3 (from config, typically). 
        # Let's create a sequence of 2 identical values and see if it is NOT flagged by default
        # but IS flagged if we set stuck_intervals=2.
        
        # Modify data to only have 2 stuck values at the beginning, then change
        self.df["met_WindSpeedRot_mean"] = [10.0, 10.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        self.df["met_WindSpeedRot_min"] = [10.0, 10.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        self.df["met_WindSpeedRot_max"] = [10.0, 10.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        self.df["met_WindSpeedRot_stddev"] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        
        # With default (3), this should NOT return stuck issues for WindSpeed (count < 3)
        # Note: integrity checks look for `n_intervals` identical values.
        # Actually logic is: constant for n_intervals. 
        # If values are [10, 10], that's 2 values.
        
        # Let's verify defaults first. Assuming config.MET_STUCK_INTERVALS >= 3
        issues_default = integrity.scan_met_integrity(self.df)
        stuck_issues_default = [i for i in issues_default if i['type'] == 'stuck_value']
        self.assertEqual(len(stuck_issues_default), 0, "Should not detect stuck values with length 2 when default is 3")

        # Now force check with stuck_intervals=2
        issues_param = integrity.scan_met_integrity(self.df, stuck_intervals=2)
        stuck_issues_param = [i for i in issues_param if i['type'] == 'stuck_value']
        self.assertTrue(len(stuck_issues_param) > 0, "Should detect stuck values with length 2 when parameter is 2")
        self.assertEqual(stuck_issues_param[0]['sensor'], 'met_WindSpeedRot')

    def test_exclude_zero(self):
        # Create a long sequence of stuck ZEROS
        self.df["met_WindSpeedRot_mean"] = [0.0] * self.periods
        self.df["met_WindSpeedRot_min"] = [0.0] * self.periods
        self.df["met_WindSpeedRot_max"] = [0.0] * self.periods
        self.df["met_WindSpeedRot_stddev"] = [0.0] * self.periods
        
        # By default (exclude_zero=False), this SHOULD be stuck
        issues_default = integrity.scan_met_integrity(self.df) # exclude_zero defaults to False
        stuck_issues_default = [i for i in issues_default if i['type'] == 'stuck_value']
        self.assertTrue(len(stuck_issues_default) > 0, "Should detect stuck zeros by default")
        
        # With exclude_zero=True, this should NOT be stuck
        issues_param = integrity.scan_met_integrity(self.df, exclude_zero=True)
        stuck_issues_param = [i for i in issues_param if i['type'] == 'stuck_value']
        self.assertEqual(len(stuck_issues_param), 0, "Should ignore stuck zeros when exclude_zero=True")

if __name__ == '__main__':
    unittest.main()
