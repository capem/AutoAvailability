import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.integrity import check_met_integrity


class TestMetIntegrity(unittest.TestCase):
    def test_range_checks(self):
        # Create dummy data
        df = pd.DataFrame(
            {
                "TimeStamp": pd.to_datetime(["2023-01-01 00:00", "2023-01-01 00:10"]),
                "StationId": [1, 1],
                "met_WindSpeedRot_mean": [10.5, 100.0],  # 100 is out of range [0, 50]
                "met_TemperatureTen_mean": [
                    -60.0,
                    20.0,
                ],  # -60 is out of range [-50, 60]
                "met_Pressure_mean": [1013, 700],  # 700 is out of range [800, 1100]
                "met_WinddirectionRot_mean": [180, 400],  # 400 is out of range [0, 360]
            }
        )

        df_clean = check_met_integrity(df)

        # Verify replacements
        self.assertTrue(np.isnan(df_clean.loc[1, "met_WindSpeedRot_mean"]))
        self.assertTrue(np.isnan(df_clean.loc[0, "met_TemperatureTen_mean"]))
        self.assertTrue(np.isnan(df_clean.loc[1, "met_Pressure_mean"]))
        self.assertTrue(np.isnan(df_clean.loc[1, "met_WinddirectionRot_mean"]))

        # Verify valid values remain
        self.assertEqual(df_clean.loc[0, "met_WindSpeedRot_mean"], 10.5)
        self.assertEqual(df_clean.loc[1, "met_TemperatureTen_mean"], 20.0)

    def test_stuck_checks(self):
        # Create dummy data with stuck values (n=3)
        df = pd.DataFrame(
            {
                "TimeStamp": pd.to_datetime(
                    [
                        "2023-01-01 00:00",
                        "2023-01-01 00:10",
                        "2023-01-01 00:20",
                        "2023-01-01 00:30",
                    ]
                ),
                "StationId": [1, 1, 1, 1],
                "met_WindSpeedRot_mean": [12.0, 12.0, 12.0, 12.0],
                "met_WindSpeedRot_min": [11.0, 11.0, 11.0, 11.0],
                "met_WindSpeedRot_max": [13.0, 13.0, 13.0, 13.0],
                "met_WindSpeedRot_stddev": [0.5, 0.5, 0.5, 0.5],
                "met_TemperatureTen_mean": [
                    20.0,
                    20.1,
                    20.0,
                    20.0,
                ],  # Not stuck (one variation)
            }
        )

        df_clean = check_met_integrity(df)

        # Wind speed should be stuck (all 4 rows)
        self.assertTrue(df_clean["met_WindSpeedRot_mean"].isna().all())
        self.assertTrue(df_clean["met_WindSpeedRot_min"].isna().all())
        self.assertTrue(df_clean["met_WindSpeedRot_max"].isna().all())
        self.assertTrue(df_clean["met_WindSpeedRot_stddev"].isna().all())

        # Temperature should NOT be stuck
        self.assertFalse(df_clean["met_TemperatureTen_mean"].isna().any())

    def test_stuck_zero_windspeed(self):
        # Zero wind speed SHOULD be marked as stuck (user requirement change)
        df = pd.DataFrame(
            {
                "TimeStamp": pd.to_datetime(
                    [
                        "2023-01-01 00:00",
                        "2023-01-01 00:10",
                        "2023-01-01 00:20",
                        "2023-01-01 00:30",
                    ]
                ),
                "StationId": [1, 1, 1, 1],
                "met_WindSpeedRot_mean": [0.0, 0.0, 0.0, 0.0],
                "met_WindSpeedRot_min": [0.0, 0.0, 0.0, 0.0],
                "met_WindSpeedRot_max": [0.0, 0.0, 0.0, 0.0],
                "met_WindSpeedRot_stddev": [0.0, 0.0, 0.0, 0.0],
            }
        )

        df_clean = check_met_integrity(df)
        self.assertTrue(df_clean["met_WindSpeedRot_mean"].isna().all())


if __name__ == "__main__":
    unittest.main()
