
import unittest
import pandas as pd
from datetime import datetime
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.integrity import check_completeness

class TestCompleteness(unittest.TestCase):

    def test_completeness_perfect_data(self):
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2023, 1, 1, 1, 0)
        # 10 min frequency: 00:00, 00:10, 00:20, 00:30, 00:40, 00:50, 01:00 -> 7 points
        timestamps = pd.date_range(start=start, end=end, freq="10min")
        df = pd.DataFrame({"TimeStamp": timestamps})
        
        result = check_completeness(df, start, end, frequency="10min")
        
        self.assertEqual(result["missing_count"], 0)
        self.assertEqual(result["completeness_percentage"], 100.0)
        self.assertEqual(len(result["missing_timestamps"]), 0)

    def test_completeness_missing_data(self):
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2023, 1, 1, 0, 30)
        # Expected: 00:00, 00:10, 00:20, 00:30 (4 points)
        # Actual: 00:00, 00:20 (Missing 00:10, 00:30)
        timestamps = [
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 1, 0, 20)
        ]
        df = pd.DataFrame({"TimeStamp": pd.to_datetime(timestamps)})
        
        result = check_completeness(df, start, end, frequency="10min")
        
        self.assertEqual(result["missing_count"], 2)
        self.assertEqual(result["total_expected"], 4)
        self.assertEqual(result["completeness_percentage"], 50.0)
        self.assertTrue(pd.Timestamp("2023-01-01 00:10:00") in result["missing_timestamps"])
        self.assertTrue(pd.Timestamp("2023-01-01 00:30:00") in result["missing_timestamps"])
        
    def test_completeness_empty_df(self):
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2023, 1, 1, 1, 0)
        df = pd.DataFrame({"TimeStamp": []})
        
        result = check_completeness(df, start, end, frequency="10min")
        
        self.assertEqual(result["missing_count"], 7)
        self.assertEqual(result["completeness_percentage"], 0.0)

    def test_completeness_out_of_range_data(self):
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2023, 1, 1, 0, 20)
        # Expected: 00:00, 00:10, 00:20 (3 points)
        # Data: 00:00, 00:30 (one valid, one out)
        timestamps = [
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 1, 0, 30)
        ]
        df = pd.DataFrame({"TimeStamp": pd.to_datetime(timestamps)})
        
        result = check_completeness(df, start, end, frequency="10min")
        
        self.assertEqual(result["missing_count"], 2) # Missing 00:10, 00:20
        self.assertEqual(result["total_expected"], 3)
        self.assertEqual(result["completeness_percentage"], 33.33)  # 1/3 present

if __name__ == "__main__":
    unittest.main()
