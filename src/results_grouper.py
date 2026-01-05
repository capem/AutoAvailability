import pandas as pd
from . import logger_config
import os

# Get a logger for this module
logger = logger_config.get_logger(__name__)

def process_grouped_results(results, period_month):
    """
    Process and group results for a specific period.

    Args:
        results (DataFrame): The results DataFrame to process
        period_month (str): The period month in format 'YYYY-MM'

    Returns:
        DataFrame: The processed and grouped results
    """
    logger.info(f"[GROUPER] Processing grouped results for period {period_month}")

    # Group and round the results
    results_grouped = (
        results.groupby("StationId").sum(numeric_only=True).round(2).reset_index()
    )

    # Extract columns only once
    columns = [
        "wtc_kWG1TotE_accum",
        "EL",
        "ELX",
        "ELNX",
        "EL_2006",
        "EL_PowerRed",
        "EL_Misassigned",
        "EL_wind",
        "EL_wind_start",
        "EL_alarm_start",
    ]
    (
        Ep,
        EL,
        ELX,
        ELNX,
        EL_2006,
        EL_PowerRed,
        EL_Misassigned,
        EL_wind,
        EL_wind_start,
        EL_alarm_start,
    ) = [results_grouped[col] for col in columns]

    # Simplified calculations
    ELX_eq = ELX - EL_Misassigned
    ELNX_eq = ELNX + EL_2006 + EL_PowerRed + EL_Misassigned
    Epot_eq = Ep + ELX_eq + ELNX_eq

    # Calculate MAA_brut and MAA_brut_mis
    results_grouped["MAA_brut"] = (
        100 * (Ep + ELX) / (Ep + ELX + ELNX + EL_2006 + EL_PowerRed)
    )
    results_grouped["MAA_brut_mis"] = round(100 * (Ep + ELX_eq) / Epot_eq, 2)

    # Calculate MAA_indefni_adjusted
    total_EL_wind = EL_wind + EL_wind_start + EL_alarm_start
    results_grouped["MAA_indefni_adjusted"] = (
        100 * (Ep + ELX) / (Ep + EL - total_EL_wind)
    )

    # Adjust index and save to CSV
    results_grouped.index += 1

    # Ensure the directory exists
    output_dir = "./monthly_data/results/Grouped_Results"
    os.makedirs(output_dir, exist_ok=True)

    csv_filename = f"{output_dir}/grouped_{period_month}-Availability.csv"
    results_grouped.to_csv(csv_filename, decimal=",", sep=",")

    logger.info(f"[GROUPER] Saved grouped results to {csv_filename}")

    return results_grouped

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import sys
    if len(sys.argv) > 1:
        period_month = sys.argv[1]
        try:
            results = pd.read_pickle(f"./monthly_data/results/{period_month}.pkl")
            process_grouped_results(results, period_month)
            print(f"Successfully processed grouped results for {period_month}")
        except Exception as e:
            print(f"Error processing grouped results: {e}")
    else:
        print("Please provide a period month in format YYYY-MM")
