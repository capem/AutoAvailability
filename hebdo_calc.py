import pandas as pd
import numpy as np


def mean_angle(deg):
    # Converting degrees to radians for computation
    radians = np.radians(deg)

    # Calculating the sum of the unit vectors for each angle
    sum_sin = np.nansum(np.sin(radians))
    sum_cos = np.nansum(np.cos(radians))

    # Calculating the mean angle
    mean_rad = np.arctan2(sum_sin, sum_cos)
    mean_deg = np.degrees(mean_rad)

    # Normalizing the result to be between 0 and 360
    mean_deg = mean_deg % 360

    return mean_deg


def main(period_range, period_start_dt, period_end_dt):
    results = pd.concat(
        [
            pd.read_pickle(f"./monthly_data/results/{dt.strftime('%Y-%m')}.pkl")
            for dt in period_range
        ],
        ignore_index=True,
    )

    # Directly querying the DataFrame
    results = results.query("@period_start_dt <= TimeStamp <= @period_end_dt")

    # Calculation of sums can be done in a loop to avoid repetition
    sum_columns = [
        "EL",
        "ELX",
        "ELNX",
        "EL_2006",
        "wtc_kWG1TotE_accum",
        "EL_PowerRed",
        "EL_Misassigned",
    ]
    sums = {col: results[col].sum() for col in sum_columns}

    # Remaining calculations
    sums["ELX_eq"] = sums["ELX"] - sums["EL_Misassigned"]
    sums["ELNX_eq"] = (
        sums["ELNX"] + sums["EL_2006"] + sums["EL_PowerRed"] + sums["EL_Misassigned"]
    )
    sums["Epot_eq"] = sums["wtc_kWG1TotE_accum"] + sums["ELX_eq"] + sums["ELNX_eq"]

    title = f"Du {period_start_dt.strftime('%Y_%m_%d')} au {period_end_dt.strftime('%Y_%m_%d')}"

    days_in_period = len(pd.date_range(period_start_dt, period_end_dt, freq="D"))

    df_exploi = pd.DataFrame(index=[title])

    df_exploi.loc[title, "Indispo. SGRE énergie (%)"] = round(
        100 * (sums["ELNX_eq"]) / (sums["Epot_eq"]), 2
    )
    df_exploi.loc[title, "Indispo. SGRE énergie (MWh)"] = round(
        sums["ELNX_eq"] / 1e3, 2
    )
    df_exploi.loc[title, "Indispo. SGRE temps (%)"] = round(
        100 * results["Period 1(s)"].sum() / 3600 / 24 / 131 / days_in_period, 2
    )
    df_exploi.loc[title, "Indispo. SGRE temps (heures)"] = round(
        results["Period 1(s)"].sum() / 3600, 2
    )

    df_exploi.loc[title, "Indispo. TAREC énergie (%)"] = round(
        100 * (sums["ELX_eq"]) / (sums["Epot_eq"]), 2
    )
    df_exploi.loc[title, "Indispo. TAREC énergie (MWh)"] = round(
        sums["ELX_eq"] / 1e3, 2
    )
    df_exploi.loc[title, "Indispo. TAREC temps (%)"] = round(
        100 * results["Period 0(s)"].sum() / 3600 / 24 / 131 / days_in_period, 2
    )
    df_exploi.loc[title, "Indispo. TAREC temps (heures)"] = round(
        results["Period 0(s)"].sum() / 3600, 2
    )

    # Filter wind speed and direction columns that exist
    wind_speed_cols = [
        col
        for col in [
            "met_WindSpeedRot_mean_38",
            "met_WindSpeedRot_mean_39",
            "met_WindSpeedRot_mean_246",
        ]
        if col in results.columns
    ]
    wind_direction_cols = [
        col
        for col in [
            "met_WinddirectionRot_mean_38",
            "met_WinddirectionRot_mean_39",
            "met_WinddirectionRot_mean_246",
        ]
        if col in results.columns
    ]

    # Calculate mean wind speed if any columns are available
    if wind_speed_cols:
        df_exploi.loc[title, "Vent moyen Mâts (m/s)"] = round(
            results[wind_speed_cols].mean().mean(), 2
        )

    # Calculate mean wind direction using your `mean_angle` function if any columns are available
    if wind_direction_cols:
        df_exploi.loc[title, "Direction Moyenne Mâts (°)"] = mean_angle(
            results[wind_direction_cols].apply(mean_angle)
        )
    df_exploi.columns.name = "."

    return df_exploi.map(lambda x: "{:,.2f}".format(x))


def Top15(period_range, period_start_dt, period_end_dt):
    title = f"From {period_start_dt.strftime('%Y_%m_%d')} To {period_end_dt.strftime('%Y_%m_%d')}"

    results = pd.concat(
        [
            pd.read_pickle(f"./monthly_data/results/{dt.strftime('%Y-%m')}.pkl")
            for dt in period_range
        ],
        ignore_index=True,
    )
    results["StationId"] -= 2307404
    df_Top15 = (
        results[["StationId", "ELNX", "ELX"]]
        .groupby("StationId")
        .sum()
        .sort_values("StationId")
        # .head(20)
        .reset_index()
    )

    df_Top15[["ELNX", "ELX"]] = df_Top15[["ELNX", "ELX"]] / 1e3

    df_Top15.rename(
        {"ELNX": "Energie perdue SGRE", "ELX": "Energie perdue TAREC"},
        inplace=True,
        axis=1,
    )

    df_Top15 = df_Top15.round(2)

    df_Top15.set_index("StationId", inplace=True)

    df_Top15 = df_Top15.sum(1).to_frame().sort_values(0, ascending=False).head(15)

    df_Top15.rename(columns={0: f"Total Energy Lost(MWh) {title}"}, inplace=True)

    df_Top15.columns.name = "."

    return df_Top15.map(lambda x: "{:,.2f}".format(x))
