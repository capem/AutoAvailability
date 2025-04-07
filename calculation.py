import os
from zipfile import ZipFile
import numpy as np
import pandas as pd

from scipy.interpolate import interp1d

def zip_to_df(data_type, period):
    """
    Read data from CSV files that are either extracted from ZIP archives
    or directly generated from the database.
    
    Args:
        data_type: Type of data (met, tur, grd, etc.)
        sql: SQL query to execute - Note: With CSV files, this parameter is mainly for backward compatibility
        period: Period in YYYY-MM format
        
    Returns:
        DataFrame with the query results
    """
    # Get file information
    file_name = f"{period}-{data_type.lower()}"
    data_type_path = f"./monthly_data/uploads/{data_type.upper()}/"
    csv_file = f"{data_type_path}{file_name}.csv"
    zip_file = f"{data_type_path}{file_name}.zip"
    
    # Check if CSV file already exists
    if not os.path.exists(csv_file):
        # Check if ZIP file exists
        if os.path.exists(zip_file):
            # Extract CSV from ZIP
            with ZipFile(zip_file, "r") as zipf:
                zipf.extractall(data_type_path)

    
    # Read CSV file directly into pandas DataFrame
    try:
        df = pd.read_csv(csv_file)         
    except Exception as e:
        raise ValueError(f"Error reading CSV file {csv_file}: {str(e)}")
    
    # Return the dataframe
    return df


# Determine alarms real periods
def cascade(df):
    df.reset_index(inplace=True, drop=True)
    df["TimeOffMax"] = df.TimeOff.cummax().shift()

    df.at[0, "TimeOffMax"] = df.at[0, "TimeOn"]

    return df


# looping through turbines and applying cascade method
def apply_cascade(result_sum):
    # Sort by alarm ID
    result_sum.sort_values(["TimeOn", "ID"], inplace=True)
    df = result_sum.groupby("StationNr", group_keys=True).apply(cascade, include_groups=False)

    mask_root = df.TimeOn.values >= df.TimeOffMax.values
    mask_children = (df.TimeOn.values < df.TimeOffMax.values) & (df.TimeOff.values > df.TimeOffMax.values)
    mask_embedded = df.TimeOff.values <= df.TimeOffMax.values

    df.loc[mask_root, "NewTimeOn"] = df.loc[mask_root, "TimeOn"]
    df.loc[mask_children, "NewTimeOn"] = df.loc[mask_children, "TimeOffMax"]
    df.loc[mask_embedded, "NewTimeOn"] = df.loc[mask_embedded, "TimeOff"]

    # df.drop(columns=["TimeOffMax"], inplace=True)

    df.reset_index(inplace=True)

    TimeOff = df.TimeOff
    NewTimeOn = df.NewTimeOn

    df["RealPeriod"] = abs(TimeOff - NewTimeOn)

    mask_siemens = df["Error Type"] == 1
    mask_tarec = df["Error Type"] == 0

    df["Period Siemens(s)"] = df[mask_siemens].RealPeriod  # .dt.seconds
    df["Period Tarec(s)"] = df[mask_tarec].RealPeriod  # .dt.seconds

    return df


def realperiod_10mins(last_df, type="1-0"):
    last_df["TimeOnRound"] = last_df["NewTimeOn"].dt.ceil("10min")
    last_df["TimeOffRound"] = last_df["TimeOff"].dt.ceil("10min")
    last_df["TimeStamp"] = last_df.apply(
        lambda row: pd.date_range(row["TimeOnRound"], row["TimeOffRound"], freq="10min"),
        axis=1,
    )
    last_df = last_df.explode("TimeStamp")
    if type != "2006":
        last_df["RealPeriod"] = pd.Timedelta(0)
        last_df["Period Siemens(s)"] = pd.Timedelta(0)
        last_df["Period Tarec(s)"] = pd.Timedelta(0)

    df_TimeOn = last_df[["TimeStamp", "NewTimeOn"]].copy()
    df_TimeOff = last_df[["TimeStamp", "TimeOff"]].copy()

    df_TimeOn["TimeStamp"] = df_TimeOn["TimeStamp"] - pd.Timedelta(minutes=10)

    last_df["10minTimeOn"] = df_TimeOn[["TimeStamp", "NewTimeOn"]].max(1).values

    last_df["10minTimeOff"] = df_TimeOff[["TimeStamp", "TimeOff"]].min(1).values

    last_df["RealPeriod"] = last_df["10minTimeOff"] - last_df["10minTimeOn"]

    if type != "2006":
        mask_siemens = last_df["Error Type"] == 1
        mask_tarec = last_df["Error Type"] == 0
        last_df.loc[mask_siemens, "Period Siemens(s)"] = last_df.loc[mask_siemens, "RealPeriod"]
        last_df.loc[mask_tarec, "Period Tarec(s)"] = last_df.loc[mask_tarec, "RealPeriod"]

    return last_df


def remove_1005_overlap(df):  # input => alarmsresultssum
    df = df[
        [
            "TimeOn",
            "TimeOff",
            "StationNr",
            "Alarmcode",
            "Parameter",
            "ID",
            "NewTimeOn",
            "OldTimeOn",
            "OldTimeOff",
            "UK Text",
            "Error Type",
            "RealPeriod",
        ]
    ].copy()
    idx_to_drop = df.loc[(df.RealPeriod == pd.Timedelta(0)) & (df.Alarmcode != 1005)].index
    df.drop(idx_to_drop, inplace=True)

    df.reset_index(drop=True, inplace=True)
    df_1005 = df.query("Alarmcode == 1005")

    df["TimeOn"] = df["NewTimeOn"]

    for _, j in df_1005.iterrows():
        overlap_end = (
            (df["TimeOn"] <= j["TimeOn"])
            & (df["TimeOn"] <= j["TimeOff"])
            & (df["TimeOff"] > j["TimeOn"])
            & (df["TimeOff"] <= j["TimeOff"])
            & (df["StationNr"] == j["StationNr"])
            & (df["Alarmcode"] != 1005)
        )

        overlap_start = (
            (df["TimeOn"] >= j["TimeOn"])
            & (df["TimeOn"] <= j["TimeOff"])
            & (df["TimeOff"] > j["TimeOn"])
            & (df["TimeOff"] >= j["TimeOff"])
            & (df["StationNr"] == j["StationNr"])
            & (df["Alarmcode"] != 1005)
        )

        embedded = (
            (df["TimeOn"] < j["TimeOn"])
            & (df["TimeOff"] > j["TimeOff"])
            & (df["StationNr"] == j["StationNr"])
            & (df["Alarmcode"] != 1005)
        )
        df_helper = df.loc[embedded].copy()

        df.loc[overlap_start, "TimeOn"] = j["TimeOff"]
        df.loc[overlap_end, "TimeOff"] = j["TimeOn"]

        # ---------------------------------------------------
        if embedded.sum():
            df.loc[embedded, "TimeOff"] = j["TimeOn"]

            df_helper["TimeOn"] = j["TimeOff"]
            df = pd.concat([df, df_helper]).sort_values(["TimeOn", "ID"])

        # ---------------------------------------------------
        # df.reset_index(drop=True, inplace=True)
        reverse_embedded = (
            (df["TimeOn"] >= j["TimeOn"])
            & (df["TimeOff"] <= j["TimeOff"])
            & (df["StationNr"] == j["StationNr"])
            & (df["Alarmcode"] != 1005)
        )
        if reverse_embedded.sum():
            # df.drop(df.loc[reverse_embedded].index, inplace=True)
            # df = df.loc[~df.index.drop(df.loc[reverse_embedded].index)]
            df.loc[reverse_embedded, "TimeOn"] = df.loc[reverse_embedded, "TimeOff"]

    df.loc[df["Alarmcode"] == 1005, "TimeOn"] = df.loc[df["Alarmcode"] == 1005, "OldTimeOn"]

    df = apply_cascade(df)

    return df


def full_range(df, full_range_var):
    new_df = pd.DataFrame(index=full_range_var)

    df = df.set_index("TimeStamp")
    return new_df.join(df, how="left")


def CF(M, WTN=131, AL_ALL=0.08):
    def AL(M):
        return AL_ALL * (M - 1) / (WTN - 1)

    return (1 - AL_ALL) / (1 - AL(M))


def ep_cf(x):
    M = len(x)
    x = x.mean()
    x = round(x * CF(M), 2)
    return x


def cf_column(x):
    M = len(x)
    return CF(M)


def Epot_case_2(df):
    CB2 = pd.read_excel("CB2.xlsx")
    CB2 = CB2.astype(int).drop_duplicates()
    CB2_interp = interp1d(CB2.Wind, CB2.Power, kind="linear", fill_value="extrapolate")

    # List of desired columns
    desired_columns = ["met_WindSpeedRot_mean_38", "met_WindSpeedRot_mean_39", "met_WindSpeedRot_mean_246"]

    # Filter out the columns that exist in df
    available_columns = [col for col in desired_columns if col in df.columns]

    # Calculate the mean of the available columns for all rows
    if available_columns:
        mean_wind_speed = df[available_columns].mean(axis=1)
        # Apply the interpolation function directly to this array
        Epot = CB2_interp(mean_wind_speed) / 6
        return Epot
    else:
        raise ValueError("None of the specified columns are present in the DataFrame.")


def Epot_case_3(period):
    NWD = pd.read_excel("NWD.xlsx", index_col=0)
    SWF = pd.read_excel("SWF.xlsx", index_col=0)
    CB2 = pd.read_excel("CB2.xlsx")
    PWE = 0.92
    NAE = 0
    CB2 = CB2.astype(int).drop_duplicates()
    CB2_interp = interp1d(CB2.Wind, CB2.Power, kind="linear", fill_value="extrapolate")

    bins_v = np.arange(1, 26, 1)
    for v in bins_v:
        NAE += CB2_interp(v) * NWD.loc[v].values[0]

    NAE *= PWE
    Epot = NAE * (1 / 8760) * (1 / 6) * SWF.loc[period].values[0]
    return Epot


class read_files:
    # ------------------------------grd-------------------------------------
    @staticmethod
    def read_grd(period):

        grd = zip_to_df(data_type="grd", period=period)
        grd["TimeStamp"] = pd.to_datetime(grd["TimeStamp"])

        return grd

    # ------------------------------cnt-------------------------------------
    @staticmethod
    def read_cnt(period):

        cnt = zip_to_df(data_type="cnt",  period=period)
        cnt["TimeStamp"] = pd.to_datetime(cnt["TimeStamp"])

        return cnt

    # -----------------------------sum---------------------------
    @staticmethod
    def read_sum(period):

        alarms = zip_to_df("sum", period)

        alarms.dropna(subset=["Alarmcode"], inplace=True)
        alarms["TimeOn"] = pd.to_datetime(alarms["TimeOn"], format="%Y-%m-%d %H:%M:%S.%f")
        alarms["TimeOff"] = pd.to_datetime(alarms["TimeOff"], format="%Y-%m-%d %H:%M:%S.%f")

        alarms = alarms[alarms.StationNr >= 2307405]
        alarms = alarms[alarms.StationNr <= 2307535].reset_index(drop=True)
        alarms.reset_index(drop=True, inplace=True)
        alarms["Alarmcode"] = alarms.Alarmcode.astype(int)
        alarms["Parameter"] = alarms.Parameter.str.replace(" ", "")

        return alarms

    # ------------------------------tur---------------------------
    @staticmethod
    def read_tur(period):

        tur = zip_to_df("tur", period)
        tur["TimeStamp"] = pd.to_datetime(tur["TimeStamp"])

        return tur

    # ------------------------------met---------------------------
    @staticmethod
    def read_met(period):

        met = zip_to_df("met", period)
        met["TimeStamp"] = pd.to_datetime(met["TimeStamp"])

        met = met.pivot_table(
            index="TimeStamp",
            columns="StationId",
            values=["met_WindSpeedRot_mean", "met_WinddirectionRot_mean"],
            aggfunc="mean",
        )

        met.columns = met.columns.to_flat_index()

        met.reset_index(inplace=True)

        met.columns = ["_".join(str(v) for v in tup) if type(tup) is tuple else tup for tup in met.columns]

        return met

    @staticmethod
    def read_din(period):

        din = zip_to_df("din", period)
        din["TimeStamp"] = pd.to_datetime(din["TimeStamp"])

        return din

    @staticmethod
    def read_all(period):
        return (
            read_files.read_met(period),
            read_files.read_tur(period),
            read_files.read_sum(period),
            read_files.read_cnt(period),
            read_files.read_grd(period),
            read_files.read_din(period),
        )


def full_calculation(period):
    # reading all files with function
    met, tur, alarms, cnt, grd, din = read_files.read_all(period)

    # ------------------------------------------------------------

    period_start = pd.Timestamp(f"{period}-01 00:00:00.000")

    # if currentPeriod_dt <= period_dt:  # if calculating ongoing month
    period_end = cnt.TimeStamp.max()
    # else:
    #     period_end = pd.Timestamp(f"{next_period}-01 00:10:00.000")

    full_range_var = pd.date_range(period_start, period_end, freq="10min")

    # ----------------------Sanity check---------------------------
    sanity_grd = grd.query(
        """-1000 <= wtc_ActPower_min <= 2600 & -1000 <= wtc_ActPower_max <= 2600 & -1000 <= wtc_ActPower_mean <= 2600"""
    ).index
    sanity_cnt = cnt.query("""-500 <= wtc_kWG1Tot_accum <= 500 & 0 <= wtc_kWG1TotE_accum <= 500""").index
    sanity_tur = tur.query("""0 <= wtc_AcWindSp_mean <= 50 & 0 <= wtc_ActualWindDirection_mean <= 360""").index
    # sanity_met = met.query('''0 <= met_WindSpeedRot_mean <= 50 & 0 <= met_WinddirectionRot_mean <= 360''').index
    sanity_din = din.query("""0 <= wtc_PowerRed_timeon <= 600""").index

    # grd_outliers = grd.loc[grd.index.difference(sanity_grd)]
    # cnt_outliers = cnt.loc[cnt.index.difference(sanity_cnt)].groupby('StationId').apply(
    #     lambda df: df.reindex(index=full_range_var)
    # )

    # cnt_outliers = cnt.groupby('StationId').apply(
    #     lambda df: df.reindex(index=full_range_var))

    # cnt_outliers = cnt_outliers.loc[cnt_outliers.wtc_kWG1TotE_accum.isna()]

    # tur_outliers = tur.loc[tur.index.difference(sanity_tur)]
    # # met_outliers = met.loc[met.index.difference(sanity_met)]
    # din_outliers = din.loc[din.index.difference(sanity_din)]

    # with pd.ExcelWriter(f'./monthly_data/results/outliers/{period}_outliers.xlsx') as writer:
    #     grd_outliers.to_excel(writer, sheet_name='grd')
    #     cnt_outliers.to_excel(writer, sheet_name='cnt')
    #     tur_outliers.to_excel(writer, sheet_name='tur')
    #     din_outliers.to_excel(writer, sheet_name='din')

    grd = grd.loc[grd.index.isin(sanity_grd)]
    cnt = cnt.loc[cnt.index.isin(sanity_cnt)]
    tur = tur.loc[tur.index.isin(sanity_tur)]
    # met = met.loc[met.index.isin(sanity_met)]
    din = din.loc[din.index.isin(sanity_din)]

    # --------------------------error list-------------------------
    error_list = pd.read_excel(r"Alarmes List Norme RDS-PP_Tarec.xlsx")

    error_list.Number = error_list.Number.astype(int)  # ,errors='ignore'

    error_list.drop_duplicates(subset=["Number"], inplace=True)

    error_list.rename(columns={"Number": "Alarmcode"}, inplace=True)

    # ------------------------------------------------------

    # for i in range(1, 12):  # append last months alarms

    #     ith_previous_period_dt = period_dt + relativedelta(months=-i)
    #     ith_previous_period = ith_previous_period_dt.strftime("%Y-%m")

    #     try:
    #         previous_alarms = read_files.read_sum(ith_previous_period)
    #         alarms = alarms.append(previous_alarms)

    #     except FileNotFoundError:
    #         print(f"Previous mounth -{i} alarms File not found")

    # ------------------------------------------------------
    alarms_0_1 = error_list.loc[error_list["Error Type"].isin([1, 0])].Alarmcode

    # ------------------------------Fill NA TimeOff-------------------------------------

    alarms["OldTimeOn"] = alarms["TimeOn"]
    alarms["OldTimeOff"] = alarms["TimeOff"]

    print(f"TimeOff NAs = {alarms.loc[alarms.Alarmcode.isin(alarms_0_1)].TimeOff.isna().sum()}")

    if alarms.loc[alarms.Alarmcode.isin(alarms_0_1)].TimeOff.isna().sum():
        print(
            f"earliest TimeOn when TimeOff is NA= \
            {alarms.loc[alarms.Alarmcode.isin(alarms_0_1) & alarms.TimeOff.isna()].TimeOn.min()}"
        )

    alarms.loc[alarms.Alarmcode.isin(alarms_0_1), "TimeOff"] = alarms.loc[
        alarms.Alarmcode.isin(alarms_0_1), "TimeOff"
    ].fillna(period_end)

    # ------------------------------Alarms ending after period end ----------------------

    alarms.loc[(alarms.TimeOff > period_end), "TimeOff"] = period_end

    # ------------------------------Keep only alarms active in period-------------
    alarms.reset_index(inplace=True, drop=True)
    # ----dropping 1 0 alarms
    alarms.drop(
        alarms.query("(TimeOn < @period_start) & (TimeOff < @period_start) & Alarmcode.isin(@alarms_0_1)").index,
        inplace=True,
    )

    alarms.drop(alarms.query("(TimeOn > @period_end)").index, inplace=True)
    alarms.reset_index(drop=True, inplace=True)
    # ------------------------------Alarms starting before period start -----------------
    warning_date = alarms.TimeOn.min()

    alarms.loc[(alarms.TimeOn < period_start) & (alarms.Alarmcode.isin(alarms_0_1)), "TimeOn"] = period_start

    # ----dropping non 1 0 alarms
    alarms.drop(
        alarms.query("~Alarmcode.isin(@alarms_0_1) & (TimeOn < @period_start)").index,
        inplace=True,
    )
    alarms.reset_index(drop=True, inplace=True)

    """ label scada alarms with coresponding error type
    and only keep alarm codes in error list"""
    result_sum = pd.merge(alarms, error_list, on="Alarmcode", how="inner", sort=False)

    # Remove warnings
    result_sum = result_sum.loc[result_sum["Error Type"].isin([1, 0])]

    # Determine alarms real periods applying cascade method

    # apply cascade
    alarms_result_sum = apply_cascade(result_sum)
    alarms_result_sum = remove_1005_overlap(alarms_result_sum)

    # -------------------2006  binning --------------------------------------

    print("binning 2006")

    alarms_df_2006 = alarms.loc[(alarms["Alarmcode"] == 2006)].copy()
    # alarms_df_2006['TimeOff'] = alarms_df_2006['NewTimeOn']
    alarms_df_2006["NewTimeOn"] = alarms_df_2006["TimeOn"]

    alarms_df_2006 = alarms_df_2006.query(
        "(@period_start < TimeOn < @period_end) | \
                                   (@period_start < TimeOff < @period_end) | \
                                   ((TimeOn < @period_start) & (@period_end < TimeOff))"
    )

    if not alarms_df_2006.empty:
        alarms_df_2006["TimeOff"] = alarms_df_2006["TimeOff"].fillna(period_end)
        alarms_df_2006_10min = realperiod_10mins(alarms_df_2006, "2006")

        alarms_df_2006_10min = alarms_df_2006_10min.groupby("TimeStamp", group_keys=True).agg(
            {"RealPeriod": "sum", "StationNr": "first"}
        )

        alarms_df_2006_10min.reset_index(inplace=True)

        alarms_df_2006_10min.rename(
            columns={"StationNr": "StationId", "RealPeriod": "Duration 2006(s)"},
            inplace=True,
        )

        alarms_df_2006_10min["Duration 2006(s)"] = alarms_df_2006_10min["Duration 2006(s)"].dt.total_seconds().fillna(0)

    else:
        print("no 2006")
        alarms_df_2006_10min = pd.DataFrame(columns=["TimeStamp", "Duration 2006(s)", "StationId"])
    # ----------------------- binning --------------------------------------

    print("Binning")
    alarms_df_clean = alarms_result_sum.loc[(alarms_result_sum["RealPeriod"].dt.total_seconds() != 0)].copy()

    alarms_df_clean_10min = realperiod_10mins(alarms_df_clean)
    alarms_df_clean_10min.reset_index(inplace=True, drop=True)

    # # ----------------------- ---------------------------------

    alarms_binned = (
        alarms_df_clean_10min.groupby(["StationNr", "TimeStamp"], group_keys=True)
        .agg(
            {
                "RealPeriod": "sum",
                "Period Tarec(s)": "sum",
                "Period Siemens(s)": "sum",
                "UK Text": "|".join,
            }
        )
        .reset_index()
    )

    alarms_binned = (
        alarms_binned.groupby("StationNr", group_keys=True)
        .apply(lambda df: full_range(df, full_range_var), include_groups=False)
        .reset_index()
        .rename(
            columns={
                "level_1": "TimeStamp",
                "StationNr": "StationId",
                "Period Tarec(s)": "Period 0(s)",
                "Period Siemens(s)": "Period 1(s)",
            }
        )
    )

    alarms_binned["Period 0(s)"] = alarms_binned["Period 0(s)"].dt.total_seconds().fillna(0)
    alarms_binned["Period 1(s)"] = alarms_binned["Period 1(s)"].dt.total_seconds().fillna(0)
    alarms_binned["RealPeriod"] = alarms_binned["RealPeriod"].dt.total_seconds().fillna(0)

    alarms_binned.drop(
        alarms_binned.loc[alarms_binned["TimeStamp"] == period_start].index,
        inplace=True,
    )

    print("Alarms Binned")

    # ----------Merging with 2006 alarms

    alarms_binned = pd.merge(
        alarms_binned, alarms_df_2006_10min, on=["TimeStamp", "StationId"], how="left"
    ).reset_index(drop=True)

    # -------merging cnt, grd, tur, met,upsampled------
    # merging upsampled alarms with energy production

    print("merging upsampled alarms with energy production")
    results = pd.merge(alarms_binned, cnt, on=["TimeStamp", "StationId"], how="left").reset_index(drop=True)

    # merging last dataframe with power
    results = pd.merge(results, grd, on=["TimeStamp", "StationId"], how="left")

    results.reset_index(drop=True, inplace=True)

    # merging last dataframe with turbine windspeed data
    results = pd.merge(
        results,
        tur[
            [
                "TimeStamp",
                "StationId",
                "wtc_AcWindSp_mean",
                "wtc_ActualWindDirection_mean",
            ]
        ],
        on=("TimeStamp", "StationId"),
        how="left",
    )

    # merging last dataframe with met mast data
    results = pd.merge(results, met, on="TimeStamp", how="left")

    # merging last dataframe with curtailement
    results = pd.merge(results, din, on=["StationId", "TimeStamp"], how="left")
    results = results.infer_objects()
    results["Duration 2006(s)"] = results["Duration 2006(s)"].fillna(0)

    # -------- operational turbines mask --------------------------------------
    mask_n = (
        (results["wtc_kWG1TotE_accum"] > 0)
        & (results["RealPeriod"] == 0)
        & (results["wtc_ActPower_min"] > 0)
        & (results["Duration 2006(s)"] == 0)
        & (
            (results["wtc_PowerRed_timeon"] == 0)
            | ((results["wtc_PowerRed_timeon"] != 0) & (results["wtc_ActPower_max"] > 2200))
        )
    )

    # -------- operational turbines -------------------------------------------
    results_n = results.loc[mask_n].copy()

    results_n["Correction Factor"] = 0
    results_n["Available Turbines"] = 0

    Epot = (
        results_n.groupby("TimeStamp", group_keys=True)
        .agg(
            {
                "wtc_kWG1TotE_accum": ep_cf,
                "Correction Factor": cf_column,
                "Available Turbines": "count",
            }
        )
        .copy()
    )

    Epot = Epot.rename(columns={"wtc_kWG1TotE_accum": "Epot"})

    del results_n["Correction Factor"]
    del results_n["Available Turbines"]

    results_n = pd.merge(results_n, Epot, on="TimeStamp", how="left")

    results_n["Epot"] = results_n["wtc_kWG1TotE_accum"]

    results_no = results.loc[~mask_n].copy()

    results_no = pd.merge(results_no, Epot, on="TimeStamp", how="left")

    mask = results_no["Epot"] < results_no["wtc_kWG1TotE_accum"]

    results_no.loc[mask, "Epot"] = results_no.loc[mask, "wtc_kWG1TotE_accum"]

    results_final = pd.DataFrame()

    results_final = pd.concat([results_no, results_n], sort=False)

    results_final = results_final.sort_values(["StationId", "TimeStamp"]).reset_index(drop=True)

    # Check if there are any NA values in the 'Epot' column
    if results_final["Epot"].isna().any():
        mask_Epot_case_2 = results_final["Epot"].isna()

        Epot_case_2_var = Epot_case_2(results_final.loc[mask_Epot_case_2])

        results_final.loc[mask_Epot_case_2, "Epot"] = np.maximum(
            Epot_case_2_var,
            results_final.loc[mask_Epot_case_2, "wtc_kWG1TotE_accum"].fillna(0).values,
        )

    results_final["EL"] = results_final["Epot"].fillna(0) - results_final["wtc_kWG1TotE_accum"].fillna(0)

    results_final["EL"] = results_final["EL"].clip(lower=0)

    # results_final = results_final.fillna(0)

    results_final["ELX"] = (
        (results_final["Period 0(s)"] / (results_final["Period 0(s)"] + results_final["Period 1(s)"]))
        * (results_final["EL"])
    ).fillna(0)

    results_final["ELNX"] = (
        (results_final["Period 1(s)"] / (results_final["Period 0(s)"] + results_final["Period 1(s)"]))
        * (results_final["EL"])
    ).fillna(0)

    results_final["EL_indefini"] = results_final["EL"] - (results_final["ELX"] + results_final["ELNX"])

    # -------------------------------------------------------------------------

    results_final["prev_AcWindSp"] = results_final.groupby("TimeStamp", group_keys=True)["wtc_AcWindSp_mean"].shift()

    results_final["next_AcWindSp"] = results_final.groupby("TimeStamp", group_keys=True)["wtc_AcWindSp_mean"].shift(-1)

    results_final["prev_ActPower_min"] = results_final.groupby("TimeStamp", group_keys=True)["wtc_ActPower_min"].shift()

    results_final["next_ActPower_min"] = results_final.groupby("TimeStamp", group_keys=True)["wtc_ActPower_min"].shift(
        -1
    )

    results_final["prev_Alarme"] = results_final.groupby("TimeStamp", group_keys=True)["RealPeriod"].shift()

    results_final["next_Alarme"] = results_final.groupby("TimeStamp", group_keys=True)["RealPeriod"].shift(-1)

    results_final["DiffV1"] = results_final.prev_AcWindSp - results_final.wtc_AcWindSp_mean

    results_final["DiffV2"] = results_final.next_AcWindSp - results_final.wtc_AcWindSp_mean

    # -------------------------------------------------------------------------

    mask_4 = (results_final["EL_indefini"] > 0) & (
        (results_final["wtc_PowerRed_timeon"] > 0) & (results_final["wtc_ActPower_max"] > 2300)
    )  # & warning 2006 > 0

    results_final.loc[mask_4, "EL_PowerRed"] = results_final.loc[mask_4, "EL_indefini"]

    results_final["EL_PowerRed"] = results_final["EL_PowerRed"].fillna(0)

    results_final["EL_indefini"] = results_final["EL_indefini"].fillna(0) - results_final["EL_PowerRed"]

    results_final["EL_PowerRed"] = results_final["EL_PowerRed"].fillna(0)

    # -------------------------------------------------------------------------

    mask_5 = (results_final["EL_indefini"] > 0) & (results_final["Duration 2006(s)"] > 0)  # & warning 2006 > 0

    results_final.loc[mask_5, "EL_2006"] = results_final.loc[mask_5, "EL_indefini"]

    results_final["EL_2006"] = results_final["EL_2006"].fillna(0)

    results_final["EL_indefini"] = results_final["EL_indefini"].fillna(0) - results_final["EL_2006"]

    # -------------------------------------------------------------------------

    def lowind(df):
        etape1 = (
            (df.DiffV1 > 1)
            & (df.DiffV2 > 1)
            & ((df.prev_AcWindSp >= 5) | (df.next_AcWindSp >= 5) | (df.wtc_AcWindSp_mean >= 5))
        )

        mask_1 = ~(
            etape1
            & (
                ((df.prev_ActPower_min > 0) & (df.next_ActPower_min > 0))
                | ((df.prev_ActPower_min > 0) & (df.next_Alarme > 0))
                | ((df.next_ActPower_min > 0) & (df.prev_Alarme > 0))
            )
        ) & (df["EL_indefini"] > 0)

        mask_1 = mask_1.astype('boolean')
        mask_2 = mask_1.shift().bfill()

        df.loc[mask_1, "EL_wind"] = df.loc[mask_1, "EL_indefini"].fillna(0)

        df.loc[mask_1, "Duration lowind(s)"] = 600

        df.loc[mask_2 & ~mask_1, "EL_wind_start"] = (df.loc[mask_2 & ~mask_1, "EL_indefini"]).fillna(0)

        df.loc[mask_2 & ~mask_1, "Duration lowind_start(s)"] = 600
        # ---------------------------------------------------------------------

        mask_3 = (df["RealPeriod"] > 0).shift() & (df["EL_indefini"] > 0) & (df["RealPeriod"] == 0)

        df.loc[~mask_1 & ~mask_2 & mask_3, "EL_alarm_start"] = (
            df.loc[~mask_1 & ~mask_2 & mask_3, "EL_indefini"]
        ).fillna(0)

        df.loc[~mask_1 & ~mask_2 & mask_3, "Duration alarm_start(s)"] = 600

        return df

    results_final = results_final.groupby("StationId", group_keys=False).apply(lowind, include_groups=False)

    results_final["EL_indefini_left"] = results_final["EL_indefini"].fillna(0) - (
        results_final["EL_wind"].fillna(0)
        + results_final["EL_wind_start"].fillna(0)
        + results_final["EL_alarm_start"].fillna(0)
    )

    # -------------------------------------------------------------------------
    # # bypass -------------
    # print('bypassing')

    # results_final = pd.read_csv(
    #     f"./monthly_data/results/{period}-Availability.csv",
    #     decimal=',', sep=';')

    # results_final['EL_Misassigned'] = 0

    # # #----end bypass

    # ---------Misassigned low wind---------------
    EL_Misassigned_mask = results_final["UK Text"].str.contains("low wind") & (
        ((results_final["next_ActPower_min"] > 0) & (results_final["prev_ActPower_min"] > 0))
        | ((results_final["next_ActPower_min"] > 0) & (results_final["prev_Alarme"] > 0))
        | ((results_final["prev_ActPower_min"] > 0) & (results_final["next_Alarme"] > 0))
    )

    results_final.loc[EL_Misassigned_mask, "EL_Misassigned"] = results_final.loc[EL_Misassigned_mask, "ELX"]

    results_final["EL_Misassigned"] = results_final["EL_Misassigned"].fillna(0)
    # -------------------------------------------------------------------------

    columns_toround = list(set(results_final.columns) - set(("StationId", "TimeStamp", "UK Text")))
    results_final[columns_toround] = results_final[columns_toround].round(2).astype(np.float32)

    # -------------------------------------------------------------------------
    print(f"warning: first date in alarm = {warning_date}")

    results_final.drop(
        results_final.loc[results_final["TimeStamp"] == period_start].index,
        inplace=True,
    )

    return results_final


if __name__ == "__main__":
    full_calculation("2025-04")
