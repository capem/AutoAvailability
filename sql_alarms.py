import pyodbc
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import csv


def construct_query(period_start, period_end, alarms_0_1):
    # Constructing the IN clause for the SQL query
    query = f"""
    SET NOCOUNT ON;
    SELECT [TimeOn]
      ,[TimeOff]
      ,[StationNr]
      ,[Alarmcode]
      ,[Parameter]
      ,[ID]
  FROM [WpsHistory].[dbo].[tblAlarmLog]
  WHERE ([Alarmcode] <> 50100)
  AND (
    ([TimeOff] BETWEEN '{period_start}' AND '{period_end}')
    OR ([TimeOn] BETWEEN '{period_start}' AND '{period_end}')
    OR ([TimeOn] <= '{period_start}' AND [TimeOff] >= '{period_end}')
    OR ([TimeOff] IS NULL AND [Alarmcode] IN {tuple(alarms_0_1.tolist())}))"""

    return query


def main(period):
    server = "10.173.224.101"
    database = "WpsHistory"
    username = "odbc_user"
    password = "0dbc@1cust"
    driver = "{ODBC Driver 11 for SQL Server}"

    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    # Define the period and derive necessary date/time variables

    period_dt = dt.strptime(period, "%Y-%m")

    next_period_dt = period_dt + relativedelta(months=+1)
    next_period = next_period_dt.strftime("%Y-%m")

    period_start = pd.Timestamp(f"{period}-01 00:00:00.000")
    period_end = pd.Timestamp(f"{next_period}-01 00:00:00.000")

    error_list = pd.read_excel(r"Alarmes List Norme RDS-PP_Tarec.xlsx")
    error_list.Number = error_list.Number.astype(int)  # ,errors='ignore'
    error_list.drop_duplicates(subset=["Number"], inplace=True)
    error_list.rename(columns={"Number": "Alarmcode"}, inplace=True)
    alarms_0_1 = error_list.loc[error_list["Error Type"].isin([1, 0])].Alarmcode

    try:
        with pyodbc.connect(connection_string) as connection:
            cursor = connection.cursor()

            # Construct and execute your query
            query = construct_query(period_start, period_end, alarms_0_1)
            cursor.execute(query)

            # Fetch all rows from the query
            rows = cursor.fetchall()

            # Get column headers
            columns = [column[0] for column in cursor.description]

            # Write to CSV file
            with open(
                f"./monthly_data/uploads/SUM/{period}-sum.rpt", mode="w", newline=""
            ) as file:
                writer = csv.writer(file, delimiter="|")

                # Write the headers
                writer.writerow(columns)

                # Write the data rows
                for row in rows:
                    writer.writerow(row)
        print(f"Data exported successfully to {period}-sum.rpt!")
    except Exception as e:
        print(f"Failed to fetch and export data. Reason: {str(e)}")


if __name__ == "__main__":
    main("2023-12")
    # Add the following lines at the end of your script
    print("Press Enter to exit...")
    input()
