from typing import Literal
from tqdm import tqdm
import logging
import pandas as pd
from pygrametl.datasources import CSVSource, SQLSource

# Configure logging
LOG_FILE = "cleaning.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(message)s")


def clear_log_file():
    """Clear the log file contents"""
    with open(LOG_FILE, 'w'):
        pass


def build_dateCode(date) -> str:
    """Build date code in format YYYY-MM-DD"""
    return f"{date.year}-{str(date.month).zfill(2)}-{str(date.day).zfill(2)}"


def build_monthCode(date) -> str:
    """Build month code in format YYYY-MM"""
    return f"{date.year}-{str(date.month).zfill(2)}"


# ==============================================================================
# PHASE 1: Data cleaning and metric pre-computation
# ==============================================================================

def pre_process_flights(flights_source: SQLSource):
    """
    Generator that processes flight data row-by-row, without staging (memory efficient).
    Applies BR1, computes date/month/FH/TDM, adds aggregation flags.
    """
    for row in tqdm(flights_source, desc="Pre-processing flight data"):
        # Business Rule 1 (BR-23): actualArrival must be posterior to actualDeparture
        if not row["cancelled"] and row["actualdeparture"] > row["actualarrival"]:
            # Swap their values
            row["actualdeparture"], row["actualarrival"] = row["actualarrival"], row["actualdeparture"]
            
        # Date/Time transformations
        # Use scheduleddeparture for date/month/year grouping to match baseline queries
        row["date"] = build_dateCode(row["scheduleddeparture"])
        row["month"] = build_monthCode(row["scheduleddeparture"])
        
        # Metric calculations
        if not row["cancelled"]:
            row["FH"] = (row["actualarrival"] - row["actualdeparture"]).total_seconds() / 3600
        else:
            row["FH"] = 0
        
        # Check if flight is delayed (arrival > 15 minutes late from scheduled arrival, and < 6*60))
        if not row["cancelled"]:
            delay_minutes = (row["actualarrival"] - row.get("scheduledarrival")).total_seconds() / 60
            is_delayed = 15 < delay_minutes < 6*60
        else:
            is_delayed = False
        
        if is_delayed:
            row["TDM"] = (row["actualarrival"] - row["scheduledarrival"]).total_seconds() / 60
        else:
            row["TDM"] = 0
        
        # Helper flags for efficient aggregation
        row["is_takeoff"] = 0 if row["cancelled"] else 1
        row["is_delayed"] = 1 if is_delayed else 0
        
        # Drop unused columns to save memory
        row.pop("scheduleddeparture")
        row.pop("scheduledarrival")
        row.pop("delaycode")
        
        yield row


def stage_flights(flights_source: SQLSource) -> pd.DataFrame:
    """Stage and sort flight data. Pre-processes with BR1, date, month, FH, TDM."""
    flights_df = pd.DataFrame(list(pre_process_flights(flights_source)))
    # Sort in order to apply the business rule filtering (BR-21)
    return flights_df.sort_values(by=["aircraftregistration", "actualdeparture"])


def pre_process_maintenance(maintenance_source: SQLSource):
    """
    Generator that processes maintenance data row-by-row, without staging (memory efficient).
    Computes month, ADOSS, and ADOSU metrics.
    """
    for row in tqdm(maintenance_source, desc="Pre-processing maintenance data"):
        # Date/Time transformation
        row["month"] = build_monthCode(row["scheduleddeparture"])
        
        # Calculate total delay duration in days
        total_delay_duration = row["scheduledarrival"] - row["scheduleddeparture"]
        total_duration_days = total_delay_duration.total_seconds() / 86400
        
        # Metric calculations - ADOSS and ADOSU
        if row["programmed"]:
            row["ADOSS"] = total_duration_days
            row["ADOSU"] = 0.0
        else:
            row["ADOSS"] = 0.0
            row["ADOSU"] = total_duration_days
        
        # Drop unused columns to save memory
        row.pop("scheduleddeparture")
        row.pop("scheduledarrival")
        row.pop("programmed")
        
        yield row


def stage_maintenance(maintenance_source: SQLSource) -> pd.DataFrame:
    """Stage, aggregate, and sort maintenance data. Pre-processes with month, ADOSS, ADOSU."""
    maintenance_df = pd.DataFrame(list(pre_process_maintenance(maintenance_source)))
    
    # Group by aircraft and month, summing ADOSS and ADOSU
    aggregated = maintenance_df.groupby(
        ["aircraftregistration", "month"]
    ).agg(
        ADOSS=("ADOSS", "sum"),
        ADOSU=("ADOSU", "sum")
    ).reset_index()
    
    return aggregated.sort_values(by=["aircraftregistration", "month"])


def pre_process_post_flight_reports(reports_source: SQLSource, valid_aircraft_codes: set[str], limit_dates):
    """
    Generator that processes post-flight reports row-by-row, without staging (memory efficient).
    Applies BR3 (validates aircraft), computes month.
    """
    for row in tqdm(reports_source, desc="Pre-processing post-flight reports"):
        
        # Business Rule 3: aircraft registration must be valid
        if row["aircraftregistration"] not in valid_aircraft_codes:
            logging.info(f"BR3 violation - Invalid aircraft registration in logbook: "
                       f"Aircraft {row['aircraftregistration']}, Reporteur {row['reporteurid']}")
            continue
        
        # Ignore reports outside the date range of recorded flights.
        # Otherwise we this data would be useless since it wouldn't be possible to compute any of the
        # metrics (RRh, RRc), because they require flight data (FH, TO)
        min_date, max_date = limit_dates
        if not (min_date <= row["reportingdate"] <= max_date):
            continue
        
        # Date/Time transformation
        row["month"] = build_monthCode(row["reportingdate"])
        
        # Drop unused columns to save memory
        row.pop("reportingdate")
        
        yield row


def stage_post_flight_reports(reports_source: SQLSource, valid_aircraft_codes: set[str], limt_dates) -> pd.DataFrame:
    """
    Stage, aggregate, and sort logbook reports. Pre-processes with BR3, month.
    """
    reports_df = pd.DataFrame(list(pre_process_post_flight_reports(reports_source, valid_aircraft_codes, limt_dates)))
    
    # Group by aircraft, reporteur, and month to count entries
    aggregated = reports_df.groupby(
        ["aircraftregistration", "reporteurid", "reporteurclass", "month"]
    ).size().reset_index(name="logbook_entries")
    
    return aggregated.sort_values(by=["aircraftregistration", "reporteurid", "month"])


def stage_aircrafts(aircraft_source: CSVSource) -> pd.DataFrame:
    aircraft_df = pd.DataFrame(list(aircraft_source))
    return aircraft_df, set(aircraft_df["aircraft_reg_code"].tolist())


# ==============================================================================
# Business Rule 2
# ==============================================================================

def enhance_BR2(stagingDB) -> pd.DataFrame:
    """
    Apply Business Rule 2 (BR-21): Two non-cancelled flights of the same aircraft cannot overlap.
    Fix: Remove the earlier overlapping flight and log it.
    """

    deletedIndexes = []
    prev_row = None
    
    for row in stagingDB[~stagingDB["cancelled"]].itertuples():
        if prev_row and prev_row.aircraftregistration == row.aircraftregistration:
            if row.actualdeparture < prev_row.actualarrival:
                logging.info(f"BR2 violation - Overlapping flights for {row.aircraftregistration}: "
                           f"Flight at {prev_row.actualdeparture} removed. "
                           f"Overlaps with flight at {row.actualdeparture}")
                deletedIndexes.append(prev_row.Index)
        
        prev_row = row
    
    return stagingDB.drop(deletedIndexes).drop(columns=["actualdeparture", "actualarrival"])


# ==============================================================================
# PHASE 3: Aggregation Functions
# ==============================================================================

def aggregate_by_aircraft_and_time(stagingDB: pd.DataFrame, time_granularity: Literal['date', 'month']) -> pd.DataFrame:
    """Aggregate flight data by day or month."""
    return stagingDB.groupby(["aircraftregistration", time_granularity]).agg(
        FH=("FH", "sum"),
        TO=("is_takeoff", "sum"),
        CN=("cancelled", "sum"),
        DY=("is_delayed", "sum"),
        TDM=("TDM", "sum")
    ).reset_index()


# ==============================================================================
# PHASE 4.1: Dimension Tables Preparation
# ==============================================================================

def prepare_dim_aircraft(aircraft_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare aircraft dimension table."""
    return aircraft_df[["aircraft_reg_code", "aircraft_model", "aircraft_manufacturer"]].rename(columns={
        "aircraft_reg_code": "aircraft_id",
        "aircraft_model": "model",
        "aircraft_manufacturer": "manufacturer"
    })


def prepare_dim_reporteur(logbookDB: pd.DataFrame, personnel_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare reporteur dimension table."""
    reporteurs = logbookDB[["reporteurid", "reporteurclass"]].drop_duplicates().rename(columns={
        "reporteurid": "reporteur_id",
        "reporteurclass": "type"
    })
    
    return pd.merge(
        reporteurs,
        personnel_df[['reporteurid', 'airport']],
        left_on="reporteur_id",
        right_on='reporteurid',
        how="left"
    ).rename(columns={'airport': "airport_code"}).drop(columns=['reporteurid'])


def prepare_dim_date(dates_list: list) -> pd.DataFrame:
    """Prepare date dimension table."""
    dim_date = pd.DataFrame({"date_code": pd.Series(dates_list).unique()})
    dim_date["date_code"] = pd.to_datetime(dim_date["date_code"])
    dim_date["month_code"] = dim_date["date_code"].apply(build_monthCode)
    dim_date["date_code"] = dim_date["date_code"].apply(build_dateCode)
    return dim_date


def prepare_dim_month(dates_list: list) -> pd.DataFrame:
    """Prepare month dimension table."""
    dates_series = pd.Series(pd.to_datetime(pd.Series(dates_list).unique()))
    
    return pd.DataFrame({
        "month_code": dates_series.apply(build_monthCode),
        "year": dates_series.dt.year
    }).drop_duplicates(subset=["month_code"])


# ==============================================================================
# PHASE 4.2: Fact Tables Preparation
# ==============================================================================

def prepare_fact_flight_daily(aggregatedDB: pd.DataFrame) -> pd.DataFrame:
    """Prepare daily flight fact table."""
    return aggregatedDB.rename(columns={
        "aircraftregistration": "aircraft_id",
        "date": "date_code",
        "FH": "flight_hours",
        "TO": "flight_cycles"
    })[["aircraft_id", "date_code", "flight_hours", "flight_cycles"]]


def prepare_fact_flight_monthly(aggregatedDB: pd.DataFrame, manteinance_source: SQLSource) -> pd.DataFrame:
    """Prepare monthly flight fact table with maintenance data merged."""
    maintenanceDB = stage_maintenance(manteinance_source)
    
    # Merge ADOSS and ADOSU from maintenanceDB using OUTER join to include maintenance-only months
    fact_monthly = pd.merge(
        aggregatedDB,
        maintenanceDB[["aircraftregistration", "month", "ADOSS", "ADOSU"]],
        on=["aircraftregistration", "month"],
        how="outer"  # use "outer" to include months with maintenance but no flights
    ).fillna(0).rename(columns={  # Fill NaN values with 0
        "aircraftregistration": "aircraft_id",
        "month": "month_code",
        "FH": "flight_hours",
        "TO": "flight_cycles",
        "ADOSS": "adoss",
        "ADOSU": "adosu",
        "DY": "delays",
        "CN": "cancellations",
        "TDM": "total_delay_minutes"
    })
        
    return fact_monthly[[
        "aircraft_id", "month_code", "flight_hours", "flight_cycles",
        "adoss", "adosu", "delays", "cancellations", "total_delay_minutes"
    ]]


def prepare_fact_logbook(logbookDB: pd.DataFrame) -> pd.DataFrame:
    """Prepare logbook fact table."""
    return logbookDB.rename(columns={
        "aircraftregistration": "aircraft_id",
        "month": "month_code",
        "reporteurid": "reporteur_id"
    })[["aircraft_id", "month_code", "reporteur_id", "logbook_entries"]]


# ==============================================================================
# PHASE 5: Main Pipeline Functions
# ==============================================================================

def transform_flights(flights_source: SQLSource):
    """
    Complete transformation pipeline for flight data.
    Returns: (fact_daily, fact_monthly_partial, dim_date, dim_month, limit_dates)
    
    The limit dates are the min_date and max_date of the date range of the flight data,
    used for filtering logbook reports to ensure referential integrity.
    """
    df = stage_flights(flights_source)
    df = enhance_BR2(df)
    
    # Extract date range from flight data (based on scheduleddeparture)
    # Convert date codes back to datetime for comparison
    date_series = pd.to_datetime(df["date"])
    limit_dates = (date_series.min().date(), date_series.max().date())
    
    fact_daily = prepare_fact_flight_daily(aggregate_by_aircraft_and_time(df, "date"))
    monthly_agg = aggregate_by_aircraft_and_time(df, "month")
    dim_date = prepare_dim_date(df["date"].dropna().tolist())
    dim_month = prepare_dim_month(df["month"].dropna().tolist())
    return fact_daily, monthly_agg, dim_date, dim_month, limit_dates


def transform_logbook(logbook_source: SQLSource, personnel_csv_data: CSVSource, valid_aircraft_codes: set[str], dates: tuple):
    """
    Complete transformation pipeline for logbook data.
    Returns: (fact_logbook, dim_reporteur)
    
    Parameters:
    - logbook_source: SQLSource with post-flight reports
    - personnel_csv_data: CSVSource with maintenance personnel data
    - valid_aircraft_codes: Set of valid aircraft registration codes
    - dates: Minimum and maximum date (datetime) from flight data
    """
    personnel_df = pd.DataFrame(list(personnel_csv_data))
    personnel_df['reporteurid'] = personnel_df['reporteurid'].astype(int)
    
    # Pass valid aircraft codes and date range for BR3 validation and temporal filtering
    logbook_df = stage_post_flight_reports(logbook_source, valid_aircraft_codes, dates)
    
    fact_logbook = prepare_fact_logbook(logbook_df)
    dim_reporteur = prepare_dim_reporteur(logbook_df, personnel_df)
    return fact_logbook, dim_reporteur
