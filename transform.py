from typing import Generator, Literal
from tqdm import tqdm
import logging
import pandas as pd
from pygrametl.datasources import CSVSource , SQLSource, UnionSource

# Configure logging
LOG_FILE = "cleaning.log"
logging.basicConfig(
    filename=LOG_FILE,  # Log file name
    level=logging.INFO,  # Logging level
    format="%(message)s",  # Log message format
)


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
# PHASE 1: Data Pre-Processing and Staging
# ==============================================================================

def stage_data(data_source: UnionSource | Generator) -> pd.DataFrame:
    """Stage data into a DataFrame"""
    return pd.DataFrame(list(data_source))


def pre_process_flights(flights_source: SQLSource):
    """
    Generator that lazily processes flight data row by row without storing in memory.
    Applies BR1, computes date, month, FH (Flight Hours), and TDM (Total Delay Minutes) on-the-fly.
    Yields one row at a time - memory efficient for large datasets.
    """
    for row in tqdm(flights_source, desc="Pre-processing flight data"):
        # Business Rule 1 (BR-23): actualArrival must be posterior to actualDeparture
        if not row["cancelled"] and row["actualdeparture"] > row["actualarrival"]:
            # Swap their values
            row["actualdeparture"], row["actualarrival"] = row["actualarrival"], row["actualdeparture"]
        
        # Date/Time transformations
        reference_date = row["actualdeparture"] if not row["cancelled"] else row["scheduleddeparture"]
        row["date"] = build_dateCode(reference_date)
        row["month"] = build_monthCode(reference_date)
        
        # Metric calculations
        if not row["cancelled"]:
            row["FH"] = (row["actualarrival"] - row["actualdeparture"]).total_seconds() / 3600
        else:
            row["FH"] = 0
        
        if pd.notna(row.get("delaycode")):
            row["TDM"] = (row["actualdeparture"] - row["scheduleddeparture"]).total_seconds() / 60
        else:
            row["TDM"] = 0
        
        yield row


def stage_flights(flights_source: SQLSource) -> pd.DataFrame:
    """
    Stage and prepare flight data with sorting.
    Uses lazy generator for preprocessing (date, month, FH, TDM) to avoid intermediate memory storage.
    Materializes data into memory only when sorting is required.
    """
    flights_df = stage_data(pre_process_flights(flights_source))
    return flights_df.sort_values(by=["aircraftregistration", "actualdeparture"])


def pre_process_maintenance(maintenance_source: SQLSource):
    """
    Generator that lazily processes maintenance data row by row without storing in memory.
    Computes month, total_delay_duration, ADOSS, and ADOSU on-the-fly for each row.
    Yields one row at a time - memory efficient for large datasets.
    """
    for row in tqdm(maintenance_source, desc="Pre-processing maintenance data"):
        # Date/Time transformation
        row["month"] = build_monthCode(row["scheduleddeparture"])
        
        # Calculate total delay duration
        row["total_delay_duration"] = row["scheduledarrival"] - row["scheduleddeparture"]
        
        # Metric calculations - ADOSS and ADOSU
        if row["programmed"]:
            row["ADOSS"] = round((row["total_delay_duration"].days + row["total_delay_duration"].seconds / 86400), 2)
            row["ADOSU"] = 0.0
        else:
            row["ADOSS"] = 0.0
            row["ADOSU"] = round((row["total_delay_duration"].days + row["total_delay_duration"].seconds / 86400), 2)
        
        yield row


def stage_maintenance(maintenance_source: SQLSource) -> pd.DataFrame:
    """
    Stage and prepare maintenance data with aggregation and sorting.
    Uses lazy generator for preprocessing to avoid intermediate memory storage.
    Materializes to DataFrame only when aggregation is required.
    """
    # Lazy processing through generator - no intermediate storage
    maintenance_df = stage_data(pre_process_maintenance(maintenance_source))
    
    # Group by aircraft and month, summing ADOSS and ADOSU
    aggregated = maintenance_df.groupby(
        ["aircraftregistration", "month"]
    ).agg(
        ADOSS=("ADOSS", "sum"),
        ADOSU=("ADOSU", "sum")
    ).reset_index()
    
    return aggregated.sort_values(by=["aircraftregistration", "month"])


def pre_process_post_flight_reports(reports_source: SQLSource, valid_aircraft_codes: set[str]):
    """
    Generator that lazily processes post flight reports row by row without storing in memory.
    Applies BR3 (validates aircraft registration), computes month on-the-fly for each row.
    Yields one row at a time - memory efficient for large datasets.
    Invalid aircraft registrations are logged and filtered out.
    """
    for row in tqdm(reports_source, desc="Pre-processing post-flight reports"):
        # Business Rule 3: aircraft registration must be valid
        if row["aircraftregistration"] not in valid_aircraft_codes:
            logging.info(f"BR3 violation - Invalid aircraft registration in logbook: "
                       f"Aircraft {row['aircraftregistration']}, Reporteur {row['reporteurid']}")
            continue  # Skip this row
        
        # Date/Time transformation
        row["month"] = build_monthCode(row["reportingdate"])
        yield row


def stage_post_flight_reports(reports_source: SQLSource, valid_aircraft_codes: set[str]) -> pd.DataFrame:
    """
    Stage and prepare post flight reports with aggregation and sorting.
    Uses lazy generator for preprocessing (applies BR3, computes month) to avoid intermediate memory storage.
    Materializes to DataFrame only when aggregation is required.
    """
    # Lazy processing through generator - no intermediate storage
    reports_df = stage_data(pre_process_post_flight_reports(reports_source, valid_aircraft_codes))
    
    # Group by aircraft, reporteur, and month to count entries
    aggregated = reports_df.groupby(
        ["aircraftregistration", "reporteurid", "reporteurclass", "month"]
    ).size().reset_index(name="logbook_entries")
    
    return aggregated.sort_values(by=["aircraftregistration", "reporteurid", "month"])


# ==============================================================================
# PHASE 2: Business Rules (Data Quality)
# ==============================================================================

def enhance_BR2(stagingDB) -> pd.DataFrame:
    """
    Apply Business Rule 2 (BR-21): 
    Two non-cancelled flights of the same aircraft cannot overlap.
    Fix: Ignore the first flight, but record the row in a log file
    Assumes rows are ordered by aircraftregistration, actualdeparture
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
    
    return stagingDB.drop(deletedIndexes)


# ==============================================================================
# PHASE 5: Aggregation Functions
# ==============================================================================

def aggregate_by_time(stagingDB: pd.DataFrame, granularity: Literal['date', 'month']) -> pd.DataFrame:
    """
    Aggregate flight data by day or month.
    Returns: aircraftregistration, date/month, FH, TO (TakeOffs), CN (Cancellations), DY (Delays), TDM
    """
    return stagingDB.groupby(["aircraftregistration", granularity]).agg(
        FH=("FH", "sum"),
        TO=("cancelled", lambda x: sum(~x)),
        CN=("cancelled", "sum"),
        DY=("delaycode", lambda x: sum(~x.isna())),
        TDM=("TDM", "sum")
    ).reset_index()


# ==============================================================================
# PHASE 6: Maintenance Data Transformations
# ==============================================================================

def merge_ADOS(flightDB: pd.DataFrame, maintenanceDB: pd.DataFrame) -> pd.DataFrame:
    """
    Merge ADOSS and ADOSU from maintenanceDB into flight aggregated data.
    Fill missing values with 0 (aircraft with no maintenance in that month)
    """
    return pd.merge(
        flightDB,
        maintenanceDB[["aircraftregistration", "month", "ADOSS", "ADOSU"]],
        on=["aircraftregistration", "month"],
        how="left"
    ).fillna({"ADOSS": 0, "ADOSU": 0})


# ==============================================================================
# PHASE 7: Data Enrichment (Lookups)
# ==============================================================================

def enrich_aircraft_manufacturer(dataDB: pd.DataFrame, aircraft_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich data with aircraft manufacturer information.
    Adds: model, manufacturer columns
    """
    
    return pd.merge(
        dataDB,
        aircraft_df[["aircraft_reg_code", "aircraft_model", "aircraft_manufacturer"]],
        left_on="aircraftregistration",
        right_on="aircraft_reg_code",
        how="left"
    ).rename(columns={
        "aircraft_model": "model",
        "aircraft_manufacturer": "manufacturer"
    }).drop(columns=["aircraft_reg_code"])


def enrich_maintenance_personnel(logbookDB: pd.DataFrame, personnel_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich logbook data with maintenance personnel airport information.
    If reporteurid is in personnel CSV -> it's maintenance (MAREP), add airport
    If reporteurid is NOT in CSV -> it's a pilot (PIREP), no airport
    """
    
    personnel_df['reporteurid'] = personnel_df['reporteurid'].astype(int)
    
    enrichedDB = pd.merge(
        logbookDB,
        personnel_df[['reporteurid', 'airport']],
        left_on="reporteurid",
        right_on='reporteurid',
        how="left"
    ).rename(columns={'airport': "airport_code"})
        
    return enrichedDB


# ==============================================================================
# PHASE 8: Dimension Table Preparation
# ==============================================================================

def prepare_dim_aircraft(aircraft_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare aircraft dimension table.
    Returns: aircraft_id, model, manufacturer
    """
    
    return aircraft_df[["aircraft_reg_code", "aircraft_model", "aircraft_manufacturer"]].rename(columns={
        "aircraft_reg_code": "aircraft_id",
        "aircraft_model": "model",
        "aircraft_manufacturer": "manufacturer"
    })


def prepare_dim_reporteur(logbookDB: pd.DataFrame, personnel_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare reporteur dimension table.
    Returns: reporteur_id, type (PIREP/MAREP), airport_code
    """
    
    reporteurs = logbookDB[["reporteurid", "reporteurclass"]].drop_duplicates().rename(columns={
        "reporteurid": "reporteur_id",
        "reporteurclass": "type"
    })
        
    dim_reporteur = pd.merge(
        reporteurs,
        personnel_df[['reporteurid', 'airport']],
        left_on="reporteur_id",
        right_on='reporteurid',
        how="left"
    ).rename(columns={'airport': "airport_code"}).drop(columns=['reporteurid'])
    
    return dim_reporteur


def prepare_dim_date(dates_list: list) -> pd.DataFrame:
    """
    Prepare date dimension table.
    Returns: date_code, month_code
    """
    dim_date = pd.DataFrame({"date_code": pd.Series(dates_list).unique()})
    dim_date["date_code"] = pd.to_datetime(dim_date["date_code"])
    dim_date["month_code"] = dim_date["date_code"].apply(build_monthCode)
    dim_date["date_code"] = dim_date["date_code"].apply(build_dateCode)
    return dim_date


def prepare_dim_month(dates_list: list) -> pd.DataFrame:
    """
    Prepare month dimension table.
    Returns: month_code, year
    """
    dates_series = pd.Series(pd.to_datetime(pd.Series(dates_list).unique()))
    
    return pd.DataFrame({
        "month_code": dates_series.apply(build_monthCode),
        "year": dates_series.dt.year
    }).drop_duplicates(subset=["month_code"])


# ==============================================================================
# PHASE 9: Fact Table Preparation
# ==============================================================================

def prepare_fact_flight_daily(aggregatedDB: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare daily flight fact table.
    Returns: aircraft_id, date_code, flight_hours, flight_cycles
    """
    return aggregatedDB.rename(columns={
        "aircraftregistration": "aircraft_id",
        "date": "date_code",
        "FH": "flight_hours",
        "TO": "flight_cycles"
    })[["aircraft_id", "date_code", "flight_hours", "flight_cycles"]]


def prepare_fact_flight_monthly(aggregatedDB: pd.DataFrame, maintenanceDB: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare monthly flight fact table with maintenance data merged.
    Returns: aircraft_id, month_code, flight_hours, flight_cycles, 
             adoss, adosu, delays, cancellations, total_delay_minutes
    """
    fact_monthly = merge_ADOS(aggregatedDB, maintenanceDB).rename(columns={
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
    """
    Prepare logbook fact table.
    Returns: aircraft_id, month_code, reporteur_id, logbook_entries
    """
    return logbookDB.rename(columns={
        "aircraftregistration": "aircraft_id",
        "month": "month_code",
        "reporteurid": "reporteur_id"
    })[["aircraft_id", "month_code", "reporteur_id", "logbook_entries"]]


# ==============================================================================
# PHASE 10: Pipeline Functions
# ==============================================================================

def transform_flights(flights_source: SQLSource, aircraft_df: pd.DataFrame):
    """
    Complete transformation pipeline for flight data.
    BR1, date, month, FH, and TDM are computed during pre-processing (lazy evaluation).
    Returns: (fact_daily, fact_monthly_partial, dim_date, dim_month)
    """
    df = stage_flights(flights_source)
    # Note: BR1 already applied in pre_process_flights
    df = enhance_BR2(df)
    df = enrich_aircraft_manufacturer(df, aircraft_df)
    # Note: date, month, FH, TDM already computed in pre_process_flights
    
    fact_daily = prepare_fact_flight_daily(aggregate_by_time(df, "date"))
    monthly_agg = aggregate_by_time(df, "month")
    dim_date = prepare_dim_date(df["date"].dropna().tolist())
    dim_month = prepare_dim_month(df["month"].dropna().tolist())
    
    return fact_daily, monthly_agg, dim_date, dim_month


def transform_maintenance(maintenance_source: SQLSource):
    """
    Complete transformation pipeline for maintenance data.
    Month, total_delay_duration, ADOSS, and ADOSU are computed during pre-processing (lazy evaluation).
    Aggregation by aircraft+month is done in staging.
    Returns: maintenance_monthly (with ADOSS and ADOSU aggregated by aircraft+month)
    """
    # All processing and aggregation already done in stage_maintenance
    return stage_maintenance(maintenance_source)


def transform_logbook(logbook_source: SQLSource, aircraft_df: pd.DataFrame, personnel_csv_data: CSVSource):
    """
    Complete transformation pipeline for logbook data.
    BR3 and month are computed during pre-processing (lazy evaluation).
    Returns: (fact_logbook, dim_reporteur)
    """
    personnel_df = pd.DataFrame(list(personnel_csv_data))
    
    # Pass valid aircraft codes to staging for BR3 validation during pre-processing
    valid_aircraft_codes = set(aircraft_df["aircraft_reg_code"].tolist())
    logbook_df = stage_post_flight_reports(logbook_source, valid_aircraft_codes)
    # Note: BR3 already applied in pre_process_post_flight_reports
    
    logbook_df = enrich_maintenance_personnel(logbook_df, personnel_df)
    
    fact_logbook = prepare_fact_logbook(logbook_df)
    dim_reporteur = prepare_dim_reporteur(logbook_df, personnel_df)
    
    return fact_logbook, dim_reporteur


# ==============================================================================
# TESTING
# ==============================================================================

if __name__ == "__main__":
    import extract
    
    
    print("\n" + "=" * 80)
    print("TESTING TRANSFORMATION PIPELINES")
    print("=" * 80 + "\n")
    
    # Clear the log file at the start of the ETL pipeline
    clear_log_file()
    
    aircrafts_df = stage_data(extract.aircraft_manufacturer_info())

    # Test flight transformation
    print("Testing flight transformation...")
    fact_daily, monthly_agg, dim_date, dim_month = transform_flights(
        extract.flights_info(),
        aircrafts_df
    )
    print(f"Fact Daily shape: {fact_daily.shape}")
    print(fact_daily.head())
    print(f"\nMonthly Aggregation shape: {monthly_agg.shape}")
    print(monthly_agg.head())
    
    # Test maintenance transformation
    print("\n" + "-" * 80)
    print("Testing maintenance transformation...")
    maintenance_monthly = transform_maintenance(extract.maintenance_info())
    print(f"Maintenance Monthly shape: {maintenance_monthly.shape}")
    print(maintenance_monthly.head())
    
    # Merge to create final monthly fact
    print("\n" + "-" * 80)
    print("Creating final monthly fact table...")
    fact_monthly = prepare_fact_flight_monthly(monthly_agg, maintenance_monthly)
    print(f"Fact Monthly shape: {fact_monthly.shape}")
    print(fact_monthly.head())
    
    # Test logbook transformation
    print("\n" + "-" * 80)
    print("Testing logbook transformation...")
    fact_logbook, dim_reporteur = transform_logbook(
        extract.post_flight_reports(),
        aircrafts_df,
        extract.maintenance_personnel_info()
    )
    print(f"Fact Logbook shape: {fact_logbook.shape}")
    print(fact_logbook.head())
    print(f"\nDim Reporteur shape: {dim_reporteur.shape}")
    print(dim_reporteur.head())
    
    # Test dimensions
    print("\n" + "-" * 80)
    print("Testing dimensions...")
    dim_aircraft = prepare_dim_aircraft(aircrafts_df)
    print(f"Dim Aircraft shape: {dim_aircraft.shape}")
    print(dim_aircraft.head())
    
    print(f"\nDim Date shape: {dim_date.shape}")
    print(dim_date.head())
    
    print(f"\nDim Month shape: {dim_month.shape}")
    print(dim_month.head())
    
    print("\n" + "=" * 80)
    print("ALL TRANSFORMATIONS COMPLETED SUCCESSFULLY!")
    print("=" * 80)

