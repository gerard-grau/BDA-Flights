from tqdm import tqdm
import logging
import pandas as pd
from pygrametl.datasources import CSVSource

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
# PHASE 1: Data Staging
# ==============================================================================

def stage_data(data) -> pd.DataFrame:
    """Stage data into a DataFrame"""
    rows = []
    for row in tqdm(data, desc="Staging data into pandas dataframe"):
        rows.append(row)
    return pd.DataFrame.from_dict(rows)


# ==============================================================================
# PHASE 2: Business Rules (Data Quality)
# ==============================================================================

def enhance_BR1(stagingDB) -> pd.DataFrame:
    """
    Apply Business Rule 1 (BR-23): 
    In a Flight, actualArrival is posterior to actualDeparture.
    Fix: Swap their values
    """
    mask = stagingDB["actualdeparture"] > stagingDB["actualarrival"]
    stagingDB.loc[mask, ["actualdeparture", "actualarrival"]] = stagingDB.loc[mask, ["actualarrival", "actualdeparture"]].values
    return stagingDB


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


def enhance_BR3(postflightreportsDB, valid_aircraft_codes: set[str]) -> pd.DataFrame:
    """
    Apply Business Rule 3: 
    The aircraft registration in a post flight report must be an aircraft.
    Fix: Ignore the report, but record the row in a log file
    """
    invalid_mask = ~postflightreportsDB["aircraftregistration"].isin(valid_aircraft_codes)
    
    for _, row in postflightreportsDB[invalid_mask].iterrows():
        logging.info(f"BR3 violation - Invalid aircraft registration in logbook: "
                   f"Aircraft {row['aircraftregistration']}, Reporteur {row['reporteurid']}")
    
    return postflightreportsDB[~invalid_mask]


# ==============================================================================
# PHASE 3: Date/Time Transformations
# ==============================================================================

def split_date(stagingDB) -> pd.DataFrame:
    """Add date column in format YYYY-MM-DD"""
    stagingDB["date"] = stagingDB.apply(
        lambda row: build_dateCode(row["actualdeparture"] if not row["cancelled"] else row["scheduleddeparture"]),
        axis=1
    )
    return stagingDB


def split_month(stagingDB) -> pd.DataFrame:
    """Add month column in format YYYY-MM"""
    stagingDB["month"] = stagingDB.apply(
        lambda row: build_monthCode(row["actualdeparture"] if not row["cancelled"] else row["scheduleddeparture"]),
        axis=1
    )
    return stagingDB


# ==============================================================================
# PHASE 4: Metric Calculations
# ==============================================================================

def compute_FH(stagingDB) -> pd.DataFrame:
    """
    Calculate Flight Hours (FH) in HOURS.
    FH = actualArrival - actualDeparture (for non-cancelled flights)
    """
    stagingDB["FH"] = stagingDB.apply(
        lambda row: (row["actualarrival"] - row["actualdeparture"]).total_seconds() / 3600 if not row["cancelled"] else 0,
        axis=1
    )
    return stagingDB


def compute_TDM(stagingDB) -> pd.DataFrame:
    """
    Calculate Total Delay Minutes (TDM) in MINUTES.
    TDM = actualDeparture - scheduledDeparture (for delayed flights)
    """
    stagingDB["TDM"] = stagingDB.apply(
        lambda row: (row["actualdeparture"] - row["scheduleddeparture"]).total_seconds() / 60 if pd.notna(row["delaycode"]) else 0,
        axis=1
    )
    return stagingDB


# ==============================================================================
# PHASE 5: Aggregation Functions
# ==============================================================================

def aggregate_by_time(stagingDB, granularity) -> pd.DataFrame:
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

def compute_ADOSS(maintenanceDB) -> pd.DataFrame:
    """
    Compute ADOSS (Aircraft Days Out of Service - Scheduled) from maintenanceDB.
    ADOSS = scheduled maintenance duration in days
    """
    maintenanceDB["ADOSS"] = maintenanceDB.apply(
        lambda row: round((row["total_delay_duration"].days + row["total_delay_duration"].seconds / 86400), 2) if row["programmed"] else 0.0,
        axis=1
    )
    return maintenanceDB


def compute_ADOSU(maintenanceDB) -> pd.DataFrame:
    """
    Compute ADOSU (Aircraft Days Out of Service - Unscheduled) from maintenanceDB.
    ADOSU = unscheduled maintenance duration in days
    """
    maintenanceDB["ADOSU"] = maintenanceDB.apply(
        lambda row: round((row["total_delay_duration"].days + row["total_delay_duration"].seconds / 86400), 2) if not row["programmed"] else 0.0,
        axis=1
    )
    return maintenanceDB


def merge_ADOS(flightDB, maintenanceDB) -> pd.DataFrame:
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

def enrich_aircraft_manufacturer(dataDB, aircraft_csv_data: CSVSource) -> pd.DataFrame:
    """
    Enrich data with aircraft manufacturer information.
    Adds: model, manufacturer columns
    """
    aircraft_df = pd.DataFrame(list(aircraft_csv_data)) if not isinstance(aircraft_csv_data, pd.DataFrame) else aircraft_csv_data
    
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


def enrich_maintenance_personnel(logbookDB, personnel_df: pd.DataFrame) -> pd.DataFrame:
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

def prepare_dim_aircraft(aircraft_csv_data) -> pd.DataFrame:
    """
    Prepare aircraft dimension table.
    Returns: aircraft_id, model, manufacturer
    """
    aircraft_df = pd.DataFrame(list(aircraft_csv_data))
    
    return aircraft_df[["aircraft_reg_code", "aircraft_model", "aircraft_manufacturer"]].rename(columns={
        "aircraft_reg_code": "aircraft_id",
        "aircraft_model": "model",
        "aircraft_manufacturer": "manufacturer"
    })


def prepare_dim_reporteur(logbookDB, personnel_df) -> pd.DataFrame:
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


def prepare_dim_month(dates_list) -> pd.DataFrame:
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

def prepare_fact_flight_daily(aggregatedDB) -> pd.DataFrame:
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


def prepare_fact_flight_monthly(aggregatedDB, maintenanceDB) -> pd.DataFrame:
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


def prepare_fact_logbook(logbookDB) -> pd.DataFrame:
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

def transform_flights(flights_source, aircraft_csv_data):
    """
    Complete transformation pipeline for flight data.
    Returns: (fact_daily, fact_monthly_partial, dim_date, dim_month)
    """
    df = stage_data(flights_source)
    df = enhance_BR1(df)
    df = enhance_BR2(df)
    df = enrich_aircraft_manufacturer(df, aircraft_csv_data)
    df = split_date(df)
    df = split_month(df)
    df = compute_FH(df)
    df = compute_TDM(df)
    
    fact_daily = prepare_fact_flight_daily(aggregate_by_time(df, "date"))
    monthly_agg = aggregate_by_time(df, "month")
    dim_date = prepare_dim_date(df["date"].dropna().tolist())
    dim_month = prepare_dim_month(df["month"].dropna().tolist())
    
    return fact_daily, monthly_agg, dim_date, dim_month


def transform_maintenance(maintenance_source):
    """
    Complete transformation pipeline for maintenance data.
    Returns: maintenance_monthly (with ADOSS and ADOSU aggregated by aircraft+month)
    """
    df = stage_data(maintenance_source)
    df = compute_ADOSS(df)
    df = compute_ADOSU(df)
    
    # Aggregate by aircraft and month (sum ADOSS and ADOSU)
    return df.groupby(["aircraftregistration", "month"]).agg(
        ADOSS=("ADOSS", "sum"),
        ADOSU=("ADOSU", "sum")
    ).reset_index()


def transform_logbook(logbook_source, aircraft_csv_data, personnel_csv_data):
    """
    Complete transformation pipeline for logbook data.
    Returns: (fact_logbook, dim_reporteur)
    """
    personnel_df = pd.DataFrame(list(personnel_csv_data))
    aircraft_df = pd.DataFrame(list(aircraft_csv_data))
    
    logbook_df = stage_data(logbook_source)
    logbook_df = enhance_BR3(logbook_df, set(aircraft_df["aircraft_reg_code"].tolist()))
    logbook_df = enrich_maintenance_personnel(logbook_df, personnel_df)
    
    fact_logbook = prepare_fact_logbook(logbook_df)
    dim_reporteur = prepare_dim_reporteur(logbook_df, personnel_df)
    
    return fact_logbook, dim_reporteur


def extract_all():
    ...


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

    # Test flight transformation
    print("Testing flight transformation...")
    fact_daily, monthly_agg, dim_date, dim_month = transform_flights(
        extract.flights_info(),
        extract.aircraft_manufacturer_info()
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
        extract.logbook_info(),
        extract.aircraft_manufacturer_info(),
        extract.maintenance_personnel_info()
    )
    print(f"Fact Logbook shape: {fact_logbook.shape}")
    print(fact_logbook.head())
    print(f"\nDim Reporteur shape: {dim_reporteur.shape}")
    print(dim_reporteur.head())
    
    # Test dimensions
    print("\n" + "-" * 80)
    print("Testing dimensions...")
    dim_aircraft = prepare_dim_aircraft(extract.aircraft_manufacturer_info())
    print(f"Dim Aircraft shape: {dim_aircraft.shape}")
    print(dim_aircraft.head())
    
    print(f"\nDim Date shape: {dim_date.shape}")
    print(dim_date.head())
    
    print(f"\nDim Month shape: {dim_month.shape}")
    print(dim_month.head())
    
    print("\n" + "=" * 80)
    print("ALL TRANSFORMATIONS COMPLETED SUCCESSFULLY!")
    print("=" * 80)

