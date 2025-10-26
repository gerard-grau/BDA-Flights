from tqdm import tqdm
import logging
import pandas as pd

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
    for row in tqdm(stagingDB.itertuples(), desc="Applying BR1 (swap arrival/departure)"):
        if row.actualdeparture > row.actualarrival:
            # BR-23: Swap the values
            stagingDB.at[row.Index, "actualdeparture"] = row.actualarrival
            stagingDB.at[row.Index, "actualarrival"] = row.actualdeparture
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
    for row in tqdm(stagingDB.itertuples(), desc="Enhancing BSR2"):

        if not row.cancelled:

            # TODO: decidir com fer el log
            if prev_row and row.actualdeparture < prev_row.actualarrival:
                logging.info(f"BR2 violation - Overlapping flights for {row.aircraftregistration}: "
                    f"Flight at {prev_row.actualdeparture} removed. "
                    f"Overlaps with flight at {row.actualdeparture}")

                deletedIndexes.append(prev_row.Index)

            prev_row = row

    stagingDB.drop(deletedIndexes, inplace=True)
    return stagingDB


def enhance_BR3(logbookDB, valid_aircraft_set) -> pd.DataFrame:
    """
    Apply Business Rule 3: 
    The aircraft registration in a post flight report must be an aircraft.
    Fix: Ignore the report, but record the row in a log file
    """
    # invalidIndexes = []
    
    # for row in tqdm(logbookDB.itertuples(), desc="Applying BR3 (valid aircraft in reports)"):
    #     if row.aircraftregistration not in valid_aircraft_set:
    #         # BR-3: Invalid aircraft registration
    #         logging.info(f"BR3 violation - Invalid aircraft registration in logbook: "
    #                    f"Aircraft {row.aircraftregistration}, Reporter {row.reporteurid}")
    #         invalidIndexes.append(row.Index)
    
    # logbookDB.drop(invalidIndexes, inplace=True)
    return logbookDB


# ==============================================================================
# PHASE 3: Date/Time Transformations
# ==============================================================================

def split_date(stagingDB) -> pd.DataFrame:
    """Add date column in format YYYY-MM-DD"""
    for row in tqdm(stagingDB.itertuples(), desc="Extracting date"):
        if not row.cancelled:
            stagingDB.at[row.Index, "date"] = build_dateCode(row.actualdeparture)
        else:
            stagingDB.at[row.Index, "date"] = build_dateCode(row.scheduleddeparture)
    return stagingDB


def split_month(stagingDB) -> pd.DataFrame:
    """Add month column in format YYYY-MM"""

    # TODO: això extreu NO de date sinó de actualarrival, etc. ja que fem servir implementació
    # que hi ha ja en el codi: build_monthCode. Fa falta?

    for row in tqdm(stagingDB.itertuples(), desc="Extracting month"):
        if not row.cancelled:
            stagingDB.at[row.Index, "month"] = build_monthCode(row.actualdeparture)
        else:
            stagingDB.at[row.Index, "month"] = build_monthCode(row.scheduleddeparture)
    return stagingDB


# ==============================================================================
# PHASE 4: Metric Calculations
# ==============================================================================

def compute_FH(stagingDB) -> pd.DataFrame:
    """
    Calculate Flight Hours (FH) in HOURS.
    FH = actualArrival - actualDeparture (for non-cancelled flights)
    """
    for row in tqdm(stagingDB.itertuples(), desc="Computing FH (Flight Hours)"):
        if not row.cancelled:
            stagingDB.at[row.Index, "FH"] = (
                row.actualarrival - row.actualdeparture
            ).total_seconds() / 3600  # Convert to hours
        else:
            stagingDB.at[row.Index, "FH"] = 0
    return stagingDB


def compute_TDM(stagingDB) -> pd.DataFrame:
    """
    Calculate Total Delay Minutes (TDM) in MINUTES.
    TDM = actualDeparture - scheduledDeparture (for delayed flights)
    """
    for row in tqdm(stagingDB.itertuples(), desc="Computing TDM (Total Delay Minutes)"):
        if row.delaycode is not None: # ! Revisar el is not None
            stagingDB.at[row.Index, "TDM"] = (
                row.actualdeparture - row.scheduleddeparture
            ).total_seconds() / 60  # Convert to minutes
        else:
            stagingDB.at[row.Index, "TDM"] = 0
    return stagingDB


# ==============================================================================
# PHASE 5: Aggregation Functions
# ==============================================================================

def aggregate_by_time(stagingDB, granularity) -> pd.DataFrame:
    """
    Aggregate flight data by day or month.
    Returns: aircraftregistration, date/month, FH, TO (TakeOffs), CN (Cancellations), DY (Delays), TDM
    """
    assert granularity in ["date", "month"], "Granularity must be 'date' or 'month'"
    
    print(f"Aggregating by {granularity}")
    aggregatedDB = (
        stagingDB.groupby(["aircraftregistration", granularity])
        .agg(
            FH=pd.NamedAgg(column="FH", aggfunc="sum"),
            TO=pd.NamedAgg(column="cancelled", aggfunc=lambda x: len(x) - sum(x)),  # Count non-cancelled
            CN=pd.NamedAgg(column="cancelled", aggfunc="sum"),  # Count cancelled
            DY=pd.NamedAgg(column="delaycode", aggfunc=lambda x: sum(~x.isna())),  # Count delayed
            TDM=pd.NamedAgg(column="TDM", aggfunc="sum"),  # Sum of delay minutes
        )
        .reset_index()
    )
    
    return aggregatedDB


# ==============================================================================
# PHASE 6: Maintenance Data Transformations
# ==============================================================================

def compute_ADOSS(maintenanceDB) -> pd.DataFrame:
    """
    Compute ADOSS (Aircraft Days Out of Service - Scheduled) from maintenanceDB.
    ADOSS = scheduled maintenance duration in days
    """
    maintenanceDB["ADOSS"] = 0.0
    for row in tqdm(maintenanceDB.itertuples(), desc="Computing ADOSS"):
        if row.programmed:
            # Convert interval to days
            maintenanceDB.at[row.Index, "ADOSS"] = round(
                (row.total_delay_duration.days + row.total_delay_duration.seconds / 86400), 2
            )
    return maintenanceDB


def compute_ADOSU(maintenanceDB) -> pd.DataFrame:
    """
    Compute ADOSU (Aircraft Days Out of Service - Unscheduled) from maintenanceDB.
    ADOSU = unscheduled maintenance duration in days
    """
    maintenanceDB["ADOSU"] = 0.0
    for row in tqdm(maintenanceDB.itertuples(), desc="Computing ADOSU"):
        if not row.programmed:
            # Convert interval to days
            maintenanceDB.at[row.Index, "ADOSU"] = round(
                (row.total_delay_duration.days + row.total_delay_duration.seconds / 86400), 2
            )
    return maintenanceDB


def merge_ADOS(flightDB, maintenanceDB) -> pd.DataFrame:
    """
    Merge ADOSS and ADOSU from maintenanceDB into flight aggregated data.
    Fill missing values with 0 (aircraft with no maintenance in that month)
    """
    mergedDB = pd.merge(
        flightDB,
        maintenanceDB[["aircraftregistration", "month", "ADOSS", "ADOSU"]],
        on=["aircraftregistration", "month"],
        how="left",
    )
    
    # Fill NaN values with 0 for aircraft without maintenance
    mergedDB["ADOSS"] = mergedDB["ADOSS"].fillna(0)
    mergedDB["ADOSU"] = mergedDB["ADOSU"].fillna(0)
    
    return mergedDB


# ==============================================================================
# PHASE 7: Data Enrichment (Lookups)
# ==============================================================================

def enrich_aircraft_manufacturer(dataDB, aircraft_csv_data) -> pd.DataFrame:
    """
    Enrich data with aircraft manufacturer information.
    Adds: model, manufacturer columns
    """
    # Convert CSV data to DataFrame if needed
    if not isinstance(aircraft_csv_data, pd.DataFrame):
        aircraft_df = pd.DataFrame(list(aircraft_csv_data))
    else:
        aircraft_df = aircraft_csv_data
    
    # Merge on aircraft registration
    enrichedDB = pd.merge(
        dataDB,
        aircraft_df[["aircraft_reg_code", "aircraft_model", "aircraft_manufacturer"]],
        left_on="aircraftregistration",
        right_on="aircraft_reg_code",
        how="left"
    )
    
    # Rename columns for consistency
    enrichedDB = enrichedDB.rename(columns={
        "aircraft_model": "model",
        "aircraft_manufacturer": "manufacturer"
    })
    
    # Drop duplicate column
    enrichedDB = enrichedDB.drop(columns=["aircraft_reg_code"], errors='ignore')
    
    return enrichedDB


def enrich_maintenance_personnel(logbookDB, personnel_csv_data) -> pd.DataFrame:
    """
    Enrich logbook data with maintenance personnel airport information.
    If reporteurid is in personnel CSV -> it's maintenance (MAREP), add airport
    If reporteurid is NOT in CSV -> it's a pilot (PIREP), no airport
    """
    # Convert CSV data to DataFrame if needed
    if not isinstance(personnel_csv_data, pd.DataFrame):
        personnel_df = pd.DataFrame(list(personnel_csv_data))
    else:
        personnel_df = personnel_csv_data
    
    # Check if reporteurid column exists, if not return the original DataFrame
    if 'reporteurid' not in logbookDB.columns:
        print(f"Warning: 'reporteurid' column not found in logbook data. Available columns: {logbookDB.columns.tolist()}")
        # Add empty airport_code column
        logbookDB['airport_code'] = ""
        return logbookDB
    
    # If personnel_df is empty or has no columns, just add empty airport_code
    if len(personnel_df.columns) == 0:
        logbookDB['airport_code'] = ""
        return logbookDB
    
    # Assume first column is reporteurid and second is airport
    personnel_cols = personnel_df.columns.tolist()
    id_col = personnel_cols[0]
    airport_col = personnel_cols[1] if len(personnel_cols) > 1 else None
    
    if not airport_col:
        logbookDB['airport_code'] = ""
        return logbookDB
    
    # Ensure types match for merging
    logbookDB['reporteurid'] = logbookDB['reporteurid'].astype(str)
    personnel_df[id_col] = personnel_df[id_col].astype(str)
    
    # Merge with personnel data (left join - keeps all logbook entries)
    enrichedDB = pd.merge(
        logbookDB,
        personnel_df[[id_col, airport_col]],
        left_on="reporteurid",
        right_on=id_col,
        how="left"
    )
    
    # Rename airport column
    enrichedDB = enrichedDB.rename(columns={airport_col: "airport_code"})
    
    # Drop the duplicate id column ONLY if it's different from 'reporteurid'
    if id_col != 'reporteurid':
        enrichedDB = enrichedDB.drop(columns=[id_col], errors='ignore')
    
    # Fill NaN with empty string for pilots (not in personnel CSV)
    enrichedDB["airport_code"] = enrichedDB["airport_code"].fillna("")
    
    return enrichedDB


# ==============================================================================
# PHASE 8: Dimension Table Preparation
# ==============================================================================

def prepare_dim_aircraft(aircraft_csv_data) -> pd.DataFrame:
    """
    Prepare aircraft dimension table.
    Returns: aircraft_id, model, manufacturer
    """
    if not isinstance(aircraft_csv_data, pd.DataFrame):
        aircraft_df = pd.DataFrame(list(aircraft_csv_data))
    else:
        aircraft_df = aircraft_csv_data
    
    dim_aircraft = aircraft_df[["aircraft_reg_code", "aircraft_model", "aircraft_manufacturer"]].copy()
    dim_aircraft = dim_aircraft.rename(columns={
        "aircraft_reg_code": "aircraft_id",
        "aircraft_model": "model",
        "aircraft_manufacturer": "manufacturer"
    })
    
    # Remove duplicates
    dim_aircraft = dim_aircraft.drop_duplicates(subset=["aircraft_id"])
    
    return dim_aircraft


def prepare_dim_reporter(logbookDB, personnel_csv_data) -> pd.DataFrame:
    """
    Prepare reporter dimension table.
    Returns: reporter_id, type (PIREP/MAREP), airport_code
    """
    if not isinstance(personnel_csv_data, pd.DataFrame):
        personnel_df = pd.DataFrame(list(personnel_csv_data))
    else:
        personnel_df = personnel_csv_data
    
    print(f"Personnel CSV columns: {personnel_df.columns.tolist()}")
    
    # Get unique reporters from logbook
    reporters = logbookDB[["reporteurid", "reporteurclass"]].drop_duplicates()
    reporters = reporters.rename(columns={
        "reporteurid": "reporter_id",
        "reporteurclass": "type"
    })
    
    # Ensure types match for merge
    reporters['reporter_id'] = reporters['reporter_id'].astype(str)
    
    # Check what columns are available in personnel_df and merge accordingly
    if len(personnel_df.columns) > 0:
        # Assume first column is reporteurid and second is airport
        personnel_cols = personnel_df.columns.tolist()
        id_col = personnel_cols[0]
        airport_col = personnel_cols[1] if len(personnel_cols) > 1 else None
        
        if airport_col:
            personnel_df[id_col] = personnel_df[id_col].astype(str)
            
            # Merge with personnel to get airport (left join)
            dim_reporter = pd.merge(
                reporters,
                personnel_df[[id_col, airport_col]],
                left_on="reporter_id",
                right_on=id_col,
                how="left"
            )
            
            dim_reporter = dim_reporter.rename(columns={airport_col: "airport_code"})
            dim_reporter = dim_reporter.drop(columns=[id_col], errors='ignore')
        else:
            dim_reporter = reporters
            dim_reporter["airport_code"] = ""
    else:
        dim_reporter = reporters
        dim_reporter["airport_code"] = ""
    
    # Fill NaN airport_code with empty string for pilots (PIREP)
    dim_reporter["airport_code"] = dim_reporter["airport_code"].fillna("")
    
    return dim_reporter


def prepare_dim_date(dates_list) -> pd.DataFrame:
    """
    Prepare date dimension table.
    Returns: date_code, month_code
    """
    unique_dates = pd.Series(dates_list).unique()
    
    dim_date = pd.DataFrame({
        "date_code": unique_dates
    })
    
    # Extract month_code from date using helper functions
    dim_date["date_code"] = pd.to_datetime(dim_date["date_code"])
    dim_date["month_code"] = dim_date["date_code"].apply(build_monthCode)
    dim_date["date_code"] = dim_date["date_code"].apply(build_dateCode)
    
    return dim_date


def prepare_dim_month(dates_list) -> pd.DataFrame:
    """
    Prepare month dimension table.
    Returns: month_code, year
    """
    # Convert to datetime - pd.to_datetime returns DatetimeIndex, convert to Series
    dates_series = pd.Series(pd.to_datetime(pd.Series(dates_list).unique()))
    
    dim_month = pd.DataFrame({
        "month_code": dates_series.apply(build_monthCode),
        "year": dates_series.dt.year
    })
    
    # Remove duplicates
    dim_month = dim_month.drop_duplicates(subset=["month_code"])
    
    return dim_month


# ==============================================================================
# PHASE 9: Fact Table Preparation
# ==============================================================================

def prepare_fact_flight_daily(aggregatedDB) -> pd.DataFrame:
    """
    Prepare daily flight fact table.
    Returns: aircraft_id, date_code, flight_hours, flight_cycles
    """
    fact_daily = aggregatedDB.rename(columns={
        "aircraftregistration": "aircraft_id",
        "date": "date_code",
        "FH": "flight_hours",
        "TO": "flight_cycles"
    })
    
    # Select only needed columns
    fact_daily = fact_daily[["aircraft_id", "date_code", "flight_hours", "flight_cycles"]]
    
    return fact_daily


def prepare_fact_flight_monthly(aggregatedDB, maintenanceDB) -> pd.DataFrame:
    """
    Prepare monthly flight fact table with maintenance data merged.
    Returns: aircraft_id, month_code, flight_hours, flight_cycles, 
             adis, adoss, adosu, delays, cancellations, total_delay_minutes
    """
    # Merge with maintenance data
    fact_monthly = merge_ADOS(aggregatedDB, maintenanceDB)
    
    # Rename columns
    fact_monthly = fact_monthly.rename(columns={
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
    
    # Calculate ADIS (Aircraft Days In Service) - placeholder, will be calculated in DW
    # For now, we don't calculate it here as it requires knowing the period length
    fact_monthly["adis"] = 0  # Will be calculated in queries
    
    # Select only needed columns
    fact_monthly = fact_monthly[[
        "aircraft_id", "month_code", "flight_hours", "flight_cycles",
        "adis", "adoss", "adosu", "delays", "cancellations", "total_delay_minutes"
    ]]
    
    return fact_monthly


def prepare_fact_logbook(logbookDB) -> pd.DataFrame:
    """
    Prepare logbook fact table.
    Returns: aircraft_id, month_code, reporter_id, logbook_entries
    """
    print(f"Logbook columns before rename: {logbookDB.columns.tolist()}")
    
    # Check which columns exist and rename accordingly
    rename_dict = {}
    if 'aircraftregistration' in logbookDB.columns:
        rename_dict['aircraftregistration'] = 'aircraft_id'
    if 'month' in logbookDB.columns:
        rename_dict['month'] = 'month_code'
    if 'reporteurid' in logbookDB.columns:
        rename_dict['reporteurid'] = 'reporter_id'
    
    fact_logbook = logbookDB.rename(columns=rename_dict)
    
    print(f"Logbook columns after rename: {fact_logbook.columns.tolist()}")
    
    # Select only needed columns (only those that exist)
    available_cols = []
    for col in ["aircraft_id", "month_code", "reporter_id", "logbook_entries"]:
        if col in fact_logbook.columns:
            available_cols.append(col)
    
    fact_logbook = fact_logbook[available_cols]
    
    return fact_logbook


# ==============================================================================
# PHASE 10: Pipeline Functions
# ==============================================================================

def transform_flights(flights_source, aircraft_csv_data):
    """
    Complete transformation pipeline for flight data.
    Returns: (fact_daily, fact_monthly_partial, dim_date, dim_month)
    """
    print("=" * 80)
    print("TRANSFORMING FLIGHT DATA")
    print("=" * 80)
    
    # Stage data
    df = stage_data(flights_source)
    
    # Apply business rules
    df = enhance_BR1(df)
    df = enhance_BR2(df)
    
    # Enrich with manufacturer info
    df = enrich_aircraft_manufacturer(df, aircraft_csv_data)
    
    # Extract date/month
    df = split_date(df)
    df = split_month(df)
    
    # Compute metrics
    df = compute_FH(df)
    df = compute_TDM(df)
    
    # Aggregate by day
    daily_agg = aggregate_by_time(df, "date")
    fact_daily = prepare_fact_flight_daily(daily_agg)
    
    # Aggregate by month
    monthly_agg = aggregate_by_time(df, "month")
    
    # Prepare dimensions
    dim_date = prepare_dim_date(df["date"].dropna().tolist())
    dim_month = prepare_dim_month(df["month"].dropna().tolist())
    
    print("Flight transformation complete!")
    return fact_daily, monthly_agg, dim_date, dim_month


def transform_maintenance(maintenance_source):
    """
    Complete transformation pipeline for maintenance data.
    Returns: maintenance_monthly (with ADOSS and ADOSU)
    """
    print("=" * 80)
    print("TRANSFORMING MAINTENANCE DATA")
    print("=" * 80)
    
    # Stage data
    df = stage_data(maintenance_source)
    
    # Compute ADOSS and ADOSU
    df = compute_ADOSS(df)
    df = compute_ADOSU(df)
    
    print("Maintenance transformation complete!")
    return df


def transform_logbook(logbook_source, aircraft_csv_data, personnel_csv_data):
    """
    Complete transformation pipeline for logbook data.
    Returns: (fact_logbook, dim_reporter)
    """
    print("=" * 80)
    print("TRANSFORMING LOGBOOK DATA")
    print("=" * 80)
    
    # Stage personnel CSV data ONCE (CSVSource can only be iterated once!)
    if not isinstance(personnel_csv_data, pd.DataFrame):
        personnel_df = pd.DataFrame(list(personnel_csv_data))
        print(f"Personnel CSV staged - columns: {personnel_df.columns.tolist()}, shape: {personnel_df.shape}")
    else:
        personnel_df = personnel_csv_data
    
    # Stage logbook data
    df = stage_data(logbook_source)
    print(f"Staged logbook columns: {df.columns.tolist()}")
    print(f"Staged logbook shape: {df.shape}")
    if len(df) > 0:
        print(f"First row: {df.iloc[0].to_dict()}")
    
    # Get valid aircraft set for BR3
    if not isinstance(aircraft_csv_data, pd.DataFrame):
        aircraft_df = pd.DataFrame(list(aircraft_csv_data))
    else:
        aircraft_df = aircraft_csv_data
    valid_aircraft_set = set(aircraft_df["aircraft_reg_code"].tolist())
    
    # Apply BR3
    df = enhance_BR3(df, valid_aircraft_set)
    
    # Enrich with personnel airport info (use the staged DataFrame)
    df = enrich_maintenance_personnel(df, personnel_df)
    
    # Prepare fact table
    fact_logbook = prepare_fact_logbook(df)
    
    # Prepare reporter dimension (use the staged DataFrame)
    dim_reporter = prepare_dim_reporter(df, personnel_df)
    
    print("Logbook transformation complete!")
    return fact_logbook, dim_reporter


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
    fact_logbook, dim_reporter = transform_logbook(
        extract.logbook_info(),
        extract.aircraft_manufacturer_info(),
        extract.maintenance_personnel_info()
    )
    print(f"Fact Logbook shape: {fact_logbook.shape}")
    print(fact_logbook.head())
    print(f"\nDim Reporter shape: {dim_reporter.shape}")
    print(dim_reporter.head())
    
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

