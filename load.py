from tqdm import tqdm
import pandas as pd
from pygrametl.datasources import PandasSource


# ==============================================================================
# DIMENSION TABLE LOADING
# ==============================================================================

def load_dim_aircraft(dw, dim_aircraft_df: pd.DataFrame):
    """Load aircraft dimension table"""
    for row in tqdm(PandasSource(dim_aircraft_df), desc="Loading dim_aircraft", total=len(dim_aircraft_df)):
        dw.dim_aircraft.insert(row)


def load_dim_reporteur(dw, dim_reporteur_df: pd.DataFrame):
    """Load reporteur dimension table"""
    # Fill NaN airport_code with empty string
    dim_reporteur_df['airport_code'] = dim_reporteur_df['airport_code'].fillna('')
    
    for row in tqdm(PandasSource(dim_reporteur_df), desc="Loading dim_reporteur", total=len(dim_reporteur_df)):
        dw.dim_reporteur.insert(row)

def load_dim_month(dw, dim_month_df: pd.DataFrame):
    """Load month dimension table"""
    for row in tqdm(PandasSource(dim_month_df), desc="Loading dim_month", total=len(dim_month_df)):
        dw.dim_month.insert(row)


def load_dim_date(dw, dim_date_df: pd.DataFrame):
    """Load date dimension table (snowflaked with month)"""
    for row in tqdm(PandasSource(dim_date_df), desc="Loading dim_date", total=len(dim_date_df)):
        dw.dim_date.root.insert(row)

def load_date_snowflake(dw, dim_month_df: pd.DataFrame, dim_date_df: pd.DataFrame):
    load_dim_month(dw, dim_month_df)
    load_dim_date(dw, dim_date_df)


# ==============================================================================
# FACT TABLE LOADING
# ==============================================================================

def load_fact_flight_daily(dw, fact_daily_df: pd.DataFrame):
    """Load daily flight fact table"""
    for row in tqdm(PandasSource(fact_daily_df), desc="Loading fact_flight_daily", total=len(fact_daily_df)):
        dw.fact_flight_daily.insert(row)


def load_fact_flight_monthly(dw, fact_monthly_df: pd.DataFrame):
    """Load monthly flight fact table"""
    for row in tqdm(PandasSource(fact_monthly_df), desc="Loading fact_flight_monthly", total=len(fact_monthly_df)):
        dw.fact_flight_monthly.insert(row)


def load_fact_logbook(dw, fact_logbook_df: pd.DataFrame):
    """Load logbook fact table"""
    for row in tqdm(PandasSource(fact_logbook_df), desc="Loading fact_logbook", total=len(fact_logbook_df)):
        dw.fact_logbook.insert(row)


# # ==============================================================================
# # MAIN LOADING PIPELINE
# # ==============================================================================

# def load_all(dw, dim_aircraft, dim_reporteur, dim_date, dim_month, 
#              fact_daily, fact_monthly, fact_logbook):
#     """
#     Load all dimensions and facts into the data warehouse.
    
#     Parameters:
#     - dw: DW instance with connection to DuckDB
#     - dim_aircraft: DataFrame with aircraft dimension
#     - dim_reporteur: DataFrame with reporteur dimension
#     - dim_date: DataFrame with date dimension
#     - dim_month: DataFrame with month dimension
#     - fact_daily: DataFrame with daily flight facts
#     - fact_monthly: DataFrame with monthly flight facts
#     - fact_logbook: DataFrame with logbook facts
#     """
#     print("=" * 80)
#     print("LOADING DATA INTO DATA WAREHOUSE")
#     print("=" * 80)
    
#     # Load dimensions first (to satisfy foreign key constraints)
#     print("\nLoading dimension tables...")
#     load_dim_aircraft(dw, dim_aircraft)
#     load_dim_reporteur(dw, dim_reporteur)
#     load_dim_month(dw, dim_month)
#     load_dim_date(dw, dim_date)
    
#     # Load fact tables
#     print("\nLoading fact tables...")
#     load_fact_flight_daily(dw, fact_daily)
#     load_fact_flight_monthly(dw, fact_monthly)
#     load_fact_logbook(dw, fact_logbook)
    
#     print("\n" + "=" * 80)
#     print("DATA WAREHOUSE LOADING COMPLETED SUCCESSFULLY!")
#     print("=" * 80)


# # ==============================================================================
# # TESTING
# # ==============================================================================

# if __name__ == "__main__":
#     import extract
#     import transform
#     from dw import DW
    
#     print("\n" + "=" * 80)
#     print("TESTING ETL LOAD PROCESS")
#     print("=" * 80 + "\n")
    
#     # Clear log file
#     transform.clear_log_file()
    
#     aircraft_df = pd.DataFrame(list(extract.aircraft_manufacturer_info()))

#     # Extract and Transform
#     print("Running transformations...")
#     fact_daily, monthly_agg, dim_date, dim_month = transform.transform_flights(
#         extract.flights_info(),
#     )
    
#     maintenance_monthly = transform.transform_maintenance(extract.maintenance_info())
#     fact_monthly = transform.prepare_fact_flight_monthly(monthly_agg, maintenance_monthly)
    
#     fact_logbook, dim_reporteur = transform.transform_logbook(
#         extract.post_flight_reports(),
#         extract.maintenance_personnel_info(),
#         set(aircraft_df["aircraft_reg_code"].tolist())
#     )
    
#     dim_aircraft = transform.prepare_dim_aircraft(aircraft_df)
    
#     # Create DW and Load
#     print("\nCreating Data Warehouse...")
#     dw = DW(create=True)
    
#     load_all(dw, dim_aircraft, dim_reporteur, dim_date, dim_month,
#              fact_daily, fact_monthly, fact_logbook)
    
#     # Verify loading
#     print("\n" + "-" * 80)
#     print("Verifying loaded data...")
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM dim_aircraft").fetchone()
#     print(f"dim_aircraft: {result[0]} rows")
    
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM dim_reporteur").fetchone()
#     print(f"dim_reporteur: {result[0]} rows")
    
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM dim_month").fetchone()
#     print(f"dim_month: {result[0]} rows")
    
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM dim_date").fetchone()
#     print(f"dim_date: {result[0]} rows")
    
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM fact_flight_daily").fetchone()
#     print(f"fact_flight_daily: {result[0]} rows")
    
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM fact_flight_monthly").fetchone()
#     print(f"fact_flight_monthly: {result[0]} rows")
    
#     result = dw.conn_duckdb.execute("SELECT COUNT(*) FROM fact_logbook").fetchone()
#     print(f"fact_logbook: {result[0]} rows")
    
#     # Close connection
#     dw.close()
    
#     print("\n" + "=" * 80)
#     print("ETL LOAD TEST COMPLETED!")
#     print("=" * 80)
