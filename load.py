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
