from dw import DW
import extract
import transform
import load


if __name__ == '__main__':

    print("Running ETL pipeline...")
    
    dw = DW(create=True)
    transform.clear_log_file()

    # load Aircraft dimension table
    aircraft_df, aircraft_codes = transform.stage_aircrafts(  # save aircraft codes for the future
        extract.aircraft_manufacturer_info()
    )
    load.load_dim_aircraft(
        dw,
        transform.prepare_dim_aircraft(aircraft_df)
    )
    
    # load Month and Date dimension tables, and Daily and Monthly fact tables
    fact_daily_df, monthly_agg_df, dim_date_df, dim_month_df, limit_dates = transform.transform_flights(  # save limit dates (max & min)
        extract.flights_info(),
    )
    load.load_date_snowflake(dw, dim_month_df, dim_date_df)
    load.load_fact_flight_daily(dw, fact_daily_df)
    load.load_fact_flight_monthly(
        dw,
        transform.prepare_fact_flight_monthly(
            monthly_agg_df,
            extract.maintenance_info()
        )
    )

    # load reporteur dimension table, and logbook fact table
    fact_logbook, dim_reporteur = transform.transform_logbook(
        extract.post_flight_reports(),
        extract.maintenance_personnel_info(),
        aircraft_codes,
        limit_dates
    )
    load.load_dim_reporteur(dw, dim_reporteur)
    load.load_fact_logbook(dw, fact_logbook)
    
    dw.close()
