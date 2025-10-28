import pandas as pd

from dw import DW
import extract
import transform
import load


if __name__ == '__main__':

    transform.clear_log_file()
    
    # Extract and Transform
    print("Running transformations...")

    aircraft_df = pd.DataFrame(list(extract.aircraft_manufacturer_info()))
    fact_daily, monthly_agg, dim_date, dim_month = transform.transform_flights(
        extract.flights_info(),
    )
    
    maintenance_monthly = transform.transform_maintenance(extract.maintenance_info())
    fact_monthly = transform.prepare_fact_flight_monthly(monthly_agg, maintenance_monthly)
    
    fact_logbook, dim_reporteur = transform.transform_logbook(
        extract.post_flight_reports(),
        extract.maintenance_personnel_info(),
        set(aircraft_df["aircraft_reg_code"].tolist())
    )
    
    dim_aircraft = transform.prepare_dim_aircraft(aircraft_df)
    
    # Create DW and Load
    print("\nCreating Data Warehouse...")
    dw = DW(create=True)
    load.load_all(dw, dim_aircraft, dim_reporteur, dim_date, dim_month,
                  fact_daily, fact_monthly, fact_logbook)

    dw.close()
