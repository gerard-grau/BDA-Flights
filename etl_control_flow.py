from dw import DW
import extract
import transform
import load


if __name__ == '__main__':

    transform.clear_log_file()
    
    # Extract and Transform
    print("Running transformations...")
    fact_daily, monthly_agg, dim_date, dim_month = transform.transform_flights(
        extract.flights_info(),
        extract.aircraft_manufacturer_info()
    )
    
    maintenance_monthly = transform.transform_maintenance(extract.maintenance_info())
    fact_monthly = transform.prepare_fact_flight_monthly(monthly_agg, maintenance_monthly)
    
    fact_logbook, dim_reporteur = transform.transform_logbook(
        extract.post_flight_reports(),
        extract.aircraft_manufacturer_info(),
        extract.maintenance_personnel_info()
    )
    
    dim_aircraft = transform.prepare_dim_aircraft(extract.aircraft_manufacturer_info())
    
    # Create DW and Load
    print("\nCreating Data Warehouse...")
    dw = DW(create=True)
    load.load_all(dw, dim_aircraft, dim_reporteur, dim_date, dim_month,
                  fact_daily, fact_monthly, fact_logbook)

    dw.close()
