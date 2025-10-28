import os
import sys
import duckdb  # https://duckdb.org
import pygrametl  # https://pygrametl.org
from pygrametl.tables import CachedDimension, SnowflakedDimension, FactTable


duckdb_filename = 'dw.duckdb'


class DW:
    def __init__(self, create=False):
        if create and os.path.exists(duckdb_filename):
            os.remove(duckdb_filename)
        try:
            self.conn_duckdb = duckdb.connect(duckdb_filename)
            print("Connection to the DW created successfully")
        except duckdb.Error as e:
            print(f"Unable to connect to DuckDB database '{duckdb_filename}':", e)
            sys.exit(1)

        if create:
            try:
                # ======================================================================
                # Create the tables in the DW
                # ======================================================================

                create_statements = [
                    """
                    CREATE TABLE dim_aircraft (
                        aircraft_id VARCHAR PRIMARY KEY,
                        model VARCHAR,
                        manufacturer VARCHAR
                    );
                    """,
                    """
                    CREATE TABLE dim_reporteur (
                        reporteur_id INTEGER PRIMARY KEY,
                        type VARCHAR,
                        airport_code VARCHAR
                    );
                    """,
                    """
                    CREATE TABLE dim_month (
                        month_code VARCHAR PRIMARY KEY,
                        year INTEGER
                    );
                    """,
                    """
                    CREATE TABLE dim_date (
                        date_code DATE PRIMARY KEY,
                        month_code VARCHAR NOT NULL,
                        FOREIGN KEY (month_code) REFERENCES dim_month(month_code)
                    );
                    """,
                    """
                    CREATE TABLE fact_flight_daily (
                        aircraft_id VARCHAR NOT NULL,
                        date_code DATE NOT NULL,
                        flight_hours DECIMAL(10, 2),
                        flight_cycles INTEGER,
                        FOREIGN KEY (aircraft_id) REFERENCES dim_aircraft(aircraft_id),
                        FOREIGN KEY (date_code) REFERENCES dim_date(date_code),
                        PRIMARY KEY (aircraft_id, date_code)
                    );
                    """,
                    """
                    CREATE TABLE fact_flight_monthly (
                        aircraft_id VARCHAR NOT NULL,
                        month_code VARCHAR NOT NULL,
                        flight_hours DECIMAL(10, 2),
                        flight_cycles INTEGER,
                        adoss DECIMAL(10, 2),
                        adosu DECIMAL(10, 2),
                        delays INTEGER,
                        cancellations INTEGER,
                        total_delay_minutes INTEGER,
                        FOREIGN KEY (aircraft_id) REFERENCES dim_aircraft(aircraft_id),
                        FOREIGN KEY (month_code) REFERENCES dim_month(month_code),
                        PRIMARY KEY (aircraft_id, month_code)
                    );
                    """,
                    """
                    CREATE TABLE fact_logbook (
                        aircraft_id VARCHAR NOT NULL,
                        month_code VARCHAR NOT NULL,
                        reporteur_id INTEGER NOT NULL,
                        logbook_entries INTEGER,
                        FOREIGN KEY (aircraft_id) REFERENCES dim_aircraft(aircraft_id),
                        FOREIGN KEY (month_code) REFERENCES dim_month(month_code),
                        FOREIGN KEY (reporteur_id) REFERENCES dim_reporteur(reporteur_id),
                        PRIMARY KEY (aircraft_id, month_code, reporteur_id)
                    );
                    """
                ]
                for statement in create_statements:
                    self.conn_duckdb.execute(statement)
                print("Data Warehouse tables created successfully")
            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)
        
        # ======================================================================
        #  Dimension and fact table objects
        # ======================================================================

        # --- Dimension Table Definitions ---
        self.dim_aircraft = CachedDimension(
            name='dim_aircraft',
            key='aircraft_id',
            attributes=('model', 'manufacturer')
        )

        self.dim_reporteur = CachedDimension(
            name='dim_reporteur',
            key='reporteur_id',
            attributes=('type', 'airport_code')
        )

        self.dim_month = CachedDimension(
            name='dim_month',
            key='month_code',
            attributes=('year',)
        )

        date_level_dim = CachedDimension(
            name='dim_date',
            key='date_code',
            attributes=('month_code',)
        )

        self.dim_date = SnowflakedDimension(
            references=[(date_level_dim, self.dim_month)]
        )

        # --- Fact Table Definitions ---
        self.fact_flight_daily = FactTable(
            name='fact_flight_daily',
            keyrefs=('aircraft_id', 'date_code'),
            measures=('flight_hours', 'flight_cycles')
        )

        self.fact_flight_monthly = FactTable(
            name='fact_flight_monthly',
            keyrefs=('aircraft_id', 'month_code'),
            measures=('flight_hours', 'flight_cycles', 'adoss', 'adosu',
                      'delays', 'cancellations', 'total_delay_minutes')
        )

        self.fact_logbook = FactTable(
            name='fact_logbook',
            keyrefs=('aircraft_id', 'month_code', 'reporteur_id'),
            measures=('logbook_entries',)
        )

    # ======================================================================
    #  Rewrite query utilization
    # ======================================================================
    def query_utilization(self):
        result = self.conn_duckdb.execute("""
            SELECT
                da.manufacturer,
                CAST(dm.year AS VARCHAR) AS year,
                CAST(ROUND(SUM(ffm.flight_hours) / COUNT(DISTINCT ffm.aircraft_id), 2) AS DECIMAL(10,2)) AS FH,
                CAST(ROUND(SUM(ffm.flight_cycles) / COUNT(DISTINCT ffm.aircraft_id), 2) AS DECIMAL(10,2)) AS TakeOff,
                CAST(ROUND(SUM(ffm.adoss) / COUNT(DISTINCT ffm.aircraft_id), 2) AS DECIMAL(10,2)) AS ADOSS,
                CAST(ROUND(SUM(ffm.adosu) / COUNT(DISTINCT ffm.aircraft_id), 2) AS DECIMAL(10,2)) AS ADOSU,
                CAST(ROUND((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id), 2) AS DECIMAL(10,2)) AS ADOS,
                CAST(365 - ROUND((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id), 2) AS DECIMAL(10,2)) AS ADIS,
                CAST(ROUND(
                    ROUND(SUM(ffm.flight_hours) / COUNT(DISTINCT ffm.aircraft_id), 2) /
                    ((365 - ROUND((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id), 2)) * 24),
                2) AS DECIMAL(10,2)) AS DU,
                CAST(ROUND(
                    ROUND(SUM(ffm.flight_cycles) / COUNT(DISTINCT ffm.aircraft_id), 2) /
                    (365 - ROUND((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id), 2)),
                2) AS DECIMAL(10,2)) AS DC,
                CAST(100 * ROUND(SUM(ffm.delays) / ROUND(SUM(ffm.flight_cycles), 2), 4) AS DECIMAL(10,4)) AS DYR,
                CAST(100 * ROUND(SUM(ffm.cancellations) / ROUND(SUM(ffm.flight_cycles), 2), 4) AS DECIMAL(10,4)) AS CNR,
                CAST(100 - ROUND(100 * (SUM(ffm.delays) + SUM(ffm.cancellations)) / SUM(ffm.flight_cycles), 2) AS DECIMAL(10,2)) AS TDR,
                CAST(100 * ROUND(SUM(ffm.total_delay_minutes) / SUM(ffm.delays), 2) AS DECIMAL(10,2)) AS ADD
            FROM fact_flight_monthly ffm
            JOIN dim_aircraft da ON ffm.aircraft_id = da.aircraft_id
            JOIN dim_month dm ON ffm.month_code = dm.month_code
            GROUP BY da.manufacturer, dm.year
            ORDER BY da.manufacturer, dm.year;
            """).fetchall()
        return result

    def query_reporting(self):
        result = self.conn_duckdb.execute("""
            WITH
                monthly_reports AS (
                    SELECT
                        da.manufacturer,
                        CAST(dm.year AS VARCHAR) AS year,
                        SUM(fl.logbook_entries) as total_reports
                    FROM fact_logbook fl
                    JOIN dim_aircraft da ON fl.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON fl.month_code = dm.month_code
                    GROUP BY da.manufacturer, dm.year
                ),
                monthly_utilization AS (
                    SELECT
                        da.manufacturer,
                        CAST(dm.year AS VARCHAR) AS year,
                        CAST(SUM(ffm.flight_hours) AS DECIMAL) as total_flight_hours,
                        CAST(SUM(ffm.flight_cycles) AS DECIMAL) as total_flight_cycles
                    FROM fact_flight_monthly ffm
                    JOIN dim_aircraft da ON ffm.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON ffm.month_code = dm.month_code
                    GROUP BY da.manufacturer, dm.year
                )
            SELECT
                mr.manufacturer,
                mr.year,
                CAST(1000 * ROUND(mr.total_reports / mu.total_flight_hours, 3) AS DECIMAL(10,3)) AS RRh,
                CAST(100 * ROUND(mr.total_reports / mu.total_flight_cycles, 2) AS DECIMAL(10,2)) AS RRc
            FROM monthly_reports mr
            JOIN monthly_utilization mu ON mr.manufacturer = mu.manufacturer AND mr.year = mu.year
            ORDER BY mr.manufacturer, mr.year;
            """).fetchall()
        return result

    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute("""
            WITH
                monthly_reports_per_role AS (
                    SELECT
                        da.manufacturer,
                        CAST(dm.year AS VARCHAR) AS year,
                        dr.type AS role,
                        SUM(fl.logbook_entries) as total_reports
                    FROM fact_logbook fl
                    JOIN dim_aircraft da ON fl.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON fl.month_code = dm.month_code
                    JOIN dim_reporteur dr ON fl.reporteur_id = dr.reporteur_id
                    GROUP BY da.manufacturer, dm.year, dr.type
                ),
                monthly_utilization AS (
                    SELECT
                        da.manufacturer,
                        CAST(dm.year AS VARCHAR) AS year,
                        CAST(SUM(ffm.flight_hours) AS DECIMAL) as total_flight_hours,
                        CAST(SUM(ffm.flight_cycles) AS DECIMAL) as total_flight_cycles
                    FROM fact_flight_monthly ffm
                    JOIN dim_aircraft da ON ffm.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON ffm.month_code = dm.month_code
                    GROUP BY da.manufacturer, dm.year
                )
            SELECT
                mr.manufacturer,
                mr.year,
                mr.role,
                CAST(1000 * ROUND(mr.total_reports / mu.total_flight_hours, 3) AS DECIMAL(10,3)) AS RRh,
                CAST(100 * ROUND(mr.total_reports / mu.total_flight_cycles, 2) AS DECIMAL(10,2)) AS RRc
            FROM monthly_reports_per_role mr
            JOIN monthly_utilization mu ON mr.manufacturer = mu.manufacturer AND mr.year = mu.year
            ORDER BY mr.manufacturer, mr.year, mr.role;
            """).fetchall()
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
