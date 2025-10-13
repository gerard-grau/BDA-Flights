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
                # ======================================================================================================= Create the tables in the DW
                create_statements = [
                    """
                    CREATE TABLE dim_aircraft (
                        aircraft_id VARCHAR PRIMARY KEY,
                        model VARCHAR,
                        manufacturer VARCHAR
                    );
                    """,
                    """
                    CREATE TABLE dim_reporter (
                        reporter_id VARCHAR PRIMARY KEY,
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
                        adis DECIMAL(10, 2),
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
                        reporter_id VARCHAR NOT NULL,
                        logbook_entries INTEGER,
                        FOREIGN KEY (aircraft_id) REFERENCES dim_aircraft(aircraft_id),
                        FOREIGN KEY (month_code) REFERENCES dim_month(month_code),
                        FOREIGN KEY (reporter_id) REFERENCES dim_reporter(reporter_id),
                        PRIMARY KEY (aircraft_id, month_code, reporter_id)
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

        # ======================================================================================================= Dimension and fact table objects
        # NOTE: Declarations updated to use natural keys.
        self.dim_aircraft = CachedDimension(
            name='dim_aircraft',
            key='aircraft_id',
            attributes=('model', 'manufacturer')
        )

        self.dim_reporter = CachedDimension(
            name='dim_reporter',
            key='reporter_id',
            attributes=('type', 'airport_code')
        )

        self.dim_month = CachedDimension(
            name='dim_month',
            key='month_code',
            attributes=('year',)
        )

        self.dim_date = SnowflakedDimension(
            name='dim_date',
            key='date_code',
            attributes=(), # date_code is the key, no other attributes
            refs={'month_code': self.dim_month}
        )

        self.fact_flight_daily = FactTable(
            name='fact_flight_daily',
            keyrefs=('aircraft_id', 'date_code'),
            measures=('flight_hours', 'flight_cycles')
        )

        self.fact_flight_monthly = FactTable(
            name='fact_flight_monthly',
            keyrefs=('aircraft_id', 'month_code'),
            measures=('flight_hours', 'flight_cycles', 'adis', 'adoss', 'adosu',
                      'delays', 'cancellations', 'total_delay_minutes')
        )

        self.fact_logbook = FactTable(
            name='fact_logbook',
            keyrefs=('aircraft_id', 'month_code', 'reporter_id'),
            measures=('logbook_entries',)
        )

    # Rewrite the queries exemplified in "extract.py"
    def query_utilization(self):
        result = self.conn_duckdb.execute("""
            SELECT
                da.manufacturer,
                dm.year,
                ROUND(SUM(ffm.flight_hours) / COUNT(DISTINCT ffm.aircraft_id), 2) AS FH,
                ROUND(SUM(ffm.flight_cycles) / COUNT(DISTINCT ffm.aircraft_id), 2) AS TakeOff,
                ROUND(SUM(ffm.adoss) / COUNT(DISTINCT ffm.aircraft_id), 2) AS ADOSS,
                ROUND(SUM(ffm.adosu) / COUNT(DISTINCT ffm.aircraft_id), 2) AS ADOSU,
                ROUND((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id), 2) AS ADOS,
                365 - ROUND((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id), 2) AS ADIS,
                ROUND(
                    (SUM(ffm.flight_hours) / COUNT(DISTINCT ffm.aircraft_id)) /
                    NULLIF(((365 - ((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id))) * 24), 0),
                2) AS DU,
                ROUND(
                    (SUM(ffm.flight_cycles) / COUNT(DISTINCT ffm.aircraft_id)) /
                    NULLIF((365 - ((SUM(ffm.adoss) + SUM(ffm.adosu)) / COUNT(DISTINCT ffm.aircraft_id))), 0),
                2) AS DC,
                100 * ROUND(SUM(ffm.delays) / NULLIF(SUM(ffm.flight_cycles), 0), 4) AS DYR,
                100 * ROUND(SUM(ffm.cancellations) / NULLIF(SUM(ffm.flight_cycles), 0), 4) AS CNR,
                100 - ROUND(100 * (SUM(ffm.delays) + SUM(ffm.cancellations)) / NULLIF(SUM(ffm.flight_cycles), 0), 2) AS TDR,
                ROUND(SUM(ffm.total_delay_minutes) / NULLIF(SUM(ffm.delays), 0), 2) AS ADD
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
                        dm.year,
                        SUM(fl.logbook_entries) as total_reports
                    FROM fact_logbook fl
                    JOIN dim_aircraft da ON fl.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON fl.month_code = dm.month_code
                    GROUP BY da.manufacturer, dm.year
                ),
                monthly_utilization AS (
                    SELECT
                        da.manufacturer,
                        dm.year,
                        SUM(ffm.flight_hours) as total_flight_hours,
                        SUM(ffm.flight_cycles) as total_flight_cycles
                    FROM fact_flight_monthly ffm
                    JOIN dim_aircraft da ON ffm.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON ffm.month_code = dm.month_code
                    GROUP BY da.manufacturer, dm.year
                )
            SELECT
                mr.manufacturer,
                mr.year,
                1000 * ROUND(mr.total_reports / NULLIF(mu.total_flight_hours, 0), 3) AS RRh,
                100 * ROUND(mr.total_reports / NULLIF(mu.total_flight_cycles, 0), 2) AS RRc
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
                        dm.year,
                        dr.type AS role,
                        SUM(fl.logbook_entries) as total_reports
                    FROM fact_logbook fl
                    JOIN dim_aircraft da ON fl.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON fl.month_code = dm.month_code
                    JOIN dim_reporter dr ON fl.reporter_id = dr.reporter_id
                    GROUP BY da.manufacturer, dm.year, dr.type
                ),
                monthly_utilization AS (
                    SELECT
                        da.manufacturer,
                        dm.year,
                        SUM(ffm.flight_hours) as total_flight_hours,
                        SUM(ffm.flight_cycles) as total_flight_cycles
                    FROM fact_flight_monthly ffm
                    JOIN dim_aircraft da ON ffm.aircraft_id = da.aircraft_id
                    JOIN dim_month dm ON ffm.month_code = dm.month_code
                    GROUP BY da.manufacturer, dm.year
                )
            SELECT
                mr.manufacturer,
                mr.year,
                mr.role,
                1000 * ROUND(mr.total_reports / NULLIF(mu.total_flight_hours, 0), 3) AS RRh,
                100 * ROUND(mr.total_reports / NULLIF(mu.total_flight_cycles, 0), 2) AS RRc
            FROM monthly_reports_per_role mr
            JOIN monthly_utilization mu ON mr.manufacturer = mu.manufacturer AND mr.year = mu.year
            ORDER BY mr.manufacturer, mr.year, mr.role;
            """).fetchall()
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
