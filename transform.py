from tqdm import tqdm
import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    filename="cleaning.log",  # Log file name
    level=logging.INFO,  # Logging level
    format="%(message)s",  # Log message format
)


def build_dateCode(date) -> str:
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date) -> str:
    return f"{date.year}{str(date.month).zfill(2)}"


# TODO: Implement here all transforming functions


def stage_data(data) -> pd.DataFrame:
    """Stage data into a DataFrame"""

    rows = []
    for row in tqdm(data, desc=f"Staging data into pandas dataframe."):
        rows.append(row)

    return pd.DataFrame.from_dict(rows)


def enhance_BSR1(stagingDB) -> pd.DataFrame:
    """ "Apply BSR1 to data"""

    for row in tqdm(stagingDB.itertuples(), desc="Enhancing BSR1"):

        if row.actualdeparture > row.actualarrival:

            stagingDB.at[row.Index, "actualdeparture"] = row.actualarrival
            stagingDB.at[row.Index, "actualarrival"] = row.actualdeparture

    return stagingDB


def enhance_BSR2(stagingDB) -> pd.DataFrame:
    """Apply BSR2 to data. Assumes that rows are ordered by actualdeparture."""

    deletedIndexes = []
    prev_row = None
    for row in tqdm(stagingDB.itertuples(), desc="Enhancing BSR2"):

        if not row.cancelled:

            # TODO: decidir com fer el log
            if prev_row and row.actualdeparture < prev_row.actualarrival:
                logging.info(prev_row)
                deletedIndexes.append(prev_row.Index)

            prev_row = row

    stagingDB.drop(deletedIndexes, inplace=True)
    return stagingDB


def split_date(stagingDB) -> pd.DataFrame:
    """Add date column"""

    for row in tqdm(stagingDB.itertuples(), desc="Splitting date"):
        if not row.cancelled:
            stagingDB.at[row.Index, "date"] = build_dateCode(row.actualdeparture)
        else:
            stagingDB.at[row.Index, "date"] = build_dateCode(row.scheduleddeparture)

    return stagingDB


def split_month(stagingDB) -> pd.DataFrame:
    """Add month column"""

    # TODO: això extreu NO de date sinó de actualarrival, etc. ja que fem servir implementació
    # que hi ha ja en el codi: build_monthCode. Fa falta?

    for row in tqdm(stagingDB.itertuples(), desc="Splitting month"):
        if not row.cancelled:
            stagingDB.at[row.Index, "month"] = build_monthCode(row.actualdeparture)
        else:
            stagingDB.at[row.Index, "month"] = build_monthCode(row.scheduleddeparture)

    return stagingDB


def compute_TH(stagingDB) -> pd.DataFrame:
    """Calculate TH metricc by day, in minutes."""
    # TODO: potser fer if not NULL: (o if not cancelled)
    # TODO: unificar sintaxi?
    # TODO: segons o minuts?
    # stagingDB["TH"] = stagingDB["actualarrival"] - stagingDB["actualdeparture"]
    # stagingDB["TH"] = stagingDB["TH"].apply(lambda td: td.total_seconds() // 60)

    for row in tqdm(stagingDB.itertuples(), desc="Computing TH metric"):
        stagingDB.at[row.Index, "TH"] = (
            row.actualarrival - row.actualdeparture
        ).total_seconds() // 60

    return stagingDB


def compute_TDM(stagingDB) -> pd.DataFrame:
    """Calculate TDM metricc by day, in minutes."""
    # TODO: potser fer if not NULL: (o if not cancelled)
    # TODO: segons o minuts?

    for row in tqdm(stagingDB.itertuples(), desc="Computing TDM metric"):
        if row.delaycode:
            stagingDB.at[row.Index, "TDM"] = (
                row.actualdeparture - row.scheduleddeparture
            ).total_seconds() // 60

    return stagingDB


def aggregate_by_time(stagingDB, granularity) -> pd.DataFrame:
    """Aggregate data by day or month."""

    # TODO: fer que agregació de month es faci directament sobre dades ja agregades per date.
    # Això té a veure com fem servir lo de build_monthCode()
    # TODO: fer que sigui for row in tqdm(...) per d'unificar sintaxi?
    assert granularity in ["date", "month"]

    print(f"Aggregating by {granularity}")
    aggregatedDB = (
        stagingDB.groupby(["aircraftregistration", granularity])
        .agg(
            TH=pd.NamedAgg(column="TH", aggfunc="sum"),
            TO=pd.NamedAgg(column="cancelled", aggfunc=lambda x: len(x) - sum(x)),
            CN=pd.NamedAgg(column="cancelled", aggfunc="sum"),
            DY=pd.NamedAgg(
                column="delaycode", aggfunc=lambda x: len(x) - sum(x.isna())
            ),
            TDM=pd.NamedAgg(column="TDM", aggfunc="sum"),
        )
        .reset_index()
    )

    return aggregatedDB


def compute_ADOSS(maintenanceDB) -> pd.DataFrame:
    """Compute ADOSS from maintenanceDB."""

    for row in tqdm(maintenanceDB.itertuples(), desc="Computing ADOSS"):

        if row.programmed:
            maintenanceDB.at[row.Index, "ADOSS"] = round(
                (row.delay.days + row.delay.seconds / 86400), 2
            )

    return maintenanceDB


def compute_ADOSU(maintenanceDB) -> pd.DataFrame:
    """Compute ADOSU from maintenanceDB."""

    for row in tqdm(maintenanceDB.itertuples(), desc="Computing ADOSS"):

        if not row.programmed:
            maintenanceDB.at[row.Index, "ADOSU"] = round(
                (row.delay.days + row.delay.seconds / 86400), 2
            )

    return maintenanceDB


def merge_ADOS(stagingDB, maintenanceDB) -> pd.DataFrame:
    """Merge ADOSS and ADOSU from maintenanceDB into stagingDB."""

    mergedDB = pd.merge(
        stagingDB,
        maintenanceDB[
            [
                "aircraftregistration",
                "month",
                "ADOSS",
                "ADOSU",
            ]
        ],
        on=["aircraftregistration", "month"],
        how="left",
    )

    return mergedDB


def check(df):

    # check la BSR1 hihiha
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        if row["actualdeparture"] > row["actualarrival"]:
            print("madafakaaa")


##### TESTING
import extract
import datetime


if __name__ == "__main__":

    if True:
        df = stage_data(extract.flights_info())
        df = enhance_BSR1(df)
        df = enhance_BSR2(df)
        df = split_date(df)
        df = split_month(df)
        df = compute_TH(df)
        df = compute_TDM(df)
        df = aggregate_by_time(df, "month")
        print(df.head(5))

        # yipyap = stage_data(extract.maintenance_info())

        # yipyap = compute_ADOSS(yipyap)
        # yipyap = compute_ADOSU(yipyap)
        # print(yipyap.head())

        # df = merge_ADOS(df, yipyap)
        # print(df.head(5))
