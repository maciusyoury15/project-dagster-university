import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os

from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff


@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn.execute(query).fetchdf()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())


# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim(-74.05, -73.90)  # Adjust longitude range
    ax.set_ylim(40.70, 40.82)  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(
    deps=["taxi_trips"],
)
def trips_by_week() -> None:
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )

    query = """
        select
            date_trunc('week', pickup_datetime + interval '1 day')::date - 1 as period,
            count(vendor_id)                  as num_trips,
            round(sum(total_amount), 2)       as total_amount,
            round(sum(trip_distance), 2)      as trip_distance,
            sum(passenger_count)::int         as passenger_count
        from trips
        where pickup_datetime >= '2023-03-05'
          and pickup_datetime <  '2023-04-02'
        group by 1
        order by 1
    """
    trips_by_week = conn.execute(query).fetchdf()

    with open(constants.TRIPS_BY_WEEK_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_week.to_csv(index=False))