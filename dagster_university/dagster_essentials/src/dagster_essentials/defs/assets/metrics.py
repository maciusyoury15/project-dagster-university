from datetime import datetime, timedelta

import dagster as dg
from dagster_duckdb import DuckDBResource
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import weekly_partition


@dg.asset(
    deps=["taxi_trips", "taxi_zones"],
    group_name="metrics"
)
def manhattan_stats(database: DuckDBResource) -> None:
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

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetchdf()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())


# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["manhattan_stats"],
    group_name="metrics"
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
    partitions_def=weekly_partition,
    group_name="metrics"
)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The weekly aggregated taxi trips dataset, loaded into a CSV file, partitioned by week.
    """

    period_to_fetch = context.partition_key

    query = f"""
    select
        period_to_fetch, *
    from (
        select
            count(vendor_id)                  as num_trips,
            round(sum(total_amount), 2)       as total_amount,
            round(sum(trip_distance), 2)      as trip_distance,
            sum(passenger_count)::int         as passenger_count
        from trips
        where pickup_datetime >= '{period_to_fetch}'        
          and pickup_datetime <  '{period_to_fetch}'::date + interval '1 week'
    ) cross join (select '{period_to_fetch}' as period_to_fetch)
    """

    with database.get_connection() as conn:
        trips_by_week = conn.execute(query).fetchdf()

    try:
        # If the file already exists, append to it, but replace the existing month's data
        existing_data = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing_data = existing_data[existing_data["period_to_fetch"] != period_to_fetch]
        existing_data = pd.concat([existing_data, trips_by_week]).sort_values(by="period_to_fetch")
        existing_data.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        # If the file doesn't exist, create it with the current data
        trips_by_week.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)