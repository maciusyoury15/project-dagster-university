from dagster_duckdb import DuckDBResource
import dagster as dg

database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")   # replaced with environment variable for database path
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"database": database_resource})