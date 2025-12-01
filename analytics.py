import duckdb
import time
import os

# Path to your Parquet files (Silver Layer)
# The '*' implies we want to read ALL parquet files in that folder
parquet_path = "spark_data/silver_layer/*.parquet"


def run_analytics():
    print("Connecting to ShopPulse Data Lake...")

    # Connect to an in-memory database
    con = duckdb.connect(database=':memory:')

    while True:
        print("\n" + "=" * 50)
        print("        SHOPPULSE REAL-TIME DASHBOARD        ")
        print("=" * 50)

        # Check if data exists first
        if not os.path.exists("spark_data/silver_layer"):
            print("Waiting for data to arrive in Data Lake...")
            time.sleep(5)
            continue

        try:
            # QUERY 1: Total Events by Type
            print("\n[Metric 1] Total Activity:")
            con.execute(f"SELECT event_type, COUNT(*) as total FROM '{parquet_path}' GROUP BY event_type")
            print(con.fetchall())

            # QUERY 2: Top 5 Active Users
            print("\n[Metric 2] Top 5 Users by Activity:")
            con.execute(
                f"SELECT user_id, COUNT(*) as actions FROM '{parquet_path}' GROUP BY user_id ORDER BY actions DESC LIMIT 5")
            print(con.fetchall())

            # QUERY 3: Traffic by Location (Simulating 'Hotspots')
            print("\n[Metric 3] Top Locations:")
            con.execute(
                f"SELECT location, COUNT(*) as traffic FROM '{parquet_path}' GROUP BY location ORDER BY traffic DESC LIMIT 5")
            print(con.fetchall())

        except Exception as e:
            print(f"Error querying data: {e}")
            print("Spark might be writing to the file right now. Retrying...")

        print("\n(Refreshing in 10 seconds...)")
        time.sleep(10)


if __name__ == "__main__":
    run_analytics()