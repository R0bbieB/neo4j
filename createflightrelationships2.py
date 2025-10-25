from neo4j import GraphDatabase
import pandas as pd
import GeneralProc
from datetime import datetime

uri = "neo4j://localhost:7687" #establishes the connection with the local database
driver = GraphDatabase.driver(uri,
                              auth=("neo4j", "Testing123"))



def create_graph(tx):
    file = r"Flight_Delay.parquet"
    df = pd.read_parquet(file)[['FlightDate', 'OriginCityName', 'DestCityName',
                                'WeatherDelay', 'DepDelay', 'ArrDelay', 'DepTime', 'ArrTime']][:10000]

    for index, flight in df.iterrows():
        origin = GeneralProc.fixAirport(str(flight['OriginCityName']))
        dest = GeneralProc.fixAirport(str(flight['DestCityName']))

        # Data type conversion for DepDelay
        dep_delay = float(flight['DepDelay']) if not pd.isna(flight['DepDelay']) else 0.0
        arr_delay = float(flight['ArrDelay']) if not pd.isna(flight['ArrDelay']) else 0.0

        # Convert to datetime, handling potential errors
        try:
            flight_date = datetime.strptime(str(flight['FlightDate']), "%Y-%m-%d")
        except ValueError:
            print(f"Skipping flight with invalid date format: {flight['FlightDate']}")
            continue

        createGraph = (
                "MERGE (Origin:Airport {Location: $origin}) " +
                "MERGE (Dest:Airport {Location: $dest}) " +
                "MERGE (Origin)-[r:FLIGHT { " +
                "    Date: datetime($date), " +
                "    WeatherDelay: $weatherDelay, " +
                "    DepDelay: $depDelay, " +
                "    ArrDelay: $arrDelay, " +
                "    DepTime: $depTime, " +
                "    ArrTime: $arrTime " +
                "}]->(Dest)\n"
        )

        tx.run(createGraph,
               origin=origin,
               dest=dest,
               date={"year": flight_date.year, "month": flight_date.month, "day": flight_date.day},
               weatherDelay=flight['WeatherDelay'],
               depDelay=dep_delay,  # Pass converted DepDelay value
               arrDelay=arr_delay,
               depTime=flight['DepTime'],
               arrTime=flight['ArrTime']
               )


with driver.session() as session:
    session.execute_write(create_graph)

driver.close()