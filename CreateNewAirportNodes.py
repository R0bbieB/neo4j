from neo4j import GraphDatabase
import pandas as pd
import GeneralProc

# Create local db with password "test"
uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "Testing123"))


def create_graph(tx):
    file = r"Flight_delay.csv"
    df = pd.read_csv(file)

    # Select both Origin (IATA code) and Org_Airport (Airport Name)
    airportsDf = df[['Origin', 'Org_Airport']]
    airportsDf = airportsDf.drop_duplicates()

    for index, airport in airportsDf.iterrows():
        iata_code = airport['Origin']
        airport_name = airport['Org_Airport']

        # Use MERGE instead of CREATE to avoid duplicate nodes
        createGraph = (
            "MERGE (a:Airport {IATA: $iata})\n"
            "SET a.Name = $name"
        )

        tx.run(createGraph, iata=iata_code, name=airport_name)


with driver.session() as session:
    session.execute_write(create_graph)

driver.close()