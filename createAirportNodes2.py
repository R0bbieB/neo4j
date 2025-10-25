from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import GeneralProc

##Create local db with password "test"

uri = "neo4j://localhost:7687" #creates a connection with the local database
driver = GraphDatabase.driver(uri,
                              auth=("neo4j", "Testing123"))

def create_graph(tx):
    file = r"Flight_Delay.parquet"
    df = pd.read_parquet(file)

    airportsDf = df[['OriginCityName']].drop_duplicates()

    for index, airport in airportsDf.iterrows():
        createGraph = (
            "CREATE (:Airport {Location: $location})"
        )
        tx.run(createGraph, location=GeneralProc.fixAirport(str(airport[0])))