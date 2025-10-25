from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import GeneralProc
##Create local db with password "test"

uri = "neo4j://localhost:7687" #establishes the connection with the local database
driver = GraphDatabase.driver(uri,
                              auth=("neo4j", "Testing123"))


def create_graph(tx): # Defines a function responsible for creating the graph structure within a Neo4j transaction (tx).
    file = r"Flight_Delay.parquet"
    df = pd.read_parquet(file)

    FlightDf = df[['FlightDate','OriginCityName', 'DestCityName', #reads in the information from these specific columnds of the flight delay csv
                   'WeatherDelay', 'DepDelay', 'ArrDelay', 'DepTime', 'ArrTime']][:100000]#Limits to 1000 entrys because putting every flight was timing out

    for index, flight in FlightDf.iterrows(): #iterates over each flight within the csv
        origin = GeneralProc.fixAirport(str(flight['OriginCityName']))
        dest = GeneralProc.fixAirport(str(flight['DestCityName']))

        createGraph = (
            "MATCH (Origin:Airport), (Dest:Airport) \n" +
            "WHERE Origin.Location = $origin AND Dest.Location = $dest \n" +
            "MERGE (Origin)-[r:FLIGHT { " +
            "Date: $date, " +
            "WeatherDelay: $weatherDelay, " +
            "DepDelay: $depDelay, " +
            "ArrDelay: $arrDelay, " +
            "DepTime: $depTime, " +
            "ArrTime: $arrTime " +
            "}]->(Dest)\n"
        )
        print(createGraph)
        tx.run(createGraph,
               origin=origin,
               dest=dest,
               date=str(flight['FlightDate']),
               weatherDelay=flight['WeatherDelay'],
               depDelay=flight['DepDelay'],
               arrDelay=flight['ArrDelay'],
               depTime=flight['DepTime'],
               arrTime=flight['ArrTime']
               )


with driver.session() as session:
    session.execute_write(create_graph)

driver.close()
