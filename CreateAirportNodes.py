from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import GeneralProc

##Create local db with password "test"

uri = "neo4j://localhost:7687" #creates a connection with the local database
driver = GraphDatabase.driver(uri,
                              auth=("neo4j", "Testing123"))



def create_graph(tx): # function for creating the graph
    file = r"Flight_Delay.parquet" #reads in the flight delay parquet
    df = pd.read_parquet(file) #creates a dataframe form that file

    airportsDf = df[['OriginCityName']]
    print(airportsDf)
    airportsDf = airportsDf.drop_duplicates()
    print(airportsDf)

    for index, airport in airportsDf.iterrows(): #iterates over each airport within the database

        createGraph = ("CREATE\n" + #creates a node for each airport with the location information
                   "(AirportNo"+str(index)+":Airport {Location:'"+GeneralProc.fixAirport(str(airport[0]))+"'})")
        print(createGraph)
        tx.run(createGraph)

with driver.session() as session:
    session.execute_write(create_graph)

driver.close()
