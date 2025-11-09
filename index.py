from neo4j import GraphDatabase
import pandas as pd

class GenerateTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def create_cities(self):
        cities = pd.read_csv('data/cities.csv', sep=';')
        for _, row in cities.iterrows():
            with self.driver.session() as session:
                session.execute_write(
                    self._create_city,
                    row['name'],
                    row['latitude'],
                    row['longitude'],
                    row['population']
                )

    def create_lines(self):
        lines = pd.read_csv('data/lines.csv', sep=';')
        for _, row in lines.iterrows():
            with self.driver.session() as session:
                session.execute_write(
                    self._create_line,
                    row['city1'],
                    row['city2'],
                    row['km'],
                    row['time'],
                    row['nbTracks']
                )
    
    def create_graph_projection(self, graph_name, prop):
        with self.driver.session() as session:
            session.execute_write(
                self._create_graph_projection,
                graph_name,
                prop
            )

    @staticmethod
    def _create_city(tx, name, latitude, longitude, population):
        query = (
            """
            CREATE (c:City { name: $name, latitude: $latitude, longitude: $longitude, population: $population })
            RETURN c
            """
        )
        result = tx.run(query, name=name, latitude=latitude, longitude=longitude, population=population)

        city_created = result.single()['c']
        print("Created City: {name}".format(name=city_created['name']))

    @staticmethod
    def _create_line(tx, city1, city2, km, time, nbTracks):
        query = (
            """
            MATCH (c1:City { name: $city1})
            MATCH (c2:City { name: $city2 })
            CREATE (c1)-[l1:Line { km: $km, time: $time, nbTracks: $nbTracks }]->(c2)
            CREATE (c2)-[l2:Line { km: $km, time: $time, nbTracks: $nbTracks }]->(c1)
            RETURN l1, l2
            """
        )
        result = tx.run(query, city1=city1, city2=city2, km=km, time=time, nbTracks=nbTracks)

        line_created = result.single()['l1']
        print("Created Line between: {city1} and {city2} that is {km}km long".format(city1=city1, city2=city2, km=line_created['km']))

    @staticmethod
    def _create_graph_projection(tx, graph_name, prop):
        query = (
            """
            MATCH (source:City)-[r:Line]->(target:City)
            RETURN gds.graph.project(
                $graph_name,
                source,
                target,
                {
                    relationshipProperties: r {.%s}
                }
            )
            """ % prop
        )
        tx.run(query, graph_name=graph_name)

if __name__ == "__main__":
    generate_train_network = GenerateTrainNetwork("neo4j://localhost:7687")
    #generate_train_network.create_cities()
    #generate_train_network.create_lines()
    generate_train_network.create_graph_projection("trainNetworkGraphTime", "time")
    generate_train_network.create_graph_projection("trainNetworkGraphDistance", "km")

