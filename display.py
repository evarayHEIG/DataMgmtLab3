from neo4j import GraphDatabase
import folium

# display city on the folium map
def display_city_on_map(m, popup, latitude, longitude, radius=1000, color="#3186cc"):
    folium.Circle(
        location=(latitude, longitude),
        radius=radius,
        popup=popup,
        color=color,
        fill=True,
        fill_opacity=0.8,
    ).add_to(m)


# display polyline on the folium map
# locations: (list of points (latitude, longitude)) – Latitude and Longitude of line
def display_polyline_on_map(m, locations, popup=None, color="#3186cc", weight=2.0):
    folium.PolyLine(
        locations,
        popup=popup,
        color=color,
        weight=weight,
        opacity=1
    ).add_to(m)

class DisplayTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def display_cities(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities, map_1)
        map_1.save('out/2_0.html')

    def display_lines(self):
        map_2 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities, map_2)
            session.execute_read(self._display_lines, map_2)
        map_2.save('out/2_1.html')

    def display_query_on_cities(self):
        map_3 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities, map_3)
            session.execute_read(self._display_lines, map_3)
            session.execute_read(self._query_on_cities, map_3)
        map_3.save('out/2_2.html')

    def display_shortest_path(self, graph_name, prop, file_suffix):
        map_4 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities, map_4)
            session.execute_read(self._display_lines, map_4)
            session.execute_read(self._shortest_path, map_4, graph_name, prop)
        map_4.save('out/2_3_{}.html'.format(file_suffix))

    def display_minimum_spanning_tree(self):
        map_5 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.execute_read(self._display_cities, map_5)
            session.execute_read(self._display_lines, map_5)
            session.execute_read(self._minimum_spanning_tree, map_5)
        map_5.save('out/2_4.html')

    @staticmethod
    def _display_cities(tx, m):
        query = (
            """
            MATCH (c:City)
            RETURN c
            """
        )
        result = tx.run(query)
        for record in result:
            display_city_on_map(
                m=m,
                popup=record['c']['name'],
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude']
            )

    @staticmethod
    def _display_lines(tx, m):
        # We retrieve all lines between cities, using a single direction to avoid duplicates
        query = (
            """
            MATCH (c1:City)-[l:Line]->(c2:City)
            RETURN c1, c2, l
            """
        )
        result = tx.run(query)
        for record in result:
            display_polyline_on_map(
                m=m,
                locations=[
                    (record['c1']['latitude'], record['c1']['longitude']),
                    (record['c2']['latitude'], record['c2']['longitude'])
                ],
                popup="{} km, {} min".format(record['l']['km'], record['l']['time'])
            )

    @staticmethod
    def _query_on_cities(tx, m):
        query = (
            """
                match (c2:City{name:"Luzern"})-[l:Line*..4]->(c1:City) 
                where c1.population > 100000 and c1.name <> c2.name
                return distinct c1
            """
        )
        result = tx.run(query)
        for record in result:
            display_city_on_map(
                m=m,
                popup=record['c1']['name'],
                latitude=record['c1']['latitude'],
                longitude=record['c1']['longitude'],
                radius=2000, 
                color="#ff0000"
            )

    @staticmethod
    def _shortest_path(tx, m, graph_name, prop):
        query = (
            """
                MATCH (source:City {name: 'Geneve'}), (target:City {name: 'Chur'})
                CALL gds.shortestPath.dijkstra.stream($graph_name, {
                    sourceNode: source,
                    targetNodes: target,
                    relationshipWeightProperty: $prop
                })
                YIELD index, path
                RETURN nodes(path) as path
                ORDER BY index
            """
        )
        result = tx.run(query, graph_name=graph_name, prop=prop)
        for record in result:
            path = record['path']
            locations = []
            for node in path:
                locations.append((node['latitude'], node['longitude']))
            display_polyline_on_map(
                m=m,
                locations=locations,
                popup="Shortest path based on {}".format(prop),
                color="#ff0000"
            )

    @staticmethod
    def _minimum_spanning_tree(tx, m):
        query = (
            """
            MATCH (c:City{name: 'Chiasso'})
            CALL gds.spanningTree.stream('trainNetworkGraphMinSpanTree', {
            sourceNode: c,
            relationshipWeightProperty: 'cost'
            })
            YIELD nodeId,parentId, weight
            RETURN gds.util.asNode(nodeId) AS node, gds.util.asNode(parentId) AS parent, weight
            ORDER BY node
            """
        )
        result = tx.run(query)
        # display the minimum spanning tree on the map
        for record in result:
            display_polyline_on_map(
                m=m,
                locations=[
                    (record['node']['latitude'], record['node']['longitude']),
                    (record['parent']['latitude'], record['parent']['longitude'])
                ],
                color="#ff0000"
            )



if __name__ == "__main__":
    display_train_network = DisplayTrainNetwork("neo4j://localhost:7687")
    center_switzerland = [46.800663464, 8.222665776]
    display_train_network.display_cities()
    display_train_network.display_lines()
    display_train_network.display_query_on_cities()
    display_train_network.display_shortest_path("trainNetworkGraphTime", "time", "2")
    display_train_network.display_shortest_path("trainNetworkGraphDistance", "km", "1")
    display_train_network.display_minimum_spanning_tree()

