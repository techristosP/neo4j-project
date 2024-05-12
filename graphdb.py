from neo4j import GraphDatabase

class Neo4j:

    def __init__(self, password, uri="bolt://localhost:7687", user="neo4j"):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._driver.verify_connectivity()

    def close(self):
        """
        Closes the connection to the Neo4j database.
        """
        self._driver.close()

    def queryDB(self, query):
        """
        Executes a query on the Neo4j database and returns the result.

        Args:
            query (str): The Cypher query to be executed.

        Returns:
            list: A list of records returned by the query.

        Raises:
            neo4j.exceptions.Neo4jError: If there is an error executing the query.

        """
        with self._driver.session() as session:
            result = session.run(query)

            records = result.data()
            runtime = result.consume().result_consumed_after
            print(f"\n> Query executed in {runtime} ms")
            return records
        
    def batch_queryDB(self, function, data, attr, batch_size=2000):
        """
        Executes a batch query on the database.

        Args:
            function (str): The name of the function to execute.
            data (list): The data to be processed in batches.
            attr (dict): The attribute to be passed to the function.
            batch_size (int, optional): The size of each batch. Defaults to 2000.
        """
        with self._driver.session() as session:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                session.execute_write(function, batch, attr)


if __name__ == "__main__":
    neo = Neo4j(password="moocmooc")
    query = "MATCH (u:User)-[]->(t) where u.id=1 RETURN u, t"

    result = neo.queryDB(query)
    print(result)

    neo.close()

