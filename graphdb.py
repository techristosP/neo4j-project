from neo4j import GraphDatabase
import time

class Neo4j:

    def __init__(self, password, uri="bolt://localhost:7687", user="neo4j"):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def queryDB(self, query):
        with self._driver.session() as session:
            # result = session.read_transaction(lambda tx: tx.run(query).data())
            # start_time = time.time()
            # records = session.execute_read(lambda tx: tx.run(query).data())
            result = session.run(query)
            # end_time = time.time()
            # print(f"Query executed in {end_time-start_time} seconds")

            records = result.data()
            runtime = result.consume().result_consumed_after
            print(f"\n> Query executed in {runtime} ms")
            return records
        
    def batch_queryDB(self, function, data, attr, batch_size=2000):
        with self._driver.session() as session:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                session.execute_write(function, batch, attr)


if __name__ == "__main__":
    neo = Neo4j()
    query = "MATCH (u:User)-[]->(t) where u.id=1 RETURN u, t"

    result = neo.queryDB(query)
    print(result)

    neo.close()

