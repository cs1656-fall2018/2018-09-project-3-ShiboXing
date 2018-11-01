from neo4j.v1 import GraphDatabase, basic_auth

#connection with authentication
#driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "cs1656"), encrypted=False)

#connection without authentication
driver = GraphDatabase.driver("bolt://localhost", encrypted=False)

session = driver.session()
transaction = session.begin_transaction()

result = transaction.run("MATCH (people:Person) RETURN people.name LIMIT 10")
for record in result:
   print (record['people.name'])
   
transaction.close()
session.close()