from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", encrypted=False)

session = driver.session()
transaction = session.begin_transaction()
f = open("output.txt", "w+")

result = transaction.run("""
    match (a:Actor)-[:ACTS_IN]->(m:Movie) 
    with count(m) as num,a as a return a.name,num 
    order by num desc 
    limit 20""")

f.write("### Q1 ###\n")
for x in result:
    f.write(x['a.name']+", "+str(x['num'])+'\n')

#todo: fix: at least one review less than 3 stars
f.write("\n### Q2 ###\n")
result = transaction.run("""
    match (p:Person)-[r:RATED]->(m:Movie) 
    where r.stars<4 
    return m.title""")

for x in result:
    f.write(x['m.title']+'\n')

result = transaction.run("""
    match (p:Person)-[r:RATED]->(m:Movie)<-[:ACTS_IN]-(a:Actor)
    with m as m, count(distinct a) as cast return m.title, cast
    order by cast desc limit 1
""")

f.write("\n### Q3 ###\n")
for x in result:
    f.write(x['m.title']+": "+str(x['cast'])+'\n')

result = transaction.run("""
    match (a:Actor)-[:ACTS_IN]->(m:Movie)<-[:DIRECTED]-(d:Director) 
    with size(collect(distinct d.name)) as num,a as a 
    where num>=3 
    return a.name,num""")

f.write("\n### Q4 ###\n")
for x in result:
    f.write(x['a.name']+", "+str(x['num'])+'\n')



result=transaction.run("""
    match (bacon:Actor {name:"Kevin Bacon"})-[:ACTS_IN]->(m:Movie)<-[:ACTS_IN]-(a:Actor)
    -[:ACTS_IN]->(m2:Movie)<-[:ACTS_IN]-(a2:Actor) 
    return a2.name
""")

f.write("\n### Q5 ###\n")

for x in result:
    f.write(str(x['a2.name'])+'\n')

result=transaction.run("""
    match (p:Person {name:'Tom Hanks'})-[:ACTS_IN]->(m:Movie) 
    with collect(distinct m.genre) as res
    return res
""")

f.write("\n### Q6 ###\n")

for x in result:
    f.write(str(x['res'])+'\n')


result=transaction.run("""
    match (d:Director)-[:DIRECTED]->(m:Movie) 
    with count(distinct m.genre) as num_g,d as d 
    where num_g>1 
    return d.name,num_g order by num_g 
""")

f.write("\n### Q7 ###\n")

for x in result:
    f.write(str(x['d.name'])+", "+str(x['num_g'])+'\n')

result=transaction.run("""
    match (d:Director)-[:DIRECTED]->(m:Movie)<-[:ACTS_IN]-(a:Actor)  
    with a as a1,d as d1 match (d1)-[:DIRECTED]->(m1:Movie)<-[:ACTS_IN]-(a1)  
    return d1.name,a1.name,count(distinct m1) 
    order by count(distinct m1) desc limit 5
""")


f.write("\n### Q8 ###\n")
for x in result:
    f.write(str(x['d1.name'])+", "+str(x['a1.name'])+", "+str(x['count(distinct m1)'])+'\n')



f.close()
transaction.close()
session.close()

