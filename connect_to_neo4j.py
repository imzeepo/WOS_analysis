import pandas as pd
from neo4j import GraphDatabase


uri ='bolt://localhost:7687'
user = 'neo4j'
password = '09150915'

driver = GraphDatabase.driver(uri, auth=(user,password))


# create coauthor graph from
def create_coauthor_graph(coauthor_dict, field):

    df = pd.DataFrame(coauthor_dict)
    df.fillna(0, inplace=True)
    authors = df.index
    # make author nodes
    with driver.session() as session:
        for author in authors:
            session.run("MERGE (a:Author {name: $name, field : $field})", name=author, field=field)

    # make edges with weights
    with driver.session() as session:
        for i in range(len(authors)) :
            for j in range(i+1, len(authors)):
                weight = df.iloc[i,j]
                if weight > 0:
                    session.run("MATCH (a1:Author {name: $name1}), (a2:Author {name: $name2}) MERGE (a1)-[:COAUTHOR {weight: $weight}]->(a2)", name1=authors[i], name2=authors[j], weight=weight)

    with driver.session() as session :
        result = session.run('''
        MATCH (n:Author {field: $field}) RETURN count(n) as num_nodes
        ''',field=field)
        num_nodes = result.single()['num_nodes']

    return num_nodes

# Define a function to calculate graph statistics for each field
def calculate_graph_stats_for_field(field):

    # get number of nodes (i.e. number of authors)
    with driver.session() as session :
        result = session.run(f'''
            MATCH (n:Author {{field: "{field}"}}) RETURN count(n) as num_nodes
            ''')

        num_authors = result.single()['num_nodes']


    # get number of edges (i.e. number of coauthor relationships
    with driver.session() as session :
        result = session.run(f'''
        MATCH p=({{field: "{field}"}})-[r:COAUTHOR]->({{field: "{field}"}}) RETURN count(r) as num_edges
        ''')
        num_edges = result.single()['num_edges']


    # project field network to gds library
    with driver.session() as session :
        session.run(f'''
        CALL gds.graph.project.cypher(
            '{field}',
            'MATCH (a:Author {{field: "{field}"}}) RETURN id(a) AS id',
            'MATCH (n)-[r:COAUTHOR]->(m) RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships:False}})
        YIELD graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels''')


    # run FastRP algorithm for embedding graph
    with driver.session() as session :
        result = session.run(f'''
            CALL gds.fastRP.mutate('{field}',{{
                embeddingDimension: 10,
                randomSeed: 42,
                mutateProperty: 'embedding'}})
            YIELD nodePropertiesWritten
        ''')
        num_author_RP = result.single()['nodePropertiesWritten']


    # get mean degree centrality value for graph
    with driver.session() as session:
        result = session.run(f'''
        CALL gds.degree.stats('{field}')
        YIELD centralityDistribution
        RETURN centralityDistribution.max AS minimumScore, centralityDistribution.mean AS meanScore''')

        max_centrality, mean_centrality = result.single()


    # get number of cluster components in graph - louvain algorithm
    with driver.session() as session :
        result = session.run(f'''
        CALL gds.louvain.stats('{field}')
        YIELD communityCount''')

        community_count = result.single()['communityCount']


    # get number of authors in the largest community of the graph
    with driver.session() as session :
        result = session.run(f'''
        CALL gds.louvain.stream('{field}') YIELD nodeId, communityId
        WITH communityId, count(*) AS num_authors
        ORDER BY num_authors DESC
        LIMIT 1
        RETURN num_authors''')

        num_authors_in_main_component = result.single()['num_authors']


    output = {'field' : field,
              'num_authors' : num_authors,
              'num_authors_FastRP' : num_author_RP,
              'num_edges' : num_edges,
              'degree_centrality_max' : max_centrality,
              'degree_centrality_mean' : mean_centrality,
              'num_community' : community_count,
              'num_authors_in_main_component': num_authors_in_main_component
              }

    return output
