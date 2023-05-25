from neo4j import GraphDatabase
import pandas as pd
from itertools import combinations
from collections import defaultdict
import os

uri ='bolt://localhost:7687'
user = 'neo4j'
password = '09150915'

driver = GraphDatabase.driver(uri, auth=(user,password))


# get author ids in pandas series format
def get_author_id(series):
    # remove comma from names
    authors = series.str.replace(',','')

    # split authors
    authors = authors.str.split(pat=';')

    # for author in authors:
    authors.apply(lambda x : [str(x[i]) for i in range(len(x))] if isinstance(x,str) else x)

    # delete first item in each list
    authors = authors.apply(lambda x: [x[i].split('/') for i in range(len(x)) ] if isinstance(x,list) else x)
    authors = authors.apply(lambda x : [x[i][1:] for i in range(len(x))] if isinstance(x,list) else x)

    # authors = authors.apply(lambda x : [x[i] for i in range(len(x)) if i % 2 != 0])
    author_id = authors.apply(lambda x : [i for elem in x for i  in elem] if isinstance(x, list) else x)

    return author_id


# Create coauthor matrix
def get_coauthor_matrix(author_series):

    # get paper : authors list
    author_ids = get_author_id(author_series)

    coauthor_matrix = defaultdict(lambda: defaultdict(int))

    for _, group in author_ids.items():
        # create a list of author combinations for this article
        if isinstance(group, list):
            author_pairs = combinations(group, 2)
            # update the co-author matrix for each author pair
            for pair in author_pairs:
                coauthor_matrix[pair[0]][pair[1]] += 1
                coauthor_matrix[pair[1]][pair[0]] += 1


    return coauthor_matrix

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

# project graph to neo4j gds
def project_graph(field):
    # project field network to gds library
    with driver.session() as session:
        result = session.run(f'''
        CALL gds.graph.project.cypher(
            '{field}',
            'MATCH (a:Author {{field: "{field}"}}) RETURN id(a) AS id',
            'MATCH (n)-[r:COAUTHOR]->(m) RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships:False}})
        YIELD graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels''')

    num_nodes = result.single()['nodes']

    return num_nodes


# Get degree centrality distribution data for projection in graph
def get_degree_distribution(field):
    with driver.session() as session:
        result = session.run(f'''
        CALL gds.degree.stream('{field}',
        {{orientation: 'UNDIRECTED'}})
        YIELD nodeId, score
        RETURN score AS degreeCentrality
        ORDER BY degreeCentrality;''')

    return result.single()['score']

def main():
    input_dir = '/Users/jiyounglim/Documents/study/공종설 네트워크 분석/files_analyze'
    stats_list = []

    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_dir, file_name)

            # Read the file into a DataFrame
            df = pd.read_csv(file_path, low_memory=False)

            field = file_name.split('.')[0]

            # Perform operations on the DataFrame
            coauthor = get_coauthor_matrix(df['Researcher Ids'])
            create_coauthor_graph(coauthor, f'{field}')
            stats = calculate_graph_stats_for_field(f'{field}')

            # Add mainCategory and subCategory to the stats dictionary

            # Add the stats to the stats_list
            stats_list.append(stats)

    # Create a DataFrame from the stats_list
    stats_df = pd.DataFrame(stats_list)

    # Save file to output path
    output_dir = '/Users/jiyounglim/Documents/study/공종설 네트워크 분석/output'
    base_filename = 'output.xlsx'

    # Check if the base filename already exists in the folder
    if os.path.isfile(os.path.join(output_dir, base_filename)):
        # Increment the filename until a unique name is found
        index = 1
        while True:
            new_filename = f"output-{index}.xlsx"
            if not os.path.isfile(os.path.join(output_dir, new_filename)):
                break
            index += 1
    else:
        new_filename = base_filename

    # Save the output with the new filename
    # Replace this line with your actual code to save the output as 'new_filename'
    result = f"Saving the output as '{new_filename}'..."
    stats_df.to_excel(f'/Users/jiyounglim/Documents/study/공종설 네트워크 분석/output/{new_filename}.xlsx')

    return result
