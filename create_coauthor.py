import pandas as pd
import numpy as np
from itertools import combinations
from collections import defaultdict






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