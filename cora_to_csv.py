'''
Source: https://relational.fit.cvut.cz/dataset/CORA
Convert cora dataset which is published as a sort of database to into csv files.
'''

import os
import concurrent.futures
import pandas as pd
import sqlalchemy

# database credentials
USER="guest"
PASSWORD="relational"
HOST="relational.fit.cvut.cz"
PORT=3306
DATABASE="CORA"
COUNT_LABELED_WORD=1434 # capacity of the labeled words for each publication

engine = sqlalchemy.create_engine(f"mariadb://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}",
    pool_size=10, max_overflow=20)


# Convert cites table into edges.csv. cited_paper_id is target node. citing_paper_id is source node.
if not os.path.exists("dataset"):
    os.mkdir(os.path.join(os.getcwd(), "dataset"), mode=0o777)
df_edges = pd.read_sql("select * from cites", engine)
df_edges.to_csv("dataset/edges.csv", index=False)

# Extract the node features from both paper and content tables. Words which are stored in content table are gathered based on paper_id type.
# At the end, each paper_id has array of words. At the end, those paper properties are gathered and converted into nodes.csv.

# Note: Operation can take a little long time (should be less than a minute with 4 CPU cores) because of the processing of more ~1500 words for each paper.

# supposed that given paper_id is unique for each call
def extract(paper_id, word_count, gd):
    words_query = "select word_cited_id from content where paper_id = %s"
    df_words = pd.read_sql_query(words_query, engine, params=list((paper_id,)))
    gd[paper_id] = [0]*word_count
    for word in df_words['word_cited_id']:
        word_id = (lambda x: int(x.split("word")[1]))(word)
        gd[paper_id][word_id] = 1

d = {}  # dictionary which refers {paper_id: [words]}
supportedWordCount = COUNT_LABELED_WORD
df_unique_content = pd.read_sql("select distinct paper_id from content", engine)
with concurrent.futures.ThreadPoolExecutor(len(df_unique_content)) as executor:
    futures = []
    for pid in df_unique_content['paper_id']:
        params = [pid, supportedWordCount, d]
        executor.submit(lambda p: extract(*p), params)
        futures.append(executor.submit(lambda p: extract(*p), params))
    for future in concurrent.futures.as_completed(futures):
        _ = future.result()

df_paper = pd.read_sql("select * from paper", engine)
paper = list(zip(df_paper.paper_id, df_paper.class_label))
data = [{'paper_id': paper_id, 'subject': subject, 'words': d[paper_id]} for paper_id, subject in paper]
df_nodes = pd.DataFrame(data, columns=['paper_id', 'subject', 'words'])
df_nodes.to_csv("dataset/nodes.csv", index=False)

