# spark-tfidf-arxiv

Spark implementation of TF-IDF using MapReduce. The goal of the project is to perform a content-based recommender system for arXiv papers from the Computer Science category and provide useful insights on the author relevance by building a co-authorship graph using GraphFrames.

The project has been developed using:

- Spark and PySpark 2.4.3
- Cassandra 
- GraphFrames
- 
## Run the code

 Assuming Spark, Cassandra and Jupyter Notebook are installed, simply run the following commands

```bash
pip install -r requirements.txt
```
Finally, all the code and experiments are explained in a jupyter notebook
```bash
jupyter-notebook project.ipynb
```
