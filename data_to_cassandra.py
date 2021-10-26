from cassandra.cluster import Cluster


class Cassandra:

	def __init__(self):
		self.session, self.cluster = self.cassandra_connection()

	def cassandra_connection(self):
		# host.docker.internal
		cluster = Cluster(['127.0.0.1'], port=9042)
		session = cluster.connect()
		session.execute("""
	        CREATE KEYSPACE IF NOT EXISTS papers_space
	        WITH REPLICATION =
	        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
	        """)
		session.set_keyspace('papers_space')
		return session, cluster


	def create_database(self):
		self.session.execute(
			"CREATE TABLE IF NOT EXISTS papers ( pid text, title text, abstract text, authors list<text>, PRIMARY KEY ( pid) );")

		self.session.execute(
			"CREATE TABLE IF NOT EXISTS pagerank_table ( id text, pagerank float, PRIMARY KEY (id));")

		self.session.execute(
			"CREATE TABLE IF NOT EXISTS community_table ( id text, label int, PRIMARY KEY (id));")

		self.session.execute(
			"CREATE TABLE IF NOT EXISTS similarity_table ( doc1 text, doc2 text, score float, PRIMARY KEY (doc1, doc2));")

	def dict_to_cassandra(self, db):

		for i, (k, v) in enumerate(db.items()):
			title = v['title']
			abstract = v['summary']
			authors = []
			for a in v['authors']:
				authors.append(a['name'])

			self.insert_element('papers', k, title, abstract, authors)


	def insert_element(self, table, pid, title, abstract, authors):
		self.session.execute(
			f"""
			INSERT INTO {table} (pid, title, abstract, authors)
			VALUES (%s, %s, %s, %s)
			""",
			(pid, title, abstract, authors)
		)

	def db_to_cassandra(self, db):
		self.create_database()
		self.dict_to_cassandra(db)

