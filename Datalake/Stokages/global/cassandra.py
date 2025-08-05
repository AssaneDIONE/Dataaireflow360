from cassandra.cluster import Cluster
import csv

# Connexion à Cassandra
cluster = Cluster(['cassandra'])
session = cluster.connect()

# Créer le keyspace (si pas existant)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS testks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
""")
session.set_keyspace('testks')

# Créer la table
session.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY,
        name text,
        email text
    );
""")

# Lire le CSV et insérer les données
with open('/Dataaireflow360/Datalake/Stokages/global/users.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        session.execute("""
            INSERT INTO users (id, name, email) VALUES (uuid(), %s, %s)
        """, (row['name'], row['email']))

print("Données insérées dans Cassandra avec succès.")
