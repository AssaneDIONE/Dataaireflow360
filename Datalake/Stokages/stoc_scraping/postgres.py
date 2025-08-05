import psycopg2
import csv

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname= "db_postgres",
    user= "dione",
    password= "Pass123",
    host= "localhost",
    port= "5433"
)
cursor = conn.cursor()

# Créer la table 'operators' si elle n'existe pas
cursor.execute("""
    CREATE TABLE IF NOT EXISTS operators (
        id SERIAL PRIMARY KEY,
        operator_name TEXT,
        city TEXT,
        state_province TEXT,
        country TEXT
    );
""")
conn.commit()

# Lire le CSV et insérer les données
with open('Datalake/GenereDonnees/Scraping/resultat.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cursor.execute("""
            INSERT INTO operators (operator_name, city, state_province, country)
            VALUES (%s, %s, %s, %s)
        """, (
            row['Operator Name (Short Name)'],
            row['City'],
            row['State / Province'],
            row['Country']
        ))

conn.commit()
cursor.close()
conn.close()

print("Données insérées dans PostgreSQL avec succès.")