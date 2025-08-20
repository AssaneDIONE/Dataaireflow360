# #!/usr/bin/env python3
# """
# Script PySpark pour extraire, transformer et modéliser un mini Data Warehouse
# à partir de fichiers CSV stockés dans Hadoop HDFS et sauvegarde vers PostgreSQL.
# Gestion des clés primaires et étrangères selon les spécifications métier.
# """

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, trim, to_timestamp, concat, lit,
#     row_number, monotonically_increasing_id, round as spark_round,
#     coalesce, when, isnan, isnull, broadcast
# )
# from pyspark.sql.window import Window
# from pyspark.sql.types import StringType, IntegerType, DoubleType

# # -------------------------------
# # Configuration PostgreSQL
# # -------------------------------
# POSTGRES_CONFIG = {
#     "url": "jdbc:postgresql://localhost:5433/warehouse",
#     "driver": "org.postgresql.Driver",
#     "user": "dione",
#     "password": "Pass123"
# }

# # -------------------------------
# # Initialisation de la SparkSession avec driver PostgreSQL
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("DataWarehouse_Transport_PostgreSQL") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
#     .config("spark.sql.adaptive.enabled", "true") \
#     .config("spark.jars", "/path/to/postgresql-42.x.x.jar") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # -------------------------------
# # Définition des fichiers utiles
# # -------------------------------
# files_to_load = {
#     "transport": "/data/mongodb_csv/OTO_transport.csv",
#     "passagers": "/data/mongodb_csv/Passagers.csv",
#     "gps": "/data/mongodb_csv/SEposition_GPS.csv",
#     "incident": "/data/mongodb_csv/incidents.csv",
#     "stops": "/data/mongodb_csv/stops.csv"
# }

# def read_csv(path):
#     """Lit un CSV depuis HDFS en DataFrame Spark"""
#     return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# def write_to_postgres(df, table_name, mode="overwrite"):
#     """Sauvegarde un DataFrame vers PostgreSQL"""
#     df.write \
#         .format("jdbc") \
#         .option("url", POSTGRES_CONFIG["url"]) \
#         .option("dbtable", table_name) \
#         .option("user", POSTGRES_CONFIG["user"]) \
#         .option("password", POSTGRES_CONFIG["password"]) \
#         .option("driver", POSTGRES_CONFIG["driver"]) \
#         .mode(mode) \
#         .save()

# # -------------------------------
# # Chargement des datasets
# # -------------------------------
# print("📊 Chargement des datasets depuis HDFS...")
# df_transport = read_csv(files_to_load["transport"])
# df_passagers = read_csv(files_to_load["passagers"])
# df_gps       = read_csv(files_to_load["gps"])
# df_incident  = read_csv(files_to_load["incident"])
# df_stops     = read_csv(files_to_load["stops"])

# # -------------------------------
# # Fonctions utilitaires de nettoyage
# # -------------------------------
# def clean_and_handle_nulls(df, exclude_cols=None):
#     """
#     Nettoie un DataFrame : 
#     - Trim les colonnes string
#     - Remplace les valeurs nulles/vides par des valeurs par défaut
#     - Supprime les doublons
#     """
#     if exclude_cols is None:
#         exclude_cols = []
    
#     for c in df.columns:
#         if c not in exclude_cols:
#             if dict(df.dtypes)[c] == "string":
#                 df = df.withColumn(c, trim(col(c)))
#                 # Remplacer les chaînes vides par null puis par une valeur par défaut
#                 df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))
#                 df = df.withColumn(c, coalesce(col(c), lit("Unknown")))
#             elif dict(df.dtypes)[c] in ("double", "float", "int", "bigint"):
#                 # Remplacer les valeurs nulles numériques par 0
#                 df = df.withColumn(c, coalesce(col(c), lit(0)))
    
#     return df.dropDuplicates()

# def round_numeric_columns(df, precision=2):
#     """Arrondit toutes les colonnes numériques à la précision spécifiée"""
#     for c, dtype in df.dtypes:
#         if dtype in ("double", "float"):
#             df = df.withColumn(c, spark_round(col(c), precision))
#     return df

# # -------------------------------
# # Transformation des datasets avec gestion des clés
# # -------------------------------

# print(" Nettoyage et transformation des datasets...")

# # 1. TRANSPORT : vehicle_id comme clé primaire, suppression de _id
# print("   - Transformation OTO_transport.csv")
# df_transport_clean = clean_and_handle_nulls(df_transport)
# df_transport_clean = round_numeric_columns(df_transport_clean)

# # Supprimer _id et s'assurer que vehicle_id n'a pas de nulls
# if "_id" in df_transport_clean.columns:
#     df_transport_clean = df_transport_clean.drop("_id")

# # Filtrer les lignes où vehicle_id est null
# df_transport_clean = df_transport_clean.filter(col("vehicle_id").isNotNull())

# # 2. PASSAGERS : _id → passager_id (pa_1, pa_2, ...), stop_id et vehicle_id comme clés étrangères
# print("   - Transformation Passagers.csv")
# df_passagers_clean = clean_and_handle_nulls(df_passagers)
# df_passagers_clean = round_numeric_columns(df_passagers_clean)

# # Supprimer _id et créer passager_id
# if "_id" in df_passagers_clean.columns:
#     df_passagers_clean = df_passagers_clean.drop("_id")

# # Générer passager_id avec le format pa_1, pa_2, etc.
# window_passager = Window.orderBy(monotonically_increasing_id())
# df_passagers_clean = df_passagers_clean.withColumn(
#     "passager_id",
#     concat(lit("pa_"), row_number().over(window_passager))
# )

# # S'assurer que les clés étrangères ne sont pas nulles
# df_passagers_clean = df_passagers_clean.filter(
#     col("vehicle_id").isNotNull() & col("stop_id").isNotNull()
# )

# # 3. GPS : _id → position_id (po_1, po_2, ...), vehicle_id comme clé étrangère
# print("   - Transformation SEposition_GPS.csv")
# df_gps_clean = clean_and_handle_nulls(df_gps)
# df_gps_clean = round_numeric_columns(df_gps_clean)

# # Supprimer _id et créer position_id
# if "_id" in df_gps_clean.columns:
#     df_gps_clean = df_gps_clean.drop("_id")

# # Générer position_id avec le format po_1, po_2, etc.
# window_gps = Window.orderBy(monotonically_increasing_id())
# df_gps_clean = df_gps_clean.withColumn(
#     "position_id",
#     concat(lit("po_"), row_number().over(window_gps))
# )

# # S'assurer que vehicle_id n'est pas null
# df_gps_clean = df_gps_clean.filter(col("vehicle_id").isNotNull())

# # 4. INCIDENTS : Incidents_id → incident_id (clé primaire), suppression de _id
# print("   - Transformation incidents.csv")
# df_incident_clean = clean_and_handle_nulls(df_incident)
# df_incident_clean = round_numeric_columns(df_incident_clean)

# # Renommer Incidents_id en incident_id et supprimer _id
# if "Incidents_id" in df_incident_clean.columns:
#     df_incident_clean = df_incident_clean.withColumnRenamed("Incidents_id", "incident_id")
# if "_id" in df_incident_clean.columns:
#     df_incident_clean = df_incident_clean.drop("_id")

# # S'assurer que les clés ne sont pas nulles
# df_incident_clean = df_incident_clean.filter(
#     col("incident_id").isNotNull() & col("vehicle_id").isNotNull()
# )

# # Normaliser les timestamps si présents
# if "timestamp" in df_incident_clean.columns:
#     df_incident_clean = df_incident_clean.withColumn("timestamp", to_timestamp("timestamp"))

# # 5. STOPS : stop_id comme clé primaire, ajout de vehicle_id et passager_id comme clés étrangères
# print("   - Transformation stops.csv")
# df_stops_clean = clean_and_handle_nulls(df_stops)
# df_stops_clean = round_numeric_columns(df_stops_clean)

# # Supprimer _id
# if "_id" in df_stops_clean.columns:
#     df_stops_clean = df_stops_clean.drop("_id")

# # S'assurer que stop_id n'est pas null
# df_stops_clean = df_stops_clean.filter(col("stop_id").isNotNull())

# # Ajouter vehicle_id depuis les véhicules existants dans transport
# vehicle_ids = df_transport_clean.select("vehicle_id").distinct().collect()
# vehicle_id_list = [row["vehicle_id"] for row in vehicle_ids]

# # Créer une relation stop -> vehicle_id (distribution cyclique pour exemple)
# if len(vehicle_id_list) > 0:
#     # Ajouter un index aux stops pour la distribution
#     window_stops = Window.orderBy("stop_id")
#     df_stops_with_index = df_stops_clean.withColumn(
#         "row_index", row_number().over(window_stops) - 1
#     )
    
#     # Distribution cyclique des vehicle_id
#     from pyspark.sql.functions import array, lit as spark_lit, element_at
#     vehicle_array = array([spark_lit(vid) for vid in vehicle_id_list])
#     df_stops_clean = df_stops_with_index.withColumn(
#         "vehicle_id",
#         element_at(vehicle_array, (col("row_index") % len(vehicle_id_list)) + 1)
#     ).drop("row_index")

# # Ajouter passager_id depuis les passagers existants (relation logique via stop_id)
# passager_stop_mapping = df_passagers_clean.select("passager_id", "stop_id").distinct()
# df_stops_clean = df_stops_clean.join(
#     passager_stop_mapping, "stop_id", "left"
# )

# # -------------------------------
# # Création des tables de dimensions
# # -------------------------------
# print("  Création des tables de dimensions...")

# # DIM_TRANSPORT : vehicle_id comme clé primaire
# dim_transport = df_transport_clean.select(
#     "vehicle_id", "capacity", "company_name", "fuel_type", "status", "type_transport"
# )

# # DIM_PASSAGER : passager_id comme clé primaire
# dim_passager = df_passagers_clean.select(
#     "passager_id", "alighting", "boarding", "passenger_count", 
#     "stop_id", "timestamp", "vehicle_id"
# )

# # DIM_GPS : position_id comme clé primaire
# dim_gps = df_gps_clean.select(
#     "position_id", "latitude", "longitude", "route_id", 
#     "speed_kmh", "timestamp", "traffic_level", "vehicle_id"
# )

# # DIM_INCIDENT : incident_id comme clé primaire
# dim_incident = df_incident_clean.select(
#     "incident_id", "delay_minutes", "description", 
#     "severity", "timestamp", "vehicle_id"
# )

# # DIM_STOP : stop_id comme clé primaire, avec vehicle_id et passager_id comme clés étrangères
# dim_stop = df_stops_clean.select(
#     "stop_id", "latitude", "longitude", "name", 
#     "shelter", "zone", "vehicle_id", "passager_id"
# ).withColumn("passager_id", coalesce(col("passager_id"), lit("0")))

# # -------------------------------
# # Création de la table de faits
# # -------------------------------
# print(" Création de la table de faits...")

# # FACT_TRANSPORT : Consolidation de toutes les informations
# fact_transport = dim_transport

# # Jointure avec les passagers (agrégation par véhicule)
# passager_agg = dim_passager.groupBy("vehicle_id").agg(
#     {"passenger_count": "sum", "passager_id": "count"}
# ).withColumnRenamed("sum(passenger_count)", "total_passengers") \
#  .withColumnRenamed("count(passager_id)", "passenger_records")

# fact_transport = fact_transport.join(
#     broadcast(passager_agg), "vehicle_id", "left"
# )

# # Jointure avec les arrêts (via vehicle_id)
# stop_agg = dim_stop.groupBy("vehicle_id").agg(
#     {"stop_id": "count"}
# ).withColumnRenamed("count(stop_id)", "stops_count")

# fact_transport = fact_transport.join(
#     broadcast(stop_agg), "vehicle_id", "left"
# )

# # Jointure avec les incidents
# incident_agg = dim_incident.groupBy("vehicle_id").agg(
#     {"delay_minutes": "sum", "incident_id": "count"}
# ).withColumnRenamed("sum(delay_minutes)", "total_delay_minutes") \
#  .withColumnRenamed("count(incident_id)", "incident_count")

# fact_transport = fact_transport.join(
#     broadcast(incident_agg), "vehicle_id", "left"
# )

# # Jointure avec GPS (dernière position par véhicule)
# gps_latest = dim_gps.withColumn(
#     "rn", row_number().over(Window.partitionBy("vehicle_id").orderBy(col("timestamp").desc()))
# ).filter(col("rn") == 1).drop("rn")

# fact_transport = fact_transport.join(
#     gps_latest.select("vehicle_id", "latitude", "longitude", "speed_kmh", "traffic_level"), 
#     "vehicle_id", "left"
# )

# # -------------------------------
# # Nettoyage final de la table de faits
# # -------------------------------
# print(" Nettoyage final et gestion des valeurs nulles...")

# fact_transport = fact_transport.withColumn(
#     "total_passengers", coalesce(col("total_passengers"), lit(0))
# ).withColumn(
#     "passenger_records", coalesce(col("passenger_records"), lit(0))
# ).withColumn(
#     "stops_count", coalesce(col("stops_count"), lit(0))
# ).withColumn(
#     "total_delay_minutes", coalesce(col("total_delay_minutes"), lit(0))
# ).withColumn(
#     "incident_count", coalesce(col("incident_count"), lit(0))
# ).withColumn(
#     "latitude", coalesce(col("latitude"), lit(0.0))
# ).withColumn(
#     "longitude", coalesce(col("longitude"), lit(0.0))
# ).withColumn(
#     "speed_kmh", coalesce(col("speed_kmh"), lit(0.0))
# ).withColumn(
#     "traffic_level", coalesce(col("traffic_level"), lit("Unknown"))
# )

# # Arrondir les valeurs numériques
# fact_transport = round_numeric_columns(fact_transport)

# # Filtrer les véhicules hors service pour éviter les incohérences
# fact_transport = fact_transport.filter(col("status") != "hors service")

# # -------------------------------
# # Sauvegarde dans PostgreSQL
# # -------------------------------
# print(" Sauvegarde des tables dans PostgreSQL...")

# try:
#     # Sauvegarde des dimensions
#     print("   - Sauvegarde dim_transport...")
#     write_to_postgres(dim_transport, "dim_transport")
    
#     print("   - Sauvegarde dim_passager...")
#     write_to_postgres(dim_passager, "dim_passager")
    
#     print("   - Sauvegarde dim_gps...")
#     write_to_postgres(dim_gps, "dim_gps")
    
#     print("   - Sauvegarde dim_incident...")
#     write_to_postgres(dim_incident, "dim_incident")
    
#     print("   - Sauvegarde dim_stop...")
#     write_to_postgres(dim_stop, "dim_stop")
    
#     # Sauvegarde de la table de faits
#     print("   - Sauvegarde fact_transport...")
#     write_to_postgres(fact_transport, "fact_transport")
    
#     print(" Toutes les tables ont été sauvegardées avec succès!")
    
# except Exception as e:
#     print(f" Erreur lors de la sauvegarde: {str(e)}")
#     print("Vérifiez votre configuration PostgreSQL et la connexion réseau.")

# # -------------------------------
# # Affichage des statistiques
# # -------------------------------
# print("\n Statistiques du Data Warehouse:")
# print(f"   - Véhicules de transport: {dim_transport.count()}")
# print(f"   - Passagers: {dim_passager.count()}")
# print(f"   - Positions GPS: {dim_gps.count()}")
# print(f"   - Incidents: {dim_incident.count()}")
# print(f"   - Arrêts: {dim_stop.count()}")
# print(f"   - Faits transport: {fact_transport.count()}")

# print("\n Data Warehouse généré avec succès dans PostgreSQL!")
# print(" Tables créées:")
# print("   - dim_transport (clé: vehicle_id)")
# print("   - dim_passager (clé: passager_id)")
# print("   - dim_gps (clé: position_id)")
# print("   - dim_incident (clé: incident_id)")
# print("   - dim_stop (clé: stop_id)")
# print("   - fact_transport (table de faits centrale)")

# print("\n Pour vous connecter à PostgreSQL et vérifier les données:")
# print(f"   psql -h localhost -p 5433 -U {POSTGRES_CONFIG['user']} -d warehouse")

# spark.stop()


#!/usr/bin/env python3
"""
Script PySpark pour extraire, transformer et modéliser un mini Data Warehouse
à partir de fichiers CSV stockés dans Hadoop HDFS et sauvegarde vers PostgreSQL.
Version améliorée avec meilleure gestion des erreurs et diagnostics.
"""

import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_timestamp, concat, lit,
    row_number, monotonically_increasing_id, round as spark_round,
    coalesce, when, isnan, isnull, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType
import traceback
import sys

# -------------------------------
# Configuration PostgreSQL avec diagnostics améliorés
# -------------------------------
def test_postgres_connection():
    """Test la connexion PostgreSQL avec diagnostics détaillés"""
    print("🔍 Test de connexion PostgreSQL...")
    
    # Test 1: Connexion de base
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="warehouse",
            user="dione", 
            password="Pass123"
        )
        print("✅ Connexion PostgreSQL établie")
        
        # Test 2: Vérification de la base de données
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_user, version();")
        db_info = cursor.fetchone()
        print(f"📊 Base: {db_info[0]}, Utilisateur: {db_info[1]}")
        print(f"📋 Version PostgreSQL: {db_info[2][:50]}...")
        
        # Test 3: Permissions d'écriture
        cursor.execute("CREATE TABLE IF NOT EXISTS test_permissions (id INTEGER);")
        cursor.execute("DROP TABLE test_permissions;")
        conn.commit()
        print("✅ Permissions d'écriture vérifiées")
        
        cursor.close()
        return conn
        
    except psycopg2.OperationalError as e:
        print(f"❌ Erreur de connexion PostgreSQL: {e}")
        print("💡 Vérifiez:")
        print("   - PostgreSQL est démarré: sudo systemctl status postgresql")
        print("   - Port 5433 est ouvert: netstat -tlnp | grep 5433")
        print("   - Base 'warehouse' existe: psql -l")
        print("   - Utilisateur 'dione' a les permissions")
        return None
    except psycopg2.Error as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        return None

def get_postgres_connection():
    """Établit une connexion PostgreSQL avec gestion d'erreurs"""
    return test_postgres_connection()

def create_tables_schema(conn):
    """Crée le schéma des tables dans PostgreSQL avec gestion d'erreurs"""
    cursor = conn.cursor()
    
    try:
        print("🗑️  Suppression des tables existantes...")
        tables_to_drop = [
            "fact_transport", "dim_transport", "dim_passager", 
            "dim_gps", "dim_incident", "dim_stop"
        ]
        
        for table in tables_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            print(f"   - Table {table} supprimée")
        
        print("🏗️  Création des nouvelles tables...")
        
        # Créer les tables de dimensions
        create_queries = {
            "dim_transport": """
                CREATE TABLE dim_transport (
                    vehicle_id VARCHAR(50) PRIMARY KEY,
                    capacity INTEGER,
                    company_name VARCHAR(100),
                    fuel_type VARCHAR(50),
                    status VARCHAR(50),
                    type_transport VARCHAR(50)
                );
            """,
            "dim_passager": """
                CREATE TABLE dim_passager (
                    passager_id VARCHAR(50) PRIMARY KEY,
                    alighting INTEGER,
                    boarding INTEGER,
                    passenger_count INTEGER,
                    stop_id VARCHAR(50),
                    timestamp TIMESTAMP,
                    vehicle_id VARCHAR(50)
                );
            """,
            "dim_gps": """
                CREATE TABLE dim_gps (
                    position_id VARCHAR(50) PRIMARY KEY,
                    latitude DECIMAL(10,8),
                    longitude DECIMAL(11,8),
                    route_id VARCHAR(50),
                    speed_kmh DECIMAL(5,2),
                    timestamp TIMESTAMP,
                    traffic_level VARCHAR(50),
                    vehicle_id VARCHAR(50)
                );
            """,
            "dim_incident": """
                CREATE TABLE dim_incident (
                    incident_id VARCHAR(50) PRIMARY KEY,
                    delay_minutes INTEGER,
                    description TEXT,
                    severity VARCHAR(50),
                    timestamp TIMESTAMP,
                    vehicle_id VARCHAR(50)
                );
            """,
            "dim_stop": """
                CREATE TABLE dim_stop (
                    stop_id VARCHAR(50) PRIMARY KEY,
                    latitude DECIMAL(10,8),
                    longitude DECIMAL(11,8),
                    name VARCHAR(100),
                    shelter BOOLEAN,
                    zone VARCHAR(50),
                    vehicle_id VARCHAR(50),
                    passager_id VARCHAR(50)
                );
            """,
            "fact_transport": """
                CREATE TABLE fact_transport (
                    vehicle_id VARCHAR(50) PRIMARY KEY,
                    capacity INTEGER,
                    company_name VARCHAR(100),
                    fuel_type VARCHAR(50),
                    status VARCHAR(50),
                    type_transport VARCHAR(50),
                    total_passengers INTEGER DEFAULT 0,
                    passenger_records INTEGER DEFAULT 0,
                    stops_count INTEGER DEFAULT 0,
                    total_delay_minutes INTEGER DEFAULT 0,
                    incident_count INTEGER DEFAULT 0,
                    latitude DECIMAL(10,8) DEFAULT 0,
                    longitude DECIMAL(11,8) DEFAULT 0,
                    speed_kmh DECIMAL(5,2) DEFAULT 0,
                    traffic_level VARCHAR(50) DEFAULT 'Unknown'
                );
            """
        }
        
        for table_name, query in create_queries.items():
            cursor.execute(query)
            print(f"   ✅ Table {table_name} créée")
        
        conn.commit()
        cursor.close()
        print("✅ Schéma des tables créé avec succès!")
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la création du schéma: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        conn.rollback()
        cursor.close()
        return False

def dataframe_to_postgres(spark_df, table_name, conn):
    """Convertit un DataFrame Spark en pandas et l'insère dans PostgreSQL avec diagnostics"""
    try:
        row_count = spark_df.count()
        print(f"📝 Traitement de {row_count} lignes pour {table_name}...")
        
        if row_count == 0:
            print(f"⚠️  Aucune donnée à insérer dans {table_name}")
            return True
        
        # Afficher le schéma du DataFrame
        print(f"📋 Schéma de {table_name}:")
        spark_df.printSchema()
        
        # Afficher quelques exemples de données
        print(f"🔍 Aperçu des données de {table_name}:")
        spark_df.show(3, truncate=False)
        
        # Convertir Spark DataFrame en pandas
        print(f"🔄 Conversion Spark → pandas pour {table_name}...")
        pandas_df = spark_df.toPandas()
        
        # Diagnostics sur les données pandas
        print(f"📊 Dimensions pandas: {pandas_df.shape}")
        print(f"📊 Colonnes: {list(pandas_df.columns)}")
        print(f"📊 Types de données:\n{pandas_df.dtypes}")
        
        # Remplacer les valeurs NaN par None pour PostgreSQL
        pandas_df = pandas_df.where(pd.notnull(pandas_df), None)
        
        # Insertion dans PostgreSQL avec transaction
        cursor = conn.cursor()
        
        try:
            # Vérifier que la table existe
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            
            if not cursor.fetchone()[0]:
                print(f"❌ La table {table_name} n'existe pas!")
                return False
            
            # Construire la requête d'insertion
            columns = list(pandas_df.columns)
            placeholders = ','.join(['%s'] * len(columns))
            columns_str = ','.join(columns)
            
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            print(f"📝 Requête d'insertion: {insert_query}")
            
            # Insertion par batch pour de meilleures performances
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(pandas_df), batch_size):
                batch = pandas_df.iloc[i:i+batch_size]
                batch_data = [tuple(row) for row in batch.values]
                
                cursor.executemany(insert_query, batch_data)
                total_inserted += len(batch_data)
                print(f"   📦 Batch {i//batch_size + 1}: {len(batch_data)} lignes insérées")
            
            conn.commit()
            print(f"✅ {total_inserted} lignes insérées dans {table_name}")
            
            # Vérification post-insertion
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            db_count = cursor.fetchone()[0]
            print(f"🔍 Vérification: {db_count} lignes dans la base")
            
            cursor.close()
            return True
            
        except Exception as insert_error:
            print(f"❌ Erreur lors de l'insertion dans {table_name}: {insert_error}")
            print(f"📋 Traceback: {traceback.format_exc()}")
            conn.rollback()
            cursor.close()
            return False
        
    except Exception as e:
        print(f"❌ Erreur générale pour {table_name}: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        return False

# -------------------------------
# Test de connectivité HDFS
# -------------------------------
def test_hdfs_connectivity():
    """Test la connectivité HDFS"""
    print("🔍 Test de connectivité HDFS...")
    
    try:
        # Créer une session Spark temporaire pour tester HDFS
        test_spark = SparkSession.builder \
            .appName("HDFS_Test") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()
        
        # Test de lecture d'un répertoire HDFS
        hadoop_conf = test_spark.sparkContext._jsc.hadoopConfiguration()
        fs = test_spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        
        # Vérifier si le répertoire /data existe
        path = test_spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/data")
        if fs.exists(path):
            print("✅ Connexion HDFS réussie - répertoire /data accessible")
            
            # Lister les fichiers dans /data
            files = fs.listStatus(path)
            print(f"📁 Contenu de /data: {len(files)} éléments")
            for file in files[:5]:  # Afficher les 5 premiers
                print(f"   - {file.getPath().getName()}")
        else:
            print("⚠️  Répertoire /data non trouvé dans HDFS")
        
        test_spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connectivité HDFS: {e}")
        print("💡 Vérifiez:")
        print("   - Hadoop est démarré: jps")
        print("   - HDFS est accessible: hdfs dfs -ls /")
        print("   - Le répertoire /data existe: hdfs dfs -ls /data")
        if 'test_spark' in locals():
            test_spark.stop()
        return False

# -------------------------------
# Initialisation de la SparkSession avec gestion d'erreurs
# -------------------------------
def initialize_spark():
    """Initialise Spark avec gestion d'erreurs"""
    try:
        print("🔥 Initialisation de Spark...")
        spark = SparkSession.builder \
            .appName("DataWarehouse_Transport_PostgreSQL") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark initialisé avec succès")
        
        # Afficher les informations de la session
        print(f"📊 Spark Version: {spark.version}")
        print(f"📊 App Name: {spark.sparkContext.appName}")
        print(f"📊 Master: {spark.sparkContext.master}")
        
        return spark
        
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation de Spark: {e}")
        print("💡 Vérifiez que Spark est correctement installé et configuré")
        return None

# -------------------------------
# SCRIPT PRINCIPAL
# -------------------------------
def main():
    print("=" * 60)
    print("🚀 DÉMARRAGE DU SCRIPT DATA WAREHOUSE")
    print("=" * 60)
    
    # Test 1: Connectivité HDFS
    if not test_hdfs_connectivity():
        print("❌ Impossible de continuer sans HDFS. Arrêt du script.")
        return 1
    
    # Test 2: Initialisation Spark
    spark = initialize_spark()
    if spark is None:
        print("❌ Impossible d'initialiser Spark. Arrêt du script.")
        return 1
    
    # Test 3: Connexion PostgreSQL
    conn = get_postgres_connection()
    if conn is None:
        print("❌ Impossible de se connecter à PostgreSQL. Arrêt du script.")
        spark.stop()
        return 1
    
    # Test 4: Création du schéma
    if not create_tables_schema(conn):
        print("❌ Impossible de créer le schéma. Arrêt du script.")
        conn.close()
        spark.stop()
        return 1
    
    try:
        # Définition des fichiers
        files_to_load = {
            "transport": "/data/mongodb_csv/OTO_transport.csv",
            "passagers": "/data/mongodb_csv/Passagers.csv",
            "gps": "/data/mongodb_csv/SEposition_GPS.csv",
            "incident": "/data/mongodb_csv/incidents.csv",
            "stops": "/data/mongodb_csv/stops.csv"
        }
        
        def read_csv_with_validation(path, name):
            """Lit un CSV avec validation"""
            try:
                print(f"📖 Lecture de {name} depuis {path}...")
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
                count = df.count()
                print(f"✅ {name}: {count} lignes chargées")
                if count > 0:
                    print(f"📋 Colonnes: {df.columns}")
                    df.show(2, truncate=False)
                return df
            except Exception as e:
                print(f"❌ Erreur lors du chargement de {name}: {e}")
                return None
        
        # Chargement des datasets avec validation
        print("\n📊 CHARGEMENT DES DATASETS DEPUIS HDFS")
        print("-" * 50)
        
        datasets = {}
        for key, path in files_to_load.items():
            df = read_csv_with_validation(path, key)
            if df is None:
                print(f"❌ Impossible de charger {key}. Arrêt du script.")
                conn.close()
                spark.stop()
                return 1
            datasets[key] = df
        
        # [Ici vous pouvez ajouter le reste de votre logique de transformation]
        # Pour l'instant, on teste juste l'insertion d'une table simple
        
        print("\n💾 TEST D'INSERTION DANS POSTGRESQL")
        print("-" * 50)
        
        # Test avec la table transport
        transport_sample = datasets["transport"].limit(5)  # Juste 5 lignes pour test
        
        if dataframe_to_postgres(transport_sample, "dim_transport", conn):
            print("✅ Test d'insertion réussi!")
        else:
            print("❌ Test d'insertion échoué!")
        
        print("\n🎉 SCRIPT TERMINÉ")
        
    except Exception as e:
        print(f"❌ Erreur dans le script principal: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        return 1
        
    finally:
        # Nettoyage
        if 'conn' in locals() and conn:
            conn.close()
            print("📝 Connexion PostgreSQL fermée")
        
        if 'spark' in locals() and spark:
            spark.stop()
            print("📝 Session Spark fermée")
    
    return 0

# -------------------------------
# Point d'entrée
# -------------------------------
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)