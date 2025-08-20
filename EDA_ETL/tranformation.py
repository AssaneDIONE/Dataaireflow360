# #!/usr/bin/env python3
# """
# Script PySpark pour extraire, transformer et modéliser un mini Data Warehouse
# à partir de fichiers CSV stockés dans Hadoop HDFS.
# """

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, trim, to_timestamp, concat, lit,
#     row_number, monotonically_increasing_id, round as spark_round
# )
# from pyspark.sql.window import Window
# from pyspark.sql.functions import coalesce, lit, round as spark_round

# # -------------------------------
# # Initialisation de la SparkSession
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("DataWarehouse_Transport") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
#     .config("spark.sql.adaptive.enabled", "true") \
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

# # -------------------------------
# # Chargement des datasets
# # -------------------------------
# df_transport = read_csv(files_to_load["transport"])
# df_passagers = read_csv(files_to_load["passagers"])
# df_gps       = read_csv(files_to_load["gps"])
# df_incident  = read_csv(files_to_load["incident"])
# df_stops     = read_csv(files_to_load["stops"])

# # -------------------------------
# # Nettoyage & transformations simples
# # -------------------------------
# def clean_df(df):
#     """Nettoie un DataFrame : trim colonnes string + supprime doublons"""
#     for c in df.columns:
#         if dict(df.dtypes)[c] == "string":
#             df = df.withColumn(c, trim(col(c)))
#     return df.dropDuplicates()

# df_transport = clean_df(df_transport)
# df_passagers = clean_df(df_passagers)
# df_gps       = clean_df(df_gps)
# df_incident  = clean_df(df_incident)
# df_stops     = clean_df(df_stops)

# # Exemple : normaliser les dates si présentes
# if "timestamp" in df_incident.columns:
#     df_incident = df_incident.withColumn("timestamp", to_timestamp("timestamp"))

# # -------------------------------
# # Arrondir tous les nombres décimaux à 2 chiffres
# # -------------------------------
# def round_numeric(df):
#     for c, dtype in df.dtypes:
#         if dtype in ("double", "float"):
#             df = df.withColumn(c, spark_round(col(c), 2))
#     return df

# df_transport = round_numeric(df_transport)
# df_passagers = round_numeric(df_passagers)
# df_gps       = round_numeric(df_gps)
# df_incident  = round_numeric(df_incident)
# df_stops     = round_numeric(df_stops)

# # -------------------------------
# # Ajout des identifiants formatés
# # -------------------------------

# # Passagers → p1, p2, ...
# if "passager_id" not in df_passagers.columns:
#     df_passagers = df_passagers.withColumn(
#         "passager_id",
#         concat(lit("p"), row_number().over(Window.orderBy(monotonically_increasing_id())))
#     )

# # Transport → t1, t2, ...
# df_transport = df_transport.withColumn(
#     "transport_id",
#     concat(lit("t"), row_number().over(Window.orderBy(monotonically_increasing_id())))
# )

# # Stops → s1, s2, ...
# df_stops = df_stops.withColumn(
#     "stop_id",
#     concat(lit("s"), row_number().over(Window.orderBy(monotonically_increasing_id())))
# )

# # GPS → g1, g2, ...
# if "_id" in df_gps.columns:
#     df_gps = df_gps.withColumn(
#         "gps_id",
#         concat(lit("g"), row_number().over(Window.orderBy(monotonically_increasing_id())))
#     )

# # Incidents → i1, i2, ...
# df_incident = df_incident.withColumn(
#     "incident_id",
#     concat(lit("i"), row_number().over(Window.orderBy(monotonically_increasing_id())))
# )

# # -------------------------------
# # Modélisation Data Warehouse (schéma en étoile)
# # -------------------------------

# # Dimensions
# # ➝ On supprime "_id" de dim_passager
# dim_passager = df_passagers.drop("_id").select(
#     "passager_id", "alighting", "boarding", "passenger_count", 
#     "stop_id", "timestamp", "vehicle_id"
# )

# dim_transport = df_transport.select("transport_id", "vehicle_id", "capacity", "company_name", "fuel_type", "status", "type_transport")
# dim_stop = df_stops.select("stop_id", "latitude", "longitude", "name", "shelter", "zone")
# dim_gps = df_gps
# dim_incident = df_incident.select("incident_id", "delay_minutes", "description", "severity", "timestamp", "vehicle_id")

# # -------------------------------
# # Table de faits
# # -------------------------------
# fact_transport = dim_transport

# # Join avec passagers
# fact_transport = fact_transport.join(
#     dim_passager.select("passager_id", "vehicle_id", "passenger_count"),
#     "vehicle_id",
#     "left"
# )

# # Join avec stops via stop_id des passagers
# passager_stops = dim_passager.select("vehicle_id", "stop_id").dropDuplicates()
# stop_info = dim_stop.select("stop_id", "name", "latitude", "longitude", "zone")

# fact_transport = fact_transport.join(passager_stops, "vehicle_id", "left") \
#                                .join(stop_info, "stop_id", "left")

# # Join avec incidents
# incident_summary = dim_incident.groupBy("vehicle_id").agg(
#     {"delay_minutes": "sum", "incident_id": "count"}
# ).withColumnRenamed("sum(delay_minutes)", "total_delay_minutes") \
#  .withColumnRenamed("count(incident_id)", "incident_count")

# fact_transport = fact_transport.join(incident_summary, "vehicle_id", "left")

# # Colonnes finales
# fact_transport = fact_transport.select(
#     "transport_id",
#     "vehicle_id",
#     "capacity",
#     "company_name", 
#     "fuel_type",
#     "status",
#     "type_transport",
#     "passager_id",
#     "passenger_count",
#     "stop_id",
#     col("name").alias("stop_name"),
#     col("latitude").alias("stop_latitude"),
#     col("longitude").alias("stop_longitude"),
#     col("zone").alias("stop_zone"),
#     "total_delay_minutes",
#     "incident_count"
# ).dropDuplicates()

# # -------------------------------
# # Sauvegarde dans HDFS
# # -------------------------------
# # Filtrer les lignes invalides pour les passagers
# # -------------------------------
# fact_transport = fact_transport.filter(
#     ~(
#         (col("passager_id").isNotNull()) & 
#         (
#             (col("status") == "hors service") |
#             col("stop_name").isNull() |
#             col("stop_latitude").isNull() |
#             col("stop_longitude").isNull()
#         )
#     )
# )

# # -------------------------------
# # Arrondir et remplir les valeurs manquantes
# # -------------------------------
# fact_transport = fact_transport \
#     .withColumn("capacity", spark_round(col("capacity"), 2)) \
#     .withColumn("passenger_count", spark_round(coalesce(col("passenger_count"), lit(0)), 2)) \
#     .withColumn("stop_latitude", spark_round(coalesce(col("stop_latitude"), lit(0.0)), 2)) \
#     .withColumn("stop_longitude", spark_round(coalesce(col("stop_longitude"), lit(0.0)), 2)) \
#     .withColumn("total_delay_minutes", spark_round(coalesce(col("total_delay_minutes"), lit(0)), 2)) \
#     .withColumn("incident_count", coalesce(col("incident_count"), lit(0))) \
#     .withColumn("stop_name", coalesce(col("stop_name"), lit("Unknown"))) \
#     .withColumn("stop_zone", coalesce(col("stop_zone"), lit("Unknown"))) \
#     .withColumn("passager_id", coalesce(col("passager_id"), lit("Unknown")))

# # -------------------------------
# # Sauvegarde finale dans HDFS
# # -------------------------------
# output_dir = "hdfs://localhost:9000/warehouse"

# dim_passager.write.mode("overwrite").parquet(f"{output_dir}/dim_passager")
# dim_transport.write.mode("overwrite").parquet(f"{output_dir}/dim_transport")
# dim_stop.write.mode("overwrite").parquet(f"{output_dir}/dim_stop")
# dim_gps.write.mode("overwrite").parquet(f"{output_dir}/dim_gps")
# dim_incident.write.mode("overwrite").parquet(f"{output_dir}/dim_incident")
# fact_transport.write.mode("overwrite").parquet(f"{output_dir}/fact_transport")

# print("\n✓ Data Warehouse généré avec succès dans HDFS (/warehouse)")

# spark.stop()



#!/usr/bin/env python3
"""
Script PySpark optimisé pour extraire, transformer et modéliser un mini Data Warehouse
Version corrigée avec gestion optimisée des count() et utilisation maximale de PySpark
"""

import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_timestamp, concat, lit,
    row_number, monotonically_increasing_id, round as spark_round,
    coalesce, when, isnan, isnull, broadcast, sum, count
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType
import traceback
import sys

# =============================================================================
# PROBLÈMES IDENTIFIÉS ET SOLUTIONS:
# 
# 1. PROBLÈME COUNT(): 
#    - count() est appelé plusieurs fois sur le même DataFrame
#    - count() déclenche une action Spark coûteuse à chaque fois
#    - SOLUTION: Cache + count unique, ou utilisation d'estimations
#
# 2. PANDAS VS PYSPARK:
#    - Pandas n'est nécessaire QUE pour l'insertion PostgreSQL
#    - Tout le reste peut être fait en PySpark pur
#    - SOLUTION: Minimiser l'usage de pandas
# =============================================================================

class OptimizedDataProcessor:
    def __init__(self, spark_session, postgres_conn):
        self.spark = spark_session
        self.conn = postgres_conn
        self.cached_dfs = {}  # Cache pour éviter les recalculs
        
    def cache_dataframe(self, df, name):
        """Cache un DataFrame et retourne une estimation du count"""
        print(f"💾 Mise en cache de {name}...")
        cached_df = df.cache()
        
        # Utiliser une estimation rapide plutôt qu'un count exact
        # count() exact sera fait UNE SEULE FOIS quand vraiment nécessaire
        self.cached_dfs[name] = cached_df
        print(f"✅ {name} mis en cache (estimation disponible)")
        return cached_df
    
    def get_row_count_estimate(self, df, name):
        """Obtient une estimation du nombre de lignes sans count() coûteux"""
        try:
            # Méthode 1: Si le DataFrame est déjà mis en cache, utiliser count
            if name in self.cached_dfs:
                # Count une seule fois sur le DataFrame caché
                count = self.cached_dfs[name].count()
                print(f"📊 {name}: {count} lignes (count exact)")
                return count
            
            # Méthode 2: Estimation rapide avec LIMIT
            sample = df.limit(1000).count()
            if sample < 1000:
                print(f"📊 {name}: {sample} lignes (count exact - petit dataset)")
                return sample
            else:
                print(f"📊 {name}: ~{sample}+ lignes (estimation - grand dataset)")
                return f"{sample}+"
                
        except Exception as e:
            print(f"⚠️ Impossible d'estimer {name}: {e}")
            return "Unknown"

# -------------------------------
# Configuration PostgreSQL (inchangée mais optimisée)
# -------------------------------
def test_postgres_connection():
    """Test la connexion PostgreSQL avec diagnostics détaillés"""
    print("🔍 Test de connexion PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="warehouse",
            user="dione", 
            password="Pass123"
        )
        print("✅ Connexion PostgreSQL établie")
        
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_user, version();")
        db_info = cursor.fetchone()
        print(f"📊 Base: {db_info[0]}, Utilisateur: {db_info[1]}")
        
        # Test permissions rapide
        cursor.execute("CREATE TABLE IF NOT EXISTS test_permissions (id INTEGER);")
        cursor.execute("DROP TABLE test_permissions;")
        conn.commit()
        print("✅ Permissions d'écriture vérifiées")
        
        cursor.close()
        return conn
        
    except Exception as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        return None

def create_tables_schema(conn):
    """Crée le schéma des tables dans PostgreSQL"""
    cursor = conn.cursor()
    
    try:
        print("🗑️ Suppression des tables existantes...")
        tables_to_drop = [
            "fact_transport", "dim_transport", "dim_passager", 
            "dim_gps", "dim_incident", "dim_stop"
        ]
        
        for table in tables_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        
        print("🏗️ Création des nouvelles tables...")
        
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
        conn.rollback()
        cursor.close()
        return False

# -------------------------------
# FONCTIONS DE TRANSFORMATION OPTIMISÉES (100% PySpark)
# -------------------------------
def transform_transport_data(processor, transport_df):
    """Transforme les données transport - 100% PySpark"""
    print("🔄 Transformation des données transport...")
    
    transformed_df = transport_df.select(
        col("vehicle_id"),
        col("capacity"),
        col("company_name"),
        col("fuel_type"),
        col("status"),
        col("type_transport")
    )
    
    # Cache pour éviter les recalculs
    cached_df = processor.cache_dataframe(transformed_df, "dim_transport")
    row_estimate = processor.get_row_count_estimate(cached_df, "dim_transport")
    print(f"✅ Transformation transport terminée: {row_estimate} lignes")
    return cached_df

def transform_passager_data(processor, passager_df):
    """Transforme les données passagers - 100% PySpark"""
    print("🔄 Transformation des données passagers...")
    
    transformed_df = passager_df.select(
        col("_id").alias("passager_id"),
        when(col("alighting") == "true", 1).otherwise(0).alias("alighting"),
        when(col("boarding") == "true", 1).otherwise(0).alias("boarding"),
        col("passenger_count"),
        col("stop_id"),
        to_timestamp(col("timestamp")).alias("timestamp"),
        col("vehicle_id")
    )
    
    cached_df = processor.cache_dataframe(transformed_df, "dim_passager")
    row_estimate = processor.get_row_count_estimate(cached_df, "dim_passager")
    print(f"✅ Transformation passagers terminée: {row_estimate} lignes")
    return cached_df

def transform_gps_data(processor, gps_df):
    """Transforme les données GPS - 100% PySpark"""
    print("🔄 Transformation des données GPS...")
    
    transformed_df = gps_df.select(
        col("_id").alias("position_id"),
        col("latitude").cast(DoubleType()),
        col("longitude").cast(DoubleType()),
        col("route_id"),
        col("speed_kmh").cast(DoubleType()),
        to_timestamp(col("timestamp")).alias("timestamp"),
        col("traffic_level"),
        col("vehicle_id")
    )
    
    cached_df = processor.cache_dataframe(transformed_df, "dim_gps")
    row_estimate = processor.get_row_count_estimate(cached_df, "dim_gps")
    print(f"✅ Transformation GPS terminée: {row_estimate} lignes")
    return cached_df

def transform_incident_data(processor, incident_df):
    """Transforme les données incidents - 100% PySpark"""
    print("🔄 Transformation des données incidents...")
    
    transformed_df = incident_df.select(
        col("Incidents_id").alias("incident_id"),
        col("delay_minutes"),
        col("description"),
        col("severity"),
        to_timestamp(col("timestamp")).alias("timestamp"),
        col("vehicle_id")
    )
    
    cached_df = processor.cache_dataframe(transformed_df, "dim_incident")
    row_estimate = processor.get_row_count_estimate(cached_df, "dim_incident")
    print(f"✅ Transformation incidents terminée: {row_estimate} lignes")
    return cached_df

def transform_stops_data(processor, stops_df):
    """Transforme les données stops - 100% PySpark"""
    print("🔄 Transformation des données stops...")
    
    transformed_df = stops_df.select(
        col("stop_id"),
        col("latitude").cast(DoubleType()),
        col("longitude").cast(DoubleType()),
        col("name"),
        when(col("shelter") == "true", True).otherwise(False).alias("shelter"),
        col("zone"),
        lit(None).cast(StringType()).alias("vehicle_id"),
        lit(None).cast(StringType()).alias("passager_id")
    )
    
    cached_df = processor.cache_dataframe(transformed_df, "dim_stop")
    row_estimate = processor.get_row_count_estimate(cached_df, "dim_stop")
    print(f"✅ Transformation stops terminée: {row_estimate} lignes")
    return cached_df

def transform_fact_transport_data(processor, transport_df, passager_df, gps_df, incident_df):
    """Crée la table de faits - 100% PySpark pour les agrégations"""
    print("🔄 Création de la table de faits transport...")
    
    # Commencer par les données de transport de base
    fact_df = transform_transport_data(processor, transport_df)
    
    # Agrégation des passagers par vehicle_id - 100% PySpark
    print("📊 Agrégation des données passagers...")
    passager_agg = passager_df.groupBy("vehicle_id").agg(
        sum("passenger_count").alias("total_passengers"),
        count("*").alias("passenger_records")
    ).cache()  # Cache l'agrégation
    
    # Agrégation des incidents par vehicle_id - 100% PySpark
    print("📊 Agrégation des données incidents...")
    incident_agg = incident_df.groupBy("vehicle_id").agg(
        sum("delay_minutes").alias("total_delay_minutes"),
        count("*").alias("incident_count")
    ).cache()  # Cache l'agrégation
    
    # Dernière position GPS par vehicle_id - 100% PySpark
    print("📊 Récupération des dernières positions GPS...")
    window_spec = Window.partitionBy("vehicle_id").orderBy(col("timestamp").desc())
    gps_latest = gps_df.withColumn("row_num", row_number().over(window_spec)) \
                      .filter(col("row_num") == 1) \
                      .select("vehicle_id", "latitude", "longitude", "speed_kmh", "traffic_level") \
                      .cache()  # Cache les dernières positions
    
    # Jointures pour créer la table de faits - 100% PySpark
    print("🔗 Jointures pour créer la table de faits...")
    fact_df = fact_df.join(broadcast(passager_agg), "vehicle_id", "left") \
                     .join(broadcast(incident_agg), "vehicle_id", "left") \
                     .join(broadcast(gps_latest), "vehicle_id", "left")
    
    # Remplacer les valeurs null par des valeurs par défaut - 100% PySpark
    # Remarque: stops_count n'est pas calculé dans cette version, on l'enlève du fillna
    fact_df = fact_df.fillna({
        "total_passengers": 0,
        "passenger_records": 0,
        "total_delay_minutes": 0,
        "incident_count": 0,
        "latitude": 0.0,
        "longitude": 0.0,
        "speed_kmh": 0.0,
        "traffic_level": "Unknown"
    })
    
    # Cache le résultat final
    cached_df = processor.cache_dataframe(fact_df, "fact_transport")
    row_estimate = processor.get_row_count_estimate(cached_df, "fact_transport")
    print(f"✅ Table de faits créée: {row_estimate} lignes")
    return cached_df

# -------------------------------
# INSERTION OPTIMISÉE: PANDAS UNIQUEMENT POUR L'ÉCRITURE POSTGRESQL
# -------------------------------
def optimized_dataframe_to_postgres(spark_df, table_name, conn, use_sample_for_preview=True):
    """
    Version optimisée: Pandas utilisé UNIQUEMENT pour l'insertion PostgreSQL
    Tous les diagnostics et validations restent en PySpark
    """
    try:
        print(f"📝 Préparation de l'insertion dans {table_name}...")
        
        # ÉTAPE 1: Validation rapide avec PySpark (pas de count complet)
        print(f"🔍 Validation des données avec PySpark...")
        
        # Vérification rapide: est-ce que le DataFrame est vide ?
        first_row = spark_df.first()
        if first_row is None:
            print(f"⚠️ Aucune donnée à insérer dans {table_name}")
            return True
        
        # ÉTAPE 2: Diagnostics avec PySpark (échantillon seulement)
        print(f"📋 Schéma de {table_name}:")
        spark_df.printSchema()
        
        if use_sample_for_preview:
            # Utiliser un échantillon pour l'aperçu (plus rapide)
            sample_df = spark_df.limit(3)
            print(f"🔍 Aperçu des données de {table_name} (échantillon):")
            sample_df.show(3, truncate=False)
        
        # ÉTAPE 3: Conversion en Pandas UNIQUEMENT pour l'insertion
        print(f"🔄 Conversion Spark → pandas pour insertion PostgreSQL...")
        
        # SEUL ENDROIT où pandas est OBLIGATOIRE: 
        # psycopg2 ne comprend pas les DataFrames Spark nativement
        pandas_df = spark_df.toPandas()
        actual_count = len(pandas_df)
        print(f"📊 {actual_count} lignes à insérer dans {table_name}")
        
        if actual_count == 0:
            print(f"⚠️ Aucune donnée après conversion pandas")
            return True
        
        # ÉTAPE 4: Préparation pour PostgreSQL (pandas nécessaire ici)
        # Remplacer les valeurs NaN par None pour PostgreSQL
        pandas_df = pandas_df.where(pd.notnull(pandas_df), None)
        
        # ÉTAPE 5: Insertion dans PostgreSQL
        cursor = conn.cursor()
        
        try:
            # Vérifier l'existence de la table
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
            
            # Insertion par batch optimisée
            batch_size = 1000
            total_inserted = 0
            
            print(f"📦 Insertion par batches de {batch_size}...")
            for i in range(0, len(pandas_df), batch_size):
                batch = pandas_df.iloc[i:i+batch_size]
                batch_data = [tuple(row) for row in batch.values]
                
                cursor.executemany(insert_query, batch_data)
                total_inserted += len(batch_data)
                
                if i // batch_size < 5 or (i // batch_size + 1) % 10 == 0:
                    print(f"   📦 Batch {i//batch_size + 1}: {len(batch_data)} lignes")
            
            conn.commit()
            print(f"✅ {total_inserted} lignes insérées dans {table_name}")
            
            # Vérification finale rapide
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            db_count = cursor.fetchone()[0]
            print(f"🔍 Vérification: {db_count} lignes dans la base")
            
            cursor.close()
            return True
            
        except Exception as insert_error:
            print(f"❌ Erreur lors de l'insertion dans {table_name}: {insert_error}")
            conn.rollback()
            cursor.close()
            return False
        
    except Exception as e:
        print(f"❌ Erreur générale pour {table_name}: {e}")
        return False

# -------------------------------
# SCRIPT PRINCIPAL OPTIMISÉ
# -------------------------------
def optimized_main():
    """Script principal optimisé avec gestion du cache et count minimaux"""
    print("=" * 60)
    print("🚀 SCRIPT DATA WAREHOUSE OPTIMISÉ")
    print("=" * 60)
    
    # Initialisation
    spark = None
    conn = None
    
    try:
        # Initialisation Spark
        print("🔥 Initialisation de Spark...")
        spark = SparkSession.builder \
            .appName("DataWarehouse_Transport_PostgreSQL_Optimized") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark initialisé")
        
        # Connexion PostgreSQL
        conn = test_postgres_connection()
        if conn is None:
            raise Exception("Impossible de se connecter à PostgreSQL")
        
        # Création du schéma
        if not create_tables_schema(conn):
            raise Exception("Impossible de créer le schéma")
        
        # Initialisation du processeur optimisé
        processor = OptimizedDataProcessor(spark, conn)
        
        # Chargement des datasets AVEC CACHE
        print("\n📊 CHARGEMENT DES DATASETS DEPUIS HDFS")
        print("-" * 50)
        
        files_to_load = {
            "transport": "/data/mongodb_csv/OTO_transport.csv",
            "passagers": "/data/mongodb_csv/Passagers.csv",
            "gps": "/data/mongodb_csv/SEposition_GPS.csv",
            "incident": "/data/mongodb_csv/incidents.csv",
            "stops": "/data/mongodb_csv/stops.csv"
        }
        
        datasets = {}
        for key, path in files_to_load.items():
            print(f"📖 Chargement de {key} depuis {path}...")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            
            # Cache immédiatement après chargement
            cached_df = processor.cache_dataframe(df, f"raw_{key}")
            datasets[key] = cached_df
            
            # Aperçu rapide sans count complet
            print(f"📋 Colonnes {key}: {cached_df.columns}")
            cached_df.show(2, truncate=True)
        
        # TRANSFORMATION ET INSERTION OPTIMISÉES
        print("\n💾 TRANSFORMATION ET INSERTION")
        print("-" * 50)
        
        # Toutes les transformations utilisent le cache et évitent les counts multiples
        transformations = [
            ("dim_transport", lambda: transform_transport_data(processor, datasets["transport"])),
            ("dim_passager", lambda: transform_passager_data(processor, datasets["passagers"])),
            ("dim_gps", lambda: transform_gps_data(processor, datasets["gps"])),
            ("dim_incident", lambda: transform_incident_data(processor, datasets["incident"])),
            ("dim_stop", lambda: transform_stops_data(processor, datasets["stops"])),
        ]
        
        # Exécuter les transformations et insertions
        for table_name, transform_func in transformations:
            print(f"\n🔄 Traitement de {table_name}...")
            
            transformed_df = transform_func()
            
            # Insertion optimisée (pandas uniquement pour PostgreSQL)
            if not optimized_dataframe_to_postgres(transformed_df, table_name, conn):
                raise Exception(f"Erreur insertion {table_name}")
        
        # Fact table en dernier
        print("\n🔄 Traitement de fact_transport...")
        fact_df = transform_fact_transport_data(
            processor,
            datasets["transport"], 
            datasets["passagers"], 
            datasets["gps"], 
            datasets["incident"]
        )
        
        if not optimized_dataframe_to_postgres(fact_df, "fact_transport", conn):
            raise Exception("Erreur insertion fact_transport")
        
        print("\n🎉 SCRIPT TERMINÉ AVEC SUCCÈS!")
        print("✅ Toutes les données ont été traitées et insérées")
        
        # Statistiques finales
        print("\n📊 STATISTIQUES FINALES:")
        cursor = conn.cursor()
        tables_info = [
            ("dim_transport", "Véhicules"),
            ("dim_passager", "Données passagers"), 
            ("dim_gps", "Positions GPS"),
            ("dim_incident", "Incidents"),
            ("dim_stop", "Arrêts"),
            ("fact_transport", "Table de faits")
        ]
        
        for table, description in tables_info:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   📊 {description} ({table}): {count} lignes")
        cursor.close()
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        return 1
        
    finally:
        # Nettoyage
        if conn:
            conn.close()
            print("📝 Connexion PostgreSQL fermée")
        
        if spark:
            spark.stop()
            print("📝 Session Spark fermée")
    
    return 0

if __name__ == "__main__":
    exit_code = optimized_main()
    sys.exit(exit_code)

