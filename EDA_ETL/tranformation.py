# #!/usr/bin/env python3
# """
# Script PySpark pour extraire, transformer et modéliser un mini Data Warehouse
# à partir de fichiers CSV stockés dans Hadoop HDFS.
# """

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, trim, to_timestamp

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
# if "date" in df_incident.columns:
#     df_incident = df_incident.withColumn("date", to_timestamp("date"))

# # -------------------------------
# # Modélisation Data Warehouse (schéma en étoile)
# # -------------------------------
# # Dimensions
# dim_passager = df_passagers.withColumnRenamed("id", "passager_id")
# dim_transport = df_transport.withColumnRenamed("id", "transport_id")
# dim_stop = df_stops.withColumnRenamed("id", "stop_id")
# dim_gps = df_gps.withColumnRenamed("id", "gps_id")
# dim_incident = df_incident.withColumnRenamed("id", "incident_id")

# # Fait : Exemple de table de faits reliant transport, passagers, stops et incidents
# fact_transport = df_transport \
#     .join(dim_passager, df_transport["passager_id"] == dim_passager["passager_id"], "left") \
#     .join(dim_stop, df_transport["stop_id"] == dim_stop["stop_id"], "left") \
#     .join(dim_incident, df_transport["incident_id"] == dim_incident["incident_id"], "left") \
#     .select(
#         df_transport["transport_id"],
#         dim_passager["passager_id"],
#         dim_stop["stop_id"],
#         dim_incident["incident_id"],
#         df_transport["distance"],
#         df_transport["duree"],
#         df_transport["cout"]
#     )

# # -------------------------------
# # Sauvegarde des tables transformées en Parquet (format optimisé)
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
Script PySpark pour extraire, transformer et modéliser un mini Data Warehouse
à partir de fichiers CSV stockés dans Hadoop HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, concat, lit, monotonically_increasing_id

# -------------------------------
# Initialisation de la SparkSession
# -------------------------------
spark = SparkSession.builder \
    .appName("DataWarehouse_Transport") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# Définition des fichiers utiles
# -------------------------------
files_to_load = {
    "transport": "/data/mongodb_csv/OTO_transport.csv",
    "passagers": "/data/mongodb_csv/Passagers.csv",
    "gps": "/data/mongodb_csv/SEposition_GPS.csv",
    "incident": "/data/mongodb_csv/incidents.csv",
    "stops": "/data/mongodb_csv/stops.csv"
}

def read_csv(path):
    """Lit un CSV depuis HDFS en DataFrame Spark"""
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# -------------------------------
# Chargement des datasets
# -------------------------------
df_transport = read_csv(files_to_load["transport"])
df_passagers = read_csv(files_to_load["passagers"])
df_gps       = read_csv(files_to_load["gps"])
df_incident  = read_csv(files_to_load["incident"])
df_stops     = read_csv(files_to_load["stops"])

# -------------------------------
# Affichage des schémas pour debug
# -------------------------------
print("=== Schémas des DataFrames ===")
print("df_incident schema:")
df_incident.printSchema()
print("df_transport schema:")
df_transport.printSchema()
print("df_stops schema:")
df_stops.printSchema()

# -------------------------------
# Nettoyage & transformations simples
# -------------------------------
def clean_df(df):
    """Nettoie un DataFrame : trim colonnes string + supprime doublons"""
    for c in df.columns:
        if dict(df.dtypes)[c] == "string":
            df = df.withColumn(c, trim(col(c)))
    return df.dropDuplicates()

df_transport = clean_df(df_transport)
df_passagers = clean_df(df_passagers)
df_gps       = clean_df(df_gps)
df_incident  = clean_df(df_incident)
df_stops     = clean_df(df_stops)

# Exemple : normaliser les dates si présentes
if "timestamp" in df_incident.columns:
    df_incident = df_incident.withColumn("timestamp", to_timestamp("timestamp"))

# -------------------------------
# Ajout d'un identifiant passager artificiel
# -------------------------------
if "passager_id" not in df_passagers.columns:
    df_passagers = df_passagers.withColumn(
        "passager_id",
        concat(lit("p"), monotonically_increasing_id())
    )

# -------------------------------
# Modélisation Data Warehouse (schéma en étoile)
# -------------------------------

# Renommer les colonnes _id AVANT les jointures pour éviter les ambiguïtés
df_transport_clean = df_transport.withColumnRenamed("_id", "transport_id")
df_passagers_clean = df_passagers.withColumnRenamed("_id", "passager_raw_id")
df_stops_clean = df_stops.withColumnRenamed("_id", "stop_raw_id") 
df_incident_clean = df_incident.withColumnRenamed("_id", "incident_raw_id").withColumnRenamed("Incidents_id", "incident_id")

# Dimensions finales
dim_passager = df_passagers_clean.select("passager_id", "passager_raw_id", "alighting", "boarding", "passenger_count", "stop_id", "timestamp", "vehicle_id")
dim_transport = df_transport_clean
dim_stop = df_stops_clean.select("stop_id", "stop_raw_id", "latitude", "longitude", "name", "shelter", "zone")
dim_gps = df_gps.withColumnRenamed("_id", "gps_id") if "_id" in df_gps.columns else df_gps
dim_incident = df_incident_clean.select("incident_id", "incident_raw_id", "delay_minutes", "description", "severity", "timestamp", "vehicle_id")

# -------------------------------
# Table de faits avec jointures corrigées
# -------------------------------

print("\n=== Colonnes après nettoyage ===")
print(f"df_transport_clean: {df_transport_clean.columns}")
print(f"dim_passager: {dim_passager.columns}")
print(f"dim_stop: {dim_stop.columns}")
print(f"dim_incident: {dim_incident.columns}")

# Commencer avec transport comme base
fact_transport = df_transport_clean

# Jointure avec passagers (left join pour garder tous les transports)
if "vehicle_id" in dim_passager.columns:
    fact_transport = fact_transport.join(
        dim_passager.select("passager_id", "vehicle_id", "passenger_count"), 
        "vehicle_id", 
        "left"
    )
    print("✓ Jointure passagers effectuée")

# Jointure avec stops via stop_id des passagers
if "stop_id" in dim_passager.columns:
    # Récupérer les infos de stop depuis la dimension passager
    passager_stops = dim_passager.select("vehicle_id", "stop_id").dropDuplicates()
    stop_info = dim_stop.select("stop_id", "name", "latitude", "longitude", "zone")
    
    fact_transport = fact_transport.join(
        passager_stops, "vehicle_id", "left"
    ).join(
        stop_info, "stop_id", "left"
    )
    print("✓ Jointure stops effectuée")

# Jointure avec incidents sur vehicle_id
if "vehicle_id" in dim_incident.columns:
    incident_summary = dim_incident.groupBy("vehicle_id").agg(
        {"delay_minutes": "sum", "incident_id": "count"}
    ).withColumnRenamed("sum(delay_minutes)", "total_delay_minutes")\
     .withColumnRenamed("count(incident_id)", "incident_count")
    
    fact_transport = fact_transport.join(incident_summary, "vehicle_id", "left")
    print("✓ Jointure incidents effectuée")

# Sélection finale pour éviter les doublons et colonnes ambiguës
fact_transport = fact_transport.select(
    col("transport_id"),
    col("vehicle_id"),
    col("capacity"),
    col("company_name"), 
    col("fuel_type"),
    col("status"),
    col("type_transport"),
    col("passager_id"),
    col("passenger_count"),
    col("stop_id"),
    col("name").alias("stop_name"),
    col("latitude").alias("stop_latitude"),
    col("longitude").alias("stop_longitude"),
    col("zone").alias("stop_zone"),
    col("total_delay_minutes").alias("total_delay_minutes"),
    col("incident_count").alias("incident_count")
).dropDuplicates()

print(f"\n=== Table de faits créée ===")
print("Colonnes finales:", fact_transport.columns)

# -------------------------------
# Sauvegarde des tables transformées en Parquet
# -------------------------------
output_dir = "hdfs://localhost:9000/warehouse"

try:
    dim_passager.write.mode("overwrite").parquet(f"{output_dir}/dim_passager")
    print("✓ dim_passager sauvegardée")
    
    dim_transport.write.mode("overwrite").parquet(f"{output_dir}/dim_transport")
    print("✓ dim_transport sauvegardée")
    
    dim_stop.write.mode("overwrite").parquet(f"{output_dir}/dim_stop")
    print("✓ dim_stop sauvegardée")
    
    dim_gps.write.mode("overwrite").parquet(f"{output_dir}/dim_gps")
    print("✓ dim_gps sauvegardée")
    
    dim_incident.write.mode("overwrite").parquet(f"{output_dir}/dim_incident")
    print("✓ dim_incident sauvegardée")
    
    fact_transport.write.mode("overwrite").parquet(f"{output_dir}/fact_transport")
    print("✓ fact_transport sauvegardée")
    
    print("\n Data Warehouse généré avec succès dans HDFS (/warehouse)")
    
except Exception as e:
    print(f" Erreur lors de la sauvegarde: {e}")

finally:
    spark.stop()