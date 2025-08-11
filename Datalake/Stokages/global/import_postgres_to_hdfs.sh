#!/bin/bash

# Configuration PostgreSQL
POSTGRES_HOST=db_postgres
POSTGRES_PORT=5433
POSTGRES_DB=postgres
POSTGRES_USER=dione
POSTGRES_PASSWORD=Pass123


# Nom de la table à importer
TABLE_NAME=operators  # <-- remplace par le vrai nom de ta table

# Dossier cible sur HDFS
TARGET_DIR=/user/sqoop/${TABLE_NAME}

# Exporter le mot de passe dans la variable d’environnement
export PGPASSWORD=$POSTGRES_PASSWORD

echo "===== DÉBUT IMPORT SQOOP ====="

# Exécution Sqoop Import
sqoop import \
  --connect jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} \
  --username ${POSTGRES_USER} \
  --password ${POSTGRES_PASSWORD} \
  --table ${TABLE_NAME} \
  --target-dir ${TARGET_DIR} \
  --as-textfile \
  --delete-target-dir \
  --m 1 \
  --driver org.postgresql.Driver \
  --verbose

echo "===== FIN IMPORT SQOOP ====="
