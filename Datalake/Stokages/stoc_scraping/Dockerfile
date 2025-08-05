FROM python:3.11

# Installer les dépendances système nécessaires pour psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev gcc

# Installer psycopg2-binary (version compilée)
RUN pip install psycopg2-binary

# Copier ton dossier de travail
WORKDIR /scripts
