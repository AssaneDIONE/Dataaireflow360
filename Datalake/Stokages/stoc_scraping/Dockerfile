FROM python:3.11

# Installer les dépendances système nécessaires pour psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev gcc

# Installer les dépendances Python
RUN pip install psycopg2-binary pymongo

# Définir le répertoire de travail
WORKDIR /app

# Copier les scripts dans l'image Docker
COPY . /app

# Commande par défaut
CMD ["python", "mongo-to-hdfs.py"]
