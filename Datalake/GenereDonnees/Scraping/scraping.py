import requests
from bs4 import BeautifulSoup
import csv

url= "https://www.transit.land/operators"

# envoie de la requete get à l'url
reponse = requests.get(url)

# converture le HTML en objet consultable
soup= BeautifulSoup(reponse.content, 'html.parser')
soup.prettify()

tableaux= soup.find_all('div', class_="table-container")
#print(Tableau)

donnees_csv = []

# Parcours chaque tableau trouvé
for tableau in tableaux:
    # Parcours chaque ligne du tableau
    lignes = tableau.find_all("tr")
    for ligne in lignes:
        cellules = ligne.find_all(["th", "td"])
        valeurs = [cell.get_text(strip=True) for cell in cellules]
        donnees_csv.append(valeurs)  # On accumule dans une liste

# Étape 5 : Sauvegarder dans un fichier CSV une fois toutes les données collectées
with open("resultat.csv", "w", newline="", encoding="utf-8") as fichier_csv:
    writer = csv.writer(fichier_csv)
    writer.writerows(donnees_csv)  # Écrit toutes les lignes en une seule fois

   