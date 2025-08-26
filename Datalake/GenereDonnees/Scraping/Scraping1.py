import requests
from bs4 import BeautifulSoup
import csv

url= "https://www.transit.land/operators"

# envoie de la requete get à l'url
reponse = requests.get(url)

# converture le HTML en objet consultable
soup= BeautifulSoup(reponse.content, 'html.parser')
soup.prettify()