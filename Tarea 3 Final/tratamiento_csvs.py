"""
    WENA CABROS SOY EL DIEKU
    FALTA MUCHO ACA AAAAAAAAAAAAA
    HAY QUE ORDENAR Y ESAS MIERDAS eso si ven este mensaje los amo



"""



"""Genera un nuevo archivo csv que tiene solo las columnas en ingles."""


import pandas as pd
from time import time
import gc

#Comienza a leer el tiempo
cabecera = pd.read_csv('steam_reviews.csv',',',nrows=0)#Lee 1 vez el archivo obtener la cabecera
only_english = pd.DataFrame(columns=cabecera.columns.values)#Crea un nuevo DF
rows = 0#Cantidad de filas leidas.
chunk_size = 1000000#Tamaño de filas a leer 
fileN = 1#Veces dividido
for chunk in pd.read_csv('steam_reviews.csv',',',chunksize=chunk_size):
    #chunk.to_csv('files/steam_reviews-' + str(fileN)+'.csv', index=False)#Crea un nuevo archivo con los datos leidos hasta ahora.
    fileN +=1#Suma 1 a la cantidad de archivos creados
    rows+=chunk_size#Suma la cantidad de filas leidas
    only_english = only_english.append(chunk[chunk['language'] == 'english'],ignore_index = True)#Agrega al nuevo csv solo las filas en inglesh

print("Cantidad de diviciones: ",fileN)
print("Total de columnas iteradas: ",rows)
tiempoFinal = time()
tiempoTotal = tiempoFinal - tiempoInicial
print("Tiempo total en leer los datos: ", tiempoTotal)
only_english.to_csv('english-steam-reviews.csv',index=False)#Genera el nuevo archivo
tiempoFinal = time()
tiempoTotal = tiempoFinal - tiempoInicial
print("Tiempo total de ejecucion: ", tiempoTotal)

"""Fin"""

"""Cruzamiento de datos !!! ORDENAR !!! """


tiempoInicial = time()

gfdf = pd.read_csv('games-features.csv', delimiter=",")
esrdf = pd.read_csv('english-steam-reviews.csv', delimiter=",")
esrd_columns = esrdf.columns

#Arreglo auxiliar que obtendra las id unicas de los juegos que hay en steam reviews
idarray = list()
#Agrega todas las id de los juegos que tienen reviews
for i in esrdf['app_id']:
    if( not (i in idarray)):
        idarray.append(i)
print("Cantidad de juegos que tienen Reviews = ", len(idarray))

#Luego liberamos la memoria.
del [[esrdf]]
gc.collect()

#Crea un nuevo dataframe final para el GameFeatures
newGameFeatures = pd.DataFrame(columns=gfdf.columns)
insertedID = []
for i in idarray:
    row = gfdf.query("ResponseID == "+str(i))
    newGameFeatures = newGameFeatures.append(row)
    
newGameFeatures = newGameFeatures.drop_duplicates(subset=['ResponseID'])
newGameFeatures

requiredID = newGameFeatures['ResponseID']
print(len(requiredID))
#print(requiredID)
#print(255710 in requiredID)

esrdf = pd.read_csv('english-steam-reviews.csv', delimiter=",")
esrdf_columns = esrdf.columns

reviewsGameExistInFeatures = pd.DataFrame(columns=esrdf_columns)


for id in requiredID:
    rows = esrdf.query('app_id == '+str(id))
    reviewsGameExistInFeatures = reviewsGameExistInFeatures.append(rows)
reviewsGameExistInFeatures

del [[esrdf]]
gc.collect()

reviewsGameExistInFeatures.to_csv('final-to-analice-feelings-s-reviews.csv',index=False)

toDelete = ['DemoCount','DeveloperCount', 'MovieCount',
                    'PackageCount', 'PublisherCount', 'Background',
                    'DRMNotice', 'HeaderImage', 'LegalNotice',
                    'SupportedLanguages', 'Website', 'ExtUserAcctNotice']

for to in toDelete:
    del newGameFeatures[to]
newGameFeatures
newGameFeatures.to_csv('final-game-features.csv', index=False)