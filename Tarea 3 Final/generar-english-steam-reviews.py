import pandas as pd
from time import time

tiempoInicial = time()#Comienza a leer el tiempo
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
