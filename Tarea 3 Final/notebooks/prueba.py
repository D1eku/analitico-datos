import pandas as pd
from time import time
import gc

def tiempoConvSTR(tSegundos):
    hor=(int(tSegundos/3600))
    minu=int((tSegundos-(hor*3600))/60)
    seg=tSegundos-((hor*3600)+(minu*60))
    return str(hor)+"h "+str(minu)+"m "+str(seg)+"s"

tiempoInicialCruzamiento = time()

tiempoInicialLectura = time()

#carga los archivos
gfdf = pd.read_csv('games-features.csv', delimiter=",")
esrdf = pd.read_csv('english-steam-reviews.csv', delimiter=",")

#Printea cuanto tiempo tomo esa accion.
tiempoFinalLectura = time() - tiempoInicialLectura
print("Tiempo de demora en leer los archivos: ", tiempoConvSTR(tiempoFinalLectura) )

#Clasification Review 
tiempoInicialClasificacion = time()

from numpy import string_

goodW = ['Great','Good','Favorite','Exciting','Fine','Best','Favourite','Excellent','Nice','Popular','Splendid'
,'Fascinating','Wonderful','Beautiful','Sweet','Magnificent','Fantastic','Complete','Balanced','Worthy','Fun']

badW = ['Terrible','Terribly','Suck','Sucks','Poor','Poorest','Stupid','Shit','Bad','Worst','Worse','Disgusting','Nasty','Appalling','Hopeless'
,'Horrible','Fool','Foolish','Idiot','Fatal','Expensive','Vulgar','Useless','Unconventional','Ugly','Troublesome','Annoying','Bothering']

def contarBuenas(oracion):
    cont = 0
    for i in range(len(goodW)):
        cont += oracion.count(goodW[i].lower())
    return cont

def contarMalas(oracion):
    cont = 0
    for i in range(len(badW)):
        cont += oracion.count(badW[i].lower())
    return cont   

def clasificarRev(contBuenas, contMalas):
    if contBuenas > contMalas:
        return 'Positive'
    elif contBuenas < contMalas:
        return 'Negative'
    else:
        return 'Neutral'

def agregarDato(rev):
    rev = str(rev)
    contGood = contarBuenas(rev.lower())
    contBad = contarMalas(rev.lower())  
    result = clasificarRev(contGood, contBad)
    return result

esrdf['clasification_reviews'] = esrdf['review'].transform(agregarDato)
esrdf

tiempoTotalClasificacion = time() - tiempoInicialClasificacion
print("Tiempo total de clasificacion: ", tiempoConvSTR(tiempoTotalClasificacion))

tiempoCambioFechas = time()

from datetime import datetime

def changeDate(timestap):
    return datetime.fromtimestamp(int(timestap))

esrdf['timestamp_created'] = esrdf['timestamp_created'].transform(changeDate)
esrdf['timestamp_updated'] = esrdf['timestamp_updated'].transform(changeDate)
tiempoFinalCambioFechas = time() - tiempoCambioFechas
print("Tiempo total de cambio de fechas: ", tiempoConvSTR(tiempoFinalCambioFechas))

esrdf_columns = esrdf.columns#Obten las columnas del steamReviews

#Empieza a contar un nuevo tiempo, de cuanto tomo reducir el gameFeatures.
tiempoInicialReduccion = time()

#Arreglo auxiliar que obtendra las id unicas de los juegos que hay en steam reviews
idarray = list()
#Agrega todas las id unicas de los juegos de steam reviews
for i in esrdf['app_id']:
    if( not (i in idarray)):#Si alguna id no esta en el arreglo de ids 
        idarray.append(i)#Agregala

print("Cantidad de juegos que tienen Reviews: ", len(idarray))

#Crea un nuevo dataframe final para el GameFeatures
newGameFeatures = pd.DataFrame(columns=gfdf.columns)
insertedID = []
for i in idarray:
    row = gfdf.query("ResponseID == "+str(i))
    newGameFeatures = newGameFeatures.append(row)
    
newGameFeatures = newGameFeatures.drop_duplicates(subset=['ResponseID'])

#Columnas que se eliminaran
toDelete = ['DemoCount','DeveloperCount', 'MovieCount',
                    'PackageCount', 'PublisherCount', 'Background',
                    'DRMNotice', 'HeaderImage', 'LegalNotice',
                    'SupportedLanguages', 'Website', 'ExtUserAcctNotice']

for to in toDelete:
    del newGameFeatures[to]
    
tiempoFinalReduccion1 = time() - tiempoInicialReduccion
print("Tiempo que tomo reducir el archivo GameFeatures: ", tiempoConvSTR(tiempoFinalReduccion1))

#Crea un nuevo archivo csv y muestra cuanto tiempo tomo eso.
tiempoInicialEscribirGameFeaturesCSV = time()
newGameFeatures.to_csv('final-game-features.csv', index=False)#Escribe el archivo
tiempoFinalEscribirGameFeaturesCSV = time() - tiempoInicialEscribirGameFeaturesCSV
print("Tiempo de escritura de nuevo GameFeaturesCSV: ", tiempoConvSTR(tiempoFinalEscribirGameFeaturesCSV))
tiempoTotalReduccionArchivoGameFeatures = time() - tiempoInicialReduccion 
print("Tiempo total de ejecucion para tratar y guardar GameFeatures: ", tiempoConvSTR(tiempoTotalReduccionArchivoGameFeatures))
requiredID = newGameFeatures['ResponseID']#Id de los juegos que se seleccionaran en el steam reviews
print("Cantidad de juegos disponibles en GameFeaturesFinal: ",len(requiredID))

#Ahora trataremos el steamReviews
tiempoInicialTratarSteamReviews = time()

reviewsGameExistInFeatures = pd.DataFrame(columns=esrdf_columns)
for id in requiredID:
    rows = esrdf.query('app_id == '+str(id))
    reviewsGameExistInFeatures = reviewsGameExistInFeatures.append(rows)
    
toDeleteReviews = ['Unnamed: 0', 'weighted_vote_score', 'author.last_played']
for to in toDeleteReviews:
    del reviewsGameExistInFeatures[to]

#Libera la memoria.
del [[esrdf]]
gc.collect()

tiempoFinalTratarSteamReviews = time() - tiempoInicialTratarSteamReviews
print("Tiempo que tomo generar nuevo data frame con las reviews de los juegos que existen en game features: ", tiempoConvSTR(tiempoFinalTratarSteamReviews))


tiempoInicialEscribirReviews = time()
reviewsGameExistInFeatures.to_csv('final-to-analice-feelings-s-reviews.csv',index=False)
tiempoFinalEscribirReviews = time() - tiempoInicialEscribirReviews
print("Tiempo que tomo escribir el nuevo steam reviews: ", tiempoConvSTR(tiempoFinalEscribirReviews))

tiempoFinalTotalSteamReviews = time() - tiempoInicialTratarSteamReviews
print("Tiempo total de tratamiento a steam reviews: ", tiempoConvSTR(tiempoFinalTotalSteamReviews))
tiempoReduccion2 = time() - tiempoInicialReduccion
print("Tiempo total reduccion ambos archivos: ", tiempoConvSTR(tiempoReduccion2))

#Libera la memoria.
del [[reviewsGameExistInFeatures]]
del [[newGameFeatures]]
gc.collect()

tiempoFinal = time()
tiempoTotal = tiempoFinal - tiempoInicialCruzamiento
print("Tiempo total de ejecucion: ", tiempoConvSTR(tiempoTotal))
