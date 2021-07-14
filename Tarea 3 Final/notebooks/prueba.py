import pandas as pd
from time import time
import sqlalchemy as sa
from sqlalchemy.ext import declarative
from uuid import uuid4
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2 as pg
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
#del [[reviewsGameExistInFeatures]]
#del [[newGameFeatures]]
#gc.collect()

def uploadToDB(newGameFeatures, reviewsGameExistInFeatures):

    conn = pg.connect(host='localhost', user='postgres', password='postgres')  
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("select exists (select * from pg_catalog.pg_database WHERE datname = 'etl3');")
    existe = cur.fetchone()[0]

    if(bool(existe)):#Si existe la base de datos eliminala
            cur.execute("commit")
            cur.execute("""SELECT
                                pg_terminate_backend (pg_stat_activity.pid)
                            FROM
                                pg_stat_activity
                            WHERE
                                pg_stat_activity.datname = 'etl3';
                        """)#Desconecta a los clientes
            cur.execute('drop database etl3')#Elimina la database
            
        #Crea la base de datos
    #Crea la base de datos

    cur.execute('create database etl3')

    engine = sa.create_engine('postgresql://postgres:postgres@localhost/etl3')
    conn = engine.connect()
    conn.execute('commit')

    #Sube el game features
    newGameFeatures.to_sql('game_dimension', conn, if_exists='append', index=False)
    #Libera esa memoria
    del [[newGameFeatures]]
    gc.collect()


    #Sube el author_dimension
    author_dimension = reviewsGameExistInFeatures[[
    'author.steamid', 'author.num_games_owned', 'author.num_reviews',
       'author.playtime_forever', 'author.playtime_last_two_weeks',
       'author.playtime_at_review','steam_purchase', 'received_for_free']]

    author_dimension.columns = ['steamid', 'num_games_owned', 'num_reviews',
       'playtime_forever', 'playtime_last_two_weeks',
       'playtime_at_review','steam_purchase', 'received_for_free']

    author_dimension = author_dimension.drop_duplicates(subset=['steamid'])#Elimina los duplicados.
    #Sube los datos a la db.
    author_dimension.to_sql('author_dimension', conn, if_exists='append', index=False, chunksize=550000)
    #Libera esa memoria
    del [[author_dimension]]
    gc.collect()

    reviewFact = reviewsGameExistInFeatures[[ 'review_id', 'language', 'review', 'clasification_reviews',
    'recommended', 'votes_helpful', 'votes_funny' , 'comment_count',
    'steam_purchase', 'received_for_free', 'written_during_early_access',
    'timestamp_created', 'timestamp_updated', 'app_id', 'author.steamid']]

    reviewFact.columns = ['review_id', 'language', 'review', 'clasification_reviews',
        'recommended', 'votes_helpful', 'votes_funny' , 'comment_count',
        'steam_purchase', 'received_for_free', 'written_during_early_access',
        'datetime_created', 'datetime_updated', 'game_id', 'author_id']


    reviewFact = reviewFact.drop_duplicates(subset=['review_id'])#Elimina los duplicados.
    #Subelo a la db
    reviewFact.to_sql('review_fact', conn, if_exists='append', index=False, chunksize=350000)
    #Libera esa memoria
    del [[reviewFact]]
    gc.collect()


    #Agrega constraint a la BD.

    queryConstrait = """ALTER TABLE game_dimension
                        ADD CONSTRAINT pk PRIMARY KEY ("QueryID");

                        ALTER TABLE author_dimension
                        ADD CONSTRAINT pk1 PRIMARY KEY ("steamid");

                        ALTER TABLE review_fact
                        ADD CONSTRAINT pk2 PRIMARY KEY ("review_id");


                        ALTER TABLE review_fact
                        ADD CONSTRAINT fk1 FOREIGN KEY (game_id)
                        REFERENCES game_dimension("QueryID");

                        ALTER TABLE review_fact
                        ADD CONSTRAINT fk2 FOREIGN KEY (author_id)
                        REFERENCES author_dimension(steamid); """
    conn.execute(queryConstrait)
    conn.close()


tiempoSubida = time()
uploadToDB(newGameFeatures , reviewsGameExistInFeatures)
tiempoFinalSubida = time() - tiempoSubida
print("Tiempo de subida a la BD: ", tiempoConvSTR(tiempoFinalSubida))



tiempoFinal = time()
tiempoTotal = tiempoFinal - tiempoInicialCruzamiento
print("Tiempo total de ejecucion: ", tiempoConvSTR(tiempoTotal))
