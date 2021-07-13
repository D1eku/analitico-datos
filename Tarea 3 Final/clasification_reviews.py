from os import path
from numpy import string_
import pandas as pd

pathEnglishReviews = 'C:/Users/faabi/Desktop/Proyecto/english-steam-reviews.csv'

goodW = ['Great','Good','Favorite','Exciting','Fine','Best','Favourite','Excellent','Nice','Popular','Splendid'
,'Fascinating','Wonderful','Beautiful','Sweet','Magnificent','Fantastic','Complete','Balanced','Worthy','Fun']

badW = ['Terrible','Terribly','Suck','Sucks','Poor','Poorest','Stupid','Shit','Bad','Worst','Worse','Disgusting','Nasty','Appalling','Hopeless'
,'Horrible','Fool','Foolish','Idiot','Fatal','Expensive','Vulgar','Useless','Unconventional','Ugly','Troublesome','Annoying','Bothering']

esrdf = pd.read_csv(pathEnglishReviews, delimiter=",")

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