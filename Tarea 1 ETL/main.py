from tratamiento_datos import tratar_dataset
import pandas as pd
import psycopg2 as pg


dataset = pd.read_csv("dataset1.csv")

print(dataset)

new_dataset = tratar_dataset(dataset)

print('==============================================================================================')
print(new_dataset)
