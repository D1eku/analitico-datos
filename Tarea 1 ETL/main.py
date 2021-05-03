from tratamiento_datos import tratar_dataset
import pandas as pd
import psycopg2 as pg
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database(database_name, conn):
    cur = conn.cursor()
    select ="SELECT 1 FROM pg_catalog.pg_database WHERE datname = '" + database_name + "';" #Busca si la base de datos existe
    cur.execute(select)
    exists_database = cur.fetchone()#Si la lista tiene datos existe la base de datos
    if not exists_database: #Si no tiene ningun dato la base de datos no existe y tenemos que crear una
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
    return exists_database


def create_tables(cur, conn, exists):
    #En el caso de que la base de datos exista eliminamos las tablas
    if exists_database:
        cur.execute("""DROP TABLE IF EXISTS Factura;
                          DROP TABLE IF EXISTS Linea_Producto;
                          DROP TABLE IF EXISTS Ciudad;
                          DROP TABLE IF EXISTS Tipo_Pago;
                          DROP TABLE IF EXISTS Tipo_Cliente;
                          DROP TABLE IF EXISTS Genero;
                          DROP TABLE IF EXISTS Rama;""")
        conn.commit()
    #Creamos las tablas
    cur.execute("""CREATE TABLE Rama(id serial primary key, rama char(1) not null);
                      CREATE TABLE Linea_Producto(id serial primary key, linea_producto text not null);
                      CREATE TABLE Ciudad(id serial primary key, ciudad text not null);
                      CREATE TABLE Tipo_Pago(id serial primary key, tipo_pago text not null);
                      CREATE TABLE Tipo_Cliente(id serial primary key, tipo text not null);
                      CREATE TABLE Genero(id serial primary key, genero text not null);
                      CREATE TABLE Factura(id text primary key,
                                           precio_unitario double precision not null,
                                           cantidad integer not null,
                                           impuesto double precision not null,
                                           total double precision not null,
                                           fecha date not null,
                                           hora time not null,
                                           cobv double precision not null,
                                           porcentaje_margen_bruto double precision not null,
                                           porcentaje_ingreso double precision not null,
                                           calificacion double precision not null,
                                           linea_producto integer not null references Linea_Producto(id),
                                           tipo_pago integer not null references Tipo_Pago(id),
                                           tipo_cliente integer not null references Tipo_Cliente(id),
                                           genero integer not null references Genero(id),
                                           rama integer not null references Rama(id),
                                           ciudad integer not null references Ciudad(id));""")
    conn.commit()
    


dataset = pd.read_csv("dataset1.csv")

print(dataset)

new_dataset = tratar_dataset(dataset)

print('==============================================================================================')
print(new_dataset)

conn = pg.connect(host='localhost', user='postgres', password='password')   
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
database_name = "Data_Set_Supermercado"
exists_database = create_database(database_name, conn)
conn = pg.connect(host="localhost", user="postgres", password="password", database=database_name)
cur = conn.cursor()
create_tables(cur, conn, exists_database)