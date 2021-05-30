from tratamiento_datos import process_data
import pandas as pd
import numpy as np
import random
from google_trans_new import google_translator
import psycopg2 as pg

def cread_db( dataset):
    #Separando tablas
    invoice_df = new_dataset[['Invoice ID','Unit price','Quantity','Tax 5%','Total','cogs','Rating']]
    product_dimension = new_dataset[['Product line','Branch']]
    customer_dimension = new_dataset[['Gender', 'Customer type', 'Payment']]
    time_dimension = new_dataset[['Date','Time']]
    city_dimension = new_dataset[['City']]
    #Generaremos una tabla de id
    largo = new_dataset.shape[0]

    ceros = np.zeros((largo,1))
    for i in range(ceros.size):
        ceros[i] = int(i+1)
        
    id = pd.DataFrame(ceros,columns=['ID'])
    id = id.astype(int)

    #Agregamos las id a todas las tablas 
    #Agrega las ID
    invoice_df['Product ID'] = id['ID'].values
    invoice_df['Costumer ID'] = id['ID'].values
    invoice_df['Time ID'] = id['ID'].values
    invoice_df['City ID'] = id['ID'].values

    product_dimension['Product ID'] = id['ID'].values
    product_dimension[['Product line','Branch']] = new_dataset[['Product line','Branch']]

    customer_dimension['Customer ID'] = id['ID'].values
    customer_dimension[['Gender', 'Customer type', 'Payment']]  = new_dataset[['Gender', 'Customer type', 'Payment']]

    time_dimension['Time ID'] = id['ID'].values
    time_dimension[['Date','Time']]  = new_dataset[['Date','Time']]

    city_dimension['City ID'] = id['ID'].values
    city_dimension[['City']] = new_dataset[['City']]

    #Subiendo a la base de datos
    import sqlalchemy as sa
    from sqlalchemy.ext import declarative
    from uuid import uuid4
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    conn = pg.connect(host='localhost', user='postgres', password='postgres')  
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("select exists (select * from pg_catalog.pg_database WHERE datname = 'supermarket2');")
    existe = cur.fetchone()[0]

    if(bool(existe)):#Si existe la base de datos eliminala
        cur.execute("commit")
        cur.execute("""SELECT
                            pg_terminate_backend (pg_stat_activity.pid)
                        FROM
                            pg_stat_activity
                        WHERE
                            pg_stat_activity.datname = 'supermarket2';
                    """)#Desconecta a los clientes
        cur.execute('drop database supermarket2')#Elimina la database
        
    #Crea la base de datos

    cur.execute('create database supermarket2')
    conn.close()

    #Cambiando nombre de las columnas
    invoice_df.columns = ['id','unit_price','quantity','tax_5_percentage','total','cogs','rating','product_id','customer_id','time_id','city_id']
    product_dimension.columns = ['product_line','branch','id']
    customer_dimension.columns = ['gender','customer_type','payment','id']
    time_dimension.columns = ['date','time','id']
    city_dimension.columns = ['city','id']

    #Query Create tables
    create_tables = """ 
    create table product_dimension(
        id int primary key,
        product_line text not null,
        branch char not null
    );

    create table customer_dimension(
        id int primary key,
        gender text not null,
        customer_type text not null,
        payment text not null
    );

    create table time_dimension(
        id int primary key,
        date date not null,
        time time not null
    );

    create table city_dimension(
        id int primary key,
        city text not null
    );
    
    create table invoice_fact(
        id TEXT PRIMARY KEY,
        unit_price float not null,
        quantity int not null,
        tax_5_percentage float not null,
        total float not null,
        cogs float not null,
        rating float not null,
        customer_id int not null,
        product_id int not null,
        time_id int not null,
        city_id int not null,
        foreign key(customer_id) references customer_dimension(id),
        foreign key(product_id) references product_dimension(id),
        foreign key(time_id) references time_dimension(id),
        foreign key(city_id) references city_dimension(id)
    ); """


    #Creamos el engine
    engine = sa.create_engine('postgresql://postgres:postgres@localhost/supermarket2')

    #Obtenemos la conexion del engine
    conn = engine.connect()
    conn.execute('commit')
    conn.execute(create_tables)#Ejecutamos crear tablas

    #Subimos los datos de product dimension
    product_dimension.to_sql('product_dimension', engine, if_exists='append', index=False)

    #Subimos los datos de customer dimension
    customer_dimension.to_sql('customer_dimension', engine, if_exists='append', index=False)

    #Subimos los datos de time dimension
    time_dimension.to_sql('time_dimension', engine, if_exists='append', index=False)

    #Subimos los datos de time dimension
    city_dimension.to_sql('city_dimension', engine, if_exists='append', index=False)

    #Subimos los datos de invoice fact
    invoice_df.to_sql('invoice_fact', engine, if_exists='append', index=False)
    print("\n\n\n\n\n\n\n\n")
    print("=============================================================================================================")
    print("Subida a la base de datos correcta, revisar la BD con nombre: supermarket2")
    print("=============================================================================================================")
new_dataset = process_data("supermarket_sales_sucio.csv")
print(new_dataset)
cread_db(new_dataset)