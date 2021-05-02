
import pandas as pd
import psycopg2 as pg

#Conectamos con la base de datos
conn = pg.connect(host="localhost", dbname="Data_Set_Supermercado", user="postgres", password="password")
cur = conn.cursor()

#Eliminamos las tablas existentes y creamos las que necesitamos
#Tabla Branch
cur.execute("DROP TABLE IF EXISTS Branch;")
conn.commit()
cur.execute("CREATE TABLE Branch(id integer PRIMARY KEY, branch character(1) not null);")
conn.commit()

#Tabla Product_Line
cur.execute("DROP TABLE IF EXISTS Product_Line;")
conn.commit()
cur.execute("CREATE TABLE Product_Line(id integer PRIMARY KEY, product_line text not null);")
conn.commit()

#Tabla City
cur.execute("DROP TABLE IF EXISTS City;")
conn.commit()
cur.execute("CREATE TABLE City(id integer PRIMARY KEY, city text not null);")
conn.commit()

#Tabla Payment
cur.execute("DROP TABLE IF EXISTS Payment;")
conn.commit()
cur.execute("CREATE TABLE Payment(id integer PRIMARY KEY, payment text not null);")
conn.commit()

#Tabla Customer_Type
cur.execute("DROP TABLE IF EXISTS Customer_Type;")
conn.commit()
cur.execute("CREATE TABLE Customer_Type(id integer PRIMARY KEY, type text not null);")
conn.commit()

#Tabla Gender
cur.execute("DROP TABLE IF EXISTS Gender;")
conn.commit()
cur.execute("CREATE TABLE Gender(id integer PRIMARY KEY, gender text not null);")
conn.commit()

#Tabla Invoice
cur.execute("DROP TABLE IF EXISTS Invoice;")
conn.commit()
cur.execute("""CREATE TABLE Invoice(id text PRIMARY KEY, 
                                    branch integer not null references Branch(id),
                                    city integer not null references City(id),
                                    customer_type integer not null references Customer_Type(id),
                                    gender integer not null references Gender(id),
                                    product_line integer not null references Product_Line(id),
                                    unit_price double precision not null,
                                    quantity integer not null,
                                    tax double precision not null,
                                    total double precision not null,
                                    date date not null,
                                    time time not null,
                                    payment integer not null references Payment(id),
                                    cogs double precision not null,
                                    gross_margin double precision not null,
                                    gross_income double precision not null,
                                    rating double precision not null);""")
conn.commit()


dataset = pd.read_csv("dataset1.csv")

print(dataset)

#Hacer el modelo 
# y utilizar pandas para dividir las tablas 

