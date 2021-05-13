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
    
def create_triggers(cur,conn, exists_database):
    query_trigger1 = """
    create or replace function verificar_ciudad() returns trigger
    as
    $$
    declare
        rec record;
    begin
        for rec in (select * from ciudad) loop
            if rec.ciudad = new.ciudad then
                return NULL;
            end if;
        end loop;
        return new;
    end;
    $$
    language plpgsql;

    create trigger t_verificar_ciudad before insert on ciudad
    for each row
    execute procedure verificar_ciudad();

    --------------

    create or replace function verificar_genero() returns trigger
    as
    $$
    declare
        rec record;
    begin
        for rec in (select * from genero) loop
            if rec.genero = new.genero then
                return NULL;
            end if;
        end loop;
        return new;
    end;
    $$
    language plpgsql;

    create trigger t_verificar_genero before insert on genero
    for each row
    execute procedure verificar_genero();

    --------------

    create or replace function verificar_linea_producto() returns trigger
    as
    $$
    declare
        rec record;
    begin
        for rec in (select * from linea_producto) loop
            if rec."linea_producto" = new."linea_producto" then
                return NULL;
            end if;
        end loop;
        return new;
    end;
    $$
    language plpgsql;

    create trigger t_verificar_linea_producto before insert on linea_producto
    for each row
    execute procedure verificar_linea_producto();

    --------------

    create or replace function verificar_rama() returns trigger
    as
    $$
    declare
        rec record;
    begin
        for rec in (select * from rama) loop
            if rec.rama = new.rama then
                return NULL;
            end if;
        end loop;
        return new;
    end;
    $$
    language plpgsql;

    create trigger t_verificar_rama before insert on rama
    for each row
    execute procedure verificar_rama();

    --------------

    create or replace function verificar_tipo_cliente() returns trigger
    as
    $$
    declare
        rec record;
    begin
        for rec in (select * from tipo_cliente) loop
            if rec.tipo = new.tipo then
                return NULL;
            end if;
        end loop;
        return new;
    end;
    $$
    language plpgsql;

    create trigger t_verificar_tipo_cliente before insert on tipo_cliente
    for each row
    execute procedure verificar_tipo_cliente();

    --------------

    create or replace function verificar_tipo_pago() returns trigger
    as
    $$
    declare
        rec record;
    begin
        for rec in (select * from tipo_pago) loop
            if rec."tipo_pago" = new."tipo_pago" then
                return NULL;
            end if;
        end loop;
        return new;
    end;
    $$
    language plpgsql;

    create trigger t_verificar_tipo_pago before insert on tipo_pago
    for each row
    execute procedure verificar_tipo_pago();

    --------------

    create or replace procedure agregar_factura_python(id_f text, prec_uni double precision, cant int, impu double precision, total double precision,
                                                    fecha date, hora time, cobv double precision, porc_mar_bru double precision, porc_ing double precision,
                                                    califi double precision, li_produ text, ti_pago text, ti_cli text, gen text, ram char, ciu text)
    as
    $$
    declare
        id_li_produ int;
        id_ti_pago int;
        id_ti_cli int;
        id_gen int;
        id_rama int;
        id_ciu int;
    begin
        select lp.id into id_li_produ from linea_producto lp where lp."linea_producto" = li_produ;
        select tp.id into id_ti_pago from tipo_pago tp where tp."tipo_pago" = ti_pago;
        select tc.id into id_ti_cli from tipo_cliente tc where tc.tipo = ti_cli;
        select ge.id into id_gen from genero ge where ge.genero = gen;
        select ra.id into id_rama from rama ra where ra.rama = ram;
        select ci.id into id_ciu from ciudad ci where ci.ciudad = ciu;
        insert into factura(id, precio_unitario, cantidad, impuesto, total, fecha, hora, cobv,
                            porcentaje_margen_bruto, porcentaje_ingreso, calificacion,
                            linea_producto, tipo_pago, tipo_cliente, genero, rama, ciudad)
        values (id_f, prec_uni, cant, impu, total, fecha, hora, cobv, porc_mar_bru, porc_ing, califi,
                id_li_produ, id_ti_pago, id_ti_cli, id_gen, id_rama, id_ciu);
    end;
    $$
    language plpgsql;
        
    """
    cur.execute(query_trigger1)
    conn.commit()

dataset = pd.read_csv("dataset1.csv")

print(dataset)

new_dataset = tratar_dataset(dataset)

print('==============================================================================================')
print(new_dataset)

conn = pg.connect(host='localhost', user='postgres', password='postgres')   
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
database_name = "Data_Set_Supermercado"
exists_database = create_database(database_name, conn)
conn = pg.connect(host="localhost", user="postgres", password="postgres", database=database_name)
cur = conn.cursor()
create_tables(cur, conn, exists_database)

create_triggers(cur,conn, exists_database)

query1 = "insert into rama(rama) values(%s)"
query2 = "insert into linea_producto(linea_producto) values(%s)"
query3 = "insert into ciudad(ciudad) values(%s)"
query4 = "insert into tipo_pago(tipo_pago) values(%s)"
query5 = "insert into tipo_cliente(tipo) values(%s)"
query6 = "insert into genero(genero) values(%s)"
query7 = "call agregar_factura_python(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

for row in new_dataset.iterrows():
    idFactura = row[1][0]
    ciudad = row[1][1]
    rama = row[1][2]
    tipo_cliente = row[1][3]
    genero = row[1][4]
    lineaProducto = row[1][5]
    tipoPago = row[1][6]
    cantidad = row[1][7]
    precioUnitario = row[1][8]
    impuesto5 = row[1][9]
    total = row[1][10]
    cobv = row[1][11]
    porceMargenBruto = row[1][12]
    porceIngreso = row[1][13]
    fecha = row[1][14]
    hora = row[1][15]
    calificacion = row[1][16]
    
    cur.execute(query1,(rama))
    conn.commit()
    cur.execute(query2,(lineaProducto,))
    conn.commit()
    cur.execute(query3,(ciudad,))
    conn.commit()
    cur.execute(query4,(tipoPago,))
    conn.commit()
    cur.execute(query5,(tipo_cliente,))
    conn.commit()
    cur.execute(query6,(genero,))
    conn.commit()

    cur.execute(query7,(idFactura, precioUnitario, cantidad, impuesto5, total, fecha, hora, cobv, porceMargenBruto, porceIngreso, calificacion, lineaProducto, tipoPago, tipo_cliente, genero, rama, ciudad))
    conn.commit()

