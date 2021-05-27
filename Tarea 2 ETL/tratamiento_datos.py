import pandas as pd
import random
from google_trans_new import google_translator

# Rellena los valores NaN
def fill_NaN(df):
    df = df.fillna({"Invoice ID": "No-ID", "Branch": "No-Branch", "City": "No-City", "Customer type": "No-C_Type", "Gender": "No-Gender", "Product line": "No-Prod_Line", "Unit price": -1, "Quantity": -1, "Tax 5%": -1, "Total": -1, "Date": "No-Date", "Time": "No-Time", "Payment": "No-Payment", "cogs": -1, "gross margin percentage": -1, "gross income": -1, "Rating": "No-Rating"})
    df["Quantity"] = df["Quantity"].astype("int64")
    df["Total"] = df["Total"].astype("float64")
    df["gross income"] = df["gross income"].astype("float64") 
    return df

def fill_other_values_with_NaN(df):
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors='coerce')     
    df["Total"] = pd.to_numeric(df["Total"], errors='coerce') 
    df["gross income"] = pd.to_numeric(df["gross income"], errors='coerce')   
    return df

def data_cleaning(df):
    df = fill_other_values_with_NaN(df)
    df = fill_NaN(df)
    return df
    

def calculate_cogs(total, gross_margin_percentage):
    return (total - ((gross_margin_percentage * total)/100))

def calculate_gross_margin_percentage(total, cogs):
    return (((total - cogs)/total)*100)

def calculate_tax(total):
    return (total * 0.05)

def check_data(df):
    drop_rows = []
    for row in range(0, len(df)):
        # (1) Verificacion total, cantidad y precio unitario
        # Existen el precio unitario y la cantidad
        if(df.loc[row, "Unit price"] > -1 and df.loc[row, "Quantity"] > -1):
            total = df.loc[row, "Unit price"] * df.loc[row, "Quantity"]
            # Si no coinciden se reemplaza
            if(total != df.loc[row, "Total"]):
                df.loc[row, "Total"] = float(total)
        # No existe algun dato
        else:
            # No existe el precio unitario, pero la cantidad y el total si
            if(df.loc[row, "Unit price"] < 0 and df.loc[row, "Quantity"] > -1 and df.loc[row, "Total"] > -1):
                df.loc[row, "Unit price"] = float(df.loc[row, "Total"]/df.loc[row, "Quantity"])
            # No existe la cantidad, pero el precio unitario y el total si
            elif(df.loc[row, "Unit price"] > -1 and df.loc[row, "Quantity"] < 0 and df.loc[row, "Total"] > -1):
                df.loc[row, "Quantity"] = int(df.loc[row, "Total"]/df.loc[row, "Unit price"])
            # No existe la cantidad ni el precio unitario, por lo tanto esta factura esta incompleta y debe ser eliminada
            elif(df.loc[row, "Unit price"] < 0 and df.loc[row, "Quantity"] < 0):
                drop_rows.append(row)
                continue
        # (2) Verificacion cogs y porcentaje margen bruto
        # Existen ambos datos y basta que verifiquemos que uno este correcto
        if(df.loc[row, "cogs"] > -1 and df.loc[row, "gross margin percentage"] > -1):
            cogs = calculate_cogs(df.loc[row, "Total"], df.loc[row, "gross margin percentage"])
            if(df.loc[row, "cogs"] != cogs):
                df.loc[row, "cogs"] = float(cogs)
        # Falta algun dato        
        else:
            # Falta el cogs, pero existe el gross margin percentage
            if(df.loc[row, "cogs"] < 0 and df.loc[row, "gross margin percentage"] > -1):
                df.loc[row, "cogs"] = float(calculate_cogs(df.loc[row, "Total"], df.loc[row, "gross margin percentage"]))
            # Falta el gross margin percentage, pero existe el cogs
            elif(df.loc[row, "cogs"] > -1 and df.loc[row, "gross margin percentage"] < 0):
                df.loc[row, "gross margin percentage"] = float(calculate_gross_margin_percentage(df.loc[row, "Total"],df.loc[row, "cogs"]))
            # No existe ninguno de los dos, por lo tanto esta factura esta incompleta y debe ser eliminada
            elif(df.loc[row, "cogs"] < 0 and df.loc[row, "gross margin percentage"] < 0):
                drop_rows.append(row)
                continue
        # (3) Verificacion Tax 5% y gross income
        #Existen ambos datos
        if(df.loc[row, "Tax 5%"] > -1 and df.loc[row, "gross income"] > -1):
            tax = calculate_tax(df.loc[row, "Total"])
            if(df.loc[row, "Tax 5%"] != tax):
                df.loc[row, "Tax 5%"] = float(tax)
            if(df.loc[row, "gross income"] != tax):
                df.loc[row, "gross income"] = float(tax)
        elif(df.loc[row, "Tax 5%"] < 0):
            df.loc[row, "Tax 5%"] = float(calculate_tax(df.loc[row, "Total"]))
        elif(df.loc[row, "gross income"] < 0):
            df.loc[row, "gross income"] = float(calculate_tax(df.loc[row, "Total"]))
    # end for
    # Eliminacion filas incompletas
    df = df.drop(drop_rows, axis=0)
    # Reseteamos el index
    df = df.reset_index(drop=True)
    return df

# Traduccion con la libreria
def translate(text):
    translator = google_translator()
    for i in range(0, len(text)):
        trans = translator.translate(text[i], lang_tgt="es")
        if(type(trans) is list):
            text[i] = trans[0]
        else:
            text[i] = trans
    return text

# Cambia los datos traducidos
def change_data(df, en, es, column):
    for row in range(0, len(df)):
        df.loc[row, column] = es[en.index(df.loc[row, column])]
    return df

# Traduccion del dataframe
def translate_data(df):
    df = change_data(df, df["Customer type"].unique().tolist(), translate(df["Customer type"].unique().tolist()), "Customer type")
    df = change_data(df, df["Gender"].unique().tolist(), translate(df["Gender"].unique().tolist()), "Gender")
    df = change_data(df, df["Product line"].unique().tolist(), translate(df["Product line"].unique().tolist()), "Product line")
    df = change_data(df, df["Payment"].unique().tolist(), translate(df["Payment"].unique().tolist()), "Payment")
    return df

# Cambia el formato de la fecha a DD-MM-AAAA
def change_date_format(date):
    if(date.find('/') > 0):
        split = date.split('/')
        return split[1] + "-" + split[0] + "-" + split[2]
    
    return date

def create_id():
    return str(random.randint(100, 999)) + "-" + str(random.randint(10, 99)) + "-" + str(random.randint(1000, 9999))

# Crea un id para las facturas que no tengan
def id_creator(df):
    ids = df["Invoice ID"].tolist()
    for row in range(0, len(df)):
        if(df.loc[row, "Invoice ID"] == "No-ID"):
            new_id = create_id()
            while(new_id in ids):
                new_id = create_id()
            df.loc[row, "Invoice ID"] = new_id
    # end for
    return df
    
def process_data(dataset):
    # (0) Creamos el dataframe
    df = pd.read_csv(dataset, delimiter=";")
    print(df)
    # (1) Limpiando valores NaN y datos de distintos tipos en las columnas
    df = data_cleaning(df)
    # (2) Verificando y calculando datos
    df = check_data(df)
    # (3) Traduccion de datos
    df = translate_data(df)
    # (4) Cambiar formato fecha
    df["Date"] = df["Date"].transform(change_date_format)
    # (5) Crear ID para las facturas que no tengan
    df = id_creator(df)
    return df







        
            





