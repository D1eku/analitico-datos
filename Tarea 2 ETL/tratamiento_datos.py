import pandas as pd
from googletrans import Translator

def fill_NaN(df):
    return df.fillna({"Invoice ID": "No-ID", "Branch": "No-Branch", "City": "No-City", "Customer type": "No-C_Type", "Gender": "No-Gender", "Product line": "No-Prod_Line", "Unit price": -1, "Quantity": -1, "Tax 5%": -1, "Total": -1, "Date": "No-Date", "Time": "No-Time", "Payment": "No-Payment", "cogs": -1, "gross margin percentage": -1, "gross income": -1, "Rating": "No-Rating"})

def calculate_cogs(total, gross_margin_percentage):
    return (total - ((gross_margin_percentage * total)/100))

def calculate_gross_margin_percentage(total, cogs):
    return (((total - cogs)/total)*100)

def calculate_tax(total):
    return (total * 0.05)

def check_data(df):
    for row in range(0, len(df)):
        # (3.1) Verificacion total, cantidad y precio unitario
        # Existen el precio unitario y la cantidad
        if(float(df.loc[row, "Unit price"]) > -1 and float(df.loc[row, "Quantity"]) > -1):
            total = float(df.loc[row, "Unit price"]) * float(df.loc[row, "Quantity"])
            # Si no coinciden se reemplaza
            if(total != float(df.loc[row, "Total"])):
                df.loc[row, "Total"] = str(total)
        # No existe algun dato
        else:
            # No existe el precio unitario, pero la cantidad y el total si
            if(float(df.loc[row, "Unit price"]) < 0 and float(df.loc[row, "Quantity"]) > -1 and float(df.loc[row, "Total"]) > -1):
                df.loc[row, "Unit price"] = str(float(df.loc[row, "Total"])/float(df.loc[row, "Quantity"]))
            # No existe la cantidad, pero el precio unitario y el total si
            elif(float(df.loc[row, "Unit price"]) > -1 and float(df.loc[row, "Quantity"]) < 0 and float(df.loc[row, "Total"]) > -1):
                df.loc[row, "Quantity"] = str(float(df.loc[row, "Total"])/float(df.loc[row, "Unit price"]))
            # No existe la cantidad ni el precio unitario, por lo tanto esta factura esta incompleta y debe ser eliminada
            elif(float(df.loc[row, "Unit price"]) < 0 and float(df.loc[row, "Quantity"]) < 0):
                df.drop([row], axis=0)
                continue
        # (3.2) Verificacion cogs y porcentaje margen bruto
        # Existen ambos datos y basta que verifiquemos que uno este correcto
        if(float(df.loc[row, "cogs"]) > -1 and float(df.loc[row, "gross margin percentage"]) > -1):
            cogs = calculate_cogs(float(df.loc[row, "Total"]), float(df.loc[row, "gross margin percentage"]))
            if(float(df.loc[row, "cogs"]) != cogs):
                df.loc[row, "cogs"] = str(cogs)
        # Falta algun dato        
        else:
            # Falta el cogs, pero existe el gross margin percentage
            if(float(df.loc[row, "cogs"]) < 0 and float(df.loc[row, "gross margin percentage"]) > -1):
                df.loc[row, "cogs"] = str(calculate_cogs(float(df.loc[row, "Total"]), float(df.loc[row, "gross margin percentage"])))
            # Falta el gross margin percentage, pero existe el cogs
            elif(float(df.loc[row, "cogs"]) > -1 and float(df.loc[row, "gross margin percentage"]) < 0):
                df.loc[row, "gross margin percentage"] = str(calculate_gross_margin_percentage(float(df.loc[row, "Total"]),float(df.loc[row, "cogs"])))
            # No existe ninguno de los dos, por lo tanto esta factura esta incompleta y debe ser eliminada
            elif(float(df.loc[row, "cogs"]) < 0 and float(df.loc[row, "gross margin percentage"]) < 0):
                df.drop([row], axis=0)
                continue
        # (3.3) Verificacion Tax 5% y gross income
        #Existen ambos datos
        if(float(df.loc[row, "Tax 5%"]) > -1 and float(df.loc[row, "gross income"]) > -1):
            tax = calculate_tax(float(df.loc[row, "Total"]))
            if(float(df.loc[row, "Tax 5%"]) != tax):
                df.loc[row, "Tax 5%"] = str(tax)
            if(float(df.loc[row, "gross income"]) != tax):
                df.loc[row, "gross income"] = str(tax)
        elif(float(df.loc[row, "Tax 5%"]) < 0):
            df.loc[row, "Tax 5%"] = str(calculate_tax(float(df.loc[row, "Total"])))
        elif(float(df.loc[row, "gross income"]) < 0):
            df.loc[row, "gross income"] = str(calculate_tax(float(df.loc[row, "Total"])))
    #end for
    return df
    

# Dataframe inicial
df_inicial= pd.read_csv("supermarket_sales_sucio.csv")
print("Dataframe inicial")
print(df_inicial)
print()

# Limpiando el dataframe
# (1) Separando por columnas
df = pd.read_csv("supermarket_sales_sucio.csv", delimiter=";")

# (2) Limpiando valores NaN
df = fill_NaN(df)

# (3) Verificando y calculando datos
df = check_data(df)


        
            





