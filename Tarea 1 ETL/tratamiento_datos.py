import pandas as pd

# Funcion para cambiar los valores de gender
def change_gender(val):
    if (val == 'Male'):
        return 'Masculino'
    else:
        return 'Femenino'

# Funcion para cambiar los valores de customer_type
def change_customer(val):
    if(val == 'Member'):
        return 'Miembro'
    else:
        return 'Normal'
    
# Funcion para cambiar los valores de Product Line
def change_product(val):
    if(val == 'Electronic accessories'): # Accesorios electronicos
        return 'Accesorios Electronicos'
    elif(val == 'Fashion accessories'): # Accesorios de moda
        return 'Accesorios Moda'
    elif(val == 'Food and beverages'): #Comida y bebidas
        return 'Alimentos y Bebidas'
    elif(val == 'Health and beauty'): # Salud y belleza
        return 'Salud y Belleza'
    elif(val == 'Home and lifestyle'): # Casa y estilo de vida
        return 'Hogar y Estilo De Vida'
    elif(val == 'Sports and travel'): # Deportes y viajes
        return 'Deportes y Viajes'
    else:
        return 'Valor Erroneo'

# Funcion para cambiar los valores de Payment
def change_payment(val):
    if(val == 'Cash'):
        return 'Efectivo'
    elif(val == 'Credit card'):
        return 'Tarjeta de Credito'
    elif(val == 'Ewallet'):
        return 'Billetera electronica'

def tratar_dataset(dataset):
    #Gender
    dataset['Gender'] = dataset['Gender'].transform(change_gender)

    # customer type
    dataset['Customer type'] = dataset['Customer type'].transform(change_customer)

    # Product line
    dataset['Product line'] = dataset['Product line'].transform(change_product)

    # Payment
    dataset['Payment'] = dataset['Payment'].transform(change_payment)

    #Nombre de las cabeceras del data set
    columnas2 = dataset[
        ['Invoice ID',
        'Branch',
        'City',
        'Customer type',
        'Gender',
        'Product line',
        'Unit price',
        'Quantity',
        'Tax 5%',
        'Total',
        'Date',
        'Time',
        'Payment',
        'cogs',
        'gross margin percentage',
        'gross income',
        'Rating']].columns.values

    columnas_eng = []
    #j = 0
    for i in columnas2:
        #j = j +1
        #print(j," --> ",i)
        columnas_eng.append(i)

    columnas_esp = [
        'ID Factura',
        'Rama',
        'Ciudad',
        'Tipo Cliente',
        'Genero',
        'Linea Producto',
        'Precio Unitario',
        'Cantidad',
        'Impuesto 5%',
        'Total',
        'Fecha',
        'Hora',
        'Tipo Pago',
        'COBV',
        'Porcentaje Margen Bruto',
        'Porcentaje Ingreso',
        'Calificacion']

    esp_dataset = pd.DataFrame(columns = columnas_esp)
    #for i in range(0, len(columnas2), 1):
    #    print(i)
    #    print(columnas2[i])
    #    print(columnas_esp[i])


    for i in range(0, len(columnas2), 1):
        esp_dataset[columnas_esp[i]] = dataset[columnas2[i]].copy()

    df = esp_dataset[[
        'ID Factura',
        'Ciudad',
        'Rama',
        'Tipo Cliente',
        'Genero',
        'Linea Producto',
        'Tipo Pago',
        'Cantidad',
        'Precio Unitario',
        'Impuesto 5%',
        'Total',
        'COBV',
        'Porcentaje Margen Bruto',
        'Porcentaje Ingreso',
        'Fecha',
        'Hora',
        'Calificacion']]

    return df