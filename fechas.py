import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re


# ########### ME TRAIGO LOS DATOS DE SALESFORCE ##############

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Cuota__c, Fecha_Importacion__c, Fecha_Registro__c, Fecha_Vencimiento__c, Fecha_AcreditacionOP__c
            FROM Transacciones__c'''

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

sf_transacciones = sf_transacciones.loc[(sf_transacciones['Cuota__c']==7)]


#sf_transacciones = sf_transacciones[sf_transacciones['Fecha_AcreditacionOP__c'].isnull()]

print(sf_transacciones)

# # ###Fechas
date = datetime(2021, 9, 6, 21, 0)
date_texto = date.strftime("%m %d %Y %H:%M")   ##convert to string 
new_date = datetime.strptime(date_texto, "%m %d %Y %H:%M")   ##De string a datetime


sf_transacciones['Fecha_Importacion__c'] = new_date
sf_transacciones['Fecha_Importacion__c'] = sf_transacciones['Fecha_Importacion__c'].dt.strftime('%Y-%m-%d %H:%M')

# sf_transacciones['Fecha_Registro__c'] = new_date
# sf_transacciones['Fecha_Registro__c'] = sf_transacciones['Fecha_Registro__c'].dt.strftime('%Y-%m-%d %H:%M:%S')

# sf_transacciones['Fecha_Vencimiento__c'] = new_date
# sf_transacciones['Fecha_Vencimiento__c'] = sf_transacciones['Fecha_Vencimiento__c'].dt.strftime('%Y-%m-%d %H:%M:%S')

# ##fecha op
# date_op = datetime(1753, 1, 1)
# date_texto_op = date_op.strftime("%m %d %Y")  ##convert to string # %H:%M:%S"
# new_date_op = datetime.strptime(date_texto_op, "%m %d %Y")   ##De string a datetime # %H:%M:%S"

# sf_transacciones['Fecha_AcreditacionOP__c'] = new_date_op
# sf_transacciones['Fecha_AcreditacionOP__c'] = sf_transacciones['Fecha_AcreditacionOP__c'].dt.strftime('%Y-%m-%d') 

# print(sf_transacciones)
# sf_transacciones.to_csv('transacciones.csv', index = False)

# sf_transacciones = sf_transacciones.drop(['Cuota__c', 'Fecha_Importacion__c','Fecha_Registro__c','Fecha_Vencimiento__c'], axis = 1)

# #sf_transacciones = sf_transacciones.head(2)

# # print(type(date))
# # print(type(date_texto))
# # print(type(new_date))

# print(sf_transacciones)
# print(type(sf_transacciones['Fecha_AcreditacionOP__c']))

sf_transacciones = sf_transacciones[['Id', 'Fecha_Importacion__c']]
sf_transacciones = sf_transacciones.replace([None],'')

print(sf_transacciones)

# ###### ACTUALIZACION EN SF #######
# transacciones = instancia('Transacciones__c')
# pagos_update = sf_transacciones.to_dict('records')

# print(pagos_update)
# resultado = sf.bulk.Transacciones__c.update(pagos_update)
# print(resultado)