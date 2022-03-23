import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re

cuil_abril = pd.read_excel("correción cuil abril.xls.xlsx")
#print(cuil_abril)

cuil_abril = cuil_abril.iloc[:,2:4]

print(cuil_abril)

########## ME TRAIGO LOS DATOS DESDE SALESFORCE  ########### 

transacciones = instancia('Transacciones__c')

query = '''SELECT Destino__c, Cod_Evento__c, Fecha_Registro__c, N_m_documento__c, Id_del_beneficiario__c
         FROM Transacciones__c'''

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

cuil_df = cuil_abril.merge(sf_transacciones, left_on = 'Número de beneficio', right_on = 'Id_del_beneficiario__c', how = 'left')

print(cuil_df)

#Cambio el tipo de dato
cuil_df = cuil_df.astype({"Destino__c": int})

##Hago las transformaciones necesarias

#Escribo la fecha de hoy 
date = datetime(2021, 4, 16, 0)
date_texto = date.strftime('%a %b %d %y')
new_date = datetime.strptime(date_texto, '%a %b %d %y')

# #Sumo estos nuevos campos al DF
cuil_df['Fecha_Registro'] = new_date

print(cuil_df)

###### INSERTAR DATOS EN EL SQL SERVER ####

# conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                               "Server=FPC14;"
#                                               "DATABASE=FPC-Migrada;"   #Esto es lo unico que cambia
#                                               "Trusted_Connection=yes;")
# cursor = conexion.cursor()

# for index,row in cuil_abril.iterrows():
#         cursor.execute('''INSERT INTO dbo.[Fundación Perez Companc$Interfaz Beneficios]([Nro_Beneficio], [CUIT_de_la_cuenta], [Destino], [Tipo_de_Evento], [Fecha_Registro], [Nro_documento], [Beneficiario]) 
#                                   VALUES (?, ?, ? ,? ,?, ?, ?)''',    
#                                   [row['Número de beneficio'],  
#                                   row['CUIL'], 
#                                   row['Destino__c'], 
#                                   row['Cod_Evento__c'], 
#                                   row['Fecha_Registro'],  
#                                   row['N_m_documento__c'], 
#                                   row['Id_del_beneficiario__c'],
# conexion.commit()
# cursor.close()
# conexion.close()