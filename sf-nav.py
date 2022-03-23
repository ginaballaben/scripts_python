import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion import *
from datetime import datetime
from sqlalchemy import create_engine
import re


########## ME TRAIGO LOS DATOS DESDE SALESFORCE  ########### 

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Estado__c, Destino__c, Cod_Tipo_de_Evento__c ,Fecha_Registro__c, N_m_documento__c, Id_del_beneficiario__c, Fecha_Vencimiento__c, Cod_Forma_Pago__c, Nro_Beneficio__c,Nombre_del_beneficiario__c , Cuota__c, Cod_Banco__c,
        Cod_Sucursal__c, Tipo_de_Cuenta__c, Nro_Cuenta__c, CBU_a_Pagar__c, Cuit_de_la_Cuenta__c, Titular_de_la_Cuenta__c, Area_de_Accion__c, Tipo_de_Persona__c, Importe__c, Cdigo_de_divisa__c, Tipo_de_Beneficio__c, Tipo_de_Cambio__c, Importado__c, Registrado__c 
        FROM Transacciones__c'''

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)


#Guardo todo en un csv
#sf_transacciones.to_csv('Transacciones_completo.csv', index = False)


#Aplico los filtros

#sf_transacciones = sf_transacciones[sf_transacciones['Estado__c']=='Aprobado (Pendiente ERP)']

#sf_transacciones =sf_transacciones.sort_values(by=['Nro_Beneficio__c'], ascending = False)

#sf_transacciones.to_csv('Transacciones_pendientes.csv', index = False)


#sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Cuota__c']==1) | (sf_transacciones_test['Cuota__c']==0)]


#sf_transacciones_test =sf_transacciones_test.sort_values(by=['Nro_Beneficio__c'], ascending = False)



##Hago las transformaciones necesarias

#Escribo la fecha de hoy 
#date = datetime(2021, 3, 19, 0)
#date_texto = date.strftime('%a %b %d %y')
#new_date = datetime.strptime(date_texto, '%a %b %d %y')

#Sumo estos nuevos campos al DF
#sf_transacciones_test['Fecha_Registro'] = new_date
#sf_transacciones_test['Fecha_Vencimiento'] = new_date


#Cambio el tipo de dato
#sf_transacciones_test = sf_transacciones_test.astype({"Destino__c": int, "Cod_Tipo_de_Evento__c": str})


#Hago los reemplazos necesarios para que el tipo de evento viaje correctamente

#eventos = {"1.0": "01", "3.0": "03"}

#sf_transacciones_test['Cod_Tipo_de_Evento__c'].replace(eventos, inplace=True)

#Chequeo que los reemplazos estén hechos
#print(sf_transacciones_test['Cod_Tipo_de_Evento__c'])



#Limpio estos campos de comas y los pongo en mayúscuas. Trunco según especificaciones del SQL Server
#sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.strip()
#sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.replace(',', '')
#sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.upper()
#sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.slice(0,30)

#sf_transacciones_test['Nombre_del_beneficiario__c'] = sf_transacciones_test['Nombre_del_beneficiario__c'].str.strip()
#sf_transacciones_test['Nombre_del_beneficiario__c'] = sf_transacciones_test['Nombre_del_beneficiario__c'].str.replace(',', '')
#sf_transacciones_test['Nombre_del_beneficiario__c'] = sf_transacciones_test['Nombre_del_beneficiario__c'].str.upper()


#sf_transacciones_test['Tipo_de_Cuenta__c'] = sf_transacciones_test['Tipo_de_Cuenta__c'].str.slice(0,50)

#sf_transacciones_test = sf_transacciones_test.head(20)


#sf_transacciones_test.to_csv('Transacciones_test_ultimo2.csv', index = False)


### MANDAR COMPROMISOS PRIMERO ####

##Compromisos

#sf_transacciones_test_CP = sf_transacciones_test[sf_transacciones_test['Cod_Tipo_de_Evento__c']=="01"]
# sf_transacciones_test_CP = sf_transacciones_test_CP.head(10) #Si quiero probar enviando primero 10


# print(sf_transacciones_test_CP)

### MANDAR OBLIGACIONES ###

#sf_transacciones_test_OB = sf_transacciones_test[sf_transacciones_test['Cod_Tipo_de_Evento__c']=="03"]

#sf_transacciones_test_OB = sf_transacciones_test_OB.head(10) #Si quiero mandar primero 10 para probar

#sf_transacciones_test_OB.to_csv('Transacciones_OB.csv', index = False)

# print (sf_transacciones_test)



 ###### INSERTAR DATOS EN EL SQL SERVER ####

# conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                        "Server=FPC14;"
#                                        "DATABASE=FPC-Migrada;"   #Esto es lo unico que cambia
#                                        "Trusted_Connection=yes;")
# cursor = conexion.cursor()

# for index,row in sf_transacciones_test.iterrows():
#     cursor.execute('''INSERT INTO dbo.[Fundación Perez Companc$Interfaz Beneficios]([Destino],
#                            [Tipo_de_Evento],[Fecha_Registro], [Nro_documento], [Beneficiario], [Fecha_Vencimiento], [Cod_Forma_Pago], [Nro_Beneficio],
#                            [Nombre_beneficiario], [Cuota], [Banco], [Sucursal], [Nro_Cuenta], [CBU_a_Pagar], [CUIT_de_la_cuenta], [Titular_de_la_cuenta],
#                            [Area_de_Accion], [Tipo_de_Persona],  [Importe],  [Cod_Divisa], [Tipo_de_Beneficio], [Tipo_cambio], [Importado], [Registrado]) 
#                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
#                             [row['Destino__c'], 
#                             row['Cod_Tipo_de_Evento__c'], 
#                             row['Fecha_Registro'],
#                             row['N_m_documento__c'], 
#                             row['Id_del_beneficiario__c'], 
#                             row['Fecha_Vencimiento'],
#                             row['Cod_Forma_Pago__c'], 
#                             row['Nro_Beneficio__c'], 
#                             row['Nombre_del_beneficiario__c'],
#                             row['Cuota__c'], 
#                             row['Cod_Banco__c'], 
#                             row['Cod_Sucursal__c'],
#                             row['Nro_Cuenta__c'], 
#                             row['CBU_a_pagar__c'],
#                             row['Cuit_de_la_Cuenta__c'], 
#                             row['Titular_de_la_Cuenta__c'], 
#                             row['Area_de_Accion__c'],
#                             row['Tipo_de_Persona__c'], 
#                             row['Importe__c'],
#                             row['Cdigo_de_divisa__c'], 
#                             row['Tipo_de_Beneficio__c'],
#                             row['Tipo_de_Cambio__c'],
#                             row['Importado__c'], 
#                             row['Registrado__c']
#                             ]
#                             ) 

# conexion.commit()
# cursor.close()
# conexion.close()



