import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re


#########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########

pagos = '''SELECT Nro_documento, Nro_beneficio, Importado as Importado__c, Registrado AS Registrado__c, 
               Nro_Documento_Pago AS Nro_Documento_Pago__c, Fecha_Importacion as Fecha_Importacion, Fecha_Registro as Fecha_Importacion__c, Fecha_AcreditacionOP AS Fecha_AcreditacionOP__c 
               FROM [Fundación Perez Companc$Interfaz Beneficios]
               WHERE Nro_Mov > 511904 and Destino = 1    
               ORDER BY Nro_Mov DESC''' 

# ###chequear destino = 1 cuando quiero actualizar fecha op, improtado = 1 cuando quiero actualizar importado 

pagos_df = datos(pagos, 'FPCBC13')


# ##Quito duplicados (siempre ordenando de manera descendente), esto por si hubo correcciones, etc

pagos_df = pagos_df.drop_duplicates(pagos_df.columns[pagos_df.columns.isin(['Nro_documento'])],
                           keep='first')

#print(pagos_df)
#pagos_df.to_csv("test_pagos_rech.csv", index = False)


########### ME TRAIGO LOS DATOS DE SALESFORCE ##############

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Destino__c, Estado__c, Cuota__c, N_mero_documento__c, Nombre_colab_f__c
                 FROM Transacciones__c'''  

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

print(sf_transacciones)
# # # # #Aplico los filtros
sf_transacciones = sf_transacciones.loc[(sf_transacciones['Cuota__c']==0)] #Las cuotas que viajaron
# #sf_transacciones = sf_transacciones.loc[(sf_transacciones['N_mero_documento__c']=='OBB0000308307')] #Las cuotas que viajaron
# #sf_transacciones = sf_transacciones.loc[(sf_transacciones['Nombre_colab_f__c']=='VITTON SERGIO ANDRES')] #Las cuotas que viajaron

print(sf_transacciones)
# #sf_transacciones.to_csv("c8_act.csv", index = False)

# # # ########## UNO LAS DOS TABLAS ##############

nav_a_sf= sf_transacciones.merge(pagos_df, left_on='N_mero_documento__c', right_on='Nro_documento', how='inner')

print(nav_a_sf)
# #nav_a_sf.to_csv("c0.csv", index = False)

# # # # # # #Transformo los datos

# # # # #nav_a_sf = nav_a_sf.astype({"Registrado__c": int, "Destino__c": int})

# # # # # # #Escribo la fecha 
# # # # # date = datetime(2021, 5, 28, 0)
# # # # # date_texto = date.strftime('%a %b %d %y')
# # # # # new_date = datetime.strptime(date_texto, '%a %b %d %y')

# # # # # # ##Sumo estos nuevos campos al DF
# # # # # nav_a_sf['Fecha_Vencimiento__c'] = new_date
# # # # nav_a_sf['Fecha_Importacion__c'] = new_date
# # # # # nav_a_sf['Fecha_Registro__c'] = new_date

# #nav_a_sf['Fecha_Importacion__c'] = nav_a_sf['Fecha_Importacion__c'].dt.strftime('%Y-%m-%d')
# #nav_a_sf['Fecha_Vencimiento__c'] = nav_a_sf['Fecha_Vencimiento__c'].dt.strftime('%Y-%m-%d')
# # # # # nav_a_sf['Fecha_Registro__c'] = nav_a_sf['Fecha_Registro__c'].dt.strftime('%Y-%m-%d')

# # nav_a_sf = nav_a_sf.where((pd.notnull(nav_a_sf)), None)
# # nav_a_sf = nav_a_sf.replace([None],'')

# # print(nav_a_sf)
# # #nav_a_sf.to_csv("c_3_act.csv", index = False)
# # # ##Elijo las columnas a exportar

# # # ##Actualizar Importado
# nav_a_sf['Importado__c']= 1
# nav_a_sf = nav_a_sf[["Id", "Importado__c"]]  ##para actualizar importado 

# print(nav_a_sf)
# nav_a_sf.to_csv('c0_act_imp_06_12.csv', index = False)

# # # ##Actualizar Fecha_OP 
nav_a_sf['Fecha_AcreditacionOP__c'] = nav_a_sf['Fecha_AcreditacionOP__c'].dt.strftime('%Y-%m-%d')
nav_a_sf['Destino__c']= 1

nav_a_sf = nav_a_sf[["Id", 'Destino__c', 'Registrado__c', "Nro_Documento_Pago__c", "Fecha_AcreditacionOP__c"
                                                 ]]  

print(nav_a_sf)
nav_a_sf.to_csv('c0_fechaop_06_12.csv', index = False)

# # ######## ENVIO A SALESFORCE #############
# transacciones = instancia('Transacciones__c')
# pagos_update = nav_a_sf.to_dict('records')

# print(pagos_update)
# resultado = sf.bulk.Transacciones__c.update(pagos_update)
# print(resultado)