import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re

pagos = '''SELECT Nro_Mov, Nro_documento, Fecha_Importacion, Fecha_Vencimiento, 
            Fecha_Registro, Fecha_AcreditacionOP, Fecha_Documento 
               FROM [Fundación Perez Companc$Interfaz Beneficios]
               WHERE NRO_MOV > 550000
               ORDER BY Nro_Mov DESC''' 

# ###chequear destino = 1 cuando quiero actualizar fecha op, improtado = 1 cuando quiero actualizar importado 

pagos_df = datos(pagos, 'FPCBC13')

########### ME TRAIGO LOS DATOS DE SALESFORCE ##############

interfaz = instancia('Interfaz_sf__c')

query = '''SELECT Id, Nro_Mov__c, Nro_documento__c 
             FROM Interfaz_sf__c'''

sf_interfaz = sf.bulk.Interfaz_sf__c.query(query)
sf_interfaz = pd.DataFrame(sf_interfaz)

#print(sf_interfaz)
# # # # #Aplico los filtros
sf_interfaz = sf_interfaz.loc[(sf_interfaz['Nro_Mov__c']>550000)] 

# print(sf_interfaz)

nav_a_sf= sf_interfaz.merge(pagos_df, left_on='Nro_Mov__c', right_on='Nro_Mov', how='inner')
print(nav_a_sf)
nav_a_sf.to_csv('test_fechas.csv')

#nav_a_sf['Fecha_Registro2__c'] = nav_a_sf['Fecha_Registro'].dt.strftime('%Y-%m-%d')
nav_a_sf['Fecha_Venc2__c'] = nav_a_sf['Fecha_Vencimiento'].dt.strftime('%Y-%m-%d')
nav_a_sf['Fecha_Documento2__c'] = nav_a_sf['Fecha_Documento'].dt.strftime('%Y-%m-%d')
nav_a_sf['Fecha_Importacion2__c'] = nav_a_sf['Fecha_Importacion'].dt.strftime('%Y-%m-%d')
#nav_a_sf['Fecha_AcreditacionOP2__c'] = nav_a_sf['Fecha_AcreditacionOP'].dt.strftime('%Y-%m-%d')

#print(nav_a_sf)

nav_a_sf = nav_a_sf[["Id", 'Fecha_Importacion2__c',"Fecha_Documento2__c", "Fecha_Venc2__c"]]  
print(nav_a_sf)

nav_a_sf = nav_a_sf.replace([None],'')
nav_a_sf.to_csv('test_fechas_venc.csv', index = False)

#Fecha_AcreditacionOP2__c, Fecha_Registro2__c
#"Fecha_Importacion2__c", "Fecha_Documento2__c", "Fecha_Venc2__c"   


# ######## ENVIO A SALESFORCE #############
# transacciones = instancia('Interfaz_sf__c')
# pagos_update = nav_a_sf.to_dict('records')

# print(pagos_update)
# resultado = sf.bulk.Interfaz_sf__c.update(pagos_update)
# print(resultado)
