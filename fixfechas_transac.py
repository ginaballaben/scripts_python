import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re

## ACTUALIZAR CAMPOS DE FECHA CON INFO DE FECHA/HORA - OBJETO TRANSACCIONES
########### ME TRAIGO LOS DATOS DE SALESFORCE ##############

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, N_mero_documento__c, Cuota__c, Fecha_AcreditacionOP__c, Fecha_Documento__c,
                Fecha_Importacion_1__c, Fecha_Registro__c, Fecha_Vencimiento__c, Programa__c
                 FROM Transacciones__c'''  

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

##Aplico los filtros
sf_transacciones = sf_transacciones.loc[(sf_transacciones['Programa__c']=='a0r4W00000dIfOVQA0')] #atencion salud 2021


sf_transacciones['Fecha_Venc2__c'] = sf_transacciones['Fecha_Vencimiento__c'].dt.strftime('%Y-%m-%d')

print(sf_transacciones)
sf_transacciones.to_csv('fechs_trans_2021.csv', index = False)


# #nav_a_sf['Fecha_Registro2__c'] = nav_a_sf['Fecha_Registro'].dt.strftime('%Y-%m-%d')
# nav_a_sf['Fecha_Venc2__c'] = nav_a_sf['Fecha_Vencimiento'].dt.strftime('%Y-%m-%d')
# nav_a_sf['Fecha_Documento2__c'] = nav_a_sf['Fecha_Documento'].dt.strftime('%Y-%m-%d')
# nav_a_sf['Fecha_Importacion2__c'] = nav_a_sf['Fecha_Importacion'].dt.strftime('%Y-%m-%d')
# #nav_a_sf['Fecha_AcreditacionOP2__c'] = nav_a_sf['Fecha_AcreditacionOP'].dt.strftime('%Y-%m-%d')

# #print(nav_a_sf)

# nav_a_sf = nav_a_sf[["Id", 'Fecha_Importacion2__c',"Fecha_Documento2__c", "Fecha_Venc2__c"]]  
# print(nav_a_sf)

# nav_a_sf = nav_a_sf.replace([None],'')
# nav_a_sf.to_csv('test_fechas_venc.csv', index = False)

#Fecha_AcreditacionOP2__c, Fecha_Registro2__c
#"Fecha_Importacion2__c", "Fecha_Documento2__c", "Fecha_Venc2__c"   


# ######## ENVIO A SALESFORCE #############
# transacciones = instancia('Transacciones__c')
# pagos_update = nav_a_sf.to_dict('records')

# print(pagos_update)
# resultado = sf.bulk.Transacciones__c.update(pagos_update)
# print(resultado)
