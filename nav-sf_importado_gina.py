import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re



##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########


pagos = '''SELECT Nro_Beneficio, Nro_documento, Importado FROM [Fundación Perez Companc$Interfaz Beneficios]
            where Destino = 1
            ORDER BY Nro_Mov DESC''' 

pagos_df = datos(pagos, 'FPCBC13')

#Quito duplicados (siempre ordenando de manera descendente), esto por si hubo correcciones, etc

pagos_df = pagos_df.drop_duplicates(pagos_df.columns[pagos_df.columns.isin(['Nro_documento'])],
                         keep='first')

print(pagos_df)

########### ME TRAIGO LOS DATOS DE SALESFORCE ##############


transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Estado__c, Cuota__c, N_mero_documento__c, Nro_Beneficio__c
             FROM Transacciones__c'''

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

# ## filtro comppr/cuota
sf_transacciones = sf_transacciones[(sf_transacciones['Cuota__c']==3)]

sf_transacciones = sf_transacciones[["Id", "Nro_Beneficio__c", "Cuota__c"]]


# #### UNO LAS DOS TABLAS #####

nav_a_sf=pagos_df.merge(sf_transacciones, left_on='Nro_Beneficio', right_on='Nro_Beneficio__c', how='left')

nav_a_sf = nav_a_sf.rename(columns={'Importado':'Importado__c'})

print(nav_a_sf)

#nav_a_sf.to_csv('Nav_SF_completo2.csv', index = False)

#nav_a_sf['Fecha_Documento'] = nav_a_sf['Fecha_Documento'].dt.strftime('%Y-%m-%d')
#nav_a_sf['Fecha_Importacion'] = nav_a_sf['Fecha_Importacion'].dt.strftime('%Y-%m-%d')
#nav_a_sf['Fecha_AcreditacionOP'] = nav_a_sf['Fecha_AcreditacionOP'].dt.strftime('%Y-%m-%d')



# #### TRANSFORMO LOS DATOS ####

nav_a_sf = nav_a_sf.replace([None],'')

nav_a_sf = nav_a_sf.drop(['Nro_Beneficio', 'Nro_Beneficio__c', 'Cuota__c'], axis=1)

print(nav_a_sf) 

# ###### ACTUALIZACION EN SF #######

# transacciones = instancia('Transacciones__c')

# pagos_update = nav_a_sf.to_dict('records')

# #print (pagos_update)
# # resultado = sf.bulk.Transacciones__c.update(pagos_update)
# # print(resultado)