import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re

########## ME TRAIGO LOS DATOS DESDE SALESFORCE  ########### 

# transacciones = instancia('Transacciones__c')

# query = '''SELECT Id, Estado__c, Destino__c, N_m_documento__c, Nro_Beneficio__c, Nombre_del_beneficiario__c , Cuota__c, Importado__c, Registrado__c 
#         FROM Transacciones__c'''
# sf_transacciones = sf.bulk.Transacciones__c.query(query)
# sf_transacciones = pd.DataFrame(sf_transacciones)

# #Aplico los filtros
# sf_transacciones = sf_transacciones[sf_transacciones['Importado__c']==1]
# print(sf_transacciones)    
# sf_transacciones.to_csv('imp_1_sf.csv', index = False)

sf_importado = pd.read_csv('sf_importado.csv', encoding = 'utf-8-sig', sep = ';')  ##8216
print(sf_importado)

# # ##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########

registros = '''SELECT Nro_mov, Nro_Beneficio, Nro_Documento, Destino, Cuota, Importado, Fecha_Registro
              FROM [Fundación Perez Companc$Interfaz Beneficios]
              WHERE Destino = 1 and Importado = 0
              ORDER BY Nro_Mov DESC'''

registros = datos(registros, 'FPCBC13')
print(registros) ## 72
registros.to_csv('imp_1_sql.csv', index = False)


### MATCH 
registros_sql = registros.merge(sf_importado, left_on = 'Nro_Documento', right_on = 'Nro documento', how = 'inner')

print(registros_sql)
registros_sql.to_csv('importado_1_match.csv', index = False)   ## 8144

# # ##Quito duplicados
# # #registros_sql = registros_sql.drop_duplicates(registros_sql.columns[registros_sql.columns.isin(['N_m_documento__c'])], keep='last')

# # ## selecciono columnas 
registros_sql_act = registros_sql[['Nro_Documento', 'Importado_y']]
registros_sql_act =  registros_sql_act.rename(columns={'Importado_y': 'Importado'})
print(registros_sql_act)
registros_sql_act.to_csv('registros_act_importado.csv', index = False)

##TEST 
# #registros_sql_act = registros_sql_act.loc[registros_sql_act['Nro_Documento'] == 'OBB0000298370'] 
#test = registros_sql_act.head()
#print(test)

##### ACTUALZIAR DATOS EN EL SQL SERVER ####

# conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                                        "Server=FPC14;"
#                                                        "DATABASE=FPC-BC13;"   #Esto es lo unico que cambia
#                                                        "Trusted_Connection=yes;")

# cursor = conexion.cursor()

# ##modificar nombre del archivo 

# ## actualización masiva
# cursor.execute('''UPDATE dbo.[Fundación Perez Companc$Interfaz Beneficios] set Importado = 1 where Nro_documento in (\'{}\');  '''.format('\',\''.join(registros_sql_act['Nro_Documento'])))  

# #actualización pocos casos
# # #cursor.execute('''UPDATE dbo.[Fundación Perez Companc$Interfaz Beneficios] set Importado = 1 where Nro_documento in ('OBB0000296685', 'COM0000307854')''')

# conexion.commit()
# cursor.close()
# conexion.close()