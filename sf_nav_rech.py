import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re


########## ME TRAIGO LOS DATOS DESDE SALESFORCE  ########### 

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Estado__c, Destino__c, Cod_Evento_f__c, Fecha_Registro__c, N_mero_documento__c, Id_Beneficiario__c, Fecha_Vencimiento__c, Fecha_AcreditacionOP__c,
        Forma_Pago__c, Nro_Beneficio__c,Nombre_colab_f__c, Cuota__c, Cod_Banco_f__c,
        Cod_Sucursal_f__c, Tipo_de_Cuenta__c, Nro_Cuenta__c, CBU_a_Pagar_f__c, CUIL_cuenta__c, Titular_de_la_Cuenta__c, Area_de_Accion__c, Tipo_de_Persona__c, 
        Importe__c, Cdigo_de_divisa__c, Tipo_de_Beneficio__c, Tipo_de_Cambio__c, Importado__c, Registrado__c
        FROM Transacciones__c'''


sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones_test = pd.DataFrame(sf_transacciones)

#print(sf_transacciones)

##Filtros y limpieza 
sf_transacciones = sf_transacciones.loc[ 
    (sf_transacciones['Estado__c']=='Aprobado (Pendiente ERP)') | (sf_transacciones['Estado__c']=='Pendiente de aprobación') | 
    (sf_transacciones['Estado__c']=='A reintegrar') ]
sf_transacciones_test = sf_transacciones.sort_values(by=['Nro_Beneficio__c'], ascending = False)

##Filtros cuotas
sf_transacciones_test = sf_transacciones.loc[(sf_transacciones['Cuota__c']==9) | (sf_transacciones['Cuota__c']==8)]

print(sf_transacciones_test)

##Filtro destino, importado, registrado
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Destino__c']==0) ]
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Importado__c']==0) ]
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Registrado__c']==0) ]

#Piso Migrado a interfaz
sf_transacciones_test['Migrado_a_interfaz__c'] = 'FALSE'

# print(sf_transacciones_test)

# ##Completo codigos de evento vacios
#sf_transacciones_test['Cod_Evento_f__c'] = np.where((sf_transacciones_test['Cod_Evento_f__c'].isnull()) & (sf_transacciones_test['Cuota__c'] ==0), '01', sf_transacciones_test['Cod_Evento_f__c'])
#sf_transacciones_test['Cod_Evento_f__c'] = np.where((sf_transacciones_test['Cod_Evento_f__c'].isnull()) & (sf_transacciones_test['Cuota__c'] ==1), '03', sf_transacciones_test['Cod_Evento_f__c'])
#sf_transacciones_test['Cod_Evento_f__c'] = np.where((sf_transacciones_test['Cod_Evento_f__c'].isnull()) & (sf_transacciones_test['Cuota__c'] ==2), '03', sf_transacciones_test['Cod_Evento_f__c'])


# # # # # # #Escribo la fecha de hoy 
date = datetime(2021, 11, 8, 0)
date_texto = date.strftime('%a %b %d %y')
new_date = datetime.strptime(date_texto, '%a %b %d %y')

# # # date2 = datetime(2021, 6, 11, 0)
# # # date2_texto = date2.strftime('%a %b %d %y')
# # # new_date2 = datetime.strptime(date2_texto, '%a %b %d %y')

# # # # # # ##Sumo estos nuevos campos al DF
sf_transacciones_test['Fecha_Registro__c'] = new_date
sf_transacciones_test['Fecha_Importacion__c'] = new_date
sf_transacciones_test['Fecha_Vencimiento__c'] = new_date

# # # # # ##Limpio estos campos de comas y los pongo en mayúscuas. Trunco según especificaciones del SQL Server
# sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.strip()
# sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.replace(',', '')
# sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.upper()
# sf_transacciones_test['Titular_de_la_Cuenta__c'] = sf_transacciones_test['Titular_de_la_Cuenta__c'].str.slice(0,30)

# sf_transacciones_test['Nombre_colab_f__c'] = sf_transacciones_test['Nombre_colab_f__c'].str.strip()
# sf_transacciones_test['Nombre_colab_f__c'] = sf_transacciones_test['Nombre_colab_f__c'].str.replace(',', '')
# sf_transacciones_test['Nombre_colab_f__c'] = sf_transacciones_test['Nombre_colab_f__c'].str.upper()

# sf_transacciones_test['Tipo_de_Cuenta__c'] = sf_transacciones_test['Tipo_de_Cuenta__c'].str.slice(0,50)

# print(sf_transacciones_test)
#sf_transacciones_test.to_csv('cuotas_6_09_08.csv', index = False)

# # ### MANDAR COMPROMISOS primero, luego OBLIGACIONES
# sf_transacciones_test = sf_transacciones_test[sf_transacciones_test['Cuota__c']==3]
#sf_transacciones_test = sf_transacciones_test.head(5)# Si quiero mandar primero para probar

# #print(sf_transacciones_test)
# sf_transacciones_test.to_csv('cuota_carrion_04_06.csv', index = False) ## 20 c0, 21 c3

# ##### INSERTAR DATOS EN EL SQL SERVER ####

# conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                                      "Server=FPC14;"
#                                                      "DATABASE=FPC-Migrada;"   #Esto es lo unico que cambia
#                                                      "Trusted_Connection=yes;")
# cursor = conexion.cursor()
# _aux = []
# for index, row in sf_transacciones_test.iterrows():
#                      _aux.append(tuple([row['Destino__c'], 
#                                           row['Cod_Evento_f__c'], 
#                                           row['Fecha_Registro'],
#                                           row['N_mero_documento__c'],
#                                           row['Id_Beneficiario__c'],   
#                                           row['Fecha_Vencimiento'],
#                                           row['Forma_Pago__c'], 
#                                           row['Nro_Beneficio__c'],
#                                           row['Nombre_colab_f__c'],
#                                           row['Cuota__c'], 
#                                           row['Cod_Banco_f__c'], 
#                                           row['Cod_Sucursal_f__c'],
#                                           row['Tipo_de_Cuenta__c'],
#                                           row['Nro_Cuenta__c'], 
#                                           row['CBU_a_Pagar_f__c'],
#                                           row['CUIL_cuenta__c'], 
#                                           row['Titular_de_la_Cuenta__c'],
#                                           row['Area_de_Accion__c'],
#                                           row['Tipo_de_Persona__c'], 
#                                           row['Importe__c'],
#                                           row['Cdigo_de_divisa__c'], 
#                                           row['Tipo_de_Beneficio__c'],
#                                           row['Tipo_de_Cambio__c'],
#                                           row['Importado__c'], 
#                                           row['Registrado__c']                                                      
#                                           ]))     

# cursor.executemany('''INSERT INTO dbo.[Fundación Perez Companc$Interfaz Beneficios]([Destino], 
#                                 [Tipo_de_Evento], [Fecha_Registro], [Nro_documento], [Beneficiario], [Fecha_Vencimiento], [Cod_Forma_Pago], [Nro_Beneficio], 
#                                 [Nombre_beneficiario], [Cuota], [Banco], [Sucursal], [Tipo_de_Cuenta], [Nro_Cuenta], [CBU_a_Pagar], [CUIT_de_la_cuenta], [Titular_de_la_cuenta], 
#                                 [Area_de_Accion], [Tipo_de_Persona],  [Importe],  [Cod_Divisa], [Tipo_de_Beneficio], [Tipo_cambio], [Importado], [Registrado])    
#                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple(_aux))  
# conexion.commit()
# cursor.close()
# conexion.close()
