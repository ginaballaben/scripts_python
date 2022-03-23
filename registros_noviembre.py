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

query = '''SELECT Id, Estado__c, Destino__c, Fecha_Registro__c, Fecha_Importacion__c,
        N_mero_documento__c, Id_Beneficiario__c, Fecha_Vencimiento__c, Fecha_AcreditacionOP__c,
        Forma_Pago__c, Nro_Beneficio__c,Nombre_colab_f__c, Cuota__c, 
        CBU_a_Pagar_f__c, CUIL_cuenta__c, Titular_de_la_Cuenta__c, Area_de_Accion__c,
        Importado__c, Registrado__c, Migrado_a_interfaz__c
        FROM Transacciones__c'''


sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones_test = pd.DataFrame(sf_transacciones)

#print(sf_transacciones)

##Filtros y limpieza 
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Estado__c']=='Aprobado (Pendiente ERP)') | (sf_transacciones_test['Estado__c']=='Pendiente de aprobación') | 
    (sf_transacciones_test['Estado__c']=='A reintegrar') ]
sf_transacciones_test = sf_transacciones_test.sort_values(by=['Nro_Beneficio__c'], ascending = False)

##Filtros cuotas
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Cuota__c']==9) | (sf_transacciones_test['Cuota__c']==8)]

print(sf_transacciones_test)

##Filtro destino, importado, registrado
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Destino__c']==0) ]
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Importado__c']==0) ]
sf_transacciones_test = sf_transacciones_test.loc[(sf_transacciones_test['Registrado__c']==0) ]

# #Piso Migrado a interfaz
sf_transacciones_test['Migrado_a_interfaz__c'] = 'False'

print(sf_transacciones_test)
sf_transacciones_test.to_csv('registros_noviembre.csv')

# # # # # # # #Escribo la fecha de hoy 
date2 = datetime(2021, 11, 8)
date2_texto = date2.strftime('%a %b %d %y')
new_date2 = datetime.strptime(date2_texto, '%a %b %d %y')

# # # # # # # # ##Sumo estos nuevos campos al DF
sf_transacciones_test['Fecha_Registro__c'] = new_date2
sf_transacciones_test['Fecha_Importacion__c'] = new_date2
sf_transacciones_test['Fecha_Vencimiento__c'] = new_date2

sf_transacciones_test['Fecha_Registro__c'] = sf_transacciones_test['Fecha_Registro__c'].dt.strftime('%Y-%m-%d')
sf_transacciones_test['Fecha_Importacion__c'] = sf_transacciones_test['Fecha_Importacion__c'].dt.strftime('%Y-%m-%d')
sf_transacciones_test['Fecha_Vencimiento__c'] = sf_transacciones_test['Fecha_Vencimiento__c'].dt.strftime('%Y-%m-%d')

print(sf_transacciones_test)
sf_transacciones_test.to_csv('registros_noviembre.csv')

##ENVIO
sf_transacciones_test = sf_transacciones_test[["Id", 'Destino__c', "Fecha_Registro__c", "Fecha_Importacion__c",
            "Fecha_Vencimiento__c", "Migrado_a_interfaz__c"     ]] 

sf_transacciones_test = sf_transacciones_test.head(3)

print(sf_transacciones_test)


# ######## ENVIO A SALESFORCE #############
# transacciones = instancia('Transacciones__c')
# pagos_update = sf_transacciones_test.to_dict('records')

# print(pagos_update)
# resultado = sf.bulk.Transacciones__c.update(pagos_update)
# print(resultado)