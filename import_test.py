import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re


########### ME TRAIGO LOS DATOS DE SALESFORCE ##############

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Beneficio_a_Personas__c, Cuota__c, Destino__c, Importado__c, Registrado__c, Nro_documento__c
              FROM Transacciones__c'''

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

#Aplico filtros
sf_transacciones = sf_transacciones[sf_transacciones['Beneficio_a_Personas__c']=='a0v4W00000PlXM3QAN']

print(sf_transacciones)


##Genero campos con valores
# sf_transacciones['Cuota__c']= 1
# sf_transacciones['Destino__c']= 0
# sf_transacciones['Importado__c']= 0
# sf_transacciones['Registrado__c']= 0
# sf_transacciones['Tipo_de_evento__c']= 'Obligación (Beneficiario) = 03'

# #Escribo la fecha 
# date = datetime(2021, 10, 10, 0)
# date_texto = date.strftime('%a %b %d %y')
# new_date = datetime.strptime(date_texto, '%a %b %d %y')

# # # # # # ##Sumo estos nuevos campos al DF
# sf_transacciones['Fecha_Vencimiento__c'] = new_date
# sf_transacciones['Fecha_Importacion__c'] = new_date
# sf_transacciones['Fecha_Registro__c'] = new_date

# sf_transacciones['Fecha_Importacion__c'] = sf_transacciones['Fecha_Importacion__c'].dt.strftime('%Y-%m-%d')
# sf_transacciones['Fecha_Vencimiento__c'] = sf_transacciones['Fecha_Vencimiento__c'].dt.strftime('%Y-%m-%d')
# sf_transacciones['Fecha_Registro__c'] = sf_transacciones['Fecha_Registro__c'].dt.strftime('%Y-%m-%d')

# print(sf_transacciones)

######## ENVIO A SALESFORCE #############
# transacciones = instancia('Transacciones__c')
# pagos_update = nav_a_sf.to_dict('records')

# print(pagos_update)
# resultado = sf.bulk.Transacciones__c.update(pagos_update)
# print(resultado)