import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion import *
from datetime import datetime
from sqlalchemy import create_engine
import re



transacciones = instancia('Formularios_Becas__c')

Benef_metadata = sf.Formularios_Becas__c.describe()
df_prog_metadata = pd.DataFrame(Benef_metadata.get('fields'))
df_prog_metadata.to_csv('Formularios_Becas__c_metadata.csv', index = False)

query = '''SELECT Id, Beca__c,Beneficios_a_Personas__c, Recibo_de_sueldo_1__c, Recibo_de_sueldo_2__c, Constancia_de_inscripci_n_alumno_regular__c, Bolet_n__c, Libreta_universitaria__c FROM Formularios_Becas__c'''

sf_transacciones = sf.bulk.Formularios_Becas__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

sf_transacciones.to_csv('Formularios_Becas__c.csv', index = False)

ultima_migracion = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\nav-sf\FA-salida.csv', encoding = 'utf-8-sig')


sf_transacciones = sf_transacciones[["Id", "Beneficios_a_Personas__c"]]

sf_transacciones=sf_transacciones.merge(ultima_migracion, left_on='Beneficios_a_Personas__c', right_on='beneficio', how='left')

sf_transacciones = sf_transacciones[sf_transacciones.beneficio.notnull()]

sf_transacciones = sf_transacciones[["Id", "Recibo_de_sueldo_1__c", "Recibo_de_sueldo_2__c", "Constancia_de_inscripci_n_alumno_regular__c", "Bolet_n__c", "Libreta_universitaria__c" ]]

print(sf_transacciones)

sf_transacciones = sf_transacciones.replace(np.nan, '', regex=True)

pagos_update = sf_transacciones.to_dict('records')

print (pagos_update)
resultado = sf.bulk.Formularios_Becas__c.update(pagos_update)
print(resultado)


# sf_transacciones_resto = sf_transacciones[sf_transacciones.Marker.notnull()]

# sf_transacciones_resto = sf_transacciones_resto[["Id", "Fantasma__c"]]

# sf_transacciones_resto['Fantasma__c'] = 1

# print(sf_transacciones_resto)

# pagos_update = sf_transacciones_resto.to_dict('records')



# for i in pagos_update:
#     lista = []
#     lista.append(i)
#     print(lista)
#     #resultado = sf.bulk.Beneficios_a_Personas__c.update(lista)
#     print(resultado)
    


#print (pagos_update)
#resultado = sf.bulk.Transacciones__c.update(pagos_update)
#print(resultado)