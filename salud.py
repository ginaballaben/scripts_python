import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from api_sf import *


#### EXPLORO EL OBJETO DE SALESFORCE #####

Contactos_metadata = sf.Contact.describe()
df_contactos_metadata = pd.DataFrame(Contactos_metadata.get('fields'))
#df_contactos_metadata.to_csv('contactos_metadata.csv', index = False)


profesionales_salud = '''SELECT * from HistorialClinico'''

prof_salud = datos(profesionales_salud, 'Beneficios')

print(prof_salud)
#prof_salud.to_csv("profesionales_salud.csv", index = False)

##Instituciones salud 
instituciones_salud = prof_salud['Profesional', 'Institucion']
nan_value = float("NaN")
instituciones_salud.replace("", nan_value, inplace=True)
instituciones_salud.dropna(axis =0, inplace=True)


print(instituciones_salud)