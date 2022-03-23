import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime


##Programas ya migrados

# programas = instancia('Programas__c')

# query = """SELECT Id, Name, Area__c, C_digo_de_Beca__c, Tipo_de_Beca__c, Nivel__c, Objetivo__c,
#             Fecha_de_Inicio__c, Fecha_de_Fin__c, Fecha_de_pago_1__c,  Fecha_de_pago_10__c,
#             Monto_de_la_Beca__c, Presupuesto_Planificado__c, Tope_de_sueldo__c, Contacto_Principal_Programa__c, Territorialidad__c, RecordTypeId FROM Programas__c """

# sf_programas = sf.bulk.Programas__c.query(query)
# sf_programas = pd.DataFrame(sf_programas)
#sf_programas.to_csv('programas-migrados.csv', index = False)
#print(sf_programas)

##Defino nuevos programas de salud

progr_salud = [['Atención de la Salud - 2010'], ['Atención de la Salud - 2011'],
                ['Atención de la Salud - 2012'], ['Atención de la Salud - 2013'], ['Atención de la Salud - 2014'],
                ['Atención de la Salud - 2015'], ['Atención de la Salud - 2016'], ['Atención de la Salud - 2017'],
                ['Atención de la Salud - 2018'], ['Atención de la Salud - 2019'], ['Atención de la Salud - 2020']
        ]

progr_df = pd.DataFrame(progr_salud, columns = ['Name'])

print(progr_df)
## Fechas de inicio y fin  

fecha_inicio_20 = datetime(2020, 4, 1, 0)
date_texto = fecha_inicio_20.strftime('%a %b %d %y')
fecha_inicio_20 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_19 = datetime(2019, 4, 1, 0)
date_texto = fecha_inicio_19.strftime('%a %b %d %y')
fecha_inicio_19 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_18 = datetime(2018, 4, 1, 0)
date_texto = fecha_inicio_18.strftime('%a %b %d %y')
fecha_inicio_18 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_17 = datetime(2017, 4, 1, 0)
date_texto = fecha_inicio_17.strftime('%a %b %d %y')
fecha_inicio_17 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_16 = datetime(2016, 4, 1, 0)
date_texto = fecha_inicio_16.strftime('%a %b %d %y')
fecha_inicio_16 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_15 = datetime(2015, 4, 1, 0)
date_texto = fecha_inicio_15.strftime('%a %b %d %y')
fecha_inicio_15 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_14 = datetime(2014, 4, 1, 0)
date_texto = fecha_inicio_14.strftime('%a %b %d %y')
fecha_inicio_14 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_13 = datetime(2013, 4, 1, 0)
date_texto = fecha_inicio_13.strftime('%a %b %d %y')
fecha_inicio_13 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_12 = datetime(2012, 4, 1, 0)
date_texto = fecha_inicio_12.strftime('%a %b %d %y')
fecha_inicio_12 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_11 = datetime(2011, 4, 1, 0)
date_texto = fecha_inicio_11.strftime('%a %b %d %y')
fecha_inicio_11 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_inicio_10 = datetime(2010, 4, 1, 0)
date_texto = fecha_inicio_10.strftime('%a %b %d %y')
fecha_inicio_10 = datetime.strptime(date_texto, '%a %b %d %y')


fechas_inicios = [fecha_inicio_10, fecha_inicio_11,
                 fecha_inicio_12, fecha_inicio_13, fecha_inicio_14,
                 fecha_inicio_15, fecha_inicio_16, fecha_inicio_17,
                 fecha_inicio_18, fecha_inicio_19, fecha_inicio_20
         ]

progr_df['Fecha_de_Inicio__c'] = fechas_inicios

fecha_fin_20 = datetime(2021, 3, 31, 0)
date_texto = fecha_fin_20.strftime('%a %b %d %y')
fecha_fin_20 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_19 = datetime(2020, 3, 31, 0)
date_texto = fecha_fin_19.strftime('%a %b %d %y')
fecha_fin_19 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_18 = datetime(2019, 3, 31, 0)
date_texto = fecha_fin_18.strftime('%a %b %d %y')
fecha_fin_18 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_17 = datetime(2018, 3, 31, 0)
date_texto = fecha_fin_17.strftime('%a %b %d %y')
fecha_fin_17 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_16 = datetime(2017, 3,31, 0)
date_texto = fecha_fin_16.strftime('%a %b %d %y')
fecha_fin_16 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_15 = datetime(2016, 3, 31, 0)
date_texto = fecha_fin_15.strftime('%a %b %d %y')
fecha_fin_15 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_14 = datetime(2015, 3, 31, 0)
date_texto = fecha_fin_14.strftime('%a %b %d %y')
fecha_fin_14 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_13 = datetime(2014, 3, 31, 0)
date_texto = fecha_fin_13.strftime('%a %b %d %y')
fecha_fin_13 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_12 = datetime(2013, 3,31, 0)
date_texto = fecha_fin_12.strftime('%a %b %d %y')
fecha_fin_12 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_11 = datetime(2012,3,31, 0)
date_texto = fecha_fin_11.strftime('%a %b %d %y')
fecha_fin_11 = datetime.strptime(date_texto, '%a %b %d %y')

fecha_fin_10 = datetime(2011, 3,31, 0)
date_texto = fecha_fin_10.strftime('%a %b %d %y')
fecha_fin_10 = datetime.strptime(date_texto, '%a %b %d %y')

fechas_fin = [fecha_fin_10, fecha_fin_11,
                 fecha_fin_12, fecha_fin_13, fecha_fin_14,
                 fecha_fin_15, fecha_fin_16, fecha_fin_17,
                 fecha_fin_18, fecha_fin_19, fecha_fin_20
         ]

progr_df['Fecha_de_Fin__c'] = fechas_fin

print(fecha_fin_10)
print(type(fecha_fin_10))

progr_df['Fecha_de_Inicio__c'] = progr_df['Fecha_de_Inicio__c'].dt.strftime('%Y-%m-%d')
progr_df['Fecha_de_Fin__c'] = progr_df['Fecha_de_Fin__c'].dt.strftime('%Y-%m-%d')

print(progr_df['Fecha_de_Fin__c'])
print(type(progr_df['Fecha_de_Fin__c']))

# ##RecordType
# progr_df['RecordTypeId'] = '0124W000001AUvAQAW' 
# progr_df['Area__c'] = 'SALUD' 

# # print(progr_df)
# progr_df.to_csv('programas_a_migrar.csv', index = False)

# # ## Inserto en Salesforce
# programas = instancia('Programas__c')
# progr_df = progr_df.to_dict('records')
# resultado = sf.bulk.Programas__c.insert(progr_df)
# print(resultado)