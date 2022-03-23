import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from connection import *
from api_sf import *

programas = instancia('Programas__c')

Programas_metadata = sf.Programas__c.describe()
df_prog_metadata = pd.DataFrame(Programas_metadata.get('fields'))
df_prog_metadata.to_csv('prog_metadata.csv', index = False)


data = [['Programa de Becas FPC - 2010','Primaria'], ['Programa de Becas FPC - 2011', 'Primaria'], ['Programa de Becas FPC - 2012', 'Primaria'],
        ['Programa de Becas FPC - 2013', 'Primaria'], ['Programa de Becas FPC - 2014', 'Primaria'], ['Programa de Becas FPC - 2015', 'Primaria'],
        ['Programa de Becas FPC - 2016', 'Primaria'], ['Programa de Becas FPC - 2017', 'Primaria'], ['Programa de Becas FPC - 2018', 'Primaria'],
        ['Programa de Becas FPC - 2019', 'Primaria'], ['Programa de Becas FPC - 2020', 'Primaria'], ['Programa de Becas FPC - 2021', 'Primaria'],
        ['Programa de Becas FPC - 2010','Secundaria 1° año'], ['Programa de Becas FPC - 2011', 'Secundaria 1° año'], ['Programa de Becas FPC - 2012', 'Secundaria 1° año'],
        ['Programa de Becas FPC - 2013', 'Secundaria 1° año'], ['Programa de Becas FPC - 2014', 'Secundaria 1° año'], ['Programa de Becas FPC - 2015', 'Secundaria 1° año'],
        ['Programa de Becas FPC - 2016', 'Secundaria 1° año'], ['Programa de Becas FPC - 2017', 'Secundaria 1° año'], ['Programa de Becas FPC - 2018', 'Secundaria 1° año'],
        ['Programa de Becas FPC - 2019', 'Secundaria 1° año'], ['Programa de Becas FPC - 2020', 'Secundaria 1° año'], ['Programa de Becas FPC - 2021', 'Secundaria 1° año'],
        ['Programa de Becas FPC - 2010','Secundaria a partir de 4°'], ['Programa de Becas FPC - 2011', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2012', 'Secundaria a partir de 4°'],
        ['Programa de Becas FPC - 2013', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2014', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2015', 'Secundaria a partir de 4°'],
        ['Programa de Becas FPC - 2016', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2017', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2018', 'Secundaria a partir de 4°'],
        ['Programa de Becas FPC - 2019', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2020', 'Secundaria a partir de 4°'], ['Programa de Becas FPC - 2021', 'Secundaria a partir de 4°'],
        ['Programa de Becas FPC - 2010','Superior 1° año'], ['Programa de Becas FPC - 2011', 'Superior 1° año'], ['Programa de Becas FPC - 2012', 'Superior 1° año'],
        ['Programa de Becas FPC - 2013', 'Superior 1° año'], ['Programa de Becas FPC - 2014', 'Superior 1° año'], ['Programa de Becas FPC - 2015', 'Superior 1° año'],
        ['Programa de Becas FPC - 2016', 'Superior 1° año'], ['Programa de Becas FPC - 2017', 'Superior 1° año'], ['Programa de Becas FPC - 2018', 'Superior 1° año'],
        ['Programa de Becas FPC - 2019', 'Superior 1° año'], ['Programa de Becas FPC - 2020', 'Superior 1° año'], ['Programa de Becas FPC - 2021', 'Superior 1° año'],
        ['Programa de Becas FPC - 2010','Superior <50%'], ['Programa de Becas FPC - 2011', 'Superior <50%'], ['Programa de Becas FPC - 2012', 'Superior <50%'],
        ['Programa de Becas FPC - 2013', 'Superior <50%'], ['Programa de Becas FPC - 2014', 'Superior <50%'], ['Programa de Becas FPC - 2015', 'Superior <50%'],
        ['Programa de Becas FPC - 2016', 'Superior <50%'], ['Programa de Becas FPC - 2017', 'Superior <50%'], ['Programa de Becas FPC - 2018', 'Superior <50%'],
        ['Programa de Becas FPC - 2019', 'Superior <50%'], ['Programa de Becas FPC - 2020', 'Superior <50%'], ['Programa de Becas FPC - 2021', 'Superior <50%'],
        ['Programa de Becas FPC - 2010','Superior >50%'], ['Programa de Becas FPC - 2011', 'Superior >50%'], ['Programa de Becas FPC - 2012', 'Superior >50%'],
        ['Programa de Becas FPC - 2013', 'Superior >50%'], ['Programa de Becas FPC - 2014', 'Superior >50%'], ['Programa de Becas FPC - 2015', 'Superior >50%'],
        ['Programa de Becas FPC - 2016', 'Superior >50%'], ['Programa de Becas FPC - 2017', 'Superior >50%'], ['Programa de Becas FPC - 2018', 'Superior >50%'],
        ['Programa de Becas FPC - 2019', 'Superior >50%'], ['Programa de Becas FPC - 2020', 'Superior >50%'], ['Programa de Becas FPC - 2021', 'Superior >50%'] 
        ] 


data_merito = [['Programa de Becas FPC - 2021','Mérito'], ['Programa de Becas FPC - 2020', 'Mérito']]

prog_df = pd.DataFrame(data_merito, columns = ['Name', 'Tipo_de_Beca__c'])

prog_df['RecordTypeId'] = '0124W000001AUv0QAG'
prog_df['Area__c'] = 'Educación'

#prog_df.to_csv('programas_migrados.csv', index = False)

prog_dict = prog_df.to_dict('records')

resultado = sf.bulk.Programas__c.insert(prog_dict)

print(prog_df)

print(resultado)