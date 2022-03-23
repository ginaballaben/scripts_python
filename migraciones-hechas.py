import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from connection import *
from api_sf import *


cuentas = instancia('Account')

##Escuelas

query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Account'
sf_benef = sf.bulk.Account.query(query)
sf_benef = pd.DataFrame(sf_benef)
sf_benef = sf_benef[sf_benef['RecordTypeId']=='0124W000001AO14QAG']

print(sf_benef)

#sf_benef.to_csv('escuelas-migradas.csv', index = False)


#Programas

# programas = instancia('Programas__c')

# query = 'SELECT Id, Name, Area__c, C_digo_de_Beca__c, Nivel__c, Tipo_de_Beca__c, Tope_de_sueldo__c FROM Programas__c'
# sf_benef = sf.bulk.Programas__c.query(query)
# sf_benef = pd.DataFrame(sf_benef)

# sf_benef.to_csv('programas-migradas.csv', index = False)

# #Colaboradores

# colaboradores = instancia('Contact')

# query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Contact'
# sf_becades = sf.bulk.Contact.query(query)
# sf_becades = pd.DataFrame(sf_becades)
# sf_becades = sf_becades[sf_becades['RecordTypeId']=='0124W000001ANfUQAW']

# sf_becades.to_csv('colaboradores_migradas_22-12.csv', index = False)


# #Becadxs

# query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Contact'
# sf_becades = sf.bulk.Contact.query(query)
# sf_becades = pd.DataFrame(sf_becades)
# sf_becades = sf_becades[sf_becades['RecordTypeId']=='0124W000001ANfeQAG']

# sf_becades.to_csv('becadxs_migradas_22-12.csv', index = False)


# #Beneficios

# beneficios = instancia('Beneficios_a_Personas__c')

# query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Beneficios_a_Personas__c'
# sf_becades = sf.bulk.Beneficios_a_Personas__c.query(query)
# sf_becades = pd.DataFrame(sf_becades)
# sf_becades = sf_becades[sf_becades['RecordTypeId']=='0124W000001AcY9QAK']

# sf_becades.to_csv('benef_migradas.csv', index = False)


# transacciones = instancia('Transacciones__c')

# query = 'SELECT Id, Name, Id_Beneficiario__c,Nro_Beneficio__c ,Area_de_Accion__c, id_db__c FROM Transacciones__c'
# sf_becades = sf.bulk.Beneficios_a_Personas__c.query(query)
# sf_becades = pd.DataFrame(sf_becades)


# sf_becades.to_csv('Transacciones.csv', index = False)