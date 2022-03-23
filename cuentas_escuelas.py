import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from connection import *
from api_sf import *


cuentas = instancia('Account')

escuelas = """ SELECT Nombre AS Name, Ámbito AS Ambito__c,Jurisdicción AS ProvinciaFPC__c, [CUE Anexo] AS CUE__c, Sector AS Tipo_de_Gestion__c, Domicilio AS ShippingStreet, CP AS ShippingPostalCode, 
		        Localidad AS ShippingCity, [Ed# Especial] AS [Escuela Especial], Primaria AS [Escuela Primaria],
		        Secundaria AS [Escuela Secundaria], [Ed# Adultos] AS [Escuela de Adultos], [Secundaria Técnica (INET)] AS [Escuela Técnica], InstitucionEducativa.Director AS Nombre_y_Apellido_de_la_Directora__c, 
				InstitucionEducativa.Id AS id_db__c

                FROM InstitucionEducativa

                LEFT JOIN InstitucionEducativaCUE ON InstitucionEducativaCUE.[CUE Anexo] = InstitucionEducativa.CUE
                
                WHERE Nombre IS NOT NULL AND InstitucionEducativa.Id IN (
				
					SELECT InstitucionEducativa.Id FROM InstitucionEducativa

					inner join Beca ON InstitucionEducativa.Id = beca.InstitucionEducativa_id
				
				) """


escuelas_resto = """ SELECT Nombre AS Name, Ámbito AS Ambito__c,Jurisdicción AS ProvinciaFPC__c, [CUE Anexo] AS CUE__c, Sector AS Tipo_de_Gestion__c, Domicilio AS ShippingStreet, CP AS ShippingPostalCode, 
		        Localidad AS ShippingCity, [Ed# Especial] AS [Escuela Especial], Primaria AS [Escuela Primaria],
		        Secundaria AS [Escuela Secundaria], [Ed# Adultos] AS [Escuela de Adultos], [Secundaria Técnica (INET)] AS [Escuela Técnica], InstitucionEducativa.Director AS Nombre_y_Apellido_de_la_Directora__c, 
				InstitucionEducativa.Id AS id_db__c

                FROM InstitucionEducativa

                LEFT JOIN InstitucionEducativaCUE ON InstitucionEducativaCUE.[CUE Anexo] = InstitucionEducativa.CUE
                
                WHERE Nombre IS NOT NULL AND InstitucionEducativa.Id NOT IN (
				
					SELECT InstitucionEducativa.Id FROM InstitucionEducativa

					inner join Beca ON InstitucionEducativa.Id = Beca.InstitucionEducativa_id) """


escuelas_datos_faltantes = """ SELECT Nombre AS Name, Ámbito AS Ambito__c,Jurisdicción AS ProvinciaFPC__c, [CUE Anexo] AS CUE__c, Sector AS Tipo_de_Gestion__c, Domicilio AS ShippingStreet, CP AS ShippingPostalCode, 
		        Localidad AS ShippingCity, [Ed# Especial] AS [Escuela Especial], Primaria AS [Escuela Primaria],
		        Secundaria AS [Escuela Secundaria], [Ed# Adultos] AS [Escuela de Adultos], [Secundaria Técnica (INET)] AS [Escuela Técnica], InstitucionEducativa.Director AS Nombre_y_Apellido_de_la_Directora__c, 
				InstitucionEducativa.Id AS id_db__c

                FROM InstitucionEducativa

                LEFT JOIN InstitucionEducativaCUE ON InstitucionEducativaCUE.[CUE Anexo] = InstitucionEducativa.CUE
                
                WHERE Nombre IS NULL AND InstitucionEducativa.Id NOT IN (
				
					SELECT InstitucionEducativa.Id FROM InstitucionEducativa

					inner join Beca ON InstitucionEducativa.Id = Beca.InstitucionEducativa_id) """

escuelas_otros = """ SELECT Nombre AS Name, Ámbito AS Ambito__c,Jurisdicción AS ProvinciaFPC__c, [CUE Anexo] AS CUE__c, Sector AS Tipo_de_Gestion__c, Domicilio AS ShippingStreet, CP AS ShippingPostalCode, 
		        Localidad AS ShippingCity, [Ed# Especial] AS [Escuela Especial], Primaria AS [Escuela Primaria],
		        Secundaria AS [Escuela Secundaria], [Ed# Adultos] AS [Escuela de Adultos], [Secundaria Técnica (INET)] AS [Escuela Técnica], InstitucionEducativa.Director AS Nombre_y_Apellido_de_la_Directora__c, 
				InstitucionEducativa.Id AS id_db__c

                FROM InstitucionEducativa

                LEFT JOIN InstitucionEducativaCUE ON InstitucionEducativaCUE.[CUE Anexo] = InstitucionEducativa.CUE
                
                WHERE  InstitucionEducativa.Id <> 8764 AND 
						InstitucionEducativa.Id <> 36285 AND
						InstitucionEducativa.Id <> 31398 AND
						InstitucionEducativa.Id  IN (
				SELECT distinct  InstitucionEducativa.Id AS id_escuelas

				FROM BECA 
				INNER JOIN InstitucionEducativa ON InstitucionEducativa.Id = Beca.InstitucionEducativa_id
				INNER JOIN DESTINATARIOFINAL DF ON BECA.DESTINATARIOFINAL_ID=DF.ID
				INNER JOIN BENEFICIO BEN ON DF.BENEFICIO_ID=BEN.ID
				INNER JOIN BENEFICIARIO BENEF ON BEN.BENEFICIARIO_ID = BENEF.ID
				INNER JOIN EMPLEADO EMP ON BECA.EMPLEADO_ID=EMP.ID
				INNER JOIN EMPRESA E ON E.ID = EMP.EMPRESA_ID
				INNER JOIN BENEFICIARIO BECADO ON DF.BENEFICIARIO_ID = BECADO.ID 
				LEFT JOIN Domicilio ON BECADO.Domicilio_id = Domicilio.Id
				LEFT JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id
				LEFT JOIN Provincia ON Localidad.Provincia_id = Provincia.Id
				LEFT JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id
				LEFT JOIN EstudiosAlcanzados ON BECADO.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id
				LEFT JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = BECADO.Id


				WHERE BEN.MotivoBeneficio_id <> 5) """

escuelas_df = datos(escuelas_otros, 'Beneficios')

#print(escuelas_df.columns)
#Pivotear columnas. Entre parentesis selecciono las que se mantienen (no lo voy a usar)
#escuelas_df = pd.melt(escuelas_df, id_vars=['Name', 'ProvinciaFPC__c', 'CUE__c', 'Tipo_de_Gestion__c', 'ShippingAddress', 'ShippingPostalCode', 'Phone', 'Correo_electr_nico__c', 'ShippingCity', 'Nombre_y_Apellido_de_la_Directora__c', 'id_db__c'], var_name= 'Tipo_de_Escuela__c', value_name = 'value')

escuelas = ['Escuela Especial', 'Escuela Primaria', 'Escuela Secundaria', 'Escuela de Adultos', 'Escuela Técnica']

#Modificar tipo de datos y reemplazar valores
for i in escuelas:
    escuelas_df.loc[escuelas_df[i] == 'X', i] = i

print(escuelas_df.dtypes)


#Ordeno por nombre
escuelas_df.sort_values('Name', inplace = True)

#Filtro por valores no nulos
#escuelas_df = escuelas_df[escuelas_df['value'] != 'nan']

#Quito duplicados
escuelas_df.drop_duplicates(subset = 'id_db__c', keep = False, inplace = True)

#Concatenar 

def concat(*args):
    strs = [str(arg) for arg in args if not pd.isnull(arg)]
    return ';'.join(strs) if strs else np.nan

np_concat = np.vectorize(concat)

escuelas_df['Tipo_de_Escuela__c'] = np_concat(escuelas_df['Escuela Especial'], escuelas_df['Escuela Primaria'], escuelas_df['Escuela Secundaria'], escuelas_df['Escuela de Adultos'],
        escuelas_df['Escuela Técnica'])


escuelas_df = escuelas_df.drop(columns = escuelas)


#Reemplazo valores para que coincidan con la picklist de SF 

escuelas_df.loc[escuelas_df['ProvinciaFPC__c'] == 'Buenos Aires', 'ProvinciaFPC__c'] = 'Provincia de Buenos Aires'
escuelas_df.loc[escuelas_df['Tipo_de_Gestion__c'] == 'Privado', 'Tipo_de_Gestion__c'] = 'Privada'


escuelas_df = escuelas_df.where((pd.notnull(escuelas_df)), None)
escuelas_df = escuelas_df.replace('nan','')
escuelas_df = escuelas_df.replace([None],'')




print(escuelas_df)

#Migracion SF 

escuelas_df['RecordTypeId'] = '0124W000001AO14QAG'

# escuelas_df.to_csv('escuelas-resto.csv', index = False)

escuelas_dic = escuelas_df.to_dict('records')

print(escuelas_dic)

resultado = sf.bulk.Account.insert(escuelas_dic) 

print(resultado)  