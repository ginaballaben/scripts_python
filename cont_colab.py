import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from api_sf import *


contactos = instancia('Contact') 



colaboradores = """ SELECT Empleado.Establecimiento_id AS Planta, empleado.FechaIngreso AS Fecha_de_ingreso_a_la_compania__c, Empleado.NumLegajo AS Num_legajo__c, CargoEmpleado.Nombre AS Ocupacin_y_tareas_que_lleva_a_cabo__c,Beneficiario.Id AS id_db__c ,Beneficiario.Nombre_Razon AS FirstName, Beneficiario.Apellido AS LastName, Beneficiario.FechaNacimiento AS Birthdate, Beneficiario.NumDocumento AS DNI__c, Beneficiario.Cuil_Cuit AS Cuil__c,
				Beneficiario.Genero AS Genero__c, Beneficiario.Tel AS HomePhone, Beneficiario.Cel AS MobilePhone, Beneficiario.Email AS Email,Domicilio.Direccion AS MailingStreet,
				Localidad.Nombre AS MailingCity, Provincia.Nombre AS ProvinciaDepartamento__c, CodigoPostal.Nombre AS MailingPostalCode, EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c, FormaPagoBeneficiario.Banco, FormaPagoBeneficiario.CBU AS CBU__c

				FROM Empleado

				INNER JOIN CargoEmpleado ON Empleado.CargoEmpleado_Id = CargoEmpleado.Id

				INNER JOIN Beneficiario ON Empleado.Beneficiario_id = Beneficiario.Id

				INNER JOIN Domicilio ON Beneficiario.Domicilio_id = Domicilio.Id

				INNER JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id

				INNER JOIN Provincia ON Localidad.Provincia_id = Provincia.Id

				INNER JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id

				INNER JOIN EstudiosAlcanzados ON Beneficiario.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id

				INNER JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = Beneficiario.Id

				INNER JOIN Beneficio ON Beneficio.Beneficiario_id = Beneficiario.Id

				WHERE Beneficio.MotivoBeneficio_id <> 5 """

colaboradores = datos(colaboradores, 'Beneficios')
print(colaboradores)
#colaboradores.to_csv("colaboradores_1.csv", index = False)

colaboradores_2 = """SELECT Empleado.Establecimiento_id AS Planta, empleado.FechaIngreso AS Fecha_de_ingreso_a_la_compania__c, Empleado.NumLegajo AS Num_legajo__c, CargoEmpleado.Nombre AS Ocupacin_y_tareas_que_lleva_a_cabo__c,Beneficiario.Id AS id_db__c ,Beneficiario.Nombre_Razon AS FirstName, Beneficiario.Apellido AS LastName, Beneficiario.FechaNacimiento AS Birthdate, Beneficiario.NumDocumento AS DNI__c, Beneficiario.Cuil_Cuit AS Cuil__c,
 				Beneficiario.Genero AS Genero__c, Beneficiario.Tel AS HomePhone, Beneficiario.Cel AS MobilePhone, Beneficiario.Email AS Email,Domicilio.Direccion AS MailingStreet,
 				Localidad.Nombre AS MailingCity, Provincia.Nombre AS ProvinciaDepartamento__c, CodigoPostal.Nombre AS MailingPostalCode, EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c, FormaPagoBeneficiario.Banco, FormaPagoBeneficiario.CBU AS CBU__c

 				FROM Empleado

 				LEFT JOIN CargoEmpleado ON Empleado.CargoEmpleado_Id = CargoEmpleado.Id

 				INNER JOIN Beneficiario ON Empleado.Beneficiario_id = Beneficiario.Id

 				LEFT JOIN Domicilio ON Beneficiario.Domicilio_id = Domicilio.Id

 				LEFT JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id

 				LEFT JOIN Provincia ON Localidad.Provincia_id = Provincia.Id

 				LEFT JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id

 				LEFT JOIN EstudiosAlcanzados ON Beneficiario.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id

				LEFT JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = Beneficiario.Id

 				INNER JOIN Beneficio ON Beneficio.Beneficiario_id = Beneficiario.Id

 				WHERE Beneficiario.Id IN (
 				select Empleado.Beneficiario_id AS empleade_id
 					from Beca

 					inner join DestinatarioFinal on DestinatarioFinal.Id = beca.DestinatarioFinal_id
 					inner join Empleado on beca.Empleado_Id = Empleado.Id
 					inner join BeneficioVersion on Beca.BeneficioVersion_id = BeneficioVersion.Id
 					inner join Beneficio on BeneficioVersion.Beneficio_id = Beneficio.Id
 					left join Erogacion on Erogacion.PlanCuotas_id = beca.PlanCuotas_id
 					left join CondicionBeca on beca.CondicionBeca_id = CondicionBeca.Id
 					left join NivelEducativo on beca.NivelEducativo_id = NivelEducativo.Id
 					left join EstadoBeca on beca.EstadoBeca_id = EstadoBeca.Id
 					left join TipoTurno on beca.TipoTurno_id = TipoTurno.Id
 					left join InstitucionEducativa on beca.InstitucionEducativa_id = InstitucionEducativa.Id
 					left join TipoInstitucionEducativa on InstitucionEducativa.TipoInstitucionEducativa_id = TipoInstitucionEducativa.Id
 					where
 					year (Beneficio.FechaPedido) >=2010 
 				)				
  				"""

colaboradores_df = datos(colaboradores_2, 'Beneficios')
print(colaboradores_df)
#colaboradores_df.to_csv("colaboradores_2.csv", index = False)

bancos = "select Entidad AS Banco__c, Codigo from [Fundación Perez Companc$Entidades]"
bancos_df = datos(bancos, 'FPCBC13')

colaboradores_df = colaboradores_df.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')

colaboradores_df = colaboradores_df.drop_duplicates(colaboradores_df.columns[colaboradores_df.columns.isin(['id_db__c'])],
                            keep='first')
colaboradores_df.to_csv("colaboradores_totales_unicos.csv", index = False)  ##2831 
print(colaboradores_df)

# ### Filtro colaboradores

# # primer_migracion = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\funciones\colaboradores_migradas_22-12.csv', encoding = 'utf-8-sig')
colab_salesforce = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\colab_sf.csv',encoding = 'utf-8-sig', sep = ';')  ##3086

colab_salesforce = colab_salesforce[['id_db__c']]

colab_salesforce.rename(columns={'id_db__c': 'id_mig'}, inplace=True)

print(colab_salesforce)

colaboradores_df = colaboradores_df.merge(colab_salesforce, left_on='id_db__c', right_on='id_mig', how='left')   ##me quedo con los que falta migrar

colaboradores_df = colaboradores_df[colaboradores_df.id_mig.isnull()]

print(colaboradores_df) 
colaboradores_df.to_csv('colaboradores_match_sf_df.csv', index = False)   # 9 algunos estan en sf , otros no.
print(colaboradores_df.dtypes)

colaboradores_df = colaboradores_df.drop_duplicates(colaboradores_df.columns[colaboradores_df.columns.isin(['id_db__c'])],
                              keep='last')
print(colaboradores_df)
colaboradores_df.to_csv('colaboradores_totales_a_migrar.csv', index = False) 

query = 'SELECT Id, Name, id_db__c, ParentId FROM Account'
sf_companias = sf.bulk.Account.query(query)
sf_companias = pd.DataFrame(sf_companias)

# #Filtro valores nulos
sf_plantas = sf_companias[sf_companias.ParentId.notnull()]

print(sf_plantas)
#print(sf_plantas.dtypes)

# colaboradores_df.to_csv('colaboradores.csv', index = False)
sf_companias.to_csv('companias.csv', index = False)
sf_plantas.to_csv('plantas.csv', index = False)

# # ##### Macth plantas - empleados

sf_plantas['id_db__c'] = pd.to_numeric(sf_plantas['id_db__c'])

colaboradores_df= colaboradores_df.merge(sf_plantas, left_on='Planta', right_on='id_db__c', how='left')

print("Lista de columnas: ",colaboradores_df.dtypes)

colaboradores_df = colaboradores_df.drop_duplicates(colaboradores_df.columns[colaboradores_df.columns.isin(['id_db__c_x'])],
                         keep='last')

colaboradores_df = colaboradores_df.drop(['attributes', 'Name', 'id_db__c_y', 'ParentId', 'Planta', 'Banco', 'Codigo', 'id_mig'], axis=1)

colaboradores_df = colaboradores_df.rename(columns={'Id':'npsp__Primary_Affiliation__c', 'id_db__c_x': 'id_db__c'})

print(colaboradores_df.dtypes)

estudios = {'No Alcanzados                                     ': 'No está escolarizado/a aun',                                    
 'Primarios                                         ': 'Primario Completo',                                         
 'Secundarios                                       ': 'Secundario Completo',                                       
 'Terciarios                                        ': 'Terciario Completo',                                        
 'Universitarios                                    ': 'Universitario Completo',                                    
 'Incompletos Primarios                             ': 'Primario Incompleto',                             
 'Incompletos Secundarios                           ': 'Secundario Incompleto',                           
 'Incompletos Terciarios                            ': 'Terciario Incompleto',                            
 'Incompletos Universitarios                        ': 'Universitario Incompleto'}  

colaboradores_df['Maximo_nivel_educativo_alcanzado__c'].replace(estudios, inplace=True)

bancos = {'Banco Columbia' : 'BANCO COLUMBIA S.A.',
 'Banco Credicoop'	: 'BANCO CREDICOOP COOPERATIVO LIMITADO',
 'Banco de Córdoba'  : 'BANCO DE LA PROVINCIA DE CORDOBA S.A.',
 'Banco de Corrientes S.A.' : 'BANCO DE CORRIENTES S.A.',
 'Banco de la Nación Argentina' : 'BANCO DE LA NACION ARGENTINA',
 'Banco de La Pampa': 'BANCO DE LA PAMPA SOCIEDAD DE ECONOMÍA',
 'Banco del Chubut': 'BANCO DEL CHUBUT S.A.',
 'Banco Francés': 'BANCO BBVA ARGENTINA S.A.',
 'Banco Galicia': 'BANCO DE GALICIA Y BUENOS AIRES S.A.U.',
 'Banco Hipotecario S.A': 'BANCO HIPOTECARIO S.A.',
 'BANCO ITAU BUEN AYRE': 'BANCO ITAU ARGENTINA S.A.',
 'Banco Macro SA': 'BANCO MACRO S.A.',
 'Banco Patagonia': 'BANCO PATAGONIA S.A.',
 'Banco Provincia de Buenos Aires': 'BANCO DE LA PROVINCIA DE BUENOS AIRES',
 'Banco Provincia de Neuquén': 'BANCO PROVINCIA DEL NEUQUÉN SOCIEDAD ANÓNIMA',
 'Banco Provincia de Santa Cruz': 'BANCO DE SANTA CRUZ S.A.',
 'Banco Regional de Cuyo': 'BANCO SUPERVIELLE S.A',
 'Banco Santander Río SA': 'BANCO SANTANDER RIO S.A.',
 'Santander Miami CC USD': 'BANCO SANTANDER RIO S.A.',
 'Banco Supervielle': 'BANCO SUPERVIELLE S.A',
 'Citibank': 'CITIBANK N.A',
 'Compañía Financiera Argentina (Efectivo SI)': 'COMPAÑIA FINANCIERA ARGENTINA S.A.',
 'HSBC': 'HSBC BANK ARGENTINA S.A.',
 'ICBC': 'INDUSTRIAL AND COMMERCIAL BANK OF CHINA',
 'Nuevo Banco de Entre Rios': 'NUEVO BANCO DE ENTRE RÍOS S.A.',
 'Nuevo Banco de Santa Fe': 'NUEVO BANCO DE SANTA FE SOCIEDAD ANONIMA',
 'Nuevo Banco del Chaco S.A.' : 'NUEVO BANCO DEL CHACO S. A.' }

print(colaboradores_df)
colaboradores_df['Banco__c'].replace(bancos, inplace=True)


# #Reemplazo provincias

# #Primero formateo el nombre

colaboradores_df['ProvinciaDepartamento__c'] = colaboradores_df['ProvinciaDepartamento__c'].str.capitalize()

print(colaboradores_df.ProvinciaDepartamento__c.unique())

colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Buenos aires', 'ProvinciaDepartamento__c'] = 'Provincia de Buenos Aires'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Capital federal', 'ProvinciaDepartamento__c'] = 'Ciudad de Buenos Aires'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Ciudad de buenos aires', 'ProvinciaDepartamento__c'] = 'Ciudad de Buenos Aires'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Santa fe', 'ProvinciaDepartamento__c'] = 'Santa Fé'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Santa cruz', 'ProvinciaDepartamento__c'] = 'Santa Cruz'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Río Negro', 'ProvinciaDepartamento__c'] = 'Río Negro'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'San juan', 'ProvinciaDepartamento__c'] = 'San Juan'


# #Cambio tipo de dato fecha a string

colaboradores_df['Fecha_de_ingreso_a_la_compania__c'] = colaboradores_df['Fecha_de_ingreso_a_la_compania__c'].dt.strftime('%Y-%m-%d')

colaboradores_df['Birthdate'] = colaboradores_df['Birthdate'].dt.strftime('%Y-%m-%d')


# #Reemplazo el género
colaboradores_df.loc[colaboradores_df['Genero__c'] == 1, 'Genero__c'] = 'Femenino'
colaboradores_df.loc[colaboradores_df['Genero__c'] == 2, 'Genero__c'] = 'Masculino'

#print(colaboradores_df.dtypes)

print(colaboradores_df)
colaboradores_df.to_csv('colaboradores_valores_cambiados.csv', index = False)

# #Remplazo guiones del cuil
 
colaboradores_df['Cuil__c'] = colaboradores_df['Cuil__c'].str.replace(r'\D', '')
colaboradores_df['DNI__c'] = colaboradores_df['DNI__c'].str.replace(r'\D', '')

# #Hago lo mismo con los nros de teléfono
colaboradores_df['HomePhone'] = colaboradores_df['HomePhone'].str.replace(r'\D', '')
colaboradores_df['MobilePhone'] = colaboradores_df['MobilePhone'].str.replace(r'\D', '')

# #Reemplazo valores "Nan"

# colaboradores_df = colaboradores_df.where((pd.notnull(colaboradores_df)), None)

# print(colaboradores_df)

# ### Migración

colaboradores_df['RecordTypeId'] = '0124W000001ANfUQAW'

colaboradores_df.to_csv('colaboradores_a_migrar.csv', index = False)


# #colaboradores_dict = colaboradores_df.to_dict('records')
# #resultado = sf.bulk.Contact.insert(colaboradores_dict) 

