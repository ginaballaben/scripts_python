import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from api_sf import *


beneficiarios_todos = """ select distinct Beneficiario.Id AS id_db__c, Beneficiario.Nombre_Razon AS FirstName, Beneficiario.Apellido AS LastName,                
                Beneficiario.FechaNacimiento AS Birthdate, Beneficiario.NumDocumento AS DNI__c, Beneficiario.Cuil_Cuit AS Cuil__c, 				
                Beneficiario.Genero AS Genero__c, Beneficiario.Tel AS HomePhone, Beneficiario.Cel AS MobilePhone, Beneficiario.Email AS Email,
                Beneficiario.Domicilio_id as Domicilio_id, Beneficiario.TipoBeneficiario_id, 
                Domicilio.Direccion AS Direcci_n__c, Localidad.Nombre AS Ciudad__c, CodigoPostal.Nombre AS CP__c,
                Provincia.Nombre AS ProvinciaDepartamento__c, 
                EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c, 
                FormaPagoBeneficiario.Banco, FormaPagoBeneficiario.CBU AS CBU__c, FormaPagoBeneficiario.Titular as Nombre_del_titular_de_la_cuenta__c 
                
                from Beneficiario  
                                
                LEFT JOIN Domicilio ON Beneficiario.Domicilio_id = Domicilio.Id    
                LEFT JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id        
     		       LEFT JOIN Provincia ON Localidad.Provincia_id = Provincia.Id  
                LEFT JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id        
                LEFT JOIN EstudiosAlcanzados ON Beneficiario.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id
  				    LEFT JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = Beneficiario.Id 
                where Beneficiario.TipoBeneficiario_id <= 3 
                """

dest_final = """ select Beneficio_Id as DF_Beneficio_Id, Beneficiario_Id as DF_Beneficiario_Id from DestinatarioFinal """

beneficio_ = """select Id as BEN_Id, Beneficiario_id as BEN_Beneficiario_Id, TipoBeneficio_id as BEN_TipoBeneficio_Id, EstadoBeneficio_id as BEN_EstadoBeneficio_Id,
                MotivoBeneficio_id as BEN_MotivoBeneficio_Id, Area_id as BEN_Area_Id, Moneda_id as BEN_Moneda_Id, ModoPago_id as BEN_ModoPago_Id from Beneficio """

empleado_ = """select Id as EMP_Id, FechaIngreso as Fecha_de_ingreso_a_la_compania__c, Empresa_id as EMP_Empresa_Id, Establecimiento_id as Planta, 
                 Beneficiario_id as EMP_Beneficiario_id, EstudiosAlcanzados_id as EMP_EstudiosAlcanzados_Id, NumLegajo AS Num_legajo__c ,CargoEmpleado_Id as EMP_CargoEmpleado_Id
                 from Empleado 
                 """ 

empresa_ = """ select Id as Id_empresa , Nombre as Nombre_Empresa from Empresa """ 

cargo_emp = """select Id as CargoEmpleado_Id, Nombre AS Ocupacin_y_tareas_que_lleva_a_cabo__c from CargoEmpleado """

beneficiarios_todos = datos(beneficiarios_todos, 'Beneficios')
dest_final = datos(dest_final, 'Beneficios')
beneficio_ = datos(beneficio_, 'Beneficios')
empleado_ = datos(empleado_, 'Beneficios')
empresa_ = datos(empresa_, 'Beneficios')
cargo_emp = datos(cargo_emp, 'Beneficios')

beneficiarios = beneficiarios_todos.merge(dest_final, left_on = 'id_db__c', right_on = 'DF_Beneficiario_Id', how = 'left')
beneficiarios_2 = beneficiarios.merge(beneficio_, left_on = 'DF_Beneficio_Id', right_on = 'BEN_Id', how = 'left' )   ## conecto beneficiarios con colaborador
beneficiarios_3 = beneficiarios_2.merge(empleado_, left_on = 'BEN_Beneficiario_Id', right_on = 'EMP_Beneficiario_id', how = 'left' )  ##conecto colab con empresa en la misma fila del beneficiario
beneficiarios_4 = beneficiarios_3.merge(empresa_, left_on = 'EMP_Empresa_Id', right_on = 'Id_empresa', how = 'left' )
beneficiarios_5 = beneficiarios_4.merge(cargo_emp, left_on = 'EMP_CargoEmpleado_Id', right_on = 'CargoEmpleado_Id', how = 'left' )


#print(beneficiarios_todos)
#print(beneficiarios)
#print(beneficiarios_2)
#print(beneficiarios_3)
#print(beneficiarios_4)
print(beneficiarios_5)

#beneficiarios_5.to_csv('test_salud.csv', index = False)

beneficiarios_5 = beneficiarios_5.drop_duplicates(beneficiarios_5.columns[beneficiarios_5.columns.isin(['id_db__c'])],
                          keep='last')

print(beneficiarios_5)
#beneficiarios_5.to_csv('beneficiarios_salud.csv', index = False)

## MERGE CON BANCOS 
bancos = "select Entidad AS Banco__c, Codigo from [Fundación Perez Companc$Entidades]"
bancos_df = datos(bancos, 'FPCBC13')

print(bancos_df)
benef_df = beneficiarios_5.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')  ## bancos se matchea en la fila del colaborador
# print(benef_df)

benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])], keep = 'last')

#print(benef_df)
#benef_df.to_csv('benefic_salud.csv', index = False)

##Split de df 
# Colab 
colaboradores_df = benef_df.loc[(benef_df['TipoBeneficiario_id']==1)]
#colab_df = benef_df.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')  ## bancos se matchea en la fila del colaborador
print(colaboradores_df)   ## 3141 
#colaboradores_df.to_csv('colab.csv', index = False)

##MATCH COLABORADORES SF
colab_salesforce = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\colab_sf.csv',encoding = 'utf-8-sig', sep = ';')  ##3089

colab_salesforce = colab_salesforce[['id_db__c']]
colab_salesforce.rename(columns={'id_db__c': 'id_mig'}, inplace=True)

# print(colab_salesforce)

colaboradores_df = colaboradores_df.merge(colab_salesforce, left_on='id_db__c', right_on='id_mig', how='left')   ##me quedo con los que falta migrar

colaboradores_df = colaboradores_df[colaboradores_df.id_mig.isnull()]

#print(colaboradores_df) 
# #colaboradores_df.to_csv('colaboradores_match_sf_df.csv', index = False) 

colaboradores_df = colaboradores_df.drop_duplicates(colaboradores_df.columns[colaboradores_df.columns.isin(['id_db__c'])],
                               keep='last')
print(colaboradores_df) ## 308
# #colaboradores_df.to_csv('colaboradores_totales.csv', index = False) ###308

# ##Me traigo sus plantas y cias
query = 'SELECT Id, Name, id_db__c, ParentId FROM Account'
sf_companias = sf.bulk.Account.query(query)
sf_companias = pd.DataFrame(sf_companias)

# # # #Filtro valores nulos
sf_plantas = sf_companias[sf_companias.ParentId.notnull()]

print(sf_plantas)
#print(sf_plantas.dtypes)

# # sf_companias.to_csv('companias.csv', index = False)
# # sf_plantas.to_csv('plantas.csv', index = False)

# # # # ##### Macth plantas - empleados

sf_plantas['id_db__c'] = pd.to_numeric(sf_plantas['id_db__c'])
#print(sf_plantas.dtypes)

colaboradores_df= colaboradores_df.merge(sf_plantas, left_on='Planta', right_on='id_db__c', how='left')

#print("Lista de columnas: ",colaboradores_df.dtypes)

colaboradores_df = colaboradores_df.drop_duplicates(colaboradores_df.columns[colaboradores_df.columns.isin(['id_db__c_x'])],
                           keep='last')

colaboradores_df = colaboradores_df.drop(['attributes', 'Name', 'id_db__c_y', 'ParentId', 'Banco', 'Planta', 'Codigo','id_mig'], axis=1)

colaboradores_df = colaboradores_df.rename(columns={'Id':'npsp__Primary_Affiliation__c', 'id_db__c_x': 'id_db__c'})

print(colaboradores_df)

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

###chequear bancos 
colaboradores_df['Banco__c'].replace(bancos, inplace=True)

# # # #Reemplazo provincias

# # #Primero formateo el nombre

colaboradores_df['ProvinciaDepartamento__c'] = colaboradores_df['ProvinciaDepartamento__c'].str.capitalize()

#print(colaboradores_df.ProvinciaDepartamento__c.unique())

colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Buenos aires', 'ProvinciaDepartamento__c'] = 'Provincia de Buenos Aires'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Capital federal', 'ProvinciaDepartamento__c'] = 'Ciudad de Buenos Aires'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Ciudad de buenos aires', 'ProvinciaDepartamento__c'] = 'Ciudad de Buenos Aires'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Santa fe', 'ProvinciaDepartamento__c'] = 'Santa Fé'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Santa cruz', 'ProvinciaDepartamento__c'] = 'Santa Cruz'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'Río Negro', 'ProvinciaDepartamento__c'] = 'Río Negro'
colaboradores_df.loc[colaboradores_df['ProvinciaDepartamento__c'] == 'San juan', 'ProvinciaDepartamento__c'] = 'San Juan'


# # # #Cambio tipo de dato fecha a string

colaboradores_df['Fecha_de_ingreso_a_la_compania__c'] = colaboradores_df['Fecha_de_ingreso_a_la_compania__c'].dt.strftime('%Y-%m-%d')

colaboradores_df['Birthdate'] = colaboradores_df['Birthdate'].dt.strftime('%Y-%m-%d')


# # # #Reemplazo el género
colaboradores_df.loc[colaboradores_df['Genero__c'] == 1, 'Genero__c'] = 'Femenino'
colaboradores_df.loc[colaboradores_df['Genero__c'] == 2, 'Genero__c'] = 'Masculino'

# print(colaboradores_df.dtypes)

# print(colaboradores_df)
#  #colaboradores_df.to_csv('colaboradores_valores_cambiados.csv', index = False)

# # # #Remplazo guiones del cuil
 
colaboradores_df['Cuil__c'] = colaboradores_df['Cuil__c'].str.replace(r'\D', '')
colaboradores_df['Cuil__c'] = colaboradores_df['Cuil__c'].str.replace('-', '')
colaboradores_df['DNI__c'] = colaboradores_df['DNI__c'].str.replace(r'\D', '')

# # # #Hago lo mismo con los nros de teléfono
colaboradores_df['HomePhone'] = colaboradores_df['HomePhone'].str.replace(r'\D', '')
colaboradores_df['MobilePhone'] = colaboradores_df['MobilePhone'].str.replace(r'\D', '')

# # # ### Migración

colaboradores_df['RecordTypeId'] = '0124W000001ANfUQAW'

print(colaboradores_df) # 308 colaboradores

##Elimino columnas innecesarias
colaboradores_df = colaboradores_df.drop(['Domicilio_id', 'TipoBeneficiario_id', 'DF_Beneficio_Id',	'DF_Beneficiario_Id',
                 'BEN_Id', 'BEN_Beneficiario_Id', 'BEN_TipoBeneficio_Id', 'BEN_EstadoBeneficio_Id', 'BEN_MotivoBeneficio_Id', 'BEN_Area_Id', 'BEN_Moneda_Id', 
                 'BEN_ModoPago_Id', 'EMP_Beneficiario_id',	'EMP_EstudiosAlcanzados_Id', 'EMP_Id', 'Id_empresa', 'Nombre_Empresa', 'EMP_Empresa_Id', 'EMP_CargoEmpleado_Id', 'CargoEmpleado_Id'], axis = 1 )

# ##Chequeo dnis no vacíos y con 7 digitos o más   
colaboradores_df['dni_length'] = colaboradores_df['DNI__c'].str.len()

colaboradores_a_migrar = colaboradores_df[colaboradores_df['dni_length'] >= 7]

##Completo País de residencia
_prov_arg = ['Provincia de Buenos Aires', 'Ciudad de Buenos Aires', 'Catamarca', 'Chaco', 'Chubut', 'Córdoba', 'Corrientes', 'Entre Ríos', 'Formosa', 'Jujuy',
               'La pampa', 'La rioja', 'Mendoza', 'Misiones', 'Neuquén', 'Río Negro', 'Salta', 'San Juan', 'San Luis', 'Santa Cruz', 'Santa Fé', 'Santiago del Estero', 
               'Tierra del Fuego', 'Tucumán']

colaboradores_a_migrar['Pais_de_residencia__c'] = np.where(colaboradores_a_migrar['ProvinciaDepartamento__c'].isin(_prov_arg), 'Argentina', '')
#colaboradores_a_migrar.to_csv('col.csv', index = False)

print(colaboradores_a_migrar)

# ## Elimino casos manuales con edad < 18 que son hijos de colaboradores y con dni no validos 
colaboradores_a_migrar = colaboradores_a_migrar[(colaboradores_a_migrar['id_db__c']!= 7447) & (colaboradores_a_migrar['id_db__c']!= 7438) & (colaboradores_a_migrar['id_db__c']!= 53193) 
                     & (colaboradores_a_migrar['id_db__c']!= 13280) & (colaboradores_a_migrar['id_db__c']!= 13715) & (colaboradores_a_migrar['id_db__c']!= 13077) &
                     (colaboradores_a_migrar['id_db__c']!= 7461) & (colaboradores_a_migrar['id_db__c']!= 12745) & (colaboradores_a_migrar['id_db__c']!= 7450) &
                     (colaboradores_a_migrar['id_db__c']!= 12496) &
                     (colaboradores_a_migrar['id_db__c']!= 12281) & (colaboradores_a_migrar['id_db__c']!= 13017) & (colaboradores_a_migrar['id_db__c']!= 7456)] 

## Casos de colaboradores sin migrar a chequear 
# colaboradores_sin_migrar = colaboradores_df[(colaboradores_df['id_db__c']== 7447) | (colaboradores_df['id_db__c']== 7438) | (colaboradores_df['id_db__c']== 53193) |
#                      (colaboradores_df['id_db__c']== 13280) | (colaboradores_df['id_db__c']== 13715) | (colaboradores_df['id_db__c']== 13077) | 
#                      (colaboradores_df['id_db__c']== 7461) | (colaboradores_df['id_db__c']== 12745) | (colaboradores_df['id_db__c']== 7450) | 
#                      (colaboradores_df['id_db__c']== 12496) | 
#                      (colaboradores_df['id_db__c']== 12281) | (colaboradores_df['id_db__c']== 13017) | (colaboradores_df['id_db__c']== 7456) | (colaboradores_df['dni_length'] < 7) ]

# print(colaboradores_sin_migrar)
# colaboradores_sin_migrar.to_csv('colaboradores_sin_migrar.csv', index = False)

# #Reemplazo valores "NAN"
colaboradores_a_migrar = colaboradores_a_migrar.where((pd.notnull(colaboradores_a_migrar)), None)
print(colaboradores_a_migrar)

# ## Migración 
colaboradores_a_migrar = colaboradores_a_migrar.drop(['dni_length'], axis = 1 )

print(colaboradores_a_migrar)
#colaboradores_a_migrar = colaboradores_a_migrar.to_csv('colaboradores_migr.csv', index = False)
# print(colaboradores_a_migrar.dtypes)

## Test - Seleccionar columnas a migrar 
#colaboradores_a_migrar_test = colaboradores_a_migrar.head(2)
# colaboradores_a_migrar_test_cont = colaboradores_a_migrar[['id_db__c', 'FirstName', 'LastName', 'Birthdate', 'DNI__c', 'Cuil__c', 'Genero__c',
#                           'HomePhone', 'MobilePhone','Email', 'ProvinciaDepartamento__c', 'Ciudad__c', 'CP__c', 'Direcci_n__c', 'Pais_de_residencia__c', 
#                           'Maximo_nivel_educativo_alcanzado__c', 'Fecha_de_ingreso_a_la_compania__c', 'Num_legajo__c', 'Ocupacin_y_tareas_que_lleva_a_cabo__c',
#                           'npsp__Primary_Affiliation__c','RecordTypeId' ]]
# print(colaboradores_a_migrar_test_cont) 
# colaboradores_a_migrar_test_cont.to_csv('colab_test_cont.csv', index = False)


# # ######## ENVIO A SALESFORCE #############
# ## A OBJETO CONTACTO
# contactos = instancia('Contact')
# colaboradores_a_migrar_test_cont = colaboradores_a_migrar_test_cont.to_dict('records')
# resultado = sf.bulk.Contact.insert(colaboradores_a_migrar_test_cont)
# print(resultado) 

##ME TRAIGO DATOS DE SF 
contactos = instancia('Contact')
query = '''SELECT Id_18__c, id_db__c, RecordTypeId
                FROM Contact'''

sf_contactos = sf.bulk.Contact.query(query)
sf_contactos = pd.DataFrame(sf_contactos)

sf_contactos = sf_contactos.loc[sf_contactos['RecordTypeId']=='0124W000001ANfUQAW']
sf_contactos['id_db__c'] = pd.to_numeric(sf_contactos['id_db__c'])
#print(sf_contactos.dtypes)

# # ##MATCH 
colab_bancos = colaboradores_a_migrar.merge(sf_contactos, left_on = 'id_db__c', right_on = 'id_db__c', how = 'inner')
print(colab_bancos)
#colab_bancos.to_csv('colab_bancos.csv')

# ## A OBJETO DATOS BANCARIOS
# colaboradores_a_migrar_test_banco = colab_bancos.rename(columns = {'Cuil__c' : 'CUIL_del_titular__c'}) 
# colaboradores_a_migrar_test_banco['Contacto_duenio_de_la_Cuenta_Bancaria__c'] = colaboradores_a_migrar_test_banco['Id_18__c']
# colaboradores_a_migrar_test_banco = colaboradores_a_migrar_test_banco[['id_db__c', 'CBU__c','Nombre_del_titular_de_la_cuenta__c', 'CUIL_del_titular__c', 'Banco__c', 'Contacto_duenio_de_la_Cuenta_Bancaria__c']]
# colaboradores_a_migrar_test_banco.to_csv('datos_bancarios_colab_18_06.csv', index = False)

# # ######## ENVIO A SALESFORCE #############
# # # ## A OBJETO DATOS BANCARIOS
# colaboradores_a_migrar_test_banco = colaboradores_a_migrar_test_banco.to_dict('records')
# resultado = sf.bulk.Datos_Bancarios__c.insert(colaboradores_a_migrar_test_banco)
# print(resultado)
