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
                Provincia.Nombre AS ProvinciaDepartamento__c, EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c,
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
                 from Empleado """ 

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
# print(beneficiarios_5)

beneficiarios_5 = beneficiarios_5.drop_duplicates(beneficiarios_5.columns[beneficiarios_5.columns.isin(['id_db__c'])],
                          keep='last')

#print(beneficiarios_5)
#beneficiarios_4.to_csv('beneficiarios_salud.csv', index = False)

## MERGE CON BANCOS 
bancos = "select Entidad AS Banco__c, Codigo from [Fundación Perez Companc$Entidades]"
bancos_df = datos(bancos, 'FPCBC13')
#print(bancos_df)

benef_df = beneficiarios_5.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')  ## bancos se matchea en la fila del colaborador

benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])], keep = 'last')

#print(benef_df)
#benef_df.to_csv('benefic_salud.csv', index = False)

##Split de df 
# Colab 
# colab_df = benef_df.loc[(benef_df['TipoBeneficiario_id']==1)]
# #colab_df = benef_df.merge(bancos_df, left_on='Banco', right_on='Codigo', how='inner')  ## bancos se matchea en la fila del colaborador
# #colab_df = colab_df[['id_db__c', 'Banco__c', 'Codigo']]
# print(colab_df)   ## 3141 
# colab_df.to_csv('colab.csv', index = False)

##Beneficiaries (hijes)
beneficiaries = benef_df.loc[(benef_df['TipoBeneficiario_id']==2) | (benef_df['TipoBeneficiario_id']==3)]
print(beneficiaries)  # 6506
#beneficiaries.to_csv('beneficiaries.csv', index = False)

## Match con beneficiarios cargados

benef_sf = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\beneficiarios_sf.csv', encoding = 'utf-8-sig', sep = ';') ## 6987 

benef_sf = benef_sf[['id_db']]

benef_sf.rename(columns={'id_db': 'id_mig'}, inplace=True)

benef_df = beneficiaries.merge(benef_sf, left_on='id_db__c', right_on='id_mig', how='left') #me quedo con los que falta migrar

benef_df = benef_df[benef_df.id_mig.isnull()]

# ##Quito duplicados
benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])],
                          keep='last')
print(benef_df)  ## 1396 
#benef_df.to_csv('beneficiaries.csv', index = False)

# ## Salesforce 
query = 'SELECT Id, AccountId, id_db__c FROM Contact'
sf_contactos = sf.bulk.Contact.query(query)
sf_contactos = pd.DataFrame(sf_contactos)

# sf_contactos.to_csv('contactos_sf.csv', index = False)

# # ##Hago el match
sf_contactos['id_db__c'] = pd.to_numeric(sf_contactos['id_db__c'])
sf_contactos = sf_contactos.rename(columns={'id_db__c':'id_colab'})

benef_df=benef_df.merge(sf_contactos, left_on='BEN_Beneficiario_Id', right_on='id_colab', how='left')

# # ##Quito duplicados
benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])],
                           keep='last')
print(benef_df)  ## 1396
# #benef_df.to_csv('beneficiaries2.csv', index = False)    

benef_df = benef_df.drop(['attributes', 'Id', 'id_colab', 'id_mig'], axis=1)
#print(benef_df)

# ##Limpieza de la base

estudios = {'No Alcanzados                                     ': 'No está escolarizado/a aun',                                    
  'Primarios                                         ': 'Primario Completo',                                         
  'Secundarios                                       ': 'Secundario Completo',                                       
  'Terciarios                                        ': 'Terciario Completo',                                        
  'Universitarios                                    ': 'Universitario Completo',                                    
  'Incompletos Primarios                             ': 'Primario Incompleto',                             
  'Incompletos Secundarios                           ': 'Secundario Incompleto',                           
  'Incompletos Terciarios                            ': 'Terciario Incompleto',                            
  'Incompletos Universitarios                        ': 'Universitario Incompleto'} 

benef_df['Maximo_nivel_educativo_alcanzado__c'].replace(estudios, inplace=True)
benef_df['ProvinciaDepartamento__c'] = benef_df['ProvinciaDepartamento__c'].str.capitalize()

provincias = {'Buenos aires' : 'Provincia de Buenos Aires',
  'Capital federal' : 'Ciudad de Buenos Aires',
  'Ciudad de buenos aires' : 'Ciudad de Buenos Aires',
  'Santa fe' : 'Santa Fé',
  'Santa cruz' : 'Santa Cruz',
  'Río Negro' : 'Río Negro',
  'San juan' : 'San Juan',
  'Entre rios' : 'Entre Ríos',
  'San luis' : 'San Luis'}

benef_df['ProvinciaDepartamento__c'].replace(provincias, inplace=True)

benef_df['Birthdate'] = benef_df['Birthdate'].dt.strftime('%Y-%m-%d')

benef_df.loc[benef_df['Genero__c'] == 1, 'Genero__c'] = 'Femenino'
benef_df.loc[benef_df['Genero__c'] == 2, 'Genero__c'] = 'Masculino'
benef_df.loc[benef_df['Genero__c'] == 0, 'Genero__c'] = ''
benef_df.loc[benef_df['Genero__c'] == None, 'Genero__c'] = ''

benef_df['Cuil__c'] = benef_df['Cuil__c'].str.replace(r'\D', '')
benef_df['Cuil__c'] = benef_df['Cuil__c'].str.replace('-', '')

benef_df['DNI__c'] = benef_df['DNI__c'].str.replace(r'\D', '')
benef_df['DNI__c'] = benef_df['DNI__c'].str.replace('-', '')

benef_df['HomePhone'] = benef_df['HomePhone'].str.replace(r'\D', '')
benef_df['MobilePhone'] = benef_df['MobilePhone'].str.replace(r'\D', '')

#benef_df['RecordTypeId'] = '0124W000001ANfeQAG'

benef_df = benef_df.drop(['Banco', 'Nombre_del_titular_de_la_cuenta__c', 'CBU__c',  'Domicilio_id', 'DF_Beneficio_Id', 'DF_Beneficiario_Id', 
          'BEN_Id', 'BEN_Beneficiario_Id',	'BEN_TipoBeneficio_Id',	'BEN_EstadoBeneficio_Id', 'BEN_MotivoBeneficio_Id',	'BEN_Area_Id',	'BEN_Moneda_Id',
          'BEN_ModoPago_Id',	'EMP_Id',	'Fecha_de_ingreso_a_la_compania__c',	'Planta', 'EMP_Beneficiario_id', 'EMP_Empresa_Id',
          'Id_empresa', 'Nombre_Empresa', 'EMP_EstudiosAlcanzados_Id', 'Num_legajo__c', 'EMP_CargoEmpleado_Id','CargoEmpleado_Id',
            'Ocupacin_y_tareas_que_lleva_a_cabo__c'], axis=1)

#print(benef_df) # 1396
#benef_df.to_csv('beneficiarios_actualizar.csv', index = False)

###Chequeo dnis no vacíos y con 6 digitos o más   
benef_df['dni_length'] = benef_df['DNI__c'].str.len()

benef_a_migrar = benef_df.loc[benef_df['dni_length'] >= 6]

##Completo País de residencia
_prov_arg = ['Provincia de Buenos Aires', 'Ciudad de Buenos Aires', 'Catamarca', 'Chaco', 'Chubut', 'Córdoba', 'Corrientes', 'Entre Ríos', 'Formosa', 'Jujuy',
               'La pampa', 'La rioja', 'Mendoza', 'Misiones', 'Neuquén', 'Río Negro', 'Salta', 'San Juan', 'San Luis', 'Santa Cruz', 'Santa Fé', 'Santiago del Estero', 
               'Tierra del Fuego', 'Tucumán']

benef_a_migrar['Pais_de_residencia__c'] = np.where(benef_a_migrar['ProvinciaDepartamento__c'].isin(_prov_arg), 'Argentina', '')

## Elimino casos manuales con con dni no validos 
benef_a_migrar = benef_a_migrar[(benef_a_migrar['id_db__c']!= 13357) & (benef_a_migrar['id_db__c']!= 7427) & (benef_a_migrar['id_db__c']!= 7429) 
                      & (benef_a_migrar['id_db__c']!= 7430) & (benef_a_migrar['id_db__c']!= 7431) & (benef_a_migrar['id_db__c']!= 7432) &
                      (benef_a_migrar['id_db__c']!= 7433) & (benef_a_migrar['id_db__c']!= 7434) & (benef_a_migrar['id_db__c']!= 7435) &
                      (benef_a_migrar['id_db__c']!= 7440) &
                      (benef_a_migrar['id_db__c']!= 7454) & (benef_a_migrar['id_db__c']!= 7462)] 

# print(benef_a_migrar)
# benef_a_migrar.to_csv('beneficiarios_actualizar.csv', index = False)

# ## Casos de beneficiarios sin migrar a chequear 
# benef_sin_migrar = benef_df[(benef_df['id_db__c']== 13357) | (benef_df['id_db__c']== 7427) | (benef_df['id_db__c']== 7429) |
#                         (benef_df['id_db__c']== 7430) | (benef_df['id_db__c']== 7431) | (benef_df['id_db__c']== 7432) |
#                        (benef_df['id_db__c']== 7433) | (benef_df['id_db__c']== 7434) | (benef_df['id_db__c']== 7435) |
#                        (benef_df['id_db__c']== 7440) | (benef_df['id_db__c']== 7454) | (benef_df['id_db__c']== 7462) | (benef_df['dni_length'] < 6) ]

# print(benef_sin_migrar)
# benef_sin_migrar.to_csv('beneficiarios_sin_migrar_18_06.csv', index = False)

##Reemplazo valores "NAN"
benef_a_migrar = benef_a_migrar.where((pd.notnull(benef_a_migrar)), None)

print(benef_a_migrar)
#benef_a_migrar = benef_a_migrar.to_csv('beneficiarios_migr.csv', index = False)

################################
##modificación  05/07 
benef_a_migrar['RecordTypeId'] = np.where(benef_a_migrar['TipoBeneficiario_id']==2, '0124W000001ANfoQAG', '0124W000001ANfeQAG')

beneficiarios_a_migrar_test = benef_a_migrar[['id_db__c', 'RecordTypeId']]

print(beneficiarios_a_migrar_test) 
beneficiarios_a_migrar_test.to_csv('beneficiarios_migr.csv', index = False)

######## ENVIO A SALESFORCE #############
# # ## UPDATE A OBJETO CONTACTO
# contactos = instancia('Contact')
# beneficiarios_a_migrar_test = beneficiarios_a_migrar_test.to_dict('records')
# resultado = sf.bulk.Contact.update(beneficiarios_a_migrar_test)
# print(resultado) 

############################

# ## Test - Seleccionar columnas a migrar 
# #beneficiarios_a_migrar_test = benef_a_migrar.head(2)
# beneficiarios_a_migrar_test = benef_a_migrar[['id_db__c', 'FirstName', 'LastName', 'Birthdate', 'DNI__c', 'Cuil__c', 'Genero__c',
#                            'HomePhone', 'MobilePhone','Email', 'ProvinciaDepartamento__c', 'Ciudad__c', 'CP__c', 'Direcci_n__c', 'Pais_de_residencia__c', 
#                            'Maximo_nivel_educativo_alcanzado__c', 'AccountId','RecordTypeId' ]]
# print(beneficiarios_a_migrar_test) 
# # beneficiarios_a_migrar_test.to_csv('benef_a_migrar_test.csv', index = False)


# # # ######## ENVIO A SALESFORCE #############
# # ## A OBJETO CONTACTO
# # contactos = instancia('Contact')
# # beneficiarios_a_migrar_test = beneficiarios_a_migrar_test.to_dict('records')
# # resultado = sf.bulk.Contact.insert(beneficiarios_a_migrar_test)
# # print(resultado) 

