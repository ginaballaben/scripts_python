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

beneficiarios_5 = beneficiarios_5.drop_duplicates(beneficiarios_5.columns[beneficiarios_5.columns.isin(['id_db__c'])],
                          keep='last')



estudios = {'No Alcanzados                                     ': 'No está escolarizado/a aun',                                    
   'Primarios                                         ': 'Primario Completo',                                         
   'Secundarios                                       ': 'Secundario Completo',                                       
   'Terciarios                                        ': 'Terciario Completo',                                        
   'Universitarios                                    ': 'Universitario Completo',                                    
   'Incompletos Primarios                             ': 'Primario Incompleto',                             
   'Incompletos Secundarios                           ': 'Secundario Incompleto',                           
   'Incompletos Terciarios                            ': 'Terciario Incompleto',                            
   'Incompletos Universitarios                        ': 'Universitario Incompleto'}  

beneficiarios_5['Maximo_nivel_educativo_alcanzado__c'].replace(estudios, inplace=True)

beneficiarios_5 = beneficiarios_5.drop(['Birthdate','DNI__c','Cuil__c','Genero__c','HomePhone','MobilePhone','Email','Domicilio_id','Direcci_n__c','Ciudad__c',
            'CP__c','ProvinciaDepartamento__c','CBU__c','Nombre_del_titular_de_la_cuenta__c','BEN_TipoBeneficio_Id','BEN_EstadoBeneficio_Id','BEN_MotivoBeneficio_Id',
            'BEN_Area_Id','BEN_Moneda_Id','BEN_ModoPago_Id','EMP_Id','Fecha_de_ingreso_a_la_compania__c','EMP_Empresa_Id','Num_legajo__c','EMP_CargoEmpleado_Id',
            'Id_empresa','Nombre_Empresa','CargoEmpleado_Id','Ocupacin_y_tareas_que_lleva_a_cabo__c'], axis = 1 )

print(beneficiarios_5)
beneficiarios_5.to_csv('beneficiarios_test.csv', index = False)

## MERGE CON BANCOS 
bancos = "select Entidad AS Banco__c, Codigo from [Fundación Perez Companc$Entidades]"
bancos_df = datos(bancos, 'FPCBC13')

print(bancos_df)
benef_df = beneficiarios_5.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')  ## bancos se matchea en la fila del colaborador
# print(benef_df)

benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])], keep = 'last')

# ##Split de df 
# # Colab 
colaboradores_df = benef_df.loc[(benef_df['TipoBeneficiario_id']==1)]
colab_df = benef_df.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')  ## bancos se matchea en la fila del colaborador
# print(colaboradores_df)   ## 3141 
# #colaboradores_df.to_csv('colab.csv', index = False)

# ##MATCH COLABORADORES SF
colab_salesforce = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\colab_sf.csv',encoding = 'utf-8-sig', sep = ';')  ##3089

colab_salesforce = colab_salesforce[['id_db__c']]
colab_salesforce.rename(columns={'id_db__c': 'id_mig'}, inplace=True)

# # print(colab_salesforce)

colaboradores_df = colaboradores_df.merge(colab_salesforce, left_on='id_db__c', right_on='id_mig', how='left')   ##me quedo con los que falta migrar

colaboradores_df = colaboradores_df[colaboradores_df.id_mig.isnull()]

# #print(colaboradores_df) 
# # #colaboradores_df.to_csv('colaboradores_match_sf_df.csv', index = False) 

colaboradores_df = colaboradores_df.drop_duplicates(colaboradores_df.columns[colaboradores_df.columns.isin(['id_db__c'])],
                                keep='last')
print(colaboradores_df) ## 308
# # #colaboradores_df.to_csv('colaboradores_totales.csv', index = False) ###308

#colaboradores_df = colaboradores_df.drop(['attributes', 'Name', 'id_db__c_y', 'ParentId', 'Banco', 'Planta', 'Codigo','id_mig'], axis=1)

colaboradores_df.to_csv('beneficiarios_test2.csv', index = False)


