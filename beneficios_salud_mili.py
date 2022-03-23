import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from api_sf import *
from datetime import datetime

beneficios = """ select Beneficio.Id, Beneficio.Nombre, Beneficio.Descripcion, 
        Beneficio.Monto, Beneficio.FechaPedido, Beneficio.TipoBeneficio_id as Tipo__c,
        Beneficio.EstadoBeneficio_id, Area_id, year (Beneficio.FechaPedido) as Ano_beneficio, Beneficio.Beneficiario_id as Colaboradores_as_pasible_de_Beca_o_Benef__c
        
		from Beneficio

        inner join Beneficiario on Beneficio.Beneficiario_id = Beneficiario.Id
        WHERE Area_id = 3
        order by FechaPedido """

dest_final = """ select Beneficio_Id as DF_Beneficio_Id, Beneficiario_Id as Beneficiario__c
                from DestinatarioFinal 
                """

beneficios_df = datos(beneficios, 'Beneficios')
dest_final_df = datos(dest_final, 'Beneficios')

## merge
beneficios_df = beneficios_df.merge(dest_final_df, left_on = 'Id', right_on = 'DF_Beneficio_Id', how = 'inner')

beneficiarios = """ select Id as Id_benef, Nombre_Razon, Apellido, Observaciones, TipoBeneficiario_id, TipoPersona_id
                from Beneficiario  """

beneficiarios_df = datos(beneficiarios, 'Beneficios')

##datos beneficiarios
beneficios_df = beneficios_df.merge(beneficiarios_df, left_on = 'Colaboradores_as_pasible_de_Beca_o_Benef__c', right_on = 'Id_benef', how = 'left')

##datos destinatario
beneficios_df = beneficios_df.merge(beneficiarios_df, left_on = 'Beneficiario__c', right_on = 'Id_benef', how = 'left')


print(beneficios_df)
beneficios_df.to_csv('beneficios_salud_todos.csv', index = False)

beneficios_df['Nombre'] = beneficios_df['Nombre'].str.strip()
beneficios_df['Nombre'] = beneficios_df['Nombre'].str.replace("\n", "")
beneficios_df['Nombre'] = beneficios_df['Nombre'].str.replace("\r", "\t")

beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.strip()
beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.replace("\n", "")
beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.replace("\r", "\t")

##tipo de persona - tipo de beneficiario 
beneficios_df['TipoBeneficiario_id_x'] = np.where(beneficios_df['TipoBeneficiario_id_x']==1, 'Empleado', 
                                           np.where(beneficios_df['TipoBeneficiario_id_x']==2, 'Tercero', 
                                           np.where(beneficios_df['TipoBeneficiario_id_x']==3, 'Familiar Empleado',
                                           np.where(beneficios_df['TipoBeneficiario_id_x']==4, 'Institución',
                                           np.where(beneficios_df['TipoBeneficiario_id_x']==5, 'Comunidad', 'Programa')))))

beneficios_df['TipoBeneficiario_id_y'] = np.where(beneficios_df['TipoBeneficiario_id_y']==1, 'Empleado', 
                                           np.where(beneficios_df['TipoBeneficiario_id_y']==2, 'Tercero', 
                                           np.where(beneficios_df['TipoBeneficiario_id_y']==3, 'Familiar Empleado',
                                           np.where(beneficios_df['TipoBeneficiario_id_y']==4, 'Institución',
                                           np.where(beneficios_df['TipoBeneficiario_id_y']==5, 'Comunidad', 'Programa')))))

beneficios_df['TipoPersona_id_x'] = np.where(beneficios_df['TipoPersona_id_x']==1, 'Física', 
                                           np.where(beneficios_df['TipoPersona_id_x']==2, 'Jurídica', 'Comunidad/Programa'))
                                           
beneficios_df['TipoPersona_id_y'] = np.where(beneficios_df['TipoPersona_id_y']==1, 'Física', 
                                           np.where(beneficios_df['TipoPersona_id_y']==2, 'Jurídica', 'Comunidad/Programa'))

beneficios_df['Nombre_Razon_x'] = beneficios_df['Nombre_Razon_x'].str.strip()
beneficios_df['Nombre_Razon_x'] = beneficios_df['Nombre_Razon_x'].str.replace("\n", "")
beneficios_df['Nombre_Razon_x'] = beneficios_df['Nombre_Razon_x'].str.replace("\r", "\t")

beneficios_df['Nombre_Razon_y'] = beneficios_df['Nombre_Razon_y'].str.strip()
beneficios_df['Nombre_Razon_y'] = beneficios_df['Nombre_Razon_y'].str.replace("\n", "")
beneficios_df['Nombre_Razon_y'] = beneficios_df['Nombre_Razon_y'].str.replace("\r", "\t")

beneficios_df['Observaciones_x'] = beneficios_df['Observaciones_x'].str.strip()
beneficios_df['Observaciones_x'] = beneficios_df['Observaciones_x'].str.replace("\n", "")
beneficios_df['Observaciones_x'] = beneficios_df['Observaciones_x'].str.replace("\r", "\t")

beneficios_df['Observaciones_y'] = beneficios_df['Observaciones_y'].str.strip()
beneficios_df['Observaciones_y'] = beneficios_df['Observaciones_y'].str.replace("\n", "")
beneficios_df['Observaciones_y'] = beneficios_df['Observaciones_y'].str.replace("\r", "\t")


# print(beneficios_df)
# beneficios_df.to_csv('beneficios_salud_todos.csv', index = False)


