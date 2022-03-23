import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime

##### ME TRAIGO LOS DATOS DEL SQL ######

beneficios = """ select *
        
		from Beneficio

        inner join EstadoBeneficio on Beneficio.EstadoBeneficio_id = EstadoBeneficio.Id
        inner join TipoBeneficio on Beneficio.TipoBeneficio_id = TipoBeneficio.Id
        inner join Beneficiario on Beneficio.Beneficiario_id = Beneficiario.Id 
        WHERE TipoPersona_id <> 1
        order by FechaPedido """

beneficios_df = datos(beneficios, 'Beneficios')

print(beneficios_df)
beneficios_df.to_csv('beneficios_programas.csv')

# # # ##Tipo y tipo beneficio 
# beneficios_df['TipoBeneficio_id'] = np.where(beneficios_df['TipoBeneficio_id']==1, 'Donación', 
#                                        np.where(beneficios_df['TipoBeneficio_id']==2, 'Subsidio',
#                                            np.where(beneficios_df['TipoBeneficio_id']==3, 'Beca', 'Préstamo')))


beneficios_df = beneficios_df.loc[(beneficios_df['EstadoBeneficio_id'] != 2)]

# # # ##Estado beneficio 
beneficios_df['EstadoBeneficio_id'] = np.where(beneficios_df['EstadoBeneficio_id']==1, 'En Evaluación', 
                                       np.where(beneficios_df['EstadoBeneficio_id']==2, 'Desestimado',
                                       np.where(beneficios_df['EstadoBeneficio_id']==3, 'Otorgado s/costo', 
                                       np.where(beneficios_df['EstadoBeneficio_id']==4, 'En Aprobación', 
                                       np.where(beneficios_df['EstadoBeneficio_id']==5, 'Rechazado',
                                       np.where(beneficios_df['EstadoBeneficio_id']==6, 'Aprobado Pte. Cuotas',
                                       np.where(beneficios_df['EstadoBeneficio_id']==7, 'En Curso', 
                                       np.where(beneficios_df['EstadoBeneficio_id']==8, 'Con Versión Pendiente',
                                       np.where(beneficios_df['EstadoBeneficio_id']==9, 'Finalizado',
                                            'Dado de Baja')))))))))

estado_beneficio = {'En Evaluación':'En análisis',
          'Desestimado':'Desestimado',
          'Otorgado s/costo': 'Finalizado',
          'En Aprobación': 'En análisis',
          'Rechazado': 'Desestimado',
          'Aprobado Pte. Cuotas': 'Finalizado' ,
          'En Curso' :  'Aprobado', 
          'Con Versión Pendiente' : 'Finalizado',
          'Finalizado' : 'Finalizado',
          'Dado de Baja': 'Finalizado'}

beneficios_df['EstadoBeneficio_id'].replace(estado_beneficio, inplace = True)

# print(beneficios_df)
# beneficios_df.to_csv('beneficios_programas.csv')


# # # # # ## Moneda
# beneficios_df['Moneda_id'] = np.where(beneficios_df['Moneda_id']==1, 
#                                                     'ARS', 'USD')

# # # # ##Modo de pago 
# beneficios_df['ModoPago_id'] = np.where(beneficios_df['ModoPago_id']==1, 
#                                                    'Fijo', np.where(beneficios_df['ModoPago_id']==2, 'Tope por cuota', 'Tope por total'))

# ## referente
# beneficios_df['Referente'] = np.where(beneficios_df['Referente']==1, 'RRHH', 
#                             np.where(beneficios_df['Referente']==2, 'Beneficiario', 
#                             np.where(beneficios_df['Referente']==3, 'Consejero', 'Fundación')))

# beneficios_df['MotivoBeneficio_id'] = np.where(beneficios_df['MotivoBeneficio_id']==1, 'Programas de Capacitación', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==2, 'Construcción | Mejora | Compra de inmueble',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==3, 'Equipamiento | Bienes muebles', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==4, 'Materiles | Publicaciones | Insumos | Difusión', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==5, 'Programa de Becas FPC',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==6, 'Congresos, seminarios y cursos',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==7, 'Becas de Formación', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==8, 'Gastos de Funcionamiento',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==9, 'Vehículos',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==10, 'Proyectos Especiales', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==11, 'Promoción humana y comunitaria',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==12, 'Alimentos', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==13, 'Asistencia Médica de Salud', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==14, 'Asistencia social | Sostenimiento',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==15, 'Becas Especiales',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==16, 'Proyectos de Investigación', 
#                                       np.where(beneficios_df['MotivoBeneficio_id']==17, 'Infraestructura y Obras Públicas',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==18, 'Distinciones',
#                                       np.where(beneficios_df['MotivoBeneficio_id']==19, 'Obras con Seguimiento',
#                                       'Proyectos Institucionales')))))))))))))))))))

print(beneficios_df['Area_id'])

##AREA 
beneficios_df['Area_id'] = np.where(beneficios_df['Area_id']==1, 'Educación', 
                            np.where(beneficios_df['Area_id']==2, 'Evangelización', 
                            np.where(beneficios_df['Area_id']==3, 'Salud',
                            np.where(beneficios_df['Area_id']==4, 'Hábitat y Vivienda', 
                            np.where(beneficios_df['Area_id']==5, 'Desarrollo Científico', 'Beneficios Especiales')))))

beneficios_df = beneficios_df.loc[(beneficios_df['Area_id']== 'Evangelización') | (beneficios_df['Area_id']== 'Hábitat y Vivienda')
                                | (beneficios_df['Area_id']== 'Desarrollo Científico') | (beneficios_df['Area_id']== 'Beneficios Especiales')]

beneficios_df['Area_id'] = "Otras Ayudas"

print(beneficios_df['Area_id'])
beneficios_df.to_csv('beneficios_programas.csv')

# # # # ##Texto 
# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.strip()
# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.replace("\n", "")
# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.replace("\r", "\t")

# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.upper()
# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.replace('"', '')
# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.replace(',', '')
# beneficios_df['Descripcion'] = beneficios_df['Descripcion'].str.strip()

# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.strip()
# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.replace("\n", "")
# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.replace("\r", "\t")

# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.upper()
# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.replace('"', '')
# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.replace(',', '')
# beneficios_df['ResumenEjecutivo'] = beneficios_df['ResumenEjecutivo'].str.strip()

# # # # ## tipo beneficiario
beneficios_df['TipoBeneficiario_id'] =  np.where(beneficios_df['TipoBeneficiario_id']==1, 'Empleado',
                                        np.where(beneficios_df['TipoBeneficiario_id']==2, 'Tercero', 
                                        np.where(beneficios_df['TipoBeneficiario_id']==3, 'Familiar Empleado',
                                        np.where(beneficios_df['TipoBeneficiario_id']==4, 'Institución',
                                        np.where(beneficios_df['TipoBeneficiario_id']==5, 'Comunidad', 'Programa')))))

# ##RINDE COMPROBANTEs
# beneficios_df['RindeComprobante'] = np.where(beneficios_df['RindeComprobante']==1, 'Sí', 
#                                                         np.where(beneficios_df['RindeComprobante']==0, 'No', ''))


## TIPO PERSONA
beneficios_df['TipoPersona_id'] = np.where(beneficios_df['TipoPersona_id']==1, 'Física', 
                                                        np.where(beneficios_df['TipoPersona_id']==2, 'Jurídica', 'Comunidad/Programa'))

# # # # # #Reemplazo valores "NAN"
beneficios_df = beneficios_df.where((pd.notnull(beneficios_df)), None)

beneficios_df = beneficios_df.drop(['SearchName', 'Observaciones', 
                    'DetalleEstado', 'Descripcion', 'ResumenEjecutivo','RindeComprobante'], axis=1)

print(beneficios_df)
beneficios_df.to_csv('beneficiarios_programas_maestros.csv')
