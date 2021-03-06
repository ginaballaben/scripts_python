import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from api_sf import *
from datetime import datetime



Beneficios = instancia('Beneficios_a_Personas__c')


#### EXPLORO EL OBJETO DE SALESFORCE #####

# Benef_metadata = sf.Beneficios_a_Personas__c.describe()
# df_prog_metadata = pd.DataFrame(Benef_metadata.get('fields'))
# df_prog_metadata.to_csv('benef_a_personas_metadata.csv', index = False)


##### ME TRAIGO LOS DATOS DEL SQL ######

beneficios = """ select Beneficio.Id as id_db__c, Beneficio.Nombre as Detalle_del_beneficio__c, Beneficio.Descripcion as Justificacion_de_la_solicitud__c, 
        Beneficio.Monto as Monto_aprobado__c, Beneficio.FechaPedido as Fecha_del_Pedido__c, Beneficio.FechaInicioVigencia as Inicio_de_vigencia__c, 
        Beneficio.FechaFinVigencia as Fin_de_vigencia__c, Beneficio.FechaAlta as Fecha_de_aprobaci_n__c, 
        Beneficio.MontoSolicitado as Monto_solicitado__c, Beneficio.RindeComprobante as Rinde_comprobantes__c, Beneficio.TipoBeneficio_id as Tipo__c,
        Beneficio.EstadoBeneficio_id, Area_id, Beneficio.Moneda_id as Moneda__c, ModoPago_id as Modo_de_pago__c, 
        Beneficio.SolicitudAdministrativa, year (Beneficio.FechaPedido) as A_o_beneficio__c, Beneficio.Beneficiario_id as Colaboradores_as_pasible_de_Beca_o_Benef__c,
        EstadoBeneficio.Nombre AS Estado_del_beneficio_de_salud__c
        
		from Beneficio

        inner join EstadoBeneficio on Beneficio.EstadoBeneficio_id = EstadoBeneficio.Id
        inner join TipoBeneficio on Beneficio.TipoBeneficio_id = TipoBeneficio.Id
        WHERE Area_id = 3
        order by FechaPedido """

dest_final = """ select Beneficio_Id as DF_Beneficio_Id, Beneficiario_Id as Beneficiario__c
                from DestinatarioFinal 
                
                inner join Beneficiario on DestinatarioFinal.Beneficiario_Id = Beneficiario.Id
                where TipoPersona_id = 1 
                """

beneficios_df = datos(beneficios, 'Beneficios')
dest_final_df = datos(dest_final, 'Beneficios')

# print(beneficios_df)
# beneficios_df.to_csv('beneficiarios.csv')

# print(dest_final_df)
# dest_final_df.to_csv('dest_final_df.csv')

##Matcheo 
beneficios_df = beneficios_df.merge(dest_final_df, left_on = 'id_db__c', right_on = 'DF_Beneficio_Id', how = 'left')

print(beneficios_df)
#beneficios_df.to_csv("df.csv", index = False)

##Me quedo con los beneficios cuyo destinatario sea una persona fisica
beneficios_df_2 = beneficios_df.dropna(subset=['DF_Beneficio_Id']) ## 741 beneficios 

#beneficios_df_2 = beneficios_df[beneficios_df['DF_Beneficio_Id'].isnull()] ## 220 beneficios de salud cuyos beneficiarios son persoans juridicas


# print(beneficios_df_2)
# beneficios_df_2.to_csv('beneficios_por_migrar.csv', index = False)

# ##Tipo y tipo beneficio 
# beneficios_df_2['Tipo__c'] = np.where(beneficios_df_2['Tipo__c']==1, 'Donaci??n', 
#                                      np.where(beneficios_df_2['Tipo__c']==2, 'Subsidio',
#                                          np.where(beneficios_df_2['Tipo__c']==3, 'Beca', 'Pr??stamo')))

# # #print(beneficios_df_2['Tipo__c'])

# beneficios_df_2['Tipo_de_Beneficio__c'] = np.where(beneficios_df_2['Estado_del_beneficio_de_salud__c']==  'Otorgado s/costo', 
#                                                  'Acompa??amiento y gesti??n', 'Asistencia financiera')

# # ##Estado beneficio 
# beneficios_df_2['Estado_del_beneficio_de_salud__c'] = np.where((beneficios_df_2['A_o_beneficio__c']==2021) & 
#                                (beneficios_df_2['Estado_del_beneficio_de_salud__c']=='Otorgado s/costo'),  'Aprobado', 
#                                 beneficios_df_2['Estado_del_beneficio_de_salud__c'])

# beneficios_df_2['Estado_del_beneficio_de_salud__c'] = np.where((beneficios_df_2['A_o_beneficio__c']==2021) & 
#                                (beneficios_df_2['Estado_del_beneficio_de_salud__c']=='Aprobado Pte. Cuotas'),  'Aprobado', 
#                                 beneficios_df_2['Estado_del_beneficio_de_salud__c'])

# beneficios_df_2['Estado_del_beneficio_de_salud__c'] = np.where((beneficios_df_2['A_o_beneficio__c']==2021) & 
#                                (beneficios_df_2['Estado_del_beneficio_de_salud__c']=='Con??Versi??n??Pendiente'),  'Aprobado', 
#                                 beneficios_df_2['Estado_del_beneficio_de_salud__c'])

# # #print(beneficios_df_2['Estado_del_beneficio_de_salud__c'])

# estado_beneficio = {'En Evaluaci??n':'En an??lisis',
#       'Desestimado':'Desestimado',
#       'Otorgado s/costo': 'Finalizado',
#       'En Aprobaci??n': 'En an??lisis',
#       'Rechazado': 'Desestimado',
#       'Aprobado Pte. Cuotas': 'Finalizado' ,
#       'En Curso' :  'Aprobado', 
#       'Con Versi??n Pendiente' : 'Finalizado',
#       'Finalizado' : 'Finalizado',
#       'Dado de Baja': 'Finalizado'}

# beneficios_df_2['Estado_del_beneficio_de_salud__c'].replace(estado_beneficio, inplace = True)

# # #print(beneficios_df_2['Estado_del_beneficio_de_salud__c'])
# # #beneficios_df_2.to_csv("beneficios_df3.csv", index = False)

# # ## Moneda
# beneficios_df_2['Moneda__c'] = np.where(beneficios_df_2['Moneda__c']==1, 
#                                                  'ARS', 'USD')

# ##Modo de pago 
# beneficios_df_2['Modo_de_pago__c'] = np.where(beneficios_df_2['Modo_de_pago__c']==1, 
#                                                  'Fijo', np.where(beneficios_df_2['Modo_de_pago__c']==2, 'Tope por cuota', 'Tope por total'))

# # ##Fechas 
# beneficios_df_2['Fecha_del_Pedido__c'] = beneficios_df_2['Fecha_del_Pedido__c'].dt.strftime('%Y-%m-%d')
# beneficios_df_2['Inicio_de_vigencia__c'] = beneficios_df_2['Inicio_de_vigencia__c'].dt.strftime('%Y-%m-%d')
# beneficios_df_2['Fin_de_vigencia__c'] = beneficios_df_2['Fin_de_vigencia__c'].dt.strftime('%Y-%m-%d')
# beneficios_df_2['Fecha_de_aprobaci_n__c'] = beneficios_df_2['Fecha_de_aprobaci_n__c'].dt.strftime('%Y-%m-%d')

# # ##Texto 
# beneficios_df_2['Justificacion_de_la_solicitud__c'] = beneficios_df_2['Justificacion_de_la_solicitud__c'].str.strip()
# beneficios_df_2['Justificacion_de_la_solicitud__c'] = beneficios_df_2['Justificacion_de_la_solicitud__c'].str.replace("\n", "")
# beneficios_df_2['Justificacion_de_la_solicitud__c'] = beneficios_df_2['Justificacion_de_la_solicitud__c'].str.replace("\r", "\t")


# beneficios_df_2['Justificacion_de_la_solicitud__c'] = beneficios_df_2['Justificacion_de_la_solicitud__c'].str.upper()
# beneficios_df_2['Detalle_del_beneficio__c'] = beneficios_df_2['Detalle_del_beneficio__c'].str.replace('"', '')
# beneficios_df_2['Detalle_del_beneficio__c'] = beneficios_df_2['Detalle_del_beneficio__c'].str.strip()

# # ##CAMPO RINDE COMPROBANTES 
# beneficios_df_2['Rinde_comprobantes__c'] = np.where(beneficios_df_2['Rinde_comprobantes__c']==1, 'TRUE', 
#                                                       np.where(beneficios_df_2['Rinde_comprobantes__c']==0, 'FALSE', ''))
# #beneficios_df_2['Rinde_comprobantes__c'] = beneficios_df_2['Rinde_comprobantes__c'].str.upper()

# # ##Selecciono columnas 
# beneficios_df_2 = beneficios_df_2.drop(['EstadoBeneficio_id','Area_id', 'SolicitudAdministrativa','DF_Beneficio_Id'], axis=1 )

# # # #Reemplazo valores "NAN"
# beneficios_df_2 = beneficios_df_2.where((pd.notnull(beneficios_df_2)), None)

# print(beneficios_df_2)
# #beneficios_df_2.to_csv("beneficios_a_migrar.csv", index = False) ## 741 beneficios

# # ##ME TRAIGO DATOS DE SF PARA MATCHEAR CONTACTOS
# contactos = instancia('Contact')
# query = '''SELECT Id_18__c, id_db__c, RecordTypeId
#                  FROM Contact'''

# sf_contactos = sf.bulk.Contact.query(query)
# sf_contactos = pd.DataFrame(sf_contactos)

# sf_contactos = sf_contactos.loc[sf_contactos['RecordTypeId']!='0124W000000tvzeQAA'] ##colaboradores, fliares o terceros
# sf_contactos['id_db__c'] = pd.to_numeric(sf_contactos['id_db__c'])

# print(sf_contactos)
# sf_contactos.to_csv('sf_contactos.csv', index = False)

# ##MATCH 
# beneficios_df_test = beneficios_df_2.merge(sf_contactos, left_on = 'Colaboradores_as_pasible_de_Beca_o_Benef__c', right_on = 'id_db__c', how = 'left') ## me quedo con beneficiarios ya migrados


# beneficios_df_test = beneficios_df_test.drop(['Colaboradores_as_pasible_de_Beca_o_Benef__c', 'attributes', 'RecordTypeId'], axis = 1)
# beneficios_df_test = beneficios_df_test.rename(columns = {'id_db__c_x' : 'id_db__c', 'Id_18__c':'Colaboradores_as_pasible_de_Beca_o_Benef__c' })

# print(beneficios_df_test) ## 741 beneficios

# beneficios_df_test = beneficios_df_test.merge(sf_contactos, left_on = 'Beneficiario__c', right_on = 'id_db__c', how = 'left') ## me quedo con destintarios ya migrados

# beneficios_df_test = beneficios_df_test.drop(['id_db__c_y','attributes','id_db__c_y','RecordTypeId', 'Beneficiario__c'], axis = 1)
# beneficios_df_test = beneficios_df_test.rename(columns = {'id_db__c_x' : 'id_db__c', 'Id_18__c':'Beneficiario__c' })

# beneficios_df_test = beneficios_df_test.dropna(subset=['Beneficiario__c', 'Colaboradores_as_pasible_de_Beca_o_Benef__c']) ## 646 beneficios , pierde 55 beneficios como 20541. 

# ###RecordtypeId
# beneficios_df_test['RecordTypeId'] = '0124W000001AcYEQA0'


# ##Asociarlo a su programa 
# beneficios_df_test['Fecha_del_Pedido__c'] = pd.to_datetime(beneficios_df_test['Fecha_del_Pedido__c'], format='%Y-%m-%d')

# beneficios_df_test['Programa__c'] = np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2010') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2011'),'a0r4W00000dJhc8QAC',
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2011') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2012'),'a0r4W00000dJhc9QAC', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2012') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2013'),'a0r4W00000dJhcAQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2013') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2014'),'a0r4W00000dJhcBQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2014') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2015'),'a0r4W00000dJhcCQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2015') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2016'),'a0r4W00000dJhcDQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2016') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2017'),'a0r4W00000dJhcEQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2017') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2018'),'a0r4W00000dJhcFQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2018') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2019'),'a0r4W00000dJhcGQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2019') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2020'),'a0r4W00000dJhcHQAS', 
#                                     np.where((beneficios_df_test['Fecha_del_Pedido__c']>='1-4-2020') & (beneficios_df_test['Fecha_del_Pedido__c']<='31-3-2021'),'a0r4W00000dJhcIQAS', 'a0r4W00000dIfOVQA0' )))))))))))

# beneficios_df_test['Fecha_del_Pedido__c'] = beneficios_df_test['Fecha_del_Pedido__c'].dt.strftime('%Y-%m-%d')
# print(beneficios_df_test)
# beneficios_df_test.to_csv('beneficios_a_migrar_con_contacto.csv', index = False)   ### 686 beneficios

##RINDECOMPROBANTES

# # ## Test 
#beneficios_df_test = beneficios_df_test.loc[beneficios_df_test['id_db__c']==29721]
#beneficios_df_test = beneficios_df_test.head(2)
#print(beneficios_df_test) 

#beneficios_df_test.to_csv('beneficios_df_test.csv', index = False)

# # ######## ENVIO A SALESFORCE #############
# # ## A OBJETO Beneficios
# beneficios_a_migrar = instancia('Beneficios_a_Personas__c')
# beneficios_df_test = beneficios_df_test.to_dict('records')
# resultado = sf.bulk.Beneficios_a_Personas__c.insert(beneficios_df_test)
# print(resultado) 

