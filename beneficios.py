import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from connection import *
from api_sf import *


Beneficios = instancia('Beneficios_a_Personas__c')


#### EXPLORO EL OBJETO DE SALESFORCE #####

Benef_metadata = sf.Beneficios_a_Personas__c.describe()
df_prog_metadata = pd.DataFrame(Benef_metadata.get('fields'))
#df_prog_metadata.to_csv('benef_a_personas_metadata.csv', index = False)


##### ME TRAIGO LOS DATOS DEL SQL ######

query = """ select EstadoBeca.Detalle AS Estado_de_la_beca__c, Beca.Acompanante_id AS  Acomp_id, DestinatarioFinal.Beneficiario_id AS becade_id, Empleado.Beneficiario_id AS empleade_id, Erogacion.Monto AS Tipo_de_acompaamiento__c, 
		NivelEducativo.Nombre AS Nivel_Educativo__c, TipoTurno.Detalle AS Turno__c, CondicionBeca.Nombre AS Tipo_de_beca__c, Beneficio.Id AS id_db__c, year (Beneficio.FechaPedido) AS Anio_beca,  TipoInstitucionEducativa.Detalle AS ambito_esc,
        Erogacion.Id AS id_pago, Beneficiario.NumDocumento AS becade_dni
        
		from Beca

		inner join BeneficioVersion on Beca.BeneficioVersion_id = BeneficioVersion.Id
		inner join Beneficio on BeneficioVersion.Beneficio_id = Beneficio.Id
        inner join DestinatarioFinal on DestinatarioFinal.Id = beca.DestinatarioFinal_id
		inner join Beneficiario on DestinatarioFinal.Beneficiario_id = Beneficiario.Id
        inner join Empleado on beca.Empleado_Id = Empleado.Id
        left join Erogacion on Erogacion.PlanCuotas_id = beca.PlanCuotas_id
        left join CondicionBeca on beca.CondicionBeca_id = CondicionBeca.Id
        left join NivelEducativo on beca.NivelEducativo_id = NivelEducativo.Id
        left join EstadoBeca on beca.EstadoBeca_id = EstadoBeca.Id
        left join TipoTurno on beca.TipoTurno_id = TipoTurno.Id
		left join InstitucionEducativa on beca.InstitucionEducativa_id = InstitucionEducativa.Id
		left join TipoInstitucionEducativa on InstitucionEducativa.TipoInstitucionEducativa_id = TipoInstitucionEducativa.Id

        WHERE year (Beneficio.FechaPedido) >=2010 


        order by FechaPedido """

Estado_beca = {'Pendiente':'Cancelada',
            'Suspendida':'Completada',
            'Dada de Baja': 'Dada de baja por cese de relación con la empresa',
            'Desestimada': 'Dada de baja por no cumplir los requisitos',
            'En Curso': 'En curso / Activa',
            'Finalizada': 'Completada'}

Condicion_beca = {
    'Especial': '',
    'Estimulo': 'Estímulo',
    'Merito' : 'Mérito',
    'No Cobra': '',
    'Promedio': 'Promedio',
    'Riesgo': 'Riesgo',
    'Rojo': 'Rojo'
}


nivel_educativo = {
    'Primario':'Primaria',
    'Secundario':'Secundaria',
    'Universitario': 'Superior',
    'Terciario': 'Superior',
    'Especial': '',
    'Merito': '',
    'Fines':''

}

turno = {'Jornada Completa': 'Jornada completa'}

#Desestimar_el_pr_ximo_a_o__c: Sí

#Tipo_de_Beneficio__c: Sin costo, Con costo

df = datos(query, 'Beneficios')



###### TRANSFORMO LOS DATOS ######

#Reemplazo valores

df['Estado_de_la_beca__c'].replace(Estado_beca, inplace=True)
df['Tipo_de_beca__c'].replace(Condicion_beca, inplace=True)
df['Nivel_Educativo__c'].replace(nivel_educativo, inplace=True)
df['Turno__c'].replace(turno, inplace=True)


#Creo columnas que necesito con valores nulos, para completarlos después 

df["Tipo_de_Beneficio__c"] = np.nan
df["Desestimar_el_pr_ximo_a_o__c"] = np.nan
df['Programa__c'] = np.nan


#Completo las columns que creé 

df['Desestimar_el_pr_ximo_a_o__c'] = np.where((df['Tipo_de_beca__c'] == 'Rojo') & (df['Anio_beca'] == 2020), 'Sí', '')


df['Tipo_de_Beneficio__c'] = np.where(df['Tipo_de_beca__c'] == 'No Cobra', 'Sin costo', 'Con costo')

df['Programa__c'] = np.where(df['Anio_beca'] == 2010, 'a0r4W00000bjGnlQAE', df['Programa__c']) 
df['Programa__c'] = np.where(df['Anio_beca'] == 2011, 'a0r4W00000bjJEbQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2012, 'a0r4W00000bjFGlQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2013, 'a0r4W00000bjFGmQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2014, 'a0r4W00000bjFGoQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2015, 'a0r4W00000bjFGqQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2016, 'a0r4W00000bjFGrQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2017, 'a0r4W00000bjFGsQAM', df['Programa__c'])
df['Programa__c'] = np.where(df['Anio_beca'] == 2018, 'a0r4W00000bjFGtQAM', df['Programa__c'])

#2019 y 2020 en Adelante


df = df.fillna(0)
df.Tipo_de_acompaamiento__c.astype(int)

#Primaria

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 900) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFGwQAM', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1000) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFGwQAM', df['Programa__c']) #2019

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1330) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHIQA2', df['Programa__c']) #2020

#Secundaria hasta 3

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1100) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFH1QAM', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1150) & (df['Anio_beca'] == 2019) &  (df['ambito_esc'] == 'Privada'), 'a0r4W00000bjFH1QAM', df['Programa__c']) #2019

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1570) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHSQA2', df['Programa__c']) #2020

#Secundaria 4 o más

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1150) & (df['Anio_beca'] == 2019) &  (df['ambito_esc'] == 'Pública'), 'a0r4W00000bjFH4QAM', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1200) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFH4QAM', df['Programa__c']) #2019

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1680) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHTQA2', df['Programa__c']) #2020

#Superior 1 año

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1300) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFH5QAM', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1840) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHUQA2', df['Programa__c']) #2020

#Superior primera mitad de la carrera

df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1900) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFH6QAM', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 2660) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHeQAM', df['Programa__c']) #2020


#Superior segunda mitad de la carrera
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 2700) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFHGQA2', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 3780) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHfQAM', df['Programa__c']) #2020


#Merito
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 1250) & (df['Anio_beca'] == 2019), 'a0r4W00000bjFGuQAM', df['Programa__c']) #2019
df['Programa__c'] = np.where((df['Tipo_de_acompaamiento__c'] == 2000) & (df['Anio_beca'] == 2020), 'a0r4W00000bjFHHQA2', df['Programa__c']) #2020

print(df)

df['Tipo_de_acompaamiento__c'] = df.Tipo_de_acompaamiento__c.astype(int)
df['Acomp_id'] = df.Acomp_id.astype(int)

print(df)



######## TRAIGO LOS DATOS DESDE SALESFORCER #########

#Datos de lxs becadxs

query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Contact'
sf_becades = sf.bulk.Contact.query(query)
sf_becades = pd.DataFrame(sf_becades)
sf_becades = sf_becades[sf_becades['RecordTypeId']=='0124W000001ANfeQAG']

sf_becades.to_csv('becadxs_migradas_22-12.csv', index = False)

print(sf_becades)

######## TRANSFORMO LOS DATOS #########

sf_becades['id_db__c'] = pd.to_numeric(sf_becades['id_db__c'])

sf_becades = sf_becades.rename(columns={'id_db__c':'id_becadx'})

#Hago el match con los datos del sql

df=df.merge(sf_becades, left_on='becade_id', right_on='id_becadx', how='left')

df = df.drop(['attributes', 'Name', 'RecordTypeId', 'id_becadx'], axis=1)

print(df)

df = df.rename(columns={'Id':'Beneficiario__c'})

#Me traigo empleadxs

query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Contact'
sf_becades = sf.bulk.Contact.query(query)
sf_becades = pd.DataFrame(sf_becades)
sf_becades = sf_becades[sf_becades['RecordTypeId']=='0124W000001ANfUQAW']

sf_becades.to_csv('colaboradores_migradas_22-12.csv', index = False)

print(sf_becades)

#Transformo los datos

sf_becades['id_db__c'] = pd.to_numeric(sf_becades['id_db__c'])

sf_becades = sf_becades.rename(columns={'id_db__c':'id_emp'})

#Hago el match

df=df.merge(sf_becades, left_on='empleade_id', right_on='id_emp', how='left')

df = df.drop(['attributes', 'Name', 'RecordTypeId', 'id_emp'], axis=1)

print(df)

df = df.rename(columns={'Id':'Colaboradores_as_pasible_de_Beca_o_Benef__c'})

#Me traigo acompañantes y hago el match

query = 'SELECT Id, Name, RecordTypeId, id_db__c, Puesto__c FROM Contact'
sf_becades = sf.bulk.Contact.query(query)
sf_becades = pd.DataFrame(sf_becades)
sf_becades = sf_becades[sf_becades['RecordTypeId']=='0124W000001ANfUQAW']
sf_becades = sf_becades[sf_becades['Puesto__c']=='Acompañante']

sf_becades.to_csv('acompaniantes_migradas_22-12.csv', index = False)

print(sf_becades)


sf_becades['id_db__c'] = pd.to_numeric(sf_becades['id_db__c'])

sf_becades = sf_becades.rename(columns={'id_db__c':'id_acomp'})

df=df.merge(sf_becades, left_on='Acomp_id', right_on='id_acomp', how='left')

df = df.drop(['attributes', 'Name', 'RecordTypeId', 'Acomp_id', 'Puesto__c', 'id_acomp'], axis=1)

print(df)

df = df.rename(columns={'Id':'Acompaniante_de_becado__c'})


#Hago las trasnformaciones adicionales 

df = df.drop_duplicates(df.columns[df.columns.isin(['id_pago'])],
                        keep='last')

df = df.drop_duplicates(df.columns[df.columns.isin(['id_db__c'])],
                        keep='last')


df = df.drop(['Anio_beca', 'ambito_esc', 'empleade_id', 'becade_id', 'id_pago', 'becade_dni', 'id_mig'], axis=1)


#df.to_csv('lista-becas-completa2.csv', index = False)

#

df['RecordTypeId'] = '0124W000001AcY9QAK'

df['Tipo_de_beca__c'] = df['Tipo_de_beca__c'].replace([0],'')
df['Turno__c'] = df['Turno__c'].replace([0],'')


df.replace(r'^\s*$', np.nan, regex=True)

df = df.where((pd.notnull(df)), None)
df = df.replace([None],'')



########  SE MIGRA ##########

#Datos migrar

pd1 = df[pd.isnull(df.Beneficiario__c)]

df2= df[(df['Beneficiario__c']!="nan")]
df2= df[(df['Programa__c']!="nan")]

print("Se migra")
print(df2)

df2.to_csv('beneficios-migrados-ultimo.csv', index = False)

#No se migra

df3 = df[df['id_db__c'].isin(df2['id_db__c']) == False]

print(df3)

#df3.to_csv('beneficios-sinmigrar.csv', index = False)

#Migración

# benef = df2.to_dict('records')
# resultado = sf.bulk.Beneficios_a_Personas__c.insert(benef)
# print(resultado)



