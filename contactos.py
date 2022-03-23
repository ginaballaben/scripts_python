import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *    #from connection import *
from api_sf import *




Contactos_metadata = sf.Contact.describe()
df_contactos_metadata = pd.DataFrame(Contactos_metadata.get('fields'))
df_contactos_metadata.to_csv('contactos_metadata.csv', index = False)


""" select * from Beca

inner join InstitucionEducativa on InstitucionEducativa.Id = beca.InstitucionEducativa_id

inner join DestinatarioFinal on DestinatarioFinal.Id = beca.DestinatarioFinal_id

inner join Beneficiario on DestinatarioFinal.Beneficiario_id = Beneficiario.Id """


#Esto me va a servir para asociar becados con instituciones educativas

""" SELECT distinct  Becado.Nombre_Razon +' ' + Becado.Apellido as Becado,BENEF.Nombre_Razon +  ' ' + BENEF.Apellido as Padre, Ben.Id, BECADO.Id AS becado_Id
FROM BECA INNER JOIN DESTINATARIOFINAL DF ON BECA.DESTINATARIOFINAL_ID=DF.ID
INNER JOIN BENEFICIO BEN ON DF.BENEFICIO_ID=BEN.ID
INNER JOIN BENEFICIARIO BENEF ON BEN.BENEFICIARIO_ID = BENEF.ID
INNER JOIN EMPLEADO EMP ON BECA.EMPLEADO_ID=EMP.ID
INNER JOIN EMPRESA E ON E.ID = EMP.EMPRESA_ID
INNER JOIN BENEFICIARIO BECADO ON DF.BENEFICIARIO_ID = BECADO.ID  """


#Becas
""" select EstadoBeca.Detalle AS Estado_beca, Beca.Acompanante_id AS  Acompañante_id, DestinatarioFinal.Beneficiario_id AS becade_id, Empleado.Beneficiario_id AS empleado_id, Beneficio.FechaPedido, Beneficio.FechaAlta, Beneficio.Monto, 
		Beneficio.MotivoBeneficio_id,NivelEducativo.Nombre AS nivel_educativo, TipoTurno.Detalle AS turno
 from Beca

 inner join DestinatarioFinal on DestinatarioFinal.Id = beca.DestinatarioFinal_id
 inner join Empleado on beca.Empleado_Id = Empleado.Id
 inner join BeneficioVersion on Beca.BeneficioVersion_id = BeneficioVersion.Id
 inner join Beneficio on BeneficioVersion.Beneficio_id = Beneficio.Id
 left join NivelEducativo on beca.NivelEducativo_id = NivelEducativo.Id
 left join EstadoBeca on beca.EstadoBeca_id = EstadoBeca.Id
 left join TipoTurno on beca.TipoTurno_id = TipoTurno.Id

 where Beneficio.MotivoBeneficio_id = 5 """