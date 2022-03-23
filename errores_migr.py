import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re



##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########


errores = '''select * from [Fundación Perez Companc$Interfaz Beneficios] 
            where Fecha_Registro >= '2021-01-01 00:00:00.000' and Destino = 1 and Importado = 0 and Registrado = 0
             ORDER BY Nro_Mov DESC''' 

errores_df = datos(errores, 'FPCBC13')
print(errores_df)

errores_df = errores_df.replace([None],'')

print(errores_df)
errores_df.to_csv("pagados.csv", index = False)

###### ACTUALIZACION EN SF #######

# transacciones = instancia('Transacciones__c')

# errores_update = errores_df.to_dict('records')

# print (errores_update)
# resultado = sf.bulk.Transacciones__c.update(errores_update)
# print(resultado)