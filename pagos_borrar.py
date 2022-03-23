import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re



##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########

pagos = '''SELECT Nro_Mov, Nro_documento, Nro_Beneficio, Cuota FROM [Fundación Perez Companc$Interfaz Beneficios]
            WHERE Nro_Mov > 511904 and Fecha_Registro = '2021-05-12 00:00:00.000' 
            ORDER BY Nro_Mov DESC''' 

pagos_df = datos(pagos, 'FPCBC13')

pagos_df = pagos_df.drop_duplicates(pagos_df.columns[pagos_df.columns.isin(['Nro_documento'])],
                         keep='first')
#print(pagos_df)

pagos_df = pagos_df[(pagos_df['Cuota']==1) | (pagos_df['Cuota']==2)]
print(pagos_df)

#pagos_df.to_csv("pagos_12__05.csv", index = False)