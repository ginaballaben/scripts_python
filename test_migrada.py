import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re

# # ##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########

registros = '''SELECT Nro_Mov, Fecha_Registro, Fecha_Documento, Fecha_Vencimiento, 
            Importe, Nro_Documento_Pago, Tipo_de_Gasto, Comentario, Tipo_Cambio, Nro_Documento, Destino, Cuota, Importado,
            Registrado, Fecha_Importacion, Fecha_AcreditacionOP
              FROM [Fundación Perez Companc$Interfaz Beneficios]
              WHERE Nro_Mov = 563903
              ORDER BY Nro_Mov DESC'''

registros = datos(registros, 'FPCBC13')
print(registros)


##### INSERTAR DATOS EN EL SQL SERVER ####

conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                                   "Server=FPC14;"
                                                   "DATABASE=FPC-Migrada;"   #Esto es lo unico que cambia
                                                   "Trusted_Connection=yes;")

cursor = conexion.cursor()

cursor.executemany('''INSERT INTO dbo.[Fundación Perez Companc$Interfaz Beneficios]([Nro_Mov],[Fecha_Registro], 
                           [Fecha_Documento], [Fecha_Vencimiento], [Importe], [Nro_Documento_Pago], [Tipo_de_Gasto], [Comentario],
                           [Tipo_Cambio], [Nro_Documento],[Destino], [Cuota], [Importado], [Registrado], [Fecha_Importacion],[Fecha_AcreditacionOP])    
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)''')  

conexion.commit()
cursor.close()
conexion.close()

 