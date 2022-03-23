import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re

###csv de la migracion 

migr_abril = pd.read_csv("Transacciones_16_04.csv")
print(migr_abril)

##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########


nro_mov = '''SELECT Nro_Mov, Nro_Beneficio, Beneficiario, Nro_Documento FROM [Fundación Perez Companc$Interfaz Beneficios]
            WHERE Importado = 1 and Tipo_de_Beneficio = 'BECA' and Fecha_Registro = '2021-04-16 00:00:00.000'
            ORDER BY Nro_Mov DESC'''

nro_mov_df = datos(nro_mov, 'FPCBC13')

## merge

migr_abril_mov = migr_abril.merge(nro_mov_df, left_on = 'N_m_documento__c', right_on = 'Nro_Documento', how = 'left')
print(migr_abril_mov)

## quito cols 
migr_abril_sil = migr_abril_mov.loc[:,('Nro_Mov', 'N_m_documento__c')]
print(migr_abril_sil)
migr_abril_sil.to_csv("migr_abril_mov.csv", index = False)
