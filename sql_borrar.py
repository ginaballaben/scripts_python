import pyodbc
import simple_salesforce
import numpy as np
from conexion_gina import *
from sqlalchemy import create_engine
import re

##### ELIMINAR DATOS EN EL SQL SERVER ####

# conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                                              "Server=FPC14;"
#                                                              "DATABASE=FPCB-C13;"   #Esto es lo unico que cambia
#                                                              "Trusted_Connection=yes;")

# cursor = conexion.cursor()
# cursor.execute('''DELETE FROM dbo.[Fundación Perez Companc$Interfaz Beneficios] WHERE  Nro_Mov >= xx555887 and Nro_Mov <= xx557721''')  

# conexion.commit()
# cursor.close()
# conexion.close()