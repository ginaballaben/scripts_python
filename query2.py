import pyodbc
import pandas as pd

print(pyodbc.drivers())

conn = pyodbc.connect("DRIVER={ODBC Driver 11 for SQL Server}; "
						"Server= FPC14; "
						"DATABASE = Beneficios; "
						"Trusted_connection=yes;")

cursor = conn.cursor()

df_tablas = pd.read_sql_query("SELECT * FROM Beneficios.dbo.Beneficiario", conn)

""" df_tablas_detalle = pd.read_sql_query(
	'''
	SELECT
	schemas.name AS Schema_Name,
	tables.name AS Table_Name,
	columns.name AS Column_Name,
	UPPER(types.name) AS Column_Data_Type,
	columns.max_length AS Column_Length,
	columns.precision AS Column_Precision,
	columns.scale AS Column_Scale
	FROM sys.schemas
	INNER JOIN sys.tables
	ON schemas.schema_id = tables.schema_id
	INNER JOIN sys.columns
	ON tables.object_id = columns.object_id
	INNER JOIN sys.types
	ON columns.user_type_id = types.user_type_id;
	'''
	, conn) """

print(df_tablas)
#print(df_tablas_detalle)

#df_tablas.to_excel(r"C:\Users\NAV2019VOXA\Downloads\ZIGLA\tablas.xlsx", index = False)

#df_tablas_detalle.to_excel(r"C:\Users\NAV2019VOXA\Downloads\ZIGLA\tablas_detalle.xlsx", index = False)


