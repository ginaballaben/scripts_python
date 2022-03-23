import pyodbc

print(pyodbc.drivers())

class Database:
	def __init__(self, name, server = 'FPC14', driver = 'SQL Server Native Client 11.0' ):
		self.__name = name
		self.__server = server
		self.__driver = driver
		self.__conexion = None
		self.__datos = None

	def conectar(self):
		self.__conexion = pyodbc.connect("DRIVER={"+self.__driver+"}; "
											"Server="+self.__server+"; "
											"DATABASE=" +self.__name+"; "
											"Trusted_connection=yes;")

	def cursor(self):
		self.__cursor = self.__conexion.cursor()

	def consulta(self, query, values = None):
		if values:
			self.__cursor.excute(query, values)
		else:
			self.__cursor.excute(query)

	def cerrar(self):
		self.__conexion.close()

	def ejecutar(self, query, values = None):
		self.conectar()
		self.cursor()
		self.consulta()
		self.cerrar()
