from beneficiosDB import *
from connection import *    #from connection import *


#Compañías

companias = "SELECT Id, Nombre AS Name FROM Beneficios.dbo.Empresa" 

plantas = '''SELECT Id, Empresa_id, Establecimiento.Nombre AS Name, Empresa.Nombre AS ParentId FROM Beneficios.dbo.Establecimiento
                INNER JOIN Beneficios.dbo.Empresa
                ON Empresa.Id = Beneficios.dbo.Establecimiento.Empresa_id'''

escuelas = '''SELECT Nombre_Razon AS Name, Tel AS Phone, Domicilio.Direccion AS ShippingAdress, Localidad.Nombre AS Localidad, Provincia.Nombre AS Provincia, CodigoPostal.Nombre AS CP FROM Beneficiario
                INNER JOIN Domicilio
                ON Beneficiario.Id = Domicilio.Id
                INNER JOIN Localidad 
                ON Domicilio.Localidad_id = Localidad.Id
                INNER JOIN Provincia
                ON Localidad.Provincia_id = Provincia.Id
                INNER JOIN CodigoPostal
                ON Domicilio.CodigoPostal_id = CodigoPostal.Id

                WHERE Beneficiario.TipoBeneficiario_id = 4 AND Beneficiario.InstitucionEducativa = 1 AND Beneficiario.Id IN (
	                SELECT Beneficiario_id FROM Beneficio
	                WHERE TipoBeneficio_id = 3)'''

df = Beneficios.Database.ejecutar(query) 

print(df)






#df = pd.read_sql_query(consulta, )