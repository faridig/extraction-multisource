import pyodbc

conn = pyodbc.connect("DRIVER={SQL Server};SERVER=adventureworks-server-hdf;DATABASE=adventureworks;UID=jvcb;PWD=cbjv592023!")
cursor = conn.cursor()
query = "SELECT * FROM your_table"
data = cursor.execute(query).fetchall()
print(data)
