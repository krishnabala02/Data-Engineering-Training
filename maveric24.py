import mysql.connector as sql
import pandas as pd
db_connection = sql.connect(host='localhost',database="maveric",
                            user='root', password="Krishna@2012")
db_cursor = db_connection.cursor()
db_cursor.execute('SELECT * FROM drug200')
table_rows = db_cursor.fetchall()
df = pd.DataFrame(table_rows)
print(df)