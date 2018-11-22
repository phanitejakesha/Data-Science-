import mysql.connector
import sys


#change the username and password with respect to your system and then execute 
databaseName = sys.argv[1]

cnx = mysql.connector.connect(user='root', database=databaseName,password='Password')
cursor = cnx.cursor(buffered=True)


query = ("show tables")
cursor.execute(query)
li = []
for table in cursor:
    li.append(str(table)[2:len(str(table))-3])


cursor.close()

li2 =[]
for i in range(0,len(li)):
    cursor = cnx.cursor(buffered=True)
    query = ("select count(*) from " + li[i])
    cursor.execute(query)
    for table_length in cursor:
        li2.append(str(table_length))
        cursor.close()

for i in range(0,len(li)):
    print(li[i],li2[i])
cnx.close()
