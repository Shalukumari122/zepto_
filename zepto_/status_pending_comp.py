import pymysql

# Connect to the database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='zepto_'
)

cur = conn.cursor()
update_query="UPDATE `zepto_links_comp` SET `status`='pending'"
cur.execute(update_query)
conn.commit()
