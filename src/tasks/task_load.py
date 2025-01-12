from prefect import task
import mysql.connector

@task(name="Load")
def task_load(data):
    try:
        conn = mysql.connector.connect(
            user='root',
            password='root',
            host='localhost',
            database='datag3'
        )
        cursor = conn.cursor()

        query_table = """
        create table if not exists linkedin_offers(
        id INT AUTO_INCREMENT PRIMARY KEY,
        titulo VARCHAR(255),
        ubicacion VARCHAR(255),
        url TEXT,
        empresa VARCHAR(255),
        fecha DATE
        )
        """
        cursor.execute(query_table)
        conn.commit()

        query_insert = """
        insert into linkedin_offers(titulo,ubicacion,empresa,fecha,url)
        values(%s,%s,%s,%s,%s)
        """

        for offer in data:
            cursor.execute(query_insert,offer)

        conn.commit()
        cursor.close()
        conn.close()
        print("datos guardados en bd...")
    except mysql.connector.Error as err:
        print(err)
