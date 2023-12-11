import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
    specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def close_connection(conn):
    conn.close()

def execute_sql(conn, sql):
    """ Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_data(conn, sql, data):
    """
    Create a new rowt in the chosen table
    :param conn:
    :param sql: 
    :param project:
    :return: project id
    """
    cur = conn.cursor()
    cur.execute(sql, data)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall() 
    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a task
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
              SET {parameters}
              WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_all(conn, table):
    """
    delete all table contents
    :param curr:
    :param table:
    """
    sql = f'''DELETE FROM {table}'''
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print(f'DELETED ALL IN {table}')
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f'{k}=?')
        values += (v,)
    q = " AND ".join(qs)
    
    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print('DELETED SELECTED')


if __name__ == '__main__':
    sql_create_cafes = """
   -- cafes table
   CREATE TABLE IF NOT EXISTS cafes (
      id integer PRIMARY KEY,
      nazwa text NOT NULL,
      start_date text,
      end_date text
   );
   """

    sql_create_orders = """
       -- orders table
       CREATE TABLE IF NOT EXISTS orders (
          id integer PRIMARY KEY,
          cafe_id integer NOT NULL,
          stolik VARCHAR(250) NOT NULL,
          zawartosc TEXT,
          status VARCHAR(15) NOT NULL,
          start_date text NOT NULL,
          end_date text NOT NULL,
          FOREIGN KEY (cafe_id) REFERENCES cafes (id)
       );
       """
    
    sql_insert_cafe = '''
    INSERT INTO cafes(nazwa, start_date, end_date)
    VALUES(?,?,?)
    '''
    
    sql_insert_order = '''
    INSERT INTO orders(cafe_id, stolik, zawartosc, status, start_date, end_date)
    VALUES(?,?,?,?,?,?)
    '''
    db_file = r"cafe_database.db"
    conn = create_connection(db_file)
    if conn:
        #add tables
        execute_sql(conn, sql_create_cafes)
        execute_sql(conn, sql_create_orders)
        #add tables contents
        delete_all(conn, 'cafes')#tutaj, żeby się nie mnożyło tych postaci
        delete_all(conn, 'orders')#tutaj, żeby się nie mnożyło tych postaci
        cafe = ("Pozegnanie z Afryka Warszawa", "2020-05-11 00:00:00", "2020-05-13 00:00:00")
        cafe_id = add_data(conn, sql_insert_cafe, cafe)
        order = (
            cafe_id,
            "ostatni w sali bez baru",
            "flat white na Nepalu, cappucino na Etiopi",
            "started",
            "2020-05-11 12:00:00",
            "2020-05-11 15:00:00"
        )
        task_id = add_data(conn, sql_insert_order, order)
        #Query
        cafes = select_all(conn, 'cafes')
        orders = select_where(conn, 'orders', status='done')
        #uptade
        update(conn, 'orders', 2, status='done')
        #delete
        
        delete_where(conn, 'orders', id=5)
        #close connection
        close_connection(conn)
        print(cafe_id, task_id)
        for cafe in cafes:
            print(cafe)
        for order in orders:
            print(order)
