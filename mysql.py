import traceback

import pymysql as MySQLdb


def connect():
    # from utils import config
    # database = config['database']
    return MySQLdb.connect(host='54.201.41.40', user='root', passwd='password', db='live', port=3306,
                           charset='utf8')

def escape(content):
    conn = connect()
    result = conn.escape(content)
    conn.close()
    return result


def query(sql, num):
    """
    Query given sql
    :param sql: SQL that want to query
    :param num: 0 - Execute insert query and get ID for inserted data,
                1 - Get first result
                2 - Get all result
                3 - Get all results with description
    :return: Query results
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute(sql)

    results = []
    if num == 0:
        sql = "SELECT LAST_INSERT_ID()"
        cursor.execute(sql)
        results = cursor.fetchone()
    elif num == 1:
        results = cursor.fetchone()
    elif num == 2:
        results = cursor.fetchall()
    elif num == 3:
        results = cursor.fetchall()
        desc = cursor.description
        cursor.close()
        conn.close()
        return results, desc
    conn.commit()
    cursor.close()
    conn.close()
    return results


def querymany(sql, data):
    '''
    Execute one query(insert or update) with multiple data sets
    :param sql: SQL to query
    :param data: Iterable data sets to map onto SQL
    :return: True if success, otherwise False
    '''
    conn = connect()
    cursor = conn.cursor()

    try:
        cursor.executemany(sql, data)
    except:
        traceback.print_exc()
        logger.exception("Logging an exception")
        return False
    conn.commit()
    cursor.close()
    conn.close()
    return True


def querySqlList(sqlList, printQuery=False):
    conn = connect()
    cursor = conn.cursor()
    try:
        for sql in sqlList:
            if printQuery:
                print(sql)
            cursor.execute(sql)
    except:
        traceback.print_exc()
        logger.exception("Logging an exception")
        conn.rollback()
        return False

    conn.commit()
    cursor.close()
    conn.close()
    return True
