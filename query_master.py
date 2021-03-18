from config import config
import psycopg2
import traceback
import sys


def get_degree_id(degree_code):
    sql = """
            SELECT id FROM degree
            WHERE code = %s;
         """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (degree_code,))
        degree_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return degree_id


def get_department_id(department_code):
    sql = """
             SELECT id FROM department
             WHERE code = %s;
          """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (department_code,))
        department_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return department_id


def get_group_id(group_name):
    sql = """
            SELECT id FROM "group"
            WHERE name = %s;
         """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (group_name,))
        group_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return group_id


def get_group_degree_id(group_id):
    sql = """
            SELECT degree_id FROM "group"
            WHERE id = %s;
        """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (group_id,))
        degree_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return degree_id


def get_level_id(level_code):
    sql = """
            SELECT id FROM level
            WHERE code = %s;
         """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (level_code,))
        level_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return level_id


def get_subject_id(subject_code, degree_id):
    sql = """
            SELECT id FROM subject
            WHERE code = %s AND degree_id = %s;
          """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (subject_code, degree_id,))
        subject_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return subject_id   


def get_topic_id(topic_name):
    sql = """
            SELECT id FROM topic
            WHERE name = %s;
         """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (topic_name,))
        topic_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return topic_id


def get_type_id(type_name):
    sql = """
            SELECT id FROM type
            WHERE name = %s;
         """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (type_name,))
        type_id = cursor.fetchone()[0]
        cursor.close()
        conn.commit
    except (Exception, psycopg2.DatabaseError) as error:
        catch_exception(error)
    finally:
        if conn is not None:
            conn.close()
        return type_id


def succeed():
    print('\033[92m' + 'OK' + '\033[0m')


def catch_exception(e):    
    print(str(e))
    print(traceback.format_exc())    
    sys.exit()
