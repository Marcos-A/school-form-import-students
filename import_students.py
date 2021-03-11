from config import config
import csv
import psycopg2
import os
import sys
from query_master import *


CF_FILE = 'input/cf_students.csv'
ESOBTX_FILE = 'input/eso-btx_students.csv'


def read_esobtx_file(input_file):
    students = {}
    with open(input_file, 'r', encoding='utf-8') as students_file:
        students_reader = csv.DictReader(students_file)

        for student in students_reader:
            email = student['Adreça electrònica']
            name = student['Nom']
            surname = student['Cognoms']
            # ESO-Batxillerat students real classgroup is ignored, it's set to their level instead
            group_id = get_group_id(student['Nivell'])
            
            students[email] = {}
            students[email]['name'] = name
            students[email]['surname'] = surname
            students[email]['group_id'] = group_id
            students[email]['enrolled_subjects'] = 'Centre'

        return students


def insert_esobtx_students(esobtx_students):
    for student in esobtx_students:
        email = student
        name = esobtx_students[student]['name']
        surname = esobtx_students[student]['surname']
        group_id = esobtx_students[student]['group_id']

        student_data = (email, name, surname, group_id,)
        sql = """
                INSERT INTO student(email, name, surname, group_id)
                VALUES(%s, %s, %s, %s)
                RETURNING id;
              """
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            cursor.execute(sql, student_data)
            student_id = cursor.fetchone()[0]
            cursor.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

        enrolled_subjects = esobtx_students[student]['enrolled_subjects']
        insert_student_enrolled_subjects(student_id, group_id, enrolled_subjects)


def read_cf_file(input_file):
    students = {}
    with open(input_file, 'r', encoding='utf-8') as students_file:
        students_reader = csv.DictReader(students_file)

        for student in students_reader:
            email = student['CORREU']
            # level = 'CF'
            name = student['ALUMNE'].split(', ')[1]
            surname = student['ALUMNE'].split(', ')[0]
            group_id = get_group_id(student['GRUP'])
            enrolled_subjects = ','.join([key for key in student
                                          if 'mp' in key.lower() and
                                          student[key].lower() == 'x'])
            if ('1' in student['GRUP']):
                enrolled_subjects += ',Tutoria1'
            elif ('2' in student['GRUP']):
                enrolled_subjects += ',Tutoria2'
            enrolled_subjects += ',Centre'

            students[email] = {}
            students[email]['name'] = name
            students[email]['surname'] = surname
            students[email]['group_id'] = group_id
            students[email]['enrolled_subjects'] = enrolled_subjects

        return students


def insert_cf_students(cf_students):
    for student in cf_students:
        email = student
        name = cf_students[student]['name']
        surname = cf_students[student]['surname']
        group_id = cf_students[student]['group_id']

        student_data = (email, name, surname, group_id,)
        sql = """
                INSERT INTO student(email, name, surname, group_id)
                VALUES(%s, %s, %s, %s)
                RETURNING id;
              """
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            cursor.execute(sql, student_data)
            student_id = cursor.fetchone()[0]
            cursor.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

        enrolled_subjects = cf_students[student]['enrolled_subjects']
        insert_student_enrolled_subjects(student_id, group_id, enrolled_subjects)


def insert_student_enrolled_subjects(student_id, group_id, enrolled_subjects):
    degree_id = get_group_degree_id(group_id)

    if ',' in enrolled_subjects:
        for subject_code in enrolled_subjects.split(','):
            insert_subject_student_data(student_id, subject_code, degree_id)
    else:
        subject_code = enrolled_subjects
        insert_subject_student_data(student_id, subject_code, degree_id)


def insert_subject_student_data(student_id, subject_code, degree_id):
    subject_id = get_subject_id(subject_code, degree_id)

    subject_data = (subject_id, student_id,)

    sql = """
        INSERT INTO subject_student(subject_id, student_id)
        VALUES(%s, %s);
    """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(sql, subject_data)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def catch_exception(e):    
    print(str(e))    
    sys.exit()


def succeed():
    print('\033[92m' + 'OK' + '\033[0m')


if __name__ == '__main__':
    if os.path.exists(os.path.join(os.getcwd(), CF_FILE)):
        print("\u200a\u200aInserting Cicles Formatius students data...", end=" ")
        try:
            cf_students = read_cf_file(CF_FILE)
            insert_cf_students(cf_students)
            succeed()
        except Exception as e:
            catch_exception(e)

    if os.path.exists(os.path.join(os.getcwd(), ESOBTX_FILE)):
        print("\u200a\u200aInserting ESO-Batxillerat students data...", end=" ")
        try:
            esobatx_students = read_esobtx_file(ESOBTX_FILE)
            insert_esobtx_students(esobatx_students)
            succeed()
        except Exception as e:
            catch_exception(e)
    