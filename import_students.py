from config import config
import csv
import psycopg2
import os
import sys
from query_master import *


CF_FILE = 'input/cf_students.csv'
ESOBTX_FILE = 'input/eso-btx_students.csv'


def insert_esobtx_students_data(input_file):
    with open(input_file, 'r', encoding='utf-8') as students_file:
        students_reader = csv.DictReader(students_file)

        for student in students_reader:
            email = student['Adreça electrònica']
            name = student['Nom']
            surname = student['Cognoms']
            # ESO-Batxillerat students real classgroup is ignored, it's set to their level instead
            group_id = get_group_id(student['Nivell'])

            enrolled_subjects = 'Centre'

            insert_student(email, name, surname, group_id, enrolled_subjects)
            

def insert_cf_students_data(input_file):
    with open(input_file, 'r', encoding='utf-8') as students_file:
        students_reader = csv.DictReader(students_file)

        for student in students_reader:
            email = student['CORREU']
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

            insert_student(email, name, surname, group_id, enrolled_subjects)


def insert_student(email, name, surname, group_id, enrolled_subjects):
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

        insert_student_enrolled_subjects_data(student_id, group_id, enrolled_subjects)


def insert_student_enrolled_subjects_data(student_id, group_id, enrolled_subjects):
    degree_id = get_group_degree_id(group_id)

    # Students with multiple enrolled subjects to evaluate
    if ',' in enrolled_subjects:
        for subject_code in enrolled_subjects.split(','):
            insert_subject_student(student_id, subject_code, degree_id)
    # Students with onse single subject to evaluate
    else:
        subject_code = enrolled_subjects
        insert_subject_student(student_id, subject_code, degree_id)


def insert_subject_student(student_id, subject_code, degree_id):
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
            insert_cf_students_data(CF_FILE)
            succeed()
        except Exception as e:
            catch_exception(e)

    if os.path.exists(os.path.join(os.getcwd(), ESOBTX_FILE)):
        print("\u200a\u200aInserting ESO-Batxillerat students data...", end=" ")
        try:
            insert_esobtx_students_data(ESOBTX_FILE)
            succeed()
        except Exception as e:
            catch_exception(e)
    