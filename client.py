#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__licence__ = 'GPL'
__version__ = '0.0.1'
__author__ = 'Tony Schneider'
__email__ = 'tonysch05@gmail.com'

import sys
import json
import logging
from flask import Flask, request
from wrappers.db_wrapper import DBWrapper

"""
Please fill the MySQL credentials!
"""

MYSQL_IP = '127.0.0.1'
MYSQL_USER = 'root'
MYSQL_PASS = ''
MYSQL_SCHEMA = 'ref_exercise'


logging.basicConfig (level=logging.INFO, format='%(asctime)s | %(levelname)-10s | %(message)s', stream=sys.stdout)
app = Flask(__name__)

db_obj = DBWrapper(host=MYSQL_IP, mysql_user=MYSQL_USER, mysql_pass=MYSQL_PASS, database=MYSQL_SCHEMA)


def calculate_gpa(students_grades):
    """
    This method calculates
    """
    gpa = 0
    if students_grades:
        for grade in students_grades:
            gpa += grade['grade']

        return gpa / len(students_grades)


@app.route('/student', methods=['POST'])
def create_student():
    try:
        data = request.get_data().decode('utf8').replace('\'', '"')
        payload = json.loads(data)
    except Exception as e:
        logging.error(f"There was an issue with the provided data. Reason - '{e}'")
        return "Bad request. please check the sent data.", 400

    try:
        assert isinstance(payload, dict)
        assert 'name' in payload.keys()
        assert 'grades' not in payload.keys() or ('grades' in payload.keys() and isinstance(payload['grades'], list))
        assert isinstance(payload['name'], str)
        assert isinstance(payload['grades'], list)
        if 'grades' in payload.keys():
            for grade_details in payload['grades']:
                assert all(field in grade_details for field in ['course_name', 'grade'])
                assert isinstance(grade_details['course_name'], str)
                assert isinstance(grade_details['grade'], int)
    except AssertionError:
        return "Bad request. please check the sent data.", 400

    student_name = payload['name']
    students_grades = payload['grades']

    gpa = calculate_gpa(students_grades)

    # insert a new student
    insert_status = db_obj.insert_row(table_name='students', keys_values={'name': student_name, 'GPA': gpa})
    if not insert_status:
        logging.error(f"Didn't manage to insert a new student. | name - {student_name}.")
        return None

    student_id = db_obj.mysql_cursor.lastrowid

    for course_details in students_grades:
        db_obj.insert_row(table_name='grades',
                          keys_values={'student_id': student_id,
                                       'course_name': course_details['course_name'],
                                       'grade': course_details['grade']
                                       }
                          )

    if not student_id:
        return "The server has an error while trying create a new provided student. please check with the developers.", 500

    return f"Student has been added successfully. Student ID - '{student_id}'", 200


@app.route('/university', methods=['POST'])
def create_university():
    try:
        data = request.get_data().decode('utf8').replace('\'', '"')
        payload = json.loads(data)
    except Exception as e:
        logging.error(f"There was an issue with the provided data. Reason - '{e}'")
        return "Bad request. please check the sent data.", 400

    try:
        assert isinstance(payload, dict)
        assert all(field in payload.keys() for field in ['name', 'max_number_of_students', 'min_gpa'])
        assert isinstance(payload['name'], str)
        assert isinstance(payload['max_number_of_students'], int)
        assert isinstance(payload['min_gpa'], int)
    except AssertionError:
        return "Bad request. please check the sent data.", 400

    insert_status = db_obj.insert_row(table_name='universities',
                                      keys_values={'university_name': payload['name'],
                                                   'max_number_of_students': payload['max_number_of_students'],
                                                   'min_gpa': payload['min_gpa'],
                                                   'available_places': payload['max_number_of_students']
                                                   }
                                      )
    if not insert_status:
        logging.error(f"Didn't manage to insert a new university. | name - {payload['name']}.")
        return None

    university_id = db_obj.mysql_cursor.lastrowid

    if not university_id:
        return "The server has an error while trying create a new provided university. please check with the developers.", 500

    return f"Student has been added successfully. University ID - '{university_id}'", 200


@app.route('/enroll/<student_id>/<university_id>', methods=['POST'])
def enroll_student(student_id, university_id):
    # verify student id
    if int(student_id) not in db_obj.get_all_values_by_field(table_name='students', field='id'):
        return f"There are no student by the provided ID ('{student_id}')", 400

    # verify university id
    if int(university_id) not in db_obj.get_all_values_by_field(table_name='universities', field='id'):
        return f"There are no university by the provided ID ('{university_id}')", 400

    # check available place at the provided university
    if not db_obj.get_all_values_by_field(table_name='universities', field='available_places', condition_field='id', condition_value=university_id, first_item=True):
        return f"There are no available places for new students at university (id - '{university_id}').", 409  # conflict status_code

    # check student's GPA if it matches the university min GPA
    students_gpa = db_obj.get_all_values_by_field(table_name='students', field='GPA', condition_field='id', condition_value=student_id, first_item=True)
    university_min_gpa = db_obj.get_all_values_by_field(table_name='universities', field='min_gpa', condition_field='id', condition_value=university_id, first_item=True)
    if students_gpa < university_min_gpa:
        return f"The GPA of this student is not enough for the provided university. Student's GPA - '{students_gpa}' | University's min GPA - '{university_min_gpa}' ", 409  # conflict status_code

    # update student's university
    db_obj.update_field(table_name='students', field='university_id', value=university_id, condition_field='id', condition_value=student_id)

    # decrease available university places
    current_available_university_places = db_obj.get_all_values_by_field(table_name='universities', field='available_places', condition_field='id', condition_value=university_id, first_item=True)
    db_obj.update_field(table_name='universities', field='available_places', value=current_available_university_places - 1, condition_field='id', condition_value=university_id)

    return f"Student (id - '{student_id}') has been enrolled successfully to the provided university (id - '{university_id}'). ", 200


@app.route('/students/<university_id>', methods=['GET'])
def get_students(university_id):
    list_of_students = db_obj.get_all_values_by_field(table_name='students', condition_field='university_id', condition_value=university_id)

    return str(list_of_students), 200


@app.route('/university/<university_id>', methods=['GET'])
def get_university(university_id):

    return str(db_obj.get_all_values_by_field(table_name='universities', condition_field='id', condition_value=university_id)), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)


def main(*args, **kwargs) -> int:
    try:
        logging.info('Starting app... Press CTRL+C to quit.')
        app.run(host="0.0.0.0", port=80)
    except KeyboardInterrupt:
        logging.info('Quitting... (CTRL+C pressed)')
        return 0
    except Exception:   # Catch-all for unexpected exceptions, with stack trace
        logging.exception(f'Unhandled exception occurred!')
        return 1


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))