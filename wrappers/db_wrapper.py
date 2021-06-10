#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Tony Schneider'
__email__ = 'tonysch05@gmail.com'

import logging
from mysql.connector import Error as MySQLError
from mysql.connector import connect as MySQLConnection


class DBWrapper:
    def __init__(self, host: str, mysql_user: str, mysql_pass: str, database: str):
        """
        This class wraps all MySQL functionality.
        """
        self.host = host
        self.database = database
        self.mysql_user = mysql_user
        self.mysql_pass = mysql_pass
        self._config = self.set_config()
        self.mysql_connector = None
        self.mysql_cursor = None

    def set_config(self) -> dict:
        return {
          'user': self.mysql_user,
          'password': self.mysql_pass,
          'host': self.host,
          'database': self.database,
          'raise_on_warnings': True,
          'auth_plugin': 'mysql_native_password'
        }

    def create_connection(self) -> None:
        try:
            self.mysql_connector = MySQLConnection(**self._config)
        except MySQLError as e:
            logging.error(f"There was an issue with mysql connection - '{e}'")

    def commit(self):
        self.mysql_connector.commit()

    def close_connection(self) -> None:
        self.mysql_connector.close()

    def execute_command(self, command: str):
        output = True
        self.create_connection()
        try:
            self.mysql_cursor = self.mysql_connector.cursor(buffered=True, dictionary=True)
            self.mysql_cursor.execute(command)
            if 'SELECT' in command:
                output = self.mysql_cursor.fetchall()
                # if output and len(output) == 1:
                #     output = output[0]
            self.mysql_connector.commit()
            self.close_connection()
        except Exception as e:
            logging.error(f"There was an issue to execute {command}. Error - '{e}'")
            output = False
        return output

    def insert_row(self, table_name: str, keys_values: dict):
        fields = ",".join(keys_values.keys())
        values = ','.join([f'"{value}"' for value in keys_values.values()])
        add_row_command = f"INSERT INTO {table_name} ({fields}) VALUES({values})"

        return self.execute_command(add_row_command)

    def update_field(self, table_name: str, field: str, value, condition_field: str, condition_value):
        update_field_command = f"UPDATE {table_name} SET {field} = '{value}' WHERE {condition_field}='{condition_value}'"

        return self.execute_command(update_field_command)

    def remove_row_if_exists(self, table_name: str, field_condition: str, value_condition):
        remove_row_command = f"DELETE FROM {table_name} WHERE {field_condition}='{value_condition}'"

        return self.execute_command(remove_row_command)

    def get_all_values_by_field(self, table_name: str, field: str = None, condition_field=None, condition_value=None, first_item=False):
        get_all_values_by_field_command = f"SELECT {field if field else '*'} FROM {table_name}"

        if condition_field:
            get_all_values_by_field_command += f" WHERE {condition_field}='{condition_value}'"

        regular_dict_result = self.execute_command(get_all_values_by_field_command)
        if field:
            result_as_list = [item[field] for item in regular_dict_result]
            return result_as_list[0] if first_item else result_as_list

        return regular_dict_result

    def get_specific_field_value(self, table_name: str, field_to_get: str, field_condition: str, value_condition):
        get_specific_field_value_command = f"SELECT {field_to_get} FROM {table_name} WHERE {field_condition}='{value_condition}'"

        return self.execute_command(get_specific_field_value_command)
