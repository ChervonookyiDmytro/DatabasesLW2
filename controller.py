import time
import psycopg2


from model import Car, Owner, Mechanic, ServiceRecord, ServiceMechanic
from view import View


class Controller:
    def __init__(self):
        self.view = View()
        self.models = {
            '1': Car(),
            '2': Owner(),
            '3': Mechanic(),
            '4': ServiceRecord(),
            '5': ServiceMechanic(),
        }
        self.model_names = {
            '1': 'Car',
            '2': 'Owner',
            '3': 'Mechanic',                                                                                                                                                                                                                                                                                
            '4': 'ServiceRecord',
            '5': 'ServiceMechanic',
        }

    def run(self):
        while True:
            choice = self.view.show_menu()
            if choice == '1':
                self.add_data()
            elif choice == '2':
                self.view_data()
            elif choice == '3':
                self.update_data()
            elif choice == '4':
                self.delete_data()
            elif choice == '5':
                self.generate_random_data()
            elif choice == '6':
                self.search_data()
            elif choice == '7':
                break
            else:
                self.view.show_message("Invalid choice.")

    def add_data(self):
        while True:
            choice = self.view.show_table_menu()
            if choice in self.models:
                model = self.models[choice]
                table_name = self.model_names[choice]
                data = {}
                if choice == '1':
                    data = self.view.get_car_input()
                elif choice == '2':
                    data = self.view.get_owner_input()
                elif choice == '3':
                    data = self.view.get_mechanic_input()
                elif choice == '4':
                    data = self.view.get_service_record_input()
                elif choice == '5':
                    data = self.view.get_service_mechanic_input()
                valid, errors = model.validate_data(data)
                if not valid:
                    for error in errors:
                        self.view.show_message(error)
                    continue
                result = model.create(data)
                if result:
                    self.view.show_message(f"{table_name} added successfully.")
                else:
                    self.view.show_message(f"Failed to add {table_name}.")
            elif choice == '6':
                break
            else:
                self.view.show_message("Invalid choice.")

    def view_data(self):
        while True:
            choice = self.view.show_table_menu()
            if choice in self.models:
                model = self.models[choice]
                records = model.read_all()
                self.view.show_records(records)
            elif choice == '6':
                break
            else:
                self.view.show_message("Invalid choice.")

    def update_data(self):
        while True:
            choice = self.view.show_table_menu()
            if choice in self.models:
                model = self.models[choice]
                table_name = self.model_names[choice]
                pk_value = self.view.get_pk_input(table_name)
                record = model.read_by_pk(pk_value)
                if not record:
                    self.view.show_message(f"{table_name} not found.")
                    continue
                data = self.view.get_update_data(table_name, record)
                if not data:
                    self.view.show_message("No updates provided.")
                    continue
                valid, errors = model.validate_data(data)
                if not valid:
                    for error in errors:
                        self.view.show_message(error)
                    continue
                result = model.update(pk_value, data)
                if result:
                    self.view.show_message(f"{table_name} updated successfully.")
                else:
                    self.view.show_message(f"Failed to update {table_name}.")
            elif choice == '6':
                break
            else:
                self.view.show_message("Invalid choice.")

    def delete_data(self):
        while True:
            choice = self.view.show_table_menu()
            if choice in self.models:
                model = self.models[choice]
                table_name = self.model_names[choice]
                pk_value = self.view.get_pk_input(table_name)
                if table_name == 'Owner':
                    car_model = Car()
                    cars = car_model.read_all()
                    owner_cars = [car for car in cars if car['ownerid'] == pk_value]
                    if owner_cars:
                        self.view.show_message("Cannot delete Owner with associated Cars.")
                        continue
                result = model.delete(pk_value)
                if result:
                    self.view.show_message(f"{table_name} deleted successfully.")
                else:
                    self.view.show_message(f"Failed to delete {table_name}.")
            elif choice == '6':
                break
            else:
                self.view.show_message("Invalid choice.")

    def generate_random_data(self):
        while True:
            choice = self.view.show_table_menu()
            if choice in self.models:
                model = self.models[choice]
                table_name = self.model_names[choice]
                num_rows = self.view.get_random_data_count()
                start_time = time.time()
                success = model.generate_data(num_rows)
                end_time = time.time()
                if success:
                    self.view.show_message(
                        f"Generated {num_rows} random records for {table_name} in {(end_time - start_time)*1000:.2f} ms."
                    )
                else:
                    self.view.show_message(f"Failed to generate random data for {table_name}.")
            elif choice == '6':
                break
            else:
                self.view.show_message("Invalid choice.")
                
    def search_data(self):
        while True:
            choice = self.view.select_search_query()
            if choice == '1':
                criteria = self.view.get_car_search_input()
                car_model = Car()
                query = "SELECT * FROM car WHERE 1=1"
                params = []
                if criteria['make']:
                    query += " AND make = %s"
                    params.append(criteria['make'])
                if criteria['year_from']:
                    query += " AND year >= %s"
                    params.append(int(criteria['year_from']))
                if criteria['year_to']:
                    query += " AND year <= %s"
                    params.append(int(criteria['year_to']))
                start_time = time.time()
                try:
                    with car_model.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                        cursor.execute(query, params)
                        records = cursor.fetchall()
                        end_time = time.time()
                        self.view.show_records(records)
                        self.view.show_message(f"Query executed in {(end_time - start_time)*1000:.2f} ms.")
                except Exception as e:
                    self.view.show_message(f"Error executing search: {e}")

            elif choice == '2':
                criteria = self.view.get_mechanic_search_input()
                mechanic_model = Mechanic()
                query = "SELECT * FROM mechanic WHERE 1=1"
                params = []
                if criteria['specialty']:
                    query += " AND specialty = %s"
                    params.append(criteria['specialty'])
                if criteria['name_pattern']:
                    query += " AND name LIKE %s"
                    params.append(criteria['name_pattern'])
                start_time = time.time()
                try:
                    with mechanic_model.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                        cursor.execute(query, params)
                        records = cursor.fetchall()
                        end_time = time.time()
                        self.view.show_records(records)
                        self.view.show_message(f"Query executed in {(end_time - start_time)*1000:.2f} ms.")
                except Exception as e:
                    self.view.show_message(f"Error executing search: {e}")

            elif choice == '3':
                criteria = self.view.get_service_record_search_input()
                service_model = ServiceRecord()
                query = "SELECT * FROM servicerecord WHERE 1=1"
                params = []
                if criteria['date_from']:
                    query += " AND servicedate >= %s"
                    params.append(criteria['date_from'])
                if criteria['date_to']:
                    query += " AND servicedate <= %s"
                    params.append(criteria['date_to'])
                if criteria['servicetype']:
                    query += " AND servicetype = %s"
                    params.append(criteria['servicetype'])
                start_time = time.time()
                try:
                    with service_model.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                        cursor.execute(query, params)
                        records = cursor.fetchall()
                        end_time = time.time()
                        self.view.show_records(records)
                        self.view.show_message(f"Query executed in {(end_time - start_time)*1000:.2f} ms.")
                except Exception as e:
                    self.view.show_message(f"Error executing search: {e}")

            elif choice == '4':
                break
            else:
                self.view.show_message("Invalid choice.")
