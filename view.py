class View:
    def show_menu(self):
        print("\nMenu:")
        print("1. Add Data")
        print("2. View Data")
        print("3. Update Data")
        print("4. Delete Data")
        print("5. Generate Random Data")
        print("6. Search Data")
        print("7. Quit")
        return input("Enter your choice: ")

    def show_table_menu(self):
        print("\nSelect Table:")
        print("1. Car")
        print("2. Owner")
        print("3. Mechanic")
        print("4. ServiceRecord")
        print("5. ServiceMechanic")
        print("6. Back to Main Menu")
        return input("Enter your choice: ")

    def get_car_input(self):
        data = {}
        data['make'] = input("Enter car make: ")
        data['model'] = input("Enter car model: ")
        while True:
            try:
                data['year'] = int(input("Enter car year: "))
                break
            except ValueError:
                print("Invalid year. Please enter a valid year.")
        data['vin'] = input("Enter car VIN: ")
        ownerid = input("Enter owner ID: ")
        data['ownerid'] = int(ownerid) if ownerid else None
        return data

    def get_owner_input(self):
        data = {}
        data['firstname'] = input("Enter owner's first name: ")
        data['lastname'] = input("Enter owner's last name: ")
        data['phone'] = input("Enter owner's phone: ")
        data['email'] = input("Enter owner's email: ")
        return data

    def get_mechanic_input(self):
        data = {}
        data['name'] = input("Enter mechanic's name: ")
        data['specialty'] = input("Enter mechanic's specialty: ")
        data['phone'] = input("Enter mechanic's phone: ")
        return data

    def get_service_record_input(self):
        data = {}
        data['carid'] = int(input("Enter car ID: "))
        data['servicedate'] = input("Enter service date (YYYY-MM-DD): ")
        data['servicetype'] = input("Enter service type: ")
        data['servicecost'] = float(input("Enter service cost: "))
        return data

    def get_service_mechanic_input(self):
        data = {}
        data['serviceid'] = int(input("Enter service ID: "))
        data['mechanicid'] = int(input("Enter mechanic ID: "))
        data['hoursworked'] = float(input("Enter hours worked: "))
        return data

    def get_pk_input(self, table_name):
        if table_name == 'ServiceMechanic':
            serviceid = input("Enter Service ID: ")
            mechanicid = input("Enter Mechanic ID: ")
            return (int(serviceid), int(mechanicid))
        else:
            pk_value = input(f"Enter {table_name} ID: ")
            return int(pk_value)

    def show_records(self, records):
        if not records:
            print("No records found.")
            return
        for record in records:
            print(dict(record))

    def show_message(self, message):
        print(message)

    def get_update_data(self, table_name, record):
        data = {}
        for key in record.keys():
            if key == table_name.lower() + 'id':
                continue
            value = input(f"Enter new value for {key} (leave blank to keep current value '{record[key]}'): ")
            if value != '':
                data[key] = value
        return data

    def get_random_data_count(self):
        count = input("Enter the number of random records to generate: ")
        return int(count)

    def select_search_query(self):
        print("\nSelect Search Query:")
        print("1. Search Cars by Make and Year Range")
        print("2. Search Mechanics by Specialty and Name Pattern")
        print("3. Search Service Records by Date Range and Service Type")
        print("4. Back to Main Menu")
        return input("Enter your choice: ")

    def get_car_search_input(self):
        make = input("Enter car make (leave blank for any): ")
        year_from = input("Enter start year (leave blank for any): ")
        year_to = input("Enter end year (leave blank for any): ")
        return {'make': make, 'year_from': year_from, 'year_to': year_to}

    def get_mechanic_search_input(self):
        specialty = input("Enter mechanic specialty (leave blank for any): ")
        name_pattern = input("Enter mechanic name pattern (e.g., '%John%'): ")
        return {'specialty': specialty, 'name_pattern': name_pattern}

    def get_service_record_search_input(self):
        date_from = input("Enter start date (YYYY-MM-DD, leave blank for any): ")
        date_to = input("Enter end date (YYYY-MM-DD, leave blank for any): ")
        servicetype = input("Enter service type (leave blank for any): ")
        return {'date_from': date_from, 'date_to': date_to, 'servicetype': servicetype}
