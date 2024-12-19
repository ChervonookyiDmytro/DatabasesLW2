from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Float, Boolean, Table, Text
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from psycopg2 import sql

# Database connection
engine = create_engine('postgresql+psycopg2://cirelote:1601@localhost:5432/electronic-car-database')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class CRUDMixin:
    @classmethod
    def create(cls, data):
        obj = cls(**data)
        session.add(obj)
        try:
            session.commit()
            return getattr(obj, cls.pk) if isinstance(cls.pk, str) else 1
        except Exception as e:
            session.rollback()
            print(f"Error inserting into {cls.__tablename__}: {e}")
            return None

    @classmethod
    def read_all(cls):
        try:
            return session.query(cls).limit(100).all()
        except Exception as e:
            print(f"Error reading from {cls.__tablename__}: {e}")
            return None

    @classmethod
    def read_by_pk(cls, pk_value):
        try:
            if isinstance(cls.pk, (list, tuple)):
                # Composite primary key
                filter_conditions = [getattr(cls, pk_col) == pk_val for pk_col, pk_val in zip(cls.pk, pk_value)]
                return session.query(cls).filter(*filter_conditions).first()
            else:
                return session.query(cls).filter(getattr(cls, cls.pk) == pk_value).first()
        except Exception as e:
            print(f"Error reading from {cls.__tablename__}: {e}")
            return None

    @classmethod
    def update(cls, pk_value, data):
        try:
            if isinstance(cls.pk, (list, tuple)):
                filter_conditions = [getattr(cls, pk_col) == pk_val for pk_col, pk_val in zip(cls.pk, pk_value)]
                obj = session.query(cls).filter(*filter_conditions).first()
            else:
                obj = session.query(cls).filter(getattr(cls, cls.pk) == pk_value).first()
            if not obj:
                return None
            for key, value in data.items():
                setattr(obj, key, value)
            session.commit()
            return 1  # Indicate success
        except Exception as e:
            session.rollback()
            print(f"Error updating {cls.__tablename__}: {e}")
            return None

    @classmethod
    def delete(cls, pk_value):
        try:
            if isinstance(cls.pk, (list, tuple)):
                filter_conditions = [getattr(cls, pk_col) == pk_val for pk_col, pk_val in zip(cls.pk, pk_value)]
                obj = session.query(cls).filter(*filter_conditions).first()
            else:
                obj = session.query(cls).filter(getattr(cls, cls.pk) == pk_value).first()
            if not obj:
                return None
            session.delete(obj)
            session.commit()
            return 1  # Indicate success
        except Exception as e:
            session.rollback()
            print(f"Error deleting from {cls.__tablename__}: {e}")
            return None

    def validate_data(self, data):
        return True, None
    
    @classmethod
    def generate_data(self, num_rows):
        """
        Generate random data for the table using SQL functions,
        handling various data types and foreign keys.
        """
        try:
            cursor = self.conn.cursor()

            # Get the list of columns excluding the primary key
            columns = self.columns.copy()
            if self.pk in columns:
                columns.remove(self.pk)

            # Retrieve data types for each column
            data_types = []
            for column in columns:
                cursor.execute("""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_name=%s AND column_name=%s
                """, [self.table_name, column])
                data_type = cursor.fetchone()[0]
                data_types.append(data_type)

            # Identify foreign key relationships
            foreign_keys = {}
            for column in columns:
                cursor.execute("""
                    SELECT 
                        tc.constraint_name, 
                        kcu.column_name AS fk_column, 
                        ccu.table_name AS foreign_table, 
                        ccu.column_name AS foreign_column
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name
                    WHERE 
                        tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_name = %s 
                        AND kcu.column_name = %s
                """, [self.table_name, column])
                fk_info = cursor.fetchone()
                if fk_info:
                    foreign_keys[column] = {
                        'foreign_table': fk_info[2],
                        'foreign_column': fk_info[3]
                    }

            # Generate and execute INSERT statements for the specified number of rows
            for _ in range(num_rows):
                # Build value expressions for each column
                value_expressions = []
                for column, data_type in zip(columns, data_types):
                    if column in foreign_keys:
                        # For foreign keys, select a random existing value from the foreign table
                        fk_table = foreign_keys[column]['foreign_table']
                        fk_column = foreign_keys[column]['foreign_column']
                        value_expr = sql.SQL("(SELECT {fk_column} FROM {fk_table} ORDER BY RANDOM() LIMIT 1)").format(
                            fk_column=sql.Identifier(fk_column),
                            fk_table=sql.Identifier(fk_table)
                        )
                    else:
                        # Generate random data based on data type using SQL functions
                        if data_type == 'integer':
                            value_expr = sql.SQL('TRUNC(RANDOM() * 1000)::INTEGER')
                        elif data_type == 'character varying':
                            value_expr = sql.SQL("LEFT(MD5(RANDOM()::TEXT), 10)")
                        elif data_type == 'text':
                            value_expr = sql.SQL("LEFT(MD5(RANDOM()::TEXT), 20)")
                        elif data_type == 'date':
                            value_expr = sql.SQL("DATE '2024-01-01' + (RANDOM() * 365)::INT")
                        elif data_type == 'boolean':
                            value_expr = sql.SQL("(RANDOM() < 0.5)")
                        elif data_type in ('double precision', 'numeric'):
                            value_expr = sql.SQL("(RANDOM() * 1000)")
                        elif data_type.startswith('timestamp'):
                            value_expr = sql.SQL("TIMESTAMP '2024-01-01 00:00:00' + (RANDOM() * INTERVAL '365 days')")
                        else:
                            value_expr = sql.SQL('NULL')
                    value_expressions.append(value_expr)

                # Build the INSERT query using psycopg2.sql to safely include identifiers
                insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                    table=sql.Identifier(self.table_name),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join(value_expressions)
                )
                # Execute the INSERT query
                cursor.execute(insert_query)

            # Commit the transaction
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error generating data for {self.table_name}: {e}")
            self.conn.rollback()
            return False


class Owner(Base, CRUDMixin):
    __tablename__ = 'owner'
    pk = 'ownerid'

    ownerid = Column(Integer, primary_key=True)
    firstname = Column(String)
    lastname = Column(String)
    phone = Column(String)
    email = Column(String)

    cars = relationship('Car', back_populates='owner')

class Car(Base, CRUDMixin):
    __tablename__ = 'car'
    pk = 'carid'

    carid = Column(Integer, primary_key=True)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    vin = Column(String)
    ownerid = Column(Integer, ForeignKey('owner.ownerid'))

    owner = relationship('Owner', back_populates='cars')
    service_records = relationship('ServiceRecord', back_populates='car')

    def validate_data(self, data):
        errors = []
        if 'year' in data:
            try:
                data['year'] = int(data['year'])
            except ValueError:
                errors.append('Year must be an integer.')
        if 'ownerid' in data and data['ownerid'] is not None:
            owner = Owner.read_by_pk(data['ownerid'])
            if not owner:
                errors.append(f"Owner with ID {data['ownerid']} does not exist.")
        return len(errors) == 0, errors

class Mechanic(Base, CRUDMixin):
    __tablename__ = 'mechanic'
    pk = 'mechanicid'

    mechanicid = Column(Integer, primary_key=True)
    name = Column(String)
    specialty = Column(String)
    phone = Column(String)

    service_mechanics = relationship('ServiceMechanic', back_populates='mechanic')

class ServiceRecord(Base, CRUDMixin):
    __tablename__ = 'servicerecord'
    pk = 'serviceid'

    serviceid = Column(Integer, primary_key=True)
    carid = Column(Integer, ForeignKey('car.carid'))
    servicedate = Column(Date)
    servicetype = Column(String)
    servicecost = Column(Float)

    car = relationship('Car', back_populates='service_records')
    service_mechanics = relationship('ServiceMechanic', back_populates='service_record')

    def validate_data(self, data):
        errors = []
        if 'carid' in data:
            car = Car.read_by_pk(data['carid'])
            if not car:
                errors.append(f"Car with ID {data['carid']} does not exist.")
        return len(errors) == 0, errors

class ServiceMechanic(Base, CRUDMixin):
    __tablename__ = 'servicemechanic'
    pk = ['serviceid', 'mechanicid']

    serviceid = Column(Integer, ForeignKey('servicerecord.serviceid'), primary_key=True)
    mechanicid = Column(Integer, ForeignKey('mechanic.mechanicid'), primary_key=True)
    hoursworked = Column(Float)

    service_record = relationship('ServiceRecord', back_populates='service_mechanics')
    mechanic = relationship('Mechanic', back_populates='service_mechanics')

    def validate_data(self, data):
        errors = []
        if 'serviceid' in data:
            service = ServiceRecord.read_by_pk(data['serviceid'])
            if not service:
                errors.append(f"ServiceRecord with ID {data['serviceid']} does not exist.")
        if 'mechanicid' in data:
            mechanic = Mechanic.read_by_pk(data['mechanicid'])
            if not mechanic:
                errors.append(f"Mechanic with ID {data['mechanicid']} does not exist.")
        return len(errors) == 0, errors
