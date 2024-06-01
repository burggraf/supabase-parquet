import duckdb
from faker import Faker
from duckdb.typing import *

# Initialize Faker
fake = Faker()

# Define the function to generate a person's details with a seed parameter
def generate_person(seed):
    fake.seed_instance(seed)
    return {
        'name': fake.name(),
        'city': fake.city(),
        'state': fake.state(),
        'zip_code': fake.zipcode(),
        'country': 'USA', # fake.country(),
        'email': fake.email(),
        'job': fake.job(),
        'company': fake.company(),
        'ssn': fake.ssn(),
        'birthdate': fake.date_of_birth(),
        'phone_number': fake.phone_number()
    }

# Connect to DuckDB
con = duckdb.connect('people.db')

# Register the function as a UDF
con.create_function(
    'generate_person',
    generate_person,
    [DOUBLE],  # Seed parameter type
    duckdb.struct_type({
        'name': 'VARCHAR',
        'city': 'VARCHAR',
        'state': 'VARCHAR',
        'zip_code': 'VARCHAR',
        'country': 'VARCHAR',
        'email': 'VARCHAR',
        'job': 'VARCHAR',
        'company': 'VARCHAR',
        'ssn': 'VARCHAR',
        'birthdate': 'DATE',
        'phone_number': 'VARCHAR'
    })
)

# Use the UDF in a SQL query
#CREATE OR REPLACE TABLE people AS
con.sql("""
INSERT INTO people
SELECT person.* FROM (
    SELECT generate_person(random()) AS person
    FROM generate_series(1, 100000)
)
""")