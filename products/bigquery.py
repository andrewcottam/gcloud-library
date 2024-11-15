"""
Wrapper class for working with Google BiqQuery that includes functions for creating, deleting, importing and exporting BigQuery tables
Prerequisites:
Packages:
    psycopg2-binary
""" 
import fiona, math, psutil, os, tempfile, subprocess, json, psycopg2, time, sys
from itertools import islice
from datetime import datetime
from enum import Enum
from google.cloud import bigquery
from google.cloud import logging
from google.cloud.exceptions import NotFound
from shapely import to_geojson
from shapely.geometry import shape
from shapely.wkt import dumps

## Constants
BQ_LIMIT_ROW_SIZE = 104857600 # length of bytes which == 100Mb - see Maximum Row Size here https://cloud.google.com/bigquery/quotas
BQ_QUOTA_JOBS_PER_TABLE_PER_DAY = 1500
DATASET_NAME_PUBLIC = 'public'
DATASET_NAME_RED_LIST = 'rl'
DATASET_NAME_SITES = 'sites'
DATASET_NAME_WDPA = 'wdpa'
FIELD_NAME_GEOMETRY = 'geometry'
LOG_NAME = 'bigquery-importer'
TABLE_NAME_LOAD_FAILURES = 'load_failures'
TABLE_NAME_LOAD_JOBS = 'load_jobs'
TABLE_NAME_SITES = 'sites'

# set up logging - by default this package logs to Google Cloud Platform using the bigquery-importer log name
logging_client = logging.Client()
logger = logging_client.logger(LOG_NAME)

def compare_dicts(dict1, dict2):
    """Returns the differences between each of the dictionaries

    Args:
        dict1 (dict): The first dictionary
        dict2 (dict): The second dictionary

    Returns:
        dict: The unique keys in each dict and the differing values
    """
    # Get the keys unique to each dictionary
    unique_to_dict1 = {k: dict1[k] for k in dict1 if k not in dict2}
    unique_to_dict2 = {k: dict2[k] for k in dict2 if k not in dict1}
    # Get the differing values
    differing_values = {k: (dict1[k], dict2[k]) for k in dict1 if k in dict2 and dict1[k] != dict2[k]}
    # Return the results
    return {
        "unique_to_dict1": unique_to_dict1,
        "unique_to_dict2": unique_to_dict2,
        "differing_values": differing_values
    }

def compare_lists(list1: list, list2: list):
    """Compares the items in both lists returning their differences and similarities.

    Args:
        list1 (list): The first list
        list2 (list): The second list

    Returns:
        _type_: The items that are in list1, list2 and both.
    """
    # Find items in list1 that are not in list2
    unique_to_list1 = list(set(list1) - set(list2))
    # Find items in list2 that are not in list1
    unique_to_list2 = list(set(list2) - set(list1))
    # Find items that are in both
    in_both = list(set(list1) & set(list2))
    return {
        "unique_to_list1": unique_to_list1,
        "unique_to_list2": unique_to_list2,
        "in_both": in_both
    }

def format_duration(start, end):
    """Formats the datetime duration as a string

    Args:
        start (datetime): The start time as a datetime object
        end (datetime): The end  time as a datetime object

    Returns:
        str: The formatted duration, e.g. 4 hours 34 minutes 2 seconds
    """
    # Calculate the difference
    delta = end - start
    # Extract days, seconds, minutes, and hours
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    # Build the formatted string
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")
    if seconds > 0:
        parts.append(f"{seconds} second{'s' if seconds > 1 else ''}")
    return ", ".join(parts) if parts else "0 seconds"

def get_memory_usage():
    """ Prints out the current memory usage and free memory
    """
    # Get the current process id
    process = psutil.Process(os.getpid())
    # Print memory usage in bytes, convert to MB for readability
    memory_usage = process.memory_info().rss / (1024 * 1024)
    print(f"Current memory usage: {memory_usage:.2f} MB")
    memory_info = psutil.virtual_memory()
    free_memory = memory_info.free / (1024 * 1024)
    print(f"Free memory: {free_memory:.2f} MB")

def log(msg: str, severity: str='DEBUG'):
    """Logs a message to the console and Google Cloud Logging

    Args:
        msg (str): The message to log
        severity (str, optional): The severity level. One of INFO, WARNING, DEBUG or ERROR. Defaults to 'DEBUG'.
    """
    print(msg)
    logger.log_text(msg, severity=severity)

class BigQueryErrorType(Enum):
    ROW_EXCEEDS_SIZE_LIMIT = f"Row is likely to exceed the size limit of {BQ_LIMIT_ROW_SIZE} - see https://cloud.google.com/bigquery/quotas#load_jobs"
    SCHEMAS_DONT_MATCH = "Input feature and table schemas do not match"
    GEOMETRY_TYPES_DONT_MATCH = "Input feature and table geometry types do not match"

class BigQueryException(Exception):
    """Custom class for exceptions raised by the BigQuery API"""
    pass

class BigQuery():
    """Python Class that wraps much of the functionality of Google BigQuery to create, delete, import and export to BigQuery tables
    """
    def __init__(self):
        """Instantiates the Google Cloud Platform BigQuery client and saves a reference to it in self.client. The project name can be retrieved from self.client.project, e.g. restor-gis
        """
        # Define your BigQuery client - the project can be retrieved from self.client.project
        self.client = bigquery.Client()
        # get the load_jobs table_id
        self.load_jobs_table_id = self.get_load_jobs_table_id()
        # get the load_failures table_id
        self.load_failures_table_id = self.get_load_failures_table_id()
        # See if the load_jobs table exists
        if (not self.table_exists(self.load_jobs_table_id)):
            # If not, create the table
            self.create_load_jobs_table()
        # See if the load_failures table exists
        if (not self.table_exists(self.load_failures_table_id)):
            # If not, create the table
            self.create_load_failures_table()

    def add_fields(self, fields: list, table_id: str):
        """Adds fields to an existing table

        Args:
            fields (list): The fields to add as a list of bigquery.SchemaField objects
            table_id (str): The fully qualified name of the table to add the fields to, e.g. restor-gis.rl.birds
        """
        # Get the table schema and append new columns
        table = self.client.get_table(table_id)
        original_schema = table.schema
        # Create a copy of the existing schema
        updated_schema = original_schema[:]
        # Add new columns to the schema
        updated_schema.extend(fields)
        # Update the table with the new schema
        table.schema = updated_schema
        # API request
        table = self.client.update_table(table, ["schema"])
        # Confirm the update
        log(f"Updated table '{table_id}' with new columns.")

    def create_dataset(self, dataset_id: str):
        """Creates a dataset

        Args:
            dataset_id (str): The full dataset id, e.g. restor-gis.rl
        """
        # Add the project id if necessary
        dataset_id = self.get_name_with_project_id(dataset_id)
        # Create a Dataset object
        dataset = bigquery.Dataset(dataset_id)
        # Create the dataset
        dataset = self.client.create_dataset(dataset)

    def create_load_failures_table(self):
        """Creates the load_failures table
        """
        # Get the load_jobs table_id
        table_id = self.get_load_failures_table_id()
        # Create the schema
        bq_schema = []
        # Add the fields
        bq_schema.append(bigquery.SchemaField('source_path', 'STRING'))
        bq_schema.append(bigquery.SchemaField('layer_name', 'STRING'))
        bq_schema.append(bigquery.SchemaField('table_id', 'STRING'))
        bq_schema.append(bigquery.SchemaField('row', 'INT64', description='Zero-based index of the row'))
        bq_schema.append(bigquery.SchemaField('props', 'JSON', description='Properties of the feature'))
        bq_schema.append(bigquery.SchemaField('fail_time', 'DATETIME'))
        bq_schema.append(bigquery.SchemaField('fail_reason', 'STRING'))
        # Create the table
        self.create_public_table(table_id, bq_schema)

    def create_load_jobs_table(self):
        """Creates the load_jobs table
        """
        # Get the load_jobs table_id
        table_id = self.get_load_jobs_table_id()
        # Create the schema
        bq_schema = []
        # Add the fields
        bq_schema.append(bigquery.SchemaField('source_path', 'STRING'))
        bq_schema.append(bigquery.SchemaField('layer_name', 'STRING'))
        bq_schema.append(bigquery.SchemaField('table_id', 'STRING'))
        bq_schema.append(bigquery.SchemaField('input_feature_count', 'INT64'))
        bq_schema.append(bigquery.SchemaField('job_size', 'INT64'))
        bq_schema.append(bigquery.SchemaField('job_count', 'INT64'))
        bq_schema.append(bigquery.SchemaField('start_at', 'INT64'))
        bq_schema.append(bigquery.SchemaField('validate_feature', 'BOOL'))
        bq_schema.append(bigquery.SchemaField('invalid_feature_count', 'INT64'))
        bq_schema.append(bigquery.SchemaField('inserted_features', 'INT64'))
        bq_schema.append(bigquery.SchemaField('table_row_count', 'INT64'))
        bq_schema.append(bigquery.SchemaField('start_time', 'DATETIME'))
        bq_schema.append(bigquery.SchemaField('end_time', 'DATETIME'))
        bq_schema.append(bigquery.SchemaField('duration', 'STRING'))
        bq_schema.append(bigquery.SchemaField('status', 'STRING'))
        # Create the table
        self.create_public_table(table_id, bq_schema)

    def create_public_table(self, table_id, schema: list) -> bool:
        """Creates a table in the public dataset

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds
            schema (list): A list of SchemaField definitions, e.g. [SchemaField('sisid','INT64'),SchemaField('sci_name','STRING'),..]

        Returns:
            bool: Returns True if the table was succesfully created
        """
        # Create the dataset if it doesn't already exist
        if (not self.dataset_exists(DATASET_NAME_PUBLIC)):
            self.create_dataset(DATASET_NAME_PUBLIC)
        # Create the table
        self.create_table(table_id, schema)

    def create_table(self, table_id: str, schema: list, description: str='', exp_back_off: bool=True)-> bool:
        """Creates a BigQuery table using the passed table_id and schema

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds
            schema (list): A list of SchemaField definitions, e.g. [SchemaField('sisid','INT64'),SchemaField('sci_name','STRING'),..]
            description (str, optional): A description for the table. Default value is ''.
            exp_back_off (bool, optional): Set to True to exponentially back off until the table has been created and the metadata have been updated. Set to False meaning that the table may not immediately be found.
        Returns:
            bool: Returns True if the table was succesfully created
        """
        # Create a table object
        table = bigquery.Table(table_id, schema=schema)
        # Add the description
        table.description = description
        # Create the table in BigQuery
        try:
            table = self.client.create_table(table)  # Make an API request
            if (exp_back_off):
                # Keep waiting 1 seconds until the table is available
                while not self.table_exists(table_id):
                    log(f"Table '{table_id}' not yet available", "DEBUG")
                    time.sleep(1)
            log(f"Table '{table_id}' created successfully", "INFO")
            return True
        except Exception as e:
            log(f"An error occurred creating the table {table_id}: {e}", "ERROR")
            return False

    def dataset_exists(self, dataset_id: str) -> bool:
        """Returns True if the dataset exists, False otherwise.add()

        Args:
            dataset_id (str): The dataset id, e.g. restor-gis.rl

        Returns:
            bool: Returns True if the dataset exists
        """
        # Check if the dataset exists
        try:
            self.client.get_dataset(dataset_id)  # Make an API request
            return True
        except NotFound:
            return False

    def delete_table(self, table_id: str) -> bool:
        """Deletes a table in BigQuery if it exists

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds

        Returns:
            bool: Returns True if the table was succesfully deleted
        """
        # Attempt to delete the table
        try:
            self.client.delete_table(table_id)  # Make an API request
            log(f"Table '{table_id}' deleted successfully.", "INFO")
            return True
        except NotFound:
            log(f"Table '{table_id}' does not exist.", "INFO")
            return False
        except Exception as e:
            log(f"An error occurred deleting table {table_id}: {e}", "ERROR")
            return False

    def export_to_geojson(self, src: fiona.Collection, features: list, output_geojson_path: str):
        """Exports the features to a new-line delimited GeoJSON file using GDAL. This format can be used to import data into BigQuery.

        Args:
            src (fiona.Collection): The source feature collection as a fiona.Collection
            features (list): A list of fiona.Feature features to export to GeoJSON
            output_geojson_path (str): The GeoJSON filename to export the features to
        """
        # log(f"Exporting features to: {output_geojson_path}")
        # output_geojson_path = 'tmp.geojson' # debugging only
        # Set the geometry type as Unknown so we can write Polygons and Multipolygons in the same file
        src.schema['geometry'] = "Unknown"
        # Open the output GeoJSON file
        with fiona.open(output_geojson_path, mode='w', driver='GeoJSONSeq', crs=src.crs, schema=src.schema) as dst:
            # Write the features to the GeoJSON file
            for feature in features:
                # Write the feature to the geojson file
                dst.write(feature)

    def geometry_field_exists(self, schema: dict) -> bool:
        """Returns True if the schema has a geometry field

        Args:
            schema (dict): The schema as a dict (can be a fiona.Collection schema or a simple dict of fieldname:fieldtype

        Returns:
            bool: True if the schema has a geometry field.
        """
        # See if the dict has two keys: properties and geometry. If it does it is a Fiona schema and we can see if the geometry is set by checking the value of the geometry key
        if ('properties') in list(schema.keys()) and 'geometry' in list(schema.keys()):
            return (schema[FIELD_NAME_GEOMETRY] != 'None') # A fiona schema
        else:
            # See if any of the fields are of type 'geometry'
            if any(value == 'user defined type' for value in schema.values()):
                return True
            else:
                return False

    def get_bq_field(self, field_name: str, field_type: str) -> bigquery.SchemaField:
        """Returns the corresponding BigQuery field from the passed Fiona or PostGIS field

        Args:
            field_name (str): The name of the field, e.g. category
            field_type (str): The fiona or PostGIS field type, e.g. int:32

        Returns:
            SchemaField: The corresponding BigQuery schema field
        """
        # Define a mapping from fiona and postgis types to BigQuery types
        type_mapping = {
            "double": "FLOAT64",
            "bool": "BOOL",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "geometry": "GEOGRAPHY",
            "blob": "BYTES",
            "character varying": "STRING", # postgis specific type
            "text": "STRING", # postgis specific type
            "boolean": "BOOL", # postgis specific type
            "timestamp with time zone": "TIMESTAMP", # postgis specific type
            "jsonb": "JSON", # postgis specific type
            "numeric": "FLOAT64", # postgis specific type
            "_varchar": "STRING", # postgis udt_type specific type
        }
        list_data_type = False
        # PostGIS ARRAY columns
        if field_type.startswith('ARRAY['): # e.g. 'ARRAY[str]'
            # Set the data type as a list data type
            list_data_type = True
            # Get the actual data type for the list, e.g. str
            field_type = field_type[6:][:-1]
        # List fields
        if field_type.startswith('List['): # e.g. 'List[str]
            # Currently Fionas can't write ARRAY data types - so for now skip them
            log(f"Skipping field '{field_name}' as it has an ARRAY data type which is currently not supported in Fiona", "WARNING")
        # Map the field type using the mapping dictionary
        bq_type = type_mapping.get(field_type.lower(), None)
        # For variable length string data types have a manual override
        bq_type = 'INT64' if field_type.lower().startswith('int') else bq_type
        bq_type = 'STRING' if field_type.lower().startswith('str') else bq_type
        bq_type = 'FLOAT64' if field_type.lower().startswith('float') else bq_type
        # Set the list data type - this only applies to PostGIS sources as Fionas can't write ARRAY data types
        if list_data_type:
            # BigQuery Array field
            field = bigquery.SchemaField(field_name, bq_type, mode="REPEATED")
        else:
            # Normal BigQuery field
            field = bigquery.SchemaField(field_name, bq_type)
        return field

    def get_bq_schema(self, source_schema: dict, geometry_field: str = 'geometry')->list:
        """Gets the BigQuery schema from an input source schema based on a data type mapping

        Args:
            source_schema (dict): The source schema to use to define the BigQuery schema. This could be read from a fiona.Collection schema or a simple dict of {fieldname: fieldtype,..}
            geometry_field (str, optional): The name of the geometry field to create in the BigQuery schema, e.g. which will contain a GEOGRAPHY data type. Defaults to 'geometry'.

        Returns:
            list: The BigQuery schema as a list of SchemaField definitions, e.g. [SchemaField('sisid','INT64'),SchemaField('sci_name','STRING'),..]
        """
        # Prepare the BigQuery schema
        bq_schema = []
        for field_name, field_type in source_schema['properties'].items():
            # Get the corresponding BigQuery field
            field = self.get_bq_field(field_name, field_type)
            if field:
                bq_schema.append(field)
            else:
                log(f"No match for source data type {field_type} in field {field}", 'WARNING')
        # Add the geometry field if one is present in the source schema
        if self.geometry_field_exists(source_schema):
            bq_schema.append(bigquery.SchemaField(geometry_field, 'GEOGRAPHY'))
        # Print the resulting BigQuery schema
        log(f"\nUsing schema:")
        for field in bq_schema:
            log(f"Field: {field.name}, Type: {field.field_type}")
        # Return the schema
        return bq_schema

    def get_feature_prop(self, feature: fiona.Feature, property_name:str)->any:
        """Returns a property from a feature

        Args:
            feature (fiona.Feature): The fiona Feature to get the property from 
            property_name (str): The name of the property to retrieve

        Returns:
            any: The property value
        """
        # Get the feature properties as a python dictionary
        props = self.get_feature_props(feature)
        # Get the property value
        return props.get(property_name, None)

    def get_feature_props(self, feature: fiona.Feature)->dict:
        """Returns the properties of a feature

        Args:
            feature (fiona.Feature): The fiona Feature to get the properties from 

        Returns:
            dict: The properties as a Python dictionary
        """
        # Get the feature properties as a python dictionary
        props = dict(feature['properties'])
        # Get the property value
        return props

    def get_field_names(self, table_id: str)->list:
        """Returns the field names for the table

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds

        Returns:
            list: The field names as a list
        """
        # Get the table
        table = self.client.get_table(table_id)
        # Get the field names
        field_names = [field.name for field in table.schema]
        # Return a value
        return field_names

    def get_fields_in_common(self, table_ids: list)-> list:
        """Gets the field names that are shared between all of the tables in the table_ids list

        Args:
            table_ids (list): The list of table_ids, e.g. ['restor-gis.rl.birds','restor-gis.rl.mammals', ..]

        Returns:
            list: The list of fields in common, e.g. ['tax_comm', 'dist_comm', 'source', 'citation', 'compiler', 'origin', 'sci_name', 'presence', 'generalisd', 'yrcompiled', 'geometry', 'seasonal']
        """
        # Initialise the list of fields
        list_of_fields = []
        for table_id in table_ids:
            # Get the field names
            fields = self.get_field_names(table_id)
            # Append them to the list of fields
            list_of_fields.append(fields)
        # Initialise the fields in common
        fields_in_common = []
        # Iterate through the list of fields and get the common ones
        for i, fields in enumerate(list_of_fields):
            if (i==0):
                fields_in_common = fields
            # Get the intersection of the fields
            fields_in_common = list(set(fields) & set(fields_in_common))
        return fields_in_common

    def get_fields_in_common_for_dataset(self, dataset_id: str)->list:
        """Gets the fields in common across all tables in the dataset

        Args:
            dataset_id (str): The dataset id, e.g. restor-gis.rl

        Returns:
            list: A list of the fields in common across all of the tables, e.g. ['tax_comm', 'dist_comm', 'source', 'citation', 'compiler', 'origin', 'sci_name', 'presence', 'generalisd', 'yrcompiled', 'geometry', 'seasonal']
        """
        # Get the fully qualified table news for the dataset
        table_names = self.get_table_names(dataset_id)
        # Get the fields in common for all of the tables
        fields_in_common = self.get_fields_in_common(table_names)
        # Return the results
        return fields_in_common

    def get_geometry_size(self, feature: fiona.Feature, as_geojson:bool=True)-> int:
        """Gets the size of the geometry by getting the length of the WKT or GeoJSON representation of the geometry

        Args:
            feature (fiona.Feature): The feature as a fiona.Feature
            as_geojson (bool, optional): Set to True to get the length of the geojson string. Default is True.

        Returns:
            int: The size of the geometry in characters
        """
        # Get the geometry
        geometry = shape(feature['geometry'])
        # Get the geojson string
        geometry_string = to_geojson(geometry) if as_geojson else geometry.wkb
        # Get the length of the encoded geometry_string
        return len(geometry_string.encode('utf-8'))

    def get_geometry_type(self, table_id: str)-> str:
        """Gets the geometry type for the table

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds

        Returns:
            str: The geometry type, e.g. POLYGON, MULTIPOLYGON etc.
        """
        # Get table schema
        table = self.client.get_table(table_id)
        # Find GEOGRAPHY columns and inspect their types
        for field in table.schema:
            if field.field_type == "GEOGRAPHY":
                log(f"Column '{field.name}' is of type GEOGRAPHY")
                # Query the table to get the geometry type from the data
                query = f"SELECT DISTINCT ST_GeometryType({field.name}) AS geometry_type FROM `{table_id}`"
                results = self.client.query(query)
                for row in results:
                    log(f"Geometry Type in '{field.name}': {row['geometry_type']}")

    def get_job_size(self, feature_count: int) -> int:
        """Gets the optimum job site based on the feature count and the daily job quota per table

        Args:
            feature_count (int): The feature count to insert

        Returns:
            int: The job size
        """
        # Calculate the job size using the number of rows and the jobs quota per table per day
        job_size = math.ceil(feature_count/BQ_QUOTA_JOBS_PER_TABLE_PER_DAY)
        return job_size

    def get_load_failures_table_id(self)->str:
        """Gets the table_id for the load_failures table

        Returns:
            str: The table_id, e.g. restor-gis.public.load_failures
        """
        return self.get_name_with_project_id(f"{DATASET_NAME_PUBLIC}.{TABLE_NAME_LOAD_FAILURES}")

    def get_load_jobs_table_id(self)->str:
        """Gets the table_id for the load_jobs table

        Returns:
            str: The table_id, e.g. restor-gis.public.load_jobs
        """
        return self.get_name_with_project_id(f"{DATASET_NAME_PUBLIC}.{TABLE_NAME_LOAD_JOBS}")

    def get_name_with_project_id(self, id: str)->str:
        """Gets the fully qualified table or dataset name from the table or dataset id, i.e. with the project prefix

        Args:
            id (str): The id of the table or dataset, e.g. rl.birds or rl

        Returns:
            str: The fully qualified table or dataset, e.g. restor-gis.rl.birds or restor-gis.rl
        """
        # get the project name
        project_name = self.client.project
        # return the full id
        return f"{project_name}.{id}"

    def get_name_without_project_id(self, id: str)->str:
        """Gets the name from the table or dataset id without the project prefix if it is present

        Args:
            id (str): The full id of the table or dataset, e.g. restor-gis.rl or restor-gis.rl.birds

        Returns:
            str: The name of the table or dataset without the project prefix, e.g. rl or rl.birds
        """
        # get the project name
        project_name = self.client.project
        if id.startswith(project_name):
            return id[len(project_name)+1:]
        else:
            return id

    def get_postgis_schema(self, conn, table_name: str) -> tuple:
        """Gets the schema from the PostGIS table 

        Args:
            conn (psycopg2.connection): The psycopg2.connection object which is already open
            table_name (str): The name of the table to get the schema for, e.g. sites

        Returns:
            tuple: The schema suitable for passing to get_bq_schema to get the corresponding BigQuery table schema and the geometry field names as a list, e.g. ['polygon','centroid']
        """
        # Query information_schema for column details
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}' ORDER BY ordinal_position;")
            # Get the columns and their data types
            columns = cursor.fetchall()
        # Initialise the geometry field names - there may be more than one, e.g. polygon and centroid
        geometry_field_names = []
        # Initialise the schema
        schema = {}
        # Get the schema details as a dict
        for column in columns:
            if (column[1] == 'USER-DEFINED'):
                # If we have a USER-DEFINED column, set the specific type as the udt_type, e.g. geometry
                data_type = column[2]
                # If it is geometry then set that as the return geometry_field
                if (data_type=='geometry'):
                    # Append the geometry field name
                    geometry_field_names.append(column[0])
            elif (column[1] == 'ARRAY'):
                # If we have an ARRAY column, set the specific type as a List[<udt_type>] of the udt_type, e.g. List[_varchar]
                data_type = f"ARRAY[{column[2]}]"
            else:
                # Normal column
                data_type = column[1]
            # Add the columnname:columntype
            schema[column[0]] = data_type        
        # Return the schema and geometry_field_names
        return schema, geometry_field_names

    def get_row_count(self, table_id: str)->int:
        """Returns the number of rows in the table 

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds

        Returns:
            int: The number of rows in the table
        """
        # Get the table object
        table = self.client.get_table(table_id)
        # Retrieve the row count
        row_count = table.num_rows
        return row_count

    def get_schema_diffs(self, table_id: str, table_ids: list):
        """ Prints out the schema differences between the schema in the table_id and the schemas in the table_ids

        Args:
            table_id (str): The source table_id to compare against, e.g. restor-gis.rl.all_species
            table_ids (list): The table_ids to compare with, e.g. ['restor-gis.rl.mammals','restor-gis.rl.birds', ..]
        """
        # Get the fields in the source table
        source_field_names = self.get_field_names(table_id)
        # Iterate through the tables
        for _table_id in table_ids:
            # Get the fields in the destination table
            dest_field_names = self.get_field_names(_table_id)
            # Compare the fields
            results = compare_lists(source_field_names, dest_field_names)
            # Print the results
            print(f"\nTable: {_table_id} has the extra fields: {results['unique_to_list2']}")

    def get_table_names(self, dataset_id: str)->list:
        """Returns a list of fully qualified table names in the dataset 

        Args:
            dataset_id (str): The dataset id, e.g. restor-gis.rl

        Returns:
            list: The list of fully qualified table names, e.g. ['restor-gis.rl.birds','restor-gis.rl.mammals', ..]
        """
        # Get the dataset id without the project id
        dataset_id = self.get_name_without_project_id(dataset_id)
        # Get a reference to the dataset
        dataset_ref = self.client.dataset(dataset_id)
        # Get a list of the tables
        tables = self.client.list_tables(dataset_ref)
        # Return the fully qualified table names
        table_names = [f"{self.client.project}.{dataset_id}.{table.table_id}" for table in tables]
        return table_names

    def get_vertex_count(self, feature: fiona.Feature)-> int:
        """Gets the number of vertices in the passed feature

        Args:
            feature (fiona.Feature): The feature as a fiona.Feature

        Returns:
            int: The number of vertices in the geometries
        """
        # Get the geometry
        geometry = shape(feature['geometry'])
        if geometry.geom_type == 'Point':
            point_count = 1
        elif geometry.geom_type in ('MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon'):
            point_count = len(list(geometry.exterior.coords)) if geometry.geom_type == 'Polygon' else len(geometry.coords)
        else:
            # Handle complex or nested geometries if needed
            point_count = sum(len(g.coords) for g in geometry.geoms)
        return point_count

    def goto_row(self, gdb_path:str, feature_class: str, row_index:int):
        """Debug method for iterating through the features and stopping at a specific index in a dataset. This is slow!

        Args:
            gdb_path (str): The path to the File Geodatabase which is in fact a folder, e.g. data.gdb
            feature_class (str): The layer name from within the File Geodatabase that will be iterated
            row_index (int): The row index, e.g. 1701 will stop at the 1701th row
        """
        with fiona.open(gdb_path, layer=feature_class) as src:
            # Use islice to start at the specified row index
            for feature in islice(src, row_index, None):
                print(self.get_feature_props(feature))
                print(f"Geometry size: {self.get_geometry_size(feature)}")
                break

    def import_redlist(self, data_folder: str):
        """Imports the IUCN Red List from the data folder

        Args:
            data_folder (str): The folder where the IUCN Red List is download, e.g. /Users/andrewcottam/Downloads
        """
        # Get the project id
        project = self.client.project
        # Import all of the spatial data
        self.load_file(f'{data_folder}/AMPHIBIANS_PART1.shp', f'{project}.{DATASET_NAME_RED_LIST}.amphibians_part1') 
        self.load_file(f'{data_folder}/AMPHIBIANS_PART2.shp', f'{project}.{DATASET_NAME_RED_LIST}.amphibians_part2') 
        self.load_file(f'{data_folder}/BOTW.gdb', f'{project}.{DATASET_NAME_RED_LIST}.birds', layer_name = 'all_species') 
        self.load_file(f'{data_folder}/MAMMALS.shp', f'{project}.{DATASET_NAME_RED_LIST}.mammals') 
        self.load_file(f'{data_folder}/PLANTS_PART1.shp', f'{project}.{DATASET_NAME_RED_LIST}.plants_part1') 
        self.load_file(f'{data_folder}/PLANTS_PART2.shp', f'{project}.{DATASET_NAME_RED_LIST}.plants_part2') 
        self.load_file(f'{data_folder}/PLANTS_PART3.shp', f'{project}.{DATASET_NAME_RED_LIST}.plants_part3') 
        self.load_file(f'{data_folder}/REPTILES_PART1.shp', f'{project}.{DATASET_NAME_RED_LIST}.reptiles_part1') 
        self.load_file(f'{data_folder}/REPTILES_PART2.shp', f'{project}.{DATASET_NAME_RED_LIST}.reptiles_part2') 
        # Add the fields to the birds table that are missing
        fields = []
        fields.append(bigquery.SchemaField('common_name', 'STRING', description='Species common name in English'))
        fields.append(bigquery.SchemaField('taxa', 'STRING', description='High level taxa name, e.g. birds, mammals'))
        fields.append(bigquery.SchemaField('category', 'STRING', description='Red List Threatened Status'))
        fields.append(bigquery.SchemaField('kingdom', 'STRING', description=''))
        fields.append(bigquery.SchemaField('phylum', 'STRING', description=''))
        fields.append(bigquery.SchemaField('class', 'STRING', description=''))
        fields.append(bigquery.SchemaField('order_', 'STRING', description=''))
        fields.append(bigquery.SchemaField('family', 'STRING', description=''))
        fields.append(bigquery.SchemaField('genus', 'STRING', description=''))
        fields.append(bigquery.SchemaField('subspecies', 'STRING', description=''))
        # Add the fields to the birds table
        self.add_fields(fields, f'{project}.{DATASET_NAME_RED_LIST}.birds')
        # Union all of the geospatial data together
        table_names = b.get_table_names(DATASET_NAME_RED_LIST)
        self.union_tables(table_names, f'{project}.{DATASET_NAME_RED_LIST}.all_species', True)
        # Import the birds checklist table
        self.load_nonspatial(f'{data_folder}/BOTW.gdb', f'{project}.{DATASET_NAME_RED_LIST}.birds_checklist', layer_name = 'Checklist_v8_txt') 
        # Add a field to the all_species table for the taxa, e.g. mammals, birds, reptiles
        self.add_fields([bigquery.SchemaField('taxa', 'STRING', description='Common name for the species group, e.g. birds, mammals etc.')], f'{project}.{DATASET_NAME_RED_LIST}.all_species')
        # Update the all_species table with the birds data from the birds_checklist table
        query = f"update `{project}.{DATASET_NAME_RED_LIST}.all_species` a set category=IUCN_Red_List_Category_2023, a.family = FamilyName from (select distinct sci_name, IUCN_Red_List_Category_2023, FamilyName from `{project}.{DATASET_NAME_RED_LIST}.birds` b, `{project}.{DATASET_NAME_RED_LIST}.birds_checklist` c where sci_name = ScientificName) as sub where a.sci_name = sub.sci_name;"
        self.client.query(query)
        # Update all the taxa values based on the source table, e.g. mammals, birds, reptiles
        query = f"update `{project}.{DATASET_NAME_RED_LIST}.all_species` set taxa = CASE WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.amphibians_part1' THEN 'Amphibians' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.amphibians_part2' THEN 'Amphibians' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.birds' THEN 'Birds' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.mammals' THEN 'Mammals' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.plants_part1' THEN 'Plants' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.plants_part2' THEN 'Plants' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.plants_part3' THEN 'Plants' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.reptiles_part1' THEN 'Reptiles' WHEN source_table = '{project}.{DATASET_NAME_RED_LIST}.reptiles_part2' THEN 'Reptiles' ELSE '' END where id!='';"
        self.client.query(query)

    def import_sites(self, source_path: str, job_size: int=1):
        """Imports the Restor sites into BigQuery using a local shapefile

        Args:
            source_path (str): The source path, e.g. /Users/andrewcottam/Documents/GitHub/postgis-microservice/restor_sites.shp
        """
        # Get the table id
        table_id = self.get_name_with_project_id(f"{DATASET_NAME_SITES}.{TABLE_NAME_SITES}")
        self.load_file(source_path, table_id, job_size=job_size)

    def import_wdpa(self, source_path: str, layer_name: str):
        """Imports the WDPA into BigQuery using the source path and layer name

        Args:
            source_path (str): The source path to the file geodatabase, e.g. /home/andrew/Downloads/wdpa_nov_2024/WDPA_Nov2024_Public.gdb
            layer_name (str): The layer name for the WDPA polygons, e.g. WDPA_poly_Nov2024
        """
        # Get the project id
        project = self.client.project
        # Check the dataset exists
        if (not self.dataset_exists(DATASET_NAME_WDPA)):
            # Create the dataset
            self.create_dataset(DATASET_NAME_WDPA)
        # Load the data
        self.load_file(source_path, f'{project}.{DATASET_NAME_WDPA}.{layer_name}', layer_name = layer_name) 
        
    def insert_rows(self, table_id: str, data: list) -> bool:
        """Inserts a row of data into the table using the Streaming API. To insert spatial data directly, the value should be Well-Known Text (WKT), e.g. {"geometry": "POINT(2 4)", ..}

        Args:
            table_id (str): table_id (str): The full table id, e.g. restor-gis.rl.birds
            data (list): The data to enter as a list of Python dicts

        Returns:
            bool: Returns True if the insert was successful
        """
        # Insert the data into the table
        errors = self.client.insert_rows_json(table_id, data)
        if errors:
            log(f"Encountered errors while inserting rows into the {table_id} table: {errors}", "ERROR")
            return False
        else:
            log(f"Inserted data into the {table_id} table successfully")
            return True

    def load_database_table(self, ip: str, db_name: str, username: str, password: str, source_table_name: str, table_id: str, port: str=5432, batch_size: int=1000):
        """Loads a database table into BigQuery using the Streaming API    

        Args:
            ip (str): The IP address of the database, e.g. '127.0.0.1'
            db_name (str): The database name, e.g. 'restor2-search'
            username (str): The username, e.g. 'analytics-ro'
            password (str): The password
            source_table_name (str): The name of the source table, e.g. 
            table_id (str): The fully qualified name of the destination table, e.g. restor-gis.sites.tmp
            port (str, optional): The port to connect to. Defaults to 5432.
            batch_size (int, optional): The size of each batch to insert at a time. Default value is 1000.
        """
        log(f"\nLoading database table '{db_name}.{source_table_name}' into '{table_id}'", "INFO")
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(host=ip,database=db_name,user=username,password=password,port=port)
        # Get the PostGIS table schema and geometry_field_names
        postgis_schema, geometry_field_names = self.get_postgis_schema(conn, source_table_name)
        # HACK - Get the geometry field name and assume it is the first geometry field
        geometry_field = geometry_field_names[0]
        # If the table already exist then delete it
        if (self.table_exists(table_id)):
            # self.delete_table(table_id)
            pass
        # Get the corresponding BigQuery schema
        bq_schema = self.get_bq_schema({'properties':postgis_schema})
        # We want to make the original geometry field a STRING field so we can add the geometries as text and then make them valid using BigQuery in another column called geometry using ST_GEOGFROMTEXT(<geometryfield>, make_value=>TRUE)
        # First get the index of the geometry field
        index = next((i for i, item in enumerate(bq_schema) if item.name == geometry_field), None)
        # Remove the original geometry field definition
        bq_schema.pop(index)
        # Add a new STRING field to hold the original geometry as a string
        field = bigquery.SchemaField('original_geometry', 'STRING')
        bq_schema.insert(index, field)
        # Add a new GEOGRAPHY field to hold the valid geometry
        geography_field = bigquery.SchemaField('geometry', 'GEOGRAPHY')
        bq_schema.append(geography_field)
        # Create the table
        self.create_table(table_id, bq_schema, f"Imported from database table '{db_name}.{source_table_name}' using the 'load_database_table' method on the BigQuery Python class on {datetime.isoformat(datetime.now())}")
        # Initialise the data to be inserted
        insert_data = []
        # Query the table to get the data
        cur = conn.cursor()
        # cur.execute(f"select * from {source_table_name} limit 20;")
        cur.execute(f"select * from {source_table_name};")
        # Get the rows
        rows = cur.fetchall()
        # Get the feature count
        feature_count = len(rows)
        log(f"\nFeature count: {feature_count}", "INFO")
        # Iterate through the rows and write them to BigQuery
        for i, row in enumerate(rows):
            # Create a row as a dictionary with the field name and the value
            data = {field:row[i] for i, field in enumerate([col.name for col in bq_schema[:-1]])}
            # Convert any numeric values to floats as numerics are not json serialisable
            numeric_fields = [k for k,v in postgis_schema.items() if v=='numeric']
            for numeric_field in numeric_fields:
                data[numeric_field] = float(data[numeric_field])
            # Convert any jsonb values to normal json
            jsonb_fields = [k for k,v in postgis_schema.items() if v=='jsonb']
            for jsonb_field in jsonb_fields:
                # The data from the Restor sites jsonb fields is a list of normal json for some reason
                if len(data[jsonb_field])>0:
                    data[jsonb_field] = json.dumps(data[jsonb_field][0])
                else:
                    data[jsonb_field] = None
            # Convert any date values to floats as numerics are not json serialisable
            date_fields = [k for k,v in postgis_schema.items() if v=='date']
            for date_field in date_fields:
                # If there is a date value
                if data[date_field]:
                    # Convert it to a string
                    data[date_field] = str(data[date_field])
            # Convert any timestamp values to formatted datetime
            timestamp_fields = [k for k,v in postgis_schema.items() if v=='timestamp with time zone']
            for timestamp_field in timestamp_fields:
                data[timestamp_field] = datetime.isoformat(data[timestamp_field])
            # Write the data for insertion
            data = {k:v for k,v in data.items()}
            insert_data.append(data)
            if (i+1) % batch_size == 0:
                # Insert the features
                self.load_features_streaming(insert_data, table_id)
                log(f"Inserted {i+1}/{feature_count} rows)", "DEBUG")
                # Reset the insert_data
                insert_data.clear()
        # Insert the final features
        self.load_features_streaming(insert_data, table_id)
        log(f"Inserted {i+1}/{feature_count} rows)", "DEBUG")
        # Run a query to update the geometry field
        log(f"Updating the geometry field with the original geometry made valid", "DEBUG")
        query = f'UPDATE {table_id} SET geometry=ST_GEOGFROMWKB(original_geometry, make_valid=>TRUE) WHERE id IS NOT NULL;'
        self.client.query(query)
        # Close the connection
        conn.close()

    def load_features_streaming(self, features: list, table_id: str):
        """Loads the feature into a BigQuery table using the WKT representation of the geometry rather than the bq cli.
            See here: https://cloud.google.com/bigquery/docs/geospatial-data#geojson-data

        Args:
            features (list): The features to insert into a table in BigQuery as a list of dictionaries
            table_id (str): The full table id, e.g. restor-gis.rl.birds
        """
        # Insert the record into the table using the streaming method 'insert_rows_json'
        errors = self.client.insert_rows_json(table_id, features)
        # Check for errors
        if errors == []:
            log("Record inserted successfully.")
        else:
            log("Errors inserting the record:", errors[0]['errors'][0]['message'])

    def load_features(self, src: fiona.Collection, features: list, table_id: str):
        """Loads the features into a BigQuery table using the BigQuery CLI and a new-line delimited GeoJSON file as an intermediate step.
            See here: https://cloud.google.com/bigquery/docs/geospatial-data#geojson-files

        Args:
            src (fiona.Collection): The source feature collection as a fiona.Collection
            features (list): A list of fiona.Feature features to import into a table in BigQuery
            table_id (str): The full table id to load the features into, e.g. restor-gis.rl.birds
        """
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            # Export the features to geojson
            self.export_to_geojson(src, features, temp_file.name)
            # Get the table_id without the project name - the bq cli doesnt need it
            table_id = self.get_name_without_project_id(table_id)
            # Call the BigQuery command line tool to load the data
            command = ["bq","load","--source_format=NEWLINE_DELIMITED_JSON","--json_extension=GEOJSON", table_id, temp_file.name]
            # Run the command
            try:
                # print(" ".join(command))
                subprocess.run(command, check=True, text=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                # Log any failures
                log(f"Failed to load features: {e.output}", "ERROR")

    def load_file(self, source_path: str, table_id: str, layer_name: str="", validate_feature: bool=True, job_size: int=-1, start_at: int=0):
        """Load data into a BigQuery table from a file. Any failures are logged to Google Cloud Platform with the bigquery-import log name. This function appends the data to an existing table if that table already exists otherwise it creates a new table with the same schema as the input data. At the end of the load process a record of the Load Job is made in the public.load_jobs table.

        Args:
            source_path (str): The path to the input data, e.g. '/home/andrew/data/rl/BOTW.gdb', '/home/andrew/data/rl/birds_subset.shp' or '/Users/andrewcottam/Documents/GitHub/restor.eco-etl/tmp.geojson'
            table_id (str): The full table id to load the data into, e.g. restor-gis.rl.birds
            layer_name (str, optional): The layer name from within the File Geodatabase that will be imported. Only needed for File Geodatabases. Default value is ''
            validate_feature (bool, optional): Set to True to validate the feature before loading into BiqQuery. The default value is True.
            job_size (int, optional): The job size of rows to load into BigQuery - this can be used to overcome quotas on the number of load jobs per day per table which is 1,500 by default. Default value is -1 which will divide the total number of features by the quota to get an optimum job size, e.g. if there are 17,000 features then the job size will be 17,000/1,500->12 (rounding up).
            start_at (int, optional): The index position to start at. Default is 0.
        """
        # Set the instance level variables
        self.source_path = source_path
        self.table_id = table_id
        self.layer_name = layer_name
        self._validate_feature = validate_feature
        self.job_size = job_size
        self.start_at = start_at
        # Get the start time
        start_time = datetime.now()
        # Get the fiona.collection depending on the source dataset
        if (source_path.endswith('shp')): # shapefile
            log(f"\nLoading shapefile '{source_path}' into '{table_id}'", "INFO")
            self.src = fiona.open(source_path, "r")
        elif (source_path.endswith('gdb')): # file geodatabase
            log(f"\nLoading file geodatabase '{source_path}' into '{table_id}'", "INFO")
            self.src = fiona.open(source_path, layer=layer_name)
        elif (source_path.endswith('geojson')): # GeoJSON file
            log(f"\nLoading GeoJSON file '{source_path}' into '{table_id}'", "INFO")
            self.src = fiona.open(source_path, "r")
        # Get the count of the features
        self.feature_count = len(self.src)
        log(f"Input feature count: {self.feature_count}", "INFO")
        # Get the filename
        self.filename = source_path.split('/')[-1]
        # See if the BigQuery table already exists
        if not self.table_exists(table_id):
            # if not, create the table using the schema from the input dataset
            bq_schema = self.get_bq_schema(self.src.schema)
            self.create_table(table_id, bq_schema, f"Imported from '{source_path}' using the 'load_spatial' method on the BigQuery Python class on {datetime.isoformat(datetime.now())}")
        # If the file has a geometry then load the data using the spatial loader
        if self.geometry_field_exists(self.src.schema):
            # Spatial data
            self.load_file_spatial()
        else:
            # Non-spatial data
            self.load_file_nonspatial()
        # Get the end time
        end_time = datetime.now()
        # Get the duration
        duration = format_duration(start_time, end_time)
        # Get the number of rows
        self.row_count = self.get_row_count(self.table_id)
        # Add a row in the load_jobs table
        data = {'source_path': source_path, 'layer_name': layer_name, 'table_id': table_id, 'input_feature_count': self.feature_count, 'job_size': self.job_size, 'job_count': self.job_count, 'start_at': start_at, 'validate_feature': validate_feature, 'invalid_feature_count': self.invalid_feature_count, 'inserted_features': self.inserted_features, 'table_row_count': self.row_count, 'start_time': datetime.isoformat(start_time), 'end_time': datetime.isoformat(end_time), 'duration': duration, 'status': self.status}
        self.insert_rows(self.load_jobs_table_id, [data])
        log(f"\nFinished loading '{source_path}' into '{table_id}'", "INFO")
        print(data)
        logger.log_struct(data)
        # Ensure src is closed when done
        self.src.close()

    def load_file_nonspatial(self):
        """Load a nonspatial data file into BigQuery using the Python Client. 
        """
        # Get the rows as a list of dict objects, e.g. [{'sci_name': 'Upupa epops', 'class': 'Aves}, {}, ..]
        data = [dict(row.properties.items()) for row in self.src]
        # Insert the rows
        self.insert_rows(self.table_id, data)
        # Set the instance variables
        self.inserted_features = len(data)
        self.job_count = -1
        self.invalid_feature_count = 0
        self.status = 'COMPLETED'

    def load_file_spatial(self):
        """Load a geospatial data file into the BigQuery table using the BigQuery CLI. Any failures are logged to Google Cloud Platform with the bigquery-import log name. This function appends the data to an existing table if that table already exists otherwise it creates a new table with the same schema as the input data. Features are validated against a number of checks to ensure they can be inserted into the table. At the end of the load process a record of the Load Job is made in the public.load_jobs table. 
        """
        try:
            # Get the optimum job size, e.g. 12
            optimum_job_size = self.get_job_size(self.feature_count)
            # If the user has passed a specific job size, try and use that
            if (self.job_size != -1):
                # The job size passed was too small and the number of jobs will exceed the daily quota
                if (self.job_size < optimum_job_size):
                    log(f"\nJob size: {self.job_size} will exceed the maximum daily load jobs per table quota. Using an optimum size.", "INFO")
                    # Use the optimum job size
                    self.job_size = optimum_job_size
            else:
                # Use the optimum job size
                self.job_size = optimum_job_size
            log(f"\nJob size: {self.job_size}", "INFO")
            # Get the number of jobs
            self.job_count = math.ceil((self.feature_count-self.start_at)/self.job_size)
            log(f"Job count: {self.job_count} (the actual job count will depend on how many invalid features there are)", "INFO")
            log(f"Starting at row: {self.start_at}\n", "INFO")
            # Initialise the list that will contain the features for the job
            features = []
            # Initialise the job counter
            job = 1
            # Initialise the invalid feature count
            self.invalid_feature_count = 0
            # Initialise the inserted feature count
            self.inserted_features = 0
            # Iterate through the features
            for i, feature in enumerate(self.src):
                # Move to the start at position
                if (i >= self.start_at):
                    # Get the feature properties
                    props = self.get_feature_props(feature)
                    try:
                        if self._validate_feature:
                            # Validate the feature to make sure we can load it into BigQuery
                            self.validate_feature(feature, self.src.schema)
                    except BigQueryException as e:
                        # increment the invalid_feature_count
                        self.invalid_feature_count += 1
                        # Get the error type
                        error = e.args[0]
                        print(f"Row {i}: Skipping: {error} - Feature: {props}")
                        logger.log_struct({'Message': f"Row {i}: Skipping: {error}", 'Feature': props}, severity="WARNING")
                        # Enter a record in the load_failures table
                        self.insert_rows(self.load_failures_table_id, [{'source_path': self.source_path, 'layer_name': self.layer_name, 'table_id': self.table_id, 'row': i, 'props': json.dumps(props), 'fail_time': datetime.isoformat(datetime.now()), 'fail_reason': error}])
                    else:
                        # Add the feature to the list of features in this job
                        features.append(feature)
                        # If we have a full job
                        if (len(features) % self.job_size == 0):
                            # Load the features into BigQuery
                            self.load_features(self.src, features, self.table_id)
                            log(f"Loading: {self.filename} Completed Jobs: {job}/{self.job_count} Completed Features: {i+1}/{self.feature_count} Invalid features: {self.invalid_feature_count}")
                            # Reset the features and job
                            features.clear()
                            job += 1
                            # Increment the inserted_features counter
                            self.inserted_features += self.job_size
                else:
                    # Log the skipping every 100 rows
                    if (i % 100)==0:
                        log(f"Skipping to row: {self.start_at} Current position: {i}")
            # Load the last features into BigQuery - we may not have a full job
            if len(features)>0:
                # Load the features into BigQuery
                self.load_features(self.src, features, self.table_id)
                log(f"Loading: {self.filename} Completed Jobs: {job}/{self.job_count} Completed Features: {i+1}/{self.feature_count} Invalid features: {self.invalid_feature_count}")
                # Increment the inserted_features counter
                self.inserted_features += len(features)
        # Make sure we free the fiona collection
        finally:
            # Get the status
            self.status = 'COMPLETED' if (i+1==self.feature_count) else 'INTERRUPTED'

    def load_fiona_feature(self, feature: fiona.Feature, table_id: str, geometry_field: str='geometry'):
        """Loads the feature into a BigQuery table using the WKT representation of the geometry rather than the bq cli.
            See here: https://cloud.google.com/bigquery/docs/geospatial-data#geojson-data

        Args:
            feature (fiona.Feature): The feature to import into a table in BigQuery as a fiona.Feature
            table_id (str): The full table id, e.g. restor-gis.rl.birds
            geometry_field (str, optional): The name of the geometry field. Defaults to 'geometry'.
        """
        # Get the feature properties as a python dictionary
        feature = dict(feature['properties'])
        # Get the geometry shape
        geom_shape = shape(feature[geometry_field])
        # log(geom_shape)
        # geom_shape = shapely.geometry.LineString([(-118.4085, 33.9416), (-73.7781, 40.6413)]) # this works!
        # geom_shape = shapely.geometry.Polygon([(0, 0),(1, 0),(1, 1),(0, 1),(0, 0)]) # ccw
        # geom_shape = shapely.geometry.Polygon([(0, 0),(0, 1),(1, 1),(1, 0),(0, 0)]) # cw
        feature[geometry_field] = dumps(geom_shape)
        # Insert the feature
        self.load_feature(feature,table_id, geometry_field)

    def schemas_match(self, schema1: dict, schema2: dict)->bool:
        """Compares the two schemas and returns True if they match

        Args:
            schema1 (dict): The source schema 
            schema2 (dict): The target schema 

        Returns:
            bool: True if the schemas match, False otherwise.
        """
        # Get the keys of both dictionaries
        keys1 = set(schema1.keys())
        keys2 = set(schema2.keys())
        # Find missing and extra keys in each dictionary
        missing_in_schema2 = keys1 - keys2
        missing_in_schema1 = keys2 - keys1
        # Check for key mismatches and print an error if found
        if missing_in_schema2 or missing_in_schema1:
            if missing_in_schema2:
                log(f"Keys missing in schema2: {missing_in_schema2}")
            if missing_in_schema1:
                log(f"Keys missing in schema1: {missing_in_schema1}")
            return False
        else:
            return True

    def table_exists(self, table_id: str) -> bool:
        """Returns True if the table exists, False otherwise

        Args:
            table_id (str): The full table id, e.g. restor-gis.rl.birds

        Returns:
            bool: Returns True if the table exists, False otherwise
        """
        # Check if the table exists
        try:
            self.client.get_table(table_id)  # Attempt to get the table
            return True
        except NotFound:
            return False

    def union_tables(self, table_ids: list, output_table_id: str, overwrite: bool=False) -> bool:
        """ Unions all of the passed tables together to create a new table with two additional fields: one called 'source_table' which points to the original source of the data and another called 'id' which is a unique identifier.

        Args:
            table_ids (list): The list of source tables, e.g. ['restor-gis.rl.plants_part1', 'restor-gis.rl.plants_part2', ..]
            output_table_id (str): The id of the output table, e.g. restor-gis.rl.plants

        Returns:
            bool: True if the table was successfully created, False otherwise.
        """
        # Check the table doesnt already exist
        if self.table_exists(output_table_id) and not overwrite:
            log(f"The table {output_table_id} already exists - skipping", 'ERROR')
            return False
        if self.table_exists(output_table_id) and overwrite:
            log(f"Deleting the output table '{output_table_id}'")
            # Delete the output table
            self.delete_table(output_table_id)
        # Get the unique fields across the source tables
        fields_in_common = self.get_fields_in_common(table_ids)
        # Initialise the SQL statement
        query = f"CREATE TABLE `{output_table_id}` AS "
        # Iterate through the table names and build the sql
        for table_id in table_ids:
            query += f"select GENERATE_UUID() AS id, {','.join(fields_in_common)}, '{table_id}' source_table from `{table_id}` UNION ALL ";
        # Run the query
        self.client.query(query[:-11])
        log(f"Table '{output_table_id}' created successfully")
        return True

    def validate_feature(self, feature: fiona.Feature, schema: dict):
        """Checks the feature to make sure it can be loaded into BigQuery, e.g. by checking against certain quotas and limitations

        Args:
            feature (fiona.Feature): The feature to validate
            schema (dict): The schema in the target table - the feature schema and target schema should match
        """
        # get_memory_usage()
        # Check we have a geometry field
        if self.geometry_field_exists(schema):
            # Check the size of the row
            row_size = self.get_geometry_size(feature)
            # Check the row size does not exceed the BigQuery row size limit
            if (row_size > BQ_LIMIT_ROW_SIZE):
                raise BigQueryException(BigQueryErrorType.ROW_EXCEEDS_SIZE_LIMIT.value)
        # get the features properties
        props = self.get_feature_props(feature)
        if (not self.schemas_match(props, schema['properties'])):
            raise BigQueryException(BigQueryErrorType.SCHEMAS_DONT_MATCH.value)
        # check the geometry types match - no longer used
        # if (not feature.geometry.type == schema['geometry']):
        #     raise BigQueryException(f"{BigQueryErrorType.GEOMETRY_TYPES_DONT_MATCH.value} ({feature.geometry.type}!={schema['geometry']})")

if __name__ == '__main__':
    # Call with nohup prefix to stop the job failing when VSCode disconnects
    b = BigQuery()

    # Testing
    # b.load_file('/Users/andrewcottam/Documents/QGIS/Data/restor/birds_subset.shp','restor-gis.public.test_spatial', job_size=5)    # spatial data
    # b.load_file('/Users/andrewcottam/Downloads/BOTW.gdb','restor-gis.public.test_nonspatial', layer_name="Checklist_v8_txt")           # nonspatial data
    b.load_database_table('127.0.0.1', 'restor2-search', 'analytics-ro', 'FAyPNRj37UiHA0DizdfB', 'sites', 'restor-gis.sites.tmp3', port='5481')
    
    # Red List
    # b.import_redlist('/home/andrew/Downloads') # on ubuntu-gnome
    # b.import_redlist('/Users/andrewcottam/Downloads') # on my mba

    # Restor sites
    # b.import_sites('/Users/andrewcottam/Documents/GitHub/postgis-microservice/restor_sites.shp', 10000) # Using a shapefile
    # b.import_sites('/Users/andrewcottam/Documents/GitHub/postgis-microservice/restor_sites.geojson', 1000) # Using a geojson file

    # World Database of Protected Areas
    # b.import_wdpa('/home/andrew/Downloads/wdpa_nov_2024/WDPA_Nov2024_Public.gdb', 'WDPA_poly_Nov2024')