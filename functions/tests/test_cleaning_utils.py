
from databricks.connect import DatabricksSession
import chispa as cp
from ..cleaning_utils import *


def test_lowercase_all_columns():
    # ASSEMBLE
    ## create test data as a list of dicts(via Rows))
    test_data = [
        {
            "ID": 1,
            "First_Name": "Bob",
            "Last_Name": "Builder",
            "Age": 24
        },
        {
            "ID": 2,
            "First_Name": "Sam",
            "Last_Name": "Smith",
            "Age": 41
        }
    ]

    spark= DatabricksSession.builder.getOrCreate()
    
    test_df = spark.createDataFrame(test_data)


    # ACT
    output_df = lowercase_all_column_names(test_df)

    # ASSERT
    expected_data = [
        {
            "id": 1,
            "first_name": "Bob",
            "last_name": "Builder",
            "age": 24
        },
        {
            "id": 2,
            "first_name": "Sam",
            "last_name": "Smith",
            "age": 41
        }
    ]
    expected_df= spark.createDataFrame(expected_data)
    cp.assert_df_equality(output_df, expected_df, ignore_column_order=False)


def test_uppercase_all_columns():
    # ASSEMBLE
    ## create test data as a list of dicts(via Rows)
    test_data= [
        {
            "id": 1,
            "first_name": "Bob",
            "last_name": "Builder",
            "age": 24
        },
        {
            "id": 2,
            "first_name": "Sam",
            "last_name": "Smith",
            "age": 41   
        }
    ]

    # create dataframe from test_data
    spark= DatabricksSession.builder.getOrCreate()
    test_df= spark.createDataFrame(test_data)

    # ACT
    output_df = uppercase_all_column_names(test_df)

    # ASSERT
    expected_data = [
        {
            "ID": 1,
            "FIRST_NAME": "Bob",
            "LAST_NAME": "Builder",
            "AGE": 24
        },
        {
            "ID": 2,
            "FIRST_NAME": "Sam",
            "LAST_NAME": "Smith",
            "AGE": 41
        }
    ]
    expected_df= spark.createDataFrame(expected_data)
    cp.assert_df_equality(output_df, expected_df, ignore_column_order=False)


def test_add_metadata():
    # ASSEMBLE
    ## create test data as a list of dicts(via Rows)
    test_data= [
        {
            "id": 1,
            "first_name": "Bob",
            "last_name": "Builder",
            "age": 24
        },
        {
            "id": 2,
            "first_name": "Sam",
            "last_name": "Smith",
            "age": 41   
        }
    ]

    # create dataframe from test_data
    spark= DatabricksSession.builder.getOrCreate()
    test_df= spark.createDataFrame(test_data)

    field_dict = {"task_id": 1, "ingestion_date": "2024-06-01"}

    # ACT
    output_df = add_metadata(test_df, field_dict)

    # ASSERT
    expected_data = [
        {
            "id": 1,
            "first_name": "Bob",
            "last_name": "Builder",
            "age": 24,
            "task_id": 1,
            "ingestion_date": "2024-06-01"
        },
        {
            "id": 2,
            "first_name": "Sam",
            "last_name": "Smith",
            "age": 41,
            "task_id": 1,
            "ingestion_date": "2024-06-01"
        }
    ]
    expected_df= spark.createDataFrame(expected_data)
    cp.assert_df_equality(output_df, expected_df, ignore_column_order=False)