#Glue_1 Code Documentation
##Description
This is a glue code that utilizes Spark to process data located in an S3 bucket. The parameters required for the code execution are stored in a DynamoDB table. Within the code, you can easily locate the section responsible for reading these parameters, as follows:

```python
FULL_FORMAT = "*.parquet"
DAILY_FORMAT = "*.parquet"
INPUT_BUCKET = response['Item']['bucket_in']['S']
OUTPUT_BUCKET = response['Item']['bucket_out']['S']
IN_LOCATION = response['Item']['prefix_in']['S']
IN_LOCATION_CDC = response['Item']['prefix_in_cdc']['S']
OUT_LOCATION = response['Item']['prefix_out']['S']
TABLE_NAME = response['Item']['table']['S']
DATABASE = response['Item']['db']['S']
``` 

##The data
Due to confidentiality, I am unable to show the data. However, it consists of catalog parts for trucks. Each truck is assigned a VIN (Vehicle Identification Number) by the manufacturer. Additionally, we have a part number, which serves as the unique code for each part. Furthermore, we have a series. For the purpose of this exercise, I am unable to reveal the specific models, so I will filter the data using 'MODEL1' and 'MODEL2'.

##The ETL
The ETL process begins by selecting the fields in the dataframe. Next, I apply a filter based on specific models within the 'series' field. Then, I perform a hashing transformation on the shortened list of part numbers for each VIN or truck. This facilitates easy grouping of parts and assigning them to a group.

The assigned group is denoted by the text 'group_' followed by a unique row identifier. In this case, I utilized the function:
```python
monotonically_increasing_id() + 1
```

To ensure that group assignments are not repeated, I validate them using a hashing mechanism during the join operation.

And finally i saved the data like parquet, using the out parameters in S3 to don't harcode the paths, this's a particular case for DynamoDB use.


## Libraries Imports

- sys
- pyspark.sql.session.SparkSession
- awsglue.context.GlueContext
- awsglue.job.Job
- awsglue.dynamicframe.DynamicFrame
- pyspark.sql.functions (all functions)
- awsglue.utils.getResolvedOptions
- pyspark.sql.types (all types)
- datetime.datetime
- time
- logging
- boto3
- awsglue.transforms (all transforms)
- pyspark.context.SparkContext
- pyspark.sql.DataFrame
- pyspark.sql.types.StructType
- pyspark.sql.types.StructField
- pyspark.sql.types.StringType
- pyspark.sql.functions.col
- pyspark.sql.functions.collect_list
- pyspark.sql.functions.max
- pyspark.sql.functions.udf
- hashlib

## Functions

### clear_s3_path(bucket_name: str, prefix: str) -> None

Removes all the objects from a S3 bucket that share a common prefix.

**Parameters:**

- `bucket_name` (str): Name of the bucket where the objects to delete are located.
- `prefix` (str): Common prefix of the S3 objects to delete.

**Returns:**

None

### hash_values(values)

This is a function to hash a list using SHA256.

**Parameters:**

- `values` (list): List containing the values to be hashed.

**Returns:**

The hash value that represents the unique list.

## Code

### Welcome Message

Prints a welcome message.

### Set Logger to Debug Mode

Sets the logger to debug mode.

### Get Resolved Options

Retrieves the resolved options from the command-line arguments.

### Initialize SparkSession

Initializes SparkSession.

### Initialize GlueContext and Job

Initializes GlueContext and Job.

### Get Table Dynamo

Retrieves the table name from the resolved options.

### Initialize DynamoDB Resource and Client

Initializes the DynamoDB resource and client.

### Retrieve Item from DynamoDB Table

Retrieves an item from the DynamoDB table based on the process name.

### Set Params

Sets the parameters based on the retrieved item from DynamoDB.

### Read Input Data

Reads the input data from the specified location.

### Print Input Data Schema

Prints the schema of the input data.

### Filter Input Data

Filters the input data based on certain conditions.

### Perform Hashing

Performs hashing on the filtered input data.

### Group Hashing Results

Groups the hashing results by the hash value.

### Write Hashing Results to Output Location

Writes the hashing results to the specified output location.

### Print Writing Time

Prints the time taken for writing.