from pyspark.sql.functions import current_timestamp, lit, explode_outer, col
from pyspark.sql.types import ArrayType, StructType

def get_read_options(filetype):
    """
    Returns appropriate read options for schema inference based on file type.
    """
    options = {}
    
    if filetype == "csv":
        options["inferSchema"] = "true"
        options["header"] = "true"
    elif filetype == "json":
        options["multiLine"] = "true"
    
    return options


def ingest_to_delta(spark, filetype, file_path, table_path, read_options=None):
    """
    Ingest any file type to Delta table
    
    Args:
        filetype: File format (json, csv, parquet, avro, etc.)
        file_path: Source file path (can be volume path)
        table_path: Target Delta table name (catalog.schema.table)
        read_options: Optional dict of read options (e.g., {"multiLine": True, "header": True})
    """
    # Read with optional format-specific options
    reader = spark.read.format(filetype)
    if read_options:
        for key, value in read_options.items():
            reader = reader.option(key, value)
    
    df = reader.load(file_path)
    
    # Add metadata columns
    df = df.withColumn("_ingest_timestamp", lit(current_timestamp())) \
           .withColumn("_source_file", lit(file_path))
    
    # Write to Delta table
    df.write.format("delta").mode("append").saveAsTable(table_path)
    
    print(f"Successfully wrote to {table_path}")
    return df


def recursive_explode(df):
    schema = df.schema
    cols_to_explode = []
    for field in schema.fields:
        if isinstance(field.dataType, ArrayType):
            cols_to_explode.append(field.name)
        elif isinstance(field.dataType, StructType):
            # Flatten struct fields
            for subfield in field.dataType.fields:
                df = df.withColumn(f"{field.name}_{subfield.name}", col(f"{field.name}.{subfield.name}"))
            df = df.drop(field.name)
    if cols_to_explode:
        for col_name in cols_to_explode:
            df = df.withColumn(col_name, explode_outer(col(col_name)))
        return recursive_explode(df)
    # Check for remaining structs to flatten
    struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
    if struct_cols:
        return recursive_explode(df)
    return df