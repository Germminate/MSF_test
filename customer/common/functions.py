from pyspark.sql.functions import current_timestamp, lit

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