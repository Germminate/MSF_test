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