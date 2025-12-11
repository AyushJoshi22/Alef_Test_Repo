# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sempy.fabric as fabric
import pandas as pd
 
# Semantic Model / Report Name
report_name = "Guardians"
# Get Table Names
df_tables = fabric.list_tables(report_name)
 
table_list = df_tables["Name"].unique().tolist()

print("Tables Found:")
for t in table_list:
    print(" -", t)
 
#  Get Columns & Datatypes
 
column_rows = []
 
for table in table_list:
    try:
        df = fabric.read_table(report_name, table)
        for col_name, dtype in df.dtypes.items():
            column_rows.append({
                "Table": table,
                "Column": col_name,
                "DataType": str(dtype)
            })
    except:
        # Table objects without row data (like calculation groups)
        column_rows.append({
            "Table": table,
            "Column": "N/A",
            "DataType": "N/A"
        })
 
df_column_metadata = pd.DataFrame(column_rows)
 
print("\n Column Datatypes for All Tables:")
print(df_column_metadata)
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
