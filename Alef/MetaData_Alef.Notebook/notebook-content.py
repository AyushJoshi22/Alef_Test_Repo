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
from IPython.display import display

# --- List of report names you want to scan ---
report_names = [
    "Guardians",
    "Pathways"
]

all_rows = []  # This will store the metadata from all reports

# Loop through each report name
for report in report_names:
    try:
        df_tables = fabric.list_tables(report)  # List tables for the current report
        table_list = df_tables["Name"].unique().tolist()
    except Exception as e:
        print(f" Could not list tables for report: {report} -> {e}")
        continue

    # Loop through each table to get columns and data types
    for table in table_list:
        try:
            df = fabric.read_table(report, table)  # Read the table
            for col_name, dtype in df.dtypes.items():
                all_rows.append({
                    "Report": report,
                    "Table": table,
                    "Column": col_name,
                    "DataType": str(dtype)
                })
        except Exception:
            # If reading the table fails, mark it as N/A
            all_rows.append({
                "Report": report,
                "Table": table,
                "Column": "N/A",
                "DataType": "N/A"
            })

# Convert all rows into a DataFrame
df_metadata = pd.DataFrame(all_rows)

# Display the results using the display() function
print("Column Metadata Across All Reports:\n")
display(df_metadata)  # This will output the DataFrame cleanly


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
import pandas as pd
from IPython.display import display
from IPython.display import FileLink

# --- List of report names you want to scan ---
report_names = [
    "Guardians",
    "Pathways"
]

all_rows = []  # This will store the metadata from all reports

# Loop through each report name
for report in report_names:
    try:
        df_tables = fabric.list_tables(report)
        table_list = df_tables["Name"].unique().tolist()
    except Exception as e:
        print(f" Could not list tables for report: {report} -> {e}")
        continue

    for table in table_list:
        try:
            df = fabric.read_table(report, table)
            for col_name, dtype in df.dtypes.items():
                all_rows.append({
                    "Report": report,
                    "Table": table,
                    "Column": col_name,
                    "DataType": str(dtype)
                })
        except Exception:
            all_rows.append({
                "Report": report,
                "Table": table,
                "Column": "N/A",
                "DataType": "N/A"
            })

# Convert collected data to DataFrame
df_metadata = pd.DataFrame(all_rows)

# ---- Display nicely ----
print("Column Metadata Across All Reports:\n")
display(df_metadata)

# ---- Export for download ----
output_file = "Report_Table_Column_Metadata.xlsx"
df_metadata.to_excel(output_file, index=False)

print("\nClick below to download the metadata file:\n")
display(FileLink(output_file))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
import pandas as pd

# --- List of report names you want to scan ---
report_names = [
    "InterimCheck","SkillsMastery"
]

all_rows = []  # This will store the metadata from all reports

# Loop through each report name
for report in report_names:
    try:
        df_tables = fabric.list_tables(report)  # List tables for the current report
        table_list = df_tables["Name"].unique().tolist()
    except Exception as e:
        print(f" Could not list tables for report: {report} -> {e}")
        continue

    # Loop through each table to get columns and data types
    for table in table_list:
        try:
            df = fabric.read_table(report, table)  # Read the table
            for col_name, dtype in df.dtypes.items():
                all_rows.append({
                    "Report": report,
                    "Table": table,
                    "Column": col_name,
                    "DataType": str(dtype)
                })
        except Exception:
            # If reading the table fails, mark it as N/A
            all_rows.append({
                "Report": report,
                "Table": table,
                "Column": "N/A",
                "DataType": "N/A"
            })

# Convert all rows into a DataFrame
df_metadata = pd.DataFrame(all_rows)

# Display the results using the display() function
print("Column Metadata Across All Reports:\n")
display(spark.createDataFrame(df_metadata))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.createDataFrame(df_metadata)
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
