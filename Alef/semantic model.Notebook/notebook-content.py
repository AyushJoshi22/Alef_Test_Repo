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

# ---------------- CONFIGURATION ----------------
report_names = [
    "InterimCheck",
    "PathwaysOld","PathwayStudent","PathwayStudentOld","PathwayTeacher","PathwayTeacherOld"
]  # Add all your report names here

# ---------------- EXECUTION ----------------
all_table_metadata = []
all_column_metadata = []
all_relationships_metadata = []

for report_name in report_names:
    print(f"\n============================")
    print(f"Processing Report: {report_name}")
    print(f"============================")

    try:
        # ---- TABLES ----
        df_tables = fabric.list_tables(report_name)
        table_list = df_tables["Name"].unique().tolist()

        print("\nTables Found:")
        for t in table_list:
            print(" -", t)
            all_table_metadata.append({
                "Report": report_name,
                "Table": t
            })

        # ---- COLUMNS + DATATYPES ----
        column_rows = []
        for table in table_list:
            try:
                df = fabric.read_table(report_name, table)
                for col_name, dtype in df.dtypes.items():
                    column_rows.append({
                        "Report": report_name,
                        "Table": table,
                        "Column": col_name,
                        "DataType": str(dtype)
                    })
            except Exception as e:
                column_rows.append({
                    "Report": report_name,
                    "Table": table,
                    "Column": "N/A",
                    "DataType": f"Error: {e}"
                })

        df_column_metadata = pd.DataFrame(column_rows)
        all_column_metadata.extend(column_rows)
        print("\nColumn Datatypes for All Tables:")
        print(df_column_metadata)

        # ---- RELATIONSHIPS (5 columns only) ----
        try:
            df_relationships = fabric.list_relationships(report_name)

            # Keep only the required 5 columns
            required_cols = ["Multiplicity", "From Table", "From Column", "To Table", "To Column"]
            df_relationships = df_relationships[required_cols]
            df_relationships["Report"] = report_name

            all_relationships_metadata.extend(df_relationships.to_dict("records"))

            print("\nRelationships:")
            print(df_relationships)

        except Exception as e:
            print("\nCould not retrieve relationships for this report.")
            print("Reason:", e)

    except Exception as e:
        print(f"\nFailed to process report '{report_name}'. Reason: {e}")

# ---------------- COMBINE RESULTS ----------------
df_all_tables = pd.DataFrame(all_table_metadata)
df_all_columns = pd.DataFrame(all_column_metadata)
df_all_relationships = pd.DataFrame(all_relationships_metadata)

print("\n============================")
print("Summary of All Reports Processed")
print("============================")
print("\nAll Tables:")
print(df_all_tables)

print("\nAll Columns:")
print(df_all_columns)

print("\nAll Relationships (5 columns only):")
print(df_all_relationships)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
