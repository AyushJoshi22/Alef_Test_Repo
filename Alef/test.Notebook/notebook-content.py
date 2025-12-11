# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "15222668-9f3f-483a-900c-ff9e6cf2e526",
# META       "default_lakehouse_name": "test",
# META       "default_lakehouse_workspace_id": "d397507c-606e-40ce-ae37-864b642f4f05",
# META       "known_lakehouses": [
# META         {
# META           "id": "15222668-9f3f-483a-900c-ff9e6cf2e526"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1, "Alice", 23),
    (2, "Bob", 30),
    (3, "Charlie", 28)
], ["id", "name", "age"])

df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").save("Files/dummy_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException

# Replace these with real IDs
WORKSPACE_ID = "d397507c-606e-40ce-ae37-864b642f4f05"
ITEM_ID = "15222668-9f3f-483a-900c-ff9e6cf2e526"

client = fabric.FabricRestClient()

def check_fabric_shortcut_access():
    url = f"v1/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/shortcuts"

    print(f"Checking access to: {url}")

    try:
        # GET list of shortcuts
        response = client.get(url)
        data = response.json()

        print("SEMPy FabricRestClient access OK ✔")
        print(f"Number of shortcuts: {len(data.get('value', []))}")
        return True

    except FabricHTTPException as e:
        print("❌ SEMPy does NOT have access to Fabric Shortcuts API")
        print("Error:", e)
        return False

    except Exception as e:
        print("❌ Unexpected error")
        print(e)
        return False

# Run access test
check_fabric_shortcut_access()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
