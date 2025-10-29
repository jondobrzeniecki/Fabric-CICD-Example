# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "938c9c25-435a-4496-93cd-bf2d41a9db1b",
# META       "default_lakehouse_name": "machine_failure_predictions",
# META       "default_lakehouse_workspace_id": "7a786b3c-4d2f-4f34-9a59-ddb2a778408f",
# META       "known_lakehouses": [
# META         {
# META           "id": "938c9c25-435a-4496-93cd-bf2d41a9db1b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

data = spark.read.format("delta").load("Tables/predictive_maintenance_data")
df = data.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from synapse.ml.predict import MLFlowTransformer

# model = MLFlowTransformer(
#     inputCols=list(df.columns),
#     outputCol='predictions',
#     modelName='machine_failure_model_rf',
#     modelVersion=1
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# predictions = model.transform(spark.createDataFrame(df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(predictions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save test data to lakehouse for use in future
# table_name = "predictive_maintenance_test_with_predictions"
# predictions.write.mode("append").format("delta").save(f"Tables/{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
