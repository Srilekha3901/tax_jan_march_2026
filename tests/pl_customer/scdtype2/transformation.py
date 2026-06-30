from src.utility.read_db_lib import (
    read_table,
    df_write
)

from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp
)


def run_transformation(
        spark,
        source_config,
        target_config):

    # ==========================================================
    # Read History
    # ==========================================================

    history = read_table(
        spark,
        "[silver].[Customer_History_Backup]"
    )

    history_active = history.filter(col("IsCurrent") == True)

    # ==========================================================
    # Read Source
    # ==========================================================

    silver_clean = read_table(
        spark,
        "[silver].[Customer_Clean]"
    )

    silver_clean = silver_clean.filter(
        col("SourceFileName") == "customer_30062026.csv"
    )

    # ==========================================================
    # NEW RECORDS
    # ==========================================================

    new_records = (
        silver_clean.alias("src")
        .join(
            history_active.alias("hist"),
            "Customer_ID",
            "left_anti"
        )
        .select(
            col("Customer_ID"),
            col("CustomerName"),
            col("Email"),
            col("PhoneNumber"),
            col("City"),
            col("StateName"),
            col("InsertDate")
        )
        .withColumn(
            "EffectiveStartDate",
            col("InsertDate")
        )
        .withColumn(
            "EffectiveEndDate",
            to_timestamp(lit("9999-12-31 00:00:00"))
        )
        .withColumn(
            "IsCurrent",
            lit(True)
        )
        .drop("InsertDate")
    )

    # ==========================================================
    # HISTORY RECORDS NOT IN CURRENT FILE
    # ==========================================================

    unchanged_history = (
        history_active.alias("hist")
        .join(
            silver_clean.alias("src"),
            "Customer_ID",
            "left_anti"
        )
        .select(
            "Customer_ID",
            "CustomerName",
            "Email",
            "PhoneNumber",
            "City",
            "StateName",
            "EffectiveStartDate",
            "EffectiveEndDate",
            "IsCurrent"
        )
    )

    # ==========================================================
    # UPDATED RECORDS
    # ==========================================================

    updated_records = (
        silver_clean.alias("src")
        .join(
            history_active.alias("hist"),
            "Customer_ID",
            "inner"
        )
        .filter(
            (col("src.CustomerName") != col("hist.CustomerName")) |
            (col("src.Email") != col("hist.Email")) |
            (col("src.PhoneNumber") != col("hist.PhoneNumber")) |
            (col("src.City") != col("hist.City")) |
            (col("src.StateName") != col("hist.StateName"))
        )
        .select(
            col("Customer_ID"),
            col("src.CustomerName").alias("CustomerName"),
            col("src.Email").alias("Email"),
            col("src.PhoneNumber").alias("PhoneNumber"),
            col("src.City").alias("City"),
            col("src.StateName").alias("StateName"),
            col("src.InsertDate").alias("InsertDate")
        )
    )

    # ==========================================================
    # INSERT NEW VERSION
    # ==========================================================

    update_new_insert = (
        updated_records
        .withColumn(
            "EffectiveStartDate",
            col("InsertDate")
        )
        .withColumn(
            "EffectiveEndDate",
            to_timestamp(lit("9999-12-31 00:00:00"))
        )
        .withColumn(
            "IsCurrent",
            lit(True)
        )
        .drop("InsertDate")
    )

    # ==========================================================
    # EXPIRE OLD VERSION
    # ==========================================================

    update_mark_history = (
        history_active.alias("hist")
        .join(
            updated_records.select(
                col("Customer_ID"),
                col("InsertDate").alias("ChangeTime")
            ),
            "Customer_ID",
            "inner"
        )
        .select(
            col("hist.Customer_ID").alias("Customer_ID"),
            col("hist.CustomerName").alias("CustomerName"),
            col("hist.Email").alias("Email"),
            col("hist.PhoneNumber").alias("PhoneNumber"),
            col("hist.City").alias("City"),
            col("hist.StateName").alias("StateName"),
            col("hist.EffectiveStartDate").alias("EffectiveStartDate"),
            col("ChangeTime").alias("EffectiveEndDate")
        )
        .withColumn(
            "IsCurrent",
            lit(False)
        )
    )

    # ==========================================================
    # FINAL DATAFRAME
    # ==========================================================

    final_df = (
        new_records
        .unionByName(unchanged_history)
        .unionByName(update_new_insert)
        .unionByName(update_mark_history)
    )

    final_df = final_df.withColumn(
        "Customer_ID",
        col("Customer_ID").cast("int")
    )

    final_df.show(truncate=False)

    # ==========================================================
    # WRITE
    # ==========================================================

    df_write(
        df=final_df,
        expected_table="[silver].[Customer_History_expected]"
    )


# from src.utility.read_db_lib import (
#     read_table,
#     df_write
# )
#
# from pyspark.sql.functions import current_timestamp,lit,to_timestamp
# from pyspark.sql.functions import col
#
#
#
#
# def run_transformation(
#         spark,
#         source_config,
#         target_config):
#
#     silver_History_backup = read_table(
#         spark,
#         '[silver].[Customer_History_Backup]'
#     )
#
#     print("history")
#
#     silver_History_backup.show()
#     silver_History_backup_active = silver_History_backup.filter("IsCurrent=true")
#     silver_History_backup_inactive = silver_History_backup.filter("IsCurrent=False")
#
#     silver_clean = read_table(
#         spark,
#         '[silver].[Customer_Clean]'
#     )
#
#     print('silver_clean')
#     silver_clean.show()
#     #
#     silver_clean = silver_clean.filter(
#         "SourceFileName='customer_30062026.csv'"
#     )
#
#     # columns = silver_clean.columns
#
#     new_records = (
#         silver_clean.alias("src")
#         .join(
#             silver_History_backup.alias("hist"),
#             on="Customer_ID",
#             how="left_anti"
#         )
#         .withColumn(
#             "EffectiveStartDate",
#             col("src.InsertDate")
#         )
#         .withColumn(
#             "EffectiveEndDate",
#             to_timestamp(lit("9999-12-31 00:00:00"))
#         )
#         .withColumn(
#             "IsCurrent",
#             lit(True)
#         )
#     )
#
#     print("new_records")
#     new_records.show()
#
#
#     present_in_back_not_in_silver = silver_History_backup.join(silver_clean,on='Customer_ID',how='left_anti')
#
#     update_new_insert =  (silver_clean.join(
#         silver_History_backup,
#         on='Customer_ID',
#         how='left_semi'
#     ).withColumn('EffectiveStartDate',  col("InsertDate")).withColumn("EffectiveEndDate",to_timestamp(lit("9999-12-31 00:00:00")))
#                          .withColumn('IsCurrent',lit(True)))
#
#     update_mark_history =  (silver_History_backup.join(
#         silver_clean,
#         on='Customer_ID',
#         how='left_semi'
#     ).withColumn('EffectiveEndDate', col("UpdateDate")).withColumn('IsCurrent',lit(False)))
#
#     columns = ['Customer_ID','CustomerName','Email','PhoneNumber','City','StateName','EffectiveStartDate','EffectiveEndDate','IsCurrent']
#
#     final_df = (new_records.select(*columns).union(present_in_back_not_in_silver.select(*columns)).
#             union(update_new_insert.select(*columns)).union(update_mark_history.select(*columns)))
#     final_df = final_df.withColumn(
#         "Customer_ID",
#         col("Customer_ID").cast("int")
#     )
#
#
#     final_df.show()
#
#     df_write(
#         df=final_df,
#         expected_table='[silver].[Customer_History_expected]'
#     )