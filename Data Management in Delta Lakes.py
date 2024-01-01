# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE people (id INT,firstName STRING, middleName STRING, lastName STRING, gender STRING, birthDate STRING, ssn STRING, salary INT);
# MAGIC   
# MAGIC INSERT INTO people VALUES (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250);
# MAGIC INSERT INTO people VALUES (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500);
# MAGIC INSERT INTO people VALUES (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Cloning Delta Lake Tables
# MAGIC
# MAGIC Shallow clone: only duplicates the Delta transaction logs of the table being cloned; the data files of the table itself are not copied. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move.These clones are cheaper but depend on the source from which they were cloned as the source of data. If the files in the source are removed using the VACUUM, a shallow clone may become unusable. Therefore, shallow clones are typically for testing and development
# MAGIC
# MAGIC Deep clone: A deep clone makes a full copy of the metadata and data files of the table being cloned. It does not depend on the original data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DeepClonePeople
# MAGIC DEEP CLONE people

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DeepClonePeople

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ShallowClonePeople
# MAGIC SHALLOW CLONE people

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC  
# MAGIC ## Overwriting a table
# MAGIC
# MAGIC There are multiple benefits to overwriting tables:
# MAGIC - Speed. Overwriting a table is much faster than deleting data.
# MAGIC - The old version of the table still exists so we can Time Travel to older versions.
# MAGIC - Concurrent queries can still read the table while you are deleting the table.
# MAGIC
# MAGIC **`CREATE OR REPLACE TABLE`** replace the contents of a table each time they execute.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE person(id INT, firstName STRING, middleName STRING, lastName STRING, gender STRING, birthDate STRING, ssn STRING, salary INT);
# MAGIC   
# MAGIC INSERT INTO person VALUES (9939998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452',NULL);
# MAGIC INSERT INTO person VALUES (9994999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500);
# MAGIC INSERT INTO person VALUES (10050000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE personTest AS
# MAGIC SELECT * FROM person

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY personTest

# COMMAND ----------

# MAGIC %md
# MAGIC **`INSERT OVERWRITE`**: It will fail if we try to change our schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE personTest
# MAGIC SELECT * FROM person

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY personTest

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE personTest
# MAGIC SELECT *, current_timestamp() FROM person

# COMMAND ----------

# MAGIC %md
# MAGIC **`INSERT INTO`** to append new rows to an existing Delta table. This allows for incremental updates to existing tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO personTest
# MAGIC SELECT * FROM person

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM persontest

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY personTest

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Merge
# MAGIC Delta Lake supports inserts, updates and deletes using **`MERGE`**.
# MAGIC
# MAGIC <strong><code>
# MAGIC MERGE INTO target a<br/>
# MAGIC USING source b<br/>
# MAGIC ON {merge_condition}<br/>
# MAGIC WHEN MATCHED THEN {matched_action}<br/>
# MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
# MAGIC </code></strong>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE table_updates(id INT, firstName STRING, middleName STRING, lastName STRING, gender STRING, birthDate STRING, ssn STRING, salary INT);
# MAGIC  INSERT INTO table_updates VALUES (9939998, 'John1234', '', 'Doe1115', 'M', '1978-01-14T04:00:00.000+000', '345-67-8921', 555033 );
# MAGIC  INSERT INTO table_updates VALUES (9994999, 'Jane445', '', 'Doe02222', 'F', '1981-06-25T04:00:00.000+000', '567-89-0113', 899053);
# MAGIC  INSERT INTO table_updates VALUES (200220119, 'Test556', '', 'Test2226', 'F', '1981-06-25T04:00:00.000+000', '567-89-0113', 89905);
# MAGIC  INSERT INTO table_updates VALUES (2010120, 'Yyyyydy', '', 'Prrrggf', 'F', '1981-06-25T04:00:00.000+000', '567-89-0113', 89901);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO person a
# MAGIC USING table_updates b
# MAGIC ON a.id = b.id
# MAGIC WHEN MATCHED AND a.salary IS NULL AND b.salary IS NOT NULL THEN
# MAGIC   UPDATE SET salary = b.salary
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE people(id INT,firstName STRING, middleName STRING, lastName STRING, gender STRING, birthDate STRING, ssn STRING, salary INT);
# MAGIC   
# MAGIC INSERT INTO people VALUES (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250);
# MAGIC INSERT INTO people VALUES (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500);
# MAGIC INSERT INTO people VALUES (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000);
# MAGIC
# MAGIC INSERT INTO people
# MAGIC VALUES 
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);
# MAGIC   
# MAGIC UPDATE people 
# MAGIC SET salary = salary + 1
# MAGIC WHERE middleName LIKE "T%";
# MAGIC
# MAGIC DELETE FROM people 
# MAGIC WHERE salary = 55250;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW updatesPeople(id, firstName, middleName, lastName, gender, birthDate, ssn, salary, type) AS VALUES
# MAGIC   (20000001, 'John123', '', 'Doe111', 'M', '1978-01-14T04:00:00.000+000', '345-67-8921', 555033, "update"),
# MAGIC   (20000002, 'Jane44', '', 'Doe0222', 'F', '1981-06-25T04:00:00.000+000', '567-89-0113', 899053, "delete"),
# MAGIC   (20000119, 'Test55', '', 'Test222', 'F', '1981-06-25T04:00:00.000+000', '567-89-0113', 89905, "insert"),
# MAGIC   (20000120, 'Yyyyyy', '', 'Prrrgg', 'F', '1981-06-25T04:00:00.000+000', '567-89-0113', 89901, "update");
# MAGIC   
# MAGIC MERGE INTO people b
# MAGIC USING updatesPeople u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL people

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY people

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/people'))

# COMMAND ----------

# MAGIC %md
# MAGIC Records in Delta Lake tables are stored as data in Parquet files.
# MAGIC Transactions to Delta Lake tables are recorded in the **`_delta_log`**.

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC Each transaction results in a new JSON file being written to the Delta Lake transaction log.

# COMMAND ----------

# MAGIC %python
# MAGIC display(spark.sql("SELECT * FROM json.`dbfs:/user/hive/warehouse/people/_delta_log/00000000000000000005.json`"))

# COMMAND ----------

# MAGIC %md
# MAGIC The **`add`** column contains a list of all the new files written to our table; the **`remove`** column indicates those files that no longer should be included in our table.
# MAGIC
# MAGIC When we query a Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current version, and ignores all other data files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compacting Small Files and Indexing for improved performance
# MAGIC Files will be combined toward an optimal size by using the **`OPTIMIZE`** command.
# MAGIC
# MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
# MAGIC
# MAGIC We can add **`ZORDER`** indexing to combine similar values within data files

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE people
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY people

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rollback Versions

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM people

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people VERSION AS OF 3

# COMMAND ----------

# MAGIC %md
# MAGIC **`RESTORE VERSION`** 

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE people TO VERSION AS OF 9

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY people

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removing Stale Files
# MAGIC If you wish to manually purge old data files, this can be performed with the **`VACUUM`** operation. **`VACUUM`** will prevent you from deleting files less than 7 days old. If you run **`VACUUM`** on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.Use the **`DRY RUN`** version of vacuum to print out all records to be deleted
# MAGIC
# MAGIC **`0 HOURS`** to keep only the current version:

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC
# MAGIC VACUUM people RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM people RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/people'))
