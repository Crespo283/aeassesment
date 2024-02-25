# Assesment

This assesment has been conducted using Google Cloud Storage, Databricks, DBT and PowerBi. It is true that for this kind of data, Databricks is not the most recomendable solution, but since I was using the 90 day trial period in Google Cloud for a personal project I considered it fitting. 

As I encountered some issues to fix during the assesment, this document will be separated by two main points. These data issues and how were they solved using the different tools mentioned avobe, and an explanation of the PowerBi dashboard, rather than the main objectives themselves. 

## Google Cloud Storage:

Google Cloud Storage is a scalable, fully-managed, and highly reliable object storage service provided by Google Cloud Platform, and in this specific case, used to store the different csv files alongside their folders. The main files, such as ApplicationData and others were stored in the main folder, while the files inside a folder, were uploaded inside a subfolder with the same name as the original folder.

Once the files were uploaded, a connection to databricks was created, using a Google Service account and other configurations such as Host Name or Http Path. 

## Databricks:

Once in databricks, a notebook with the initial purpose of the ingestion was created. Since the connection was already stablished, the files were able to be accessed with a simple path, which was stored in a variable. While reading the csv files and transforming them into dataframes using PySpark, the first issue was found, some of the files didn't have column names, and databricks file system was recognizing the first row as the column names of the files, as they are csv. To solve this, the first idea was to create an schema instead of using an inferschema parameter while creating the dataframe, as it would let me modify the names of the columns as well as defining the id as an integer. That would make us lose one row of data though, so we created a new data frame, defining its values and using the custom schema. This row of data would later be appended to the existing dataframe and the dataframe saved in a delta table in the Hive Metastore.

As this process was to be made several times, and some of the files had more than a docen of fields, it was decided to create a secondary notebook with a function that would automatice this process. This new function would use the inferschema for a sample of the data creating a sample dataframe, creating a new schema in the function for the input, then reading the data into a dataframe, changing the the name of the fields in the process, addin the new schema and creating the new row to append after, returning a new dataframe. This function would be used for all the files except the ApplicationInformation file and all the files inside the folders.

It is the ApplicationInformation information file the one that confirms the suspicion that the number values of the other files are actually ids and that the names  of the files in the folders are the same ids too. Considering this, instead of using the functions, the read options during the creation of the dataframe were modified to read all the files of each folder into the same dataframe, creating a new column with the name of the file, in this case, a new column with the ids, the original name of each file, as a value. Once the dataframe was created, it was saved as a delta table in the Hive Metastore.

## DBT

Once DBT was connected to Databricks using the same configuration as the conection with Google Cloud Storage, DBT had access to the delta tables saved in the Hive Metastore. In order to have a tidy and ordered project 4 folders were created inside the folder "models": 1.source, 2.staging, 3.intermediate, 4.marts. The idea behind these folders were to have source as the main entrance of the data into DBT to be referred after, to have the staging folder for models which would select the necessary  columns, rename them or transform them if necessary, the intermediate folder to make all the necessary transformations and joins, and finally the marts, the general models able to be used in different cases and the ones which would be imported into PowerBi. 

The source models are nothing more than the access of the saved delta tables into DBT. Once the source models were created, the staging models came next. The staging focus on changing the name of the columns if necessary and transforming them. The id field name was changed to appid in most cases, to follow the convention of the ApplicationInformation data. Since some of the tables had repeated columns with necessary data, such as the languages, the genres or the tags, all these columns with the same data were unified into one. At first there were used several CTE to UNION ALL the data, but after the first staging model, it was considered best to create a macro to automatize this process.

In the intermediate models, the data was cleaned, taking out all the unnecessary nulls, and avoiding duplicates using GROUP BY and SELECT DISTINCT, finally joining the data into two big models. In the marts folder exist the two final models, a fact model, and a dimensional model, just a SELECT of the necessary cleaned columns from the two big intermediate models. It was decided to have two final models instead of one to, first make them more general and usable, and second to not create huge tables with millions of rows.

Once the models of each folder were created, the tables were built to follow a lineagolly within the tool.

## PowerBi

