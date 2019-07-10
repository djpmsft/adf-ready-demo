# Movies Analytics using Azure Data Factory

*Get One liner from Anand*

*If you're new to Azure Data Factory, see [Introduction to Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/introduction).*

In this Lab, we will utilize Azure Data Factory's visual authoring experience to create a pipeline that copies movie data stored in Amazon S3 to Azure Data Lake Storage Gen2 and then executes a Mapping Data Flow to transform and write to a SQL Data Warehouse.

## Prerequisites

* **Azure subscription**: If you don't have an Azure subscription, create a [free account](https://azure.microsoft.com/free/) before you begin.
* **Azure Data Lake Storage Gen2 storage account**: If you don't have an ADLS Gen2 storage account, see the instructions in [Create an ADLS Gen2 storage account](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account).
* **Azure SQL Data Warehouse** If you don't have an Azure SQL DW, see the instructions in [Create an SQL DW](https://docs.microsoft.com/en-us/azure/sql-data-warehouse/create-data-warehouse-portal)
* **Amazon S3 storage account** Used as our initial data source in our copy activity

## Setting up your environment

* **Create your data factory:** Use the [Azure Portal](https://portal.azure.com) to create your Data Factory. Detailed instructions can be found at [Create a Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal)
  * Mapping Data Flow is not currently available in the following data factory regions: West Central US, Brazil South and France Central
* **Create your linked services** You will need to create four linked services for this lab
  * [ADLS Gen2](https://docs.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage)
  * [Azure Blob Storage](https://docs.microsoft.com/en-us/azure/data-factory/connector-azure-blob-storage)
  * [Aure SQL DW](https://docs.microsoft.com/en-us/azure/data-factory/connector-azure-sql-data-warehouse)
  * [Amazon S3](https://docs.microsoft.com/en-us/azure/data-factory/connector-amazon-simple-storage-service)
  
  ## Copy Data From Amazon S3 to Azure Data Lake Storage Gen2
  
  First we use a Copy Activity to move 
