-- ingesting raw sales.csv to bronze.sales_pl table
create streaming table sales_pl as 
select *, current_timestamp as ingestion_date, _metadata.file_path as path from stream read_files("/Volumes/dev/naval/raw/pipeline/sales/");


create streaming table customers_pl as 
select *, current_timestamp as ingestion_date, _metadata.file_path as path from stream read_files("/Volumes/dev/naval/raw/pipeline/customers/");


create streaming table products_pl as 
select *, current_timestamp as ingestion_date, _metadata.file_path as path from stream read_files("/Volumes/dev/naval/raw/pipeline/products/");