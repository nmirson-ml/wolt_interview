# Wolt Snack Store Interview - Data Modelling 


This model is going to be loaded using SQLMesh portable data stack project:

Which is based on the following project. 
- [Portable Data Stack with SQLMesh](https://github.com/cnstlungu/portable-data-stack-sqlmesh) 

# Data model

![Data Model](resources/data_model.png) 

## Layers

- `raw` unrefined input data
- `staging` staging area
- `core` curated data

### Core
#### Dimensions
- dim_customers
- dim_items
- dim_promotions
- dim_time

#### Facts
- fact_purchases
- fact_item_sales

### Staging
- staging_item_logs
- staging_purchase_logs
- staging_promotions

### Raw
- raw_item_logs
- raw_purchase_logs
- raw_promotions


## Source Data
The project uses the following seed data:

### Items Log
Contains historical item data with the following key fields:
- LOG_ITEM_ID: Unique log identifier
- ITEM_KEY: Unique identifier for the item
- TIME_LOG_CREATED_UTC: Timestamp for log creation
- PAYLOAD: JSON field containing item details

### Promotions
Tracks promotional activities:
- PROMO_START_DATE: Start date of the promotion
- PROMO_END_DATE: End date of the promotion
- ITEM_KEY: Unique identifier for the item
- PROMO_TYPE: Type of the promotion
- DISCOUNT_IN_PERCENTAGE: Discount percentage

### Purchase Logs
Records customer purchases:
- TIME_ORDER_RECEIVED_UTC: Order timestamp
- PURCHASE_KEY: Primary key for the purchase
- CUSTOMER_KEY: Customer identifier
- DELIVERY_DISTANCE_LINE_METERS: Delivery distance
- WOLT_SERVICE_FEE: Service fee
- COURIER_BASE_FEE: Base delivery fee
- TOTAL_BASKET_VALUE: Total order value
- ITEM_BASKET_DESCRIPTION: JSON array of items and quantities


## Setting up the project

1. Rename `.env.example` to `.env`. This will contain relative paths for the database file (datamart.duckdb) and parquet input files

2. Rename `shared\db\datamart.duckdb.example` to `shared\db\datamart.duckdb` or initiate an empty database there with the same name.

3. Create a Python Virtual Environment

`python3 -m venv .venv`

4. Add environment variables to the virtual environment

`cat .env >> .venv/Scripts/activate`

If Mac, use `cat .env >> .venv/bin/activate`


5. Activate the Python venv

`source .venv/bin/activate`

6. Install the required packages

`pip install -r requirements.txt`


## Running the dbt model

1. Ensure the virtual environment is activated

`source .venv/bin/activate`

2. Make sure you are located in the directory  `wolt_data_model`

`cd wolt_data_model`

3. Run `dbt deps` to install dependencies

4. Run `dbt seed` to import the seed (static) data

5. Run `dbt compile` to compile the project

6. Run `dbt run` to run the models

7. Run `dbt test` to run the tests
