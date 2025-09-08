!pip install snowflake-connector-python

import json
import random
import datetime
import time
import kagglehub
import pandas as pd
import os
import snowflake.connector

# Download dataset

path = kagglehub.dataset_download("dataset link")

print("Path to dataset files:", path)


# List files in the dataset directory
files = os.listdir(path)
# print("Files:", files)

# Pick the correct CSV file (example: "shopify_products.csv" or similar)
csv_file = [f for f in files if f.endswith(".csv")][0]

# Read the CSV
p = pd.read_csv(os.path.join(path, csv_file))
p = p[['Product_ID','Product_Name','Category','Price']]

p
# p.head()
# p.info()
p =p.values.tolist()

conn = snowflake.connector.connect(
    # include user, password, account, warehouse, database, schema names

    user="",
    password="",
    account="",   # e.g. abcde-xy12345.snowflakecomputing.com
    warehouse="",
    database="",
    schema=""
)
cs = conn.cursor()

class SalesDataGenerator:
    def __init__(self):
        self.products = p
        self.regions = ["North America", "Europe", "Asia Pacific","South America", "Australia", "Africa", "Middle East", "South Asia"]

    def generate_sale(self):
        product = random.choice(self.products)
        x = random.randint(1,3)
        return {
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "quantity": x,
            "unit_price": float(product[3]),
            "total_amount": float(product[3] * x),
            "region": random.choice(self.regions),
            "order_timestamp": datetime.datetime.utcnow().isoformat()
        }

    def run_simulation(self, duration_minutes=60):
        # Generate and upload sales data every 30-60 seconds
        end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=duration_minutes)

        while datetime.datetime.utcnow() < end_time:
            # Generate 1-5 sales records
            batch = [self.generate_sale() for _ in range(random.randint(20,30))]

            timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"sales_{timestamp}.json"
            with open(filename, "w") as f:
              json.dump(batch, f, indent=4)
            put_sql = f"PUT file://{filename} @INTERNAL_STAGE AUTO_COMPRESS=TRUE"
            cs.execute(put_sql)
            print(f"Generated {len(batch)} sales records in {filename}")
            print(batch)
            # # Wait 30-60 seconds before next batch
            time.sleep(random.randint(5,10))

def main():
    gen = SalesDataGenerator();
    print(gen.run_simulation(60))
main()
