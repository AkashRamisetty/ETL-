# Importing Libraries
import pandas as pd
import sqlite3 as db

# Connecting to the DB and accessing it
conn = db.connect('S30 ETL Assignment.db')
cn = conn.cursor()

# Converting all tables to Pandas DataFrames
df_customers = pd.read_sql_query("select * from customers", conn)
items = pd.read_sql_query("select * from items", conn)
df_orders = pd.read_sql_query("select * from orders", conn).dropna()
sales = pd.read_sql_query("select * from sales", conn)

# Manipulating the data through Pandas by applying conditions
customers = df_customers.query("18<=age<=35")
orders = df_orders.dropna()

# Join the tables

join_1 = pd.merge(customers, sales, on='customer_id', how='left')
join_2 = pd.merge(join_1, sales, on='sales_id', how='left')
join_3 = pd.merge(join_2, orders, on='sales_id', how='left')
join_4 = pd.merge(join_3, items, on='item_id', how='inner')

# Get the Final View

view_1 = join_4.drop(['sales_id', 'customer_id_y', 'order_id', 'item_id'], axis=1)
view_2 = view_1.rename(columns={'customer_id_x': 'Customer', 'age': 'Age', 'item_name': 'Item', 'quantity': 'Quantity'})
view_3 = view_2.groupby(['Customer', 'Age', 'Item']).sum('Quantity')
final_view = view_3['Quantity'].astype('int64')

# Get Into CSV
final_view.to_csv('Pandas.csv', sep=';')


# Print the Final View
print(final_view)
