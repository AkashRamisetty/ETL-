# importing the  libraries
import pandas as pd
import sqlite3 as db
from sqlalchemy import create_engine


def run_query():
    conn = db.connect('S30 ETL Assignment.db')  # Connecting to the respective DB
    cn = conn.cursor()  # To execute commands for DB
    cn.execute(
        "SELECT customers.customer_id as Customer,customers.age as Age, items.item_name as Item,CAST(sum( orders.quantity)as INT)as "
        "Quantity from customers join sales on customers.customer_id=sales.customer_id join orders on "
        "orders.sales_id=sales.sales_id join items on items.item_id=orders.item_id where customers.age between "
        "18 and 35 and quantity is not null group by customers.customer_id,customers.age, items.item_name "
        "order by customers.customer_id")  # SQL query to achieve the desired view
    return cn.fetchall()  # To fetch all rows of query result


def run_query_pandas():
    sql = "SELECT customers.customer_id as Customer,customers.age as Age, items.item_name as Item,CAST(sum( orders.quantity)as INT)as  Quantity " \
          "from customers join sales on customers.customer_id=sales.customer_id join orders on " \
          "orders.sales_id=sales.sales_id join items on items.item_id=orders.item_id where customers.age between  18 " \
          "and 35 and quantity is not null group by customers.customer_id,customers.age, items.item_name order by " \
          "customers.customer_id "
    cnx = create_engine(
        'sqlite:///S30 ETL Assignment.db').connect()  # Creates the connection to the DB to use it for pandas
    df = pd.read_sql_query(sql, cnx)  # To read SQL query of database into dataframe
    df.to_csv('MarketingStrategyAgeGroup.csv', sep=';')  # Storing the query to CSV file with a delimiter
    return df


if __name__ == '__main__':
    print(run_query_pandas())  # To exhibit the desired final view
