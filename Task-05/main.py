from datetime import datetime
from textwrap import dedent
import csv
from pathlib import Path
import psycopg2
from psycopg2 import sql


def load_from_csv(conn, file_path, row_map_func):
    table = file_path.stem
    cur = conn.cursor()
    with file_path.open('r') as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        cols = reader.fieldnames
        for row in reader:
            row = row_map_func(row)
            q = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                sql.Identifier(table),
                sql.SQL(',').join(map(sql.Identifier, cols)),
                sql.SQL(',').join(map(sql.Placeholder, cols))
            )
            # print(q.as_string(conn))
            cur.execute(q, row)
        conn.commit()


def create_schema(conn):
    cur = conn.cursor()
    # Accounts
    cur.execute(dedent('''
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id INT NOT NULL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            address_1 TEXT NOT NULL,
            address_2 TEXT,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            zip_code INT NOT NULL,
            join_date DATE NOT NULL )
    ''').strip())
    # Products
    cur.execute(dedent('''
        CREATE TABLE IF NOT EXISTS products (
            product_id INT NOT NULL PRIMARY KEY,
            product_code TEXT NOT NULL UNIQUE,
            product_description TEXT NOT NULL )
    ''').strip())
    # Transactions
    cur.execute(dedent('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT NOT NULL PRIMARY KEY,
            transaction_date DATE NOT NULL,
            product_id INT NOT NULL,
            product_code TEXT NOT NULL,
            product_description TEXT NOT NULL,
            quantity INT NOT NULL,
            account_id INT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts (customer_id),
            FOREIGN KEY (product_id) REFERENCES products (product_id) )
    ''').strip())
    # finalize schema creation
    conn.commit()


def main():
    # host = "postgres"
    # database = "postgres"
    # user = "postgres"
    # pas = "postgres"

    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    create_schema(conn)

    data_path = Path('data')

    def cast_attr(obj, attrs, map_func):
        for attr in attrs:
            obj[attr] = map_func(obj[attr])

    # Load accounts
    accounts_path = data_path / 'accounts.csv'

    def map_account(row):
        cast_attr(row, ('customer_id', 'zip_code'), int)
        row['address_2'] = row['address_2'] or None
        row['join_date'] = datetime.strptime(row['join_date'], '%Y/%m/%d').date()
        return row

    load_from_csv(conn, accounts_path, map_account)

    # Load products
    products_path = data_path / 'products.csv'

    def map_products(row):
        row['product_id'] = int(row['product_id'])
        return row

    load_from_csv(conn, products_path, map_products)

    # Load transactions
    tx_path = data_path / 'transactions.csv'

    def map_tx(row):
        row['transaction_date'] = datetime.strptime(row['transaction_date'], '%Y/%m/%d').date()
        cast_attr(row, ('product_id', 'quantity', 'account_id'), int)
        return row

    load_from_csv(conn, tx_path, map_tx)


if __name__ == "__main__":
    main()
