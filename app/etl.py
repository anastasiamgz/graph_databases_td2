import os
import time
from pathlib import Path
import psycopg2
from neo4j import GraphDatabase
import pandas as pd

# Environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "app")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "shop")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def wait_for_postgres(max_retries=30, delay=2):
    """Wait for PostgreSQL to be ready."""
    print("Waiting for PostgreSQL...")
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB
            )
            conn.close()
            print("✓ PostgreSQL is ready")
            return
        except psycopg2.OperationalError:
            if i < max_retries - 1:
                time.sleep(delay)
            else:
                raise Exception("PostgreSQL not ready after maximum retries")


def wait_for_neo4j(max_retries=30, delay=2):
    """Wait for Neo4j to be ready."""
    print("Waiting for Neo4j...")
    for i in range(max_retries):
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            print("✓ Neo4j is ready")
            return
        except Exception as e:
            if i < max_retries - 1:
                time.sleep(delay)
            else:
                raise Exception(f"Neo4j not ready after maximum retries: {e}")


def run_cypher(driver, query, parameters=None):
    """Execute a single Cypher query."""
    with driver.session() as session:
        result = session.run(query, parameters or {})
        return result.consume()


def run_cypher_file(driver, filepath):
    """Execute multiple Cypher statements from a file."""
    print(f"Running Cypher file: {filepath}")
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Split by semicolons and filter empty statements
    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    
    for stmt in statements:
        if stmt:
            try:
                run_cypher(driver, stmt)
                print(f"✓ Executed: {stmt[:50]}...")
            except Exception as e:
                print(f"✗ Error executing statement: {e}")
                print(f"  Statement: {stmt[:100]}")


def chunk(df, chunk_size=100):
    """Split DataFrame into chunks for batch processing."""
    for i in range(0, len(df), chunk_size):
        yield df[i:i + chunk_size]


def etl():
    """
    Main ETL function that migrates data from PostgreSQL to Neo4j.
    """
    wait_for_postgres()
    wait_for_neo4j()

    queries_path = Path(__file__).with_name("queries.cypher")

    print("\n=== Connecting to databases ===")
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB
    )
    
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        print("\n=== Setting up Neo4j schema ===")
        if queries_path.exists():
            run_cypher_file(neo4j_driver, queries_path)
        else:
            print(f"Warning: {queries_path} not found, skipping schema setup")

        print("\n=== Extracting data from PostgreSQL ===")
        
        customers_df = pd.read_sql("SELECT * FROM customers", pg_conn)
        categories_df = pd.read_sql("SELECT * FROM categories", pg_conn)
        products_df = pd.read_sql("SELECT * FROM products", pg_conn)
        orders_df = pd.read_sql("SELECT * FROM orders", pg_conn)
        order_items_df = pd.read_sql("SELECT * FROM order_items", pg_conn)
        events_df = pd.read_sql("SELECT * FROM events", pg_conn)
        
        print(f"✓ Extracted {len(customers_df)} customers")
        print(f"✓ Extracted {len(categories_df)} categories")
        print(f"✓ Extracted {len(products_df)} products")
        print(f"✓ Extracted {len(orders_df)} orders")
        print(f"✓ Extracted {len(order_items_df)} order items")
        print(f"✓ Extracted {len(events_df)} events")

        print("\n=== Loading data into Neo4j ===")
        
        print("Loading categories...")
        for _, row in categories_df.iterrows():
            query = """
            MERGE (cat:Category {id: $id})
            SET cat.name = $name
            """
            run_cypher(neo4j_driver, query, {
                'id': row['id'],
                'name': row['name']
            })
        print(f"✓ Loaded {len(categories_df)} categories")

        print("Loading products...")
        for _, row in products_df.iterrows():
            query = """
            MERGE (p:Product {id: $id})
            SET p.name = $name, p.price = $price
            WITH p
            MATCH (cat:Category {id: $category_id})
            MERGE (p)-[:IN_CATEGORY]->(cat)
            """
            run_cypher(neo4j_driver, query, {
                'id': row['id'],
                'name': row['name'],
                'price': float(row['price']),
                'category_id': row['category_id']
            })
        print(f"✓ Loaded {len(products_df)} products")

        print("Loading customers...")
        for _, row in customers_df.iterrows():
            query = """
            MERGE (c:Customer {id: $id})
            SET c.name = $name, c.join_date = date($join_date)
            """
            run_cypher(neo4j_driver, query, {
                'id': row['id'],
                'name': row['name'],
                'join_date': str(row['join_date'])
            })
        print(f"✓ Loaded {len(customers_df)} customers")

        print("Loading orders...")
        for _, row in orders_df.iterrows():
            # Convert timestamp to ISO format!!!!!!!!!!!
            ts_str = row['ts'].isoformat() if hasattr(row['ts'], 'isoformat') else str(row['ts'])
            query = """
            MERGE (o:Order {id: $id})
            SET o.ts = datetime($ts)
            WITH o
            MATCH (c:Customer {id: $customer_id})
            MERGE (c)-[:PLACED]->(o)
            """
            run_cypher(neo4j_driver, query, {
                'id': row['id'],
                'customer_id': row['customer_id'],
                'ts': ts_str
            })
        print(f"✓ Loaded {len(orders_df)} orders")

        print("Loading order items...")
        for _, row in order_items_df.iterrows():
            query = """
            MATCH (o:Order {id: $order_id})
            MATCH (p:Product {id: $product_id})
            MERGE (o)-[r:CONTAINS]->(p)
            SET r.quantity = $quantity
            """
            run_cypher(neo4j_driver, query, {
                'order_id': row['order_id'],
                'product_id': row['product_id'],
                'quantity': int(row['quantity'])
            })
        print(f"✓ Loaded {len(order_items_df)} order items")

        print("Loading events...")
        event_type_map = {
            'view': 'VIEWED',
            'click': 'CLICKED',
            'add_to_cart': 'ADDED_TO_CART'
        }
        
        for _, row in events_df.iterrows():
            rel_type = event_type_map.get(row['event_type'], 'INTERACTED')
            # Convert timestamp to ISO format
            ts_str = row['ts'].isoformat() if hasattr(row['ts'], 'isoformat') else str(row['ts'])
            query = f"""
            MATCH (c:Customer {{id: $customer_id}})
            MATCH (p:Product {{id: $product_id}})
            CREATE (c)-[r:{rel_type}]->(p)
            SET r.ts = datetime($ts), r.event_id = $event_id
            """
            run_cypher(neo4j_driver, query, {
                'customer_id': row['customer_id'],
                'product_id': row['product_id'],
                'ts': ts_str,
                'event_id': row['id']
            })
        print(f"✓ Loaded {len(events_df)} events")

        print("\n=== ETL Complete ===")
        print("ETL done.")

    finally:
        pg_conn.close()
        neo4j_driver.close()


if __name__ == "__main__":
    etl()