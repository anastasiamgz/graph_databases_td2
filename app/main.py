import os
from fastapi import FastAPI
from neo4j import GraphDatabase

app = FastAPI(title="E-Commerce Recommendations API")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = None

@app.on_event("startup")
async def startup_event():
    global driver
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    print("✓ Connected to Neo4j")

@app.on_event("shutdown")
async def shutdown_event():
    if driver:
        driver.close()
    print("✓ Closed Neo4j connection")

@app.get("/health")
async def health():
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 as health")
            result.single()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/customers")
async def get_customers():
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer)
            RETURN c.id as id, c.name as name, c.join_date as join_date
            ORDER BY c.name
        """)
        customers = [dict(record) for record in result]
    return {"customers": customers}

@app.get("/products")
async def get_products():
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
            RETURN p.id as id, p.name as name, p.price as price, 
                   cat.name as category
            ORDER BY p.name
        """)
        products = [dict(record) for record in result]
    return {"products": products}

@app.get("/recommendations/collaborative/{customer_id}")
async def collaborative_recommendations(customer_id: str, limit: int = 5):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer {id: $customer_id})-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
            WITH c, collect(DISTINCT p) as customer_products
            MATCH (other:Customer)-[:PLACED]->(o2:Order)-[:CONTAINS]->(p2:Product)
            WHERE other.id <> c.id AND p2 IN customer_products
            WITH c, customer_products, other, count(DISTINCT p2) as common_products
            ORDER BY common_products DESC
            LIMIT 10
            MATCH (other)-[:PLACED]->(o3:Order)-[:CONTAINS]->(rec:Product)
            WHERE NOT rec IN customer_products
            WITH rec, count(DISTINCT other) as popularity
            RETURN rec.id as product_id, rec.name as product_name, 
                   rec.price as price, popularity
            ORDER BY popularity DESC, rec.price ASC
            LIMIT $limit
        """, {"customer_id": customer_id, "limit": limit})
        recommendations = [dict(record) for record in result]
    return {"customer_id": customer_id, "strategy": "collaborative_filtering", "recommendations": recommendations}

@app.get("/recommendations/popular")
async def popular_products(limit: int = 5):
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product)<-[:CONTAINS]-(o:Order)
            WITH p, count(o) as order_count
            RETURN p.id as product_id, p.name as product_name, 
                   p.price as price, order_count
            ORDER BY order_count DESC, p.price ASC
            LIMIT $limit
        """, {"limit": limit})
        recommendations = [dict(record) for record in result]
    return {"strategy": "popular_products", "recommendations": recommendations}

#----------

@app.get("/recommendations/content/{product_id}")
async def content_based_recommendations(product_id: str, limit: int = 5):
    """Content-based filtering: recommend products in the same category."""
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product {id: $product_id})-[:IN_CATEGORY]->(cat:Category)
            MATCH (rec:Product)-[:IN_CATEGORY]->(cat)
            WHERE rec.id <> p.id
            OPTIONAL MATCH (rec)<-[:CONTAINS]-(o:Order)
            WITH rec, count(o) as popularity
            RETURN rec.id as product_id, rec.name as product_name, 
                   rec.price as price, popularity
            ORDER BY popularity DESC, rec.price ASC
            LIMIT $limit
        """, {"product_id": product_id, "limit": limit})
        recommendations = [dict(record) for record in result]
    return {"product_id": product_id, "strategy": "content_based", "recommendations": recommendations}

@app.get("/recommendations/co-purchase/{product_id}")
async def co_purchase_recommendations(product_id: str, limit: int = 5):
    """Co-occurrence based: products frequently bought together."""
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product {id: $product_id})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(rec:Product)
            WHERE rec.id <> p.id
            WITH rec, count(o) as co_purchase_count
            RETURN rec.id as product_id, rec.name as product_name, 
                   rec.price as price, co_purchase_count
            ORDER BY co_purchase_count DESC, rec.price ASC
            LIMIT $limit
        """, {"product_id": product_id, "limit": limit})
        recommendations = [dict(record) for record in result]
    return {"product_id": product_id, "strategy": "co_purchase", "recommendations": recommendations}

@app.get("/analytics/customer-journey/{customer_id}")
async def customer_journey(customer_id: str):
    """Analyze a customer's journey: views, clicks, cart additions, and purchases."""
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer {id: $customer_id})
            OPTIONAL MATCH (c)-[v:VIEWED]->(viewed:Product)
            OPTIONAL MATCH (c)-[cl:CLICKED]->(clicked:Product)
            OPTIONAL MATCH (c)-[a:ADDED_TO_CART]->(added:Product)
            OPTIONAL MATCH (c)-[:PLACED]->(o:Order)-[:CONTAINS]->(purchased:Product)
            
            RETURN c.name as customer_name,
                   count(DISTINCT viewed) as views,
                   count(DISTINCT clicked) as clicks,
                   count(DISTINCT added) as cart_additions,
                   count(DISTINCT purchased) as purchases
        """, {"customer_id": customer_id})
        journey = dict(result.single())
    return {"customer_id": customer_id, "journey": journey}

#------

@app.get("/stats")
async def get_stats():
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer) WITH count(c) as customers
            MATCH (p:Product) WITH customers, count(p) as products
            MATCH (o:Order) WITH customers, products, count(o) as orders
            MATCH (cat:Category) WITH customers, products, orders, count(cat) as categories
            MATCH ()-[r]->() WITH customers, products, orders, categories, count(r) as relationships
            RETURN customers, products, orders, categories, relationships
        """)
        stats = dict(result.single())
    return {"stats": stats}