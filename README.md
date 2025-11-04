# graph_database_tp1

# Structure:

.
├── docker-compose.yml
├── api_test_results.txt    # 
├── postgres/
│   └── init/
│       ├── 01_schema.sql
│       ├── 02_seed.sql
├── neo4j/
│   ├── data/         # persisted DB files
│   └── import/       # CSVs for direct Neo4j LOAD CSV 
├── app/
│   ├── main.py       # FastAPI app
│   ├── etl.py        # ETL from Postgres -> Neo4j
│   ├── queries.cypher
│   ├── start.sh      # starter script for running the uvicorn server 
│   ├── requirements.txt # the libs you need 
└── README.md         # documnetation + **answers to the open-ended questions**

This project implements a complete e-commerce recommendation engine demonstrating the practical application of graph databases in production systems. The architecture showcases:

* ETL Pipeline: Automated data migration from relational (PostgreSQL) to graph (Neo4j) databases
* Graph Analytics: Four distinct recommendation strategies leveraging graph relationships
* REST API: FastAPI service exposing 10 endpoints for recommendations and analytics
* Production Patterns: Docker containerization, health checks, and automated testing

## Architecture of the ETL pipeline:

1. Wait for Dependencies - Ensures PostgreSQL and Neo4j are ready
2. Schema Setup - Creates constraints and indexes in Neo4j
3. Extract - Reads all tables from PostgreSQL using pandas
4. Transform - Converts relational rows to graph nodes/relationships
5. Load - Writes to Neo4j using parameterized Cypher queries

## Which recommendation strategies can I implement?

This system implements four graph-based recommendation strategies:

* Collaborative Filtering:leverages customer similarity by traversing purchase patterns. It finds customers who bought similar products and recommends their additional purchases
* Content-Based Filtering: uses product categories to recommend similar items through IN_CATEGORY relationships. When a customer views electronics, it suggests other electronics, working well for new users with minimal purchase history.
* Co-Purchase Analysis : identifies products frequently bought together in the same order using CONTAINS relationships. This enables "frequently bought together" bundles for cross-selling opportunities.
* Popularity-Based ranks products by order frequency, useful for trending sections and cold-start scenarios when user data is unavailable.

## What improvements would make this production-ready?

#### Scalability & Performance:

* add connection pooling for PostgreSQL and Neo4j to handle concurrent users
* pre-compute recommendations for popular items
* Use incremental ETL with Change Data Capture instead of full database refresh

#### Monitoring:

* tracking API latency, error rates, and recommendation quality
* Set up automated testing (unit, integration, load tests) with CI/CD pipeline

#### Security:

* Add HTTPS/TLS encryption and input validation to prevent injection attacks
