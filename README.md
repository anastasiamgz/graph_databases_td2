# graph_database_tp1

# Structure:

.
├── docker-compose.yml
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
└── README.md         # your documentation for this project at the root of your git repo 