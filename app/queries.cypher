CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT category_id IF NOT EXISTS FOR (cat:Category) REQUIRE cat.id IS UNIQUE;
CREATE INDEX customer_name IF NOT EXISTS FOR (c:Customer) ON (c.name);
CREATE INDEX product_name IF NOT EXISTS FOR (p:Product) ON (p.name);
CREATE INDEX product_price IF NOT EXISTS FOR (p:Product) ON (p.price);