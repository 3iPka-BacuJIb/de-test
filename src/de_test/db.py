import duckdb
db = duckdb.connect()
db.execute("CREATE VIEW taxpayers AS SELECT * FROM parquet_scan('./data/processed.parquet')")
db.sql("select * from taxpayers limit 10").show()
db.close()