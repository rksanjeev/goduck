package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	var s3_prefix string = "s3://5b02238f-3466-44bc-8131-71576d473f97/"
	// Open database connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal("Error opening database connection: ", err)
	}
	defer db.Close()

	// Establish connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal("Error establishing database connection: ", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), `
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_region='us-east-1';
	`)

	if err != nil {
		log.Fatal("Error configuring S3: ", err)
	}

	// Enable profiling
	_, err = conn.ExecContext(context.Background(), `PRAGMA enable_profiling`)
	if err != nil {
		log.Fatal("Error enabling profiling: ", err)
	}
	_, err = conn.ExecContext(context.Background(), `PRAGMA profiling_mode = 'detailed'`)
	if err != nil {
		log.Fatal("Error setting profiling mode: ", err)
	}

	// Corrected query with proper JOINs
	query := `CREATE TABLE result_table AS
SELECT
    CAST(o.customer_id AS VARCHAR) AS customer_id,
    LIST(
        STRUCT_PACK(
            productid := oi.product_id,
            product_name := p.product_name,
            quantity := oi.quantity,
            unit_price := oi.unit_price
        )
    ) AS ordered
FROM '` + s3_prefix + `data/parquet/orders.parquet' o
JOIN '` + s3_prefix + `data/parquet/order_items.parquet' oi
    ON o.order_id = oi.order_id
JOIN '` + s3_prefix + `data/parquet/products.parquet' p
    ON oi.product_id = p.product_id
GROUP BY o.customer_id;

COPY result_table
TO '` + s3_prefix + `data/result/result.json'
(ARRAY);



`

	// Execute query and get result
	_, err = conn.ExecContext(context.Background(), query)
	if err != nil {
		log.Fatal("Error executing query: ", err)
	}
	//	defer res.Close()

	// Process results (for demonstration, let's just print out the rows)
	//	for res.Next() {
	//		var orderID, customerID, productID, quantity int
	//		var unitPrice float64
	//		var productName string
	//		err = res.Scan(&orderID, &customerID, &productID, &unitPrice, &quantity, &productName)
	//		if err != nil {
	//			log.Fatal("Error scanning result: ", err)
	//		}
	//		fmt.Printf("Order ID: %d, Customer ID: %d, Product ID: %d, Unit Price: %f, Quantity: %d, Product Name: %s\n", orderID, customerID, productID, unitPrice, quantity, productName)
	//	}

	// Disable profiling
	_, err = conn.ExecContext(context.Background(), `PRAGMA disable_profiling`)
	if err != nil {
		log.Fatal("Error disabling profiling: ", err)
	}
}
