// Command fakewms impersonates a customer Warehouse Management System for
// local demos and integration testing. The full publisher/subscriber harness is
// implemented in step 5 of the build.
package main

import (
	"flag"
	"fmt"
)

func main() {
	orders := flag.Int("orders", 100, "number of work orders to send")
	flag.Parse()
	fmt.Printf("fakewms harness scaffold: would send %d orders (full harness lands in step 5)\n", *orders)
}
