package dbcontroller

import (
	"context"
	"log"
	"reflect"

	"github.com/jackc/pgx/v5"
	db "wb.ru/sevices/orders/backend-base/db/generate"
)

func TestDb() error {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://Ruslan:123456@localhost:5432/wb_orders?sslmode=disable")
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	queries := db.New(conn)

	// list all orders
	orders, err := queries.ListOrders(ctx)
	if err != nil {
		return err
	}
	log.Println(orders)

	// create an order
	insertedOrder, err := queries.CreateOrder(ctx, []byte(`{}`))
	if err != nil {
		return err
	}
	log.Println(insertedOrder)

	// get the order we just inserted
	fetchedAuthor, err := queries.GetOrder(ctx, insertedOrder.ID)
	if err != nil {
		return err
	}

	// prints true
	log.Println(reflect.DeepEqual(insertedOrder, fetchedAuthor))
	return nil
}
