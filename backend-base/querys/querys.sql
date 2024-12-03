-- name: GetOrder :one
SELECT * FROM Orders
WHERE id = $1 LIMIT 1;

-- name: ListOrders :many
SELECT * FROM Orders
ORDER BY id;

-- name: CreateOrder :one
INSERT INTO Orders (
  data
) VALUES (
  $1
)
RETURNING *;

-- name: UpdateOrder :exec
UPDATE Orders
  set data = $2
WHERE id = $1;

-- name: DeleteOrder :exec
DELETE FROM Orders
WHERE id = $1;