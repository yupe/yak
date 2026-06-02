package entity

type Order struct {
	ID    string  `json:"order_id"`
	Price float64 `json:"price"`
}
