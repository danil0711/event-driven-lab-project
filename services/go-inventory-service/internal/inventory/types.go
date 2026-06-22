package inventory

import "fmt"

type PaymentType string

const (
	PaymentSuccess PaymentType = "payment_success"
	PaymentFailed  PaymentType = "payment_failed"
)

type InventoryResponseType string

const (
	InventorySkipped  InventoryResponseType = "inventory_skipped"
	InventoryFailed   InventoryResponseType = "inventory_failed"
	InventoryReserved InventoryResponseType = "inventory_reserved"
)

type OrderItem struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

type PaymentEvent struct {
	EventID string      `json:"event_id"`
	OrderID int         `json:"order_id"`
	Type    PaymentType `json:"type"`
	Reason  *string     `json:"reason"`
	Items   []OrderItem `json:"items"`
}

type InventoryResponse struct {
	EventID   string
	Type      InventoryResponseType
	OrderID   int
	Reason    *string
	ProductID *int
}

func (p PaymentType) Validate() error {
	switch p {
	case PaymentSuccess, PaymentFailed:
		return nil
	default:
		return fmt.Errorf("invalid payment type: %s", p)
	}
}

func (i InventoryResponseType) IsValid() error {
	switch i {
	case InventorySkipped,
		InventoryFailed,
		InventoryReserved:
		return nil
	default:
		return fmt.Errorf("invalid response type: %s", i)
	}
}
