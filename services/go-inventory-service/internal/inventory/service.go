package inventory

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	db *pgxpool.Pool
}

var Stock = map[int]int{
	10: 50,
	20: 100,
}

func NewService(db *pgxpool.Pool) *Service {
	return &Service{
		db: db,
	}
}

func (s *Service) HandleBusiness(
	ctx context.Context,
	tx pgx.Tx,
	event PaymentEvent,
) (*InventoryResponse, error) {

	isNew, err := s.claimEvent(ctx, tx, event.EventID)
	if err != nil {
		log.Printf("Ошибка БД: %v", err)
		return nil, err
	}

	if !isNew {
		log.Printf("Дубликат event: %s", event.EventID)
		return nil, nil
	}

	response := s.handle(event)

	if err := s.writeOutbox(ctx, tx, response); err != nil {
		return nil, err
	}

	return &response, nil

}

func (s *Service) handle(event PaymentEvent) InventoryResponse {
	log.Printf(
		"processing event_id=%s order_id=%d",
		event.EventID,
		event.OrderID,
	)

	if err := event.Type.Validate(); err != nil {
		log.Printf("invalid event type: %v", err)
		return InventoryResponse{
			EventID: event.EventID,
			Type:    InventoryFailed,
			OrderID: event.OrderID,
		}
	}

	if event.Type != PaymentSuccess {
		log.Printf("Проверка склада пропущена, платеж неуспешен")

		return InventoryResponse{
			EventID: event.EventID,
			Type:    InventorySkipped,
			OrderID: event.OrderID,
		}
	}

	for _, item := range event.Items {
		available, exists := Stock[item.ProductID]

		if !exists {
			log.Printf("Товар не найден. product_id=%d", item.ProductID)

			reason := "product_not_found"
			productID := item.ProductID

			return InventoryResponse{
				EventID:   event.EventID,
				Type:      InventoryFailed,
				OrderID:   event.OrderID,
				Reason:    &reason,
				ProductID: &productID,
			}
		}

		if available < item.Quantity {
			log.Printf(
				"Недостаточно товара на складе. product_id=%d available=%d requested=%d",
				item.ProductID,
				available,
				item.Quantity,
			)
			reason := "out_of_stock"
			productID := item.ProductID
			return InventoryResponse{
				EventID:   event.EventID,
				Type:      InventoryFailed,
				OrderID:   event.OrderID,
				Reason:    &reason,
				ProductID: &productID,
			}
		}
	}

	for _, item := range event.Items {
		Stock[item.ProductID] -= item.Quantity
	}

	log.Printf("Товар успешно зарезервирован")

	return InventoryResponse{
		EventID: event.EventID,
		Type:    InventoryReserved,
		OrderID: event.OrderID,
	}

}

func (s *Service) claimEvent(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {

	cmdTag, err := tx.Exec(
		ctx,
		`
		INSERT INTO processed_events(event_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
		`,
		eventID,
	)

	if err != nil {
		// важно: сюда попадём если уже существует (no rows)
		return false, nil
	}

	return cmdTag.RowsAffected() > 0, nil
}

func (s *Service) writeOutbox(ctx context.Context, tx pgx.Tx, response InventoryResponse) error {

	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}

	log.Println("Создаем outbox событие:", response.EventID)

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			event_id,
			type,
			payload	
		)
		VALUES ($1, $2, $3)
		`,
		response.EventID,
		response.Type,
		payload,
	)
	return err
}
