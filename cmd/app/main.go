package main

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/MatheusDomingues/go-kafka/internal/infra/akafka"
	"github.com/MatheusDomingues/go-kafka/internal/infra/repository"
	"github.com/MatheusDomingues/go-kafka/internal/infra/web"
	"github.com/MatheusDomingues/go-kafka/internal/usecase"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(host.docker.internal:3306)/products")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repository := repository.NewProductRepositoryMysql(db)
	createProductUseCase := usecase.NewCreateProductUseCase(repository)
	listProductsUseCase := usecase.NewListProductsUseCase(repository)

	productHandlers := web.NewProductHandlers(createProductUseCase, listProductsUseCase)

	r := chi.NewRouter()
	r.Get("/products", productHandlers.ListProductsHandler)
	r.Post("/products", productHandlers.CreateProductHandler)

	go http.ListenAndServe(":8080", r)

	msgChan := make(chan *kafka.Message)
	go akafka.Consume([]string{"products"}, "host.docker.internal:9094", msgChan)

	for msg := range msgChan {
		dto := usecase.CreateProductInputDto{}
		err := json.Unmarshal(msg.Value, &dto)
		if err != nil {
			// logar o erro
		}
		_, err = createProductUseCase.Execute(dto)
	}
}
