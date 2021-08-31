package app

import (
	"log"
	"net/http"
	"traino/src/domain"
	"traino/src/service"

	"github.com/gorilla/mux"
)

func Start() {

	router := mux.NewRouter()

	//wiring
	//ch := CustomerHandlers{service.NewCustomerService(domain.NewCustomerRepositoryStub())}
	ch := CustomerHandlers{service.NewCustomerService(domain.NewCustomerRepositoryDb())}

	router.HandleFunc("/customers", ch.getAllCustomers).Methods(http.MethodGet)
	router.HandleFunc("/customers/{customer_id:[0-9]+}", ch.getCustomer).Methods(http.MethodGet)

	log.Fatal(http.ListenAndServe("localhost:8081", router))

}
