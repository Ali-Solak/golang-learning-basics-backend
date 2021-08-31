package domain

import (
	"database/sql"
	"log"
	"net/http"
	"time"
	"traino/src/errs"

	_ "github.com/go-sql-driver/mysql"
)

type customerRepositoryDb struct {
	client *sql.DB
}

func (d customerRepositoryDb) ById(id string) (*Customer, *errs.AppError) {
	customerSql := "select customer_id, name, city, zipcode, date_of_birth, status from customers where customer_id = ?"
	row := d.client.QueryRow(customerSql, id)

	var c Customer
	err := row.Scan(&c.Id, &c.Name, &c.City, &c.ZipCode, &c.DateOfBirth, &c.Status)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errs.NewNotFoundError("Customer not found")
		} else {
			log.Print("Error while scanning customer " + err.Error())
			return nil, errs.NewUnexpectedError("Unexpected database error")
		}
	}
	return &c, nil
}

func (d customerRepositoryDb) FindAll() ([]Customer, *errs.AppError) {

	findAllSql := "select customer_id, name, city, zipcode, date_of_birth, status from customers"
	rows, err := d.client.Query(findAllSql)

	if err != nil {
		log.Print("Error while querying customer table " + err.Error())
		return nil, &errs.AppError{
			Message: "internal error",
			Code:    http.StatusInternalServerError}
	}
	customers := make([]Customer, 0)
	for rows.Next() {
		var c Customer
		err := rows.Scan(&c.Id, &c.Name, &c.City, &c.ZipCode, &c.DateOfBirth, &c.Status)

		if err != nil {
			if err == sql.ErrNoRows {
				return nil, errs.NewNotFoundError("Customer not found")
			} else {
				log.Print("Error while scanning customers " + err.Error())
				return nil, errs.NewUnexpectedError("Unexpected database error")
			}
		}
		customers = append(customers, c)
	}
	return customers, nil
}

func NewCustomerRepositoryDb() customerRepositoryDb {
	client, err := sql.Open("mysql", "root:123@tcp(localhost:3306)/banking")
	if err != nil {
		panic(err)
	}
	// See "Important settings" section.
	client.SetConnMaxLifetime(time.Minute * 3)
	client.SetMaxOpenConns(10)
	client.SetMaxIdleConns(10)

	return customerRepositoryDb{client}
}
