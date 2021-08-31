package service

import (
	"traino/src/domain"
	"traino/src/errs"
)

type CustomerService interface {
	GetAllCustomers() ([]domain.Customer, *errs.AppError)
	GetCustomer(id string) (*domain.Customer, *errs.AppError)
}

type DefaultCustomerService struct {
	repo domain.CustomerRepository
}

func (s DefaultCustomerService) GetCustomer(id string) (*domain.Customer, *errs.AppError) {
	return s.repo.ById(id)
}

func (s DefaultCustomerService) GetAllCustomers() ([]domain.Customer, *errs.AppError) {
	return s.repo.FindAll()
}

func NewCustomerService(repository domain.CustomerRepository) DefaultCustomerService {
	return DefaultCustomerService{repository}
}
