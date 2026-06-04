package entities

type Customer struct {
	id string
}

func NewCustomer(id string) (Customer, error) {
	if id == "" {
		return Customer{}, ErrEmptyCustomerID
	}

	return Customer{id: id}, nil
}

func (c Customer) ID() string {
	return c.id
}
