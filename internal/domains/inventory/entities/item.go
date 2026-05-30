package entities

type Item struct {
	sku               string
	availableQuantity int
	priceCents        int
}

func NewItem(sku string, availableQuantity int, priceCents int) Item {
	return Item{
		sku:               sku,
		availableQuantity: availableQuantity,
		priceCents:        priceCents,
	}
}

func (i Item) SKU() string {
	return i.sku
}

func (i Item) AvailableQuantity() int {
	return i.availableQuantity
}

func (i Item) PriceCents() int {
	return i.priceCents
}
