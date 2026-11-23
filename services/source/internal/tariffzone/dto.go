package tariffzone

type TariffZoneResponse struct {
	ID             string `json:"id"`
	PricePerMinute int64  `json:"price_per_minute"`
	PriceUnlock    int64  `json:"price_unlock"`
	DefaultDeposit int64  `json:"default_deposit"`
}
