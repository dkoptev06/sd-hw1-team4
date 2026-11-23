package config

import "encoding/json"

type PriceCoefficientSettings struct {
	Surge             json.Number `json:"surge"`
	LowChargeDiscount json.Number `json:"low_charge_discount"`
}

type ConfigsResponse struct {
	PriceCoefficientSettings  PriceCoefficientSettings `json:"price_coeff_settings"`
	DefaultRetentionDays      int                      `json:"default_retention_days"`
	RetentionDaysByZone       map[string]int           `json:"retention_days_by_zone"`
	TariffZoneCacheTTLMinutes int                      `json:"tariff_zone_cache_ttl_minutes"`
}
