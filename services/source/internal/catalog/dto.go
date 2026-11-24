package catalog

type scooterResponse struct {
	ID     string `json:"id"`
	ZoneID string `json:"zone_id"`
	Charge int    `json:"charge"`
}

func scooterDTO(s scooter) scooterResponse {
	return scooterResponse{ID: s.id, ZoneID: s.zoneID, Charge: s.charge}
}

type tariffZoneResponse struct {
	ID             string `json:"id"`
	PricePerMinute int64  `json:"price_per_minute"`
	PriceUnlock    int64  `json:"price_unlock"`
	DefaultDeposit int64  `json:"default_deposit"`
}

func tariffZoneDTO(z tariffZone) tariffZoneResponse {
	return tariffZoneResponse{ID: z.id, PricePerMinute: z.pricePerMinute, PriceUnlock: z.priceUnlock, DefaultDeposit: z.defaultDeposit}
}

type userProfileResponse struct {
	ID              string `json:"id"`
	HasSubscription bool   `json:"has_subscription"`
	Trusted         bool   `json:"trusted"`
}

func userProfileDTO(u userProfile) userProfileResponse {
	return userProfileResponse{ID: u.id, HasSubscription: u.hasSubscription, Trusted: u.trusted}
}

type priceCoeffResponse struct {
	Surge             float64 `json:"surge"`
	LowChargeDiscount float64 `json:"low_charge_discount"`
}

type configsResponse struct {
	PriceCoeffSettings        priceCoeffResponse `json:"price_coeff_settings"`
	DefaultRetentionDays      int                `json:"default_retention_days"`
	RetentionDaysByZone       map[string]int     `json:"retention_days_by_zone"`
	TariffZoneCacheTTLMinutes int                `json:"tariff_zone_cache_ttl_minutes"`
}

func configsDTO(c configs) configsResponse {
	return configsResponse{
		PriceCoeffSettings:        priceCoeffResponse{Surge: c.surge, LowChargeDiscount: c.lowChargeDiscount},
		DefaultRetentionDays:      c.defaultRetentionDays,
		RetentionDaysByZone:       c.retentionDaysByZone,
		TariffZoneCacheTTLMinutes: c.tariffZoneCacheTTLMinutes,
	}
}

type errorBody struct {
	ErrorCode string `json:"error_code"`
}
