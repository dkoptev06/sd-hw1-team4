package scooter

type ScooterResponse struct {
	ID     string `json:"id"`
	ZoneID string `json:"zone_id"`
	Charge int    `json:"charge"`
}
