package userprofile

type UserProfileResponse struct {
	ID              string `json:"id"`
	HasSubscription bool   `json:"has_subscription"`
	Trusted         bool   `json:"trusted"`
}
