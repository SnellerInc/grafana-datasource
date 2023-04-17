package plugin

type snellerJSONData struct {
	Endpoint string `json:"Endpoint"`
}

type snellerQuery struct {
	Database *string `json:"Database"`
	SQL      string  `json:"SQL"`
}

type snellerDatabase struct {
	Name string `json:"name"`
}
