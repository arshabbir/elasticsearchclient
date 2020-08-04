package domain

type Student struct {
	StudentId string `json:"studentid"`
	Name      string `json:"name"`
	Marks     int64  `json:"marks"`
	Class     string `json:"class"`
}

