package model

type Feeling struct{
	Id int
	VoteDate string
	TopicId int
	CountryId int
	CityId int
	DistrictId int
	UserId string
	IsHappy int
	Latitude float64
	Longitude float64
}
