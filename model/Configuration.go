package model

type Configuration struct{
	DbAddress string
	DbName string
	Port int
	User string
	Password string
	SslMode string
	DbDriver string
	OpenStreetMapUrl string
	LogFile string
	Interval int
}
