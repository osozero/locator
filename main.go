package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Configuration struct {
	DbAddress        string
	DbName           string
	Port             int
	User             string
	Password         string
	SslMode          string
	DbDriver         string
	OpenStreetMapUrl string
	LogFile          string
	Interval         int
}

type Feeling struct {
	Id         int
	VoteDate   string
	TopicId    int
	CountryId  int
	CityId     int
	DistrictId int
	UserId     string
	IsHappy    int
	Latitude   float64
	Longitude  float64
}

func convertFloatToString(float float64) string {
	return strconv.FormatFloat(float, 'f', -1, 64)
}

func getIdFromName(config *Configuration, country, city, district interface{}) (int, int, int) {
	var countryId, cityId, districtId int

	db, err := newDBConnection(config)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if country != nil {
		strCountry := country.(string)

		err = db.QueryRow("select id from countries where name = " + "'" + strCountry + "'").Scan(&countryId)
		if err != nil {
			log.Fatalln(err)
		}
	}

	if city != nil {
		strCity := city.(string)

		err := db.QueryRow("select id from cities where name =" + "'" + strCity + "'").Scan(&cityId)

		if err != nil {
			log.Fatalln(err)
		}
	}

	if district != nil {
		strDistrict := district.(string)

		err := db.QueryRow("select id from districts where name =" + "'" + strDistrict + "'").Scan(&districtId)

		if err != nil {
			log.Fatal(err)
		}
	}
	return countryId, cityId, districtId
}

func getLocationId(config *Configuration, lat, long float64) (int, int, int) {
	strLat := convertFloatToString(lat)
	strLong := convertFloatToString(long)

	url := config.OpenStreetMapUrl + "lat=" + strLat + "&lon=" + strLong + "&zoom=7"

	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	var data interface{}

	byteArray, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(byteArray, &data); err != nil {
		log.Fatal(err)
	}

	address := data.(map[string]interface{})["address"]

	log.Println(address)

	country := address.(map[string]interface{})["country"]
	city := address.(map[string]interface{})["state"]
	district := address.(map[string]interface{})["county"]

	if district == nil {
		district = address.(map[string]interface{})["district"]
	}

	log.Printf("COUNTRY: %s - CITY: %s - DISTRICT: %s\n", country, city, district)

	countryId, cityId, districtId := getIdFromName(config, country, city, district)

	return countryId, cityId, districtId
}

func updateLocation(config *Configuration, id, countryId, cityId, districtId int) {
	db, err := newDBConnection(config)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	strCountryId := strconv.Itoa(countryId)
	strCityId := strconv.Itoa(cityId)
	strDistrictId := strconv.Itoa(districtId)
	strId := strconv.Itoa(id)

	if _, err := db.Exec("update  feelings set country_id=" + "'" + strCountryId + "'" + ", city_id= " + "'" + strCityId + "'" + ", district_id= " + "'" + strDistrictId + "'" + " where id= " + "'" + strId + "'"); err != nil {
		log.Fatalln(err)
	}
}

func configure() *Configuration {
	file, err := os.Open("conf.json")

	if err != nil {
		log.Fatalln(err)
	}

	defer file.Close()

	decoder := json.NewDecoder(file)
	configuration := &Configuration{}
	err = decoder.Decode(&configuration)

	if err != nil {
		log.Fatalln(err)
	}

	return configuration
}

func prepareLogFile(config *Configuration) *os.File {
	file, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Panic(err)
	}

	log.SetOutput(file)

	return file
}

func newDBConnection(config *Configuration) (*sql.DB, error) {

	if config.DbDriver == "sqlite3" {
		db, err := sql.Open(config.DbDriver, config.DbAddress)
		if err != nil {
			log.Fatalln(err)
		}

		err = db.Ping()
		if err != nil {
			log.Fatalln(err)
		}

		return db, nil
	} else if config.DbDriver == "postgres" {
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			config.DbAddress, config.Port, config.User, config.Password, config.DbName)

		db, err := sql.Open(config.DbDriver, psqlInfo)
		if err != nil {
			log.Fatalln(err)
		}

		err = db.Ping()
		if err != nil {
			log.Fatalln(err)
		}

		return db, nil
	}

	return nil, errors.New("Invalid db driver")
}

func getUnlocatedFeelings(config *Configuration) []Feeling {
	db, err := newDBConnection(config)

	if err != nil {
		log.Fatalln(err)
	}

	defer db.Close()

	rows, err := db.Query("select * from feelings where country_id = -1 and city_id = -1 and district_id = -1")
	if err != nil {
		log.Fatalln(err)
	}

	defer rows.Close()

	var feelingList []Feeling

	for rows.Next() {
		feeling := Feeling{}
		err := rows.Scan(&feeling.Id, &feeling.VoteDate, &feeling.TopicId, &feeling.CountryId, &feeling.CityId, &feeling.DistrictId, &feeling.UserId, &feeling.IsHappy, &feeling.Latitude, &feeling.Longitude)
		if err != nil {
			log.Fatalln(err)
		}

		feelingList = append(feelingList, feeling)
	}

	err = rows.Err()

	if err != nil {
		log.Fatalln(err)
	}

	return feelingList

}

func main() {
	var countryId, cityId, districtId int

	config := configure()

	file := prepareLogFile(config)

	defer file.Close()

	for {
		feelingList := getUnlocatedFeelings(config)

		for _, feeling := range feelingList {
			countryId, cityId, districtId = getLocationId(config, feeling.Latitude, feeling.Longitude)
			updateLocation(config, feeling.Id, countryId, cityId, districtId)
		}

		time.Sleep(time.Duration(config.Interval) * time.Second)
	}
}
