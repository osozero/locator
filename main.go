package main

import (
	_ "fmt"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	_"github.com/lib/pq"
	_ "github.com/osozero/locator/model"
	"log"
	"github.com/osozero/locator/model"
	"net/http"
	"strconv"
	"encoding/json"
	"io/ioutil"
	"os"
	"fmt"
	"errors"
	"time"
)

func convertFloatToString(float float64) string{
	return strconv.FormatFloat(float,'f',-1,64)
}

func getIdFromName(config *model.Configuration,country,city,district interface{}) (int,int,int){

	log.Println("getIdFromName executing")

	var countryId, cityId,districtId int

	db, err := newDBConnection(config)
	if err!=nil {
		log.Fatal(err)
	}

	defer db.Close()

	if country!=nil {
		strCountry := country.(string)

		err=db.QueryRow("select id from countries where name = "+"'"+strCountry+"'").Scan(&countryId)
		if err!=nil {
			log.Fatal(err)
		}
	}

	if city!=nil {
		strCity := city.(string)

		err:= db.QueryRow("select id from cities where name ="+"'"+strCity+"'").Scan(&cityId)

		if err!=nil {
			log.Fatal(err)
		}
	}

	if district !=nil{
		strDistrict := district.(string)

		err:=db.QueryRow("select id from districts where name ="+"'"+strDistrict+"'").Scan(&districtId)

		if err!=nil {
			log.Fatal(err)
		}
	}

	log.Println("getIdFromName leaving")

	return countryId,cityId,districtId
}

func getLocationId(config *model.Configuration,lat,long float64) (int,int,int){

	log.Println("getLocationId executing")

	strLat := convertFloatToString(lat)
	strLong := convertFloatToString(long)

	url := config.OpenStreetMapUrl+"lat="+strLat+"&lon="+strLong+"&zoom=7"


	resp,err := http.Get(url)
	if err!=nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	var data  interface{}

	byteArray,err := ioutil.ReadAll(resp.Body)
	if err!=nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(byteArray,&data);err!=nil{
		log.Fatal(err)
	}

	address:=data.(map[string]interface{})["address"]

	log.Println(address)

	country:=address.(map[string]interface{})["country"]
	city:=address.(map[string]interface{})["state"]
	district:=address.(map[string]interface{})["county"]

	if district == nil {
		district = address.(map[string]interface{})["district"]
	}

	log.Printf("country: %s - city: %s - district: %s\n",country,city,district)

	countryId,cityId,districtId := getIdFromName(config,country,city,district)

	log.Println("getLocationId leaving")

	return countryId,cityId,districtId
}

func updateLocation(config *model.Configuration,id,countryId,cityId,districtId int){

	log.Println("updateLocation executing")

	db, err := newDBConnection(config)
	if err!=nil {
		log.Fatal(err)
	}

	defer db.Close()

	strCountryId := strconv.Itoa(countryId)
	strCityId := strconv.Itoa(cityId)
	strDistrictId := strconv.Itoa(districtId)
	strId := strconv.Itoa(id)

	if _,err := db.Exec("update  feelings set country_id="+"'"+strCountryId+"'"+", city_id= "+"'"+strCityId+"'"+", district_id= "+"'"+strDistrictId+"'"+" where id= "+"'"+strId+"'");err!=nil{
		log.Fatal(err)
	}

	log.Printf("%s id'li kayıt basariyla guncellendi\n",strId)

	log.Println("updateLocation leaving")
}

func configure() *model.Configuration{
	file,err := os.Open("conf.json")

	if err != nil{
		log.Fatal(err)
	}

	defer file.Close()

	decoder:=json.NewDecoder(file)
	configuration := model.Configuration{}
	err = decoder.Decode(&configuration)

	if err !=nil{
		log.Fatal(err)
	}

	return &configuration
}

func prepareLogFile(config *model.Configuration) *os.File{
	file,err := os.OpenFile(config.LogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err!=nil {
		log.Panic(err)
	}

	log.SetOutput(file)

	log.Println("log initialized")

	return file
}

func newDBConnection(config *model.Configuration) (*sql.DB,error) {

	if config.DbDriver == "sqlite3" {
		db, err := sql.Open(config.DbDriver,config.DbAddress)
		if err != nil{
			log.Fatal(err)
		}

		err = db.Ping()
		if err != nil {
			log.Fatal(err)
		}

		return db,nil
	}else if config.DbDriver == "postgres"{
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			config.DbAddress, config.Port, config.User, config.Password, config.DbName)

		db,err := sql.Open(config.DbDriver,psqlInfo)
		if err!=nil {
			log.Fatal(err)
		}

		err = db.Ping()
		if err!=nil {
			log.Fatal(err)
		}

		return db,nil
	}

	return nil,errors.New("Invalid db driver")
}

func getUnlocatedFeelings(config *model.Configuration) []model.Feeling{
	db, err := newDBConnection(config)

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	rows, err := db.Query("select * from feelings where country_id = -1 and city_id = -1 and district_id = -1")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var feelingList []model.Feeling

	for rows.Next() {
		feeling := model.Feeling{}
		err := rows.Scan(&feeling.Id, &feeling.VoteDate, &feeling.TopicId, &feeling.CountryId, &feeling.CityId, &feeling.DistrictId, &feeling.UserId,&feeling.IsHappy, &feeling.Latitude, &feeling.Longitude)
		if err != nil {
			log.Fatal(err)
		}

		feelingList = append(feelingList, feeling)
	}

	err = rows.Err()

	if err != nil {
		log.Fatal(err)
	}

	return feelingList

}

func main() {
	var countryId, cityId, districtId int

	config := configure()

	file := prepareLogFile(config)

	defer file.Close()

	for  {
		feelingList := getUnlocatedFeelings(config)

		for _, feeling := range feelingList {
			countryId, cityId, districtId = getLocationId(config, feeling.Latitude, feeling.Longitude)
			updateLocation(config, feeling.Id, countryId, cityId, districtId)
		}

		time.Sleep( time.Duration(config.Interval)* time.Second)
	}
}
