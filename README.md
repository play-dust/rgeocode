# rgeocode
#### Offline reverse geocoder

rgeocode accepts a geographic coordinate pair (latitude and longitude) and returns a list containing the name of:

*	A geographical point
*	First administrative unit
*	Second administrative unit
*	Country

Credits: rgeocode uses data sourced from https://www.geonames.org/ and is used under a CC 4.0 license [(link to Creative Commons 4.0)](https://creativecommons.org/licenses/by/4.0/)

## Install

   
    >>>   import rgeocode
     
    >>>   location = rgeocode.Post(40.689247, -74.044502)
     
    >>>   print(location.locationlist)
        
    ['Statue of Liberty', 'New York', 'New York County', 'United States']


## Requirements

*	sqlite3
*	python version >= 2.5

## First run

The first time rgeocode is run, it attempts to download the required files (*countryInfo.txt*, 
*admin1CodesASCII.txt*, *admin2Codes.txt*) from geonames.org. The three files have a combined size of ~1.6GB. After the download completes, the required data is copied to a local sqlite3 database. After the database is created, the downloaded files are deleted. rgeocode creates - *admin1.tsv, admin2.tsv, countries.tsv, and geo.db.* The combined size of these files is ~600MB. Subsequent calls to *start_rgeocode()* reference the data in these files. The files are created in the same folder as the script in which *start_rgeocode()* is imported. When *start_rgeocode()* is called from the interactive shell the files are downloaded and created in the home path.

##  Options

If you need to reverse geocode for locations only in specific countries, you can reduce the size of the database *(geo.db)* by calling *filter_rgeocode(stringList)*. 

     >>>  codelist = ['IN', 'US']
     
     >>>  status = location.filter_rgeocode(codelist)
     
     >>>  print(status)
     
     Database filtered: Deleted 9325631 rows.


To retrieve ISO country codes:
 
     >>>  codes = location.country_code()
                                                                                                                 
     >>>  print(codes)
     
     {'AD': 'Andorra', 'AE': 'United Arab Emirates', ... , CS': 'Serbia and Montenegro', 'AN': 'Netherlands Antilles'}

