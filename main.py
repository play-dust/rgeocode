import sys
import os
from os.path import expanduser
import csv
import sys
from sys import platform
import sqlite3
import zipfile
import subprocess

from haversine import haversine

if sys.version_info[0] == 3:
	import urllib.request
elif sys.version_info[0] < 3:
	import urllib

class Post:	
	def __init__(self, latitude, longitude):
		self.LOCATION = ''
		self.conn = None
		self.countries = {}
		self.admin2 = {}
		self.admin1 = {}
		self.country_code_dictionary={}
		self.locationlist =[]
		self.BASE_URL = "http://download.geonames.org/export/dump/"
		self.FILE_ONE = "allCountries.zip"
		self.FILE_TWO = "countryInfo.txt"
		self.FILE_THREE = "admin1CodesASCII.txt"
		self.FILE_FOUR = "admin2Codes.txt"
	
		self.LOCATIONDICT = sys._getframe(1).f_globals
		self.latitude = latitude
		self.longitude = longitude
		self.LOCATION = self.user_cwd(self.LOCATIONDICT)
		
		if isinstance(latitude, float) and isinstance(longitude, float):
			status=self.do_check()
		else:
			status='Invalid data type'
		
		if 'Start reverse geocode' in status:
			if 'Error in reverse geocode: ' in status:
				return(status)
			else:
				self.geo_dictionary()

				self.locationlist=self.get_location(self.latitude, self.longitude)
			
				self.cleanup()
		else:
			self.cleanup()
			self.locationlist = status

	def country_code(self):
		try:
			with open(os.path.join(self.LOCATION, 'countries.tsv'), 'r', encoding="utf8") as source:
				reader = csv.reader(source, delimiter='\t')
				for row in reader:
					code = row[0]
					if not '#' in code:
						name = row[4]
						self.country_code_dictionary[code] = name
		except FileNotFoundError:
			status = 'File not found countries.tsv'
			return(status)
		return(self.country_code_dictionary)

	def filter_rgeocode(self, codelist):
		country_code_dictionary = self.country_code()
		if country_code_dictionary == 'File not found countries.tsv':
			status = 'File not found countries.tsv'
			return(status)

		self.connectdatabase()
		dictionary_keys = country_code_dictionary.keys()

		for key in range(len(codelist)):
			if codelist[key] not in dictionary_keys:
				status = 'Invalid country code: ' + str(codelist[key])
				return(status)

		code="'"
		delim = "',"
		for i in range(len(codelist)):
			code = code + "'" + str(codelist[i]) + delim
		
		code = code[1:-1]

		sql="""DELETE
		FROM geotable
		WHERE geo_countrycode NOT IN (""" + code +");"
		
		try: 
			cursor = self.conn.execute(sql)
			self.conn.commit()
			self.conn.execute("vacuum")	#This is to reduce file size of geo.db from ~600MB
			status = 'Database filtered: '
		except sqlite3.Error as e:
			status = 'Error in filter_rgeocode delete ' + str(e)
			
		if status == 'Database filtered: ':
			sql="SELECT changes();"
			try: 
				cursor = self.conn.execute(sql)
				count = cursor.fetchone()
				status = status + 'Deleted ' + str(count[0]) + ' rows.'
			except sqlite3.Error as e:
			  	status = 'Error in filter_rgeocode changes() ' + str(e)
			
		self.cleanup()
		return(status)

	def connectdatabase(self):
		try:
			self.conn = sqlite3.connect(os.path.join(self.LOCATION, 'geo.db'))
			status = 'Database connected'
		except sqlite3.Error as e:
			status='Error in reverse geocode: '+str(e)

		return(status)

	def cleanup(self):
		if self.conn is not None:
			self.conn.close()

		if os.path.exists(os.path.join(self.LOCATION, 'allCountries.txt')):
			os.remove(os.path.join(self.LOCATION, 'allCountries.txt'))

		if os.path.exists(os.path.join(self.LOCATION, 'geonamesdata.csv')):
			os.remove(os.path.join(self.LOCATION, 'geonamesdata.csv'))

		if os.path.exists(os.path.join(self.LOCATION, 'allCountries.zip')):
			os.remove(os.path.join(self.LOCATION, 'allCountries.zip'))

	def creategeotable(self):
		try:
			sql ="""CREATE TABLE geotable(
	        geo_name TEXT NOT NULL,
	        geo_lat REAL NOT NULL,
	        geo_lng REAL NOT NULL,
	        geo_countrycode TEXT,
	        geo_statecode TEXT,
	        geo_citycode TEXT
	        )"""
			cursor = self.conn.cursor()
			cursor.execute(sql)
			self.conn.commit()
			status = 'geotable created'
		except sqlite3.Error as e:
			if not 'table already exists' in str(e):
				status='Error occured in creating table geotable: '+ str(e)

		return(status)

	def downloadfile(self, filename, savetofile):
		savetofile = os.path.join(self.LOCATION, savetofile)
		if sys.version_info[0] == 3:
			try:
				urllib.request.urlretrieve(self.BASE_URL + filename, savetofile)
				status = filename + ' download complete ...'
			except Exception as e:
				status='Error downloading file: ' + filename + ' ' + str(e)
		elif sys.version_info[0] < 3:
			try:
				urllib.urlretrieve(self.BASE_URL + filename, savetofile)
				status = filename + ' download complete ...'
			except Exception as e:
				status='Error downloading file: ' + filename + ' ' + str(e)
		return(status)

	def do_check(self):
		downloadflag = 0
		if sys.version_info[0] < 3 and sys.version_info[1] < 5:
			status = 'Python version should be greater than 2.5'
			return(status)

		if platform == "win32":
			if not os.path.exists(os.path.join(self.LOCATION, 'sqlite3.exe')):
				status='sqlite3.exe not found.'
				return(status)
			self.connectdatabase()
		else:
			if not os.path.exists(os.path.join(self.LOCATION, 'sqlite3')):
				status='sqlite3 not found.'
				return(status)
			self.connectdatabase()
		
		sql="""SELECT
		NAME
		FROM
		SQLITE_MASTER
		WHERE
		TYPE = 'table'
		AND
		NAME = 'geotable';
		"""
		try:    
			cursor = self.conn.execute(sql)
			row = cursor.fetchone()
			if row is None:
				self.creategeotable()
				downloadflag = 1
			else:
				status = 'Start reverse geocode - geotable already exists ...'
		except sqlite3.Error as e:
		  	status = 'Error in reverse geocode: ' + str(e)
		  	return(status)

		if not os.path.exists(os.path.join(self.LOCATION, 'allCountries.zip')) and downloadflag == 1:
			status = self.downloadfile(self.FILE_ONE, 'allCountries.zip')
			if 'Error downloading file: ' in status:
				return(status)
			with zipfile.ZipFile(os.path.join(self.LOCATION, 'allCountries.zip'), 'r') as z:
				z.extractall(self.LOCATION)

			f=open(os.path.join(self.LOCATION, 'geonamesdata.csv'), 'w', encoding='UTF-8')
			
			with open(os.path.join(self.LOCATION, 'allCountries.txt'), 'r', encoding="utf8") as source:
				reader = csv.reader(source, delimiter='\t')
				for r in reader:
					stringVal = '|'.join([r[2], r[4], r[5], r[8], r[10], r[11]])
					f.write(stringVal+'\n')
				f.close()

			#Enclosing LOCATION quotes allows for spaces in file path
			NL = '"' + self.LOCATION  + '/' + "geonamesdata.csv" + '"' 
			
			subprocess.call([
			os.path.join(self.LOCATION, "sqlite3"), 
			os.path.join(self.LOCATION, "geo.db"), 
			"-separator", "|" ,
			".import "+ NL + " geotable"
			])

		if not os.path.exists(os.path.join(self.LOCATION, 'countries.tsv')):
			status = self.downloadfile(self.FILE_TWO, 'countries.tsv')
			if 'Error downloading file: ' in status:
				return(status)

		if not os.path.exists(os.path.join(self.LOCATION, 'admin1.tsv')):
			status = self.downloadfile(self.FILE_THREE, 'admin1.tsv')
			if 'Error downloading file: ' in status:
				return(status)

		if not os.path.exists(os.path.join(self.LOCATION, 'admin2.tsv')):
			status = self.downloadfile(self.FILE_FOUR, 'admin2.tsv')
			if 'Error downloading file: ' in status:
				return(status)
		status = 'Start reverse geocode'
		return(status)

	def geo_dictionary(self):
		with open(os.path.join(self.LOCATION, 'countries.tsv'), 'r', encoding="utf8") as source:
			reader = csv.reader(source, delimiter='\t')
			for row in reader:
				code = row[0]
				if not '#' in code:
					name = row[4]
					self.countries[code] = name
		with open(os.path.join(self.LOCATION, 'admin1.tsv'), 'r', encoding="utf8") as source:
			reader = csv.reader(source, delimiter='\t')
			for row in reader:
				code = row[0]
				name = row[1]
				self.admin1[code] = name
		with open(os.path.join(self.LOCATION, 'admin2.tsv'), 'r', encoding="utf8") as source:
			reader = csv.reader(source, delimiter='\t')
			for row in reader:
				code = row[0]
				name = row[1]
				self.admin2[code] = name

	def get_location(self, latitude, longitude):
		locationlist=[]
		geolocation = []
		haversinedistancelist=[]

		coordinates = latitude, longitude
		allcoordinates=[]

		latitude = str(latitude)
		dotindex = latitude.index('.')
		latitiude = latitude[0:dotindex]

		longitude = str(longitude)
		dotindex = longitude.index('.')
		longitude = longitude[0:dotindex]

		sql = """SELECT
		rowid,
		geo_name,
		geo_lat,
		geo_lng,
		geo_countrycode,
		geo_statecode,
		geo_citycode
		FROM geotable 
		WHERE geo_lat LIKE '""" + latitiude + """%'
		AND geo_lng LIKE '""" + longitude +  "%';"

		try: 
			cursor = self.conn.execute(sql)
			rows = cursor.fetchall()
		except sqlite3.Error as e:
		  	status = 'Error in reverse geocode: ' + str(e)
		  	return(status)
		
		for row in rows:
			geolocation.append(dict(locality=row[1], 
									country_code=row[4], 
		            				state_code=str(row[4])+'.'+str(row[5]), 
		            				city_code=str(row[4])+'.'+str(row[5])+'.'+str(row[6])
		            				)
								)
			allcoordinates.append((row[2], row[3]))

		try:
			for i in range(len(allcoordinates)):
				haversinedistancelist.append(haversine(coordinates, allcoordinates[i]))

			likelyplace = min(haversinedistancelist)
			placeindex = haversinedistancelist.index(likelyplace)

			try:
				self.locationlist.append(geolocation[placeindex]['locality'])
			except Exception as e:
				self.locationlist.append('')
			try:
				self.locationlist.append(self.admin1[geolocation[placeindex]['state_code']])
			except Exception as e:
				self.locationlist.append('')
			try:	
				self.locationlist.append(self.admin2[geolocation[placeindex]['city_code']])
			except Exception as e:
				self.locationlist.append('')
			try:
				self.locationlist.append(self.countries[geolocation[placeindex]['country_code']])
			except Exception as e:
				self.locationlist.append('')
		except Exception as e:
		  status = 'Error in reverse geocode: ' + str(e)
		  return(status + str(self.locationlist))

		return(self.locationlist)

	def user_cwd(self, LOCATIONDICT):
		try:
			LOCATION = os.path.dirname(LOCATIONDICT['__file__'])
		except KeyError:
			#LOCATION is set to home path when start_rgeocode is run from interactive shell
			LOCATION = expanduser("~") 
		
		if platform == "win32":
			LOCATION = LOCATION + '\\'
			LOCATION = LOCATION.replace('\\', '\\\\')

		if len(LOCATION) == 0:			
			LOCATION = os.getcwd()		#LOCATION is set to cwd when rgeocode.py is main
		return(LOCATION)

if __name__ == '__main__':
	location = Post(40.689247, -74.044502)
	print(location.locationlist)
	