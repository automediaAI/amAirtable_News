#Service that loops through Airtable producer, and uploads correct data by pulling from a central repo. Also runs text filters if needed. Single or List of data 

##############
#### Airtable Data Service for News ####
#################
## Feed it news data. Pulls payload from airtable & pulls output from amPayload_News record, uploads data in correct format as needed.  ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############


## Declarations 
import os
from airtable import Airtable
import json
import ast #to covert string to list 
# from amLibrary_ETLFunctions import getSingleByRegion, topListByTitle, dataSingleParse, dataTableParse

# Airtable settings 
base_key = os.environ.get("PRIVATE_BASE_KEY")
table_producer = os.environ.get("PRIVATE_TABLE_NAME_PRODUCER")
api_key_airtable = os.environ.get("PRIVATE_API_KEY_AIRTABLE")
airtable_producer = Airtable(base_key, table_producer, api_key_airtable)

### DATA UPLOAD FUNCTIONS
#Uploads single json, or list to data_output of record ID as given
def uploadData(inputDictList, recToUpdate):
	recID = recToUpdate
	if isinstance(inputDictList, dict):
		fields = {'data_output': json.dumps(inputDictList)}
		# fields = {'data_output': str(inputDictList)} #Seems if I do str thats same too
	else:
		fields = {'data_output': str(inputDictList)}
	airtable_producer.update(recID, fields)


# Gives back correct news data based on ask ie how many, and what format
def getNewsData(inputNews, inputDataFormat):
	type_asked = inputDataFormat["type"] #dataTable or dataSingle
	data_asked = inputDataFormat["data_needed"] #format of data asked
	
	#Different functions if List or Single data asked for
	if type_asked == 'dataSingle': #In case only 1 item asked for or available 
		tempDict = {} #since this will return a single dict
		recID_asked = inputDataFormat["recID_needed"] #which rec needed, only used if dataSingle
		if recID_asked > len(inputNews):
			return {"error":"🚫 Record asked not in dict"}
		else:
			for i in inputNews:
				if (str(i['recID']) == str(recID_asked)): #Only returning that recID
					for key, value in data_asked.items(): #To map it
						if value in i.keys(): 
							tempDict[key] = i[value]
			return tempDict
	
	elif type_asked == 'dataTable': #In case of newsTable
		count_asked = inputDataFormat["count_needed"] #how many needed, only used if dataTable
		range_to_check = count_asked if (count_asked <= len(inputNews)) else len(inputNews)
		outputDict = {
				"table_info": {
					"type":"dataTable",
				},
				"rows" : []
			}
		outputList = []		
		for rec in range(range_to_check): 
			tempDict = {}
			tempDict['recID'] = rec #to give it a sequence
			dataIn = inputNews[rec] #already a dict
			
			for key, value in data_asked.items():
				if value in dataIn: 
					tempDict[key] = dataIn[value] 
			outputList.append(tempDict)
		outputDict["rows"] = outputList
		return outputDict
	
	else:
		return {"error":"🚫Data input incorrect"}

#Goes through all records and updates ones that are in the master dict
def updateLoop():
	allRecords_news = airtable_producer.get_all(view='Service - amDataNews')
	allRecords_images = airtable_producer.get_all(view='Service - amImagePuller')
	allRecords = allRecords_news + allRecords_images
	for i in allRecords:
		try: #In case have a prod payload or anything wrong 
			if "Prod_Ready" in i["fields"]: #Only working on prod ready ie checkboxed
				rec_ofAsked = i["id"]
				payload_native = i["fields"]["payload"]
				payload_json = json.loads(payload_native)
				data_asked_json = payload_json #Giving it entire JSON like in amDatacovid
				news_output = i["fields"]["output - amPayload_News"][0] #Since airtable stores as a list
				news_output_json = ast.literal_eval(news_output) #since List from airtable is in String
				data_toUpload = getNewsData(news_output_json, data_asked_json)
				uploadData(data_toUpload, rec_ofAsked) #Just that bit updated 
				print ("Row upload to CMS done..")
		except Exception: 
			pass
	print ("Upload to CMS done")

updateLoop()

