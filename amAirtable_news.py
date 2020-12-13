#Service that loops through Airtable producer, and uploads correct data by pulling from a central repo. Also runs text filters if needed 

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
def cleanNewsData(inputNews, inputDataFormat, count_asked):
	outputList = []
	range_to_check = count_asked if (count_asked <= len(inputNews)) else len(inputNews)
	for rec in range(range_to_check): 
		outputDict = {}
		outputDict['recID'] = rec+1 #to give it a sequence
		dataIn = inputNews[rec] #already a dict
		for key, value in inputDataFormat.items():
			if value in dataIn: 
				outputDict[key] = dataIn[value] 
		outputList.append(outputDict)
	return outputList

#Goes through all records and updates ones that are in the master dict
def updateLoop():
	allRecords = airtable_producer.get_all(view='Service - amDataNews')
	
	for i in allRecords:
		try: #In case have a prod payload or anything wrong 
			if "Prod_Ready" in i["fields"]: #Only working on prod ready ie checkboxed
				rec_ofAsked = i["id"]
				payload_native = i["fields"]["payload"]
				payload_json = json.loads(payload_native)
				count_asked = payload_json["count_needed"] #How many records needed 
				data_asked = payload_json["data_needed"]
				news_output = i["fields"]["output - amPayload_News"][0] #Since airtable stores as a list
				news_output_json = ast.literal_eval(news_output) #since List from airtable is in String
				data_toUpload = cleanNewsData(news_output_json, data_asked, count_asked)
				uploadData(data_toUpload, rec_ofAsked) #Just that bit updated 
		except Exception: 
			pass
	print ("Upload to CMS done")

updateLoop()