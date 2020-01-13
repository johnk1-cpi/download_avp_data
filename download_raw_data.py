import pickle
import os.path
import os
import httplib2
import io
import pandas as pd

import datetime
from google.oauth2 import service_account
from apiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
import urllib.parse as urlparse

## Google Sheets File ID
SHEET_ID = '1slolMpeZE9kJHlFnz0qlyyLnju639JGoNTGTu0myVMQ'

## SEE: https://developers.google.com/drive/api/v3/manage-downloads
##      https://developers.google.com/sheets/api/guides/valuesS

SCOPE = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly']

CREDFILE =  'hs-american-voices-project-b12d8ea9a82f.json'
RANGE = 'Dashboard for Audio Files and Field Notes'

def get_sheets_data(SHEET_ID):
    ##
    ## Get data from Google Sheets
    ##
    ## CREDFILE contains path to Project Credentials
    ## SCOPES are sheets and drive
    ## SHEET_ID is unique ID of Google sheets (found in URL)
    ## Returns pd.DataFrame object

    ## Set up Credentials using Google.oauth2.service_account
    credentials = service_account.Credentials.from_service_account_file(CREDFILE, scopes=SCOPE)
    
    ## 
    service = discovery.build('sheets', 'v4', credentials=credentials)
    
    ## Create spreadsheets object from Google.
    sheet = service.spreadsheets()
    ## Get sheet data
    result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE).execute()
    ret = result.get('values', [])
    out = pd.DataFrame.from_records(ret[1:], 
        columns = ret[0][:16])
    return(out)

#FILE_ID = '1C98BnnoHYBCtmvPISET7CoOgRVcHjtW6'

def get_drive_file(file_id):
    ## file_id is id of Google Drive file 
    ## CREDFILE is path to credential 
    ## SCOPES are sheets and drive
    ## Returns tuple (request, filed_id)
    ## request: is service.files() object
    ## file_id is str of file_id passed to function


    credentials = service_account.Credentials.from_service_account_file(CREDFILE, scopes=SCOPE)
    service = discovery.build('drive', 'v3', credentials=credentials)
    request = service.files()

    ## TODO: 
    ## Recognize File mimeType to parse PDFs different from
    ## DOCX or Google Docs files, etc

    return((request, file_id))

def get_file_ext(r):
    ## extracts name of google_drive file from request object 
    ## Takes (request, file_id) 
    ## returns filename

    FILE_ID = r[1]
    request = r[0]
    if FILE_ID != '': 
        file_name = request.get(fileId=FILE_ID).execute()['name']
    else:
        file_name = ''
    return(file_name)

def get_file_fh(r):
    ## Downloads file with request.get_media
    ## puts file into io.BytesIO() object
    ## Handles large files
    ## Takes (request, filed_id)
    ## Returns io.Bytes() object containing Downloaded data
    
    FILE_ID = r[1]
    request = r[0]
    print(request)
    if FILE_ID == '':
        return('')

    ## TODO: 
    ## get_media works for files that aren't google Docs
    ## use request.export() for google docs
    
    ## See documentation for io.BytesIO
    request = request.get_media(fileId=FILE_ID)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print('Download %d%%.' % int(status.progress() * 100))
    return(fh)


def clean_url(url):
    ## Get Drive fileId from URL
    ## Takes url from sheets
    ## Returns file_id
    parsed = urlparse.urlparse(url)
    print(parsed)
    if 'id' in urlparse.parse_qs(parsed.query):
        file_id = urlparse.parse_qs(parsed.query)['id']
        return(file_id[0])
    else:
        return('')


def save_files(row):
    ## Runs row-by-row down google sheets
    ## Takes a row from the spreadsheet
    ## saves to an organized Directory
    ## file_name is a list containing the str names of files from each link
    ## audio_files is a list containing the BytesIO objects ready to be saved from each drive link
    ## Returns nothing

    ## TODO: This needs a lot of work...

    #HHID 
    hhid = row['Household ID:']
    #Timestamp for Week
    timestamp = datetime.datetime.strptime(row.Timestamp, "%m/%d/%Y %H:%M:%S")

    ##file names and audio file objects for each column of the audio file sheet
    file_name = [get_file_ext(row['file_obj_'+str(i)]) for i in range(1,6)]
    audio_files = [get_file_fh(row['file_obj_'+str(i)]) for i in range(1,6)]

    ## Get week 
    week = timestamp.strftime("%V")

    ## If file doesn't exist, create directory structure
    if not os.path.exists('./files_out/' + str(hhid)):
        os.mkdir('./files_out/' +  str(hhid))

    for k in range(len(file_name)):
        ## Go through all of the audio file objects (may be mutliple per row)
        ## TODO: This is very messy
        ##       and it takes forever to run
  
        ## Unique ID
        ## There are multiple file objects per row
        uid = row['file_obj_'+str(k+1)][1]

        ## Audio File names can contain the word "backup" 
        ## I put that in the file name
        backup = 'backup' if 'backup' in file_name[k].lower() else 'main'
        
        ## Get the file extension here, this should be moved up to its own function
        ## and, should use mimeType not file ext. for filetype 
        ext = '.' + file_name[k].split('.')[-1]

        ## File name - HHID + ['backup' if backup] + uid
        newfile = str(hhid) + '_' + backup + '_' + str(uid)

        ## if there is an audio file, save it using the name created above
        if audio_files[k]!='':
            with open('./files_out/' +   str(hhid)+ '/'+ newfile + ext, 'wb') as f:
               audio_files[k].seek(0)
               f.write(audio_files[k].read())


## TODO: Also very messy
## This should be in a def __main__() 
sheet = get_sheets_data(SHEET_ID)
sheet = sheet.iloc[-100:-75]

for i in range(1,6): 
    sheet['file_obj_'+str(i)] = sheet['Audio file upload ' + str(i)].apply(lambda x: get_drive_file(clean_url(x)))
print(sheet)

# sheet.to_pickle('tmp.pkl')
# sheet = pd.read_pickle('tmp.pkl')
sheet.apply(save_files, axis=1)
