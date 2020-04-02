#### This program downloads the files stored in each row of the "AVP: Audio File and Field Notes Uploads (Responses)" Google Sheet.

#### TO-DO:
    ## 1. Update the program to download field notes as well
        ## This is probably fairly easy to do. We may just need to iterate over the field note fields in the Google Sheet as well.
        ## The code for this would take place in the for loop at the end of the program, similar to how we iterate through the
        ## audio files.
    ## 2. Error/exception handling
        ## Every once in a while, the Google API generates an error. At present, this halts the program until it can be resumed.
        ## We can reduce this inefficiency by incorporating error/exception handling for these errors.
    ## 3. Gather document size
        ## We could also record the size of different files as we download them (this would be helpful for audio files to give us
        ## a crude benchmark of the length of the different files.
    ## 4. Generally cleaning up this program.
        ## There are a few parts of this program that are certainly a bit messy, and we could likely improve the style.




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
##      https://developers.google.com/sheets/api/guides/values

SCOPE = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly']

CREDFILE =  '/Users/johnkingsley/Desktop/Python/hs-american-voices-project-e9bc74b03eef.json'
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

def get_drive_file(file_id):
    ## file_id is id of Google Drive file 
    ## CREDFILE is path to credential 
    ## SCOPES are sheets and drive
    ## Returns tuple (request, filed_id)
    ## request: is service.files() object
    ## file_id is str of file_id passed to function
    print(file_id)

    
    credentials = service_account.Credentials.from_service_account_file(CREDFILE, scopes=SCOPE)
    service = discovery.build('drive', 'v3', credentials=credentials)
    request = service.files()


    ## TODO: 
    ## Recognize File mimeType to parse PDFs different from
    ## DOCX or Google Docs files, etc

    ## JWK: does this need to take place within this function?? Or where should it be? Seems like this function
        ## is relatively contained - it is already returning the desired tuple. Should the file parsing take
        ## place here? Or elsewhere? What's the goal here?
            ## If it is stored here, where should it be stored? Seems like this would require a fundamental restructuring
            ## of this tuple (which maybe isn't the most significant thing to deal with, but does changes how other functions
            ## interact with this one.)

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
    if FILE_ID == '':
        return('')

    ## TODO: 
    ## get_media works for files that aren't google Docs
    ## use request.export() for google docs

        ## JWK: relevant part is then just throwing an if statement that detects whether or not a document is, in fact,
            ## a Google Document, and then updating the object "request" as needed.
    
    ## See documentation for io.BytesIO

    ## JWK addition (this is messy!):

    #print(request.get("mimeType"))

    #print("mimeType:")
    docMimeType = request.get(fileId = FILE_ID, fields = "mimeType").execute()['mimeType']
    #print(docMimeType)
    ## files of mimeType "application/vnd.google-apps.document" do not have a size field! As such, there is no sense in
        ## checking their size (and doing it this way will prevent any related issues):
    if docMimeType != "application/vnd.google-apps.document":
        docSize = request.get(fileId = FILE_ID, fields = "size").execute()["size"]
        ## If a file is 0 bytes, we will receive a 416 error. Given that these files have no content, we can circumvent
            ## this issue by simply returning an empty string instead of progressing forward with these strings.
        if docSize == '0':
            return('')
    
    if docMimeType == "application/vnd.google-apps.document":
        request = request.export_media(fileId = FILE_ID, mimeType='application/pdf')
    else:
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
    file_name = [get_file_ext(row['file_obj_'+str(i)]) for i in range(1,7)]
    audio_files = [get_file_fh(row['file_obj_'+str(i)]) for i in range(1,7)]

    # if (all(v is None for v in audio_files)):
    #     raise Exception("No files larger than 0 bytes.")

    ## Get week 
    week = timestamp.strftime("%V")

    for k in range(len(file_name)):
        ## Go through all of the audio file objects (may be mutliple per row)
        ## TODO: This is very messy
        ##       and it takes forever to run

  
        ## Unique ID
        ## There are multiple file objects per row
        uid = row['file_obj_'+str(k+1)][1]

        ## Audio File names can contain the word "backup" 
        ## I put that in the file name
        backup_or_main_label = 'backup' if 'backup' in file_name[k].lower() else 'main'
        
        ## Get the file extension here, this should be moved up to its own function
        ## and, should use mimeType not file ext. for filetype 
        ext = '.' + file_name[k].split('.')[-1]

        ## File name - HHID + ['backup' if backup] + uid
        # newfile = str(hhid) + '_' + backup + '_' + str(uid)
        newfile = file_name[k]

        ## If file doesn't exist, create directory structure
        if not os.path.exists('./files_out/' + backup_or_main_label + "/" + str(hhid)):
            os.mkdir('./files_out/' + backup_or_main_label + "/" + str(hhid))

        ## if there is an audio file, save it using the name created above
        if audio_files[k]!='':
            #with open('./files_out/' +   str(hhid)+ '/'+ newfile + ext, 'wb') as f:
            with open("./files_out/" + backup_or_main_label + "/" + str(hhid) + "/" + newfile, "wb") as f:
                audio_files[k].seek(0)
                f.write(audio_files[k].read())


## TODO: Also very messy
## This should be in a def __main__() 
sheet = get_sheets_data(SHEET_ID)

## Let's quickly clean the timestamp, and reorder the dataset to always been from oldest timestamp to newest.
## Thus, new entries will always be last and earlier entries will ALWAYS have the same row number (even as the
## dataset grows). We need to make sure that the date and time variables are being treated as datetime, and not characters
## (otherwise sorting on these variables will produce inaccurate results — for example, "9:00:00" alphabetically comes after "16:00:00",
## despite the former occuring before the latter chronologically).
timestampFromSheet = sheet["Timestamp"].str.split(" ", 1, expand = True)
sheet["date"] = timestampFromSheet[0]
sheet["date"] = pd.to_datetime(sheet.date)
sheet["date"] = sheet["date"].dt.strftime('%Y/%m/%d')
sheet["time"] = timestampFromSheet[1]
sheet["time"] = pd.to_datetime(sheet.time)
sheet["time"] = sheet["time"].dt.strftime('%H:%M:%S')

sheet = sheet.sort_values(by=['date', "time"], inplace=False)
sheet = sheet.reset_index(drop=True)

print(sheet)


## We want to create a log file, "completedDownloads.csv" that lists each submission, and whether or not the files in this row
    ## have been downloaded. If for whatever reason, the script gets disrupted (which happens for a variety of reasons, from 
    ## computers falloing asleep to stray errors in the Google API, this will allow us to continue from where we left off.

    ## JWK NOTE: This is not a perfect system. It makes assumptions, notably that individuals are not re-editing submissions that
        ## they have made previously (i.e. changing the linked files in a row, updating the HHID, etc). Since we are recording progress
        ## based on the index of each file, indeces that we have passed will NOT get re-visited.

        ## If fellows are in fact editing rows that already exist in the sheet, we could address the issue with updating HHIDs by 
        ## instead switching to a log system that looks more like a dictionary object. We could generate unique keys for each submission 
        ## (perhaps by joining the HHID, fellow name, timestamp, and file type), and have the corresponding value returned be 
        ## an array of associated files.

## If we already have an existing log, we'll pick up from where we left off:
if os.path.exists("completedDownloads.csv"):
    completedDownloads = pd.read_csv("completedDownloads.csv", index_col=0)
    completedDownloads
else: 
    ## If we don't, we'll need to create a new log file:
    completedDownloads = sheet[["Timestamp", "Fellow Full Name:", "Household ID:", "Which files are you uploading?"]]
    completedDownloads["dl_complete"] = False


## The Google Sheet that the log is generated from is updated in real time. That is, new rows are regularly added to this sheet.
    ## Since the log was created from a prior version, the log won't have the most recent submissions.

    ## Thus, if the most recent "sheet" file has more rows than the prior completed downloads log, we need to add those new rows
    ## to the list of rows that we have here.
    ## It is critical that rows are not being removed from the original sheet at any time. That is, we are depending upon
    ## the idea that these rows will always be in the sheet (which would severely affect the ordering of variables). 
    ## Otherwise, we would need to check which files had preivously been downloaded in another way.
most_recent_file_rows = sheet.shape[0]
## One extra row was added for the variable names:
completed_downloads_rows = completedDownloads.shape[0] 

print(most_recent_file_rows)
print(completed_downloads_rows)

if most_recent_file_rows > completed_downloads_rows:
    new_rows = sheet.iloc[completed_downloads_rows:most_recent_file_rows]
    new_rows = new_rows[["Timestamp", "Fellow Full Name:", "Household ID:", "Which files are you uploading?"]]
    new_rows["dl_complete"] = False
    completedDownloads = completedDownloads.append(new_rows)
    print("new rows added to completedDownloads log file!")

## Now, we start from the first file that hasn't been recorded as downloaded in the completedDownloads log:
firstFalseIndex = min(completedDownloads.index[completedDownloads['dl_complete'] == False].tolist())
print(firstFalseIndex)

## Iterate through the indices of all downloads that haven't been completed.
for z in range(firstFalseIndex, (completedDownloads.shape[0])):
    ## Save the files in this row of the Google Sheet:
    tempSheet = sheet.iloc[z:z+1:]
    print("starting with row " + str(z))
    for i in range(1,7): 
        tempSheet['file_obj_'+str(i)] = tempSheet['Audio file upload ' + str(i)].apply(lambda x: get_drive_file(clean_url(x)))
    tempSheet.apply(save_files, axis=1)
    print("finished " + str(z))

    ## Update the log accordingly, and save it.
    completedDownloads.set_value(z, "dl_complete", True)
    completedDownloads.to_csv("completedDownloads.csv")
