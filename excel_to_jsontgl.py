# -*- coding: utf-8 -*-
"""
eBao T_BATCH_JOB_processed.xlsx to Control-M job-def.json
input file: f'{defin_f}_processed.xlsx', 5 sheets: 'job_def','depend', 'email', 'cal', 'comp'
    sheet name 為此程式之輸入，不可更改 sheet name

重要參數設定:
    ordermethod: Control-M folder 自動或手動執行
        'Manual': 手動執行，測試跑批用
        'Automatic': Control-M 換日時自動執行，上線用
    uniq:  Control-M unique folder name 不可存在同名folder，同名則覆蓋
        例如'tgl': output folder name: '1#tglOLTP'

output json file:
    One Control-M folder, one json file.
    json file name is folder name
"""
import pandas as pd
import numpy as np
import json
import copy
import os, psutil
process = psutil.Process(os.getpid())
print(f'Memory Usage Preloading: {round(process.memory_info().rss/1024/1024/1024,2)} GB')

# you may want to update these variable values
new_data = False # Does your T_BATCH_JOB.xlsx need pre_process()?
datain = 'T_BATCH_JOB_processed_mailoff' # input file
help_needed = False # pre_process() argument
mail_off = False # pre_process() argument
ip = '10.67.70.16:9082' # eBao ip
output = True # Do you need output json file for Control-M?
trimmedList = False # Do you need a list of eBao jobname as in Control-M?
# df_datain['PARENT_ID'].fillna(value=na) b/f df_datain['PARENT_ID'].astype(string)
na = 999999 # fillna to avoid NoneType Error
ordermethod = 'Manual' # Control-M folder OrtherMethod
uniq = 'tst' # unique Control-M folder name and jobname

if new_data:
    from pre_process import pre_process
    datain = pre_process(datain, 'T_BATCH_JOB_DEPEND', 'mapping',\
                         'T_COMPONENT_DEF', help_needed, mail_off)
    print('Done pre-processing.')

df_dep = pd.read_excel(f'{datain}.xlsx', sheet_name='depend')
df_mail = pd.read_excel(f'{datain}.xlsx', sheet_name='email')
df_datain = pd.read_excel(f'{datain}.xlsx', sheet_name='job_def')
df_cal = pd.read_excel(f'{datain}.xlsx', sheet_name='cal')
df_comp = pd.read_excel(f'{datain}.xlsx', sheet_name='comp')

# if RI col exists
cols = df_datain.columns
RI = ''
if cols[-1].find('RI') != -1:
    RI = cols[-1]

#---------iterate thru df rows-------------------------------------------------
#---------universal properties: jobname(will be df.index) and ['Description']----------------
# df_datain['name'] = df_datain['JOB_ID'].astype(str) + '#' + df_datain['JOB_NAME']
df_datain['name'] = df_datain['JOB_ID'].astype(str).str.cat(df_datain['JOB_NAME'], sep=f'#{uniq}')

# Control-M jobname forbidden characters:
df_datain['name'] = df_datain['name'].str.replace(' ', '') # replace full space with half space
df_datain['name'] = df_datain['name'].str.replace('　', '')
df_datain['name'] = df_datain['name'].str.replace('/', '_')
df_datain['name'] = df_datain['name'].str.replace(':', '_')
df_datain['name'] = df_datain['name'].str.replace('(', '_')
df_datain['name'] = df_datain['name'].str.replace(')', '')
df_datain['name'] = df_datain['name'].str.replace('>=', '大於等於')
df_datain['name'] = df_datain['name'].str.replace('.', '_')
df_datain['name'] = df_datain['name'].str.replace('、', ',') # replace full sep with half sep

# eventname uses ['Description'](full jobname) instead of ['name'](trimmed)
df_datain['Description'] = df_datain['name']

# Control-M jobname max 64 characters. 1 Chinese character occupies 3 positions        
for irow, row in df_datain.iterrows():
    length = 0
    for ichar, char in enumerate(row['name']):
        if '\u4e00' <= char <= '\u9fa5':
            length += 3
        else:
            length += 1
        if length > 64:
            df_datain.at[irow,'name'] = row['name'][:ichar]
            break

if trimmedList:
    # df.where: keep df_datain['name'] if True; nan if False
    df_datain['trimmed'] = df_datain['name'].where(df_datain['name'] != df_datain['Description'])
    # df.to_excel or create an ExcelWriter obj for more features 
    df_datain.loc[:,['name', 'Description', 'trimmed']].to_excel('trimmed_jobname.xlsx')
    # axis=1 or ='columns'
    df_datain.drop('trimmed', axis=1, inplace=True)
    
#-----universal properties: ['DaysKeepActive', 'Application']-------------------------
df_datain['DaysKeepActive'] = '9'
df_datain['Application'] = 'eBao'

#--------------------['CreatedBy'] remove Chinese-------------------------
# Control-M 'CreatedBy' does not support Chinese input
df_datain['CreatedBy'] = df_datain['JOB_OWNER']
df_datain['CreatedBy'].fillna('GSS', inplace=True)
   
jobowners = df_datain['CreatedBy'].unique() # size=42
jobowners = np.delete(jobowners, np.where(jobowners=='GSS'))
jobowners_inemail = df_mail['JOB_OWNER'].unique() # size=44
set(jobowners).issubset(set(jobowners_inemail)) # False

ownermap = {}

for jobowner in jobowners:
    owner_letter = []
    # owner_letter = list(jobowner)
    for l in jobowner:
        if not '\u4e00' <= l <= '\u9fa5' and l != ' ' and l != '　' and l != ')' and l != '(':
            owner_letter.append(l)
        elif l == '(':
            owner_letter.append('_')
    owner_new = ''.join(owner_letter)
    ownermap.update({jobowner:owner_new})
    
df_datain['CreatedBy'].replace(ownermap, inplace=True)
df_mail['CreatedBy'] = df_mail['JOB_OWNER']
df_mail['CreatedBy'].replace(ownermap, inplace=True)      

jobowners_new = df_datain['CreatedBy'].unique() # size=38 cuz some job owners are writtin differently
jobowners_new = np.delete(jobowners_new, np.where(jobowners_new=='GSS')) # GSS not in email sheet
jobowners_inemail_new = df_mail['CreatedBy'].unique() # size=44
# making sure all job owners in job_def has an email in email sheet, which is needed in If/Actions
set(jobowners_new).issubset(set(jobowners_inemail_new)) # True  

#--------------------df_datain.set_index-------------------------       
# df.index is output JSON obj name
df_datain.set_index('name', inplace=True)

#---------df_datain['PARENT_ID'].fillna(value=na) b/f df_datain['PARENT_ID'].astype(string)
df_datain['PARENT_ID'].fillna(value=na, inplace=True)
df_datain['JOB_ID'] = df_datain['JOB_ID'].astype('int')
df_datain['PARENT_ID'] = df_datain['PARENT_ID'].astype('int')
df_datain['JOB_ID'] = df_datain['JOB_ID'].astype('string')
df_datain['PARENT_ID'] = df_datain['PARENT_ID'].astype('string')

#--------------------dependency (jobflow)-------------------------
# pass in a jobid and find its predecessors or succcessors
# 1 job can have multiple [predecessors] or successors
# find successors by RESTRICT; predecessors by DEPEND
def jobNeighbours(df, jobid, pre_or_suc):
    if pre_or_suc == 'pre':
        jobs = df[df['DEPEND']==jobid]['RESTRICT'].values
    elif pre_or_suc == 'suc':
        jobs = df[df['RESTRICT']==jobid]['DEPEND'].values
    for job in jobs:
        neighbourname = df_datain.loc[df_datain['JOB_ID'] == str(job),'Description'].item()
        yield neighbourname

df_datain['eventsToWaitFor'] = None
df_datain['eventsToDelete'] = None
df_datain['eventsToAdd'] = None
# needs this col when logic==0
df_datain["IfBase:Folder:CompletionStatus_99"] = None

# interate thru df_dep to find each row's successors and predecessors
# call depnd(df) twice: pass in logic0 and logic1 separately
def depend(df = df_dep):
    for i, row in df.iterrows():
        eventwait = {"Type":"WaitForEvents",
                 'Events': []
            }
        eventdel = {"Type":"DeleteEvents",
                 'Events': []
            }
        eventadd = {"Type":"AddEvents",
                 'Events': []
            }
        eventlogic = {"Type":"If:CompletionStatus",
                      "CompletionStatus":"NOTOK",
            
            }
        # eventWait = eventDel
        eventlst_suc=[]
        eventlst_pre=[]
        # dep and restrict are counterparts
        # in each iteration, do row['DEPEND'] then do row['RESTRICT'], then go to next row
        dep = row['DEPEND']
        depname = df_datain.loc[df_datain['JOB_ID'] == str(dep),'Description'].item()
        predecessors = jobNeighbours(df, dep, 'pre')
        for predecessor in predecessors:
            eventname = f'{predecessor}-TO-{depname}'
            eventlst_suc.append({ 'Event' : eventname })
            
    
        restrict = row['RESTRICT']
        restrictname = df_datain.loc[df_datain['JOB_ID'] == str(restrict),'Description'].item()
        successors = jobNeighbours(df, restrict, 'suc')
        n = 0
        
        # returns an array of logic values 0 or 1 
        logics = df[df['RESTRICT']==restrict]['LOGIC'].values
        # zip each successor with its logic value
        for successor, logic in zip(successors, logics):
            eventname = f'{restrictname}-TO-{successor}'
            eventlst_pre.append({ 'Event' : eventname })
            
            event_inif = {"Type":"Event:Add",
                          'Event':''
                }
            if logic == 0:
                event_inif.update({'Event':eventname})
                eventlogic.update({f'Event:Add_{n}': event_inif})
                ilogic = df_datain.index[df_datain['Description']==restrictname][0]
                df_datain.at[ilogic,'IfBase:Folder:CompletionStatus_99']=eventlogic
                n += 1
            
        eventwait.update({ 'Events': eventlst_suc } )
        eventdel.update({ 'Events': eventlst_suc } ) 
        eventadd.update({ 'Events': eventlst_pre } ) 
        
        idep = df_datain.index[df_datain['Description']==depname][0]
        irestrict = df_datain.index[df_datain['Description']==restrictname][0]
        
        df_datain.at[idep,'eventsToWaitFor']=eventwait
        df_datain.at[idep,'eventsToDelete']=eventdel
        df_datain.at[irestrict,'eventsToAdd']=eventadd

depend()

#----------------determine each row is a job, a sub, or a folder----------------------    
df_datain.rename(columns = {'JOB_NET': 'Type'}, inplace=True)
df_datain['Type'].mask(df_datain['Type']=='Y', 'SubFolder', inplace=True)
df_datain['Type'].mask(df_datain['Type']=='N', 'Job:Command', inplace=True)
# the row is a folder if the row has no parent_id 
df_datain['Type'].mask(df_datain['PARENT_ID']==str(na),'Folder', inplace=True)
# .replace is an alternative to .mask/.where
# Type = {'Y':'SubFolder','N':'Job:Command'}
# df_datain['Type'].replace(Type, inplace=True)
# df_datain.loc[df_datain['PARENT_ID'].isna(),'Type'] = 'Folder'


#------------creating 3 df to separate jobs, subs, and folders---------------
df_folder = df_datain.loc[df_datain['Type'] == 'Folder']
df_folder.dropna(axis='columns', how='all', inplace=True)
df_sub = df_datain.loc[df_datain['Type'] == 'SubFolder']
df_sub.dropna(axis='columns', how='all', inplace=True)
df_job = df_datain.loc[df_datain['Type'] == 'Job:Command']
df_job.dropna(axis='columns', how='all', inplace=True)

#----------------------import calendar lists---------------------------
df_cal.dropna(axis='rows', inplace=True)
cals = set(df_cal.iloc[:,0])

# Folder property: 'ControlmServer', 'ActiveRetentionPolicy', 'AdjustEvents', 'When', 'OrderMethod']
df_folder['ControlmServer'] = 'CTM'
df_folder['ActiveRetentionPolicy'] = 'CleanEndedOK'
df_folder['AdjustEvents'] = True
df_folder['OrderMethod'] = ordermethod
df_folder['Confirm'] = True

# *df_cnt cus ValueError: Length of values (1) does not match length of index (1029)
folder_cnt = len(df_folder)
df_folder['When'] = [{"ToTime":">",
                    "RuleBasedCalendars":{
                        "Included":list(cals)} }]*folder_cnt

df_folder['Variables'] = [[{"pDate":'2022-04-27'},
                           {"partial_success":"/ctm/life/partial_success.py" }]]*folder_cnt
#----- Sub property: ['AdjustEvents', 'When']-----------
df_sub['AdjustEvents'] = True

sub_cnt = len(df_sub)
df_sub['When'] = [{"ToTime":">",
                    "DaysRelation":"OR",
                    "RuleBasedCalendars":{
                        "Relationship":"AND",
                        "Included":['USE PARENT']} }]*sub_cnt

#--- job property: ['PostCommand', 'Host', 'RunAs', 'RunAsDummy', 'Variables']--------
df_job['PostCommand'] = '/ctm/ctmsvr/bmcpython/bmcpython_V2/python %%partial_success %%ORDERID %%RUNCOUNT %%tolerance %%JOBNAME'
df_job['Host'] = 'CTMAG'
df_job['RunAs'] = 'life'

df_job.rename(columns = {'DUE_TIME': 'RunAsDummy'}, inplace=True)
# 'RunAsDummy':true in output json if DUE_TIME:2099/12/31 in input T_BATCH-JOB.xlsx 
df_job['RunAsDummy'].fillna(na, inplace=True)
df_job['RunAsDummy'].mask(df_job['RunAsDummy'].str.contains('99'), True, inplace=True)
df_job['RunAsDummy'].where(df_job['RunAsDummy'].str.contains('99'), False, inplace=True)

job_cnt = len(df_job)
df_job['Variables'] = [[{"tolerance":'10'}]]*job_cnt

#------------------------- job property: ['FileName']------------------------------
df_job.rename(columns = {'COMPONENT': 'FileName'}, inplace=True)

compt = tuple(zip(df_comp.iloc[:,0], df_comp.iloc[:,1]))
compd = dict(compt)
df_job['FileName'].replace(compd, inplace=True)

#--------------------['When']-calendars------------------------
freq_cols = ['FREQUENCY', 'FREQUENCY_MULTIPLE', 'FREQUENCY2_MONTH', 'FREQUENCY3_DAY',\
         'FREQUENCY3_MULTIPLE', 'FREQUENCY3_WEEKLYDAY']
    
df_job[freq_cols] = df_job[freq_cols].convert_dtypes(convert_integer=True)

df_job['cal'] = None
df_job['cal'].mask( (df_job[freq_cols[0]]==4) & \
                       (df_job[freq_cols[1]]==1) & \
                       (df_job[RI]=='Y'), 'TWDAD', inplace=True)
                   
df_job['cal'].mask( (df_job[freq_cols[0]]==4) & \
                       (df_job[freq_cols[1]]==1) & \
                       (df_job[RI]!='Y'), 'TWCAD', inplace=True)

df_job['cal'].mask( (df_job[freq_cols[0]]==4) & \
                       (df_job[freq_cols[1]]==-1), 'TWDWD', inplace=True)

df_job['cal'].mask( (df_job[freq_cols[0]]==3) & \
                       (df_job[freq_cols[1]]==1) & \
                       (df_job[freq_cols[-1]]==1), 'TWW01', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==3) & \
                       (df_job[freq_cols[1]]==1) & \
                       (df_job[freq_cols[-1]]==7), 'TWW07', inplace=True)

df_job['cal'].mask( (df_job[freq_cols[0]]==2) & \
                       (df_job[freq_cols[1]]==1) & \
                       (df_job[freq_cols[-2]]==2) & \
                       (df_job[freq_cols[-1]]==5), 'TWM00', inplace=True)

df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==1), 'TWM01', inplace=True)

df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==2), 'TWM02', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==5), 'TWM05', inplace=True)

df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==10), 'TWM10', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==15), 'TWM15', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==20), 'TWM20', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==28), 'TWM28', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==-2), 'TWM02S', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==-1), 'TWM01S', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==8) & \
                    (df_job[freq_cols[1]]==6) & \
                    (df_job[freq_cols[-3]]==1), 'TWH301', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==13) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[-3]]==1), 'TWM01L', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==13) & \
                    (df_job[freq_cols[1]]==3) & \
                    (df_job[freq_cols[-3]]==1), 'TWQ11L', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==10) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==0) & \
                    (df_job[freq_cols[3]]==1), 'TWY00', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==10) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==0) & \
                    (df_job[freq_cols[3]]==-1), 'TWY01', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==10) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==7) & \
                    (df_job[freq_cols[3]]==31), 'TWY02', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==10) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==3) & \
                    (df_job[freq_cols[3]]==30), 'TWY03', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==14) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==9) & \
                    (df_job[freq_cols[3]]==1), 'TWY04', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==14) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==1) & \
                    (df_job[freq_cols[3]]==1), 'TWY05', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==14) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==11) & \
                    (df_job[freq_cols[3]]==1), 'TWY06', inplace=True)
    
df_job['cal'].mask( (df_job[freq_cols[0]]==14) & \
                    (df_job[freq_cols[1]]==1) & \
                    (df_job[freq_cols[2]]==0) & \
                    (df_job[freq_cols[3]]==1), 'TWY07', inplace=True)

# df_job['FREQUENCY'] == 5 or 6 are cyclic jobs    
df_job['cal'].mask( (df_job[freq_cols[0]]==5) | (df_job[freq_cols[0]]==6), \
                       'TWDAD', inplace=True)

# iterate ['cal'] later in If/Action section

#--------------------separating _1# and _2# from other jobs---------------------
df_jobebao = df_job
# df_jobebao = df_job.iloc[:-2,:]
# df_job_1_2 = df_job.iloc[[-1,-2],:]

# job_1_2_cnt = len(df_job_1_2)

# df_job_1_2['PreCommand'] = " ctmcontb -DELETE \"%%JOBNAME-TO*\" \"*\" "
# df_job_1_2['Command'] = "echo \"pDateNoCyclic=\"%%pDateNoCyclic;"
# df_job_1_2.at['_2#Cyclic', 'Command'] = "echo \"pDateCyclic=\"%%pDateCyclic;"

# df_job_1_2['Confirm'] = True

# df_job_1_2['Variables'] = [[ {"\\pDateNoCyclic":"2022-01-02"   }  ]]*job_1_2_cnt
# df_job_1_2.at['_2#Cyclic', 'Variables'] = [ 
#                                 {"\\partial_success":"/ctm/life/partial_success.py" },
#                                 {"\\pDateCyclic":"2022-01-01"}
#                                 ]
# df_job_1_2['When'] = [ {"WeekDays":["NONE"],\
#                        "MonthDays":["ALL"],\
#                         "ToTime":">",
#                         "DaysRelation":"OR",
#                         "RuleBasedCalendars":{
#                             "Relationship":"AND",
#                             "Included":[ 'TWDAD' ]
#                                         } 
#                         }  ]*job_1_2_cnt
    
#--------------------job property ['Command'] NoCyc and cyclic----------------------
back_slash2 = '\\\\' # resolves to double back_slash
squigglyL = '{'
squigglyR = '}'
# %%pDate is a self-defined Control-M variable
pDate = '%%pDate'


df_jobebao['Command'] = (f'curl -H \\"Content-Length: 250\\" '                     
                f'-H \\"Content-Type: application/json\\" '
                f'-H \\"Accept: application/json\\" '  
            f'http://{ip}/ls/rest/batch/start.d -X POST -d \\"'
            f'{squigglyL}{back_slash2}\\"jobId{back_slash2}\\":'
            + df_jobebao['JOB_ID'].astype(str) + ', '
            f'{back_slash2}\\"processDate{back_slash2}\\"'
            f':{back_slash2}\\"{pDate}{back_slash2}\\", '
            f'{back_slash2}\\"dueDate{back_slash2}\\"'
            f':{back_slash2}\\"1900-01-01{back_slash2}\\"{squigglyR}\\" -v')

# df_job['FREQUENCY'] == 5 or 6 are cyclic jobs    
# df_jobebao['Command'].mask( (df_jobebao[freq_cols[0]]==5) | (df_jobebao[freq_cols[0]]==6), \
#                        df_jobebao['Command'].str.replace('%%pDateNoCyclic', '%%pDateCyclic'), inplace=True)

#--------------------job property If:Actions ----------------------
email=''
jobebao_cnt = len(df_jobebao)
df_jobebao['IfBase:Folder:Output_0'] = [{"Type":"If:CompletionStatus",
                                     "CompletionStatus":"ANY",
                                     "Action:SetToNotOK_0":{
                                         "Type":"Action:SetToNotOK"}
                                     },]*jobebao_cnt

df_jobebao['IfBase:Folder:Output_1'] = [{"Type":"If:Output",
                                     "Code":"*\"status\":107*",
                                     "Action:SetToOK_0":{
                                         "Type":"Action:SetToOK"}
                                     },]*jobebao_cnt

if105dct = {"Type":"If:Output",
          "Code":"*\"status\":105*",
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 105 execution failure."
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 105 execution failure.",
           "Message":"%%JOBNAME %%$ODATE 105 execution failure.",
           "Urgency":"Urgent",
           "To": email}
         }
if105s = []

if223dct = {"Type":"If:Output",
          "Code":"*\"status\":223*",
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 223 STOP"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 223 STOP",
           "Message":"%%JOBNAME %%$ODATE 223 STOP",
           "Urgency":"Urgent",
           "To": email}
         }
if223s = []

if106_abvdct = {"Type":"If:Output",
          "Code":"*Attention! Fail count exceeds tolerance*",
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 106 partially success. Failure counts over %%tolerance"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 106 partially success. Failure counts over %%tolerance",
           "Message":"%%JOBNAME %%$ODATE 106 partially success. Failure counts over %%tolerance",
           "Urgency":"Urgent",
           "To": email}
         }
if106_abvs = []

if106_withindct = {"Type":"If:Output",
          "Code":"*Fail count within tolerance*",
          "Action:SetToOK_0":{"Type":"Action:SetToOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 106 partially success. Failure counts under %%tolerance"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 106 partially success. Failure counts under %%tolerance",
           "Message":"%%JOBNAME %%$ODATE 106 partially success. Failure counts under %%tolerance",
           "Urgency":"Urgent",
           "To": email}
         }
if106_withins = []

if995dct = {"Type":"If:Output",
          "Code":"*\"status\":995*",
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 995 error in submission"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 995 error in submission",
           "Message":"%%JOBNAME %%$ODATE 995 error in submission",
           "Urgency":"Urgent",
           "To": email}
         }
if995s = []

if996dct = {"Type":"If:Output",
          "Code":"*\"status\":996*",
          "Action:SetToOK_0":{"Type":"Action:SetToOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 996 write t_batch_job.due_time error, please data patch the current data"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 996 write t_batch_job.due_time error, please data patch the current data",
           "Message":"%%JOBNAME %%$ODATE 996 write t_batch_job.due_time error, please data patch the current data",
           "Urgency":"Urgent",
           "To": email}
         }
if996s = []

if997dct = {"Type":"If:Output",
          "Code":"*\"status\":997*",
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 997 not started"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 997 not started",
           "Message":"%%JOBNAME %%$ODATE 997 not started",
           "Urgency":"Urgent",
           "To": email}
         }
if997s = []

if998dct = {"Type":"If:Output",\
          "Code":"*\"status\":998*",\
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},\
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 998 over 90min",
           "Message":"%%JOBNAME %%$ODATE 998 over 90min",
           "Urgency":"Urgent",
           "To": email}
         }
if998s = []

if999dct = {"Type":"If:Output",
          "Code":"*\"status\":999*",
          "Action:SetToNotOK_0":{"Type":"Action:SetToNotOK"},
          "DoNotify_0": {
              "Type": "Action:Notify",
              "Urgency": "Urgent",
              "Message": "%%JOBNAME %%$ODATE 999 unknown exception"
              },
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE 999 unknown exception",
           "Message":"%%JOBNAME %%$ODATE 999 unknown exception",
           "Urgency":"Urgent",
           "To": email}
         }
if999s = []

if_notokdct = {"Type":"If:CompletionStatus",\
          "CompletionStatus":"NOTOK",\
         "Mail_1":{
           "Type":"Action:Mail",
           "Subject":"%%JOBNAME %%$ODATE failed",
           "Message":"%%JOBNAME %%$ODATE failed",
           "Urgency":"Urgent",
           "To": email}
         }
if_notoks = []

# iterate When and If/Action together
cal = ''

# []#num is shallow copy and dct is mutable so we need copy.deepcopy
Whendct = {"WeekDays":["NONE"],\
               "MonthDays":["ALL"],\
                "ToTime":">",
                "DaysRelation":"OR",
                "RuleBasedCalendars":{
                    "Relationship":"AND",
                    "Included":[ cal ]
                                } 
                }
Whens = []

ifdcts = [if105dct, if223dct, if106_abvdct, if106_withindct, if995dct, if996dct, if997dct,\
          if998dct, if999dct, if_notokdct, Whendct]# len(ifdcts) = 11

if_lsts = [if105s, if223s, if106_abvs, if106_withins, if995s, if996s, if997s,\
          if998s, if999s, if_notoks, Whens]

for i in range(jobebao_cnt):
    for if_lst, ifdct in zip(if_lsts, ifdcts):
        if_lst.append(copy.deepcopy(ifdct))
        
for When, cal, if105, if223, if106_abv, if106_within, if995, if996, if997, if998, if999, if_notok,\
    job_owner in \
    zip(Whens, df_jobebao['cal'], \
        if105s, if223s, if106_abvs, if106_withins, if995s, if996s, if997s, if998s, if999s, \
        if_notoks, df_jobebao['CreatedBy']):
    When['RuleBasedCalendars'].update({'Included':[cal]})
    email = df_mail['Email'].tail(1).item()
    email += df_mail[df_mail['CreatedBy']==job_owner]['Email'].item()
    if105['Mail_1'].update({'To':email})
    if223['Mail_1'].update({'To':email})
    if106_abv['Mail_1'].update({'To':email})
    if106_within['Mail_1'].update({'To':email})
    if995['Mail_1'].update({'To':email})
    if996['Mail_1'].update({'To':email})
    if997['Mail_1'].update({'To':email})
    if998['Mail_1'].update({'To':email})
    if999['Mail_1'].update({'To':email})
    if_notok['Mail_1'].update({'To':email})

df_jobebao['When'] = Whens
df_jobebao['IfBase:Folder:Output_2'] = if105s
df_jobebao['IfBase:Folder:Output_3'] = if223s
df_jobebao['IfBase:Folder:Output_4'] = if106_abvs
df_jobebao['IfBase:Folder:Output_5'] = if106_withins
df_jobebao['IfBase:Folder:Output_6'] = if995s
df_jobebao['IfBase:Folder:Output_7'] = if996s
df_jobebao['IfBase:Folder:Output_8'] = if997s
# df_jobebao['IfBase:Folder:Output_9'] = if998s  # status:998 no longer shown in response
df_jobebao['IfBase:Folder:Output_10'] = if999s
df_jobebao['IfBase:Folder:Output_98'] = if_notoks

#--------prep for if106_withins. Investigating logic=0 are folders(no If/Action) or jobs---------------

logic0 = set(df_dep[df_dep['LOGIC']==0]['RESTRICT'].astype(str)) # len(logic0) = 137
jobs = set(df_job['JOB_ID']) # len(jobs) = 734
logic0_jobs = jobs.intersection(logic0) # len(logic0_jobs) = 83 jobs are logic=0

folders = set(df_folder['JOB_ID']) 
folders = folders.union(set(df_sub['JOB_ID'])) # len(folders) = 295
logic0_folders = folders.intersection(logic0) # len(logic0_folders) = 47 folders are logic=0

#--------------------------If/Action Event:Add to status:105, 106_abv, 223------------------------------

# when logic0
for i, row in df_jobebao[df_jobebao['IfBase:Folder:CompletionStatus_99'].notna()].iterrows():
    for num, event in enumerate(row['eventsToAdd']['Events']):
        event_inif = {"Type":"Event:Add",
                      'Event':'' }
        event_inif.update({'Event': event['Event']})
        # pass events to successors when these 2 ifs
        row['IfBase:Folder:Output_2'].update({f'Event:Add_{num}': event_inif})
        row['IfBase:Folder:Output_3'].update({f'Event:Add_{num}': event_inif})
        row['IfBase:Folder:Output_4'].update({f'Event:Add_{num}': event_inif})
        
    
#--------job property ['Rerun'], ['IfBase:Folder:Output_11'], ['DaysKeepActive']---------------
# df_job['FREQUENCY'] == 5 or 6 are cyclic jobs    
df_jobrerun = df_jobebao[(df_jobebao[freq_cols[0]]==5) | (df_jobebao[freq_cols[0]]==6)]

minute = '0'
rerunlst = []
rerundct = {"Units":"Minutes",
            "Every": minute }

jobrerun_cnt = len(df_jobrerun)
for i in range(jobrerun_cnt):
    rerunlst.append(copy.deepcopy(rerundct))
    
for rerun, hfreq, mfreq in zip(rerunlst, df_jobrerun[ freq_cols[0] ], \
                               df_jobrerun[ freq_cols[1] ] ):
    rerun.update({ 'Every':str(mfreq) })
    if hfreq == 5:
        rerun.update({ 'Units':'Hours' })
        
df_jobrerun['Rerun'] = rerunlst
df_jobrerun['IfBase:Folder:Output_11'] = [{"Type":"If:NumberOfFailures",
                                     "NumberOfFailures":"10",
                                     "Action:StopCyclicRun_0":{
                                         "Type":"Action:StopCyclicRun"}
                                     },]*jobrerun_cnt
df_jobrerun['DaysKeepActive'] = 'Forever'

#------------------sorting df and dropping columns--------------------------        
#------------------------------------Folders: 2 df-------------------------------    
# ['eventsToWaitFor'] and ['eventsToDelete'] have the same values
# df_folder[:, -2:] -> 2 cols as 1 so 2 combinations(2 to the power of 1), all values or all none

df_folder = df_folder.loc[:, ['Type', 'ControlmServer','Description', 'ActiveRetentionPolicy',\
                              'DaysKeepActive', 'AdjustEvents', 'Confirm', 'OrderMethod', 'Application',\
                              'CreatedBy', 'Variables', 'When' ]]

# 2 non-cyclic folders: 1#OLTP and 1337182#
# df_folder_allcol = df_folder[df_folder.all(axis='columns', skipna=False)] # all values 
# df_folder_allcol['OrderMethod'] = ordermethod

# # cyclic folders and _0#Gate
# df_folder_dropcol = df_folder[~df_folder.all(axis='columns', skipna=False)]
# df_folder_dropcol.dropna(axis='columns', how='all', inplace=True) # all None
# # df_folder_dropcol.at['_0#Gate', 'OrderMethod'] = ordermethod
# df_folder_dropcol['OrderMethod'].mask(df_folder_dropcol['Description'].str.contains('_0#Gate'),ordermethod, inplace=True)

# df_folder_dropcol['DaysKeepActive'].where(df_folder_dropcol['Description'].str.contains('_0#Gate'),'Forever', inplace=True)

df_folder['DaysKeepActive'] = 'Forever'
df_folder.at[f'1#{uniq}OLTP', 'DaysKeepActive'] = '9'
df_folder.at[f'1337182#{uniq}SA_PA', 'DaysKeepActive'] = '9'

# df_splited = [ df_folder_allcol, df_folder_dropcol ]
df_splited = [ df_folder ]

#-------------------Subs, jobebao, jobrerun: 6 df each. Plus _1_2------------------------------  
# if ['IfBase:Folder:CompletionStatus_99'] != None, ['eventsToAdd'] has value
# df_folder[:, -4:] -> 4 cols as 3, 6 combinations(2 to the power of 2 plus 2)
df_sub = df_sub.loc[:, ['Type', 'Description', 'DaysKeepActive', 'AdjustEvents',\
                        'Application', 'CreatedBy', 'When', 'eventsToWaitFor', 'eventsToDelete',\
                         'eventsToAdd', 'IfBase:Folder:CompletionStatus_99']]

    
ebaocols = ['Type', "FileName", 'Description', 'DaysKeepActive', 'Application', 'CreatedBy', 'Host', 'RunAs',\
           'Command', 'PostCommand', 'RunAsDummy', 'Variables', 'When', 'IfBase:Folder:Output_0',\
           'IfBase:Folder:Output_1', 'IfBase:Folder:Output_2','IfBase:Folder:Output_3',\
            'IfBase:Folder:Output_4', 'IfBase:Folder:Output_5', 'IfBase:Folder:Output_6',\
           'IfBase:Folder:Output_7', 'IfBase:Folder:Output_8',\
             'IfBase:Folder:Output_10', 'IfBase:Folder:Output_98', \
          'eventsToWaitFor', 'eventsToDelete', 'eventsToAdd','IfBase:Folder:CompletionStatus_99']
df_jobebao = df_jobebao.loc[:, ebaocols]    

reruncols = copy.deepcopy(ebaocols)
reruncols.insert(reruncols.index('When'), 'Rerun')
reruncols.insert(reruncols.index('IfBase:Folder:Output_98'), 'IfBase:Folder:Output_11')
df_jobrerun = df_jobrerun.loc[:, reruncols]    

# _1_2cols = copy.deepcopy(ebaocols)
# _1_2cols = _1_2cols[: _1_2cols.index('IfBase:Folder:Output_0')]
# _1_2cols.remove('PostCommand')
# _1_2cols.remove('RunAsDummy') 
# _1_2cols.remove("FileName")
# _1_2cols.insert(_1_2cols.index('Command'), 'PreCommand')
# _1_2cols.insert(_1_2cols.index('PreCommand'), 'Confirm')
# _1_2cols.append('eventsToAdd')
# df_job_1_2 = df_job_1_2.loc[:, _1_2cols]


# The 3 dfs have the 4 cols: ['eventsToWaitFor'], ['eventsToDelete'], ['eventsToAdd'], ['IfBase:Folder:CompletionStatus_99']
df_nosplit = df_sub, df_jobebao, df_jobrerun # type(dfs) = tuple

for df in df_nosplit:
    if 'RunAsDummy' in df.columns:
        # df.all returns whether all elements are True so can't have values as False
        df['RunAsDummy'].replace(False, 'f', inplace=True)
    for i in range(6):
        if i == 0:
            # 4-col combo: all values
            tmp = df[df.all(axis='columns', skipna=False)] # len(df_suballcol)=36
        elif i == 1:
            # 3-col combo
            tmp = df[df.iloc[:,:-1].all(axis='columns', skipna=False)] #len(tmp)=82
            tmp = tmp[tmp.iloc[:,-1].isna()] #len(tmp)=46
        elif i == 2:
            # 2-col combo
            tmp = df[df.iloc[:,:-2].all(axis='columns', skipna=False)] #len(tmp)=118
            # find rows with last 2 cols of None value
            tmpbool = tmp.iloc[:,[-1,-2]].isna() # 2-col df of True or False
            tmpbool = tmpbool.all(axis='columns', skipna=False) # Series of True or False
            tmp = tmp[tmpbool] #len(tmp)=36
        elif i == 3:
            # 2-col combo reverse
            tmp = df[df.iloc[:,-2:].all(axis='columns', skipna=False)] #len(tmp)=54
            # find rows with [-4:-2] cols of None value
            tmpbool = tmp.iloc[:,-4:-2].isna() # 2-col df of True or False
            tmpbool = tmpbool.all(axis='columns', skipna=False) # Series of True or False
            tmp = tmp[tmpbool] #len(tmp)=18
        elif i == 4:
            # only 3rd col combo
            tmp = df[df.iloc[:,-2:-1].all(axis='columns', skipna=False)] #len(tmp)=120
            # find rows with [-1,-3,-4] cols of None value
            tmpbool = tmp.iloc[:,[-1,-3,-4]].isna() # 3-col df of True or False
            tmpbool = tmpbool.all(axis='columns', skipna=False) # Series of True or False
            tmp = tmp[tmpbool] #len(tmp)=20
        elif i == 5:
            # 4-col None combo
            # find rows with [-4:] cols of None value
            tmpbool = df.iloc[:,-4:].isna() # 4-col df of True or False
            tmpbool = tmpbool.all(axis='columns', skipna=False) # Series of True or False
            tmp = df[tmpbool] #len(tmp)=131
        if 'RunAsDummy' in tmp.columns:
            # df.all returns whether all elements are True so can't have values as False
            tmp['RunAsDummy'].replace('f', False, inplace=True)
        if not tmp.empty:
            if tmp['Type'][0] =='Job:Command':
                tmp.drop(columns=['IfBase:Folder:CompletionStatus_99'], inplace=True)
            tmp.dropna(axis='columns', how='all', inplace=True) # 7 columns
            df_splited.append(tmp)

            
#------------------converting all df to dct and combing to 1 dct--------------------
# to_json returns a dict-like string
# force_ascii=False to show Chinese as-is; otherwise in excaped sequence '\u'
# jstr_folder = df_folder.to_json(orient='index', force_ascii=False)
# obj_folder = json.loads(jstr_folder)

obj = {}
obj_tops = {}
# tops = set(df_folder_allcol.index).union(set(df_folder_dropcol.index))
tops = set(df_folder.index)

for df in df_splited:
    tmp = copy.deepcopy(obj)
    tmp = df.to_dict(orient='index')
    obj = {**obj, **tmp}
    if set(tmp.keys()).issubset(tops):
        obj_tops = {**obj_tops, **tmp}


# df_job_1_2.loc['_1#NoCyclic'] returns a series (13,1)
# desired df size: (1 row, 13 cols) so pd.DataFrame([[series]])
# df_1 = pd.DataFrame( [  df_job_1_2.loc['_1#NoCyclic']  ] )
# df_1.dropna(axis='columns', how='all', inplace=True)
# df_2 = pd.DataFrame([df_job_1_2.loc['_2#Cyclic']])
# df_2.dropna(axis='columns', how='all', inplace=True)

# obj = {**obj, **df_1.to_dict(orient='index'), **df_2.to_dict(orient='index')}
# obj_tops={**df_folder_allcol.to_dict(orient='index'), **df_folder_dropcol.to_dict(orient='index')}

#-----------------------recursive functions----------------------------   
# pass in parent_objname and return df.index object
def findKids2(parent):
    # remove string in parent_objname and get the num
    parent = str(parent).split(f'#{uniq}',1)
    kids = df_datain.index[df_datain['PARENT_ID']==parent[0]]
    return kids

# my winner top-down approach
def cometoMama2(dct=obj):
    dataout = dct
    # o can be json job properties e.g. "Type" in second layer
    for o in dataout:
        kids = findKids2(o)
        if len(kids) > 0:
            for kid in kids:
                kid = str(kid)
                dataout.get(o).update({kid:obj.get(kid)})
            dataout.update({ o : cometoMama2(dataout.get(o)) })
    return dataout
dataout = cometoMama2()

# mutable obj is updated when dataout updated

if output:
    for obj in obj_tops:
        with open(f'{obj}.json', 'w', encoding='UTF-8') as f:        
            f.write(json.dumps({obj:obj_tops.get(obj)}, indent=4, ensure_ascii=False))
    
process = psutil.Process(os.getpid())
print(f'Memory Usage Postloading: {round(process.memory_info().rss/1024/1024/1024,2)} GB')
