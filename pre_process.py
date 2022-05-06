# -*- coding: utf-8 -*-
"""
eBao T_BATCH_JOB.xlsx to Control-M job-def.json - raw data pre-process
input xlsx list:
    1. T_BATCH_JOB.xlsx - eBao job list. 需手動新增「是否為RI批次」於末欄並將RI批次標記'Y'
    2. T_BATCH_JOB_DEPEND.xlsx - eBao job dependency list
    3. mapping.xlsx (2 sheets)- eBao job_owner email mapping and Control-M calendar lists
    4. T_COMPONENT_DEF.xlsx - eBao job component-module mapping

預處理內容
    1. T_BATCH_JOB.xlsx drop RUN_TYPE:2 rows
    2. T_BATCH_JOB.xlsx drop JOB_NAME contains 'Delete' rows
    3. def pre_process() parameter: help_needed
        True - T_BATCH_JOB.xlsx 需要整理
        False - T_BATCH_JOB.xlsx 不需要整理
    4. def pre_process() parameter: mail_off
        True - 修改成假的 eamil address，測試跑批用
        False - 不更動 eamil address，上線用
    5. def pre_process() parameter: logic0
        True - update lodic = 0 ，測試跑批用
        False - 不更動 logic，上線用
ourput xlsx: f'{defin_f}_processed.xlsx', 5 sheets: 'job_def','depend', 'email', 'cal', 'comp'
    sheet name 為下一段程式之輸入，不可更改 sheet name
"""
import pandas as pd

def pre_process(defin, depend, mapping, comp, help_needed=False, mail_off='', logic0=''):
    defin = f'{defin}.xlsx'
    depend = f'{depend}.xlsx'
    mapping = f'{mapping}.xlsx'
    comp = f'{comp}.xlsx' 
    processed = defin[:-5] + f'_processed_{logic0}{mail_off}.xlsx'
    df_email = pd.read_excel(mapping, sheet_name='email')
    df_cal = pd.read_excel(mapping, sheet_name='freq_to_cal', usecols='I', keep_default_na=False)
    df_comp = pd.read_excel(comp)
    
    # read excel to a dataframe
    df_job = pd.read_excel(defin)
    df_dep = pd.read_excel(depend)
    
    # drop RUN_TYPE:2 rows
    df_job = df_job[df_job['RUN_TYPE'] == 1]
    # df = df[df['DISABLED'] == 'N']
    # 1337122 DISABLED:Y but we want to keep this row
    # drop JOB_NAME:Delete*
    df_job = df_job[df_job['JOB_NAME'].str.contains('Delete')==False]

    # set df index and modify data
    if help_needed:
        df_job = df_job.set_index('JOB_ID')
        df_job.at[1337122,'DUE_TIME'] = '99'
        df_job.at[1337122,'FREQUENCY'] = 6
        df_job.at[1337122,'DISABLED'] = 'N'
        df_job.at[1333762,'PARENT_ID'] = None
        df_job.at[168,'FREQUENCY_MULTIPLE'] = 1

        new_job_owner={'Rock Chen 陳澄高': 'Rock Chen 陳澄高 (003001)',
                       'Vita 朱益貞(001758)': 'Vita Chu 朱益貞(001758)',
                       'IBM Alex Lin 林逸德': 'Alex Lin 林逸德 (002997)',
                    'Carlos Linag 梁尊凱 (002186)': 'Carlos Liang 梁尊凱 (002186)' }
            
        df_job['JOB_OWNER'].replace(new_job_owner, inplace=True)
        
    # convert index back to column
        df_job.reset_index(inplace=True)
        df_job.rename(columns={'index':'JOB_ID'})

    # turn off Control-M email notification
    if mail_off:
        df_email['Email'] = df_email['Email'].str.replace('transglobe', 'transglobee')
    
    if logic0:
        df_dep['LOGIC'] = 0

    # add rows _0#Gate, _1#NoCyclic, _2#Cyclic and their dependency
    # _0 = {'JOB_ID':'_0', 'JOB_NET':'Y', 'JOB_NAME':'Gate'}
    # _1 = {'JOB_ID':'_1', 'JOB_NET':'N', 'PARENT_ID':'_0', 'JOB_NAME':'NoCyclic',
    #       'FREQUENCY':4, 'FREQUENCY_MULTIPLE':1}
    # _2 = {'JOB_ID':'_2', 'JOB_NET':'N', 'PARENT_ID':'_0', 'JOB_NAME':'Cyclic', 
    #       'FREQUENCY':4, 'FREQUENCY_MULTIPLE':1}

    # df_job = df_job.append(_0, ignore_index=True)
    # df_job = df_job.append(_1, ignore_index=True)
    # df_job = df_job.append(_2, ignore_index=True)
    
    # job1_dep = {'RESTRICT':'_1', 'DEPEND':1, 'LOGIC':1}
    # job1337182_dep = {'RESTRICT':'_1', 'DEPEND':1337182, 'LOGIC':1}
        
    # df_dep = df_dep.append(job1_dep, ignore_index=True)
    # df_dep = df_dep.append(job1337182_dep, ignore_index=True)
    
    # # if RI column exists
    # cols = df_job.columns
    # if cols[-1].find('RI') != -1:
    #     df_job.iloc[-1,-1]='Y'
    #     df_job.iloc[-2,-1]='Y'

    with pd.ExcelWriter(processed) as writer:
        df_job.to_excel(writer, sheet_name='job_def', index=False)
        df_dep.to_excel(writer, sheet_name='depend', index=False)
        df_email.to_excel(writer, sheet_name='email', index=False)
        df_cal.to_excel(writer, sheet_name='cal', index=False)
        df_comp.to_excel(writer, sheet_name='comp', index=False)
    
    return str(processed)[:-5]
        
if __name__=='__main__':
    processed_f = pre_process('T_BATCH_JOB', 'T_BATCH_JOB_DEPEND',
                 'mapping', 'T_COMPONENT_DEF')
    # print(processed_f)
