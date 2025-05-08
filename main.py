import pandas as pd
# import os
import warnings


def select_record(group):
    if len(group) == 1:
        return group
    # Filter to preferred districts
    preferred = group[group['District'].isin(preferred_districts)]
    if len(preferred) == 1:
        return preferred
    elif len(preferred) > 1:
        return preferred.loc[[preferred['MS_Exit_Date'].idxmax()]]
    else:
        return group.loc[[group['MS_Exit_Date'].idxmax()]]


file_path = ('S:/ES/AACICore/RCCD/class_of_2024_nsc_detail.csv')
nsc = pd.read_csv(file_path, low_memory=False)

nsc['SSID'] = nsc['SSID'].astype('int64')
nsc = nsc[nsc['Enrollment_End'] > nsc["High_School_Grad_Date"]]
nsc = nsc[["SSID", "Record_Found_Y/N", "College_Name", "College_State", "2-Year/4-Year", "Public/Private",
           "Enrollment_Begin", 'Enrollment_End', "Enrollment_Status"]]

file_path = ('input/2023-2024_152_account.parquet')
cohort = pd.read_parquet(file_path)
cohort['SSID'] = cohort['SSID'].astype('int64')

removes = ['RemovedFromCohort', 'Dropout', 'OtherTransfers']
for remove in removes:
    cohort = cohort.loc[cohort['CohortCategory'] != remove]
cohort = cohort.loc[cohort['DistrictCode'] == "3367207"]

file_path = ('input/CDESchoolDirectoryExport.csv')
dcodes = pd.read_csv(file_path, low_memory=False)
dcodes['CDS Code'] = dcodes['CDS Code'].astype(str).str[:7]
dist_codes = dict(zip(dcodes['CDS Code'], dcodes['District']))

file_path = ('input/2019-2020_81_eoy_snap.parquet')
school_his_bulid = pd.read_parquet(file_path)
school_his_bulid = school_his_bulid.loc[school_his_bulid['EnrollmentStatus'] == "Primary enrollment"]
school_his_bulid = school_his_bulid[["SSID", "SchoolName", "DistrictCode", 'ExitDate', "Grade"]]
school_his_bulid.rename(columns={'DistrictCode': 'Gr 8 District', "SchoolName": "Gr 8 School", "ExitDate": "MS_Exit_Date"}, inplace=True)

school_his_bulid['SSID'] = school_his_bulid['SSID'].astype('int64')
outcomes = pd.merge(cohort, school_his_bulid, on='SSID', how='left')

collist = ["DistrictCode", 'Gr 8 District']
for col in collist:
    for dist in outcomes[col].dropna().unique().tolist():
        outcomes.loc[outcomes[col] == dist, col] = dist_codes[dist]
outcomes.rename(columns={'DistrictCode': 'District'}, inplace=True)

preferred_districts = {'Perris Union High', 'Romoland Elementary', 'Nuview Union', 'Menifee Union', 'Perris Elementary'}
with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=DeprecationWarning)
    outcomes = outcomes.groupby('SSID', group_keys=False).apply(select_record).reset_index(drop=True)

file_path = ('C:/Users/moneill/PycharmProjects/calpads/enrollments.csv')
missing_enrol = pd.read_csv(file_path)
print(missing_enrol.shape[0])
print(missing_enrol["GradeLevel"])
missing_enrol = missing_enrol.loc[missing_enrol['GradeLevel'] == '8']
print(missing_enrol.shape[0])
missing_enrol['ReportingLEA'] = missing_enrol['ReportingLEA'].astype(str).str.split('-').str[0]
missing_enrol['SchoolOfAttendance'] = missing_enrol['SchoolOfAttendance'].astype(str).str.split('-').str[0]
missing_enrol = missing_enrol[["SSID", 'ReportingLEA', 'SchoolOfAttendance', 'EnrollmentExitDate', "GradeLevel"]]
missing_enrol.rename(columns={'ReportingLEA': 'District', 'EnrollmentExitDate': "MS_Exit_Date"}, inplace=True)

with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=DeprecationWarning)
    missing_enrol = missing_enrol.groupby('SSID', group_keys=False).apply(select_record).reset_index(drop=True)
    missing_enrol = missing_enrol.groupby('SSID', group_keys=False).apply(select_record).reset_index(drop=True)

print(missing_enrol.columns.values.tolist())
missing_enrol.rename(columns={'District': 'District2', 'SchoolOfAttendance': "MS2", 'MS_Exit_Date': 'MS_Exit_Date2'}, inplace=True)
missing_enrol = missing_enrol[["SSID", 'District2', 'MS2', "MS_Exit_Date2", "GradeLevel"]]
missing_enrol['MS_Exit_Date2'] = pd.to_datetime(missing_enrol['MS_Exit_Date2'], errors='coerce').dt.strftime('%#m/%#d/%Y')

print(missing_enrol["MS2"])
outcomes = pd.merge(outcomes, missing_enrol, on='SSID', how='left')
outcomes.loc[outcomes["Gr 8 District"] == 'nan', "Gr 8 District"] = None
outcomes['Gr 8 District'] = outcomes['Gr 8 District'].fillna(outcomes['District2'])
outcomes['Gr 8 School'] = outcomes['Gr 8 School'].fillna(outcomes['MS2'])
outcomes['MS_Exit_Date'] = outcomes['MS_Exit_Date'].fillna(outcomes['MS_Exit_Date2'])
outcomes['Grade'] = outcomes['Grade'].fillna(outcomes['GradeLevel'])

outcomes = outcomes.drop(['District2', 'MS2', 'MS_Exit_Date2', 'GradeLevel'], axis=1)
outcomes['Gr 8 School'] = outcomes['Gr 8 School'].fillna("No California Enrollment")
outcomes['Gr 8 District'] = outcomes['Gr 8 District'].fillna("No California Enrollment")

outcomes = pd.merge(outcomes, nsc, on='SSID', how='left')
print(nsc.SSID.dtype)
print(outcomes.SSID.dtype)

outcomes['Enrollment_End'] = pd.to_datetime(outcomes['Enrollment_End'], errors='coerce')

# outcomes = (
#     outcomes.sort_values(['SSID', 'Enrollment_End'], ascending=[True, False])
#       .drop_duplicates(subset='SSID')
#       .reset_index(drop=True)
# )

# drop enroll date cols - discuss pulling in feeder data for the 40% that are missing
outcomes.to_parquet('output.parquet', index=False)
outcomes.to_csv('output.csv', index=False)


