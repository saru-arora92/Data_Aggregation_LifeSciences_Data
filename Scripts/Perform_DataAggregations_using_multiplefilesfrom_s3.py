import logging
import boto3
import pandas as pd
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime

client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

today = date.today()
folder_date = today.strftime("%m-%d-%y")
now = datetime.now()
current_time = now.strftime("%H_%M_%S")
folder_name = "Run-Date" + folder_date + "Time" + current_time
##objects = bucket.objects.all()

##for object in objects:
##  if object.key.startswith('raw/external/SHA/batch/weekly/07272020/CLIENT_SW_PP_DEMO') and object.key.endswith('.txt'):
##    object.download_file('/tmp/' + object.key)
##Demog file
demog = client.get_object(Bucket=bucket_name,
                          Key='raw/external/SHA/batch/weekly/07272020/CLIENT_SW_PP_DEMO_20200619.txt')
demog_df = pd.read_csv(demog['Body'], sep="|")

##PPData File
ppdata = client.get_object(Bucket=bucket_name,
                           Key='raw/external/SHA/batch/weekly/07272020/CLIENT_SW_PP_DATA_20200619.txt')
ppdata_df = pd.read_csv(ppdata['Body'], sep="|")
ppdata_df.columns = ppdata_df.columns.map(lambda x: str(x) + '_PPDataFile')

columns = ['Market ID_PPDataFile', 'Market Name_PPDataFile',
           'Product ID_PPDataFile', 'Product Name_PPDataFile',
           'Data Type_PPDataFile', 'Rel ID_PPDataFile', 'Provider ID_PPDataFile',
           'Writer Type_PPDataFile', 'Plan ID_PPDataFile',
           'Payment Type Indicator_PPDataFile', 'Week Ending Date_PPDataFile',
           'Month Ending Date_PPDataFile', '01_N_M1_PPDataFile',
           '01_R_M1_PPDataFile', '01_T_M1_PPDataFile', '02_N_M1_PPDataFile',
           '02_R_M1_PPDataFile', '02_T_M1_PPDataFile', '03_N_M1_PPDataFile',
           '03_R_M1_PPDataFile', '03_T_M1_PPDataFile', 'Specialty_Specfile']
ppdata_spec = pd.DataFrame(columns=columns)
## Specialty File
spec = client.get_object(Bucket=bucket_name,
                         Key='master/dm_specialty.csv')
spec_df = pd.read_csv(io.BytesIO(
    spec['Body'].read()), sep=',', engine='python')
spec_df.columns = spec_df.columns.map(lambda x: str(x) + '_Specfile')

##SpecialtyConfig File
specparam = client.get_object(Bucket='dsp-data-lake-dev',
                              Key='demo/specialtyconfig.json')
jsonObject = json.load(specparam["Body"])
specparam_dataframe = pd.DataFrame(jsonObject)
for k, v in specparam_dataframe.iterrows():
    File = v['File']
    flag = v['flag']
    desc = v['description']
    if (File == 'dm_specialty' and flag == 1):
        included_spec_df = spec_df[(spec_df.Excluded_Specfile == 'I') & (spec_df.Product_Name_Specfile != "null")]
        ppdata_spec = ppdata_df.merge(included_spec_df, how='inner', left_on='Product Name_PPDataFile',
                                      right_on='Product_Name_Specfile').drop(
            ['Excluded_Specfile', 'Product_Name_Specfile'], axis=1)
        ppdata_spec2 = ppdata_spec.drop(['Specialty_Specfile'], axis=1)
        ppdata_spec1 = ppdata_spec[['Specialty_Specfile', 'Rel ID_PPDataFile']]
        demog_spec = demog_df.merge(ppdata_spec1, how='inner', left_on=['Specialty_Description', 'Rel_ID'],
                                    right_on=['Specialty_Specfile', 'Rel ID_PPDataFile']).drop(
            ['Specialty_Specfile', 'Rel ID_PPDataFile'], axis=1)
    elif (File == 'dm_specialty' and flag == 0):
        included_spec_df = spec_df[(spec_df.Excluded_Specfile == 'I') & (spec_df.Product_Name_Specfile.isnull())]
        demog_spec = demog_df.merge(included_spec_df, how='inner', left_on='Specialty_Description',
                                    right_on='Specialty_Specfile').drop(
            ['Specialty_Specfile', 'Excluded_Specfile', 'Product_Name_Specfile'], axis=1)
        ppdata_spec2 = ppdata_df
##HR Inclusion from PP_Demo
hrexclude = client.get_object(Bucket=bucket_name,
                              Key='master/HR_Exclusion_List.csv')
hrexclude_df = pd.read_csv(io.BytesIO(
    hrexclude['Body'].read()), sep=',', engine='python')
hrexclude_df.columns = hrexclude_df.columns.map(lambda x: str(x) + '_HRfile')
demog_spec_hr = pd.merge(demog_spec, hrexclude_df, how='outer', left_on='NPI_Number', right_on='NPI_Number_HRfile',
                         indicator=True).drop(
    ['NPI_Number_HRfile', 'First_Name_HRfile', 'Middle_Name_HRfile', 'Last_Name_HRfile'], axis=1)
demog_spec_hr = (demog_spec_hr.loc[demog_spec_hr._merge == 'left_only']).drop(['_merge'], axis=1)

##Product File
product = client.get_object(Bucket=bucket_name,
                            Key='raw/external/SHA/batch/weekly/07272020/CLIENT_SW_PP_PRODUCT_20200619.txt')

product_df = pd.read_csv(product['Body'], sep="|")
product_df.columns = product_df.columns.map(lambda x: str(x) + '_Productfile')

##ClientMarket File
clientmarket = client.get_object(Bucket=bucket_name,
                                 Key='raw/external/SHA/batch/weekly/07272020/Market_Definition.csv')
clientmarket_df = pd.read_csv(clientmarket['Body'], sep="|")
clientmarket_df.columns = clientmarket_df.columns.map(lambda x: str(x) + '_MarketDeffile')

##Product Exclusion based on Client Market Def
product_crunch = product_df.merge(clientmarket_df, how='inner', left_on='Product ID_Productfile',
                                  right_on='product_id_MarketDeffile')

##Product Crunch with PPData Crunch
ppdata_crunch = ppdata_spec2.merge(product_crunch, how='inner', left_on='Product ID_PPDataFile',
                                   right_on='product_id_MarketDeffile')
ppdata_crunch = ppdata_crunch.drop(
    ['Product ID_PPDataFile', 'Product Name_PPDataFile', 'Data Type_PPDataFile', 'Provider ID_PPDataFile',
     'Writer Type_PPDataFile', 'Plan ID_PPDataFile', 'Payment Type Indicator_PPDataFile', 'Market ID_Productfile',
     'Market Name_Productfile', 'Product ID_Productfile',
     'Product Name_Productfile', 'USC Code_Productfile',
     'USC Description_Productfile', 'BB USC Code_Productfile',
     'BB USC Description_Productfile', 'Drug Name_Productfile',
     'Drug Generic Name_Productfile', 'Form Code_Productfile',
     'Form Description_Productfile', 'Strength Description_Productfile',
     'Package Size_Productfile', 'Manufacturer_Productfile', 'Market_MarketDeffile', 'Description_MarketDeffile',
     'Company_Product_MarketDeffile', 'Market Name_PPDataFile', 'NDC11_Productfile', 'Product_Group_MarketDeffile',
     'Veeva_product_Name_MarketDeffile'
     ], axis=1)
##ppdata_crunch.columns
ppdata_crunch = ppdata_crunch.rename(columns={'Market ID_PPDataFile': 'MarketId', 'Rel ID_PPDataFile': 'RelId',
                                              'Week Ending Date_PPDataFile': 'WeekEndingDate',
                                              'Month Ending Date_PPDataFile': 'MonthEndingDate',
                                              '01_N_M1_PPDataFile': '01_N_M1',
                                              '01_R_M1_PPDataFile': '01_R_M1', '01_T_M1_PPDataFile': '01_T_M1',
                                              '02_N_M1_PPDataFile': '02_N_M1', '02_R_M1_PPDataFile': '02_R_M1',
                                              '02_T_M1_PPDataFile': '02_T_M1', '03_N_M1_PPDataFile': '03_N_M1',
                                              '03_R_M1_PPDataFile': '03_R_M1', '03_T_M1_PPDataFile': '03_T_M1',
                                              'product_id_MarketDeffile': 'productId',
                                              })

cols = ['RelId', 'MarketId', 'productId', 'WeekEndingDate', 'MonthEndingDate']
dfs = ppdata_crunch.groupby(cols, as_index=False).agg(
    {'01_N_M1': 'sum', '01_R_M1': 'sum', '01_T_M1': 'sum', '02_N_M1': 'sum', '02_R_M1': 'sum', '02_T_M1': 'sum',
     '03_N_M1': 'sum', '03_R_M1': 'sum', '03_T_M1': 'sum'})  ##.add_suffix('_agg')

dfs['RelId'] = dfs['RelId'].apply(lambda x: '{0:0>9}'.format(x))
dfss = dfs.rename(
    columns={'01_N_M1': 'newrxcount', '01_R_M1': 'refillrxcount', '01_T_M1': 'totalrxcount', '02_N_M1': 'newrxquantity',
             '02_R_M1': 'refillrxquantity', '02_T_M1': 'totalrxquantity', '03_N_M1': 'newrxcost',
             '03_R_M1': 'refillrxcost', '03_T_M1': 'totalrxcost'}, inplace=False)
dfss["1"] = ""
dfss.index.name = 2

FileName = "unaligned/UnalignedFile_" + folder_date + "_" + current_time + ".csv"
csv_buffer = StringIO()
dfss.to_csv(csv_buffer)
response = client.put_object(
    ACL='private',
    Body=csv_buffer.getvalue(),
    Bucket=bucket_name,
    Key=FileName
)







