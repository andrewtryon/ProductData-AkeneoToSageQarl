from dotenv import load_dotenv
load_dotenv()
import subprocess
import json
import csv
from datetime import date
import os
import numpy as np
import pandas as pd
import pyodbc
import requests
from subprocess import Popen
import json
import pickle
from datetime import datetime, timedelta

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    print(response)
    return response       


def flatten_json(nested_json, exclude=['']):
    #print(nested_json)
    out = {}
    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                if '_products' in name:
                    out[name[:-1]] = x
                elif a not in exclude: 
                    flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(nested_json)
    return out    

def construct_qarl_sql(table, row, code, sql_type = 'update'):   
    no_quote_columns = ['ClearanceFlag','Weight','ShipWeight','ShipLength','ShipWidth','ShipHeight']
    row_dict = row.dropna().to_dict()
    if table == 'ProductInfo':
        row_dict['DateUpdated'] = datetime.today().strftime("%m/%d/%Y")
    if sql_type == 'update':
        del row_dict['ItemCode']
        #sql_set_data = ", ".join([k + " = '" + v + "'" for k,v in row_dict.items()])
        sql_set_data = ", ".join([k + " = " + str(v) if k in no_quote_columns else k + " = '" + v.replace("'","''") + "'" for k,v in row_dict.items()])
        sql = """UPDATE target_table 
                SET data_set
                WHERE ItemCode = 'row_ItemCode'""".replace('target_table',table).replace('row_ItemCode',code).replace('data_set',sql_set_data)
    elif sql_type =='add':
        row_keys = ",".join([k for k in row_dict.keys()])
        row_data = ",".join([str(row_dict[k]) if k in no_quote_columns else "'" + row_dict[k] + "'" for k in row_dict.keys()])
        sql = """INSERT INTO target_table (table_columns) 
                VALUES (table_values)""".replace('target_table',table).replace('table_columns',row_keys).replace('table_values',row_data)   
    return sql

def make_json_attribute_data_nest(row, column_name, unit, currency):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan':
        # or str(row[column_name]) == ''
        row[column_name] = np.nan  
    elif type(row[column_name]) != list:
        if isinstance(row[column_name], bool):
            d = row[column_name]
        elif not isinstance(row[column_name], str):
            d = str(row[column_name]).encode().decode()
        else:
            d = row[column_name].encode().decode()
        if unit is not None and currency is None:
            if row[column_name] == '':
                row[column_name] = np.nan
                return row
            else:
                d = np.array({"amount":d,"unit":unit}).tolist()
        elif unit is None and currency is not None:
            d = [np.array({"amount":d,"currency":currency}).tolist()]
        d = {"data":d,"locale":None,"scope":None}
        row[column_name] = [d]
    return row    

if __name__ == '__main__':

    current_run_time = datetime.today()# - timedelta(hours=5)
    print(current_run_time)

    # Uncomment Below to establish a new pickle for last runtime
    # current_run_time = datetime.today() - timedelta(hours=48)
    # with open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last_akene-O-nator_runtime.p', 'wb') as f:
    #     pickle.dump(current_run_time, f)
    # exit()
    
    last_run_time = pickle.load(open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last_akene-O-nator_runtime.p','rb')) - timedelta(hours=.15)
    print(last_run_time)

    #logzero.loglevel(logging.WARN)

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    #adding better errorhandling
    try:
        AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
        AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
        AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
        AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
        AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")

        # # Why are the keys in the .env, but then also in the file directly?
        # AKENEO_CLIENT_ID="14_3ytdvjhnsnok40w8ow8c8k0sck8kckoogkcowccwkwckkgsskc"
        # AKENEO_SECRET="4cwkg7q2ta2owwkcw8gcc480ckcw80oo0c4cw8os0cs8ggk0ks"
        # AKENEO_USERNAME="DataAdmin_Andrew_4124"
        # AKENEO_PASSWORD="826a30dc9"
        # AKENEO_BASE_URL="https://fotronic.cloud.akeneo.com"      


        akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                        AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)

        qarl_General_table = ['PriceListDescription','ProductLine','ProductType','ShipWeight','CatalogNumber','CountryofOrigin','RFQEnabled','DisplayName','Condition','Height','Length','Weight','Width']
        qarl_Google_table = ['GoogleId','GoogleProductCategory','GoogleProductType']
        qarl_ProductInfo_table = ['Header','Title150','Title70','Description','DatasheetUrl','ProductUrl','Accessories','AdditionalImages','BrochureUrl','Category1','Category2','Category3','Components','Features','InformationSource','Keywords','ImageUrl','PersonUpdated']    

        #need to remove this at some point
        qarl_association_cols = {
        }

        #These are the k:v mapping of System to the Akeneo equive
        qarl_cols = {
            "AdditionalImages" : "AdditionalImages",
            "BrochureUrl" : "BrochureUrl",
            "CatalogNumber" : "Catalog",
            "Category1" : "webCategory1",
            "Category2" : "webCategory2",
            "Category3" : "webCategory3",
            "Components" : "TextOnlyComponents",
            "Condition" : "Condition",
            "CountryofOrigin" : "COO",
            "DatasheetUrl" : "DatasheetUrl",
            "Description" : "TextOnlyDescription",
            "DisplayName" : "DisplayName",
            "Features" : "TextOnlyFeatures",
            "GoogleId" : "GoogleId",
            "GoogleProductCategory" : "google_product_category",
            "GoogleProductType" : "google_product_type",
            "Header" : "Header",
            "Height" : "ProductHeight",
            "ImageUrl" : "ImageUrl",
            "InformationSource" : "InformationSource",
            "Keywords" : "Keywords",
            "Length" : "ProductLength",
            "MainOrAccessory" : "MainOrAccessory",
            "ManualUrl" : "ManualUrl",
            "MetaDescription" : "MetaDescription",
            "MetaKeywords" : "MetaKeywords",
            "PriceListDescription" : "PriceListDescription",
            "ProductLine" : "Brand",
            "ProductType" : "ProductType",
            "ProductUrl" : "ProductUrl",
            "QuickstartUrl" : "QuickstartUrl",
            "ShipWeight" : "ShippingWeight",
            "Specs" : "Specs",
            "Title150" : "Title150",
            "Title70" : "Title70",
            "VendorAlias" : "VendorAlias",
            "VideoUrl" : "VideoUrl",
            "Weight" : "product_weight",
            "Width" : "ProductWidth",
            "Accessories" : "Accessories",
            "MainUnits" : "MainUnits", 
            "RelatedProducts" : "RelatedProducts"        
        }

        sage_cols = {
            "ItemCodeDesc" : "Header",
            "UDF_PRODUCT_NAME_150" : "Title150",
            "UDF_PRODUCT_NAME_100" : "Title70",
            "UDF_PRODUCT_NAME_70" : "Title70",
            "Weight" : "product_weight",        
            "UDF_WEB_DISPLAY_MODEL" : "DisplayName"
        }

        akeneo_att_list = list(qarl_cols.values()) + list(sage_cols.values())
        akeneo_att_list = list(set(akeneo_att_list)) #removes dupes
        akeneo_att_string = ','.join(akeneo_att_list) + ",InformationSource_Delta,AkeneoSyncSupport" #these fellas toggle whether or not data needs to be synced back to systems


        query_run_time = last_run_time.strftime("%Y-%m-%d %H:%M:%S") #Time/Date formatting for Akeneo API

        searchparams = """
        {
            "limit": 100,
            "scope": "ecommerce",
            "attributes": "search_atts",
            "with_count": true,        
            "search": {
                "updated":[{"operator":">","value":"since_date"}]
            }
        }
        """.replace('since_date',query_run_time).replace('search_atts',akeneo_att_string)

        print(searchparams)

        #make JSON for API call
        aksearchparam = json.loads(searchparams)
        #Get API object to iternate through
        result = akeneo.products.fetch_list(aksearchparam)

        #setting up the dataframe to be filled   
        pandaObject = pd.DataFrame(data=None)  

        #loopy toogles
        go_on = True
        count = 0
        #for i in range(1,3):  #this is for testing ;)  
        while go_on:
            count += 1

            print(str(count) + ": normalizing")                        
            page = result.get_page_items()
            #flatten a page JSON response into a datafarme (excludes the JSON fields that are contained in the list below)
            pagedf = pd.DataFrame([flatten_json(x,['scope','locale','currency','unit','categories']) for x in page])

            #below code cleans up column headers that exploded during the flattening process
            pagedf.columns = pagedf.columns.str.replace('values_','')
            pagedf.columns = pagedf.columns.str.replace('_0','')
            pagedf.columns = pagedf.columns.str.replace('_data','')
            pagedf.columns = pagedf.columns.str.replace('_amount','')
            pagedf.columns = pagedf.columns.str.replace('associations_','')
            pagedf.columns = pagedf.columns.str.replace('_products','')

            #This code would be used if you only wanted certain columns...since we defined which attributes to grab, we don't need this
            #pagedf.drop(pagedf.columns.difference(akeneo_att_list), 1, inplace=True)
            
            #This appends each 'Page' from the the API to the              
            pandaObject = pandaObject.append(pagedf, sort=False)

            go_on = result.fetch_next_page()

        print(pandaObject)

        if pandaObject.shape[0] == 0:
            #this checks if anything was returned from the API (if nothing has been updated in Akeneo since last run...this should happen[no reason to record this runtime])
            print("nothing to sync...i guess")
            exit()

        if 'AkeneoSyncSupport' not in pandaObject:
            pandaObject['AkeneoSyncSupport'] = np.nan                  

        #Rename identifier to ItemCode
        pandaObject = pandaObject.rename(columns={"identifier": "ItemCode"})

        #Determine which of the 'Updated in Akeneo since last run actually need to be synced
        #Anything with this toggle 'AkeneoSyncSupport' will be synced
        #Changes to information source will also trigger
        #at end of script, InformationSource_Delta will be overwritten with current InformationSource...also AkeneoSyncSupport will be flipped to false
        pandaObject.loc[(pandaObject['AkeneoSyncSupport'] == True),'InformationSource_Delta'] = "SYNC ME PLRSE ^-^"   
        pandaObject = pandaObject.rename(columns={"identifier": "ItemCode"})
        DataSyncDF = pandaObject[pandaObject['InformationSource'] != pandaObject['InformationSource_Delta']]#.rename(columns={"identifier": "ItemCode"})#.set_index('ItemCode', drop=True)

        #viewing and backup
        print(DataSyncDF)
        DataSyncDF.to_csv(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\last-akene-o-natorAPIpull.csv')

        #The code above could result in an empty datafram where nothing needs to be synced anywhere
        if DataSyncDF.shape[0] > 0:

            #Need to use PandaObject later...so making another df 'workingdf'to facilate data maniuplations
            #This 
            item_count = DataSyncDF.shape[0]
            akeneo_att_list = akeneo_att_list + ['identifier'] + ['ItemCode']
            workingdf = DataSyncDF.drop(DataSyncDF.columns.difference(akeneo_att_list), 1)
            workingdf = workingdf.reset_index().set_index('ItemCode')

            #qarl data prep      
            qarl_cols = {**qarl_cols, **qarl_association_cols}
            workingdf = workingdf.filter(items=list(qarl_cols.values()))
            qarl_fields = dict((v,k) for k,v in qarl_cols.items())
            workingdf = workingdf.rename(columns=qarl_fields)
            workingdf = workingdf.reindex(columns=qarl_Google_table + qarl_General_table + qarl_ProductInfo_table)
            workingdf = workingdf.rename_axis('ItemCode')
            workingdf = workingdf.fillna('')
            for i in ['Weight','ShipWeight']:
                workingdf[i] = pd.to_numeric(workingdf[i]).round(2)
            workingdf[['Length','Width','Height']] = workingdf[['Length','Width','Height']].round(2).astype(str)
            workingdf['PersonUpdated'] = "Akeneo Return Task Sync"
            workingdf.to_csv(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\qarl_cols.csv')


            #Establish
            #Qarl Connnections
            qarl_conn_str = (
                r'DSN=QARL_64;'
                r'CharSet=utf8;'
                )
            qarl_cnxn = pyodbc.connect(qarl_conn_str)
            cursor = qarl_cnxn.cursor() 

            #General Qarl Table load
            qarl_General_df = workingdf[qarl_General_table].reset_index()
            for index, row in qarl_General_df.iterrows():
                sql = construct_qarl_sql(table = 'General', row = row, code = row['ItemCode'], sql_type = 'update')
                # try:
                #     print(sql)
                # except:
                #     print("weird special char")
                cursor.execute(sql) 
            #qarl_General_df.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\qarl_General_df.csv')
            print('general')     

            #Product Info Qarl Table load  
            qarl_ProductInfo_df = workingdf[qarl_ProductInfo_table].reset_index()
            for index, row in qarl_ProductInfo_df.iterrows():
                sql = construct_qarl_sql(table = 'ProductInfo', row = row, code = row['ItemCode'], sql_type = 'update')
                # try:
                #     print(sql)
                # except:
                #     print("weird special char")
                cursor.execute(sql)                            
            #qarl_ProductInfo_df.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\qarl_ProductInfo_df.csv')
            print('ProductInfo')       
            qarl_cnxn.commit()               

            #sage data
            DataSyncDF.loc[:,'Title100'] = DataSyncDF['Title70']
            
            workingdf = DataSyncDF.filter(items=list(sage_cols.values())+['ItemCode']).set_index('ItemCode')
            workingdf = workingdf.dropna(how='all')
            workingdf = workingdf.fillna('')
            for column in workingdf:
                print(column)
                try:
                    workingdf[column] = workingdf[column].str.encode('ascii', 'ignore').str.decode('ascii')
                except:
                    pass
            workingdf.to_csv(r'\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\AkeneoSync\from_akeneo_sync.csv', header=False, sep='|', index=True, encoding='ANSI', errors='ignore', quoting=csv.QUOTE_NONE)   
            print("sending sage data")
            p = subprocess.Popen('Auto_SyncAkeneoRecords_VIWI5W.bat', cwd= r'Y:\Qarl\Automatic VI Jobs\AkeneoSync', shell = True)
            stdout, stderr = p.communicate()       

            #Data need to be sent back to akeneo to prevent these items from continusiously resyncing
            #prepping
            DataSyncDF = DataSyncDF.rename(columns={"ItemCode": "identifier"})
            DataSyncDF['InformationSource_Delta'] = DataSyncDF['InformationSource'] 
            DataSyncDF['AkeneoSyncSupport'] = False
            
            #Flatten df to JSON
            valuesCols = [
                'InformationSource_Delta',
                'AkeneoSyncSupport'
            ]
            for cols in valuesCols:
                DataSyncDF = DataSyncDF.apply(make_json_attribute_data_nest, column_name = cols, currency = None, unit = None, axis = 1)     
            jsonDF = (DataSyncDF.groupby(['identifier'], as_index=False)
                        .apply(lambda x: x[valuesCols].dropna(axis=1).to_dict('records'))
                        .reset_index()
                        .rename(columns={'':'values'}))
            jsonDF.rename(columns={ jsonDF.columns[2]: "values" }, inplace = True)

            load_failure = False

            #Send data
            values_for_json = jsonDF.loc[:, ['identifier','values']].dropna(how='all',subset=['values']).to_dict(orient='records')   
            data_results = akeneo.products.update_create_list(values_for_json)
            print(data_results)   

        #Saving Last run time
        print("pickling")
        with open(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\last_akene-O-nator_runtime.p', 'wb') as f:
            pickle.dump(current_run_time, f)
        f.close()               

    except Exception as e:
        print("rut-ro")
        assignees = '[KUACOUUA]'#,KUALCDZR,KUAEL7RV]' # Andrew, Anthony
        folderid = 'IEAAJKV3I4JEW3BI' #Web Requests IEAAJKV3I4GOVKOA
        wrikedescription = "Akeneo to Sage/Qarl had an error -> \n\n" + str(e)
        wriketitle = str(date.today().strftime('%Y-%m-%d'))+ " - Akeneo to Sage-Qarl error"
        response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
        response_dict = json.loads(response.text)
        print('File attached!')   
    finally: 
        pass

