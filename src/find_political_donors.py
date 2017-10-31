### Data Engineering Challenge

## Importing necessary packages
#!/usr/bin/python

def generate_donors_by_date(input_file, output):
    import pandas as pd
    import datetime as dt
    import numpy as np
    import time
    from collections import defaultdict

    ## Defining columns
    name=["CMTE_ID","AMNDT_IND","RPT_TP","TRANSACTION_PGI","IMAGE_NUM","TRANSACTION_TP","ENTITY_TP","NAME","CITY","STATE","ZIP_CODE","EMPLOYER","OCCUPATION","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID","TRAN_ID","FILE_NUM","MEMO_CD","MEMO_TEXT","SUB_ID"]
    ## Reading csv file and building dataframe
    df = pd.read_csv(input_file, sep='|', header=None, names=name, dtype={"TRANSACTION_DT":"str"})

    ## Returning NaN for invalid zip codes and first 5 digits/chars for valid zip codes

    def trim_zipcode(num):
        if type(num) is not str:
            num = str(num)
        num = num.replace('.', '')
        num = num.replace('-', '')
        if len(num) >= 5:
            return num[:5]
        else:
            return np.nan

    ## Required columns
    df["OTHER_ID"].fillna(value=0, inplace=True)
    selected_col =["CMTE_ID", "ZIP_CODE", "TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID"]
    df2 = df[selected_col]

    ## individual contributions
    ## Remove any row with none empty OTHER_ID
    df2 = df2[df2["OTHER_ID"] == 0]

    ## 5 chars zip code
    df2["ZIP_CODE"]  = df2["ZIP_CODE"].apply(trim_zipcode)

    ## Remove rows with empty TRANSACTION_AMT or empty CMTE_ID

    df2.dropna(axis=0, subset=["TRANSACTION_AMT","CMTE_ID"], inplace=True) ##ignore entries with empty amount

    ## Here we should check if TRANSACTION_AMT > 0

    ### By Date

    ## Building dataframe for by date

    df2["TRANSACTION_DT"] = pd.to_datetime(df2["TRANSACTION_DT"], format="%m%d%Y")
    df_bydate = df2.dropna(axis=0, subset=["TRANSACTION_DT"]) ##ignore entries with incorrect date

    ## Selected columns for new dataframe
    sel_col = ["CMTE_ID", "TRANSACTION_DT", "TRANSACTION_AMT"]
    df_bydate = df_bydate[sel_col]

    df_grp = df_bydate.groupby(["CMTE_ID","TRANSACTION_DT"])

    df_med = df_grp.median().reset_index()
    df_count = df_grp.count().reset_index()
    df_sum = df_grp.sum().reset_index()

    df_med['TRANSACTION_AMT'] = df_med['TRANSACTION_AMT'].apply(round)
    df_med['TRANSACTION_AMT'] = df_med['TRANSACTION_AMT'].apply(int)

    df_med = df_med.merge(df_count,how="inner", on=["CMTE_ID","TRANSACTION_DT"], suffixes=["-median", "-count"])
    df_med = df_med.merge(df_sum,how="inner", on=["CMTE_ID","TRANSACTION_DT"])

    df_med.columns = ['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'COUNT', 'SUM']

    df_date_sort = df_med.sort_values(['CMTE_ID', 'TRANSACTION_DT'])#, ascending=[True, False])

    df_date_sort.to_csv(output, header=None, index=None, sep='|')#, mode='a')

def generate_donors_by_zip(input_file, output):
    import pandas as pd
    import datetime as dt
    import numpy as np
    import time
    from collections import defaultdict

    ## Defining columns
    name=["CMTE_ID","AMNDT_IND","RPT_TP","TRANSACTION_PGI","IMAGE_NUM","TRANSACTION_TP","ENTITY_TP","NAME","CITY","STATE","ZIP_CODE","EMPLOYER","OCCUPATION","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID","TRAN_ID","FILE_NUM","MEMO_CD","MEMO_TEXT","SUB_ID"]
    ## Reading csv file and building dataframe
    df = pd.read_csv(input_file, sep='|', header=None, names=name, dtype={"TRANSACTION_DT":"str"})

    ## Returning NaN for invalid zip codes and first 5 digits/chars for valid zip codes

    def trim_zipcode(num):
      if type(num) is not str:
          num = str(num)
      num = num.replace('.', '')
      num = num.replace('-', '')
      if len(num) >= 5:
          return num[:5]
      else:
          return np.nan

    ## Required columns
    df["OTHER_ID"].fillna(value=0, inplace=True)
    selected_col =["CMTE_ID", "ZIP_CODE", "TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID"]
    df2 = df[selected_col]

    ## individual contributions
    ## Remove any row with none empty OTHER_ID
    df2 = df2[df2["OTHER_ID"] == 0]

    ## 5 chars zip code
    df2["ZIP_CODE"]  = df2["ZIP_CODE"].apply(trim_zipcode)

    ## Remove rows with empty TRANSACTION_AMT or empty CMTE_ID

    df2.dropna(axis=0, subset=["TRANSACTION_AMT","CMTE_ID"], inplace=True) ##ignore entries with empty amount

    ## Here we should check if TRANSACTION_AMT > 0
    ### By Zip

    df_byzip = df2.dropna(axis=0, subset=["ZIP_CODE"]) ##ignore entries with incorrect zip code

    ## Selecting specific rows
    sel_col = ["CMTE_ID", "ZIP_CODE", "TRANSACTION_AMT"]
    df_byzip = df_byzip[sel_col]

    list_test = []
    dic_zip2 = defaultdict(list)

    for i in range(df_byzip.shape[0]):
    	row = df_byzip.iloc[i]
    	k = (row.CMTE_ID, row.ZIP_CODE)
    	dic_zip2[k].append(row.TRANSACTION_AMT)
    	temp_med = int(round(np.median(dic_zip2[k])))
    	temp_count = len(dic_zip2[k])
    	temp_sum = np.sum(dic_zip2[k])
    	row_new = [k[0], k[1], temp_med, temp_count, temp_sum]
    	list_test.append(row_new)

    df_test = pd.DataFrame(columns=["CMTE_ID", "ZIP_CODE", "MEDIAN", "COUNT", "SUM"])
    df_test = pd.DataFrame(list_test)

    df_test.to_csv(output, header=None, index=None, sep='|')

def main(input_file, output1, output2):
        generate_donors_by_zip(input_file, output1)
       	generate_donors_by_date(input_file, output2)	

if __name__ == "__main__":
    import sys	
    arg = []
    for a in sys.argv:
        arg.append(a)
    main(str(arg[1]), str(arg[2]), str(arg[3]))
    


