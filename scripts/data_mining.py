class data():
    
    def __init__(self):
        self.global_imports()
        self.datapath="../data/"
        self.read_files()
        
        self.population_data()
        self.collect_who_data()
        
    def global_imports(self):
        global pd, np, requests, tqdm
        import pandas, numpy, requests, tqdm
        tqdm, requests, pd, np = tqdm.tqdm, requests, pandas, numpy
        
    def read_files(self):
        self.submission_data = pd.read_csv(self.datapath + "submission.csv")
        self.test_data = pd.read_csv(self.datapath + "test.csv")
        self.train_data = pd.read_csv(self.datapath + "train.csv")
        self.countrycodes=pd.read_csv(self.datapath+"wikipedia-iso-country-codes.csv")
        self.indicator_data=pd.read_excel(self.datapath+"Indicator_list.xlsx").fillna("")
        self.age = pd.read_excel(self.datapath+"API_SP.POP.65UP.TO.ZS_DS2_en_excel_v2_887753.xls")
        self.population = pd.read_excel(self.datapath+"total_population.xls")
        self.population_dens = pd.read_excel(self.datapath+"population_density.xls")
        self.reshape_rawdata()
        
    def reshape_rawdata(self):
        self.indicator_data["long_form"]=self.indicator_data["long_form"].apply(lambda x: "_".join(x.split(" ")))
        self.train_data[["ConfirmedCases","Fatalities"]]=self.train_data[["ConfirmedCases","Fatalities"]].astype(int)
        self.submission_data[["ConfirmedCases","Fatalities"]]=self.submission_data[["ConfirmedCases","Fatalities"]].astype(int)
        
    def population_data(self):
        data_sets = [(self.age,"Population ages 65 and above"), (self.population,"Pop"), (self.population_dens,"Pop_dens")]
        self.world= []
        for worldbank, name in data_sets:
            worldbank.columns = worldbank.iloc[2]
            worldbank.drop(index=[0, 1,2],inplace = True)
            worldbank = worldbank[["Country Code", 2018.0]]
            worldbank.columns = ["Shortcut", name]
            self.world.append(worldbank)

    def import_data(self,indicator="",indicator_type="",name=""):
        url="https://ghoapi.azureedge.net/api/{}".format(indicator)
        data = pd.DataFrame(requests.get(url).json()["value"])
        data.drop([col for col in data.columns if data[col].isna().sum() == data.shape[0]],axis=1,inplace=True)
        data=data.drop(data[data.NumericValue.isna()].index,axis=0).reset_index(drop=True)
        data=data[data.Dim1==indicator_type] if len(indicator_type)>0 else data
        data=data[["IndicatorCode","SpatialDim","TimeDim","NumericValue"]].drop_duplicates(subset=["IndicatorCode","SpatialDim"], keep="last").reset_index(drop=True)
        data=data[["SpatialDim","NumericValue"]]
        data.columns=["Shortcut",name]
        return data

    def import_who_indicators(self,indicator_data):
        data=[self.import_data(indicator["short_form"],indicator["addition"],indicator["long_form"]) for _,indicator in tqdm(indicator_data.iterrows())]
        return data

    def collect_who_data(self):
        self.who_data_list=self.import_who_indicators(self.indicator_data)
        for info in self.world:
            self.who_data_list.append(info)
            
    def match_country_code(self,countrycodes,traindata):
        countrycodes=countrycodes[["Alpha-3 code", "English short name lower case"]]
        countrycodes=countrycodes.drop(countrycodes[countrycodes["English short name lower case"].isna()].index,axis=0).reset_index(drop=True)
        countrycodes.columns = ["Shortcut", "Country_Region"]
        result = traindata.merge(countrycodes,on="Country_Region",how="left").reset_index(drop=True)
        return result

    def match_dataframes(self,new_traindata,who_data):
        result = new_traindata.merge(who_data,on="Shortcut",how="left")
        return result
    
    def merge_whole_dataset(self):
        traindata = self.match_country_code(self.countrycodes,self.train_data)
        for who_data in tqdm(self.who_data_list):
            traindata=self.match_dataframes(traindata,who_data)
        return traindata
            