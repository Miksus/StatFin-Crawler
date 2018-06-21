


__author__ = "Mikael Koli"
__copyright__ = "Copyright 2018"
__credits__ = [" "]
__license__ = "MIT License"
__version__ = "0.9.3"
__maintainer__ = "Mikael Koli"
__email__ = "koli.mikael@gmail.com"
__status__ = "Development"


'''
This file contains class "StatFinCrawler".
This class can be used to navigate through Statistics Finland's (Tilastokeskus) database directories and 
acquire data for further processing or analyzing with Python.


To be implemented (with no deadline):
Search tables using given keywords
Crawl through the databases automatically and orderly
Download the data to csv-file
Manual navigation with command window (or with GUI if I ever find the time)
More support for customized variables.
'''


#External packages:
try:
        import pandas as pd
        _PANDAS_IMPORTED = True
except ImportError:
        StatFinCrawler.output = 'csv'
        ImportWarning("Pandas is not installed. Pandas' dataframes cannot be used. \nStatFinCrawler.set_output_format(output_format='nested list'")
        _PANDAS_IMPORTED = False
import requests


## Standard libraries ##
import json
import time
import random
import datetime
import csv

# End of Modules





class StatFinCrawler:
        #Format of the API: PXWEB/{API-NAME}/{API-VERSION}/{LANGUAGE}/{DATABASE-ID}/{LEVELS}/{TABLE-ID}
        base_url = 'http://pxnet2.stat.fi/PXWeb/api/v1/en/StatFin'
        speed = 2
        output = 'pandas'
        debug = True
        def __init__(self):
                self.URL = self.base_url
                self.table_history = {}
                self.go_next(command='start')
                

                
                
####### SETTERS (OBJECT) #########
        def set_daterange(self, start_date, end_date):
                "Set desired range of date in format: 'start year as integer' and 'end year as integer' OR 'dd.mm.yyyy' and 'dd.mm.yyyy'"
                self.daterange = {}
                if isinstance(start_date, int) and isinstance(end_date, int):
                        "Range is in years."
                        self.daterange['year'] = tuple(range(start_date, end_date))
                        
                elif len(start_date) == 4 and len(end_date) == 4:
                        "Range is in datetime. TO BE IMPLEMENTED"
                        self.daterange['year'] = tuple(range(start_date, end_date))
                        self.daterange['date'] = None

        
        def to_start(self):
                "Sets the crawler to the root of the StatFin's API"
                self.go_next(command='start')


                
####### WALKERS ##########
        def walk_random(self, **kwargs):
                "Walk through the database randomly getting a random database."
                failed_times = 0
                while not self.last_page or failed_times >=2:
                        select = random.choice(self.options)
                        try:
                                self.go_next(select)
                                failed_times = 0
                        except requests.HTTPError as err:
                                print("Error occured: \n{}".format(err))
                                print("With the following options: {}".format(self.options))
                                failed_times +=1
                                
                self.table_variables(**kwargs)
                try:
                        dataframe = self.read_table()
                except requests.HTTPError as err:
                        print("The data could not be obtained. The following error occured.")
                        print(err)
                        return None
                return dataframe
                
        def walk_to(self, path, **kwargs):
                self.URL = path
                self.go_next()
                self.table_variables(**kwargs)
                file = self.read_table()
                return file


####### Structural methods #######
        def table_variables(self, **kwargs):
                query = {}
                if not hasattr(self, 'content'):
                        self.go_next()
                variables = self.content['variables']
                for variable in variables:
                        code = variable['code']
                        values = variable['values']
                        valuetexts = variable['valueTexts']
                        text = variable['text']

                        "Getting the list of values from the _get_variablevalues() method"
                        set_of_values = self._get_variablevalues(code=code, values=values, text=text, valuetexts=valuetexts, **kwargs)

                        query[code] = set_of_values
                
                self.query_dict = query


        def _get_variablevalues(self, code, values, text, valuetexts, **kwargs):
                'Order of variable values: 1. from kwargs by code string 2. from kwargs by text string 3. from attributes 4. all the values'

                'code is key and has string value'
                'values is key and has list of options'
                'text is key and has string'
                'valuetext is key and has list of options'
                
                'kwargs is dictionary of lists'

                "Make the select_list for deciding values from keyword arguments if found, from attributes if found or left None"
                select_list = (kwargs[code] if code in kwargs else
                               kwargs[text] if text in kwargs else
                               getattr(self, text+'_variable') if hasattr(self, text+'_variable') else
                               getattr(self, code+'_variable') if hasattr(self, code+'_variable') else
                               None)
                select_list = ([select_list] if not isinstance(select_list, (list, tuple)) else select_list)

                if all([select in values for select in select_list]):
                        "If all the elements of 'select_list* are found in 'values', the value set is 'select_list'"
                        set_of_values = select_list
                        
                elif all([select in valuetexts for select in select_list]):
                        "If all the elements of 'select_list' are found in 'valuetext', the value set is got by finding the indexes in 'valuetext' and then corresponding location on 'values'."
                        locations = [valuetexts.index(select) for select in select_list]
                        set_of_values = [values[loc] for loc in locations]
                        
                elif code == 'vuosi' and hasattr(self, 'daterange'):
                        "Time is special type of variable"
                        set_of_values = self.daterange['year']
                        
                elif select_list == None:
                        raise AttributeError("Values are not found ({}). Check if the values are right.".format(', '.join(select_list)))
                        
                else:
                        "If not defined, all of the values are taken."
                        set_of_values = values
                return set_of_values


        
####### READ TABLE
        def read_table(self, format_get='csv', **kwargs):
                
                list_of_queries = []
                for variable in self.query_dict:
                        query = {
                                "code": variable,
                                "selection": {
                                "filter": "item",
                                "values": self.query_dict[variable]
                                }}
                        list_of_queries.append(query)

                POST = {
                        "query": list_of_queries,
                        "response": {
                                "format": format_get
                                }
                        }
                with requests.Session() as sess:
                        response = sess.post(self.URL, json=POST)
                        self._check_statuscode(response.status_code)
                        try:
                                download = response.content.decode('utf-8')
                        except UnicodeDecodeError:
                                download = response.content.decode('ISO-8859-1')
                        reader = csv.reader(download.splitlines(), delimiter=',')
                        data = []
                        for row in reader:
                                data.append(row)
                time.sleep(self.speed)
                                
                self.table_history[self.table_title] = self.URL
                
                if self.output == 'pandas':
                        dataframe = pd.DataFrame(data[1:], columns=data[0])
                        dataframe = dataframe.set_index(dataframe.columns[0])
                        return dataframe
                elif self.output == 'nested list':
                        return data
                else:
                        raise NotImplementedError("Given output is not implemented. Please use 'pandas' or 'nested list' in output.")
        
        def go_next(self, selection=None, command=None):
                "Go next page. Please use only values listed in object.options. Use command='start' to reset. Please use object.go_back(steps) to navigate backwards."
                # Core method of the class
                if command=='start':
                        URL = self.base_url
                        self.last_page = False
                        self.table_title = None
                elif self.URL[-3:] == '.px':
                        URL = self.URL
                elif command == 'update':
                        URL = self.URL
                elif isinstance(selection, int):
                        URL = self.URL+'/'+self.options[selection]
                        URL_end = '/'+self.options[selection]
                elif selection in self.options:
                        URL = self.URL+'/'+selection
                        URL_end = '/'+selection
                else:
                        "ERROR"
                        raise AttributeError("Selection invalid.")

                page_content = requests.get(URL)
                status = page_content.status_code
                time.sleep(self.speed)
                
                self._check_statuscode(status)

                content = page_content.json()
                self.options = [row['id'] if 'id' in row else
                                row['dbid'] if 'dbid' in row
                                else False for row in content]
                self.URL = URL
                if self.debug:
                        print(self.URL)
                
                if URL[-3:] == '.px':
                        "Final page."
                        self.last_page = True
                        self.table_title = content['title']
                        self.content = content
                        return

                return self.options

        
        def go_back(self, steps=1):
                "Go back given steps. Default = 1"
                if steps > len(self.URL.spit('/')) - len(self.base_URL.spit('/')):
                        "The step amount is over the allowed. Going to root."
                        self.go_next(command='start')
                else:
                        self.table_title = None
                        self.last_page = False
                        URL = '/'.join(self.URL.split('/')[:-steps])
                        self.URL = URL
                        self.go_next(command='update')
                
        def _check_statuscode(self, status):
                if self.debug:
                        print("Status {}".format(status))
                if status == 200:
                        "OK"
                elif status == 204:
                        print("No content. URL: {}".format(self.URL))
                        
                elif 400 <= status and status <= 499:
                        "Client error"
                        description = ('Bad request' if status == 400 else 'Unauthorized' if status == 401 else
                                      'Forbidden' if status == 403 else 'Not Found' if status == 404 else
                                      'Gone' if status == 410 else 'Too Many Requests' if status == 429 else 'Else')

                        raise requests.HTTPError("HTTP error at connecting to {}. \nCode {} Description: {}".format(self.URL, status, description)) 
                else:
                        print("Status code not defined ({}) URL: {}".format(status, self.URL))

                        
        def get_url(self):
                return self.URL

        @classmethod
        def get_base_url(cls):
                return cls.base_url

        
        @classmethod
        def settings(cls, API_name=None, API_version=None, language=None, database_id=None):
                "Set the class variables. If 'None' no changes made for that variable."
                # API_name        index 4
                # API_version     index 5
                # language        index 6
                # dabatabase_id   index 7
                
                temp_URL = cls.base_url.split('/')
                temp_URL[4] = temp_URL[4] if API_name == None else API_name
                temp_URL[5] = temp_URL[5] if API_version == None else API_version
                temp_URL[6] = temp_URL[6] if language == None else language
                temp_URL[7] = temp_URL[7] if database_id == None else database_id
                if database_id == '':
                        del temp_URL[7]
                print(temp_URL)
                cls.base_url = '/'.join(temp_URL)

                
####### SETTERS (CLASS) #########
        @classmethod
        def settings_reset_url(cls):
                "Reset the base_url."
                cls.base_url = 'http://pxnet2.stat.fi/PXWeb/api/v1/en/StatFin'

        @classmethod
        def set_output_format(cls, output):
                "Set the format of the table produced."
                #If output is 'Pandas' the output table is a Pandas dataframe
                #If output is 'nested list' the returned table is nested list (rows are as lists inside a list).
                output = output.lower()
                cls.output = output
         
        @classmethod
        def set_speed(cls, pages_per_second=2):
                "Set the speed of the crawler. Please respect StatFin crawling speeds."
                cls.speed = pages_per_second


####### DECLORATORS #############
        def __str__(self):
                content = ["\n"+"*"*80]
                content.append("This is instance of 'Tilastokeskus' class for automated data search.\n") 
                content.append("Base URL:             "+self.base_url)
                content.append("Current URL:          "+self.URL)
                content.append("Tables gone through:  "+', '.join(self.table_history))
                content.append("Speed:                "+str(self.speed))
                content.append("Output format:        "+self.output)
                content.append("*"*80+"\n")
                content = '\n'.join(content)
                return content
        
## END OF CLASS


            
if __name__ == "__main__":
        # Code example
        scrp = StatFinCrawler()
        if not _PANDAS_IMPORTED:
                scrp.set_output_format('nested list')

                
        df = scrp.walk_random()
        print(scrp)
        try:
                print(df.head())
        except:
                pass
        input("Press enter to quit.")
        print("END OF PROGRAM")
        

#EOF
