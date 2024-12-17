import os
from dotenv import load_dotenv
load_dotenv

class create_url:
    def __init__(self, state_input, year, startyear, endyear):
        self.state_input = state_input
        self.startyear = startyear
        self.endyear = endyear
    #form water data url based on state input
    def makeurl(self):
        state_input = self.state_input.strip().lower()
        urls = {
            key.strip().lower(): os.getenv(key)
            for key in os.environ
        }
        state_url = None 
        
        if state_input in urls:
            state_url = urls[state_input]
        elif state_input not in urls:
            print('No water data for that state')
        else:
            print('Error with code')
        return state_url 
    
    #get year for water data
    def getyear_url(self):
        state_url = self.makeurl()

        if self.year:
            data_url = f"{state_url}&startDateLo=01-01-{self.year}&startDateHi=12-31-{self.year}&mimeType=csv&dataProfile=resultPhyschem&providers=NWIS&providers=NWIS&providers=STORET"
        else:
            print('Please enter year in "YYYY" format')
        return data_url

    def rangeyear_url(self):
        state_url = self.makeurl()

        if self.startyear and self.endyear:
            
            data_url = f"{state_url}&startDateLo=01-01{self.startyear}&startDateHi=12-31-{self.endyear}&mimeType=csv&dataProfile=resultPhysChem&providers=NWIS&providers=STORET"
        else:
            print('Please enter date in "mm-dd-yy" format')

        return data_url 

        