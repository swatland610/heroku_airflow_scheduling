import pandas as pd
import requests
import json

# extract budget datas
class Wrangler:
    def __init__(self):
        self.extract_budgets = Wrangler.set_col_labels_and_new_department_names()

    # creating a class to store the budget year with api endpoint
    class Budget_Data:
        def __init__(self, budget_year, endpoint_id):
            self.budget_year = budget_year
            self.endpoint_id = endpoint_id 

    def build_budget_objects():
        # Create Budget Objects with Budget Year and endpoint id
        budgets_list = [
            Wrangler.Budget_Data(2011, 'drv3-jzqp'),
            Wrangler.Budget_Data(2012, '8ix6-nb7q'),
            Wrangler.Budget_Data(2013, 'b24i-nwag'),
            Wrangler.Budget_Data(2014, 'ub6s-xy6e'),
            Wrangler.Budget_Data(2015, 'qnek-cfpp'),
            Wrangler.Budget_Data(2016, '36y7-5nnf'),
            Wrangler.Budget_Data(2017, '7jem-9wyw'),
            Wrangler.Budget_Data(2018, '6g7p-xnsy'),
            Wrangler.Budget_Data(2019, 'h9rt-tsn7'),
            Wrangler.Budget_Data(2020, 'fyin-2vyd'),
            Wrangler.Budget_Data(2021, '6tbx-h7y2')
        ]
        return budgets_list  
    
    # The next three functions are chained together:
    ## 1. Bring in all 10 sets and concat them on top of each other
    ## 2. Pull 2021 department names to unify naming differences over the years
    ## 3. Rename Columns & Set new values for Department names based on Department ID
    def portal_data():
        # Bring in Budgets
        budgets_list = Wrangler.build_budget_objects()

        # initiate Empty Dataframe Object to use concat 
        ## in the budget datasets, columns are at the same index, but occasionaly have different labels
        ## assigning index values for columns for easier matching later on
        ten_year_budgets = pd.DataFrame(columns=[int(i) for i in range(0,11)])

        # iterate through budget list
        for budget in budgets_list:

            # set budget year and url for pulling api data
            # Be sure to make sure the 'limit' is set, as it defaults to only returning 1000 records
            budget_year = budget.budget_year
            url = 'https://data.cityofchicago.org/resource/{}.json?$limit=10000'.format(budget.endpoint_id)

            # initiate get request
            response = requests.get(url)
            response_dict = json.loads(response.text)

            # load into DF
            budget_data = pd.DataFrame(response_dict)

            # add a budget year column
            budget_data['budget_year'] = budget_year

            # add a check to see if budget year = 2011 as there is an extra column we do not need in this data set
            if budget_year == 2011:
                budget_data.drop(columns='department', inplace=True)
            # if it is not 2011, then we just move on    
            else:
                pass

            # convert column names to index values so we can easier concat dataframes
            budget_data.rename(columns={x:y for x,y in zip(budget_data.columns, range(0,len(budget_data.columns)))},
                               inplace=True)

            # Stack these Budget DataFrames on top of one another
            ten_year_budgets = pd.concat([ten_year_budgets, budget_data], ignore_index=True)

        return ten_year_budgets

    def pull_department_names():
        # In our larger dataset the department descriptions vary from year to year, but 
        # the department account id stay the same. Bringing in 2021 Data again to unify the
        # department descriptions so they match 2021's data
        url = 'https://data.cityofchicago.org/resource/6tbx-h7y2.json?$limit=10000' 

        # initiate get request
        response = requests.get(url)
        response_dict = json.loads(response.text)

        # Set DataFrame and Subset to needed Columns
        budget_2021 = pd.DataFrame(response_dict)
        departments_2021 = budget_2021.loc[:][['department_number', 'department_description']].\
                                drop_duplicates(ignore_index=True)

        return departments_2021

    def set_col_labels_and_new_department_names():
        # Bring in two DFs created in the prior 2 functions
        ten_year_budgets = Wrangler.portal_data()
        departments_2021 = Wrangler.pull_department_names()

        # first, we are just going to rename the columns
        cols = {0:'fund_type',
                1:'fund_code',
                2:'fund_description',
                3:'department_number',
                4:'department_description',
                5:'approp_authority',
                6:'approp_auth_description',
                7:'approp_account',
                8:'approp_account_description',
                9:'amount',
                10:'budget_year'}

        ten_year_budgets.rename(columns=cols, inplace=True)
        
        # for every id, find matching department id in ten_year_budgets and change the value
        departments_2021_ids = departments_2021['department_number'].to_list()

        def assign_department_categories(dept_id):
            # To get a better sense of general department functions, let's assign overall department categories
            # Using whatever matches could be found within the 2020 Budget Review doc
            # follow this link, and check out page 12: https://www.chicago.gov/content/dam/city/depts/obm/supp_info/2020Budget/2020BudgetOverview.pdf
            
            # Convert Department ID to Int as the ids are passed through as strings
            dept_id = int(dept_id)

            # set department categories
            city_clerk = [25]
            city_council = [15]
            city_development = [21, 23, 54]
            community_services = [41, 45, 48, 50, 91]
            finance_administration = [5, 27, 28, 30, 31, 33, 35, 38]
            finance_general = [99]
            infrastructure_services = [81, 84, 85, 88]
            legislative_elections = [39]
            mayors_office = [1]
            public_safety = [51, 55, 57, 58, 59, 60]
            regulatory = [3, 67, 70, 73, 77, 78]

            # run through each list to see if there's a match
            if dept_id in city_clerk:
                return "City Clerk"
            elif dept_id in city_council:
                return "City Council"
            elif dept_id in city_development:
                return "City Development"
            elif dept_id in community_services:
                return "Community Services"
            elif dept_id in finance_administration:
                return "Finance & Administration"
            elif dept_id in finance_general:
                return "Finance General"
            elif dept_id in infrastructure_services:
                return "Infrastructure Services"
            elif dept_id in legislative_elections:
                return "Legislative & Elections"
            elif dept_id in mayors_office:
                return "Mayor's Office"
            elif dept_id in public_safety:
                return "Public Safety"
            elif dept_id in regulatory:
                return "Regulatory"
            else:
                return "Other"
        # grab department category
        ten_year_budgets['dept_category'] = ten_year_budgets['department_number'].apply(lambda dept_id: assign_department_categories(dept_id))

        for dept_id in departments_2021_ids:
            ten_year_budgets.loc[(ten_year_budgets['department_number']==dept_id), 'department_description'] = departments_2021[departments_2021['department_number']==dept_id]['department_description'].values

        # set desire column order
        cols = ['budget_year', 'fund_type', 'fund_code', 'fund_description', 'department_number', 'department_description', 'dept_category',
                'approp_authority', 'approp_auth_description', 'approp_account', 'approp_account_description', 'amount']

        # return value in correct order and convert it to json
        return ten_year_budgets[cols]


if __name__ == '__main__':
    ten_year_budgets = Wrangler().extract_budgets
    print("Hey! This data has this many rows: {%d}".format(len(ten_year_budgets)))