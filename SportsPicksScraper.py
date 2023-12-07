#!/usr/bin/env python3
import io
import re
import time
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
from pyvirtualdisplay import Display


# **********************************************************************
# This method is for site login
# **********************************************************************
def site_login(user):
    driver.get(user)
    xpath_btn_login = '//*[@id="navbar-collapse"]/ul/li[4]/a'
    btn_login = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, xpath_btn_login)))
    time.sleep(3)
    btn_login.click()
    username = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, 'username')))
    username.send_keys("thepicker")
    driver.find_element_by_id('password').send_keys("Packard84")
    time.sleep(3)
    driver.find_element_by_class_name('btn').click()
    time.sleep(3)


# **********************************************************************
# This method get SPORTS_PICKS
# **********************************************************************
def get_sports_picks(url):
    picks_data_list = []
    site_login(url)
    time.sleep(3)
    xpath_btn_picks = '//*[@id="sports-hub"]/div[2]/div[2]/div[1]/ng-include[2]/section/div[1]/div[1]/label[2]'
    btn_picks = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, xpath_btn_picks)))
    btn_picks.click()
    time.sleep(5)
    html_element = driver.find_element_by_tag_name('html')
    # Scrolling down the page
    for i in range(0, 7):
        html_element.send_keys(Keys.END)
        time.sleep(5)
    print('************** SPORTS PICKS **************')
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    table = soup.findAll('table')[0]
    # Giving the HTML table to pandas to put in a dataframe object
    pd.set_option('display.max_columns', 10)
    print('************** CONVERTING TABLE TO DATA-FRAMES **************')
    dfs = pd.read_html(str(table))
    # Store the data-frames in a list
    print('************** APPENDING DATA-FRAMES TO LIST **************')
    [picks_data_list.append(df) for df in dfs]
    print('************** CONCATENATING DATA-FRAMES TO LIST **************')
    # combine all pandas dataframes in the list into one big dataframe
    picks_data_list = pd.concat([pd.DataFrame(picks_data_list[i]) for i in range(len(picks_data_list))])
    print('************** DATA-FRAMES CONCATENATED TO A SINGLE DATA FRAME **************')
    # Dropping empty columns by name
    # picks_data_list.drop(['Unnamed: 1', 'Unnamed: 8', 'Unnamed: 9'], axis=1, inplace=True)
    # Dropping rows and columns with more than 3 empty cells
    # picks_data_list.dropna(axis=0, how='any', thresh=3, subset=None, inplace=True)
    picks_data_list.dropna(axis=1, how='any', thresh=3, subset=None, inplace=True)
    picks_data_list = picks_data_list[:-1]
    # print(picks_data_list)
    picks_data_list.to_csv('SPORTS_PICKS.csv', date_format=True, index_label="pick_id")
    # print('************** END SPORTS PICKS **************')
    return picks_data_list
    # End of get_picks function


# **********************************************************************
# This method uploads CSV file directly to the database
# **********************************************************************
def upload_postgres_swiftly(data_frame, t_name):
    try:
        engine = create_engine('postgresql+psycopg2://alitoori:Alitoori84@68.183.210.14:5432/wagerminds')
        connection = engine.raw_connection()
        cursor = connection.cursor()
        print("************** CONNECTED TO POSTGRES **************")
        # Truncates the table
        data_frame.head(0).to_sql(t_name, engine, if_exists='replace', index=False)
        output = io.StringIO()
        data_frame.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        cursor.copy_from(output, t_name, null="")  # null values become ''
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("************** POSTGRESQL CONNECTION IS CLOSED **************")


# **********************************************************************
#    The program starts from here
# **********************************************************************
sports_picks_url = 'https://social.fansunite.com/sports-picks'

display = Display(visible=0, size=(1382, 744)).start()
options = webdriver.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument("--window-size=1382x744")
options.add_argument('--headless')
options.add_argument('--disable-gpu')
# options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
print("CHROME WINDOWS SIZE : " + str(driver.get_window_size()))
picks_list = get_sports_picks(sports_picks_url)
picks_list.index.name = "pick_id"
table_name = 'sports_picks'
upload_postgres_swiftly(data_frame=picks_list, t_name=table_name)

driver.close()
driver.quit()
display.stop()
