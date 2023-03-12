import pandas as pd
import numpy as np
import random
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import sqlite3
import datetime
import time
import requests
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException

def configure_driver():
    # Add additional Options to the webdriver
    chrome_options = Options()
    # add the argument and make the browser Headless.
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    # Instantiate the Webdriver: Mention the executable path of the webdriver you have downloaded
    # For linux/Mac
    # driver = webdriver.Chrome(options = chrome_options)
    # For windows
    driver = webdriver.Chrome(executable_path="C:/Users/barto/chromedriver.exe", options = chrome_options)
    return driver


driver = configure_driver()


# pobieram liczbę stron oraz listę ofert
def scrap_otodom(link_startowy):
    driver.get(link_startowy)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    soup = BeautifulSoup(driver.page_source, "lxml")
    liczba_stron=soup.find_all(class_="eoupkm71 css-190hi89 e11e36i3")
    liczba_stron=liczba_stron[3].text
    print(liczba_stron)
    print(link_startowy[:-1])

    all_site_urls=[]
    errors_urls=[]
    for i in range(1,int(liczba_stron)+1):
        try:
            #tworzę link
            link_site=link_startowy[:-1]+str(i)
            driver.get(link_site)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(random.randint(10,20)/10)
            soup = BeautifulSoup(driver.page_source, "lxml")

            #pobieram linki z każdej ofert na stronie i wrzucam do listy all_site_urls
            all_offers=soup.find_all(class_="css-1sxg93g e76enq87")
            urls_offers=all_offers[0].find_all(class_="css-b2mfz3 es62z2j16")
            for url in urls_offers:
                all_site_urls.append(url["href"])
                print(url["href"])
            print(str(i)+" z "+str(int(liczba_stron)))
        except:
            print("błąd", i)
            errors_urls.append(i)
    
    all_site_urls= list(dict.fromkeys(all_site_urls))
    #towrzę puste df w celu ich uzupełnienia
    df_info = pd.DataFrame()

    df_info_detailed=pd.DataFrame()

    df_price=pd.DataFrame(columns=["Cena"])

    df_place=pd.DataFrame(columns=["Dzielnica"])

    control=0
    for offer_url in all_site_urls:
        offer_base="https://www.otodom.pl"+offer_url
        try:
            driver.get(offer_base)
            time.sleep(random.randint(10,20)/10)
            soup = BeautifulSoup(driver.page_source, "lxml")

            ## tabela  z danymi podstawowymi
            info_table=soup.find_all(class_="css-wj4wb2 emxfhao1")
            values=[]
            columns=[]
            for i in info_table:
                elements_info_table=i.find_all(class_="css-1ccovha estckra9")
                for k in elements_info_table:
                    elements=k.find_all(class_="css-1qzszy5 estckra8")
                    columns.append(elements[0].text)
                    values.append(elements[-1].text)
            table_info = pd.DataFrame(values).T
            table_info.columns=columns
            table_info["link"]=offer_url
            df_info=pd.concat([df_info,table_info])

            ## tabela z danymi szczegółowymi
            detailed_table=soup.find_all(class_="css-1l1r91c emxfhao1")
            columns2=[]
            values2=[]
            for w in detailed_table:
                detailed_elements=w.find_all(class_="css-f45csg estckra9")
                for s in detailed_elements:
                    detailed_element=s.find_all(class_="css-1qzszy5 estckra8")
                    if len(detailed_element)>0:
                        columns2.append(detailed_element[0].text)
                        values2.append(detailed_element[-1].text)
                    else:
                        print("brak danych")
            table_info_detailed = pd.DataFrame(values2).T
            table_info_detailed.columns=columns2
            table_info_detailed["link"]=offer_url
            df_info_detailed=pd.concat([df_info_detailed,table_info_detailed])

            ##tabela z ceną
            price=soup.find_all(class_="css-8qi9av eu6swcv19")
            table_price=pd.DataFrame({"Cena":[price[0].text],"link":[offer_url]})
            df_price=pd.concat([df_price,table_price])

            #tabela z lokalizacją
            places=soup.find_all(class_="css-1hbnbbd e1je57sb5")
            place=places[0].find_all(class_="css-1in5nid e1je57sb4")
            table_place=pd.DataFrame({"Dzielnica":[place[3].text],"link":[offer_url]})
            df_place=pd.concat([df_place,table_place])

            control=control+1
            print(str(control)+" z "+str(len(all_site_urls)))

        except:
            print(offer_url)
    final_df = df_info.merge(df_info_detailed,how ='left',on="link").merge(df_price,how ='left',on="link").merge(df_place,how ='left',on="link")

    final_df=final_df.drop_duplicates()
    final_df["numer_tygodnia"]=datetime.date.today().isocalendar()[1]
    final_df.to_excel("C:/Users/barto/Downloads/test_vscode.xlsx")
    print("done")
    return final_df

if __name__ == "__main__":

    final_df=scrap_otodom("https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/warszawa?distanceRadius=0&market=ALL&locations=%5Bcities_6-26%5D&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie&searchingCriteria=cala-polska&limit=72&page=1")
    
    engine = sqlalchemy.create_engine("sqlite:///mieszkania_magisterka.sqlite")
    conn = sqlite3.connect('mieszkania_magisterka.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS mieszkania_magisterka(
        Powierzchnia VARCHAR(200),
        Forma_własności VARCHAR(200),
        Liczba_pokoi VARCHAR(200),
        Stan_wykończenia VARCHAR(200),
        Piętro VARCHAR(200),
        Balkon_ogród_taras VARCHAR(200),
        Czynsz VARCHAR(200),
        Miejsce_parkingowe VARCHAR(200),
        Obsługa_zdalna VARCHAR(200),
        Ogrzewanie VARCHAR(200),
        link VARCHAR(200),
        Rynek VARCHAR(200),
        Typ_ogłoszeniodawcy VARCHAR(200),
        Rok_budowy VARCHAR(200),
        Rodzaj_zabudowy VARCHAR(200),
        Okna VARCHAR(200),
        Winda VARCHAR(200),
        Media VARCHAR(200),
        Zabezpieczenia VARCHAR(200),
        Wyposażenie VARCHAR(200),
        Informacje_dodatkowe VARCHAR(200),
        Materiał_budynku VARCHAR(200),
        Dostępne_od VARCHAR(200),
        Cena VARCHAR(200),
        Dzielnica VARCHAR(200),
        numer_tygodnia VARCHAR(200)
    )
    """
    cursor.execute(sql_query)

    final_df.to_sql("mieszkania_magisterka", engine, index=False, if_exists='append')
    conn.close()