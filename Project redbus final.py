#Working code for WBTC

import time
from mysql import connector
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

WAIT_10 = 10
DATABASE = "RedBus"

USER = 'root'  # Replace with your MySQL username
PASSWORD = ''  # Replace with your MySQL password
HOST = 'localhost'
PORT = 3306

"""
This method establishes connection with MySQL and returns the connection.
"""
def connect_db():
    try:
        # First, connect without specifying the database
        connection = connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD
        )
        
        # Create a cursor to execute SQL commands
        mycursor = connection.cursor()
        
        # Create the database if it doesn't exist
        mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE};")
        
        # Now connect to the newly created database
        connection.database = DATABASE
        
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

"""
This method is used to create the database and the table, if it doesn't exist.
"""
def create_db_table(mycursor):
    create_bus_table = """
    CREATE TABLE IF NOT EXISTS bus_routes(
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        route_name VARCHAR(250),
        route_link VARCHAR(250),
        bus_name VARCHAR(250),
        bus_type VARCHAR(250),
        departing_time TIME,
        duration VARCHAR(50),
        reaching_time TIME,
        star_rating DOUBLE(5,3),
        price DECIMAL(10,3),
        seats_available INT(10)
    );
    """
    mycursor.execute(f"USE {DATABASE};")
    mycursor.execute(create_bus_table)

"""
This method is used to insert data inside the table.
"""
def insert_row(connection, route_name, route_link, bus_name, bus_type, depart_time, duration, reach_time, star, price, seats):
    try:
        mycursor = connection.cursor()
        sql = """INSERT INTO bus_routes (route_name, route_link, bus_name, bus_type, departing_time, duration, 
                 reaching_time, star_rating, price, seats_available) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        record = (route_name, route_link, bus_name, bus_type, depart_time, duration, reach_time, star, price, seats)
        mycursor.execute(sql, record)
        connection.commit()
    except connector.Error as error:
        connection.rollback()
        print(f"Failed to insert into table: {error}")

"""
Open the Government bus page(like APSRTC) and fetch all the route names and URLs in the form of a dictionary.
"""
def getRouteDict(driver: webdriver, govt_bus_url: str):
    driver.get(govt_bus_url)
    driver.maximize_window()
    WebDriverWait(driver, WAIT_10).until(EC.presence_of_element_located((By.CLASS_NAME, 'DC_117_pageTabs')))

    pages = driver.find_elements(By.CLASS_NAME, 'DC_117_pageTabs')
    num_pages = len(pages)
    print(f"Pages: {num_pages}")

    route_dict = {}
    for i in range(1, num_pages + 1):
        print(f"Start scrape of Page: {i}")
        WebDriverWait(driver, WAIT_10).until(EC.presence_of_element_located((By.CLASS_NAME, 'route')))
        routes = driver.find_elements(By.CLASS_NAME, 'route')

        for j in range(len(routes)):
            route_url = routes[j].get_attribute("href")
            route_title = routes[j].get_attribute("title")
            route_dict[tuple((i, j))] = [route_url, route_title]

        if i < num_pages:
            driver.execute_script("arguments[0].click();", pages[i])
    return route_dict

"""
Parse travels details and store in MySQL.
"""
def getBusDetails(connection, driver: webdriver, route_dict: dict):
    for route in route_dict.values():
        print(f"Route: {route}")
        driver.get(route[0])
        time.sleep(3)

        WebDriverWait(driver, WAIT_10).until(EC.element_to_be_clickable((By.CLASS_NAME, "next")))
        driver.execute_script("arguments[0].click();", driver.find_element(By.CLASS_NAME, "next"))
        time.sleep(3)

        if driver.find_elements(By.CLASS_NAME, "oops-page"):
            print(f"No buses found in route: {route[1]}")
            continue

        WebDriverWait(driver, WAIT_10).until(EC.presence_of_element_located((By.CLASS_NAME, "button")))
        driver.find_element(By.CLASS_NAME, "button").click()

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        WebDriverWait(driver, WAIT_10).until(EC.presence_of_element_located((By.CLASS_NAME, "busFound")))

        total_bus = driver.find_element(By.CLASS_NAME, 'busFound').text
        total_buses = int(total_bus.split(" ")[0])
        print(f"Total Buses mentioned in page: {total_buses}")

        last_bus_count = 0
        while True:
            bus_count = len(driver.find_elements(By.CLASS_NAME, 'travels'))
            if bus_count == last_bus_count:
                break
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            last_bus_count = bus_count

        print(f"Total bus found on {route[1]}: {bus_count}")

        bus_route_names = driver.find_elements(By.CLASS_NAME, 'travels')
        bus_type_s = driver.find_elements(By.CLASS_NAME, 'bus-type')
        departure_time_s = driver.find_elements(By.CLASS_NAME, 'dp-time')
        duration_s = driver.find_elements(By.CLASS_NAME, 'dur')
        reaching_time_s = driver.find_elements(By.CLASS_NAME, 'bp-time')
        star_rating_s = driver.find_elements(By.XPATH, '//span[@class=""]')
        fare_s = driver.find_elements(By.CLASS_NAME, 'fare')
        seats_available_s = driver.find_elements(By.CLASS_NAME, 'seat-left')

        for i in range(bus_count):
            bus_route_name = bus_route_names[i].text
            bus_type = bus_type_s[i].text
            departure_time = departure_time_s[i].text
            duration = duration_s[i].text
            reaching_time = reaching_time_s[i].text
            star_rating = float(star_rating_s[i].text)
            if star_rating > 10:
                star_rating = 10
            fare = fare_s[i].text.split(" ")[-1]
            seats_available = int(seats_available_s[i].text.split(" ")[0])

            insert_row(connection, route[1], route[0], bus_route_name, bus_type, departure_time, duration, reaching_time, star_rating, fare, seats_available)
        print(f"Finished route: {route[1]}")

def main():
    driver = webdriver.Chrome()
    connection = connect_db()
    if connection:
        mycursor = connection.cursor()
        create_db_table(mycursor)
        WBTC_URL = "https://www.redbus.in/online-booking/wbtc-ctc/"
        route_dict = getRouteDict(driver, WBTC_URL)
        getBusDetails(connection, driver, route_dict)

        mycursor.close()
        connection.close()
    driver.quit()

main()
