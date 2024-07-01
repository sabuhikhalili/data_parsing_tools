import requests as req
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from_date = "2022-01-01"
to_date = "2022-01-02"
url = "https://ar.coinmonitor.site/busqueda/"
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# List of month names in Spanish
months_spanish = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                  "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

# Convert strings to datetime objects
start_date = datetime.strptime(from_date, "%Y-%m-%d")
end_date = datetime.strptime(to_date, "%Y-%m-%d")

# Generate a list of hourly timestamps
current_date = start_date

all_data = []
while current_date < end_date:

    payload = {
        "anio": f"{current_date.year}",
        "mes": f"{months_spanish[current_date.month - 1]}",
        "dia": f"{current_date.day:02}",
        "hora": f"{current_date.hour:02}"
    }

    response = req.post(url, headers=headers, data=payload)

    current_date += timedelta(hours=1)
    # Check if data request was successful
    if response.ok:
        # Save CSV response to a file
        soup = BeautifulSoup(response.content.decode("utf-8"))
    else:
        print('Failed to retrieve data:', response.status_code)
        continue

    if not soup.find("div", id="box_02_2").find(class_="tit_23").text:
        # if returned date is empty, that means no data, just continue to next request.
        continue

    search_date = datetime.strptime(soup.find("div", id="box_02_1").find(class_="tit_23").text, "%d-%m-%Y | %H")
    returned_date = datetime.strptime(soup.find("div", id="box_02_2").find(class_="tit_23").text, "%d-%m-%Y | %H")

    if search_date != returned_date:
        # requested date doesn't exist, instead website returns a different date
        continue

    tdate = returned_date
    data_container = soup.find("div", id="box_01").find_all("div", recursive=False)

    for ins in data_container:
        inst_data = ins.find_all("span")
        inst_name = inst_data[0].text
        inst_value = inst_data[1].text
        inst_cur = inst_data[2].text
        _data = [tdate, inst_name, inst_value, inst_cur]
        all_data.append(_data)


df = pd.DataFrame(all_data)
df.columns = ["Date", "Instrument", "Value", "Currency"]

df['Value'] = df['Value'].str.replace(',', '')
df['Value'] = pd.to_numeric(df['Value'])

# Clean 'Currency' column
df['Currency'] = df['Currency'].str.replace(r'[\r\n\t]', '', regex=True).str.strip()
df['Instrument'] = df['Instrument'].str.replace(r'[\r\n\t:]', '', regex=True).str.strip()

df['Year-Month'] = df['Date'].dt.to_period('M')
monthly_avg = df.groupby(['Year-Month', 'Instrument', 'Currency'])['Value'].mean().reset_index()

monthly_avg.to_excel("parsed_data.xlsx")
