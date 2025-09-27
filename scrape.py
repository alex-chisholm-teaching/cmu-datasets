import time
import requests

url = "https://www.cmu.edu/"

# Wait 1 second between requests
time.sleep(1)  

# Tell the server who you are
headers = {
    "User-Agent": "Educational Web Scraper - Python 2 CMU Course Project"
}

response = requests.get(url, headers=headers)

response.text


web_page = """
<!DOCTYPE html>
<html>
<head>
    <title>Page Title</title>
</head>
<body>
    <h1 class="main-heading">Welcome</h1>
    <div id="content">
        <p class="description">This is a paragraph.</p>
        <ul class="item-list">
            <li data-price="29.99">Product A</li>
            <li data-price="39.99">Product B</li>
        </ul>
    </div>
</body>
</html>
"""


from bs4 import BeautifulSoup


soup = BeautifulSoup(web_page, "html.parser")
soup.find("h1").text
soup.find("p")


soup.find("li")
soup.find_all("li")
soup.select("li")
[li.text for li in soup.find_all("li")]

soup.find('h1', class_="main-heading").text
 
soup.find(id="content")


import requests
response = requests.get("https://www.cmu.edu/")
cmu_homepage = response.text
soup = BeautifulSoup(cmu_homepage, "html.parser")

soup.find(class_="Hero__posts")


response = requests.get("https://play.google.com/store/games")
play = response.text
soup = BeautifulSoup(play, "html.parser")
soup.find(class_="ftgkle").get_text(strip = True)



# data


response = requests.get(url).text
soup = BeautifulSoup(response, "html.parser")



url = "https://www.olympics.com/en/olympic-games/paris-2024/results/swimming/women-100m-backstroke"


url = "https://eredivisie.eu/competition/table/"
# headers = {'User-Agent': 'Educational Web Scraper'}
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')


import pandas as pd
# Look for results table
table = soup.find('table')

# After you have: table = soup.find('table')
rows = []

# Extract all table rows
for tr in table.find_all('tr'):
    # Get text from each cell (td or th)
    row_data = [cell.get_text(strip=True) for cell in tr.find_all(['td', 'th'])]
    rows.append(row_data)

# Convert to DataFrame (first row becomes column headers)
df = pd.DataFrame(rows[1:], columns=rows[0])
