import streamlit as st
import pandas as pd
import mysql.connector
import requests
from streamlit_option_menu import option_menu

# ---------------- MYSQL CONNECTION ----------------
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="harvardfinalproject"
)
cursor = mydb.cursor(buffered=True)

# ---------------- API ----------------
api_key = "api_key"  # replace
url = "https://api.harvardartmuseums.org/object"

# ---------------- CREATE TABLES ----------------
def create_tables():
    cursor.execute("""CREATE TABLE IF NOT EXISTS artifact_metadata(
        id INT PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INT,
        accessionmethod TEXT
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS artifact_media(
        objectid INT,
        imagecount INT,
        mediacount INT,
        colorcount INT,
        rank INT,
        datebegin INT,
        dateend INT,
        FOREIGN KEY(objectid) REFERENCES artifact_metadata(id)
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS artifact_colors(
        objectid INT,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent FLOAT,
        css3 TEXT,
        FOREIGN KEY(objectid) REFERENCES artifact_metadata(id)
    )""")

create_tables()

# ---------------- FETCH DATA ----------------
def classes(api_key, class_name):
    all_records = []
    for page in range(1, 26):
        params = {
            "apikey": api_key,
            "size": 100,
            "page": page,
            "hasimage": 1,
            "classification": class_name   # fixed typo
        }
        r = requests.get(url, params=params)
        data = r.json()
        all_records.extend(data.get("records", []))
    return all_records

# ---------------- PROCESS ----------------
def artifact_details(records):
    metadata, media, colors = [], [], []

    for i in records:
        metadata.append(dict(
            id=i.get('id'),
            title=i.get('title'),
            culture=i.get('culture'),
            period=i.get('period'),
            century=i.get('century'),
            medium=i.get('medium'),
            dimensions=i.get('dimensions'),
            description=i.get('description'),
            department=i.get('department'),
            classification=i.get('classification'),
            accessionyear=i.get('accessionyear'),
            accessionmethod=i.get('accessionmethod')
        ))

        media.append(dict(
            objectid=i.get('id'),
            imagecount=i.get('imagecount'),
            mediacount=i.get('mediacount'),
            colorcount=i.get('colorcount'),
            rank=i.get('rank'),
            datebegin=i.get('datebegin'),
            dateend=i.get('dateend')
        ))

        if i.get("colors"):
            for j in i["colors"]:
                colors.append(dict(
                    objectid=i.get('id'),
                    color=j.get('color'),
                    spectrum=j.get('spectrum'),
                    hue=j.get('hue'),
                    percent=j.get('percent'),
                    css3=j.get('css3')
                ))

    return metadata, media, colors

# ---------------- INSERT ----------------
def insert_values(metadata, media, colors):
    insert_metadata = """INSERT IGNORE INTO artifact_metadata 
        (id,title,culture,period,century,medium,dimensions,description,
         department,classification,accessionyear,accessionmethod)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    insert_media = """INSERT IGNORE INTO artifact_media
        (objectid,imagecount,mediacount,colorcount,rank,datebegin,dateend)
        VALUES (%s,%s,%s,%s,%s,%s,%s)"""

    insert_colors = """INSERT IGNORE INTO artifact_colors
        (objectid,color,spectrum,hue,percent,css3)
        VALUES (%s,%s,%s,%s,%s,%s)"""

    for i in metadata:
        cursor.execute(insert_metadata, tuple(i.values()))
    for i in media:
        cursor.execute(insert_media, tuple(i.values()))
    for i in colors:
        cursor.execute(insert_colors, tuple(i.values()))

    mydb.commit()

# ---------------- UI ----------------
st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align:center;'>Harvard Art Museums</h1>", unsafe_allow_html=True)

classification = st.text_input("Enter a classification")
button = st.button("Collect data")

menu = option_menu(None, ["Preview Data", "Migrate to SQL", "SQL Queries"], orientation="horizontal")

# ---------------- COLLECT ----------------
if button and classification:
    records = classes(api_key, classification)
    metadata, media, colors = artifact_details(records)
    st.success(f"{len(metadata)} artifacts collected")

    if menu == "Preview Data":
        st.dataframe(pd.DataFrame(metadata))

# ---------------- INSERT ----------------
if menu == "Migrate to SQL":
    if st.button("Insert"):
        records = classes(api_key, classification)
        metadata, media, colors = artifact_details(records)
        insert_values(metadata, media, colors)
        st.success("Inserted into database")

# ---------------- QUERIES ----------------
if menu == "SQL Queries":

    option = st.selectbox("Queries", (
        "1.List all artifacts from 11th century belonging to Byzantine culture.",
        "2.What are the unique cultures represented in the artifacts?",
        "3.List all artifacts from the Archaic Period.",
        "4.List artifact titles ordered by accession year in descending order.",
        "5.How many artifacts are there per department?",
        "6.Which artifacts have more than 3 images?",
        "7.What is the average rank of all artifacts?",
        "8.Which artifacts have a higher mediacount than colorcount?",
        "9.List all artifacts created between 1500 and 1600.",
        "10.How many artifacts have no media files?",
        "11.What are all the distinct hues used in the dataset?",
        "12.What are the top 5 most used colors by frequency?",
        "13.What is the average coverage percentage for each hue?",
        "14.List all colors used for a given artifact ID.",
        "15.What is the total number of color entries in the dataset?",
        "16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.",
        "17.List each artifact title with its associated hues.",
        "18.Get artifact titles, cultures, and media ranks where the period is not null.",
        "19.Find artifact titles ranked in the top 10 that include the color hue 'Grey'.",
        "20.How many artifacts exist per classification, and what is the average media count for each?",
        "21.Find the artifact with the highest media count.",
        "22.List all artifacts that have more than one hue assigned.",
        "23.Find the earliest and latest accession years in the dataset.",
        "24.List all cultures with their average artifact rank.",
        "25.Show the top 5 departments with the most artifacts."
    ))

    q = option.split(".")[0]

    if q == "1":
        cursor.execute("SELECT * FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine'")
    elif q == "2":
        cursor.execute("SELECT DISTINCT culture FROM artifact_metadata")
    elif q == "3":
        cursor.execute("SELECT * FROM artifact_metadata WHERE period='Archaic'")
    elif q == "4":
        cursor.execute("SELECT title FROM artifact_metadata ORDER BY accessionyear DESC")
    elif q == "5":
        cursor.execute("SELECT department,COUNT(*) FROM artifact_metadata GROUP BY department")
    elif q == "6":
        cursor.execute("SELECT * FROM artifact_media WHERE imagecount>3")
    elif q == "7":
        cursor.execute("SELECT AVG(rank) FROM artifact_media")
    elif q == "8":
        cursor.execute("SELECT * FROM artifact_media WHERE mediacount>colorcount")
    elif q == "9":
        cursor.execute("SELECT * FROM artifact_metadata WHERE accessionyear BETWEEN 1500 AND 1600")
    elif q == "10":
        cursor.execute("SELECT COUNT(*) FROM artifact_media WHERE imagecount=0")
    elif q == "11":
        cursor.execute("SELECT DISTINCT hue FROM artifact_colors")
    elif q == "12":
        cursor.execute("SELECT color,COUNT(*) FROM artifact_colors GROUP BY color ORDER BY COUNT(*) DESC LIMIT 5")
    elif q == "13":
        cursor.execute("SELECT hue,AVG(percent) FROM artifact_colors GROUP BY hue")
    elif q == "14":
        cursor.execute("SELECT * FROM artifact_colors WHERE objectid=1")
    elif q == "15":
        cursor.execute("SELECT COUNT(*) FROM artifact_colors")
    elif q == "16":
        cursor.execute("""SELECT title,hue FROM artifact_metadata
                          JOIN artifact_colors ON artifact_metadata.id=artifact_colors.objectid
                          WHERE culture='Byzantine'""")
    elif q == "17":
        cursor.execute("""SELECT title,hue FROM artifact_metadata
                          JOIN artifact_colors ON artifact_metadata.id=artifact_colors.objectid""")
    elif q == "18":
        cursor.execute("""SELECT title,culture,rank FROM artifact_metadata
                          JOIN artifact_media ON artifact_metadata.id=artifact_media.objectid
                          WHERE period IS NOT NULL""")
    elif q == "19":
        cursor.execute("""SELECT title FROM artifact_metadata
                          JOIN artifact_colors ON artifact_metadata.id=artifact_colors.objectid
                          JOIN artifact_media ON artifact_metadata.id=artifact_media.objectid
                          WHERE hue='Grey' ORDER BY rank ASC LIMIT 10""")
    elif q == "20":
        cursor.execute("""SELECT classification,COUNT(*),AVG(mediacount)
                          FROM artifact_metadata
                          JOIN artifact_media ON artifact_metadata.id=artifact_media.objectid
                          GROUP BY classification""")
    elif q == "21":
        cursor.execute("""SELECT title,mediacount FROM artifact_metadata
                          JOIN artifact_media ON artifact_metadata.id=artifact_media.objectid
                          ORDER BY mediacount DESC LIMIT 1""")
    elif q == "22":
        cursor.execute("""SELECT title,COUNT(DISTINCT hue)
                          FROM artifact_metadata
                          JOIN artifact_colors ON artifact_metadata.id=artifact_colors.objectid
                          GROUP BY title HAVING COUNT(DISTINCT hue)>1""")
    elif q == "23":
        cursor.execute("SELECT MIN(accessionyear),MAX(accessionyear) FROM artifact_metadata")
    elif q == "24":
        cursor.execute("""SELECT culture,AVG(rank) FROM artifact_metadata
                          JOIN artifact_media ON artifact_metadata.id=artifact_media.objectid
                          GROUP BY culture""")
    elif q == "25":
        cursor.execute("""SELECT department,COUNT(*) FROM artifact_metadata
                          GROUP BY department ORDER BY COUNT(*) DESC LIMIT 5""")

    df = pd.DataFrame(cursor.fetchall())
    st.dataframe(df)