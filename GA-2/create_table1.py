import mysql.connector
import re
import pandas as pd

# This is a script to be ran only once. Stores only the youngest six age groups for each race and sex
# for the Table 1 tables. Mostly Dharmick's code that I barely changed

db = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "CSCI_620"
    )

mycursor = db.cursor()

for i in range(1, 10):

    file = f"Table1/table-1-{i}.xlsx"

    df = pd.read_excel(file, header=5)

    df.iloc[:, 0] = df.iloc[:, 0].replace("\t", "", regex=True)

    df = df.drop([0, 1, 3, 15, 16, 18, 30, 31, 33])

    df.replace('Z', 0, inplace=True)

    df.set_index(df.columns[0], inplace=True)

    # Only want to store the first six age groups for this part of the sharded database
    both_sex = df.iloc[:6].copy()
    male = df.iloc[12:18].copy()
    female = df.iloc[24:30].copy()

    genders = {
        'both': both_sex,
        'male': male,
        'female': female
    }

    for gender_type, gender_data in genders.items():
        create_table_query = f"CREATE TABLE IF NOT EXISTS table_1_{i}_{gender_type} (`Age groups` VARCHAR(100), "
        count = 0
        for j in gender_data.columns:
            if count > 0:
                create_table_query += ", "
            else:
                count += 1
            create_table_query += "`" + j + "` int UNSIGNED"

        create_table_query += ", PRIMARY KEY (`Age groups`))"
        mycursor.execute(create_table_query)
        db.commit()

        batch_size = 100
        batch_i = 0

        vallist = []
        for k in range(len(gender_data)):
            batch_i += 1
            values = gender_data.iloc[k].tolist()
            values.insert(0, re.sub(r'^\s+', '', gender_data.index[k]))
            vallist.append(values)
            placeholders = ", ".join(['%s'] * len(values))
            query = (f"INSERT INTO table_1_{i}_{gender_type} VALUES ({placeholders})")
            if batch_i % batch_size == 0:
                mycursor.executemany(query, vallist)
                db.commit()

        mycursor.executemany(query, vallist)
        db.commit()