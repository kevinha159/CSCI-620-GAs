import pandas

def format_table1(filename):

    # headers on row 5
    df = pandas.read_excel(file, header=5)

    # ":," means select all rows, 0 is for the column
    # this just gets rid of the tabs in the first column because they look weird when doing df.head()
    # turning regex on means it'll find the \t anywhere in the string
    df.iloc[:,0] = df.iloc[:,0].replace("\t", "", regex=True) 

    # These rows were just descriptions of the rows underneath
    # 0 said Both Sexes, 15 said Male, 30 said Female
    df = df.drop([0, 15, 30])

    all_both_sex = df.iloc[0:14]
    all_male = df.iloc[14:29]
    all_female = df.iloc[29:44]

    all_both_sex.head(30)

file = "Table1/table-1-1.xlsx"
format_table1(file)