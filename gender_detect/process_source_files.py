"""
Adapt England, Wales, Northern Ireland, Scotland and US birth names to one file

https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths/datasets/babynamesinenglandandwalesfrom1996
https://www.nisra.gov.uk/publications/baby-names-2016
https://www.nrscotland.gov.uk/statistics-and-data/statistics/statistics-by-theme/vital-events/names/babies-first-names
https://www.ssa.gov/oact/babynames/limits.html

"""

import os

import pandas as pd

ni_start_year = 1997
ni_end_year = 2016
ew_start_year = 1996
ew_end_year = 2019
s_start_year = 1974
s_end_year = 2019
us_start_year = 1950
us_end_year = 2019


def get_scotland_year(year):
    print("reading scotland {year}".format(year=year))
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "s_raw",
                            "babies-first-names-{year}.csv")
    raw_file = raw_file.format(year=year)
    cols = ["boys.position",
            "boys.name",
            "boys.count",
            "[blank]",
            "girls.position",
            "girls.name",
            "girls.count"]
    df = pd.read_csv(raw_file, header=6, names=cols, encoding="ISO-8859-1")

    dfs = []
    for g in ["boys", "girls"]:
        reduce = [x for x in cols if g in x]
        ndf = df[reduce]
        ndf.columns = ["position", "name", "count"]
        ndf = ndf.drop(columns=["position"])
        if g == "boys":
            ndf["gender"] = "M"
        else:
            ndf["gender"] = "F"
        dfs.append(ndf)
    df = pd.concat(dfs)
    df["year"] = year
    return df


def get_ni_tab(tab_name, gender):
    """
    get tab of male or female
    """
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "ni_raw",
                            "Full_Name_List_9716.xlsx")
    years = range(ni_start_year, ni_end_year + 1)
    template = ["{year}.name", "{year}.count", "{year}.rank"]
    columns = [x.format(year=y) for y in years for x in template]
    df = pd.read_excel(raw_file, sheet_name=tab_name, header=3, names=columns)
    dfs = []
    for y in years:
        year_df = df[[x.format(year=y) for x in template]]
        col_map = {x.format(year=y): x.split(".")[1] for x in template}
        year_df = year_df.rename(columns=col_map)
        year_df = year_df.drop(columns="rank")
        year_df["year"] = y
        dfs.append(year_df)
    df = pd.concat(dfs)
    df["count"] = df["count"].replace("..", 2)
    df["gender"] = gender
    return df


def get_ew_tab(tab_name, gender):
    """
    get tab of male or female
    """
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "e_raw",
                            "babynames1996to2019.xls")
    years = range(ew_start_year, ew_end_year + 1)
    template = ["{year}.rank", "{year}.count"]
    columns = ["name"] + [x.format(year=y) for y in years for x in template]
    df = pd.read_excel(raw_file, sheet_name=tab_name, header=4, names=columns)

    dfs = []
    for y in years:
        year_df = df[["name"] + [x.format(year=y) for x in template]]
        col_map = {x.format(year=y): x.split(".")[1] for x in template}
        year_df = year_df.rename(columns=col_map)
        year_df = year_df.drop(columns="rank")
        year_df["year"] = y
        dfs.append(year_df)
    df = pd.concat(dfs)
    df["count"] = df["count"].replace(":", 0)
    df = df[df["count"] != 0]
    df["gender"] = gender
    return df


def process_ni():
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "processed",
                            "ni_all_time.csv")
    # these tabs are the rank! not count! work harder!
    male = get_ni_tab("Table 1", "M")
    female = get_ni_tab("Table 2", "F")
    df = pd.concat([male, female])
    ndf = df.pivot_table('count', 'name', 'gender', 'sum', fill_value=0)
    ndf = ndf.reset_index()
    ndf["source"] = "ni"
    ndf.to_csv(raw_file, index=False)


def process_ew():
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "processed",
                            "ew_all_time.csv")
    # these tabs are the rank! not count! work harder!
    male = get_ew_tab("Boys", "M")
    female = get_ew_tab("Girls", "F")
    df = pd.concat([male, female])
    ndf = df.pivot_table('count', 'name', 'gender', 'sum', fill_value=0)
    ndf = ndf.reset_index()
    ndf["source"] = "ew"
    ndf = ndf.iloc[4:]
    ndf.to_csv(raw_file, index=False)


def process_scotland():
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "processed",
                            "s_all_time.csv")
    years = range(s_start_year, s_end_year+1)
    dfs = [get_scotland_year(x) for x in years]
    df = pd.concat(dfs)
    ndf = df.pivot_table('count', 'name', 'gender', 'sum', fill_value=0)
    ndf = ndf.reset_index()
    ndf["source"] = "s"
    ndf.to_csv(raw_file, index=False)


def process_us():
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "processed",
                            "us_all_time.csv")
    dfs = []
    for x in range(us_start_year, us_end_year+1):
        dfs.append(adapt_us_year(x))
    df = pd.concat(dfs)
    ndf = df.pivot_table('count', 'name', 'gender', 'sum', fill_value=0)
    ndf = ndf.reset_index()
    ndf["source"] = "US"
    ndf.to_csv(raw_file, index=False)


def adapt_us_year(year: int):
    folder = os.path.dirname(__file__)
    raw_file = os.path.join(folder, "assets", "a_raw",
                            "yob{year}.txt".format(year=year))
    df = pd.read_csv(raw_file,
                     names=["name", "gender", "count"],
                     dtype={"name": "string",
                            "gender": "string",
                            "count": "int"})
    df["year"] = year
    return df

def combine_counts():
    """
    get a single count for all UK countries
    """
    folder = os.path.dirname(__file__)
    path = os.path.join(folder, "assets", "processed")
    sources = ["ew", "s", "ni"]
    dfs = []
    for s in sources:
        filepath = os.path.join(path, "{source}_all_time.csv").format(source=s)
        dfs.append(pd.read_csv(filepath))
    df = pd.concat(dfs)
    df["name"] = df["name"].str.lower()
    ndf = df.pivot_table(["F", "M"], 'name', aggfunc='sum', fill_value=0)
    ndf = ndf.reset_index()
    ndf["source"] = "UK"
    ndf.to_csv(os.path.join(path, "uk_all_time.csv"), index=False)


def fill_in():
    """
    expand with extra values from US while preserving uk info
    """
    folder = os.path.dirname(__file__)
    path = os.path.join(folder, "assets", "processed")
    filepath = os.path.join(path, "{source}_all_time.csv").format(source="uk")
    usfilepath = os.path.join(
        path, "{source}_all_time.csv").format(source="us")
    df = pd.read_csv(filepath)
    us_df = pd.read_csv(usfilepath)
    us_df["name"] = us_df["name"].str.lower()
    us_df = us_df[us_df['name'].isin(df["name"]) == False]
    ndf = pd.concat([df, us_df])
    ndf.to_csv(os.path.join(path, "uk_plus_all_time.csv"), index=False)


def process_source():
    process_scotland()
    process_ni()
    process_ew()
    process_us()


if __name__ == "__main__":
    process_source()
    combine_counts()
    fill_in()