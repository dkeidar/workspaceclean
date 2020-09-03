# dependencies
import pandas as pd
import csv
import re


# read in file
filepath = f"VZ_DWK_MonQuarReport_Updated - VZ Global - (verizontelecomglobal) - Jul 2, 2020.csv"
print(f"processing file: {filepath}")


class WorkspaceClean():

    def __init__(self, file):
        self.file = file

    def readtables(self):
        # list of all table start demarcations from Adobe Analytics Workspace ouput file
        tablenumbers = []

        # opens file and reads it line by line
        with open(self.file) as f:
            data = csv.reader(f, delimiter=",")

            # loops through all lines of file and stores index of lines
            # demarking table title start to "tablenumbers" list
            for ix, row in enumerate(data):
                if "##############################################" in row:
                    tablenumbers.append(ix)

        # dictionary to store table name and table as pandas dataframe
        tablesdict = {}
        # cycles through every other tablenumbers list element as each table title is
        # preceeded and followed by the line of number hashes "#"
        tablestarts = tablenumbers[1::2]
        with open(self.file) as f:
            alllines = f.readlines()
            for ix, tablerow in tqdm(enumerate(tablestarts)):

                # each table name beings with a hash and is between two rows of hashes
                tablename = alllines[tablerow - 1].replace("# ", "").strip()

                # each table length is determined by the start of the next table and is plugged into the nrows param,
                # since the final table does not have a "next table" we need a special case for it stated in the "if"
                if ix == len(tablestarts) - 1:
                    df = pd.read_csv(self.file, header=tablerow + 1, skip_blank_lines=False)
                else:
                    df = pd.read_csv(self.file, header=tablerow + 1, nrows=tablestarts[ix + 1] - tablestarts[ix] - 3,
                                     skip_blank_lines=False)

                # fills all empty rows in the index cols for future cleaning purposes
                tablesdict[tablename] = df.fillna(method='bfill', axis=0).dropna()
        return tablesdict

    def tablefix(self, tablesdict):
        for name, df in tablesdict.items():
            # each table has "index" cols and "header" cols which data is cut by
            # this for loop determines where those "index" and "header" cols end, or where the data begins
            # and saves the i, j (row, col) to use later when creating headers and cleaning the tables
            headerend = 0
            indexend = 0
            for i, j in df.iterrows():
                for ix, row in enumerate(j):

                    # some tables have the one header row thus pandas can read in some tables by correct dtype
                    # we can determine this seeing which columns are not dtype("O")
                    # as the start of the data for those tables
                    if type(row) is not str:
                        headerend = 0
                        indexend = ix - 1
                        break

                    # tables with multiple headers will have every columne dtype be dtype("O")
                    # if a cell at i, j contains a string that contains only digit characters
                    # it is recognized as a numerical data cellthe first occurance of this marks
                    # the min(i, j) which would be the start of the numerical data in a table
                    # once that cell is found we can break from the loop
                    if re.search(r"\D+", row) is None:
                        headerend = i
                        indexend = ix - 1
                        break
                else:
                    continue
                break

            # once the final row containing header information is determined we begin to combine
            # each row for one header if there is only one header row then we simply need to
            # replace the "Unnamed" columns caused by Adobe Analytics table structure
            # we can just rename those unnamed columns by the header information in row 0
            if headerend == 0:
                for ix, colname in enumerate(df.columns):
                    if "Unnamed:" in colname:
                        df.rename(columns={colname: df.iloc[0, ix]}, inplace=True)

            # if there are two header rows then we combine both with a " -- " for future cleaning
            if headerend == 1:
                df.columns = (df.columns + " -- " + df.iloc[0])

            # same goes for 3 header rows
            # based off my experience with Adobe Analytics, more than 3 header rows seems unlikely so is not included
            # if your table has more than 3 header rows then recreate this elif for headerend = 3
            # and follow the same structure
            elif headerend == 2:
                df.columns = (df.columns + " -- " + df.iloc[0] + " -- " + df.iloc[1])

            # we then remove the "Unnamed" columne name from any columns that may have that string in them
            regexstr = r"\.\d+|(Unnamed: \d -- )"
            df.columns = [re.sub(regexstr, "", x) for x in df.columns]

            # for any total cols this process will return a column header with the structure
            # <metric A> -- <metric A> -- <metric A>
            # thus we can remove the redundant information and assume that it is a <metric A> Total column
            df.columns = [col.split(" -- ")[0] + " Total"
                          if (len(col.split(" -- ")) > 1) and (col.split(" -- ")[0] == col.split(" -- ")[1])
                          else col
                          for col in df.columns]

            # once all headers and columns are fixed,
            # we can remove all rows with header information in them to leave a table
            # of headers, "indexes", and data
            rows = len([row for row in df.iloc[:, indexend] if row in df.columns[indexend]])
            df = df.drop(labels=list(range(0, rows)))

            # we now have to ensure that all numerical data columns are in
            # dtype("float") or dtype("int") so we convert them,
            # coercing any numbers that may have commas or may raise errors
            df.iloc[:, indexend + 1:] = df.iloc[:, indexend + 1:].apply(pd.to_numeric, errors='coerce')

            # the name of the table that is pulled from the Adobe Analytics output file structure
            # and is inserted as the index col name for user use upon excel output
            df.index.name = tablename

            # we save the header row end and "index" col end as metadata to carry with us for future cleaning
            dfmetadata = dict(headerend=headerend, indexend=indexend)

        return df, dfmetadata


def dwktablefix(df, dfmetadata):
    indexend = dfmetadata["indexend"]

    # tagging issues can lead to drops in tagging information passed to some rows resulting in [invalidrsid]
    # this is removed and replaced with verizon dwk site tagging structure
    df.iloc[:, 0] = df.iloc[:, 0].apply(lambda row: row.replace(r"[invalidrsid]| ", r"esp| "))
    df.iloc[:, 0] = df.iloc[:, 0].apply(lambda row: r"esp| " + row if r"esp|" not in row else row)
    df.iloc[:, 0] = df.iloc[:, 0].apply(lambda row: row.replace(r"|", r"| ").replace("  ", " "))

    # this cleaning creates redundancies in the "index" col so we group them together to consolidate
    groupercols = [col for col in df.columns[:indexend + 1]]

    # reset the index and rename the index col to the name of the dataframe
    df = df.groupby(groupercols).sum().reset_index()
    df.index.name = name

    return df


def dwktoppagesclean(df, metadata):
    df.iloc[:, 0] = df.iloc[:, 0].apply(lambda row: re.sub(r"search.(.*)", "search results", row))
    df = dwktablefix(df, metadata)
    return df


def dwksearchclean(df):
    searchmappath = "VZ_DWK_SearchTermMap.xlsx"
    searchmap = pd.read_excel(searchmappath, encoding='latin1')

    if "querycleaned" in df.columns:
        df.drop(columns=["searchquery", "querycleaned", "sitesection"], inplace=True)
    droprows = df[~df.iloc[:, 0].str.contains("search")].index
    df = df.drop(droprows)

    df['searchquery'] = df.iloc[:, 0].apply(lambda row: re.sub(r"(.*\| )", "", row))
    df.searchquery = df.searchquery.apply(lambda row: "search results|" if row == '' else row)

    df = df.merge(searchmap, how='left', on="searchquery")

    missingqueries = df[df.querycleaned.isna()][["searchquery", "querycleaned", "sitesection"]]

    if len(missingqueries) > 0:
        searchmap = searchmap.append(missingqueries, ignore_index=True, sort=False)
        searchmap.to_excel(searchmappath, index=False, encoding='latin1')
        subprocess.Popen(searchmappath, shell=True)
        updatemap = input("please update search map and type 'done' when done.")
        if updatemap == 'done':
            df = dwksearchclean(df)

    sectionpivotdf = df.groupby("sitesection")[[col for col in df.columns if "Visits" in col]] \
        .sum().sort_values(by="Visits Total", ascending=False)

    termpivotdf = pd.DataFrame(df.groupby(["sitesection", "querycleaned"])[df.columns[-6:-4]].sum())
    termpivotdf["Total"] = termpivotdf.sum(axis=1)
    termpivotdf = termpivotdf.sort_values(by="Total", ascending=False).head(20)

    return df, sectionpivotdf, termpivotdf


tables = readintables(filepath)
tablesmeta = {}
searchpivots = {}

for (name, table) in tqdm(tables.items()):
    tables[name], meta = tablefix(table, name)
    tablesmeta[name] = meta
    tables[name] = dwktablefix(tables[name], meta)
    if "Page" in name:
        tables[name] = dwktoppagesclean(tables[name], meta)
    if "Searches" in name:
        tables[name], sectionpivot, termpivot = dwksearchclean(tables[name])
        searchpivots[f"{name} - sectionpivot"] = sectionpivot
        searchpivots[f"{name} - termpivot"] = termpivot
tables.update(searchpivots)

monthly = {name: table for (name, table) in tables.items() if " - Monthly" in name}
quarterlyall = {name: table for (name, table) in tables.items() if " - All Visitors" in name}
quarterlynvr = {name: table for (name, table) in tables.items() if (" - Visitor Type" in name) or (" Visits" in name)}
tablegroups = {"monthly": monthly, "quarterlyall": quarterlyall, "quarterlynvr": quarterlynvr}


savepath = r"C:\Users\Daniel.Keidar\Documents\Clients\Verizon\VS_DWK_Quarterly\VZ_DWK_MonQuarReportCleaned.xlsx"
writer = pd.ExcelWriter(savepath)

for (tablegroup, group) in tablegroups.items():
    startrow = 1
    for (name, table) in group.items():
        table.to_excel(writer, sheet_name=tablegroup, startcol=1, startrow=startrow)
        startrow += len(table) + 4

writer.save()
writer.close()
