# dependencies
import pandas as pd
import csv
import re


class Workspace:

    def __init__(self, filepath, process="clean"):
        processlist = ["read", "clean"]
        if process not in processlist:
            raise Exception("""prcoess input not valid, please choose from the following 
                                'read' will only read in the workspace fild
                                'clean' will read in and clean the tables""")
        self.file = filepath
        if process == "read":
            self.tables = self.read_tables()
        if process == "clean":
            self.tables = self.clean_table()

    def read_tables(self):
        # list of all panel and table start demarcations from Adobe Analytics Workspace ouput file
        panelnumbers = []
        tablenumbers = []

        # opens file and reads it line by line
        with open(self.file) as f:
            data = csv.reader(f, delimiter=",")

            # loops through all lines of file and stores index of lines
            # demarking table title start to "tablenumbers" list
            for ix, row in enumerate(data):
                if len(row) > 0:
                    if "#=================================================================" in row[0]:
                        panelnumbers.append(ix + 1)
                    if "##############################################" in row[0]:
                        tablenumbers.append(ix)

        # dictionary to store table name and table as pandas dataframe
        tablesdict = {}
        # cycles through every other tablenumbers list element as each table title is
        # preceeded and followed by the line of number hashes "#"
        panelstarts = panelnumbers[::2]
        tablestarts = tablenumbers[1::2]

        allrows = panelstarts + tablestarts
        allrows.sort()
        with open(self.file) as f:
            alllines = f.readlines()
            for ix, tablerow in enumerate(allrows):
                if tablerow in panelstarts:
                    panelname = re.sub(r"# |\n", "", alllines[tablerow])
                    tablesdict.update({panelname: {}})
                else:
                    # each table name beings with a hash and is between two rows of hashes
                    tablename = alllines[tablerow - 1].replace("# ", "").strip()

                    # each table length is determined by the start of the next table
                    # and is plugged into the nrows param, since the final table does not have a
                    # "next table" we need a special case for it stated in the "if"
                    if ix == len(allrows) - 1:
                        df = pd.read_csv(self.file, header=tablerow + 1, skip_blank_lines=False)
                    else:
                        df = pd.read_csv(self.file, header=tablerow + 1, nrows=allrows[ix + 1] - allrows[ix] - 3,
                                         skip_blank_lines=False)

                    # fills all empty rows in the index cols for future cleaning purposes
                    tablesdict[panelname].update({tablename: df.fillna(method='bfill', axis=0).dropna()})
        return tablesdict

    def clean_table(self):
        tablesdict = self.read_tables()
        for (panelname, panel) in tablesdict.items():
            for (tablename, df) in panel.items():
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

                        # tables with multiple headers will have every column dtype be dtype("O")
                        # if a cell at i, j contains a string that contains only digit characters
                        # it is recognized as a numerical data cell the first occurence of this marks
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
                    df.columns = (df.columns + [" -- "] + df.iloc[0])

                # same goes for 3 header rows
                # based off my experience with Adobe Analytics,
                # more than 3 header rows seems unlikely so is not included
                # if your table has more than 3 header rows then recreate this elif for headerend = 3
                # and follow the same structure
                elif headerend == 2:
                    df.columns = (df.columns + [' -- '] + df.iloc[0] + [" -- "] + df.iloc[1])

                # we then remove the "Unnamed" column name from any columns that may have that string in them
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
                df.reset_index(inplace=True, drop=True)
                df.index.name = tablename


                # we save the header row end and "index" col end as metadata to carry with us for future cleaning
                dfmetadata = dict(headerend=headerend, indexend=indexend)

                # overwrite the unformatted tables in tabledicts with newly cleaned tables
                # in a list with relevant metadata
                tablesdict[panelname].update({tablename: [df, dfmetadata]})

        return tablesdict

    def export(self, savepath):
        # initialize ExcelWriter to save all panels and tables to a new workbook
        # with all panels creating new worksheets and each corresponding table appending to the end
        writer = pd.ExcelWriter(savepath)

        # loop through the nested dictionary of panels to create each worksheet
        for (panelname, panel) in self.tables.items():
            panelname = panelname.replace(" ", "").replace("-", "")
            if len(panelname) > 31:
                panelname = panelname[:31]
            startrow = 1
            # loop through the panel to append each table to the worksheet
            # each table has a length which will mark where the next table should be placed with 4 rows as buffer
            # the nested dictionary contains the table and metadata of table which helps with manipulation
            # but is unneeded for export
            for (name, table) in panel.items():
                table[0].to_excel(writer, sheet_name=panelname, startcol=1, startrow=startrow)
                startrow += len(table[0]) + 4

        writer.save()
        writer.close()

        print("Export to Excel complete")
