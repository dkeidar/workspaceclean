# workspaceclean

### Intro
I wrote this script because I couldn't get admin access to an Adobe Analytics Account to use the API.

Thus, I had to create overly complicated workspaces that were a mess and took too long to clean up for reporting.

And so began this package.

workspace clean will take this 

<img width="1428" alt="beforeworkspaceclean" src="https://github.com/dkeidar/workspaceclean/blob/master/beforeworkspaceclean.png?raw=True">

and output give you back a workbook that looks like this

<img width="1428" alt="beforeworkspaceclean" src="https://github.com/dkeidar/workspaceclean/blob/master/afterworkspaceclean.png?raw=True">

### How to Use
#### Simple (without manipulation)
If you don't need to manipulate any of the tables or want to do so in excel all you need to run is the following code:

```
from workspaceclean import Workspace
filepath = r"path\to\file.csv"
ws = Workspace(filepath)
ws.export(r"path\to\new_file.xlsx")
```

### Less Simple(?) (with manipulation)
Say you want to run manipulations on your tables once they're all formatted and pretty you'll have to do some extra steps

```
from workspaceclean import Workspace
filepath = r"path\to\file.csv"
ws = Workspace(filepath)
```
create a variable that holds the nested dict
```
tabledicts = ws.tables
<your code to manipulate tables within tabledicts>
```

then you need to take the updated tabledicts which has all your manipulated data and override ws.tables
```
ws.tables = tabledicts
```
then export as normal
```
ws.export(r"path\to\new_file.xlsx")
```

### NOTES
this is far from perfect but if you stumble upon this and have additional stuff you want added let me know and I'll see if I can fix/add to it
