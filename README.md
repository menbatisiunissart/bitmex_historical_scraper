# BITMEX Historical Data Scraper

Bitmex no longer offers historical trade data on their REST API. They do have the data in a public AWS bucket, which this scrapes and converts to CSV files (by year).


### Installation
1. Clone/download repository
2. Install requirements: `pipenv install`

### Select Python interpreter in VS Code
To run debugger in VS Code, the Python interpreter needs to point to the virtual environment created by pipenv
1. Check where the virtual environment was created
```sh
pipenv --venv
```

2. To select that specific virtual environment, use the `Python: Select Interpreter command` from the Command Palette `(⇧⌘P)`.

3. The `Python: Select Interpreter command` displays a list of available environments. Select the environment pointing to the virtual environment created earlier.

4. If step 3. does not work, reset the interpreter path as follows: Selected interpreters are stored in a shared persistent state, if you want to remove them you need to run either of the following commands (or both):

`Python: Clear Internal Extension Cache (this will affect all interpreters selected for all workspaces)`
`Python: Clear Workspace Interpreter Setting (this will only affect the current workspace)`

### Usage
* `python scrape.py` - Scrape all available data
* `python scrape.py --start YYYYMMDD` - Scrape data from start date through yesterday
* `python scrape.py --start YYYYMMDD --end YYYYMMDD` - Scrape data from start date through end date (inclusive)
* `python scrape.py --end YYYYMMDD` - Scrape data from start of data through end date
