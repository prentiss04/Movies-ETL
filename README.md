# Movies-ETL

# Movies-ETL_pdouglass

## Assumptions
* While not an assumption per se, I had trouble calling the config.py file for my db_password once in the VS Code environment, despite having the aforementioned file open. Upon guidance/recommendation I had success once I opened the entire folder in VS Code which then provided a clean path to reference the password file. 
* I utilized three for-except blocks when structuring the code with one block per initial extraction (the Wikipedia JSON file, the Kaggle movie_data csv file and the MovieLens ratings csv file). 
* As it is difficult to determine what potential errors may disrupt the extraction (i.e. unforseen additional columns, column misspellings, misspelled row items), the block was wrapped around as large a block as possible to cover all bases. 
* This currently assumes that within the raw wiki_movies JSON file, no additional languages will be considered in the future. For example, if Tamal, Hindi or another prevelant language used in the mammoth Mumbai movie industry are incorporated in the wiki, this program will not consider them without additional refinement. 
* In the event that the wiki does include the roughly 1000 movies per year produced in "Bollywood" AND the code is modified to include those additioanl columns, both the box office and budget parsing steps will need to be considered due to the non-US currrency values. 
* While the module chose to exclude the overseas revenue/budget figures due to uncertaintly pertaining to currency conversions, if an overseas film industry (like the one mentioned above) becomes a sizeable portion of the database, treatement of that exchange is needed (spot price, yearly average, weighted average?). 
