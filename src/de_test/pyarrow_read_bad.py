from pyarrow.csv import InvalidRow, ParseOptions
from pyarrow.csv import read_csv

filename = r"./data/Налогоплательщики_2022_60M.txt"

def onBadLineHandler(bad_line:InvalidRow):
    with open("./data/bad.txt", 'a+') as file:
        file.write(bad_line.text)
    return 'skip'

arrow_po = ParseOptions(
    invalid_row_handler=onBadLineHandler,
    newlines_in_values=True
)
df = read_csv(input_file=filename, parse_options=arrow_po)