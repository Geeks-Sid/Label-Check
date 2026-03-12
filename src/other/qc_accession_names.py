

file_name = r"C:\Users\sthakur1\Downloads\Work\WSI-Processing\2024-12-20-assoc\output.csv"

with open(file_name, "r") as f:
    lines = f.readlines()

    # Look for the expression NP23 in the lines
    for line in lines:
        if "NP23" in line:
            pass
        else:
            print("No NP23 found in this line : ", line.strip())