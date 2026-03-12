"""
This script takes an input CSV file produced from the Label-Check pipeline (post-QC), extracts the accession ID, stain, block number, and original file paths, and creates new file paths according to guidelines in IuCompPath/Wiki/Digital_Pathology/Database_construction_and_management/PseudoID_conventions.md.

For the time being, this script assumes that it is working with only two types of tissue: brain and breast. The tissue is labelled as BRAIN if the accession ID begins with "NP"; otherwise, it is labelled as BRST.

This script also creates a PID column that assigns unique patient identifiers starting from the end of the current brain mastersheet. Currently, the brain mastersheet ends at 'AAAHQF'; this value can be edited in the PID.pid variable below.

Finally, this script renames the original files to the newly-generated file paths. Both the original and new file paths are retained in the resulting CSV.
"""

import sys
import pandas as pd
from pathlib import Path
import argparse

class PID:
    pid = 'AAAHQF'
    prev = {}

    def __init__(self, instance):
        self.instance = instance


def assign_pid(x: str) -> str:
    pid = PID.pid

    if x not in PID.prev:
        reverse = list(pid[::-1])
        for i in range(len(reverse)):
            cur = reverse[i]
            if cur == 'Z':
                reverse[i] = 'A'
            else:
                reverse[i] = chr(ord(cur) + 1)
                break
        reverse = "".join(reverse)
        pid = reverse[::-1]
        PID.pid = pid
        PID.prev[x] = pid

    return PID.prev[x]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Rename files using CSV produced from the fourth step of the Label-Check pipeline (app.py)"
    )
    parser.add_argument(
            "--input_csv",
            type=Path,
            required=True,
            help="CSV produced by step three of Label-Check pipeline and edited by step four (QC)",
    )

    args = parser.parse_args()

    post_qc = pd.read_csv(args.input_csv)

    fields = ['AccessionID', 'Stain', 'BlockNumber', 'original_slide_path']

    # check that input CSV has the above columns and populate the new CSV with these columns
    final = pd.DataFrame()
    for i in range(len(fields)):
        f = fields[i]
        if f not in post_qc.columns:
            print(f"ERR: input CSV does not have a {f} field")
            print("Make sure that your CSV has been produced by the Label-Check pipeline and edited via the flask app")
            sys.exit(1)

        final.insert(i, fields[i], post_qc.pop(fields[i]))

    # generate PIDs
    final.insert(0, 'PID', None)
    for i in range(len(final)):
        orig = final.loc[i, 'AccessionID']
        pid = assign_pid(orig)
        final.loc[i, 'PID'] = pid
        print(f"Assigned {pid} to {orig}")
    
    # create new paths
    final.insert(final.columns.get_loc('original_slide_path') + 1, 'new_slide_path', None)
    num_changed = 0
    for i in range(len(final)):
        o_path = Path(final.loc[i, 'original_slide_path'])
        o_parent = o_path.parent
        o_ext = o_path.suffix
        
        pid = final.loc[i, 'PID']
        stain = final.loc[i, 'Stain']
        block = final.loc[i, 'BlockNumber']
       
        # organ type in this script is determined by whether the accession ID
        # has NP at the beginning; if yes, organ is BRAIN, if no, organ is BRST
        a_id = final.loc[i, 'AccessionID']
        if a_id[:2] == "NP":
            organ = "BRAIN"
        else:
            organ = "BRST"

        # for now, ImageType and SampleAcqType are assumed, and there is no SectionCount field
        new_name = f"{organ}_{pid}_XXXXXXXX_XXXX_{stain}_WSI_RE{block}{o_ext}"
        new_path = o_parent / new_name
       
        # make sure path exists before renaming
        if o_path.exists():
            # not renaming yet, for testing purposes
            # o_path.rename(new_path)
            num_changed += 1
            final.loc[i, 'new_slide_path'] = new_path   
        else:
            print(f"ERR: {o_path} doesn't exist")
            continue

    # write new CSV to post_qc.csv in the QC directory
    parent = args.input_csv.parent
    post_qc = parent / "post_qc.csv"
    final.to_csv(post_qc, index=False)
    print(f"Changed {num_changed} file paths")
    print(f"Wrote new CSV to {post_qc}")
