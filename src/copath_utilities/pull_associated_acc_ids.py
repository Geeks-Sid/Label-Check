import argparse
import csv
import re
import sys

from copath_texttypes import (
    format_text_agg_columns,
    format_text_select_columns,
    format_texttype_id_list,
)

INVALID_ACCESSIONS_FILENAME = "invalid_accessions.csv"
ACCESSION_PATTERN = re.compile(r"^([A-Za-z]+)(\d{2})-(\d+)$")

query = """
  WITH text_agg AS (
      SELECT
          t.specimen_id,
{text_agg_columns}
      FROM c_spec_text t
      WHERE t.texttype_id IN (
{texttype_id_list}
      )
      GROUP BY t.specimen_id
  ),
  part_base AS (
      SELECT DISTINCT
          p.specimen_id,
          pt.name AS part_type_name,
          sp.name AS surgproc_name
      FROM p_part p
      LEFT JOIN c_d_parttype pt
          ON pt.id = p.parttype_id
      LEFT JOIN c_d_surgproc sp
          ON sp.id = pt.surgproc_id
  ),
  part_agg AS (
      SELECT
          specimen_id,
          STRING_AGG(part_type_name, '; ') AS part_types,
          STRING_AGG(surgproc_name, '; ') AS surgical_procedures
      FROM part_base
      GROUP BY specimen_id
  ),
  seed_accession AS (
      SELECT
          s.specimen_id,
          s.specnum_formatted,
          s.patdemog_id,
          s.client_id
      FROM c_specimen s
      WHERE s.numwheel_id = '{numwheel_id}'
        AND s.specnum_year = {specnum_year}
        AND s.specnum_num = {specnum_num}
  ),
  validated_patient AS (
      SELECT DISTINCT
          sa.patdemog_id,
          sa.client_id,
          mr.medrec_num
      FROM seed_accession sa
      JOIN r_medrec mr
          ON mr.patdemog_id = sa.patdemog_id
         AND mr.client_id = sa.client_id
         AND mr.medrec_num = '{mrn}'
  )
  SELECT
      s.specnum_formatted AS accession_id,
      vp.medrec_num AS mrn,
      s.specimen_id AS specimen_id,
      s.accession_date,
      pd.date_of_birth,
      ap.attending_physician,
      s.specclass_id AS specimen_class_id,
      sc.name AS specimen_class_name,
      pa.part_types AS sample_acquisition_type,
{text_select_columns}
  FROM validated_patient vp
  JOIN c_specimen s
      ON s.patdemog_id = vp.patdemog_id
  JOIN r_pat_demograph pd
      ON pd.patdemog_id = s.patdemog_id
  LEFT JOIN text_agg ta
      ON ta.specimen_id = s.specimen_id
  LEFT JOIN part_agg pa
      ON pa.specimen_id = s.specimen_id
  LEFT JOIN c_d_specclass sc
      ON sc.id = s.specclass_id
  LEFT JOIN r_encounter e
      ON e.encounter_id = s.encounter_id
  OUTER APPLY (
      SELECT TOP 1
          p.prettyprint_name AS attending_physician
      FROM r_other_mds om
      JOIN c_d_person p
          ON p.id = om.person_id
      WHERE om.encounter_id = e.encounter_id
      ORDER BY
          CASE om.tier_level
              WHEN 'P' THEN 1
              WHEN 'S' THEN 2
              ELSE 3
          END,
          om.sequence,
          p.prettyprint_name
  ) ap
  WHERE NOT (
      s.numwheel_id = '{numwheel_id}'
      AND s.specnum_year = {specnum_year}
      AND s.specnum_num = {specnum_num}
  )
  ORDER BY
      s.accession_date DESC,
      s.specnum_year DESC,
      s.specnum_num DESC;
"""


def escape_sql_literal(value):
    return value.replace("'", "''")


def parse_accession_id(accession_id):
    normalized_accession_id = accession_id.strip()
    match = ACCESSION_PATTERN.fullmatch(normalized_accession_id)
    if match is None:
        raise ValueError("expected format PREFIXYY-NUMBER, e.g. SP25-0001")

    numwheel_id, two_digit_year, specnum_num = match.groups()
    year_value = int(two_digit_year)
    specnum_year = 1900 + year_value if year_value >= 80 else 2000 + year_value

    return {
        "accession_id": normalized_accession_id,
        "numwheel_id": numwheel_id.upper(),
        "specnum_year": specnum_year,
        "specnum_num": int(specnum_num),
    }


def write_invalid_accessions_csv(invalid_accessions, output_path=INVALID_ACCESSIONS_FILENAME):
    if not invalid_accessions:
        return None

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["accession_id", "reason"])
        writer.writeheader()
        writer.writerows(invalid_accessions)

    return output_path


def format_query(mrn, accession):
    return query.format(
        mrn=escape_sql_literal(mrn),
        accession_id=escape_sql_literal(accession["accession_id"]),
        numwheel_id=escape_sql_literal(accession["numwheel_id"]),
        specnum_year=accession["specnum_year"],
        specnum_num=accession["specnum_num"],
        text_agg_columns=format_text_agg_columns(indent="          "),
        texttype_id_list=format_texttype_id_list(indent="          "),
        text_select_columns=format_text_select_columns("ta", indent="      "),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Script to create a SQL query for CoPath to pull data for all accession IDs for a given MRN and associated accession ID"
    )
    parser.add_argument(
            "-m", "--mrn",
            help="MRN of patient"
    )
    parser.add_argument(
            "-a", "--accession_id",
            help="Verified accession ID related to MRN",
            required=False
    )
    parser.add_argument(
            "-o", "--output_file",
            help="Optional name of output_file (defaults to pull_associated_ids_for_{accession_id}.sql",
            required=False
    )

    args = parser.parse_args()

    mrn = args.mrn
    acc = args.accession_id
    invalid_accessions_csv = None

    if mrn is None:
        print("Error: --mrn is required")
        sys.exit(1)

    if acc is None:
        print("Error: --accession_id is required")
        sys.exit(1)

    try:
        parsed_accession = parse_accession_id(acc)
    except ValueError as exc:
        reason = str(exc)
        print(f"Error: invalid accession '{acc}': {reason}")
        invalid_accessions_csv = write_invalid_accessions_csv([{
            "accession_id": acc,
            "reason": reason,
        }])
        if invalid_accessions_csv is not None:
            print(f"Wrote invalid accession IDs to {invalid_accessions_csv}")
        sys.exit(1)

    formatted_query = format_query(mrn, parsed_accession)

    output_file = args.output_file
    if output_file is None:
        output_file = f"pull_associated_ids_for_{acc}.sql"

    with open(output_file, 'w') as f:
        f.write(formatted_query)
        print(f"Wrote SQL query to {output_file}")
