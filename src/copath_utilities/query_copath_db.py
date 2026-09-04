import argparse
import csv
import re
import sys

import pyodbc
import pandas as pd
from striprtf.striprtf import rtf_to_text

from copath_connection import prepared_connection_string

from copath_texttypes import (
    compile_report_fields,
    format_text_agg_columns,
    format_text_select_columns,
    format_texttype_id_list,
    TEXT_TYPES
)

DEFAULT_INSERT_BATCH_SIZE = 900
INVALID_ACCESSIONS_FILENAME = "invalid_accessions.csv"

acc_query = """
  SET NOCOUNT ON;
  DROP TABLE IF EXISTS #input_accessions;
  DROP TABLE IF EXISTS #matched_specimens;
  CREATE TABLE #input_accessions (
      accession_id NVARCHAR(MAX) NOT NULL
  );

{insert_statements}

  CREATE TABLE #matched_specimens (
      specimen_id VARCHAR(15) NOT NULL PRIMARY KEY,
      patdemog_id VARCHAR(15) NOT NULL,
      client_id VARCHAR(15) NULL,
      encounter_id VARCHAR(15) NULL,
      specnum_formatted VARCHAR(60) NOT NULL,
      accession_date DATETIME NULL,
      specclass_id VARCHAR(15) NULL,
      signout_date DATETIME NULL,
      specstatus_id VARCHAR(15) NULL,
      status_date DATETIME NULL
  );

  INSERT INTO #matched_specimens (
      specimen_id,
      patdemog_id,
      client_id,
      encounter_id,
      specnum_formatted,
      accession_date,
      specclass_id,
      signout_date,
      specstatus_id,
      status_date
  )
  SELECT DISTINCT
      s.specimen_id,
      s.patdemog_id,
      s.client_id,
      s.encounter_id,
      s.specnum_formatted,
      s.accession_date,
      s.specclass_id,
      s.signout_date,
      s.specstatus_id,
      s.status_date
  FROM c_specimen s
  JOIN #input_accessions i
      ON i.accession_id = s.specnum_formatted;

  WITH text_agg AS (
      SELECT
          t.specimen_id,
{text_agg_columns}
      FROM c_spec_text t
      JOIN #matched_specimens ms
          ON ms.specimen_id = t.specimen_id
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
      JOIN #matched_specimens ms
          ON ms.specimen_id = p.specimen_id
      LEFT JOIN c_d_parttype pt
          ON pt.id = p.parttype_id
      LEFT JOIN c_d_surgproc sp
          ON sp.id = pt.surgproc_id
  ),
  part_agg AS (
      SELECT
          specimen_id,
          STRING_AGG(part_type_name, '; ') AS part_types
      FROM part_base
      GROUP BY specimen_id
  ),
  block_stain_base AS (
      SELECT DISTINCT
          ps.specimen_id,
          CONCAT(
              COALESCE(NULLIF(LTRIM(RTRIM(pp.part_designator)), ''), ''),
              COALESCE(NULLIF(LTRIM(RTRIM(pb.blkdesig_label)), ''), ''),
              ',',
              COALESCE(NULLIF(LTRIM(RTRIM(dsp.name)), ''), 'NO_STAIN')
          ) AS block_stain_pair
      FROM p_stainprocess ps
      JOIN #matched_specimens ms
          ON ms.specimen_id = ps.specimen_id
      LEFT JOIN p_part pp
          ON pp.specimen_id = ps.specimen_id
         AND pp.part_inst = ps.part_inst
      LEFT JOIN p_block pb
          ON pb.specimen_id = ps.specimen_id
         AND pb.part_inst = ps.part_inst
         AND pb.block_inst = ps.block_inst
      LEFT JOIN c_d_stainprocess dsp
          ON dsp.id = ps.stainprocess_id
      WHERE (
              NULLIF(LTRIM(RTRIM(pp.part_designator)), '') IS NOT NULL
           OR NULLIF(LTRIM(RTRIM(pb.blkdesig_label)), '') IS NOT NULL
            )
        AND NULLIF(LTRIM(RTRIM(dsp.name)), '') IS NOT NULL
  ),
  block_stain_agg AS (
      SELECT
          specimen_id,
          STRING_AGG(block_stain_pair, '; ') AS blocks_and_stains
      FROM block_stain_base
      GROUP BY specimen_id
  ),
  report_status_agg AS (
      SELECT
          rs.specimen_id,
          MAX(rs.repstatus_id) AS report_status,
          MAX(rs.status_datetime) AS latest_report_status_datetime
      FROM c_spec_reportstatus rs
      JOIN #matched_specimens ms
          ON ms.specimen_id = rs.specimen_id
      GROUP BY rs.specimen_id
  ),
  special_proc_ranked AS (
      SELECT
          sp.specimen_id,
          sp.sprostatus_id,
          sp.signout_date,
          ROW_NUMBER() OVER (
              PARTITION BY sp.specimen_id
              ORDER BY
                  CASE WHEN sp.signout_date IS NULL THEN 1 ELSE 0 END,
                  sp.signout_date DESC,
                  sp.sprostatus_id DESC
          ) AS row_num
      FROM p_special_proc sp
      JOIN #matched_specimens ms
          ON ms.specimen_id = sp.specimen_id
  ),
  special_proc_agg AS (
      SELECT
          specimen_id,
          MAX(CASE WHEN row_num = 1 THEN sprostatus_id END) AS proc_or_add_status,
          MAX(signout_date) AS latest_procedure_addendum_signout_date
      FROM special_proc_ranked
      GROUP BY specimen_id
  )
  SELECT
      ms.specnum_formatted AS accession_id,
      mr.medrec_num AS mrn,
      ms.specimen_id AS specimen_id,
      CONCAT_WS(
          ' ',
          NULLIF(LTRIM(RTRIM(pd.firstname)), ''),
          NULLIF(LTRIM(RTRIM(pd.middlename)), ''),
          NULLIF(LTRIM(RTRIM(pd.lastname)), '')
      ) AS patient_name,
      dr.description AS race_ethnicity,
      ms.accession_date,
      pd.date_of_birth,
      ap.attending_physician,
      ms.specclass_id AS specimen_class_id,
      sc.name AS specimen_class_name,
      pa.part_types AS sample_acquisition_type,
      bsa.blocks_and_stains,
{text_select_columns},
      ms.signout_date,
      ms.specstatus_id AS specimen_status,
      ms.status_date,
      rsa.report_status,
      rsa.latest_report_status_datetime,
      spa.proc_or_add_status,
      spa.latest_procedure_addendum_signout_date

  FROM #matched_specimens ms
  JOIN r_pat_demograph pd
      ON pd.patdemog_id = ms.patdemog_id
  LEFT JOIN c_d_race dr
      ON dr.id = pd.race_id
  LEFT JOIN text_agg ta
      ON ta.specimen_id = ms.specimen_id
  LEFT JOIN part_agg pa
      ON pa.specimen_id = ms.specimen_id
  LEFT JOIN block_stain_agg bsa
      ON bsa.specimen_id = ms.specimen_id
  LEFT JOIN c_d_specclass sc
      ON sc.id = ms.specclass_id
  LEFT JOIN report_status_agg rsa
      ON rsa.specimen_id = ms.specimen_id
  LEFT JOIN special_proc_agg spa
      ON spa.specimen_id = ms.specimen_id
  OUTER APPLY (
      SELECT TOP 1 mr.medrec_num
      FROM r_medrec mr
      WHERE mr.patdemog_id = pd.patdemog_id
        AND mr.client_id = ms.client_id
      ORDER BY mr.sequence
  ) mr
  LEFT JOIN r_encounter e
      ON e.encounter_id = ms.encounter_id
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
  ) ap;
"""

# Patient-history queries use verified accession IDs as seeds, then expand to every
# specimen for the same CoPath patient/client pair. Keeping the rest of acc_query
# identical ensures exact and longitudinal exports have the same columns.
_EXACT_MATCH_SQL = """  FROM c_specimen s
  JOIN #input_accessions i
      ON i.accession_id = s.specnum_formatted;"""
_HISTORY_MATCH_SQL = """  FROM c_specimen seed
  JOIN #input_accessions i
      ON i.accession_id = seed.specnum_formatted
  JOIN c_specimen s
      ON s.patdemog_id = seed.patdemog_id
     AND (
          s.client_id = seed.client_id
          OR (s.client_id IS NULL AND seed.client_id IS NULL)
     );"""
patient_history_query = acc_query.replace(_EXACT_MATCH_SQL, _HISTORY_MATCH_SQL, 1)
if patient_history_query == acc_query:
    raise RuntimeError("could not construct patient-history CoPath query")

mrn_query = """
  SET NOCOUNT ON;
  DROP TABLE IF EXISTS #input_ids;
  DROP TABLE IF EXISTS #matched_specimens;
  CREATE TABLE #input_ids (
      id_value VARCHAR(40) NOT NULL PRIMARY KEY
  );

{insert_statements}

  CREATE TABLE #matched_specimens (
      specimen_id VARCHAR(15) NOT NULL PRIMARY KEY,
      patdemog_id VARCHAR(15) NOT NULL,
      client_id VARCHAR(15) NULL,
      encounter_id VARCHAR(15) NULL,
      specnum_formatted VARCHAR(60) NOT NULL,
      accession_date DATETIME NULL,
      specclass_id VARCHAR(15) NULL,
      signout_date DATETIME NULL,
      specstatus_id VARCHAR(15) NULL,
      status_date DATETIME NULL
  );

  INSERT INTO #matched_specimens (
      specimen_id,
      patdemog_id,
      client_id,
      encounter_id,
      specnum_formatted,
      accession_date,
      specclass_id,
      signout_date,
      specstatus_id,
      status_date
  )
  SELECT DISTINCT
      s.specimen_id,
      s.patdemog_id,
      s.client_id,
      s.encounter_id,
      s.specnum_formatted,
      s.accession_date,
      s.specclass_id,
      s.signout_date,
      s.specstatus_id,
      s.status_date
  FROM c_specimen s
  JOIN r_medrec rm
      ON rm.patdemog_id = s.patdemog_id
     AND rm.client_id = s.client_id
  JOIN #input_ids i
      ON i.id_value = rm.medrec_num;

    WITH text_agg AS (
      SELECT
          t.specimen_id,
{text_agg_columns}
      FROM c_spec_text t
      JOIN #matched_specimens ms
          ON ms.specimen_id = t.specimen_id
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
      JOIN #matched_specimens ms
          ON ms.specimen_id = p.specimen_id
      LEFT JOIN c_d_parttype pt
          ON pt.id = p.parttype_id
      LEFT JOIN c_d_surgproc sp
          ON sp.id = pt.surgproc_id
  ),
  part_agg AS (
      SELECT
          specimen_id,
          STRING_AGG(part_type_name, '; ') AS part_types
      FROM part_base
      GROUP BY specimen_id
  ),
  block_stain_base AS (
      SELECT DISTINCT
          ps.specimen_id,
          CONCAT(
              COALESCE(NULLIF(LTRIM(RTRIM(pp.part_designator)), ''), ''),
              COALESCE(NULLIF(LTRIM(RTRIM(pb.blkdesig_label)), ''), ''),
              ',',
              COALESCE(NULLIF(LTRIM(RTRIM(dsp.name)), ''), 'NO_STAIN')
          ) AS block_stain_pair
      FROM p_stainprocess ps
      JOIN #matched_specimens ms
          ON ms.specimen_id = ps.specimen_id
      LEFT JOIN p_part pp
          ON pp.specimen_id = ps.specimen_id
         AND pp.part_inst = ps.part_inst
      LEFT JOIN p_block pb
          ON pb.specimen_id = ps.specimen_id
         AND pb.part_inst = ps.part_inst
         AND pb.block_inst = ps.block_inst
      LEFT JOIN c_d_stainprocess dsp
          ON dsp.id = ps.stainprocess_id
      WHERE (
              NULLIF(LTRIM(RTRIM(pp.part_designator)), '') IS NOT NULL
           OR NULLIF(LTRIM(RTRIM(pb.blkdesig_label)), '') IS NOT NULL
            )
        AND NULLIF(LTRIM(RTRIM(dsp.name)), '') IS NOT NULL
  ),
  block_stain_agg AS (
      SELECT
          specimen_id,
          STRING_AGG(block_stain_pair, '; ') AS blocks_and_stains
      FROM block_stain_base
      GROUP BY specimen_id
  ),
  report_status_agg AS (
      SELECT
          rs.specimen_id,
          MAX(rs.repstatus_id) AS report_status,
          MAX(rs.status_datetime) AS latest_report_status_datetime
      FROM c_spec_reportstatus rs
      JOIN #matched_specimens ms
          ON ms.specimen_id = rs.specimen_id
      GROUP BY rs.specimen_id
  ),
  special_proc_ranked AS (
      SELECT
          sp.specimen_id,
          sp.sprostatus_id,
          sp.signout_date,
          ROW_NUMBER() OVER (
              PARTITION BY sp.specimen_id
              ORDER BY
                  CASE WHEN sp.signout_date IS NULL THEN 1 ELSE 0 END,
                  sp.signout_date DESC,
                  sp.sprostatus_id DESC
          ) AS row_num
      FROM p_special_proc sp
      JOIN #matched_specimens ms
          ON ms.specimen_id = sp.specimen_id
  ),
  special_proc_agg AS (
      SELECT
          specimen_id,
          MAX(CASE WHEN row_num = 1 THEN sprostatus_id END) AS proc_or_add_status,
          MAX(signout_date) AS latest_procedure_addendum_signout_date
      FROM special_proc_ranked
      GROUP BY specimen_id
  )
  SELECT DISTINCT
      ms.specnum_formatted AS accession_id,
      mr.medrec_num AS mrn,
      ms.specimen_id AS specimen_id,
      CONCAT_WS(
          ' ',
          NULLIF(LTRIM(RTRIM(pd.firstname)), ''),
          NULLIF(LTRIM(RTRIM(pd.middlename)), ''),
          NULLIF(LTRIM(RTRIM(pd.lastname)), '')
      ) AS patient_name,
      dr.description AS race_ethnicity,
      ms.accession_date,
      pd.date_of_birth,
      ap.attending_physician,
      ms.specclass_id AS specimen_class_id,
      sc.name AS specimen_class_name,
      pa.part_types AS sample_acquisition_type,
      bsa.blocks_and_stains,
{text_select_columns},
      ms.signout_date,
      ms.specstatus_id AS specimen_status,
      ms.status_date,
      rsa.report_status,
      rsa.latest_report_status_datetime,
      spa.proc_or_add_status,
      spa.latest_procedure_addendum_signout_date

  FROM #matched_specimens ms
  JOIN r_pat_demograph pd
      ON pd.patdemog_id = ms.patdemog_id
  LEFT JOIN c_d_race dr
      ON dr.id = pd.race_id
  LEFT JOIN text_agg ta
      ON ta.specimen_id = ms.specimen_id
  LEFT JOIN part_agg pa
      ON pa.specimen_id = ms.specimen_id
  LEFT JOIN block_stain_agg bsa
      ON bsa.specimen_id = ms.specimen_id
  LEFT JOIN c_d_specclass sc
      ON sc.id = ms.specclass_id
  LEFT JOIN report_status_agg rsa
      ON rsa.specimen_id = ms.specimen_id
  LEFT JOIN special_proc_agg spa
      ON spa.specimen_id = ms.specimen_id
  OUTER APPLY (
      SELECT TOP 1 mr.medrec_num
      FROM r_medrec mr
      WHERE mr.patdemog_id = pd.patdemog_id
        AND mr.client_id = ms.client_id
      ORDER BY mr.sequence
  ) mr
  LEFT JOIN r_encounter e
      ON e.encounter_id = ms.encounter_id
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
  ORDER BY mr.medrec_num, ms.accession_date, ms.specnum_formatted;
"""

REPORT_FIELDS = [ tt[1] for tt in TEXT_TYPES ]

def parse_field(field):
    if not isinstance(field, str):
        return field # Returns original value if it's NaN or Not a String
    
    # Optional: Only convert if the RTF header is present
    if field.startswith('{\\rtf1'):
        try:
            return rtf_to_text(field)
        except:
            return field
    
    return field # Return as-is if it's just plain text


def normalize_report_field(field):
    parsed = parse_field(field)
    if parsed is None or pd.isna(parsed):
        return ""

    return str(parsed).replace('\r', '\n').replace('\x00', '')


def clean_column(results, column):
    for i in range(len(results[column])):
       field = results.loc[i, column]
       results.loc[i, column] = normalize_report_field(field)


def clean_results(results):
    for column in results.columns.to_list():
        if column in REPORT_FIELDS:
            clean_column(results, column)
    return results


def compile_report_column(results):
    report_columns = [
        column for column in REPORT_FIELDS
        if column in results.columns
    ]
    if not report_columns:
        results["report"] = ""
        return results

    report_column_index = min(results.columns.get_loc(column) for column in report_columns)
    compiled_reports = results[report_columns].apply(
        lambda row: compile_report_fields(row, report_columns),
        axis=1,
    )

    results.drop(columns=report_columns, inplace=True)
    results.insert(report_column_index, "report", compiled_reports)
    return results


def process_input_file(file_path, target_column):
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        if target_column is None:
            return [line.strip() for line in f if line.strip()]

        try:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
        except csv.Error as exc:
            raise ValueError(f"Error: {file_path} is not a standard delimited file") from exc

        if not fieldnames:
            raise ValueError(f"Error: {file_path} does not contain a header row")

        if target_column not in fieldnames:
            raise KeyError(f"Error: Column '{target_column}' not found. "
                           f"Columns detected: {fieldnames}")

        return list({row[target_column] for row in reader})


def escape_sql_literal(value):
    return value.replace("'", "''")


def parse_accession_id(accession_id):
    normalized_accession_id = accession_id.strip()
    if not normalized_accession_id:
        raise ValueError("accession ID must not be blank")
    return {"accession_id": normalized_accession_id}


def split_valid_invalid_accessions(accession_ids):
    valid_accessions = []
    invalid_accessions = []
    seen_accession_keys = set()

    for accession_id in accession_ids:
        try:
            parsed_accession = parse_accession_id(accession_id)
        except ValueError as exc:
            reason = str(exc)
            print(f"Error: invalid accession '{accession_id}': {reason}")
            invalid_accessions.append({
                "accession_id": accession_id,
                "reason": reason,
            })
            continue

        accession_key = parsed_accession["accession_id"].casefold()
        if accession_key in seen_accession_keys:
            continue

        seen_accession_keys.add(accession_key)
        valid_accessions.append(parsed_accession)

    return valid_accessions, invalid_accessions


def write_invalid_accessions_csv(invalid_accessions, output_path=INVALID_ACCESSIONS_FILENAME):
    if not invalid_accessions:
        return None

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["accession_id", "reason"])
        writer.writeheader()
        writer.writerows(invalid_accessions)

    return output_path


def normalize_ids(ids):
    normalized_ids = []
    seen = set()

    for raw_id in ids:
        cur_id = raw_id.strip()
        if cur_id == "" or cur_id in seen:
            continue

        normalized_ids.append(cur_id)
        seen.add(cur_id)

    if not normalized_ids:
        raise ValueError("Error: no non-empty identifiers were found in the input")

    return normalized_ids


def chunk_ids(ids, chunk_size):
    return [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]


def format_insert_statements(ids, batch_size):
    statements = []
    for chunk in chunk_ids(ids, batch_size):
        statements.append(format_insert_statement(chunk))

    return "\n\n".join(statements)


def validate_batch_size(batch_size):
    if batch_size < 1:
        raise ValueError("Error: batch size must be at least 1")

    return batch_size


def format_insert_statement(ids):
    formatted_rows = [f"('{escape_sql_literal(cur_id)}')" for cur_id in ids]
    values_block = ",\n  ".join(formatted_rows)

    return (
        "  INSERT INTO #input_ids (id_value)\n"
        "  VALUES\n"
        f"  {values_block};"
    )


def format_accession_insert_statements(accessions, batch_size):
    statements = []
    for chunk in chunk_ids(accessions, batch_size):
        statements.append(format_accession_insert_statement(chunk))

    return "\n\n".join(statements)


def format_accession_insert_statement(accessions):
    formatted_rows = [
        f"(N'{escape_sql_literal(accession['accession_id'])}')"
        for accession in accessions
    ]
    values_block = ",\n  ".join(formatted_rows)
    return (
        "  INSERT INTO #input_accessions "
        "(accession_id)\n"
        "  VALUES\n"
        f"  {values_block};"
    )


def format_report_query(query_template, insert_statements):
    return query_template.format(
        insert_statements=insert_statements,
        text_agg_columns=format_text_agg_columns(indent="          "),
        texttype_id_list=format_texttype_id_list(indent="          "),
        text_select_columns=format_text_select_columns("ta", indent="      "),
    )


def format_query(insert_statements, id_type):
    match id_type:
        case 'accession':
            return format_report_query(acc_query, insert_statements)
        case 'mrn':
            return format_report_query(mrn_query, insert_statements)
        case 'history':
            return format_report_query(patient_history_query, insert_statements)


def build_query_output(ids, id_type, batch_size, separate_queries):
    if not separate_queries:
        insert_statements = format_insert_statements(ids, batch_size)
        return format_query(insert_statements, id_type)

    query_blocks = []
    id_chunks = chunk_ids(ids, batch_size)
    total_chunks = len(id_chunks)

    for idx, chunk in enumerate(id_chunks, start=1):
        insert_statement = format_insert_statement(chunk)
        query_block = format_query(insert_statement, id_type)
        query_blocks.append(f"-- Batch {idx} of {total_chunks}\n{query_block}")

    return "\n\n".join(query_blocks)


def build_accession_query_output(accessions, batch_size, separate_queries, id_type='accession'):
    if not separate_queries:
        insert_statements = format_accession_insert_statements(accessions, batch_size)
        return format_query(insert_statements, id_type)

    query_blocks = []
    accession_chunks = chunk_ids(accessions, batch_size)
    total_chunks = len(accession_chunks)

    for idx, chunk in enumerate(accession_chunks, start=1):
        insert_statement = format_accession_insert_statement(chunk)
        query_block = format_query(insert_statement, id_type)
        query_blocks.append(f"-- Batch {idx} of {total_chunks}\n{query_block}")

    return "\n\n".join(query_blocks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Script to query CoPath to get reports for provided patient identifiers (accession IDs or MRNs)"
    )
    parser.add_argument(
            "input_file",
            help="List of patient identifiers or delimited file that contains a column of patient identifiers"
    )
    parser.add_argument(
            "-i", "--id_type",
            help="The type of patient identifier to search by ('accession' or 'mrn')",
            choices=['accession', 'mrn', 'history'],
            required=True
    )
    parser.add_argument(
            "-c", "--column",
            help="Name of column that contains accession IDs (if input_file contains columns)",
            required=False
    )
    parser.add_argument(
            "-o", "--output_file",
            help="Optional name of output_file (defaults to copath_SQL_results.csv)",
            default="copath_SQL_results.csv",
            required=False
    )
    parser.add_argument(
            "-b", "--batch-size",
            help="Maximum number of IDs per INSERT ... VALUES block",
            type=int,
            default=DEFAULT_INSERT_BATCH_SIZE,
            required=False
    )
    parser.add_argument(
            "--separate-queries",
            help="Generate separate full query blocks per batch instead of one combined query",
            action="store_true"
    )

    args = parser.parse_args()
    invalid_accessions_csv = None

    try:
        ids = process_input_file(args.input_file, args.column)
        batch_size = validate_batch_size(args.batch_size)
    except Exception as e:
        print(f"Error processing input file: {e}")
        sys.exit(1)

    try:
        normalized_ids = normalize_ids(ids)
        if args.id_type in ('accession', 'history'):
            valid_accessions, invalid_accessions = split_valid_invalid_accessions(normalized_ids)
            invalid_accessions_csv = write_invalid_accessions_csv(invalid_accessions)
            if not valid_accessions:
                raise ValueError("Error: no valid accession IDs were found in the input")
            formatted_query = build_accession_query_output(
                valid_accessions,
                batch_size,
                args.separate_queries,
                args.id_type,
            )
        else:
            formatted_query = build_query_output(
                normalized_ids,
                args.id_type,
                batch_size,
                args.separate_queries,
            )
    except Exception as e:
        print(f"Error generating SQL query: {e}")
        if invalid_accessions_csv is not None:
            print(f"Wrote invalid accession IDs to {invalid_accessions_csv}")
        sys.exit(1)

    # connect to database and run query
    try:
        conn = pyodbc.connect(prepared_connection_string())
        results = pd.read_sql(formatted_query, conn)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

    output_file = args.output_file
    try:
        clean_results(results)
        compile_report_column(results)
        results.to_csv(output_file, index=False)
        print(f"CoPath data exported to {output_file}")
    except Exception as e:
        print(f"Error parsing results: {e}")
