TEXT_TYPES = [
    ("$final", "final_diagnosis"),
    ("$adddx", "addendum_diagnosis"),
    ("$dxcom", "note"),
    ("$addcom", "addendum_comment"),
    ("$clindx", "clinical_history"),
    ("$clinsum", "clinical_summary"),
    ("$micro", "microscopic_description"),
    ("$gross", "gross_description"),
    ("$prelim-com", "preliminary_comment"),
    ("$aprelim-com", "anatomic_preliminary_comment"),
    ("$prelim-dx", "preliminary_diagnosis"),
    ("$aprelim-dx", "anatomic_preliminary_diagnosis"),
    ("$preopdx", "pre_operative_diagnosis"),
    ("$frodx1", "intraoperative_diagnosis"),
    ("$frodx2", "intraoperative_diagnosis_detail"),
    ("$postopdx", "post_operative_diagnosis"),
    ("qeq6", "results"),
    ("$procres", "results_comments"),
    ("$procint", "interpretation"),
    ("$cardcom", "card_case_comment"),
    ("$casedis", "case_discussion"),
    ("$confnot", "conference_note"),
    ("$hotseat", "hot_seat_diagnosis"),
    ("$n-final", "neuropathology_final_diagnosis"),
    ("$n-dxcom", "neuropathology_diagnosis_comment"),
    ("$nadd-dx", "neuropathology_addendum_diagnosis"),
    ("$nadd-com", "neuropathology_addendum_comment"),
    ("$n-micro", "neuropathology_microscopic_description"),
    ("$n-gross", "neuropathology_gross_description"),
    ("$n-othergross", "neuropathology_other_gross_description"),
    ("$n-othertxt", "neuropathology_other_text"),
    ("$nprelim-com", "neuropathology_preliminary_comment"),
    ("$nprelim-dx", "neuropathology_preliminary_diagnosis"),
    ("$othclin", "other_related_clinical_data"),
    ("$othdx", "other_diagnoses"),
    ("$othgross", "other_gross_description"),
    ("$phynot", "physician_notification"),
    ("$review", "cytology_review"),
    ("$rptcom", "report_comments"),
    ("$slideblk", "slide_block_description"),
    ("$specialreq", "special_requests"),
    ("$synop", "synoptic_worksheet"),
    ("qeq1", "abn"),
    ("qeq2", "ancillary_procedures"),
    ("qeq3", "operative_procedure"),
    ("qeq4", "postmortem_imaging_studies"),
    ("qeq5", "procedure_note"),
]


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def format_text_agg_columns(indent="        "):
    lines = []
    last_index = len(TEXT_TYPES) - 1
    for index, (texttype_id, column_name) in enumerate(TEXT_TYPES):
        comma = "," if index < last_index else ""
        lines.append(
            f"{indent}MAX(CASE WHEN t.texttype_id = {sql_literal(texttype_id)} "
            f"THEN CAST(t.text_data AS VARCHAR(MAX)) END) AS {column_name}{comma}"
        )
    return "\n".join(lines)


def format_texttype_id_list(indent="        "):
    return ",\n".join(f"{indent}{sql_literal(texttype_id)}" for texttype_id, _ in TEXT_TYPES)


def format_text_select_columns(table_alias, indent="    "):
    return ",\n".join(
        f"{indent}{table_alias}.{column_name}"
        for _, column_name in TEXT_TYPES
    )


def text_field_references(table_alias):
    return [f"{table_alias}.{column_name}" for _, column_name in TEXT_TYPES]


def compile_report_fields(row, field_names):
    """Combine report fields into labeled sections, retaining empty fields."""
    sections = []
    for field_name in field_names:
        value = row[field_name]
        if not isinstance(value, str):
            value = ""
        label = field_name.replace("_", " ").title()
        sections.append(f"{label}:\n{value}")
    return "\n\n".join(sections)
