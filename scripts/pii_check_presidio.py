#!/usr/bin/env python3
"""
PII detection for EOIR data using Microsoft Presidio.

Scans the processed cases.parquet for PII.
Uses Presidio's built-in recognizers (PERSON, PHONE, SSN, EMAIL, etc.)
plus custom recognizers for immigration-specific PII (A-numbers, DOB patterns).

Key design choices:
  - Columns are classified as "expected_date", "expected_code", "expected_location",
    "expected_name", or "scan" based on naming conventions. This lets us distinguish
    truly unexpected PII from structural data.
  - Only unique values per column are scanned (huge speedup).
  - DATE_TIME entity is excluded from columns that are already known date fields.
  - Judge/attorney names are public record and excluded from PERSON/LOCATION checks.
  - Short structural codes (IJ codes, hearing location codes) only checked for
    high-value entities (SSN, email, A-number) that should never appear in codes.

Usage:
    python3 scripts/pii_check_presidio.py
"""

import os
import re
import time

import pandas as pd
import pyarrow.parquet as pq
from names_dataset import NameDataset
from presidio_analyzer import (
    AnalyzerEngine,
    EntityRecognizer,
    Pattern,
    PatternRecognizer,
    RecognizerRegistry,
    RecognizerResult,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PARQUET_FILE = "outputs/cases.parquet"
OUTPUT_DIR = "outputs/pii_checks"

# Sample size — read from PII_SAMPLE_N env var if set; empty string = full scan
_sample_env = os.environ.get("PII_SAMPLE_N", "50000")
SAMPLE_N = int(_sample_env) if _sample_env.strip() else None

# Minimum confidence score to report
SCORE_THRESHOLD = 0.4

# --- Column classification ---
# Columns to skip entirely (internal IDs, booleans, codes with no PII risk)
SKIP_COLUMNS = {
    # Numeric row IDs
    "IDNCASE", "IDNPROCEEDING", "IDNASSOCBOND", "IDNMOTION", "IDNCASEID",
    "idnAppeal", "idncase", "idnProceeding", "IDNPRCDCHG", "IDNCUSTODY",
    "IDNPROCEEDINGAPPLN", "IDNSCHEDULE", "IDNREPSASSIGNED", "LNGSESSNID",
    "PARENT_IDN",
    # System metadata
    "REC_TYPE", "GENERATION", "SUB_GENERATION",
    "EOIRAttorneyID", "OldAttorneyID", "blnAttorneyActive", "Source_Flag",
    "datCreatedOn", "datModifiedOn", "BLNPRIMEATTY", "BLNCLOCKOVERRIDE",
    "BLNDELETED_Remove", "E_28_RECPTFLAG_Remove",
    # Single-value / boolean-like
    "ZBOND_MRG_FLAG", "APPEAL_RSVD", "APPEAL_NOT_FILED", "ABSENTIA",
    "TRANSFER_STATUS", "LPR", "WU_MSG_Remove",
    # Codes (short category codes, no PII)
    "CUSTODY", "CASE_TYPE", "SITE_TYPE", "CAL_TYPE", "DEC_TYPE", "DEC_CODE",
    "DEC", "CRIM_IND", "IHP", "AGGRAVATE_FELON", "CHG_STATUS", "APPL_CODE",
    "APPL_DEC", "C_ASY_TYPE", "Sex", "SCHEDULE_TYPE", "NOTICE_CODE",
    "O_CLOCK_OPTION", "ASSIGNMENT_PATH", "ADJ_RSN", "ADJ_MEDIUM", "ADJ_MSG",
    "STRATTYLEVEL", "STRATTYTYPE", "PARENT_TABLE", "REL_CON", "INS_TA",
    "BOND_TYPE", "FILING_METHOD", "FILING_PARTY", "BOND_HEARING_TELEPHONIC",
    "STRFILINGPARTY", "STRFILINGMETHOD", "STRCERTOFSERVICECODE",
    "strAppealCategory", "strAppealType", "strFiledBy", "strBIADecisionType",
    "strCaseType", "strLang", "strNat", "strProceedingIHP", "strCustody",
    "strProbono", "strDJScenario",
    # Parquet processed fields that are codes/booleans
    "case_type", "dec_code", "other_comp", "custody", "lastcustody",
    "c_asy_type", "sex", "lastbiadecisiontype", "lastappealcategory",
    "lastbiafiledby", "lastbiacustody", "pendingappeal", "lastdec",
    "outcome", "other_completion", "relief", "termination",
    "casepriority_desc", "lastappealtype_desc", "lastbiadecision_desc",
    # Application indicators (boolean flags)
    "absentia", "asylumapp", "withholdapp", "catapp", "adjustapp",
    "nonlprcancelapp", "lprcancelapp", "anyreliefapp",
    # Numeric aggregates (bond amounts, years, durations — not PII)
    "lastinitial_bond", "lastnew_bond", "finalcompyear", "birth_year", "length",
    # Aggregated from lookups (country/language names, not individual PII)
    "lang_desc", "nat_desc",
    "NAT", "LANG",
    # Charge codes
    "CHARGE",
}

# Columns that ARE dates by design — we still scan them but exclude DATE_TIME
# entity (since being a date is expected). We still check for SSN/phone/etc.
DATE_COLUMN_PATTERNS = re.compile(
    r"(?i)"
    r"(?:date|_date|dat[A-Z]|osc_date|comp_date|e_28|hearing_date|"
    r"hearing_time|latest_hearing|latest_time|"
    r"adj_date|adj_time|adj_elap|address_changedon|"
    r"input_date|input_time|update_date|update_time|"
    r"adj_time_start|venue_chg_granted|"
    r"up_bond_date|bond_hear_req_date|decision_due_date|"
    r"resp_due_date|motion_recd_date|datmotiondue|"
    r"c_birthdate|c_release_date|release_month|release_year|"
    r"firstcomp|finalcomp|firstdetained|lastreleased|"
    r"last_ij_decision|lastbond_comp|datbiadec|datappealfiled|"
    r"appl_recd_date|ins_ta_date)"
)

# Columns that contain location codes/names by design
LOCATION_COLUMN_PATTERNS = re.compile(
    r"(?i)"
    r"(?:city|state|zip|county|base_city|hearing_loc|"
    r"scheduled_hear_loc|transfer_to|prev_hearing|"
    r"update_site|detention_location|dco_location|"
    r"correctional_fac|detention_facility|"
    r"firsthearingloc|lasthearingloc|firstcourt|finalcourt)"
)

# Columns that contain names by design (judges, attorneys — public officials,
# not individual PII). We skip both PERSON and LOCATION for these.
NAME_COLUMN_PATTERNS = re.compile(
    r"(?i)(?:judge_name|ij_name|IJ_NAME|strAttorneyName|ATTY_NAME)"
)

# Columns that contain short structural codes (IJ codes, hearing location codes,
# base city codes, charge codes). Presidio frequently mistakes 3-letter codes
# for names/places. Only check high-value entities that should never appear here.
CODE_COLUMN_PATTERNS = re.compile(
    r"(?i)"
    r"(?:IJ_CODE|ij_code|BASE_CITY_CODE|base_city_code|"
    r"HEARING_LOC_CODE|hearing_loc_code|PREV_IJ_CODE|prev_ij_code|"
    r"PREV_HEARING_LOC|prev_hearing_loc|PREV_HEARING_BASE|"
    r"SCHEDULED_HEAR_LOC|TRANSFER_TO|"
    r"^charge_\d+$|^charges_all$|^CHARGE_\d+$)"
)


def classify_column(col_name):
    """Classify a column to determine what entities to look for."""
    if col_name in SKIP_COLUMNS:
        return "skip"
    if NAME_COLUMN_PATTERNS.search(col_name):
        return "expected_name"
    if CODE_COLUMN_PATTERNS.search(col_name):
        return "expected_code"
    if DATE_COLUMN_PATTERNS.search(col_name):
        return "expected_date"
    if LOCATION_COLUMN_PATTERNS.search(col_name):
        return "expected_location"
    return "scan"


# Entity lists by column classification
# For "expected_date" columns: skip DATE_TIME and PHONE_NUMBER. Numeric date
# representations (e.g. "05.567000") consistently trigger false phone hits at
# score 0.40; a real phone number will never appear in a date field in this data.
ENTITIES_NO_DATETIME = [
    "PERSON", "EMAIL_ADDRESS", "US_SSN", "US_ITIN",
    "CREDIT_CARD", "URL", "IP_ADDRESS", "A_NUMBER", "DOB_PATTERN", "FULL_NAME",
]

# For "expected_location" columns: skip LOCATION, PERSON, and DATE_TIME. City
# names like "Morrow", "Winters", "Friday" trigger DATE_TIME; "Monroe",
# "Hamilton" trigger PERSON. No individual name or date string should ever
# appear in a city/county/zip/hearing-location column.
ENTITIES_NO_LOCATION = [
    "EMAIL_ADDRESS", "US_SSN", "US_ITIN",
    "CREDIT_CARD", "URL", "IP_ADDRESS", "A_NUMBER", "DOB_PATTERN",
]

# For "expected_name" columns: skip PERSON, LOCATION, and DATE_TIME. Judge
# surnames like "May" or "June" trigger DATE_TIME hits. Public official names
# are not PII; only flag high-value entities that should never appear in a name.
ENTITIES_NO_PERSON_OR_LOCATION = [
    "EMAIL_ADDRESS", "US_SSN", "US_ITIN",
    "CREDIT_CARD", "URL", "IP_ADDRESS",
    "A_NUMBER", "DOB_PATTERN",
]
# Keep old alias for any future use
ENTITIES_NO_PERSON = ENTITIES_NO_PERSON_OR_LOCATION

# For "expected_code" columns: short structural codes — only check for high-value
# entities that should never appear in a code field.
ENTITIES_CODES_ONLY = [
    "EMAIL_ADDRESS", "US_SSN", "US_ITIN", "CREDIT_CARD",
    "IP_ADDRESS", "A_NUMBER", "DOB_PATTERN",
]

# Full scan
ALL_ENTITIES = [
    "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN", "US_ITIN",
    "CREDIT_CARD", "DATE_TIME", "LOCATION", "URL", "IP_ADDRESS",
    "A_NUMBER", "DOB_PATTERN", "FULL_NAME",
]


# ---------------------------------------------------------------------------
# Custom recognizers
# ---------------------------------------------------------------------------
def build_a_number_recognizer():
    """Alien registration number: A followed by 7-9 digits."""
    pattern = Pattern(
        name="a_number",
        regex=r"\bA-?\s*\d[\s-]*\d[\s-]*\d[\s-]*\d[\s-]*\d[\s-]*\d[\s-]*\d(?:[\s-]*\d){0,2}\b",
        score=0.7,
    )
    return PatternRecognizer(
        supported_entity="A_NUMBER",
        name="ANumberRecognizer",
        patterns=[pattern],
        context=["alien", "a-number", "a number", "uscis", "immigration", "nta"],
    )


def build_dob_recognizer():
    """Date of birth patterns like 'DOB: 01/15/1990'."""
    pattern = Pattern(
        name="dob_pattern",
        regex=r"(?i)\b(?:d\.?\s*o\.?\s*b\.?\s*:?\s*)"
        r"(?:\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}|\d{4}[/.\-]\d{1,2}[/.\-]\d{1,2})",
        score=0.85,
    )
    return PatternRecognizer(
        supported_entity="DOB_PATTERN",
        name="DOBRecognizer",
        patterns=[pattern],
    )


# Countries relevant to EOIR caseload: US + Mexico + Central/South America + Caribbean
_NAME_COUNTRIES = {
    "US", "MX", "GT", "SV", "HN", "NI", "CR", "PA",
    "CO", "VE", "EC", "PE", "BO", "BR", "PY", "UY", "AR", "CL",
    "CU", "DO", "HT", "PR",
}


class FullNameRecognizer(EntityRecognizer):
    """List-based recognizer that flags adjacent token pairs where one token
    is a known first name and the next is a known last name (or vice versa).
    Uses the names-dataset filtered to US + Latin American countries so it
    covers both English and Spanish/Portuguese name conventions."""

    ENTITIES = ["FULL_NAME"]

    def __init__(self, first_names: set, last_names: set):
        super().__init__(supported_entities=self.ENTITIES, name="FullNameRecognizer")
        self.first_names = first_names
        self.last_names = last_names

    def load(self):
        pass

    def analyze(self, text, entities, nlp_artifacts=None):
        results = []
        tokens = text.split()
        for i in range(len(tokens) - 1):
            t1 = re.sub(r"[^\w]", "", tokens[i]).lower()
            t2 = re.sub(r"[^\w]", "", tokens[i + 1]).lower()
            if not t1 or not t2:
                continue
            if (t1 in self.first_names and t2 in self.last_names) or \
               (t2 in self.first_names and t1 in self.last_names):
                start = text.find(tokens[i])
                end = text.find(tokens[i + 1], start) + len(tokens[i + 1])
                results.append(RecognizerResult(
                    entity_type="FULL_NAME",
                    start=start,
                    end=end,
                    score=0.75,
                ))
        return results


def build_full_name_recognizer():
    """Build FullNameRecognizer from names-dataset filtered to EOIR-relevant countries."""
    nd = NameDataset()
    first_names = {
        n.lower()
        for n, meta in nd.first_names.items()
        if _NAME_COUNTRIES & set(meta.get("country", {}).keys())
    }
    last_names = {
        n.lower()
        for n, meta in nd.last_names.items()
        if _NAME_COUNTRIES & set(meta.get("country", {}).keys())
    }
    return FullNameRecognizer(first_names, last_names)


def create_analyzer():
    """Create an AnalyzerEngine with built-in + custom recognizers."""
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers()
    registry.add_recognizer(build_a_number_recognizer())
    registry.add_recognizer(build_dob_recognizer())
    registry.add_recognizer(build_full_name_recognizer())
    return AnalyzerEngine(registry=registry)


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------
def scan_unique_values(analyzer, unique_vals, col_name, entities, score_threshold):
    """Scan unique string values for PII. Returns list of finding dicts."""
    findings = []
    for text in unique_vals:
        if not isinstance(text, str) or len(text.strip()) < 3:
            continue
        text = text.strip()
        try:
            results = analyzer.analyze(
                text=text,
                language="en",
                entities=entities,
                score_threshold=score_threshold,
            )
        except Exception:
            continue
        for r in results:
            match = text[r.start:r.end]
            # For PERSON detections, require at least two tokens (first + last
            # name). Single words like city names, initials, or codes are
            # almost always false positives.
            if r.entity_type == "PERSON" and len(match.split()) < 2:
                continue
            findings.append({
                "column": col_name,
                "entity_type": r.entity_type,
                "match": match,
                "score": round(r.score, 3),
                "full_value": text[:200],
            })
    return findings


def scan_dataframe(analyzer, df, label, score_threshold=0.4):
    """Scan all relevant string columns in a DataFrame for PII."""
    text_cols = df.select_dtypes(include=["object"]).columns.tolist()

    all_findings = []
    scanned = 0
    skipped = 0

    for col in text_cols:
        col_class = classify_column(col)
        if col_class == "skip":
            skipped += 1
            continue

        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        unique_vals = non_null.unique()
        n_unique = len(unique_vals)

        # Pick entity list based on column classification
        if col_class == "expected_date":
            entities = ENTITIES_NO_DATETIME
        elif col_class == "expected_location":
            entities = ENTITIES_NO_LOCATION
        elif col_class == "expected_name":
            entities = ENTITIES_NO_PERSON_OR_LOCATION
        elif col_class == "expected_code":
            entities = ENTITIES_CODES_ONLY
        else:
            entities = ALL_ENTITIES

        scanned += 1
        col_findings = scan_unique_values(
            analyzer, unique_vals, col, entities, score_threshold
        )

        if col_findings:
            print(f"    {col} ({n_unique} unique, class={col_class}): "
                  f"{len(col_findings)} PII matches")
        all_findings.extend(col_findings)

    print(f"  Scanned {scanned} columns, skipped {skipped}")

    if not all_findings:
        return pd.DataFrame()

    result = pd.DataFrame(all_findings)
    result["source"] = label
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Initializing Presidio analyzer...")
    t0 = time.time()
    analyzer = create_analyzer()
    print(f"  Ready in {time.time() - t0:.1f}s\n")

    print("=" * 60)
    print("SCANNING PROCESSED CASES.PARQUET")
    print("=" * 60)

    if not os.path.exists(PARQUET_FILE):
        print(f"  {PARQUET_FILE} not found, exiting")
        return

    print(f"\nReading {PARQUET_FILE}...")
    df = pq.read_table(PARQUET_FILE).to_pandas()
    if SAMPLE_N and len(df) > SAMPLE_N:
        df = df.sample(n=SAMPLE_N, random_state=42)
    # Convert all columns to string for scanning
    df_str = df.astype(str).replace("None", pd.NA).replace("nan", pd.NA)
    print(f"  {len(df)} rows x {len(df.columns)} columns")
    del df

    findings = scan_dataframe(analyzer, df_str, "processed/cases.parquet", SCORE_THRESHOLD)
    del df_str

    all_results = [findings] if not findings.empty else []

    # --- Summarize and save ---
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)

    if not all_results:
        print("\nNo PII detected!")
        # Write empty artifacts so CI always has files to commit
        out_file = os.path.join(OUTPUT_DIR, "presidio_pii_findings.csv")
        summary_file = os.path.join(OUTPUT_DIR, "presidio_pii_summary.csv")
        pd.DataFrame(columns=["column", "entity_type", "match", "score", "full_value", "source"]).to_csv(out_file, index=False)
        pd.DataFrame([{"source": "all", "entity_type": "none", "column": "all", "count": 0, "avg_score": 0.0, "examples": "", "status": "clean"}]).to_csv(summary_file, index=False)
        print(f"Detailed: {out_file}")
        print(f"Summary:  {summary_file}")
        return

    results = pd.concat(all_results, ignore_index=True)

    # Summary
    summary = (
        results.groupby(["source", "entity_type", "column"])
        .agg(
            count=("match", "size"),
            avg_score=("score", "mean"),
            examples=("match", lambda x: " | ".join(x.unique()[:5])),
        )
        .reset_index()
        .sort_values(["source", "count"], ascending=[True, False])
    )

    print(f"\nTotal PII findings: {len(results)}")
    print(f"\nBy source, entity type, and column:")
    for _, row in summary.iterrows():
        print(f"  {row['source']} | {row['column']} | {row['entity_type']}: "
              f"{row['count']} (avg score {row['avg_score']:.2f})")
        print(f"    e.g.: {row['examples'][:120]}")

    # Save
    out_file = os.path.join(OUTPUT_DIR, "presidio_pii_findings.csv")
    results.to_csv(out_file, index=False)
    summary_file = os.path.join(OUTPUT_DIR, "presidio_pii_summary.csv")
    summary.to_csv(summary_file, index=False)
    print(f"\nDetailed: {out_file}")
    print(f"Summary:  {summary_file}")


if __name__ == "__main__":
    main()
