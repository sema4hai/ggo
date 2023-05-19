# ggo
Open source script for the Ground Glass Opacity (GGO) publication.

## main script to generate the data and plots
code/draft_v2.1.ipynb

## Database views generated to summarize the notes_ggo table

### v_persistent_cohort: the patients with persistent GGO as defined below.
* Patients with multiple GGO reports, except for those with the last report as resolved.
* Or patients with only one GGO report, but reported with a worsened status (number, size or solidity).

### v_notes_ggo_summary: a report level summary of GGO.
* D_patient_id: patient identifier
* Note_date: the deidentified note date
* Term_types: the types of GGO_terms (pure, mixed)
* Max_size: the maximal size (in mm) of the GGOs in the report
* Locations: the body locations of the reported GGOs
* potential_causes: the potential cause category
* Status_changes: the GGO status change categorized (increase, decrease, stable, resolved, …)
* Shape_margins: the shape of GGO margins (round, irregular, …)
* Solidity_changes: the solidity change category if available
* Numbers: the number of GGO as (single, multiple)
* is_persistent: 'YES' if the patient has persistent GGO
