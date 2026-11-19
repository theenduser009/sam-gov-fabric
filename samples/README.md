# SAM.gov Contract Award API header samples

- [Header-only CSV](sam-award-idv-headers.csv) contains the published column names in first-appearance order.
- [Field map](sam-award-idv-field-map.csv) pairs each published JSON field path with its CSV name.

The General Services Administration lists a common mapping for contract awards and indefinite-delivery vehicles (IDVs). These samples come from the [GSA mapping dated November 4, 2026](https://open.gsa.gov/api/contract-awards/v1/ContractAwardAPI_JSONvsCSV.pdf). The generator joins field names split across pages and removes identical duplicate rows. The header order follows the mapping document. Live export order and which fields appear on particular records depend on SAM.gov.

GSA has announced an upcoming CSV header change. The effective date is still listed as TBD in the [Contract Awards API release note](https://open.gsa.gov/api/contract-awards/#additional-help-references).

The header workflow downloads the public GSA mapping and compares the generated files with these samples. The verification uses the mapping document as its source.
