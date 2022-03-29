# Key Stage 2 test results and GCSE exam results by school governance

This work was written to rebut the Governmant's claims in their document ["The case for a fully trust-led system"](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1063615/The_case_for_a_fully_trust-led_system__web_.pdf) which is a collection of half truths. This analysis looks at the test and exam performance tables presented on page 18 of their document.

Source data from the Department for Education is stored in the folder [data folder](data). It was downloaded from [Find and compare schools in England](https://www.compare-school-performance.service.gov.uk/download-data).

[edubase.py](edubase.py) downloads the latest version of school information from [Get Information About Schools](https://get-information-schools.service.gov.uk/Downloads).

[governance_exam_performance.py](governance_exam_performance.py) collates Key Stage 2 and GCSE exam results by school governance - local authority maintained schools, single-academy trusts and multi-acadmey trusts. The script produces two spreadsheets that are saved in the [output folder](output).

The report is [The Governments flawed case for a fully trust-led system.pdf](The Governments flawed case for a fully trust-led system.pdf).
