# GXKent
A simple library that allows Great Expectations to run easily in python notebook and CLI environments

## Idea
Kent was the city featured in the Charles Dickens classic, and is therefore the sensible name for a container of expectations
The central issue that Kent resolves is to ensure that pandas dataframes are available and populated with data
in both of our data contexts: CLI and Notebooks

## Usage
Kent = GXKent()
Kent.is_print_on_success = False

  sql_text =
SELECT count(DISTINCT a.npi) AS new_npi_cnt
FROM default_npi_setting_count.{table_name} a
WHERE a.npi NOT IN (
    SELECT DISTINCT b.npi
    FROM default_npi_setting_count.persetting_2021_12 b
    );

gxDF = Kent.gx_df_from_sql(sql_text)

Kent.capture_expectation(
    expectation_name='Between year comparision {this_year} {that_year}',
    expectation_result=gxDF.expect_column_max_to_be_between('new_npi_cnt',112671,253511)
)

Kent.capture_expectation(
    expectation_name='Between year comparision {this_year} {that_year}',
    expectation_result=gxDF.expect_column_min_to_be_between('new_npi_cnt',11671,23511)
)

Kent.capture_expectation(
    expectation_name='Between year comparision {this_year} {that_year}',
    expectation_result=gxDF.expect_column_avg_to_be_between('new_npi_cnt',50000,60000)
)


## Authors
Fred Trotter and Jose Cortina
