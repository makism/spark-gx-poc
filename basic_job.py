import great_expectations as gx


if __name__ == "__main__":
    context = gx.get_context()
    spark = gx.core.util.get_or_create_spark_application()

    df = spark.read.csv("data/titanic.csv", header=True)
    ds = context.sources.add_spark("titanic")

    data_asset = ds.add_dataframe_asset(name="titanic_df")

    batch_req = data_asset.build_batch_request(dataframe=df)

    val = context.get_validator(
        batch_request=batch_req, create_expectation_suite_with_name="test_batch_suite"
    )

    val.expect_column_values_to_be_in_set("Sex", ["male", "female"])
    val.expect_table_row_count_to_be_between(min_value=0, max_value=10)
    val.expect_column_values_to_be_in_set("PClass", ["1st", "2nd", "3rd"])

    val.save_expectation_suite(
        "expecations.json",
        discard_failed_expectations=False,
    )
