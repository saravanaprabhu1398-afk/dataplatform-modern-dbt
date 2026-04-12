from dataplatform.core.pipeline_generator import generate_pipeline_yaml_from_text


def test_generate_pipeline_from_multi_step_text_creates_ordered_tasks():
    result = generate_pipeline_yaml_from_text(
        (
            "Build a daily sales pipeline. First extract orders from postgres, "
            "then clean the data with Python, then run dbt models, and finally "
            "load curated results to Snowflake."
        )
    )

    parsed = result["parsed_config"]
    tasks = parsed["tasks"]

    assert parsed["pipeline_name"] == "daily_sales"
    assert [task["plugin"] for task in tasks] == ["postgres", "python", "dbt", "snowflake"]
    assert [task["name"] for task in tasks] == [
        "extract_orders_postgres",
        "clean_python",
        "run_dbt_models",
        "load_curated_snowflake",
    ]
    assert tasks[1]["depends_on"] == [tasks[0]["name"]]
    assert tasks[2]["depends_on"] == [tasks[1]["name"]]
    assert tasks[3]["depends_on"] == [tasks[2]["name"]]
