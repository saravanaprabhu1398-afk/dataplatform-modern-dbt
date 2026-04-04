#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, '/Users/prabhusaravanan/Desktop/GitHub/data-platform-modern-dbt')

def test_pipeline_loading():
    """Test that the pipeline can be loaded and parsed."""
    try:
        from dataplatform.core.config import load_config
        from dataplatform.core.dag import DAGBuilder

        print("Testing pipeline loading...")

        config_path = 'sample_pipeline.yaml'
        config = load_config(config_path)
        print(f"✓ Loaded pipeline: {config.pipeline_name}")
        print(f"✓ Found {len(config.tasks)} tasks")

        # Check task names
        task_names = [task.name for task in config.tasks]
        print(f"✓ Tasks: {task_names}")

        # Check for required tasks
        required_tasks = ['load_employee_data', 'validate_employee_data', 'department_analytics',
                         'enrich_employee_data', 'generate_reports', 'load_to_snowflake']
        for task_name in required_tasks:
            if task_name not in task_names:
                print(f"✗ Missing task: {task_name}")
                return False
            else:
                print(f"✓ Found task: {task_name}")

        # Test DAG building
        dag_builder = DAGBuilder(config.tasks)
        dag = dag_builder.build()
        execution_order = dag_builder.get_execution_order()
        print(f"✓ DAG built successfully, execution order: {execution_order}")

        return True

    except Exception as e:
        print(f"✗ Pipeline loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_plugin_loading():
    """Test that plugins can be loaded."""
    try:
        from dataplatform.core.executor import TaskExecutor

        print("\nTesting plugin loading...")

        executor = TaskExecutor()

        # Test DuckDB plugin
        duckdb_plugin = executor.load_plugin('duckdb', 'executor')
        print("✓ DuckDB plugin loaded")

        # Test Snowflake plugin
        snowflake_plugin = executor.load_plugin('snowflake', 'executor')
        print("✓ Snowflake plugin loaded")

        return True

    except Exception as e:
        print(f"✗ Plugin loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PIPELINE VALIDATION TESTS")
    print("=" * 60)

    pipeline_ok = test_pipeline_loading()
    plugins_ok = test_plugin_loading()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if pipeline_ok and plugins_ok:
        print("✓ All validation tests passed!")
        print("\nNext steps:")
        print("1. Install dependencies: pip install -e .")
        print("2. Configure your Snowflake credentials in sample_pipeline.yaml")
        print("3. Run: dataplatform run sample_pipeline.yaml")
    else:
        print("✗ Some tests failed. Please fix the issues above.")

    sys.exit(0 if (pipeline_ok and plugins_ok) else 1)