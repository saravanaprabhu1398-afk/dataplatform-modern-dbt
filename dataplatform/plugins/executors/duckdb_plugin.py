import duckdb
from dataplatform.plugins.base import ExecutorPlugin


class DuckdbExecutor(ExecutorPlugin):
    """
    Generic DuckDB executor that is fully configuration-driven.
    
    No hardcoded column names or task-specific logic.
    All behavior is determined by the 'operation' and 'config' fields in the pipeline YAML.
    
    Supported operations:
    - load: Load data from file and show summary
    - query: Execute arbitrary SQL
    - aggregate: Group and aggregate with flexible metrics
    - validate: Run data quality checks
    - transform: Apply transformations with derived columns
    """

    def execute(self, config: dict) -> bool:
        """
        Execute DuckDB operation based on configuration.
        
        config = {
            "file_path": "data/input.csv",
            "operation": "aggregate" | "query" | "validate" | "load" | "transform",
            ... operation-specific fields ...
        }
        """
        file_path = config.get("file_path")
        if not file_path:
            print("❌ Error: No file_path specified for DuckDB executor")
            return False

        try:
            conn = duckdb.connect(':memory:')
            
            # Load the data file
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{file_path}')")
            
            row_count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
            print(f"\n✓ Loaded {row_count} rows from {file_path}")

            # Get operation type from config
            operation = config.get("operation", "load")
            
            # Route to appropriate operation handler
            if operation == "query":
                return self._execute_query(conn, config)
            elif operation == "aggregate":
                return self._execute_aggregate(conn, config)
            elif operation == "validate":
                return self._execute_validate(conn, config)
            elif operation == "transform":
                return self._execute_transform(conn, config)
            else:
                # Default: just show data summary
                return self._execute_load(conn, config)

        except Exception as e:
            print(f"❌ DuckDB error: {e}")
            return False, None
        finally:
            conn.close()

    def _execute_load(self, conn, config: dict) -> tuple[bool, None]:
        """
        Default operation: Load and display data summary.
        
        Config:
        {
            "show_columns": true,
            "show_sample": true
        }
        """
        show_columns = config.get("show_columns", True)
        show_sample = config.get("show_sample", True)

        print("\n📊 LOAD: Data Summary")
        print("-" * 60)

        if show_columns:
            cols = conn.execute("PRAGMA table_info(data)").fetchall()
            print("Columns:")
            for col_name, col_type, *_ in cols:
                print(f"  - {col_name}: {col_type}")

        count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        print(f"\n✓ Total records: {count}")
        
        if show_sample:
            print("\nSample rows:")
            sample = conn.execute("SELECT * FROM data LIMIT 3").fetchall()
            for row in sample:
                print(f"  {row}")

        return True, None

    def _execute_query(self, conn, config: dict) -> tuple[bool, list]:
        """
        Execute arbitrary SQL query.
        
        Config format:
        {
            "sql": "SELECT * FROM data WHERE amount > 100",
            "return_data": true  # Optional: return query results for next task
        }
        """
        sql = config.get("sql")
        return_data = config.get("return_data", False)
        
        if not sql:
            print("❌ Error: 'sql' field required for query operation")
            return False, None

        print(f"\n🔍 QUERY: Custom SQL")
        print("-" * 60)

        try:
            result = conn.execute(sql).fetchall()
            print(f"✓ Query returned {len(result)} rows")
            
            # Show results
            if result:
                for row in result[:5]:
                    print(f"  {row}")
                if len(result) > 5:
                    print(f"  ... and {len(result) - 5} more rows")
            
            if return_data:
                return True, result
            else:
                return True, None
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return False, None

    def _execute_aggregate(self, conn, config: dict) -> bool:
        """
        Generic aggregation operation.
        
        Config format:
        {
            "operation": "aggregate",
            "group_by": ["category", "region"],      # Columns to group by
            "metrics": [
                {
                    "column": "amount",
                    "function": "sum",               # sum, avg, count, min, max
                    "alias": "total_amount"
                },
                {
                    "column": "id",
                    "function": "count",
                    "alias": "num_transactions"
                }
            ],
            "order_by": "total_amount desc",         # Optional
            "output_table": "results_table"          # Optional: create persistent table
        }
        """
        group_by = config.get("group_by", [])
        metrics = config.get("metrics", [])
        order_by = config.get("order_by")
        output_table = config.get("output_table")

        if not group_by or not metrics:
            print("❌ Error: 'group_by' and 'metrics' required for aggregate operation")
            return False

        print(f"\n📈 AGGREGATE: Dynamic Aggregation")
        print("-" * 60)

        # Build SELECT clause
        select_parts = []
        
        # Add GROUP BY columns
        for col in group_by:
            select_parts.append(col)
        
        # Add metrics
        for metric in metrics:
            column = metric.get("column")
            function = metric.get("function", "count")
            alias = metric.get("alias", f"{function}_{column}")
            
            select_parts.append(f"{function.upper()}({column}) as {alias}")
        
        select_clause = ", ".join(select_parts)
        group_clause = ", ".join(group_by)
        
        # Build query
        sql = f"SELECT {select_clause} FROM data GROUP BY {group_clause}"
        
        if order_by:
            sql += f" ORDER BY {order_by}"

        try:
            if output_table:
                # Create a table with the results
                create_sql = f"CREATE TABLE {output_table} AS {sql}"
                conn.execute(create_sql)
                result = conn.execute(f"SELECT * FROM {output_table}").fetchall()
                print(f"✓ Created table '{output_table}' with {len(result)} rows")
            else:
                result = conn.execute(sql).fetchall()
                print(f"✓ Aggregation returned {len(result)} groups")
            
            # Show results
            for row in result:
                print(f"  {row}")
            
            return True, None
        except Exception as e:
            print(f"❌ Aggregation failed: {e}")
            return False, None

    def _execute_validate(self, conn, config: dict) -> bool:
        """
        Data quality validation based on rules.
        
        Config format:
        {
            "operation": "validate",
            "checks": [
                {
                    "name": "no_nulls_in_email",
                    "sql": "SELECT COUNT(*) FROM data WHERE email IS NULL",
                    "expect": 0                      # Expected result
                },
                {
                    "name": "positive_amounts",
                    "sql": "SELECT COUNT(*) FROM data WHERE amount < 0",
                    "expect": 0
                },
                {
                    "name": "record_count",
                    "sql": "SELECT COUNT(*) FROM data",
                    "expect_gt": 0                   # Greater than
                }
            ]
        }
        """
        checks = config.get("checks", [])
        
        if not checks:
            print("⚠️  No checks defined for validation")
            return True

        print(f"\n✅ VALIDATE: Data Quality Checks")
        print("-" * 60)

        passed = 0
        failed = 0

        for check in checks:
            check_name = check.get("name", "unnamed_check")
            sql = check.get("sql")
            expect = check.get("expect")
            expect_gt = check.get("expect_gt")
            expect_lt = check.get("expect_lt")

            if not sql:
                print(f"⚠️  Skipping check '{check_name}': no sql provided")
                continue

            try:
                result = conn.execute(sql).fetchone()[0]
                
                # Evaluate check
                check_passed = True
                if expect is not None:
                    check_passed = result == expect
                elif expect_gt is not None:
                    check_passed = result > expect_gt
                elif expect_lt is not None:
                    check_passed = result < expect_lt

                if check_passed:
                    print(f"✓ {check_name}: {result}")
                    passed += 1
                else:
                    print(f"✗ {check_name}: {result} (check failed)")
                    failed += 1

            except Exception as e:
                print(f"✗ {check_name}: Error - {e}")
                failed += 1

        print(f"\nSummary: {passed} passed, {failed} failed")
        return failed == 0, None

    def _execute_transform(self, conn, config: dict) -> bool:
        """
        Apply transformations to create derived columns.
        
        Config format:
        {
            "operation": "transform",
            "columns": [
                {
                    "name": "amount_usd",
                    "sql": "amount * 1.2"               # Expression to compute
                },
                {
                    "name": "category_upper",
                    "sql": "UPPER(category)"
                },
                {
                    "name": "year",
                    "sql": "YEAR(date_column)"
                }
            ],
            "output_table": "transformed_data"         # Optional
        }
        """
        columns = config.get("columns", [])
        output_table = config.get("output_table", "transformed_data")

        if not columns:
            print("⚠️  No columns to transform")
            return True

        print(f"\n➕ TRANSFORM: Derived Columns")
        print("-" * 60)

        # Build SELECT with original + new columns
        select_parts = ["*"]
        
        for col_def in columns:
            col_name = col_def.get("name")
            col_sql = col_def.get("sql")
            
            if col_name and col_sql:
                select_parts.append(f"({col_sql}) as {col_name}")

        select_clause = ", ".join(select_parts)
        sql = f"CREATE TABLE {output_table} AS SELECT {select_clause} FROM data"

        try:
            conn.execute(sql)
            result_count = conn.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
            
            print(f"✓ Created '{output_table}' with {len(columns)} new columns")
            print(f"✓ Processed {result_count} rows")
            
            return True, None
        except Exception as e:
            print(f"❌ Transform failed: {e}")
            return False, None