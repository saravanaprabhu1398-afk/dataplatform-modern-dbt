#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, '/Users/prabhusaravanan/Desktop/GitHub/data-platform-modern-dbt')

def test_api_endpoints():
    """Test that API endpoints are working."""
    try:
        from dataplatform.core.api import app
        from pathlib import Path

        print("=" * 60)
        print("API ENDPOINT DIAGNOSIS")
        print("=" * 60)

        # Check if app is properly configured
        print("\n1. FastAPI App Configuration:")
        print(f"   ✓ FastAPI app loaded")
        
        # Check routes
        endpoints = []
        for route in app.routes:
            if hasattr(route, 'methods') and hasattr(route, 'path'):
                methods = list(route.methods)
                endpoints.append((route.path, methods))
        
        print(f"   ✓ {len(endpoints)} routes defined")
        
        # Check for required endpoints
        required_paths = ['/health', '/info', '/pipelines', '/run', '/status', '/dag']
        for required in required_paths:
            found = any(required in str(path) for path, _ in endpoints)
            status = "✓" if found else "✗"
            print(f"   {status} {required}")

        # Check static files
        print("\n2. Static Files Configuration:")
        static_dir = Path(__file__).parent / "dataplatform" / "static"
        if static_dir.exists():
            print(f"   ✓ Static directory exists: {static_dir}")
            index_file = static_dir / "index.html"
            if index_file.exists():
                print(f"   ✓ index.html found ({index_file.stat().st_size} bytes)")
            else:
                print(f"   ✗ index.html NOT found")
        else:
            print(f"   ✗ Static directory NOT found: {static_dir}")

        # Check workspace
        print("\n3. Workspace Configuration:")
        # The workspace root is the directory where diagnose_api.py is located
        workspace_root = Path(__file__).parent
        print(f"   Workspace root: {workspace_root}")
        
        yaml_files = list(workspace_root.glob("*.yaml"))
        print(f"   ✓ Found {len(yaml_files)} YAML files:")
        for yaml_file in yaml_files:
            print(f"      - {yaml_file.name}")

        # Check config loading
        print("\n4. Configuration Loading:")
        from dataplatform.core.config import load_config
        
        for yaml_file in yaml_files:
            try:
                config = load_config(str(yaml_file))
                task_count = len(config.tasks)
                print(f"   ✓ {yaml_file.name}: {task_count} tasks")
            except Exception as e:
                print(f"   ✗ {yaml_file.name}: {e}")

        print("\n" + "=" * 60)
        print("MANUAL API TESTING")
        print("=" * 60)
        print("\nTo test the API manually, run:")
        print("  python3 -m dataplatform.cli.main serve")
        print("\nThen in another terminal:")
        print("  curl http://localhost:8000/health")
        print("  curl http://localhost:8000/info")
        print("  curl http://localhost:8000/pipelines")
        print("\nOr open in browser:")
        print("  http://localhost:8000")

        return True

if __name__ == "__main__":
    success = test_api_endpoints()
    sys.exit(0 if success else 1)