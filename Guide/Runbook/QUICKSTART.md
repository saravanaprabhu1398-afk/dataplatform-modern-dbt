# Quick Start Guide

This guide will help you get the dashboard running in 3 easy steps.

## Step 1: Install Dependencies (First Time Only)

```bash
python3 -m pip install -e .
```

This installs all required packages including FastAPI, DuckDB, Snowflake connector, and more.

---

## Step 2: Start the API Server

Open a terminal and run:

```bash
python3 -m dataplatform.cli.main serve
```

You should see output like:
```
Starting API server on 0.0.0.0:8000
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

✅ **Keep this terminal open** - the server needs to keep running.

---

## Step 3: Open the Dashboard

Now open your browser and go to:

```
http://localhost:8000
```

You should see:
- ✓ Green "Connected" indicator in the header
- ✓ List of available pipelines
- ✓ Buttons to run and view DAG for each pipeline

(If you don't see pipelines, make sure `sample_pipeline.yaml` exists in the workspace root)

---

## Testing the Server (Optional)

To verify the server is working correctly, open a **new terminal** and run:

```bash
python3 test_api.py
```

This will test all API endpoints and show you the results.

Or test manually with `curl`:

```bash
# Check server is running
curl http://localhost:8000/health

# List available pipelines
curl http://localhost:8000/pipelines

# Get server info
curl http://localhost:8000/info
```

---

## Troubleshooting

### "Connection refused" or "Failed to connect"
- Make sure the API server is running (Step 2)
- Check that port 8000 is not in use by another application

### "No pipelines found"
- Make sure `sample_pipeline.yaml` exists in the workspace root
- Run `python3 diagnose_api.py` to check workspace configuration

### "Module not found" errors
- Run Step 1 again: `python3 -m pip install -e .`
- Make sure you're using Python 3.8 or later

### Server crashes on startup
- Run: `python3 diagnose_api.py` to check configuration
- Check `logs/pipeline.log` for detailed error messages
- Read: `TROUBLESHOOTING.md` for detailed diagnostics

---

## What's Next?

1. **Configure Snowflake** (if needed):
   - Edit `sample_pipeline.yaml`
   - Add your Snowflake credentials to the `load_to_snowflake` task

2. **Create your own pipeline**:
   - Copy `sample_pipeline.yaml` to a new file
   - Edit the tasks and configuration
   - Refresh the dashboard to see it appear

3. **Run pipelines**:
   - Click "Run" button next to any pipeline
   - Watch the status update in real-time
   - Check logs in `logs/pipeline.log`

4. **View DAG**:
   - Click "DAG" button to see pipeline dependencies
   - Understand execution order and task flow

---

## Quick Command Reference

```bash
# Install dependencies
python3 -m pip install -e .

# Start the dashboard
python3 -m dataplatform.cli.main serve

# Test the API
python3 test_api.py

# Run diagnostics
python3 diagnose_api.py

# Run a pipeline from CLI
python3 -m dataplatform.cli.main run sample_pipeline.yaml

# View logs
tail -f logs/pipeline.log

# List available commands
python3 -m dataplatform.cli.main --help
```

---

## Need Help?

1. Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed diagnostics
2. Review [FIX_SUMMARY.md](FIX_SUMMARY.md) for previous fixes
3. Run `python3 diagnose_api.py` to check configuration
4. Check `logs/pipeline.log` for error messages