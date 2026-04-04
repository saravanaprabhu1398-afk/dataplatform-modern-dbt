# Fix: Failed to Load Pipelines

## Problem
When accessing the web dashboard at `http://localhost:8000`, the error message "Failed to load pipelines" appeared, preventing the dashboard from loading pipeline configurations.

## Root Causes
1. **Incorrect path resolution**: API was looking for YAML files in the current working directory (`.`) instead of the workspace root
2. **Poor error messages**: Frontend wasn't displaying the actual API error, making debugging difficult
3. **No connection status**: Dashboard didn't indicate whether the API was reachable

## Solutions Implemented

### 1. Fixed Pipeline Path Resolution
**File**: `dataplatform/core/api.py`

```python
# BEFORE: Used current directory
config_dir = Path(".")

# AFTER: Use workspace root
workspace_root = Path(__file__).parent.parent.parent
yaml_files = list(workspace_root.glob("*.yaml"))
```

**Impact**: API now correctly finds all YAML files in the workspace root, regardless of where the server is started from.

### 2. Improved Error Handling
**File**: `dataplatform/core/api.py`

- Added detailed logging to trace pipeline scanning
- Error messages now include actual error details instead of generic messages
- Return workspace root information to help users understand the search path

### 3. Enhanced Frontend Error Display
**File**: `dataplatform/static/index.html`

- Shows actual error messages from the API instead of generic text
- Displays workspace root being scanned
- Shows "Looking in: /path/to/workspace" message for clarity

### 4. Added Connection Status Indicator
**File**: `dataplatform/static/index.html`

- Connection status shows in header with live indicator
- Green dot = Connected, Red dot = Disconnected
- Indicator updates every time health check runs
- Automatic retry logic if API is not initially available

### 5. Added New API Endpoints
**File**: `dataplatform/core/api.py`

- `GET /health` - Simple health check endpoint
- `GET /info` - Returns workspace root and path information

### 6. Created Diagnostic Tools

#### `diagnose_api.py`
Run this to verify the API and workspace configuration:
```bash
python3 diagnose_api.py
```

Checks:
- ✓ FastAPI app configuration
- ✓ Static files setup
- ✓ Workspace paths
- ✓ YAML file availability
- ✓ Configuration loading

#### `start-server.sh` (New)
Easy server startup with error checking:
```bash
chmod +x start-server.sh
./start-server.sh
```

Checks:
- ✓ Working directory
- ✓ Python availability
- ✓ Package installation
- ✓ Dependencies
- ✓ Port availability

### 7. Created Troubleshooting Guide
**File**: `TROUBLESHOOTING.md`

Comprehensive guide covering:
- Quick diagnostics
- Common issues and solutions
- Expected behavior
- Debug checklist
- How to view logs

## How to Use the Fix

### 1. Ensure dependencies are installed:
```bash
python3 -m pip install -e .
```

### 2. Start the server:
```bash
# Option A: Use the shell script (recommended)
chmod +x start-server.sh
./start-server.sh

# Option B: Use Python module directly
python3 -m dataplatform.cli.main serve

# Option C: Use installed command
dataplatform serve
```

### 3. Open the dashboard:
```
http://localhost:8000
```

### 4. Verify connection:
- Look for green "Connected" indicator in header
- Pipeline list should show available configurations
- If issues persist, run `python3 diagnose_api.py`

## Verification Steps

### Quick Test
```bash
# Terminal 1: Start server
python3 -m dataplatform.cli.main serve

# Terminal 2: Test health endpoint
curl http://localhost:8000/health

# Terminal 3: Test pipelines endpoint
curl http://localhost:8000/pipelines
```

### Expected Output for /pipelines:
```json
{
  "pipelines": [
    {
      "name": "sample_pipeline.yaml",
      "display_name": "Employee Analytics Pipeline",
      "description": "Complete employee data processing with Snowflake integration",
      "file_path": "/Users/prabhusaravanan/Desktop/GitHub/data-platform-modern-dbt/sample_pipeline.yaml",
      "task_count": 7
    }
  ],
  "workspace_root": "/Users/prabhusaravanan/Desktop/GitHub/data-platform-modern-dbt"
}
```

## Files Modified

1. `dataplatform/core/api.py`
   - Fixed `/pipelines` endpoint path resolution
   - Added better error messages
   - Added `/health` endpoint
   - Added `/info` endpoint

2. `dataplatform/static/index.html`
   - Added connection status indicator
   - Improved error messages
   - Enhanced notification system
   - Added connection checking on startup

3. `README.md`
   - Added troubleshooting section
   - Updated quick start guide

## New Files Created

1. `diagnose_api.py` - API diagnostic tool
2. `TROUBLESHOOTING.md` - Comprehensive troubleshooting guide
3. `start-server.sh` - Easy server startup script

## Benefits

✅ **More Reliable**: Path resolution works from any directory
✅ **Better Debugging**: Clear error messages help identify issues
✅ **Faster Troubleshooting**: Diagnostic tools and guides included
✅ **User Friendly**: Connection indicator shows API status
✅ **Professional**: Error messages explain what to do next

## Testing

The fixes have been tested to:
- ✓ Correctly find YAML files in workspace root
- ✓ Display clear error messages when issues occur
- ✓ Show connection status in UI
- ✓ Handle missing dependencies gracefully
- ✓ Provide diagnostic information for troubleshooting