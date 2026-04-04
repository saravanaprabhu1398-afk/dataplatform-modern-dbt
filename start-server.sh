#!/bin/bash

# Data Platform Dashboard Server Script
# Starts the API server with proper error checking

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Data Platform Dashboard Server                              ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"

# Check if we're in the right directory
if [ ! -f "sample_pipeline.yaml" ]; then
    if [ ! -f "$SCRIPT_DIR/sample_pipeline.yaml" ]; then
        echo -e "${RED}✗ Error: Not in data-platform-modern-dbt directory${NC}"
        echo -e "${YELLOW}Please run this script from the workspace root:${NC}"
        echo -e "${YELLOW}  cd /Users/prabhusaravanan/Desktop/GitHub/data-platform-modern-dbt${NC}"
        exit 1
    fi
    cd "$SCRIPT_DIR"
fi

echo -e "${GREEN}✓${NC} Working directory: $PWD"

# Check Python is available
echo -en "${YELLOW}Checking Python...${NC} "
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1)
    echo -e "${GREEN}✓${NC} Found: $PYTHON_VERSION"
else
    echo -e "${RED}✗ Python3 not found${NC}"
    exit 1
fi

# Check if dataplatform is installed
echo -en "${YELLOW}Checking dataplatform package...${NC} "
if python3 -c "import dataplatform" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Package installed"
else
    echo -e "${RED}✗ Package not installed${NC}"
    echo -e "${YELLOW}Installing dataplatform...${NC}"
    python3 -m pip install -e . || {
        echo -e "${RED}✗ Failed to install dataplatform${NC}"
        exit 1
    }
    echo -e "${GREEN}✓${NC} Installation complete"
fi

# Check required dependencies
echo -en "${YELLOW}Checking dependencies...${NC} "
if python3 -c "import fastapi, uvicorn, duckdb" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} All dependencies available"
else
    echo -e "${RED}✗ Missing dependencies${NC}"
    exit 1
fi

# Check if port is available
PORT=${1:-8000}
echo -en "${YELLOW}Checking if port $PORT is available...${NC} "
if ! python3 -c "import socket; s = socket.socket(); s.bind(('', $PORT)); s.close()" 2>/dev/null; then
    echo -e "${RED}✗ Port $PORT is in use${NC}"
    echo -e "${YELLOW}Try a different port: $0 8001${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Port is available"

# Start the server
echo ""
echo -e "${GREEN}✓ Starting API server...${NC}"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "Dashboard URL: ${GREEN}http://localhost:$PORT${NC}"
echo -e "API Docs:      ${GREEN}http://localhost:$PORT/docs${NC}"
echo -e "Health Check:  ${GREEN}curl http://localhost:$PORT/health${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"
echo ""

# Start server
python3 -m dataplatform.cli.main serve --host 0.0.0.0 --port $PORT || {
    echo ""
    echo -e "${RED}✗ Server failed to start${NC}"
    echo -e "${YELLOW}For troubleshooting, run: python3 diagnose_api.py${NC}"
    exit 1
}