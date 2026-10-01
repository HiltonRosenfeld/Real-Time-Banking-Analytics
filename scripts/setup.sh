#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🚀 Setting up environment..."


# use uv if found
if command -v uv >/dev/null 2>&1; then
    echo "Using uv..."
    uv venv --python 3.12
    
    echo "📥 Installing dependencies..."
    uv pip install -q -r requirements.txt

# otherwise use pip
else
    echo "uv not found — falling back to pip..."
    python3 -m venv "$REPO_ROOT/.venv"
    
    echo "🔌 Activating virtual environment..."
    source "$REPO_ROOT/.venv/bin/activate"
    
    echo "⬆️  Upgrading pip..."
    "$REPO_ROOT/.venv/bin/pip" install --upgrade pip
    
    echo "📥 Installing dependencies..."
    "$REPO_ROOT/.venv/bin/pip" install -r requirements.txt
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
fi

echo "✅ Setup complete!"
