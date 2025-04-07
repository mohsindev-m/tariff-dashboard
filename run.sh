#!/bin/bash
# Cleanup script for Python FastAPI project cache files

# Remove __pycache__ directories
echo "Cleaning up __pycache__ directories..."
find . -type d -name "__pycache__" -exec rm -rf {} +

# Remove .pytest_cache directories
echo "Cleaning up .pytest_cache directories..."
find . -type d -name ".pytest_cache" -exec rm -rf {} +

# Remove .mypy_cache directories (if using mypy)
echo "Cleaning up .mypy_cache directories..."
find . -type d -name ".mypy_cache" -exec rm -rf {} +

# Remove compiled Python files (.pyc and .pyo)
echo "Cleaning up .pyc and .pyo files..."
find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete

echo "Cleanup complete!"
