"""Generate API documentation automatically."""
from pathlib import Path

def generate_api_pages():
    """Generate API pages for all modules."""
    docs_path = Path("docs")
    api_path = docs_path / "api"
    api_path.mkdir(exist_ok=True)
    
    # Core modules
    core_content = """# Core Modules

::: core.Config
    options:
      show_root_heading: true
      show_source: true
      members_order: source

::: core.EarthquakeAnalyzer
    options:
      show_root_heading: true
      show_source: true
      members_order: source
"""
    
    (api_path / "core.md").write_text(core_content)
    
    # Utilities
    utils_content = """# Utility Modules

::: utils.helpers
    options:
      show_root_heading: true
      show_source: true

::: utils.visualization
    options:
      show_root_heading: true
      show_source: true
"""
    
    (api_path / "utils.md").write_text(utils_content)

if __name__ == "__main__":
    generate_api_pages()