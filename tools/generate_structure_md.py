import os
from pathlib import Path
from datetime import datetime
import difflib

# Define folders
SCRIPT_DIR = Path.cwd()
PROJECT_ROOT = SCRIPT_DIR.parent
VERSIONS_DIR = SCRIPT_DIR / "versions"
CHANGES_DIR = SCRIPT_DIR / "changes"
VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
CHANGES_DIR.mkdir(parents=True, exist_ok=True)

# Format size in o or Ko
def format_file_size(size_bytes):
    return f"{size_bytes / 1024:.1f} Ko" if size_bytes >= 1024 else f"{size_bytes} o"

# Generate a pretty tree-like structure in Markdown
def generate_tree_markdown(root_path):
    lines = []

    def walk(path, prefix=""):
        contents = list(sorted((p for p in path.iterdir() if p.name not in {
            ".git", "venv", "__pycache__", "tools", ".DS_Store", "node_modules", ".venv"
        }), key=lambda x: (x.is_file(), x.name.lower())))

        for i, p in enumerate(contents):
            connector = "└── " if i == len(contents) - 1 else "├── "
            if p.is_dir():
                lines.append(f"{prefix}{connector}{p.name}/")
                extension = "    " if i == len(contents) - 1 else "│   "
                walk(p, prefix + extension)
            else:
                size = format_file_size(p.stat().st_size)
                lines.append(f"{prefix}{connector}{p.name} ({size})")

    walk(root_path)
    return lines

# Get the latest version number
def get_latest_version():
    existing = sorted(VERSIONS_DIR.glob("version_*.md"))
    if not existing:
        return 0
    latest = existing[-1].stem
    return int(latest.split("_")[1])

# Compare path lists to get added and removed
def compare_versions(old, new):
    diff = difflib.ndiff(old, new)
    added = [line[2:] for line in diff if line.startswith('+ ')]
    removed = [line[2:] for line in diff if line.startswith('- ')]
    return added, removed

# Main function
def create_structure_and_diff():
    tree_lines = generate_tree_markdown(PROJECT_ROOT)
    version_number = get_latest_version()
    new_version = version_number + 1

    # Save main project structure .md
    tree_md_path = VERSIONS_DIR / f"version_{new_version:03}.md"
    with open(tree_md_path, "w") as f:
        f.write(f"# 📁 Project Structure — Version {new_version}\n")
        f.write(f"_Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n\n")
        for line in tree_lines:
            f.write(f"{line}\n")

    # Compare with previous version (if any)
    if version_number > 0:
        prev_path = VERSIONS_DIR / f"version_{version_number:03}.md"
        with open(prev_path, "r") as f:
            prev_lines = [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#") and not line.startswith("_")]

        added, removed = compare_versions(prev_lines, tree_lines)

        # Save change report
        diff_md_path = CHANGES_DIR / f"changes_from_v{version_number:03}_to_v{new_version:03}.md"
        with open(diff_md_path, "w") as f:
            f.write(f"# 🔄 Changes from Version {version_number} to {new_version}\n")
            f.write(f"_Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n\n")
            if added:
                f.write("## ✅ Added\n")
                for path in added:
                    f.write(f"- `{path}`\n")
            if removed:
                f.write("\n## ❌ Removed\n")
                for path in removed:
                    f.write(f"- `{path}`\n")
            if not added and not removed:
                f.write("✅ No changes detected.\n")

    print(f"✅ Project structure saved to: {tree_md_path.name}")
    if version_number > 0:
        print(f"🔍 Changes saved to: {diff_md_path.name}")

create_structure_and_diff()