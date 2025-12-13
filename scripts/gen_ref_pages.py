import mkdocs_gen_files
from pathlib import Path

nav = mkdocs_gen_files.Nav()

for path in sorted(Path("src").rglob("*.py")):
    module_path = path.relative_to("src").with_suffix("")
    doc_path = path.relative_to("src").with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = tuple(module_path.parts)
    
    # Skip __pycache__ and other cache directories
    if "__pycache__" in parts:
        continue

    # Skip __init__.py files completely
    if parts[-1] == "__init__":
        continue
    
    # # Skip __init__ files and special files
    # if parts[-1] == "__init__":
    #     parts = parts[:-1]
    #     doc_path = doc_path.with_name("index.md")
    #     full_doc_path = full_doc_path.with_name("index.md")
    # elif parts[-1].startswith("_"):
    #     continue
    
    if not parts:  # Skip if no valid parts
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        ident = ".".join(parts)
        fd.write(f"::: {ident}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())