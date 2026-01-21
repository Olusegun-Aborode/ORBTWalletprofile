#!/usr/bin/env python3
"""
Fix Script Paths - Use Project Root Detection
Updates scripts to find project root and use paths relative to that
"""

import os
import re
from pathlib import Path

# Template for finding project root
PROJECT_ROOT_FINDER = '''
# Auto-detect project root (where .env file is located)
import os
from pathlib import Path

def get_project_root():
    """Find project root by looking for .env file"""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / '.env').exists():
            return parent
    return Path.cwd()

PROJECT_ROOT = get_project_root()
'''

def add_project_root_finder(filepath):
    """Add project root detection to a script"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Check if already has project root detection
        if 'get_project_root' in content or 'PROJECT_ROOT' in content:
            return False, "Already has PROJECT_ROOT"
        
        # Find where to insert (after imports, before first constant)
        lines = content.split('\n')
        insert_pos = 0
        
        # Find last import or first constant definition
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                insert_pos = i + 1
            elif line.strip() and not line.startswith('#') and '=' in line:
                break
        
        # Insert project root finder
        lines.insert(insert_pos, PROJECT_ROOT_FINDER)
        
        # Now update all CSV paths to use PROJECT_ROOT
        new_content = '\n'.join(lines)
        
        # Replace relative paths with PROJECT_ROOT based paths
        replacements = [
            (r'"../../data/', r'"" + str(PROJECT_ROOT / "data/'),
            (r"'../../data/", r"'' + str(PROJECT_ROOT / 'data/"),
            (r'"../../sql/', r'"" + str(PROJECT_ROOT / "sql/'),
            (r"'../../sql/", r"'' + str(PROJECT_ROOT / 'sql/"),
        ]
        
        for old, new in replacements:
            new_content = re.sub(old, new, new_content)
        
        # Fix the closing quotes
        new_content = re.sub(r'data/([^/]+)/([^"\']+)"', r'data/\1/\2") + ""', new_content)
        new_content = re.sub(r"data/([^/]+)/([^'\"]+)'", r"data/\1/\2') + ''", new_content)
        new_content = re.sub(r'sql/([^"\']+)"', r'sql/\1") + ""', new_content)
        new_content = re.sub(r"sql/([^'\"]+)'", r"sql/\1') + ''", new_content)
        
        with open(filepath, 'w') as f:
            f.write(new_content)
        
        return True, "Added PROJECT_ROOT detection"
    
    except Exception as e:
        return False, f"Error: {e}"

def simpler_fix(filepath):
    """Simpler approach: just change ../../ to nothing and assume run from root"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        
        original = content
        
        # Simply remove ../../ prefix
        content = content.replace('"../../', '"')
        content = content.replace("'../../", "'")
        
        if content != original:
            with open(filepath, 'w') as f:
                f.write(content)
            return True
        return False
    
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    base_path = Path.cwd()
    scripts_path = base_path / "scripts"
    
    print("="*60)
    print("Fixing Script Paths (Simple Approach)")
    print("="*60)
    print("\nRemoving ../../ prefix (scripts will run from project root)\n")
    
    py_files = list(scripts_path.rglob("*.py"))
    updated = 0
    
    for py_file in py_files:
        if simpler_fix(py_file):
            updated += 1
            print(f"✓ Fixed: {py_file.relative_to(base_path)}")
    
    print(f"\n✅ Updated {updated}/{len(py_files)} files")
    print("\n💡 Scripts now use paths relative to project root")
    print("   Example: data/input/final_active_wallets.csv")
    print("\n📌 Always run scripts from project root:")
    print("   python3 scripts/utilities/check_duplicates.py")

if __name__ == "__main__":
    main()
