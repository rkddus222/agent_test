
import os
import yaml
import difflib

def read_file(path):
    """
    Reads the content of a file.
    Args:
        path (str): Relative path to the file from the playground directory.
    Returns:
        dict: A dictionary containing the content of the file.
    """
    # Restrict access to playground directory for safety
    base_dir = os.path.join(os.path.dirname(__file__), 'playground')
    target_path = os.path.join(base_dir, path)
    
    if not os.path.exists(target_path):
        return {"error": f"File not found: {path}"}
    
    try:
        with open(target_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return {"content": content}
    except Exception as e:
        return {"error": str(e)}

def edit_file(proposals):
    """
    Edits files based on proposals and checks for YAML syntax errors.
    Args:
        proposals (list): List of dictionaries containing file path and edits.
    Returns:
        dict: Success status, issues list, and error/warning counts.
    """
    base_dir = os.path.join(os.path.dirname(__file__), 'playground')
    issues = []
    error_count = 0
    warning_count = 0
    diff = ""
    
    for proposal in proposals:
        path = proposal['file']
        edits = proposal['edits']
        target_path = os.path.join(base_dir, path)
        
        if not os.path.exists(target_path):
            return {"success": False, "error": f"File not found: {path}"}
        
        try:
            with open(target_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # Normalize line endings to \n for consistent matching
            full_text = original_content.replace('\r\n', '\n')
            
            for edit in edits:
                old_text = edit.get('oldText', '')
                new_text = edit.get('newText', '')
                
                # Normalize edit texts as well
                old_text = old_text.replace('\r\n', '\n')
                new_text = new_text.replace('\r\n', '\n')

                # Check for occurrences
                count = full_text.count(old_text)
                if count == 0:
                     return {"success": False, "error": f"Target content not found. Check indentation and exact characters."}
                elif count > 1:
                     return {"success": False, "error": f"Target content found {count} times. Please provide more context to make it unique."}
                
                full_text = full_text.replace(old_text, new_text)
            
            new_full_text = full_text

            # Save the new text directly
            with open(target_path, 'w', encoding='utf-8', newline='\n') as f:
                f.write(new_full_text)
                
            # Lint Check (YAML Syntax only)
            try:
                yaml.safe_load(new_full_text)
            except yaml.YAMLError as exc:
                error_count += 1
                issues.append({
                    "severity": "ERROR",
                    "file": path,
                    "line": getattr(exc, 'problem_mark', None).line if hasattr(exc, 'problem_mark') else 0,
                    "code": "YAML_SYNTAX_ERROR",
                    "message": str(exc)
                })

        except Exception as e:
             return {"success": False, "error": str(e)}

        # Generate Diff
        original_lines = original_content.replace('\r\n', '\n').splitlines(keepends=True)
        new_lines = new_full_text.splitlines(keepends=True)
        
        diff = "".join(difflib.unified_diff(
            original_lines, 
            new_lines,
            fromfile=f"a/{path}", 
            tofile=f"b/{path}"
        ))

    return {
        "success": error_count == 0,
        "issues": issues,
        "error_count": error_count,
        "warning_count": warning_count,
        "diff": diff
    }
