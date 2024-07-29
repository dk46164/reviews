import os
from datetime import datetime
import re
import shutil

def get_all_objects(base_folder):
    """
    Get all objects in the specified local folder.
    Args:
        base_folder (str): Path to the source folder
    Returns:
        list: List of absolute paths of all the files within source folder
    """
    all_objects = []
    for root, _, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.jsonl'):
                all_objects.append(os.path.join(root, file))
    return all_objects

def get_latest_files(base_folder):
    """
    Get the latest version of each unique file based on the date folder.
    Args:
        base_folder (str): Base folder path
    Returns:
        list: List of absolute paths of the latest version of each unique file
    """
    all_objects = get_all_objects(base_folder)
    date_pattern = r'\d{4}-\d{2}-\d{2}'
    file_dict = {}
    for path in all_objects:
        match = re.search(date_pattern, path)
        if match:
            date_str = match.group()
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d')
                file_name = os.path.basename(path)
                if file_name not in file_dict or date > file_dict[file_name]['date']:
                    file_dict[file_name] = {'date': date, 'path': path}
            except ValueError:
                continue
    latest_files = [file_info['path'] for file_info in file_dict.values()]
    return latest_files

def move_processed_files(files, proc_folder):
    """
    Move the given files to a new folder in the local filesystem.
    Args:
        files (list): List of file paths to move
        proc_folder (str): New folder path
    Returns:
        list: List of new paths of the moved files
    """
    moved_files = []
    for source_path in files:
        base_name = source_path.split(os.sep)
        file_name = base_name[-1]
        date_folder = base_name[-2]
        new_path = os.path.join(proc_folder, date_folder, file_name)
        
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        
        # Move the file
        shutil.move(source_path, new_path)
        moved_files.append(new_path)
    return moved_files
