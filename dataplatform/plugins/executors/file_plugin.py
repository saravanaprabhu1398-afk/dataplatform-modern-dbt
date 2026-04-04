import os
import shutil
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class FileExecutor:
    """File operations executor for local and cloud file handling."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute file operations.
        
        config = {
            "operation": "copy" | "move" | "delete" | "create" | "read" | "append" | "merge",
            "source": "path/to/source",
            "destination": "path/to/dest",
            "content": "file content (for create)",
            ...
        }
        """
        try:
            operation = config.get("operation", "read").lower()

            if operation == "read":
                return self._read_file(config)
            elif operation == "create":
                return self._create_file(config)
            elif operation == "append":
                return self._append_file(config)
            elif operation == "copy":
                return self._copy_file(config)
            elif operation == "move":
                return self._move_file(config)
            elif operation == "delete":
                return self._delete_file(config)
            elif operation == "merge":
                return self._merge_files(config)
            elif operation == "list":
                return self._list_files(config)
            else:
                return False, {"error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"File operation error: {e}")
            return False, {"error": str(e)}

    def _read_file(self, config: dict) -> tuple[bool, dict]:
        """Read file content."""
        try:
            file_path = config.get("file_path")
            if not file_path or not os.path.exists(file_path):
                return False, {"error": "File not found"}

            with open(file_path, 'r') as f:
                content = f.read()

            file_size = os.path.getsize(file_path)
            logger.info(f"✓ Read file: {file_path} ({file_size} bytes)")

            return True, {
                "file": file_path,
                "size": file_size,
                "content": content
            }

        except Exception as e:
            logger.error(f"Failed to read file: {e}")
            return False, {"error": str(e)}

    def _create_file(self, config: dict) -> tuple[bool, dict]:
        """Create a new file with content."""
        try:
            file_path = config.get("file_path")
            content = config.get("content", "")

            if not file_path:
                return False, {"error": "file_path required"}

            # Create parent directories if needed
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w') as f:
                f.write(content)

            logger.info(f"✓ Created file: {file_path}")

            return True, {"file": file_path, "size": len(content)}

        except Exception as e:
            logger.error(f"Failed to create file: {e}")
            return False, {"error": str(e)}

    def _append_file(self, config: dict) -> tuple[bool, dict]:
        """Append content to a file."""
        try:
            file_path = config.get("file_path")
            content = config.get("content", "")

            if not file_path:
                return False, {"error": "file_path required"}

            with open(file_path, 'a') as f:
                f.write(content)

            logger.info(f"✓ Appended to file: {file_path}")

            return True, {"file": file_path}

        except Exception as e:
            logger.error(f"Failed to append to file: {e}")
            return False, {"error": str(e)}

    def _copy_file(self, config: dict) -> tuple[bool, dict]:
        """Copy file from source to destination."""
        try:
            source = config.get("source")
            destination = config.get("destination")

            if not source or not destination:
                return False, {"error": "source and destination required"}

            if not os.path.exists(source):
                return False, {"error": f"Source file not found: {source}"}

            # Create parent directories if needed
            Path(destination).parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(source, destination)
            logger.info(f"✓ Copied file: {source} -> {destination}")

            return True, {"source": source, "destination": destination}

        except Exception as e:
            logger.error(f"Failed to copy file: {e}")
            return False, {"error": str(e)}

    def _move_file(self, config: dict) -> tuple[bool, dict]:
        """Move/rename file from source to destination."""
        try:
            source = config.get("source")
            destination = config.get("destination")

            if not source or not destination:
                return False, {"error": "source and destination required"}

            if not os.path.exists(source):
                return False, {"error": f"Source file not found: {source}"}

            # Create parent directories if needed
            Path(destination).parent.mkdir(parents=True, exist_ok=True)

            shutil.move(source, destination)
            logger.info(f"✓ Moved file: {source} -> {destination}")

            return True, {"source": source, "destination": destination}

        except Exception as e:
            logger.error(f"Failed to move file: {e}")
            return False, {"error": str(e)}

    def _delete_file(self, config: dict) -> tuple[bool, dict]:
        """Delete a file."""
        try:
            file_path = config.get("file_path")

            if not file_path:
                return False, {"error": "file_path required"}

            if not os.path.exists(file_path):
                return False, {"error": f"File not found: {file_path}"}

            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
                logger.info(f"✓ Deleted directory: {file_path}")
            else:
                os.remove(file_path)
                logger.info(f"✓ Deleted file: {file_path}")

            return True, {"file": file_path}

        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            return False, {"error": str(e)}

    def _merge_files(self, config: dict) -> tuple[bool, dict]:
        """Merge multiple files into one."""
        try:
            source_files = config.get("source_files", [])
            destination = config.get("destination")

            if not source_files or not destination:
                return False, {"error": "source_files and destination required"}

            # Create parent directories if needed
            Path(destination).parent.mkdir(parents=True, exist_ok=True)

            with open(destination, 'w') as outfile:
                for file_path in source_files:
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as infile:
                            outfile.write(infile.read() + "\n")

            logger.info(f"✓ Merged {len(source_files)} files into {destination}")

            return True, {
                "source_files": source_files,
                "destination": destination,
                "file_count": len(source_files)
            }

        except Exception as e:
            logger.error(f"Failed to merge files: {e}")
            return False, {"error": str(e)}

    def _list_files(self, config: dict) -> tuple[bool, list]:
        """List files in a directory."""
        try:
            directory = config.get("directory", ".")
            pattern = config.get("pattern", "*")
            recursive = config.get("recursive", False)

            if not os.path.exists(directory):
                return False, {"error": f"Directory not found: {directory}"}

            files = []
            if recursive:
                for root, dirs, filenames in os.walk(directory):
                    for filename in filenames:
                        if self._match_pattern(filename, pattern):
                            files.append(os.path.join(root, filename))
            else:
                for item in os.listdir(directory):
                    if self._match_pattern(item, pattern):
                        files.append(os.path.join(directory, item))

            logger.info(f"✓ Listed {len(files)} files in {directory}")

            return True, {
                "directory": directory,
                "file_count": len(files),
                "files": files
            }

        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return False, {"error": str(e)}

    @staticmethod
    def _match_pattern(filename: str, pattern: str) -> bool:
        """Simple pattern matching."""
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)
