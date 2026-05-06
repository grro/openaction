import shutil
import uuid
import logging
import re
import json
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)




class Image:
    TEMP_PREFIX = 'temp_'

    def __init__(self, codedir: Path, unit_name: str):
        if not unit_name or not re.match(r"^[\w\-]+$", unit_name):
            raise ValueError("Task name must be alphanumeric (with _ or - allowed)")

        self.codedir = codedir
        self.unit_name = unit_name

        codedir.mkdir(parents=True, exist_ok=True)
        self.unit_path.mkdir(parents=True, exist_ok=True)


    @staticmethod
    def new(codedir: Path) -> 'Image':
        return Image(codedir, Image.TEMP_PREFIX + str(uuid.uuid4()))

    def __str__(self) -> str:
        return self.unit_name

    def __repr__(self) -> str:
        return f"Image(name='{self.unit_name}')"

    def delete(self) -> None:
        """Deletes the image directory and all its contents."""
        dir_path = self.unit_path
        if dir_path.exists() and dir_path.is_dir():
            shutil.rmtree(dir_path)
            logger.info(f"Deleted image directory: {dir_path}")
        else:
            logger.warning(f"Image directory {dir_path} does not exist or is not a directory.")

    def is_temp(self) -> bool:
        return self.unit_name.startswith(Image.TEMP_PREFIX)

    def rename(self, new_name: str):
        """Rename the image directory to match the new unit name."""
        old_path = Path(self.codedir) / f"{self.unit_name}"
        new_path = Path(self.codedir) / f"{new_name}"

        if old_path.exists():
            if new_path.exists():
                temp_path = Path(self.codedir) / (Image.TEMP_PREFIX + self.unit_name)
                old_path.rename(temp_path)
                new_path.rename(old_path)
                temp_path.rename(new_path)
                shutil.rmtree(old_path)
                logger.info(f"Replaced existing directory: {new_path} was updated and previous content deleted.")
            else:
                old_path.rename(new_path)
                logger.info(f"Renamed image directory from {old_path} to {new_path}")
        else:
            logger.warning(f"Image directory {old_path} does not exist and cannot be renamed.")

        self.unit_name = new_name  # Update the unit name after renaming
        return self

    @property
    def unit_path(self) :
        return  Path(self.codedir) /  self.unit_name

    def _get_paths(self) -> tuple[Path, Path]:
        """Helper to generate standard file paths for a given task name."""
        return (
            self.unit_path / f"{self.unit_name}.py",
            self.unit_path / f"{self.unit_name}.props"
        )

    def write_data(self, code: str, props: Dict[str, Any]) -> None:
        code_file, props_file = self._get_paths()

        # Write files
        code_file.write_text(code, encoding="utf-8")
        props_file.write_text(json.dumps(props, indent=2), encoding="utf-8")


    def read(self) -> tuple[str, dict[str, Any]]:
        code_file, props_file = self._get_paths()

        task_code = code_file.read_text(encoding="utf-8")
        try:
            if props_file.exists():
                props = json.loads(props_file.read_text(encoding="utf-8"))
            else:
                props = {}
        except json.JSONDecodeError:
            props = {}

        return task_code, props


class CodeRepository:

    def __init__(self, codedir: str | Path):
        self._codedir = Path(codedir)
        # Ensure the directory exists
        self._codedir.mkdir(parents=True, exist_ok=True)

    def create_image(self, name) -> Image:
        return Image.new(self._codedir).rename(name)

    def get_image(self, name) -> Image:
        return Image(self._codedir, name)

    def delete_image(self, name):
        image = self.get_image(name)
        if image:
            image.delete()

    def list_images(self, incl_temp: bool = False) -> list[Image]:
        """Lists all valid images in the code repository."""
        if not self._codedir.exists() or not self._codedir.is_dir():
            return []

        images = []
        for entry in self._codedir.iterdir():
            if entry.is_dir() and not entry.name.startswith("_"):
                img = self.get_image(entry.name)
                if not incl_temp and img.is_temp():
                    continue
                images.append(img)
        return images

