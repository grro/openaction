import json
import logging
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


# Allowed characters for a unit name: letters, digits, underscore and dash.
_NAME_PATTERN = re.compile(r"^[\w\-]+$")


class Image:
    """
    On-disk representation of a single "unit" (e.g. a task) inside a
    :class:`CodeRepository`.

    An image is stored as a dedicated directory ``<codedir>/<unit_name>/``
    containing three sibling files:

      * ``<unit_name>.py``    — the source code
      * ``<unit_name>.props`` — a JSON-encoded property dict
      * ``<unit_name>.desc``  — a free-form text description

    Instances are cheap; the constructor only validates the name and
    ensures the backing directory exists on disk.
    """

    # Prefix used for newly created, not-yet-named images.
    TEMP_PREFIX = "temp_"

    def __init__(self, codedir: Path, unit_name: str):
        """
        Args:
            codedir:   Root directory of the owning :class:`CodeRepository`.
            unit_name: Name of this image. Must match ``[A-Za-z0-9_-]+``.

        Raises:
            ValueError: If ``unit_name`` is empty or contains illegal chars.
        """
        if not unit_name or not _NAME_PATTERN.match(unit_name):
            raise ValueError(
                f"Invalid unit name '{unit_name}'. "
                "Must be alphanumeric (underscores and dashes allowed)."
            )

        self.codedir = Path(codedir)
        self.unit_name = unit_name

        self.codedir.mkdir(parents=True, exist_ok=True)
        self.unit_path.mkdir(parents=True, exist_ok=True)

    # ----- Construction helpers ------------------------------------------------

    @staticmethod
    def new(codedir: Path) -> "Image":
        """Create a fresh image with a temporary, unique name."""
        return Image(codedir, Image.TEMP_PREFIX + uuid.uuid4().hex)

    def __str__(self) -> str:
        return self.unit_name

    def __repr__(self) -> str:
        return f"Image(name='{self.unit_name}')"

    # ----- Lifecycle -----------------------------------------------------------

    def is_temp(self) -> bool:
        """True if this image still carries the auto-generated temp name."""
        return self.unit_name.startswith(Image.TEMP_PREFIX)

    def delete(self) -> None:
        """Remove the image directory and all of its contents."""
        dir_path = self.unit_path
        if dir_path.exists() and dir_path.is_dir():
            shutil.rmtree(dir_path)
            logger.info(f"Deleted image directory: {dir_path}")
        else:
            logger.warning(
                f"Image directory {dir_path} does not exist or is not a directory."
            )

    def rename(self, new_name: str) -> "Image":
        """
        Rename the on-disk directory backing this image.

        If a directory with ``new_name`` already exists, it is atomically
        replaced by the current image's content (the previously stored
        directory is deleted).

        Args:
            new_name: New unit name. Must satisfy the same naming rules
                as the constructor.

        Returns:
            ``self`` (for fluent chaining).

        Raises:
            ValueError: If ``new_name`` is invalid.
        """
        if not new_name or not _NAME_PATTERN.match(new_name):
            raise ValueError(
                f"Invalid target name '{new_name}'. "
                "Must be alphanumeric (underscores and dashes allowed)."
            )

        old_path = self.codedir / self.unit_name
        new_path = self.codedir / new_name

        if not old_path.exists():
            logger.warning(
                f"Image directory {old_path} does not exist and cannot be renamed."
            )
            self.unit_name = new_name
            return self

        if new_path.exists():
            # Atomic-ish swap: old -> tmp, new -> old (so the previous
            # tenant of new_path is now under old_path), tmp -> new.
            # Finally delete the old (= replaced) content.
            temp_path = self.codedir / (Image.TEMP_PREFIX + self.unit_name)
            old_path.rename(temp_path)
            new_path.rename(old_path)
            temp_path.rename(new_path)
            shutil.rmtree(old_path)
            logger.info(
                f"Replaced existing directory: {new_path} was updated "
                "and previous content deleted."
            )
        else:
            old_path.rename(new_path)
            logger.info(f"Renamed image directory from {old_path} to {new_path}")

        self.unit_name = new_name
        return self

    # ----- Paths ---------------------------------------------------------------

    @property
    def unit_path(self) -> Path:
        """Absolute path to this image's backing directory."""
        return self.codedir / self.unit_name

    def _get_paths(self) -> Tuple[Path, Path, Path]:
        """Return the ``(code, props, desc)`` file paths for this image."""
        return (
            self.unit_path / f"{self.unit_name}.py",
            self.unit_path / f"{self.unit_name}.props",
            self.unit_path / f"{self.unit_name}.desc",
        )

    # ----- I/O -----------------------------------------------------------------

    def write_data(self, code: str, desc: str, props: Dict[str, Any]) -> None:
        """Persist code, description and properties to disk (overwriting)."""
        code_file, props_file, desc_file = self._get_paths()
        code_file.write_text(code, encoding="utf-8")
        props_file.write_text(json.dumps(props, indent=2), encoding="utf-8")
        desc_file.write_text(desc, encoding="utf-8")

    def read(self) -> Tuple[str, str, Dict[str, Any]]:
        """
        Read code, description and properties from disk.

        Missing ``.props`` / ``.desc`` files are tolerated and yield
        ``{}`` / ``""`` respectively; a missing or unreadable ``.py``
        file raises the underlying :class:`OSError`.
        """
        code_file, props_file, desc_file = self._get_paths()

        task_code = code_file.read_text(encoding="utf-8")

        props: Dict[str, Any] = {}
        if props_file.exists():
            try:
                props = json.loads(props_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in {props_file}: {e}. Using empty props.")

        desc = desc_file.read_text(encoding="utf-8") if desc_file.exists() else ""

        return task_code, desc, props


class CodeRepository:
    """
    Filesystem-backed collection of :class:`Image` objects, one directory
    per image, rooted at a single ``codedir``.

    The repository is intentionally stateless: every public method
    inspects the filesystem on demand, so multiple processes / instances
    can share the same ``codedir`` safely as long as they don't race on
    the same image name.
    """

    def __init__(self, codedir: str | Path):
        """
        Args:
            codedir: Root directory under which all images live. Created
                automatically if it doesn't yet exist.
        """
        self._codedir = Path(codedir)
        self._codedir.mkdir(parents=True, exist_ok=True)

    def create_image(self, name: str) -> Image:
        """
        Create a brand-new image and rename it to ``name``.

        If an image with that name already exists, it is atomically
        replaced (see :meth:`Image.rename`).
        """
        return Image.new(self._codedir).rename(name)

    def get_image(self, name: str) -> Image:
        """Return the image with the given name (creating the dir if absent)."""
        return Image(self._codedir, name)

    def delete_image(self, name: str) -> None:
        """Delete the image with the given name if it exists."""
        path = self._codedir / name
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
            logger.info(f"Deleted image directory: {path}")
        else:
            logger.warning(f"Cannot delete image '{name}': directory not found.")

    def list_images(self, incl_temp: bool = False) -> List[Image]:
        """
        List all images currently stored in the repository.

        Args:
            incl_temp: If ``False`` (default), temporary images are filtered out.
                Directories whose name starts
                with ``_`` are always skipped (treated as hidden).
        """
        if not self._codedir.exists() or not self._codedir.is_dir():
            return []

        images: List[Image] = []
        for entry in self._codedir.iterdir():
            if not entry.is_dir() or entry.name.startswith("_"):
                continue
            try:
                img = self.get_image(entry.name)
            except ValueError:
                # Skip directories whose name doesn't pass Image's validation.
                logger.debug(f"Skipping invalid image directory: {entry.name}")
                continue
            if not incl_temp and img.is_temp():
                continue
            images.append(img)
        return images
