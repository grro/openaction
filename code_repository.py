import json
import logging
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


# Regex of allowed characters in a unit (task) name: word characters
# (letters, digits, underscore) and hyphen. Anything else is rejected
# to keep names safe for use as directory and file names across
# platforms.
_VALID_NAME_PATTERN = re.compile(r"^[\w\-]+$")


class Image:
    """
    On-disk representation of a single unit (typically a task).

    An ``Image`` owns a directory ``<codedir>/<unit_name>/`` containing
    three sibling files:

      * ``<unit_name>.py``    -- the unit's Python source code,
      * ``<unit_name>.props`` -- a JSON document with arbitrary properties,
      * ``<unit_name>.desc``  -- a free-form text description.

    Images can be created with a temporary, randomly generated name via
    :meth:`new` and later renamed to their final name via :meth:`rename`.
    This two-step pattern allows the caller to populate an image
    atomically and only "publish" it (by renaming) once it is fully
    written.

    Instances are cheap to construct and do **not** load any file
    content eagerly; use :meth:`read` to fetch the contents and
    :meth:`write_data` to persist them.
    """

    TEMP_PREFIX = 'temp_'

    def __init__(self, codedir: Path, unit_name: str):
        """
        Args:
            codedir: Root directory under which all images live. Created
                automatically if it does not yet exist.
            unit_name: Name of the unit. Must match
                :data:`_VALID_NAME_PATTERN` (alphanumeric plus ``_`` and
                ``-``); otherwise a :class:`ValueError` is raised.

        Side effects:
            The image directory ``<codedir>/<unit_name>/`` is created on
            disk if it does not yet exist.
        """
        if not unit_name or not _VALID_NAME_PATTERN.match(unit_name):
            raise ValueError("Task name must be alphanumeric (with _ or - allowed)")

        self.codedir = codedir
        self.unit_name = unit_name

        codedir.mkdir(parents=True, exist_ok=True)
        self.unit_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def new(codedir: Path) -> 'Image':
        """
        Create a new image with a temporary, random unit name.

        Use this together with :meth:`rename` to atomically publish a
        freshly written image under its final name.
        """
        return Image(codedir, Image.TEMP_PREFIX + str(uuid.uuid4()))

    def is_temp(self) -> bool:
        """``True`` if this image still carries the temporary prefix."""
        return self.unit_name.startswith(Image.TEMP_PREFIX)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    @property
    def unit_path(self) -> Path:
        """Absolute path of the directory that holds this image's files."""
        return Path(self.codedir) / self.unit_name

    def _get_paths(self) -> tuple[Path, Path, Path]:
        """Return the ``(code, props, desc)`` file paths for this image."""
        return (
            self.unit_path / f"{self.unit_name}.py",
            self.unit_path / f"{self.unit_name}.props",
            self.unit_path / f"{self.unit_name}.desc",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def delete(self) -> None:
        """Recursively delete the image directory and all its files."""
        dir_path = self.unit_path
        if dir_path.exists() and dir_path.is_dir():
            shutil.rmtree(dir_path)
            logger.info(f"Deleted image directory: {dir_path}")
        else:
            logger.warning(f"Image directory {dir_path} does not exist or is not a directory.")

    def rename(self, new_name: str) -> 'Image':
        """
        Rename the image directory to ``new_name``.

        If a directory with ``new_name`` already exists, it is replaced
        atomically: the current directory is parked under a temporary
        name, the existing target is moved into the parking slot, the
        current directory takes the target's place, and the old content
        is finally deleted. This keeps a valid image under ``new_name``
        visible at every point in time.

        Returns:
            ``self``, with :attr:`unit_name` updated.
        """
        old_path = Path(self.codedir) / f"{self.unit_name}"
        new_path = Path(self.codedir) / f"{new_name}"

        if old_path.exists():
            if new_path.exists():
                # Atomic swap: park old, move existing target to parking
                # slot, promote our directory to the target, then drop
                # the parked (now stale) content.
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

        self.unit_name = new_name
        return self

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    def write_data(self, code: str, desc: str, props: Dict[str, Any]) -> None:
        """
        Persist the unit's code, description and properties to disk.

        Existing files with the same names are overwritten in place.
        """
        code_file, props_file, desc_file = self._get_paths()
        code_file.write_text(code, encoding="utf-8")
        props_file.write_text(json.dumps(props, indent=2), encoding="utf-8")
        desc_file.write_text(desc, encoding="utf-8")

    def read(self) -> tuple[str, str, dict[str, Any]]:
        """
        Load the unit's code, description and properties from disk.

        Missing or malformed ``.props`` files are tolerated and produce
        an empty dict; a missing ``.desc`` file produces an empty
        string. The code file is required and a missing/unreadable code
        file will raise the underlying :class:`OSError`.

        Returns:
            A tuple ``(code, description, properties)``.
        """
        code_file, props_file, desc_file = self._get_paths()

        task_code = code_file.read_text(encoding="utf-8")

        try:
            props = json.loads(props_file.read_text(encoding="utf-8")) if props_file.exists() else {}
        except json.JSONDecodeError:
            props = {}

        desc = desc_file.read_text(encoding="utf-8") if desc_file.exists() else ""

        return task_code, desc, props

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return self.unit_name

    def __repr__(self) -> str:
        return f"Image(name='{self.unit_name}')"


class CodeRepository:
    """
    Filesystem-backed collection of :class:`Image` objects.

    The repository owns a single root directory (``codedir``) under
    which every image lives in its own subdirectory. It is purely a
    thin lookup/factory layer: it does not cache image content and
    every call hits the filesystem.
    """

    def __init__(self, codedir: str | Path):
        """
        Args:
            codedir: Root directory for all images. Created if missing.
        """
        self._codedir = Path(codedir)
        self._codedir.mkdir(parents=True, exist_ok=True)

    def create_image(self, name: str) -> Image:
        """
        Create a fresh image and publish it under ``name``.

        Internally creates a temp-named image first and renames it, so
        partial state is never visible under the final name.
        """
        return Image.new(self._codedir).rename(name)

    def get_image(self, name: str) -> Image:
        """Return an :class:`Image` handle for ``name`` (does not check existence)."""
        return Image(self._codedir, name)

    def delete_image(self, name: str) -> None:
        """Delete the image directory for ``name`` if it exists."""
        image = self.get_image(name)
        if image:
            image.delete()

    def list_images(self, incl_temp: bool = False) -> List[Image]:
        """
        List every image currently stored in the repository.

        Args:
            incl_temp: When ``False`` (the default), images whose name
                still carries the temporary prefix are skipped. Set to
                ``True`` to also receive in-flight / orphaned temp
                images, e.g. for cleanup tasks.

        Notes:
            * Hidden directories (those starting with ``_``) are always
              ignored.
            * Files at the top level of ``codedir`` are ignored; only
              subdirectories are treated as images.
        """
        if not self._codedir.exists() or not self._codedir.is_dir():
            return []

        images: List[Image] = []
        for entry in self._codedir.iterdir():
            if entry.is_dir() and not entry.name.startswith("_"):
                img = self.get_image(entry.name)
                if not incl_temp and img.is_temp():
                    continue
                images.append(img)
        return images
