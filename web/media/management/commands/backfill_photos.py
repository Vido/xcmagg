import os
import shutil

from django.core.files.storage import default_storage
from django.core.management.base import BaseCommand

from media.models import Photo, _photo_upload_path


class Command(BaseCommand):
    help = "Backfill alt_text and slugify filenames for existing photos"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--skip-rename",
            action="store_true",
            help="Only backfill alt_text, skip file rename",
        )

    def handle(self, *args, **opts):
        dry = opts["dry_run"]
        skip_rename = opts["skip_rename"]

        for photo in Photo.objects.select_related("node").all():
            changed = []
            update_fields = []

            if not photo.alt_text:
                photo.alt_text = str(photo.node)
                changed.append("alt_text")
                update_fields.append("alt_text")

            if not skip_rename:
                old_name = photo.image.name
                new_name = _photo_upload_path(photo, old_name)
                if old_name != new_name:
                    if not dry:
                        old_path = default_storage.path(old_name)
                        new_path = default_storage.path(new_name)
                        os.makedirs(os.path.dirname(new_path), exist_ok=True)
                        shutil.copy2(old_path, new_path)
                        photo.image.name = new_name
                    changed.append(f"image: {old_name} → {new_name}")
                    update_fields.append("image")

            if changed and not dry:
                photo.save(update_fields=update_fields)

            status = ", ".join(changed) if changed else "no changes"
            prefix = "[DRY] " if dry else ""
            self.stdout.write(f"{prefix}pk={photo.pk}: {status}")

        self.stdout.write(self.style.SUCCESS("Done."))
