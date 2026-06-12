import json
from dataclasses import dataclass
from typing import List, Dict

from django.db import transaction
from nodes.models import Node, NodeKind
from media.models import Photo


@dataclass
class PhotoData:
    id: str
    is_primary: bool = False
    order: int = 99

    @classmethod
    def parse_list(cls, data: str) -> List["PhotoData"]:
        try:
            raw_list = json.loads(data or '[]')
        except json.JSONDecodeError:
            raw_list = []

        for obj in raw_list:
            raw_id = obj.get("id")
            if raw_id is None:
                continue
            yield cls(
                    id=str(raw_id),
                    is_primary=bool(obj.get("is_primary", False)),
                    order=int(obj.get("order", 99)) if "order" in obj else 99
                )

    @classmethod
    def parse_dict(cls, data: str) -> Dict[str, "PhotoData"]:
        return {int(pd.id): pd for pd in cls.parse_list(data)}

    @classmethod
    def parse_ids(cls, data: str) -> List[str]:
        return [pd.id for pd in cls.parse_list(data)]


class PhotoService:

    @staticmethod
    @transaction.atomic
    def add(*, node, add_json, request_files):
        data = PhotoData.parse_list(add_json)
        photos_to_create = [
            Photo(
                node=node,
                image=photo_file,
                is_primary=photo_data.is_primary,
                order=photo_data.order,
            )
            for photo_data, photo_file in zip(data, request_files)
        ]
        if photos_to_create:
            Photo.objects.bulk_create(photos_to_create)

    @staticmethod
    @transaction.atomic
    def delete(*, node, deleted_json):
        ids = PhotoData.parse_ids(deleted_json)
        if not ids:
            return
        Photo.objects.filter(node=node, id__in=ids).delete()

    @staticmethod
    @transaction.atomic
    def update(*, node, update_json, add_json=None, request_files=None):
        qs = Photo.objects.filter(node=node)
        qs.filter(is_primary=True).update(is_primary=False)
        
        if add_json:
            PhotoService.add(node=node,
                request_files=request_files, add_json=add_json)

        _map = PhotoData.parse_dict(update_json)
        for photo in qs.filter(id__in=_map.keys()).select_for_update():
            data, update_fields = _map[photo.pk], []
            if photo.order != data.order:
                photo.order = data.order
                update_fields.append('order')

            if photo.is_primary != data.is_primary:
                photo.is_primary = data.is_primary
                update_fields.append('is_primary')

            if update_fields:
                photo.save(update_fields=update_fields)

    @staticmethod
    def json_photos(node):
        return [
            {
                'id': photo.id,
                'url': photo.image.url,
                'name': photo.image.name.rsplit('/', 1)[-1],
                'is_primary': photo.is_primary,
                'order': photo.order,
            }
            for photo in node.photos.all().order_by("order", "id")
        ]