from django.shortcuts import render

# Create your views here.
def photos_for(obj):
    ct = ContentType.objects.get_for_model(obj.__class__)

    return (
        Photo.objects
        .filter(content_type=ct, object_id=obj.pk)
        .order_by("order", "created_at")
    )



from django.db import transaction
class PhotoService:

    @staticmethod
    @transaction.atomic
    def add_photo(*, content_object, image, user=None, make_primary=False):
        content_type = ContentType.objects.get_for_model(content_object)

        existing = Photo.objects.filter(
            content_type=content_type,
            object_id=content_object.id
        )

        order = existing.count()

        # First photo is always primary
        if not existing.exists():
            make_primary = True

        if make_primary:
            existing.update(is_primary=False)

        return Photo.objects.create(
            image=image,
            user=user,
            content_type=content_type,
            object_id=content_object.id,
            order=order,
            is_primary=make_primary,
        )

    @staticmethod
    @transaction.atomic
    def set_primary(photo: Photo):
        Photo.objects.filter(
            content_type=photo.content_type,
            object_id=photo.object_id
        ).update(is_primary=False)

        photo.is_primary = True
        photo.save(update_fields=["is_primary"])

    @staticmethod
    @transaction.atomic
    def reorder_photos(content_object, ordered_ids):
        content_type = ContentType.objects.get_for_model(content_object)

        for index, photo_id in enumerate(ordered_ids):
            Photo.objects.filter(
                id=photo_id,
                content_type=content_type,
                object_id=content_object.id
            ).update(order=index)