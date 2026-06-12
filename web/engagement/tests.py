import io
import pytest
from PIL import Image
from django.core.files.uploadedfile import SimpleUploadedFile

from social.models import Photo
from social.services import PhotoService
from catalog.models import Category, Manufacturer, Item
from inventory.models import InventoryItem

def fake_image(name="test.jpg"):
    file = io.BytesIO()
    image = Image.new("RGB", (100, 100), color="red")
    image.save(file, "JPEG")
    file.seek(0)

    return SimpleUploadedFile(
        name=name,
        content=file.read(),
        content_type="image/jpeg",
    )

@pytest.mark.django_db
class TestPhotoService:
    def test_first_photo_is_primary(
        self, inventory_item_public, user):
        photo = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image(),
            user=user
        )

        assert photo.is_primary is True
        assert Photo.objects.count() == 1

    def test_second_photo_not_primary_by_default(
        self, inventory_item_public, user):
        PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("1.jpg"),
            user=user
        )

        photo2 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("2.jpg"),
            user=user
        )

        assert photo2.is_primary is False

    def test_make_primary_demotes_existing_primary(
        self, inventory_item_public, user):
        photo1 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("1.jpg"),
            user=user
        )

        photo2 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("2.jpg"),
            user=user,
            make_primary=True
        )

        photo1.refresh_from_db()
        photo2.refresh_from_db()

        assert photo2.is_primary is True
        assert photo1.is_primary is False

    def test_photo_ordering_is_sequential(
        self, inventory_item_public, user):
        p1 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("1.jpg"),
            user=user
        )
        p2 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("2.jpg"),
            user=user
        )
        p3 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("3.jpg"),
            user=user
        )

        assert p1.order == 0
        assert p2.order == 1
        assert p3.order == 2

    def test_set_primary_service(
        self, inventory_item_public, user):
        p1 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("1.jpg"),
            user=user
        )
        p2 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("2.jpg"),
            user=user
        )

        PhotoService.set_primary(p2)

        p1.refresh_from_db()
        p2.refresh_from_db()

        assert p2.is_primary is True
        assert p1.is_primary is False

    def test_primary_is_scoped_per_object(
        self, inventory_item_public, inventory_item_private, user):
        p1 = PhotoService.add_photo(
            content_object=inventory_item_public,
            image=fake_image("1.jpg"),
            user=user
        )

        p2 = PhotoService.add_photo(
            content_object=inventory_item_private,
            image=fake_image("2.jpg"),
            user=user
        )

        assert p1.is_primary is True
        assert p2.is_primary is True

