import pytest

from django.contrib.auth import get_user_model

from catalog.models import Category, Item, Manufacturer
from inventory.models import InventoryItem, InventoryPrivacy

pytestmark = pytest.mark.django_db

User = get_user_model()

@pytest.fixture
def user(db):
    return User.objects.create_user(
        username="testuser",
        email="test@example.com",
        password="password",
    )


@pytest.fixture
def other_user(db):
    return User.objects.create_user(
        username="otheruser",
        email="other@example.com",
        password="password",
    )


@pytest.fixture
def category(db):
    return Category.objects.create(
        name="Knife",
        slug="knife",
    )

@pytest.fixture
def manufacturer(db):
    return Manufacturer.objects.create(
        name="Acme Corp",
        slug="acme",
        website="https://example.com",
    )


@pytest.fixture
def catalog_item(db, category, manufacturer):
    return Item.objects.create(
        name="Test Item",
        category=category,
        manufacturer=manufacturer,
    )

@pytest.fixture
def inventory_item_public(db, user, category):
    return InventoryItem.objects.create(
        user=user,
        category=category,
        custom_name="Public Knife",
        privacy=InventoryPrivacy.PUBLIC,
        is_sold=False,
    )


@pytest.fixture
def inventory_item_private(db, user, category):
    return InventoryItem.objects.create(
        user=user,
        category=category,
        custom_name="Private Knife",
        privacy=InventoryPrivacy.PRIVATE,
        is_sold=False,
    )


@pytest.fixture
def inventory_item_friends(db, user, category):
    return InventoryItem.objects.create(
        user=user,
        category=category,
        custom_name="Friends Knife",
        privacy=InventoryPrivacy.FRIENDS,
        is_sold=False,
    )


@pytest.fixture
def inventory_item_sold(db, user, category):
    return InventoryItem.objects.create(
        user=user,
        category=category,
        custom_name="Sold Knife",
        privacy=InventoryPrivacy.PUBLIC,
        is_sold=True,
    )

@pytest.fixture
def inventory_item_with_catalog(db, user, category, catalog_item):
    return InventoryItem.objects.create(
        user=user,
        category=category,
        item=catalog_item,
        custom_name="My Test Item",
        privacy=InventoryPrivacy.PUBLIC,
        is_sold=False,
    )