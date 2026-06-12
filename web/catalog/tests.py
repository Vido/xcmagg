import pytest

from catalog.selectors import featured_categories
from catalog.models import Category
from inventory.models import InventoryItem, InventoryPrivacy

pytestmark = pytest.mark.django_db


class TestFeaturedCategories:
    def test_counts_all_inventory_items_by_category(
        self,
        category,
        inventory_item_public,
        inventory_item_private,
        inventory_item_friends,
        inventory_item_sold,
    ):
        categories = featured_categories()
        cat = categories[0]
        # 4 items total, regardless of privacy or sold status
        #assert cat.item_count == 4
        assert cat.item_count == 1

    def test_private_friends_and_sold_items_are_not_counted(
        self,
        category,
        inventory_item_private,
        inventory_item_friends,
        inventory_item_sold,
    ):
        categories = featured_categories()
        cat = categories[0]
        assert cat.item_count == 0

    def test_orders_by_item_count_desc(
        self,
        category,
        user,
    ):
        other_category = Category.objects.create(
            name="Flashlight",
            slug="flashlight",
        )

        InventoryItem.objects.create(
            user=user,
            category=other_category,
            custom_name="Light 1",
            privacy=InventoryPrivacy.PUBLIC,
            is_sold=False,
        )

        InventoryItem.objects.create(
            user=user,
            category=other_category,
            custom_name="Light 2",
            privacy=InventoryPrivacy.PUBLIC,
            is_sold=False,
        )

        categories = featured_categories(limit=2)

        assert categories[0].item_count >= categories[1].item_count

    def test_respects_limit(
        self,
        category,
        user,
    ):
        Category.objects.create(name="A", slug="a")
        Category.objects.create(name="B", slug="b")
        Category.objects.create(name="C", slug="c")

        categories = featured_categories(limit=2)

        assert len(categories) == 2

    def test_returns_categories_with_zero_items(self):
        Category.objects.create(name="Empty", slug="empty")

        categories = featured_categories()

        empty = next(c for c in categories if c.slug == "empty")

        assert empty.item_count == 0

import pytest

from inventory.models import InventoryItem, InventoryPrivacy
from inventory.selectors import visible_inventory_for


@pytest.mark.django_db
class TestVisibleInventoryFor:

    def test_owner_sees_all_non_sold_items(
        self,
        user,
        inventory_item_private,
        inventory_item_public,
        inventory_item_friends,
        inventory_item_sold,
    ):
        """
        Owner should see all items regardless of privacy.
        """
        qs = visible_inventory_for(viewer=user, owner=user)

        assert inventory_item_private in qs
        assert inventory_item_public in qs
        assert inventory_item_friends in qs
        assert inventory_item_sold in qs

    def test_anonymous_user_sees_only_public_items(
        self,
        user,
        inventory_item_private,
        inventory_item_public,
        inventory_item_friends,
    ):
        """
        Anonymous users only see public items.
        """
        qs = visible_inventory_for(viewer=None, owner=user)

        assert inventory_item_public in qs
        assert inventory_item_private not in qs
        assert inventory_item_friends not in qs

    def test_other_user_sees_only_public_items(
        self,
        user,
        other_user,
        inventory_item_private,
        inventory_item_public,
        inventory_item_friends,
    ):
        """
        Logged-in non-owner users behave like anonymous users.
        """
        qs = visible_inventory_for(viewer=other_user, owner=user)

        assert inventory_item_public in qs
        assert inventory_item_private not in qs
        assert inventory_item_friends not in qs

    def test_sold_items_are_never_visible(
        self,
        user,
        inventory_item_sold,
    ):
        """
        Sold items should appear only of ex-owner.
        """
        qs_owner = visible_inventory_for(viewer=user, owner=user)
        qs_public = visible_inventory_for(viewer=None, owner=user)

        assert inventory_item_sold in qs_owner
        assert inventory_item_sold not in qs_public
