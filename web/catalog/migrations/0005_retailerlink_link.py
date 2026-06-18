from django.db import migrations, models
import django.db.models.deletion


def url_to_link(apps, schema_editor):
    RetailerLink = apps.get_model("catalog", "RetailerLink")
    Link = apps.get_model("linkcloak", "Link")
    from django.utils.crypto import get_random_string

    for rl in RetailerLink.objects.all():
        link = Link.objects.create(
            target_url=rl.url,
            slug=get_random_string(6).lower(),
            cloak=True,
            rel_sponsored=rl.is_affiliate,
            rel_nofollow=True,
            rel_ugc=False,
        )
        rl.link = link
        rl.save(update_fields=["link"])


def link_to_url(apps, schema_editor):
    RetailerLink = apps.get_model("catalog", "RetailerLink")
    for rl in RetailerLink.objects.select_related("link"):
        if rl.link_id:
            rl.url = rl.link.target_url
            rl.save(update_fields=["url"])


class Migration(migrations.Migration):

    dependencies = [
        ("catalog", "0004_alter_retailerlink_options_remove_retailerlink_item_and_more"),
        ("nodes", "0006_node_unique_user_profile_node"),
        ("linkcloak", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="retailerlink",
            name="link",
            field=models.OneToOneField(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="retailer_link",
                to="linkcloak.link",
            ),
        ),
        migrations.RunPython(url_to_link, link_to_url),
        migrations.AlterField(
            model_name="retailerlink",
            name="link",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="retailer_link",
                to="linkcloak.link",
            ),
        ),
        migrations.RemoveField(
            model_name="retailerlink",
            name="url",
        ),
        migrations.AlterField(
            model_name="retailerlink",
            name="node",
            field=models.ForeignKey(
                limit_choices_to={"kind__in": ["catalog_item", "inventory_item"]},
                on_delete=django.db.models.deletion.CASCADE,
                related_name="retailer_links",
                to="nodes.node",
            ),
        ),
    ]
