from django.db import migrations, models


def migrate_bike_to_parts(apps, schema_editor):
    Category = apps.get_model("catalog", "Category")
    Category.objects.filter(audience="bike").update(audience="parts")


def reverse_parts_to_bike(apps, schema_editor):
    Category = apps.get_model("catalog", "Category")
    Category.objects.filter(audience="parts").update(audience="bike")


class Migration(migrations.Migration):

    dependencies = [
        ("catalog", "0007_category_audience_category_durability"),
    ]

    operations = [
        migrations.AlterField(
            model_name="category",
            name="audience",
            field=models.CharField(
                blank=True,
                choices=[
                    ("bike", "Bicycle"),
                    ("parts", "Parts & Accessories"),
                    ("rider", "Rider Gear"),
                ],
                max_length=16,
            ),
        ),
        migrations.RunPython(migrate_bike_to_parts, reverse_parts_to_bike),
    ]
