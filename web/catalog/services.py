import json
from dataclasses import dataclass
from typing import List, Optional

from django.db import transaction

from catalog.models import RetailerLink
from linkcloak.models import Link


@dataclass
class RetailerLinkData:
    text: str
    url: str
    is_affiliate: bool = False
    promo_code: str = ""
    order: int = 0
    id: Optional[int] = None
    slug: str = ""
    cloak: bool = True
    rel_sponsored: bool = False
    rel_nofollow: bool = True
    rel_ugc: bool = False
    utm_source: str = ""
    utm_medium: str = ""
    utm_campaign: str = ""
    utm_term: str = ""
    utm_content: str = ""

    @classmethod
    def parse_list(cls, data: str) -> List["RetailerLinkData"]:
        try:
            raw_list = json.loads(data or "[]")
        except json.JSONDecodeError:
            raw_list = []

        for order, obj in enumerate(raw_list):
            url = (obj.get("url") or "").strip()
            if not url:
                continue

            raw_id = obj.get("id")
            try:
                link_id = int(raw_id)
            except (TypeError, ValueError):
                link_id = None

            is_affiliate = bool(obj.get("is_affiliate", False))
            yield cls(
                id=link_id,
                text=(obj.get("text") or "").strip(),
                url=url,
                is_affiliate=is_affiliate,
                promo_code=(obj.get("promo_code") or "").strip(),
                order=order,
                slug=(obj.get("slug") or "").strip(),
                cloak=bool(obj.get("cloak", True)),
                rel_sponsored=bool(obj.get("rel_sponsored", is_affiliate)),
                rel_nofollow=bool(obj.get("rel_nofollow", True)),
                rel_ugc=bool(obj.get("rel_ugc", False)),
                utm_source=(obj.get("utm_source") or "").strip(),
                utm_medium=(obj.get("utm_medium") or "").strip(),
                utm_campaign=(obj.get("utm_campaign") or "").strip(),
                utm_term=(obj.get("utm_term") or "").strip(),
                utm_content=(obj.get("utm_content") or "").strip(),
            )


class RetailerLinkService:

    @staticmethod
    @transaction.atomic
    def sync(*, node, links_json):
        incoming = list(RetailerLinkData.parse_list(links_json))
        keep_ids = {ld.id for ld in incoming if ld.id is not None}

        # Drop removed links (and their Link rows — cascade is Link -> RetailerLink).
        stale = node.retailer_links.exclude(id__in=keep_ids)
        stale_link_ids = list(stale.values_list("link_id", flat=True))
        stale.delete()
        Link.objects.filter(id__in=stale_link_ids).delete()

        existing = {
            rl.pk: rl
            for rl in node.retailer_links.filter(id__in=keep_ids).select_related("link")
        }

        for ld in incoming:
            rl = existing.get(ld.id)
            if rl is None:
                link = Link(
                    target_url=ld.url,
                    slug=ld.slug,
                    cloak=ld.cloak,
                    rel_sponsored=ld.rel_sponsored,
                    rel_nofollow=ld.rel_nofollow,
                    rel_ugc=ld.rel_ugc,
                    utm_source=ld.utm_source,
                    utm_medium=ld.utm_medium,
                    utm_campaign=ld.utm_campaign,
                    utm_term=ld.utm_term,
                    utm_content=ld.utm_content,
                )
                link.slug_hint = ld.text
                link.save()

                RetailerLink.objects.create(
                    node=node,
                    link=link,
                    label="",
                    text=ld.text,
                    is_affiliate=ld.is_affiliate,
                    promo_code=ld.promo_code,
                    order=ld.order,
                )
                continue

            rl.text = ld.text
            rl.is_affiliate = ld.is_affiliate
            rl.promo_code = ld.promo_code
            rl.order = ld.order
            rl.save(update_fields=["text", "is_affiliate", "promo_code", "order"])

            link = rl.link
            link.target_url = ld.url
            link.cloak = ld.cloak
            link.rel_sponsored = ld.rel_sponsored
            link.rel_nofollow = ld.rel_nofollow
            link.rel_ugc = ld.rel_ugc
            link.utm_source = ld.utm_source
            link.utm_medium = ld.utm_medium
            link.utm_campaign = ld.utm_campaign
            link.utm_term = ld.utm_term
            link.utm_content = ld.utm_content
            if ld.slug:
                link.slug = ld.slug
            link.save()

    @staticmethod
    def json_links(node):
        return [
            {
                "id": rl.id,
                "text": rl.text,
                "url": rl.link.target_url,
                "is_affiliate": rl.is_affiliate,
                "promo_code": rl.promo_code,
                "order": rl.order,
                "slug": rl.link.slug,
                "cloak": rl.link.cloak,
                "rel_sponsored": rl.link.rel_sponsored,
                "rel_nofollow": rl.link.rel_nofollow,
                "rel_ugc": rl.link.rel_ugc,
                "utm_source": rl.link.utm_source,
                "utm_medium": rl.link.utm_medium,
                "utm_campaign": rl.link.utm_campaign,
                "utm_term": rl.link.utm_term,
                "utm_content": rl.link.utm_content,
            }
            for rl in node.retailer_links.select_related("link").order_by("order", "id")
        ]
