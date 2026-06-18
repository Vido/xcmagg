window.retailerLinks = function () {
  return {
    links: [],

    init() {
      const el = document.getElementById("links-data");
      const initial = el ? JSON.parse(el.textContent) : [];
      for (const l of initial) {
        this.links.push({
          id: l.id,
          text: l.text || "",
          url: l.url || "",
          is_affiliate: !!l.is_affiliate,
          promo_code: l.promo_code || "",
          slug: l.slug || "",
          cloak: l.cloak !== undefined ? !!l.cloak : true,
          rel_sponsored: !!l.rel_sponsored,
          rel_nofollow: l.rel_nofollow !== undefined ? !!l.rel_nofollow : true,
          rel_ugc: !!l.rel_ugc,
        });
      }
    },

    add() {
      this.links.push({
        id: "new-" + Math.random().toString(36).slice(2),
        text: "",
        url: "",
        is_affiliate: false,
        promo_code: "",
        slug: "",
        cloak: true,
        rel_sponsored: false,
        rel_nofollow: true,
        rel_ugc: false,
      });
    },

    remove(index) {
      this.links.splice(index, 1);
    },

    // Sensible default: affiliate links should carry rel="sponsored".
    onAffiliateChange(link) {
      if (link.is_affiliate) link.rel_sponsored = true;
    },

    serialized() {
      return JSON.stringify(
        this.links.map(l => {
          const out = {
            text: l.text,
            url: l.url,
            is_affiliate: l.is_affiliate,
            promo_code: l.promo_code,
            slug: l.slug,
            cloak: l.cloak,
            rel_sponsored: l.rel_sponsored,
            rel_nofollow: l.rel_nofollow,
            rel_ugc: l.rel_ugc,
          };
          if (typeof l.id === "number") out.id = l.id;
          return out;
        })
      );
    },
  };
};
