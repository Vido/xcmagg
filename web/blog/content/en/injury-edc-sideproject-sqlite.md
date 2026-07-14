---
title: "When a Cyclist Can't Ride"
description: "An elbow injury and a new obsession that turned into technical proof of concepts."
lang: en
publish_date: 2026-01-15
updated_date: 2026-01-15
translation_group: injury-edc-sideproject-sqlite
draft: false
tags: ["edcx", "edc", "indie-hacking", "sqlite", "memcached", "personal"]
author: "Lucas Vido"
---

November 2025. Elbow nerve compression from a multiple day bike trip — cubital tunnel syndrome. No riding for a while — physio's orders.

This sounds minor. It wasn't. I've been cycling obsessively for years. My brain runs on focus objects: things I research at 2am, things I price-track, things I think about when I should be doing something else. The bike was that thing. Without it, my brain immediately started looking for a replacement *surrogate activities*.

It found one: **EDC**.

## The Rabbit Hole

EDC — Every Day Carry. If you're not familiar, think r/EDC on Reddit: people posting photos of what's in their pockets. Knives, flashlights, multitools, wallets, pens. A whole culture of gear that you actually use, every day, that earns its place in your pocket or bag.

I dove fast into this hole. Flashlight spec — lumens, CRI, throw vs flood, tint, driver modes, runtime curves. The endless steel debates — D2 vs VG-10, MagnaCut vs S35VN, and whether CPM-20CV is worth the price premium. The kind of research that feels productive but is mostly just fun.

After a while I figured out that EDC is just like TikTok — it hijacks your brain and gives you empty informational calories in return. <a href="https://www.youtube.com/watch?v=-3-5lbzaQdU" rel="nofollow">Boys emotional support</a>. I felt very guilty spending so much focus on a trap.

## Reddit Isn't Enough

I kept bouncing between Reddit, YouTube, and Telegram groups. r/EDC is great — very active, great pictures.
I noticed there EDC has similar dynamics as the Watch community - there's some high-end stuff with an active second hand marketplace.

But I noticed some gaps:

- The exchange markets (r/EDCexchange, r/Watchexchange) work, but they're detached from the community context.
- No inventory tracking. Every "what's in your collection" post is a write-once photo that disappears in the feed.
- No structured catalog. Reviews live in comment threads. No rating system, no comparison.

I wanted something that combined the community feel of Reddit with actual structure:
gear pages with ratings, user loadouts you can browse and track, a place to advertise gear without a middleman.

## EDCX

I don't have to hide: I felt very guilty spending so much focus and money on EDC.
My thinking process was: I want something useful at the end of this process - redirect this energy into a productive channel.
That's EDCX. Not a store, not a marketplace — a community. Users post pocket dumps, comment, rate gear, build their own inventory.
When someone wants to sell or trade, they post it — EDCX doesn't intermediate, doesn't take cuts, doesn't touch the transaction. Think r/EDCexchange but attached to the actual community that cares about the gear.

Simple idea. Building it lean was the mental exercise my OCD brain craves.

## The Technical Bets

Three bets I'd stand behind:

### SQLite3 all the way

No Postgres, no RDS, no managed database costs. SQLite running on a host bind mount, served by Django.
The database is literally a file on disk. For an early-stage website, this is the right call: near-zero infra cost, zero ops overhead. Backups are just `scp`.
Performance is unmatched - something most people wouldn't assume.

Three PRAGMAs that unlock most of SQLite's performance ceiling:

```sql
PRAGMA journal_mode = WAL;      -- writers don't block readers
PRAGMA mmap_size = 268435456;   -- 256 MB memory-mapped I/O, reads go straight from page cache
PRAGMA synchronous = 1;         -- NORMAL: fsync at checkpoints, not every commit
```

WAL (Write-Ahead Log) flips the default locking model: readers and the single writer run concurrently instead of queuing. `mmap_size` tells SQLite to map the database file directly into process memory — reads skip the syscall overhead entirely. `synchronous = NORMAL` trades a sliver of durability (safe under WAL — only a power loss right at a checkpoint can lose a commit) for far fewer fsync calls. Together they turn a "just a file" database into something that handles real concurrent traffic without breaking a sweat. <a href="https://www.youtube.com/watch?v=yTicYJDT1zE" rel="nofollow">DjangoCon Europe 2023 — Use SQLite in production</a> by Tom Dyson gives a wonderful overview.

### Star architecture — Node and NodeKind

Instead of separate database tables for users, gear items, posts, collections, and transactions, everything is a `Node`. A `NodeKind` distinguishes what it is. Relationships between nodes are just edges in the same graph. Everything is a Node. Node handles identity — shortcodes, slugs, timestamps — and every satellite entity links to it: Photos, Comments, Votes, Transactions. New content type? New `NodeKind`, zero migrations. Many projects walk into a tarpit because their schema grows without a strong conceptual model underneath — this sidesteps that.

### Magiclink OTT + QR

Passwordless authentication. Users get a one-time token link by email — no password to forget, no account to recover.

The wonderful side benefit is an **authentication bridge for mobile**: scan the QR code on desktop, point your camera on mobile, tap — you're in. Onboarding for a gear community shouldn't feel like filling out tax forms. The passwordless path removes friction at the exact moment new users decide whether to stay.

Security overhead? Token invalidation, replay protection — all the usual concerns. Simple answer: store tokens in a cache backend (Memcached here). Cache naturally has TTL, so ephemeral tokens expire on their own. No cleanup jobs, no `used_at` columns, no stale token graveyard in the database.

Same Memcached instance doubles as Django's session backend (`cached_db`) — fast reads from cache, durable writes to SQLite as fallback. Memcached isn't an afterthought — it's the core of how auth works.

## What's Next

EDCX is being built: an outlet for some technical thesis, an outlet for my obsessive focus.
If you're into EDC — or just curious what a knife nerd with Django skills builds when his elbow won't let him ride — come find it.


*PS: <a href="https://www.youtube.com/watch?v=HcMitJpkNnM" rel="nofollow">The EDC dig also works on cyclists.</a>.*
