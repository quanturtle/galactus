"""Step 3: LLM refinement of silver.canonical_products (cluster-based).

For each unrefined seed canonical, build a small cluster of similar names via
pg_trgm, then ask the local MLX server to PARTITION the cluster into groups
where each group is one true product. Apply per group: collapse members to a
survivor, delete losers; stamp ``llm_refined_at`` on every canonical the LLM
saw.

Why cluster-based instead of pairwise: the model sees the full neighborhood
in one call, so it can recognize "these 5 names are 2 distinct products"
instead of chaining bad merges across consecutive pairwise decisions.
"""

import json
import logging

import httpx

from galactus import db, llm

logger = logging.getLogger(__name__)


SIM_LOW = 0.55
SIM_HIGH = 0.95
# 8 keeps the LLM token budget healthy and matches the MAX neighbors we ask
# pg_trgm to return per seed; the JSON repair pass in galactus.llm handles
# the missing-close-bracket pattern that gemma-3-4b emits at this size.
MAX_CLUSTER = 8


REFINE_SYSTEM = (
    "You partition Paraguayan supermarket product-name variants into groups "
    "where each group is ONE product. Output strictly ONE JSON object. "
    "No prose, no explanations, no code fences."
)

REFINE_USER_TMPL = (
    'Cluster the product names below: put names that refer to the SAME PRODUCT '
    'into the same group. Names in different groups MUST be different products.\n\n'
    'Output ONE JSON object:\n'
    '{{"groups": [{{"canonical": "<clean name>", "members": ["<exact input name>", ...]}}, ...]}}\n\n'
    'Each "members" entry MUST be copied verbatim from the input list (same '
    'casing, punctuation, whitespace).\n\n'
    'IGNORE these differences when matching — they are formatting noise, NOT '
    'product differences:\n'
    '- Case (UPPER vs lower vs Mixed)\n'
    '- Accents (Limon == Limón, Acordeon == Acordeón)\n'
    '- Punctuation (KG. vs KG, "C/Queso" vs "C / Queso")\n'
    '- Whitespace and word order\n'
    '- Unit/quantity SUFFIXES that name the unit being sold:\n'
    '    * "X KG", "x Kg.", "X KL" (typo for KG), "por kilo", "1 KG", "1KG"\n'
    '    * "X UN", "x Un.", "POR UNIDAD", "X UNIDAD", "(UN)"\n'
    '    * "X MAZO", "X PAQ", "POR PAQUETE"\n'
    '  These are the unit being sold, not a SIZE — ignore them.\n'
    '- Parenthetical SKU codes like "(1167)", "(249)", "(8549)" at start/end\n'
    '- Trailing brand qualifiers when same brand: "Marca X" suffix\n\n'
    'TREAT AS DIFFERENT PRODUCTS when names disagree on:\n'
    '- Protein: vacuna, cerdo, pollo, cordero, pescado, pavo, atun\n'
    '- Cut or preparation: costeleta vs milanesa vs filete vs lomo vs costilla; '
    'pechuga vs muslo vs ala; entera vs trozada\n'
    '- Brand (Coca-Cola/Pepsi, Nivea/Dove, Heinz/Sun, Ochsi/Perdigao)\n'
    '- NUMERIC quantity that names PACKAGE SIZE: 500ml vs 1L, 90g vs 180g, '
    '"x 2" vs "x 4", 1kg vs 2kg, 500g vs 567g (these refer to how much '
    'product is INSIDE the package, not the unit it is sold in)\n'
    '- Flavor: frutilla vs vainilla vs chocolate, manzana vs naranja, '
    'limon vs frutilla\n'
    '- Variant: light vs regular, zero vs normal, sin sal vs con sal, '
    'c/pic vs s/pic, integral vs blanco, aclarado vs natural, '
    'CON COCO vs without coco, importado vs nacional\n'
    '- Form factor: aerosol vs roll-on, polvo vs liquido, lata vs botella\n'
    '- Sub-product within a category: Pan Felipe vs Pan Salvado vs Pan Cañon '
    'are DIFFERENT breads (treat the qualifier word as a real distinguisher)\n\n'
    'EVERY input name MUST appear in EXACTLY ONE group, copied verbatim. '
    'A group of size 1 is fine.\n\n'
    'Worked examples:\n\n'
    'Input:\n'
    '- Poroto Peky\n'
    '- Poroto Peky por kilo\n'
    'Output:\n'
    '{{"groups": [{{"canonical": "Poroto Peky x Kg", '
    '"members": ["Poroto Peky", "Poroto Peky por kilo"]}}]}}\n'
    '(merge: "por kilo" is just the unit-being-sold suffix, not a size diff)\n\n'
    'Input:\n'
    '- JAMON COCIDO OCHSI KG.\n'
    '- JAMON COCIDO OCHSI\n'
    '- JAMON COCIDO PERDIGAO\n'
    '- COSTELETA VACUNA X KG\n'
    '- COSTELETA DE CERDO X KG\n'
    '- MILANESA DE CERDO X KG\n'
    'Output:\n'
    '{{"groups": ['
    '{{"canonical": "Jamón Cocido Ochsi x Kg", '
    '"members": ["JAMON COCIDO OCHSI KG.", "JAMON COCIDO OCHSI"]}}, '
    '{{"canonical": "Jamón Cocido Perdigao x Kg", '
    '"members": ["JAMON COCIDO PERDIGAO"]}}, '
    '{{"canonical": "Costeleta Vacuna x Kg", '
    '"members": ["COSTELETA VACUNA X KG"]}}, '
    '{{"canonical": "Costeleta de Cerdo x Kg", '
    '"members": ["COSTELETA DE CERDO X KG"]}}, '
    '{{"canonical": "Milanesa de Cerdo x Kg", '
    '"members": ["MILANESA DE CERDO X KG"]}}'
    ']}}\n'
    '(jamon ochsi 1 and 2 merge — only differ by "KG." suffix. '
    'Different brands, different proteins, different cuts stay separate.)\n\n'
    'Now partition this list:\n'
    '{names}'
)


SET_THRESHOLD_SQL = f"SET LOCAL pg_trgm.similarity_threshold = {SIM_LOW};"


# Pick unrefined seed canonicals (oldest-first so we don't reshuffle priorities).
SEEDS_SQL = """
SELECT id, canonical_name
FROM silver.canonical_products
WHERE llm_refined_at IS NULL
ORDER BY id
LIMIT %(limit)s;
"""


# Direct neighbors of a given seed (within similarity range, excluding self).
NEIGHBORS_SQL = """
SELECT b.id, b.canonical_name
FROM silver.canonical_products b
WHERE b.canonical_name %% %(seed_name)s
  AND b.id <> %(seed_id)s
  AND similarity(b.canonical_name, %(seed_name)s) <= %(high)s
ORDER BY similarity(b.canonical_name, %(seed_name)s) DESC
LIMIT %(neighbors)s;
"""


# Confirm rows still exist before mutating (a previous cluster in this batch
# may have already swallowed some of these ids).
EXISTING_SQL = """
SELECT id FROM silver.canonical_products WHERE id = ANY(%(ids)s);
"""


MERGE_MEMBERS_SQL = """
UPDATE silver.canonical_product_members
SET canonical_product_id = %(survivor)s,
    match_method = 'llm',
    matched_at = NOW()
WHERE canonical_product_id = ANY(%(losers)s);
"""

UPDATE_SURVIVOR_SQL = """
UPDATE silver.canonical_products
SET canonical_name = %(name)s,
    llm_refined_at = NOW(),
    updated_at = NOW()
WHERE id = %(id)s;
"""

DELETE_LOSERS_SQL = "DELETE FROM silver.canonical_products WHERE id = ANY(%(ids)s);"

MARK_REFINED_SQL = """
UPDATE silver.canonical_products
SET llm_refined_at = NOW()
WHERE id = ANY(%(ids)s);
"""


async def _build_cluster(seed: dict) -> list[dict]:
    """Return [seed, *neighbors] capped at MAX_CLUSTER, deduped by id."""
    rows = await db.execute(
        NEIGHBORS_SQL,
        {
            "seed_id": seed["id"],
            "seed_name": seed["canonical_name"],
            "high": SIM_HIGH,
            "neighbors": MAX_CLUSTER - 1,
        },
    )
    return [seed, *rows]


def _format_names(cluster: list[dict]) -> str:
    return "\n".join(f"- {c['canonical_name']}" for c in cluster)


def _norm_for_match(s: str) -> str:
    """Aggressive normalization: strip non-alphanumerics, lower-case.

    Used as a tie-breaker when small models drop a trailing period or change
    casing in a "verbatim" member string.
    """
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _validate_partition(parsed, cluster: list[dict]) -> list[dict] | None:
    """Return list of {canonical, member_ids:[id]} on success, None on bad shape.

    Tolerates four common small-model failures:
    - **Dropped duplicates:** if the LLM omits a near-duplicate input (treats
      it as already-listed), we add it back as its own singleton group.
    - **Split-same-canonical:** if two groups share an identical canonical
      string (case-insensitive), we merge them.
    - **Almost-verbatim member strings:** if the model dropped a trailing
      period or changed case, fall back to normalized lookup (case-fold,
      strip non-alphanumerics).
    - **Genuinely hallucinated members:** drop them silently rather than
      bailing on the whole cluster.

    Rejects only on structural breakage (bad JSON shape, no groups at all).
    """
    if not isinstance(parsed, dict):
        return None
    groups = parsed.get("groups")
    if not isinstance(groups, list) or not groups:
        return None

    exact = {c["canonical_name"]: c["id"] for c in cluster}
    fuzzy: dict[str, int] = {}
    for c in cluster:
        fuzzy.setdefault(_norm_for_match(c["canonical_name"]), c["id"])

    seen_ids: set[int] = set()
    out: list[dict] = []
    for g in groups:
        if not isinstance(g, dict):
            return None
        canonical = (g.get("canonical") or "").strip()
        members = g.get("members")
        if not canonical or not isinstance(members, list) or not members:
            return None
        member_ids: list[int] = []
        for m in members:
            if not isinstance(m, str):
                continue
            cid = exact.get(m) or fuzzy.get(_norm_for_match(m))
            if cid is None or cid in seen_ids:
                continue  # hallucinated or already-claimed; skip silently
            seen_ids.add(cid)
            member_ids.append(cid)
        if member_ids:
            out.append({"canonical": canonical, "member_ids": member_ids})

    # Recovery: any cluster id the LLM didn't assign becomes its own group.
    for c in cluster:
        if c["id"] not in seen_ids:
            out.append({
                "canonical": c["canonical_name"], "member_ids": [c["id"]],
            })

    # Coalesce groups with identical canonical strings (case/whitespace folded).
    bucket: dict[str, dict] = {}
    for g in out:
        key = " ".join(g["canonical"].lower().split())
        if key in bucket:
            bucket[key]["member_ids"].extend(g["member_ids"])
        else:
            bucket[key] = {
                "canonical": g["canonical"],
                "member_ids": list(g["member_ids"]),
            }
    return list(bucket.values()) or None


async def _propose_for_cluster(
    cluster: list[dict], http: httpx.AsyncClient,
) -> list[dict] | None:
    """Send cluster to LLM, return validated partition (groups). None on failure.

    A "group" is {canonical: str, member_ids: [int]}; a group with len>1 is
    a proposed merge.
    """
    n = len(cluster)
    user_msg = REFINE_USER_TMPL.format(names=_format_names(cluster))
    try:
        out = await llm.chat_json(
            REFINE_SYSTEM, user_msg, client=http,
            # Each group's JSON is ~80-120 tokens (canonical + members array);
            # budget covers a fat partition where every name lands in its
            # own singleton, plus headroom for verbose canonicals.
            max_tokens=256 + 200 * n,
        )
    except Exception as exc:
        logger.warning(
            "llm_refine: error on cluster seed=%s size=%d: %s",
            cluster[0]["id"], n, exc,
        )
        return None
    if isinstance(out, list) and out and isinstance(out[0], dict):
        out = out[0]
    return _validate_partition(out, cluster)


async def _apply_partition(cluster: list[dict], partition: list[dict]) -> int:
    """Apply a validated partition to the DB. Returns merges performed."""
    cluster_ids = [c["id"] for c in cluster]
    merges = 0
    async with db.transaction() as tx:
        # Drop ids that were destroyed by an earlier cluster in this batch.
        existing = await db.execute(
            EXISTING_SQL, {"ids": cluster_ids}, conn=tx,
        )
        alive = {r["id"] for r in existing}
        if not alive:
            return 0

        for group in partition:
            group_ids = [cid for cid in group["member_ids"] if cid in alive]
            if not group_ids:
                continue
            survivor = group_ids[0]
            losers = group_ids[1:]
            if losers:
                await db.execute(
                    MERGE_MEMBERS_SQL,
                    {"survivor": survivor, "losers": losers}, conn=tx,
                )
                await db.execute(
                    DELETE_LOSERS_SQL, {"ids": losers}, conn=tx,
                )
                merges += len(losers)
            await db.execute(
                UPDATE_SURVIVOR_SQL,
                {"name": group["canonical"], "id": survivor}, conn=tx,
            )
    return merges


def _print_mapping_report(mapping: list[dict]) -> None:
    """Print a stable, easy-to-scan unified-name -> original-names report."""
    if not mapping:
        print("(no merges proposed)")
        return
    # Width-cap unified col so wide tables stay readable.
    name_w = min(50, max(len(m["unified_name"]) for m in mapping))
    print()
    print(f"{'unified_name':<{name_w}}  original_names")
    print(f"{'-' * name_w}  {'-' * 60}")
    # Stable order: largest groups first, then alphabetical.
    sorted_map = sorted(
        mapping,
        key=lambda m: (-len(m["original_names"]), m["unified_name"]),
    )
    for m in sorted_map:
        unified = m["unified_name"][:name_w]
        joined = ", ".join(m["original_names"])
        print(f"{unified:<{name_w}}  {joined}")
    print()
    print(f"total: {len(mapping)} merge groups, "
          f"{sum(len(m['original_names']) for m in mapping)} canonicals collapsed "
          f"into {len(mapping)} unified names")


async def run(
    *, conn=None, limit: int = 200, dry_run: bool = False,
) -> int:
    """Run cluster-based LLM refinement.

    Each unrefined seed is grown into a small cluster of similar names via
    pg_trgm; the LLM partitions each cluster into groups; groups of size > 1
    become proposed merges. A combined report is printed at the end.

    ``limit`` is the number of seed clusters to process (NOT pair count).
    Worst case names touched is ``limit * MAX_CLUSTER``.

    ``dry_run`` skips both DB writes and the ``llm_refined_at`` stamp, so the
    same seeds will be re-proposed on the next run. Use it to preview what
    the model would merge before committing.

    ``conn`` is accepted for interface symmetry but ignored — this step opens
    its own short-lived transactions per cluster so a single MLX failure does
    not kill the whole batch.
    """
    async with db.transaction() as tx:
        await db.execute(SET_THRESHOLD_SQL, conn=tx)
        seeds = await db.execute(SEEDS_SQL, {"limit": limit}, conn=tx)
    if not seeds:
        logger.info("llm_refine: no unrefined canonicals")
        return 0

    logger.info(
        "llm_refine: %d seed canonicals%s",
        len(seeds), " (dry-run)" if dry_run else "",
    )
    timeout = llm.llm_timeout()
    mapping: list[dict] = []  # unified_name -> [original_names]
    # In dry-run we don't actually delete losers, so emulate the
    # "first cluster wins" behaviour to keep the report deduped.
    claimed: set[int] = set()
    total_merges = 0
    failed = 0

    async with httpx.AsyncClient(timeout=timeout) as http:
        for i, seed in enumerate(seeds, 1):
            if dry_run and seed["id"] in claimed:
                continue

            still_there = await db.execute(
                EXISTING_SQL, {"ids": [seed["id"]]},
            )
            if not still_there:
                continue

            fresh = await db.execute(
                "SELECT id, canonical_name, llm_refined_at "
                "FROM silver.canonical_products WHERE id = %(id)s",
                {"id": seed["id"]},
            )
            if not fresh:
                continue
            seed_now = fresh[0]
            if seed_now["llm_refined_at"] is not None:
                continue

            cluster = await _build_cluster(seed_now)
            partition = await _propose_for_cluster(cluster, http)

            if partition is None:
                failed += 1
                if not dry_run:
                    await db.execute(
                        MARK_REFINED_SQL,
                        {"ids": [c["id"] for c in cluster]},
                    )
                continue

            id_to_name = {c["id"]: c["canonical_name"] for c in cluster}
            for group in partition:
                # In dry-run mode, drop ids that an earlier cluster's group
                # already claimed; in real mode the DB enforces the same via
                # EXISTING_SQL inside _apply_partition.
                ids = [
                    mid for mid in group["member_ids"]
                    if not dry_run or mid not in claimed
                ]
                if dry_run:
                    claimed.update(ids)
                if len(ids) > 1:
                    mapping.append({
                        "unified_name": group["canonical"],
                        "original_names": [
                            id_to_name[mid] for mid in ids if mid in id_to_name
                        ],
                        "member_ids": ids,
                    })

            if not dry_run:
                total_merges += await _apply_partition(cluster, partition)

            if i % 50 == 0:
                logger.info(
                    "llm_refine: %d/%d seeds, %d proposed merges so far",
                    i, len(seeds), len(mapping),
                )

    logger.info(
        "llm_refine: %d seed clusters, %d merge groups, "
        "%d canonicals collapsed, %d failed%s",
        len(seeds), len(mapping),
        sum(len(m["original_names"]) for m in mapping),
        failed,
        " (dry-run, no DB changes)" if dry_run else "",
    )
    _print_mapping_report(mapping)
    return total_merges if not dry_run else len(mapping)
