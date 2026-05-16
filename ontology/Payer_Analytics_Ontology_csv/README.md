# Payer Analytics Ontology — CSV Format

CSV-driven companion to `Payer_Analytics_Ontology/` (Fabric ontology item).
Covers the **9 new payer entities** and **14 new relationships** added in
Phase D2 of the payer extension (Plan, Premium, Authorization, Capitation,
Appeal, ProviderContract, HEDISCompliance, StarRating, RiskAdjustment).

## Files

- `entities.csv` — entity_id, name, gold_table, key_column, natural_key
- `relationships.csv` — relationship_id, from/to entity, join columns, cardinality
- `data_bindings.csv` — entity → lakehouse table binding, partition + SCD + refresh mode

## ID conventions

- EntityType IDs: `8000000014`–`8000000022` (continues from existing 8000000001–13).
- RelationshipType IDs: `8200000000000000020`–`8200000000000000033`.
- DataBinding IDs: `b1000001-0001-4000-b001-0000000000<NN>` matching entity NN.

## Source

All entities bind to tables in `lh_gold_curated` produced by
`workspace/06b_Gold_Transform_Load_v2.Notebook` (Phase C3-Gold).

## Mapping to Fabric ontology item

If/when these entities are imported into the JSON-format
`Payer_Analytics_Ontology/`, generate `EntityTypes/<id>/definition.json` +
`RelationshipTypes/<id>/definition.json` from these CSVs and append matching
paths to `manifest.json` `exportedParts`.
