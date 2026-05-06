"""
Local RTI Event Simulator — sends synthetic events to Eventstream endpoint.
Tests the full downstream pipeline: Eventstream → KQL → update policies → typed tables.
Run from local machine (no Fabric/Spark needed).
"""
import json
import random
import uuid
from datetime import datetime, timedelta

import os

from azure.eventhub import EventHubProducerClient, EventData

# Set ES_CONNECTION_STRING env var with the Eventstream custom endpoint
# connection string. Do NOT commit secrets to source control.
ES_CONNECTION_STRING = os.environ.get("ES_CONNECTION_STRING", "")
if not ES_CONNECTION_STRING:
    raise SystemExit(
        "ES_CONNECTION_STRING env var is required. "
        "Get it from Eventstream → custom endpoint → SAS Key Connection String."
    )

BATCH_SIZE = 100
STREAM_BATCHES = 5

# Synthetic reference data
PATIENTS = [f"PAT-{i:04d}" for i in range(1, 51)]
PROVIDERS = [f"PROV-{i:04d}" for i in range(1, 21)]
FACILITIES = [
    {"id": "FAC-0001", "name": "Metro General Hospital", "lat": 42.96, "lon": -85.67},
    {"id": "FAC-0002", "name": "Lakeside Medical Center", "lat": 42.33, "lon": -83.05},
    {"id": "FAC-0003", "name": "Northern Health System", "lat": 43.01, "lon": -85.69},
    {"id": "FAC-0004", "name": "Valley Urgent Care", "lat": 42.73, "lon": -84.56},
]
PAYERS = ["PAYER-001", "PAYER-002", "PAYER-003"]
DIAGNOSIS_CODES = ["Z00.00", "I10", "E11.9", "J06.9", "M54.5", "K21.0"]
PROCEDURE_CODES = ["99213", "99214", "99215", "99283", "99284", "93000"]
CLAIM_TYPES = ["professional", "institutional", "pharmacy"]
ADT_TYPES = ["ADMIT", "DISCHARGE", "TRANSFER"]
ADMISSION_TYPES = ["EMERGENCY", "URGENT", "ELECTIVE"]
MEDICATIONS = [
    {"code": "RX001", "name": "Lisinopril", "class": "ACE Inhibitor"},
    {"code": "RX002", "name": "Metformin", "class": "Biguanide"},
    {"code": "RX003", "name": "Atorvastatin", "class": "Statin"},
    {"code": "RX004", "name": "Omeprazole", "class": "PPI"},
]
CARE_GAP_MEASURES = [
    {"id": "HBA1C", "name": "Hemoglobin A1c Testing"},
    {"id": "BCS", "name": "Breast Cancer Screening"},
    {"id": "CCS", "name": "Cervical Cancer Screening"},
    {"id": "COL", "name": "Colorectal Cancer Screening"},
]


def generate_claims_event():
    now = datetime.utcnow()
    fac = random.choice(FACILITIES)
    fraud_flags = []
    amount = round(random.gauss(400, 200), 2)
    if random.random() < 0.08:
        amount *= random.uniform(3, 8)
        fraud_flags.append("amount_outlier")
    if random.random() < 0.05:
        fraud_flags.append("velocity_burst")
    if random.random() < 0.04:
        fraud_flags.append("upcoding")
    return {
        "_table": "claims_events",
        "event_id": str(uuid.uuid4()),
        "event_timestamp": (now - timedelta(minutes=random.randint(0, 60))).isoformat(),
        "event_type": "CLAIM_SUBMITTED",
        "claim_id": f"CLM-{uuid.uuid4().hex[:8].upper()}",
        "patient_id": random.choice(PATIENTS),
        "provider_id": random.choice(PROVIDERS),
        "facility_id": fac["id"],
        "payer_id": random.choice(PAYERS),
        "diagnosis_code": random.choice(DIAGNOSIS_CODES),
        "procedure_code": random.choice(PROCEDURE_CODES),
        "claim_type": random.choice(CLAIM_TYPES),
        "claim_amount": max(25.0, amount),
        "latitude": fac["lat"] + random.uniform(-0.02, 0.02),
        "longitude": fac["lon"] + random.uniform(-0.02, 0.02),
        "injected_fraud_flags": json.dumps(fraud_flags) if fraud_flags else "",
    }


def generate_adt_event():
    now = datetime.utcnow()
    fac = random.choice(FACILITIES)
    patient = random.choice(PATIENTS)
    has_gaps = random.random() < 0.3
    gap_measure = random.choice(CARE_GAP_MEASURES) if has_gaps else None
    return {
        "_table": "adt_events",
        "event_id": str(uuid.uuid4()),
        "event_timestamp": (now - timedelta(minutes=random.randint(0, 120))).isoformat(),
        "event_type": random.choice(ADT_TYPES),
        "patient_id": patient,
        "facility_id": fac["id"],
        "facility_name": fac["name"],
        "admission_type": random.choice(ADMISSION_TYPES),
        "primary_diagnosis": random.choice(DIAGNOSIS_CODES),
        "latitude": fac["lat"] + random.uniform(-0.01, 0.01),
        "longitude": fac["lon"] + random.uniform(-0.01, 0.01),
        "has_open_care_gaps": has_gaps,
        "open_gap_measures": gap_measure["name"] if gap_measure else "",
    }


def generate_rx_event():
    now = datetime.utcnow()
    med = random.choice(MEDICATIONS)
    return {
        "_table": "rx_events",
        "event_id": str(uuid.uuid4()),
        "event_timestamp": (now - timedelta(minutes=random.randint(0, 180))).isoformat(),
        "event_type": "RX_DISPENSED",
        "patient_id": random.choice(PATIENTS),
        "provider_id": random.choice(PROVIDERS),
        "medication_code": med["code"],
        "medication_name": med["name"],
        "drug_class": med["class"],
        "quantity": random.randint(15, 90),
        "days_supply": random.choice([30, 60, 90]),
        "latitude": 42.96 + random.uniform(-0.05, 0.05),
        "longitude": -85.67 + random.uniform(-0.05, 0.05),
    }


def main():
    print("=" * 60)
    print("  LOCAL RTI EVENT SIMULATOR")
    print("=" * 60)
    print(f"  Endpoint: ...{ES_CONNECTION_STRING[-40:]}")
    print(f"  Batches: {STREAM_BATCHES}, Events/batch: {BATCH_SIZE}")
    print()

    producer = EventHubProducerClient.from_connection_string(ES_CONNECTION_STRING)
    print("  Connected to Eventstream Custom Endpoint")
    print()

    total_sent = 0
    for batch_num in range(1, STREAM_BATCHES + 1):
        events = []
        # Mix: 50% claims, 30% ADT, 20% Rx
        for _ in range(int(BATCH_SIZE * 0.5)):
            events.append(generate_claims_event())
        for _ in range(int(BATCH_SIZE * 0.3)):
            events.append(generate_adt_event())
        for _ in range(int(BATCH_SIZE * 0.2)):
            events.append(generate_rx_event())

        random.shuffle(events)

        batch = producer.create_batch()
        for ev in events:
            data = EventData(json.dumps(ev))
            try:
                batch.add(data)
            except ValueError:
                producer.send_batch(batch)
                batch = producer.create_batch()
                batch.add(data)
        producer.send_batch(batch)

        total_sent += len(events)
        claims_n = sum(1 for e in events if e["_table"] == "claims_events")
        adt_n = sum(1 for e in events if e["_table"] == "adt_events")
        rx_n = sum(1 for e in events if e["_table"] == "rx_events")
        print(f"  Batch {batch_num}/{STREAM_BATCHES}: {len(events)} events "
              f"(claims={claims_n}, adt={adt_n}, rx={rx_n})")

    producer.close()
    print()
    print(f"  DONE — {total_sent} total events sent")
    print()
    print("  Next: Check KQL tables in Healthcare_RTI_Eventhouse")
    print("    rti_all_events → should have all events")
    print("    claims_events, adt_events, rx_events → routed by update policies")
    print("=" * 60)


if __name__ == "__main__":
    main()
