import os
import csv
import struct
import pytest
from lab2_sequential import sequentialFile, Record, FORMAT, RECORD_SIZE

# test_lab2_sequential.py


def generate_csv(path, n):
    header = ['Employee_ID','Employee_Name','Age','Country','Department','Position','Salary','Joining_Date']
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)
        for i in range(1, n + 1):
            writer.writerow([
                i,
                f'Name{i}',
                20 + (i % 45),
                f'Country{i % 7}',
                f'Dep{i % 5}',
                f'Pos{i % 9}',
                float(30000 + (i % 1000)),
                f'2020-01-{(i % 28) + 1:02d}'
            ])

def read_all_records(file_path):
    records = []
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return records
    size = os.path.getsize(file_path)
    with open(file_path, 'rb') as f:
        while f.tell() < size:
            data = f.read(RECORD_SIZE)
            if not data or len(data) < RECORD_SIZE:
                break
            rec = Record.unpack(data)
            records.append(rec)
    return records

@pytest.mark.parametrize("record_count", [10, 100, 1000, 10000])
def test_insertion_search_range_delete(record_count, tmp_path):
    # Arrange
    csv_path = tmp_path / "employee.csv"
    main_file = tmp_path / "employees.dat"
    aux_file = tmp_path / "auxiliary.dat"
    generate_csv(csv_path, record_count)

    sf = sequentialFile(main_file=str(main_file), aux_file=str(aux_file), k=50)

    # Act: mimic insertar_datos()
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            rec = Record(
                Employee_ID=int(row['Employee_ID']),
                Employee_Name=row['Employee_Name'],
                Age=int(row['Age']),
                Country=row['Country'],
                Department=row['Department'],
                Position=row['Position'],
                Salary=float(row['Salary']),
                Joining_Date=row['Joining_Date']
            )
            sf.insert(rec)

    # Sample IDs for tests
    sample_ids = []
    if record_count >= 1:
        sample_ids.append(1)
    if record_count >= 2:
        mid = record_count // 2
        if mid not in sample_ids:
            sample_ids.append(mid)
    if record_count >= 3:
        if record_count not in sample_ids:
            sample_ids.append(record_count)

    # Sequential search BEFORE final reconstruction (some tail records may still be in aux)
    for eid in sample_ids:
        r = sf.search(eid)
        assert r is not None, f"Sequential search failed for {eid} before reconstruction"
        assert r.Employee_ID == eid

    # Reconstruct to consolidate and sort for binary search
    sf.reconstruct_main_file()

    # Binary search AFTER reconstruction
    for eid in sample_ids:
        r = sf.binary_search(eid)
        assert r is not None, f"Binary search failed for {eid} after reconstruction"
        assert r.Employee_ID == eid

    # Range search
    start_id = max(1, record_count // 4)  # falls back to 1 if small
    end_id = min(record_count, start_id + min(25, max(1, record_count // 4)))
    range_results = sf.range_search(start_id, end_id)
    returned_ids = [r.Employee_ID for r in range_results]
    assert returned_ids == list(range(start_id, end_id + 1)), "Range search returned incorrect IDs"

    # Delete a subset (first and last sample ids if distinct)
    delete_ids = list(dict.fromkeys(sample_ids[:2]))  # up to two unique IDs
    for did in delete_ids:
        assert sf.remove(did) is True, f"Failed to mark deletion for {did}"

    # Verify logical deletion (search should now fail)
    for did in delete_ids:
        assert sf.search(did) is None, f"Deleted ID {did} still found via sequential search"

    # Reconstruct to physically purge deleted records
    sf.reconstruct_main_file()

    # Verify deletions persist post reconstruction (binary search)
    for did in delete_ids:
        assert sf.binary_search(did) is None, f"Deleted ID {did} found after reconstruction"

    # Count active records
    active_records = read_all_records(str(main_file))
    for r in active_records:
        assert r.Employee_ID != -1, "Found logically deleted record after reconstruction"
    expected_active = record_count - len(delete_ids)
    assert len(active_records) == expected_active, "Active record count mismatch after deletions"

    # Final integrity: validate ordering
    sorted_ids = sorted(r.Employee_ID for r in active_records)
    assert [r.Employee_ID for r in active_records] == sorted_ids, "Records not sorted by Employee_ID after reconstruction"