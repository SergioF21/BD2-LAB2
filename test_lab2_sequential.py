import os
import csv
import struct
import tempfile
import shutil
import unittest
import time

from lab2_sequential import sequentialFile, Record, FORMAT, RECORD_SIZE

TIMES_CSV = "test_times_sequential_ms.csv"

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

def save_times(record_count, times):
    file_exists = os.path.exists(TIMES_CSV)
    with open(TIMES_CSV, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Records", "Insertion(ms)", "Sequential_Search(ms)", "Reconstruction(ms)",
                "Binary_Search(ms)", "Range_Search(ms)", "Deletion(ms)"
            ])
        writer.writerow([
            record_count,
            times["insertion"] * 1000,
            times["sequential_search"] * 1000,
            times["reconstruction"] * 1000,
            times["binary_search"] * 1000,
            times["range_search"] * 1000,
            times["deletion"] * 1000
        ])

class TestSequentialFile(unittest.TestCase):
    def run_full_test(self, record_count):
        tmp_dir = tempfile.mkdtemp()
        times = {}
        try:
            print(f"\n--- Testing with {record_count} records ---")
            csv_path = os.path.join(tmp_dir, "employee.csv")
            main_file = os.path.join(tmp_dir, "employees.dat")
            aux_file = os.path.join(tmp_dir, "auxiliary.dat")
            generate_csv(csv_path, record_count)

            sf = sequentialFile(main_file=main_file, aux_file=aux_file, k=50)

            # Insert records
            t0 = time.time()
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
            t1 = time.time()
            times["insertion"] = t1-t0

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

            # Sequential search BEFORE final reconstruction
            t0 = time.time()
            for eid in sample_ids:
                r = sf.search(eid)
                self.assertIsNotNone(r, f"Sequential search failed for {eid} before reconstruction")
                self.assertEqual(r.Employee_ID, eid)
            t1 = time.time()
            times["sequential_search"] = t1-t0

            # Reconstruct to consolidate and sort for binary search
            t0 = time.time()
            sf.reconstruct_main_file()
            t1 = time.time()
            times["reconstruction"] = t1-t0

            # Binary search AFTER reconstruction
            t0 = time.time()
            for eid in sample_ids:
                r = sf.binary_search(eid)
                self.assertIsNotNone(r, f"Binary search failed for {eid} after reconstruction")
                self.assertEqual(r.Employee_ID, eid)
            t1 = time.time()
            times["binary_search"] = t1-t0

            # Range search
            start_id = max(1, record_count // 4)
            end_id = min(record_count, start_id + min(25, max(1, record_count // 4)))
            t0 = time.time()
            range_results = sf.range_search(start_id, end_id)
            t1 = time.time()
            returned_ids = [r.Employee_ID for r in range_results]
            self.assertEqual(returned_ids, list(range(start_id, end_id + 1)), "Range search returned incorrect IDs")
            times["range_search"] = t1-t0

            # Delete a subset (first and last sample ids if distinct)
            delete_ids = list(dict.fromkeys(sample_ids[:2]))
            t0 = time.time()
            for did in delete_ids:
                self.assertTrue(sf.remove(did), f"Failed to mark deletion for {did}")
            t1 = time.time()
            times["deletion"] = t1-t0

            # Verify logical deletion (search should now fail)
            for did in delete_ids:
                self.assertIsNone(sf.search(did), f"Deleted ID {did} still found via sequential search")

            # Reconstruct to physically purge deleted records
            sf.reconstruct_main_file()

            # Verify deletions persist post reconstruction (binary search)
            for did in delete_ids:
                self.assertIsNone(sf.binary_search(did), f"Deleted ID {did} found after reconstruction")

            # Count active records
            active_records = read_all_records(main_file)
            for r in active_records:
                self.assertNotEqual(r.Employee_ID, -1, "Found logically deleted record after reconstruction")
            expected_active = record_count - len(delete_ids)
            self.assertEqual(len(active_records), expected_active, "Active record count mismatch after deletions")

            # Final integrity: validate ordering
            sorted_ids = sorted(r.Employee_ID for r in active_records)
            self.assertEqual([r.Employee_ID for r in active_records], sorted_ids, "Records not sorted by Employee_ID after reconstruction")

            # Save times for this test
            save_times(record_count, times)
            print(f"Times for {record_count} records: {times}")
        finally:
            shutil.rmtree(tmp_dir)

    def test_10_records(self):
        self.run_full_test(10)

    def test_20_records(self):
        self.run_full_test(20)

    def test_50_records(self):
        self.run_full_test(50)

    def test_100_records(self):
        self.run_full_test(100)

    def test_200_records(self):
        self.run_full_test(200)

    def test_500_records(self):
        self.run_full_test(500)

    def test_1000_records(self):
        self.run_full_test(1000)

    def test_2000_records(self):
        self.run_full_test(2000)

    def test_5000_records(self):
        self.run_full_test(5000)

    def test_10000_records(self):
        self.run_full_test(10000)

if __name__ == "__main__":
    unittest.main()