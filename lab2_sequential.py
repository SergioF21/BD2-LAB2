import struct
import os 
import csv
import time
import random 

#pasos:
# 1.- carga de datos de un archivo csv
#2.- funcion para insertar nuevos registros usando espacio auxiliar. El archivo orginal debe reconstruirse con el espacio
# extra cuando este ultimo exceda k registros
# 3.- funcion de busqueda secuencial por Employee_ID
#4 .- funcion para eliminar un registro por Employee_ID (marcar como eliminado) En la reconstruccion
#    del archivo original no se deben copiar los registros marcados como eliminados logicamente
#5.- funcion para la busqueda por rango que retorne todos los empleados
# entre un rango de Employee_ID especificado

FORMAT = 'i30si20s20s20sf10s'  # Formato para struct (ajustado para los campos)
RECORD_SIZE = struct.calcsize(FORMAT)
K = 5

def time_execution(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

class Record:
    def __init__(self,  Employee_ID, Employee_Name, Age, Country, Department, Position, Salary, Joining_Date):
        self.Employee_ID = Employee_ID
        self.Employee_Name = Employee_Name
        self.Age = Age
        self.Country = Country
        self.Department = Department
        self.Position = Position
        self.Salary = Salary
        self.Joining_Date = Joining_Date
        self.is_deleted = False  

    def pack(self):
        name_bytes = self.Employee_Name.encode('utf-8')[:30].ljust(30, b'\x00')
        country_bytes = self.Country.encode('utf-8')[:20].ljust(20, b'\x00')
        department_bytes = self.Department.encode('utf-8')[:20].ljust(20, b'\x00')
        position_bytes = self.Position.encode('utf-8')[:20].ljust(20, b'\x00')
        joining_date_bytes = self.Joining_Date.encode('utf-8')[:10].ljust(10, b'\x00')
        
        return struct.pack(FORMAT, 
                          self.Employee_ID,           
                          name_bytes,                 
                          self.Age,                   
                          country_bytes,              
                          department_bytes,           
                          position_bytes,             
                          self.Salary,                
                          joining_date_bytes)

    @classmethod
    def unpack(cls, data):
        unpacked_data = struct.unpack(FORMAT, data)
        employee_id = unpacked_data[0]
        employee_name = unpacked_data[1].decode('utf-8').rstrip('\x00')
        age = unpacked_data[2]
        country = unpacked_data[3].decode('utf-8').rstrip('\x00')
        department = unpacked_data[4].decode('utf-8').rstrip('\x00')
        position = unpacked_data[5].decode('utf-8').rstrip('\x00')
        salary = unpacked_data[6]
        joining_date = unpacked_data[7].decode('utf-8').rstrip('\x00')
        return cls(employee_id, employee_name, age, country, department, position, salary, joining_date)
                                                          

class sequentialFile:
    def __init__(self, main_file='employees.dat', aux_file='auxiliary.dat', k=1000):  # Aumentamos K
        self.main_file = main_file
        self.aux_file = aux_file
        self.k = k
        self.record_size = RECORD_SIZE

        for i in [self.main_file, self.aux_file]:
            if not os.path.exists(i):
                with open(i, 'wb'):
                    pass  # Crear archivo vacío

    def is_full(self):
        return os.path.getsize(self.aux_file) // self.record_size >= self.k
    
    def insert(self, record):
        with open(self.aux_file, 'ab') as f:
            f.write(record.pack())
        if self.is_full():
            self.reconstruct_main_file()
    def reconstruct_main_file(self):
        records = []
        
        # Leer archivo principal si existe y no está vacío
        if os.path.exists(self.main_file) and os.path.getsize(self.main_file) > 0:
            with open(self.main_file, 'rb') as f:
                while (data := f.read(self.record_size)):
                    record = Record.unpack(data)
                    if record.Employee_ID != -1:
                        records.append(record)

        # Leer archivo auxiliar si existe y no está vacío
        if os.path.exists(self.aux_file) and os.path.getsize(self.aux_file) > 0:
            with open(self.aux_file, 'rb') as f:
                while (data := f.read(self.record_size)):
                    record = Record.unpack(data)
                    if record.Employee_ID != -1:
                        records.append(record)
                        
        records.sort(key=lambda r: r.Employee_ID)

        # Escribir registros ordenados al archivo principal
        with open(self.main_file, 'wb') as f:
            for record in records:
                f.write(record.pack())
                
        print(f"Archivo principal '{self.main_file}' reconstruido con {len(records)} registros.")

        # Limpiar archivo auxiliar
        with open(self.aux_file, 'wb'):
            pass

        print(f"Archivo auxiliar '{self.aux_file}' limpiado.")

    def search(self, employee_id): # secuencial
        for file in [self.main_file, self.aux_file]:
            with open(file, 'rb') as f:
                while (data := f.read(self.record_size)):
                    record = Record.unpack(data)
                    if record.Employee_ID == employee_id and record.Employee_ID != -1:
                        return record
        return None
    
    def binary_search(self, employee_id):
        with open(self.main_file, 'rb') as f:
            low = 0
            high = os.path.getsize(self.main_file) // self.record_size - 1
            while low <= high:
                mid = (low + high) // 2
                f.seek(mid * self.record_size)
                data = f.read(self.record_size)
                record = Record.unpack(data)
                if record.Employee_ID == employee_id and record.Employee_ID != -1:
                    return record
                elif record.Employee_ID < employee_id:
                    low = mid + 1
                else:
                    high = mid - 1
        return None
    
    def remove(self, employee_id):
        found = False
        for file in [self.main_file, self.aux_file]:
            with open(file, 'r+b') as f:
                while (data := f.tell())< os.path.getsize(file):
                    record_data = f.read(self.record_size)
                    record = Record.unpack(record_data)
                    if record.Employee_ID == employee_id and record.Employee_ID != -1:
                        record.Employee_ID = -1
                        f.seek(data)
                        f.write(record.pack())
                        found = True
                        break
        return found
    
    def range_search(self, start_id, end_id):
        results = []
        for file in [self.main_file, self.aux_file]:
            with open(file, 'rb') as f:
                while (data := f.read(self.record_size)):
                    record = Record.unpack(data)
                    if start_id <= record.Employee_ID <= end_id and record.Employee_ID != -1:
                        results.append(record)
        results.sort(key=lambda r: r.Employee_ID)
        return results

def main():
    sf = sequentialFile()
    all_ids = []

    print("Cargando datos desde employee.csv...")

    def insertar_datos():
        with open('employee.csv', 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            rows = list(reader)
            print(f"Total de filas en CSV: {len(rows)}")
            
            # Limitar a 1000 registros para pruebas iniciales
            rows = rows[:1000]
            print(f"Procesando {len(rows)} registros...")
            
            for i, row in enumerate(rows):
                if i % 100 == 0:  # Mostrar progreso cada 100 registros
                    print(f"Procesando registro {i+1}/{len(rows)}")
                    
                record = Record(
                    Employee_ID=int(row['Employee_ID']),
                    Employee_Name=row['Employee_Name'],
                    Age=int(row['Age']),
                    Country=row['Country'],
                    Department=row['Department'],
                    Position=row['Position'],
                    Salary=float(row['Salary']),
                    Joining_Date=row['Joining_Date']
                )
                sf.insert(record)
                all_ids.append(record.Employee_ID)
    _, insert_time = time_execution(insertar_datos)
    print(f"Datos insertados en {insert_time:.6f} segundos.")
    print(f"Total de registros insertados: {len(all_ids)}")

    print("\nRealizando búsquedas secuenciales...")
    search_ids = random.sample(all_ids, min(10, len(all_ids)))
    _, search_time = time_execution(lambda: [sf.search(eid) for eid in search_ids])
    print(f"Búsquedas completadas en {search_time:.6f} segundos.")

    print("\nRealizando búsquedas binarias...")
    search_ids = random.sample(all_ids, min(10, len(all_ids)))
    _, binary_time = time_execution(lambda: [sf.binary_search(eid) for eid in search_ids])
    print(f"Búsquedas binarias completadas en {binary_time:.6f} segundos.")

    range_pairs = [(min(a, b) , max(a, b)) for a, b in zip(random.choices(all_ids, k=5), random.choices(all_ids, k=5))]

    _, range_time = time_execution(lambda: [sf.range_search(start, end) for start, end in range_pairs])
    print(f"Búsquedas por rango completadas en {range_time:.6f} segundos.")
    delete_ids = random.sample(all_ids, min(5, len(all_ids)))
    _, delete_time = time_execution(lambda: [sf.remove(eid) for eid in delete_ids])
    print(f"Eliminaciones completadas en {delete_time:.6f} segundos.")

if __name__ == "__main__":
    main()