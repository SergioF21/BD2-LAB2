import struct
import os 
import csv
import time
import random 

FORMAT = 'i30si20s20s20sf10s'  # Formato para struct (ajustado para los campos)
RECORD_SIZE = struct.calcsize(FORMAT)

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

class AVLNode:
    def __init__(self, employee_id, record_pos):
        self.employee_id = employee_id  # Llave (ID del empleado)
        self.record_pos = record_pos    # Posición del registro en el archivo
        self.height = 1                 # Altura del nodo
        self.left = None               # Puntero izquierdo
        self.right = None              # Puntero derecho
    

class AVLFile:
    def __init__(self, data_file='employees_avl.dat'):
        self.root = None
        self.data_file = data_file
        self.record_size = RECORD_SIZE
        
        # Crear archivo de datos si no existe
        if not os.path.exists(self.data_file):
            with open(self.data_file, 'wb'):
                pass

    def get_height(self, node):
        if not node:
            return 0
        return node.height

    def get_balance(self, node):
        if not node:
            return 0
        return self.get_height(node.left) - self.get_height(node.right)

    def update_height(self, node):
        if node:
            node.height = 1 + max(self.get_height(node.left), self.get_height(node.right))

    def insert(self, record):
        # insertr registro en archivo y pos
        with open(self.data_file, 'ab') as f:
            record_pos = f.tell()
            f.write(record.pack())
        
        # insert nodo
        self.root = self._insert_node(self.root, record.Employee_ID, record_pos)

    def _insert_node(self, node, employee_id, record_pos):
        # nuevo nodo
        if not node:
            return AVLNode(employee_id, record_pos)
        
        # inset recursivo
        if employee_id < node.employee_id:
            node.left = self._insert_node(node.left, employee_id, record_pos)
            node = self.rebalance(node)
        elif employee_id > node.employee_id:
            node.right = self._insert_node(node.right, employee_id, record_pos)
            node = self.rebalance(node)
        else:
            # existe id, entonces actualizamos
            node.record_pos = record_pos
            return node

        # upd altura
        self.update_height(node)
        
        return node

    def rotate_right(self, y):
        x = y.left
        T2 = x.right

        # Rotación
        x.right = y
        y.left = T2

        # Actualizar alturas
        self.update_height(y)
        self.update_height(x)

        return x
    
    def rotate_left(self, x):
        y = x.right
        T2 = y.left

        # Rotación
        y.left = x
        x.right = T2

        # Actualizar alturas
        self.update_height(x)
        self.update_height(y)

        return y

    def rebalance(self, node):
        balance = self.get_balance(node)
        # caso izquierda-izquierda
        if balance > 1 and self.get_balance(node.left) >= 0:
            print("Rebalanceo: izquierda-izquierda")
            return self.rotate_right(node)
        # caso derecha-derecha
        if balance < -1 and self.get_balance(node.right) <= 0:
            print("Rebalanceo: derecha-derecha")
            return self.rotate_left(node)
        # caso izquierda-derecha
        if balance > 1 and self.get_balance(node.left) < 0:
            node.left = self.rotate_left(node.left)
            print("Rebalanceo: izquierda-derecha")
            return self.rotate_right(node)
        # caso derecha-izquierda
        if balance < -1 and self.get_balance(node.right) > 0:
            node.right = self.rotate_right(node.right)
            print("Rebalanceo: derecha-izquierda")
            return self.rotate_left(node)
        return node

    def read_record_from_file(self, pos):
        with open(self.data_file, 'rb') as f:
            f.seek(pos)
            data = f.read(self.record_size)
            if data:
                return Record.unpack(data)
        return None

    def search(self, employee_id):
        node = self._search_node(self.root, employee_id)
        if node:
            return self.read_record_from_file(node.record_pos)
        return None

    def _search_node(self, node, employee_id):
        if not node or node.employee_id == employee_id:
            return node
        
        if employee_id < node.employee_id:
            return self._search_node(node.left, employee_id)
        else:
            return self._search_node(node.right, employee_id)
    def delete(self, employee_id):
        self.root = self._delete_node(self.root, employee_id)

    def _delete_node(self, node, employee_id):
        if not node:
            return node
        if employee_id < node.employee_id:
            node.left = self._delete_node(node.left, employee_id)
        elif employee_id > node.employee_id:
            node.right = self._delete_node(node.right, employee_id)
        else:
            # Nodo con un solo hijo o sin hijos
            if not node.left:
                return node.right
            elif not node.right:
                return node.left
            # Nodo con dos hijos: obtener el sucesor inorder (el más pequeño en el subárbol derecho)
            temp = self.get_min_value_node(node.right)
            node.employee_id = temp.employee_id
            node.record_pos = temp.record_pos
            node.right = self._delete_node(node.right, temp.employee_id)
        self.update_height(node)
        return self.rebalance(node)
    def get_min_value_node(self, node):
        if node is None or node.left is None:
            return node
        return self.get_min_value_node(node.left)

def main():
    avl = AVLFile()
    all_ids = []

    print("Cargando datos desde employee.csv usando AVL Tree...")
    print("Solo probando INSERT y SEARCH (funciones simplificadas)")

    def insertar_datos():
        with open('employee.csv', 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            rows = list(reader)
            print(f"Total de filas en CSV: {len(rows)}")
            
            rows = rows[:100]
            print(f"Procesando {len(rows)} registros...")
            
            for i, row in enumerate(rows):
                if i % 20 == 0:  # Mostrar progreso cada 20 registros
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
                avl.insert(record)
                all_ids.append(record.Employee_ID)

    _, insert_time = time_execution(insertar_datos)
    print(f"Datos insertados en {insert_time:.6f} segundos.")
    print(f"Total de registros insertados: {len(all_ids)}")

    print("\nRealizando búsquedas en AVL Tree...")
    search_ids = random.sample(all_ids, min(5, len(all_ids)))
    print(f"Buscando IDs: {search_ids}")
    
    for eid in search_ids:
        result = avl.search(eid)
        if result:
            print(f"✓ Encontrado ID {eid}: {result.Employee_Name}")
        else:
            print(f"✗ No encontrado ID {eid}")

    _, search_time = time_execution(lambda: [avl.search(eid) for eid in search_ids])
    print(f"Búsquedas completadas en {search_time:.6f} segundos.")

if __name__ == "__main__":
    main()
