
# - ListV2
#  - __init__
#     - self.values
#  - __add__
#  - __sub__
#  - __mul__
#  - __truediv__
#  - append
#  - mean
#  - __iter__
#  - __next__
#  - __repr___
 
# - DataFrame
#  - __init__
#      - self._index - a dictionary to map text to row index
#      - self.data (dict of ListV2 where each column is a key)
#      - self.columns a simple list
#  - set_index
#  - __setitem__
#  - __getitem__
#  - loc
#  - iteritems
#  - iterrows
#  - as_type
#  - drop
#  - mean
#  - __repr__


 
class ListV2:
    def __init__(self, values):
        if isinstance(values, (list, tuple)):
            self.values=values[:]
        else:
            raise ValueError
        
    def __str__(self):
        return str(self.values)
    
    def __repr__(self):
        return f'{self.values}'
        
    def __iter__(self):
        self.i=0
        return self
    
    def __next__(self):
        if (self.i >=len(self.values)):
            raise StopIteration
        else:
            values_val= self.values[self.i]
            self.i += 1
            return values_val
        
        
    def __add__(self,y):
        if(isinstance(y, (int, float))):
            return ListV2([y + ele for ele in self.values])
        
        elif(isinstance(y, (list,tuple))):
            if (len(self.values)==len(y)):
                return ListV2([self.values[ele]+y[ele] for ele in range(len(y))])
            else: 
                raise ValueError
        else:
            return ListV2([self.values[ele] + y.values[ele] for ele in range(len(self.values))])
          
    
    def __sub__(self,y):
        if(isinstance(y, (int, float))):
            return ListV2([ele - y for ele in self.values])
        
        elif(isinstance(y, (list,tuple))):
            if (len(self.values)==len(y)):
                return ListV2([self.values[ele] - y[ele] for ele in range(len(y))])
            else:
                raise ValueError
                
        else: 
            return ListV2([self.values[i] - y.values[i] for i in range(len(self.values))])

    def __mul__(self,y):
        
        if(isinstance(y, (int, float))):
            return ListV2([ele * y for ele in self.values])
        
        elif(isinstance(y, (list,tuple))):
            if (len(self.values)==len(y)):
                return ListV2([self.values[ele] * y[ele] for ele in range(len(y))])
            else:
                raise ValueError
                
        else: 
            return ListV2([self.values[i] * y.values[i] for i in range(len(self.values))])
        
    def __truediv__(self,y):
        
        if(isinstance(y, (int, float))):
            return ListV2([ele/y for ele in self.values])
        
        elif(isinstance(y, (list,tuple))):
            if (len(self.values)==len(y)):
                return ListV2([round(self.values[ele] / y[ele],2) for ele in range(len(y))])
            else:
                raise ValueError
                
        else: 
            return ListV2([round(self.values[i] / y.values[i],2) for i in range(len(self.values))])

    @property        
    def tolist(self):
        return list(self.values)

    def append(self, value):
        self.values.append(value)

    def mean(self):
        return round(sum(self.values) / len(self.values),2)
    
    def __getitem__(self, index):
        if isinstance(index, int):
            return self.values[index]
        elif isinstance(index, slice):
            start, stop, step = index.indices(len(self.values))
            return ListV2(self.values[start:stop:step])
        elif isinstance(index, tuple):
            rows, cols = index
            if isinstance(rows, int):
                return self.values[rows][cols]
            elif isinstance(rows, slice):
                start_row, stop_row, step_row = rows.indices(len(self.values))
                if isinstance(cols, int):
                    return self.values[start_row:stop_row:step_row][cols]
                elif isinstance(cols, slice):
                    start_col, stop_col, step_col = cols.indices(len(self.values[0]))
                    return ListV2([row[start_col:stop_col:step_col] for row in self.values[start_row:stop_row:step_row]])
        else:
            raise ValueError("Invalid index type")

    


class DataFrame:
    def __init__(self, data, columns=None,index=None):
        self.index = index or None
        self._index = {}
        self.data = {}
        self.columns = list(columns) if columns is not None else []


        if isinstance(data, dict):
            columns = list(data.items())[0][1].keys()
            self.columns = list(columns)
            for i, k in enumerate(data.items()):
                for col, value in k[1].items():
                    if col not in self.data:
                        self.data[col] = []
                    self.data[col].append(value)
                    if k[0] not in self._index:
                        self._index[k[0]] = {}
                    self._index[k[0]][col] = value

        elif isinstance(data[0], dict):
            for i, row_dict in enumerate(data):
                for col, value in row_dict.items():
                    if col not in self.columns:
                        self.columns.append(col)
                    if col not in self.data:
                        self.data[col] = ListV2([])
                    self.data[col].append(value)
                    if i not in self._index:
                        self._index[i] = {}
                    self._index[i][col] = value
        elif isinstance(data[0], tuple):
            for i, row_tuple in enumerate(data):
                for j, value in enumerate(row_tuple):
                    col = self.columns[j] if j < len(self.columns) else str(j)
                    if len(self.columns) < j + 1:
                        self.columns.append(col)
                    if col not in self.data:
                        self.data[col] = ListV2([])
                    self.data[col].append(value)
                    if i not in self._index:
                        self._index[i] = {}
                    self._index[i][col] = value
        else:
            for i, values in enumerate(data):
                for j, value in enumerate(values):
                    col = self.columns[j] if j < len(self.columns) else str(j)
                    if len(self.columns) < j + 1:
                        self.columns.append(col)
                    if col not in self.data:
                        self.data[col] = ListV2([])
                    self.data[col].append(value)
                    if i not in self._index:
                        self._index[i] = {}
                    self._index[i][col] = value


    def set_index(self, columns):
         self._index={k:v for k,v in zip(columns, self._index.values())}

    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            column, row = key
            if column not in self.columns:
                self.columns.append(column)
                self.data[column] = ListV2([None] * len(self._index))
                for i, col in enumerate(self.columns):
                    for j, val in enumerate(self.data[col].values):
                        if i == len(self.columns) - 1:
                            self._index[j][column] = None
                        else:
                            self._index[j][column] = val
            self.data[column][row] = value
            self._index[row][column] = value
        elif isinstance(key, tuple) and len(key) == 2 and isinstance(key[0], slice) and isinstance(key[1], slice):
            column_slice, row_slice = key
            for i, col in enumerate(self.columns[column_slice]):
                self.data[col] = self.data[col][row_slice]
            self._index = {i: {col: self._index[j][col] for col in self.columns[column_slice]} for i, j in enumerate(range(row_slice.start or 0, row_slice.stop or len(self._index), row_slice.step or 1))}


    def __getitem__(self, key):
        if isinstance(key, ListV2):
            indices = [i for i, val in enumerate(key) if val]
            return DataFrame([row[indices] for row in self.data], columns=self.columns)
        elif isinstance(key, tuple):
            if len(key) >= 2 and (isinstance(key[0], slice) or isinstance(key[1], slice)):
                rows = key[0] or slice(None)
                cols = key[1] or slice(None)
                data = [self.data[col][rows] for col in self.columns[cols]]
                return DataFrame(list(zip(*data)), columns=self.columns[cols])
            else:
                return DataFrame([self.data[self.columns[i]][key[1]] for i in range(len(self.columns)) if i in key[1]], columns=key[1])
        elif isinstance(key, slice):
            start, stop, step = key.start or 0, key.stop or len(self._index), key.step or 1
            data = [self.loc(i) for i in range(start, stop, step)]
            return DataFrame(data, columns=self.columns)
        elif isinstance(key, list):
                data_dict = {col: self.data[col].values for col in key}
                return DataFrame(list(zip(*[data_dict[col] for col in key])), columns=key)
        else:
            return self.data[key]



    def loc(self, index):
        if isinstance(index,tuple):
            row, col = index
            result = {key: {k: int(v) for k, v in value.items() if k in col} for key, value in self._index.items() if key in row}
            self.index = {key: index for index, key in enumerate(result.keys())}
            return DataFrame((result),col,self.index)
        else:
            return {col: self.data[col][index] for col in self.columns}

    def iteritems(self):
        new_dict = {}
        for i, col_name in enumerate(self.data):
            new_dict[f'E{i+1}'] = list(self.data[col_name])
        return new_dict

    def iterrows(self):
        self._index = {k: {k2: int(v2) for k2, v2 in v.items()} for k, v in self._index.items()}
        return [(student_name, tuple(self._index[student_name][exam_name] for exam_name in self._index[student_name].keys()))
            for student_name in self._index.keys()]


    def as_type(self, column, type):
        self.data[column] = ListV2([type(val) if val is not None else None for val in self.data[column].values])

    def drop(self, col):
        if col in self.columns:
            self.columns.remove(col)
            del self.data[col]
            for i in self._index:
                del self._index[i][col]
        else:
            print(f"Column '{col}' not found")

    def mean(self, columns=None):
        if columns is None:
            columns = self.columns
        return {col: sum(self.data[col].values) / len(self.data[col].values) for col in columns}


    def __repr__(self):
        rows = []
        for i, values in self._index.items():
            row_values = [str(i)]
            for col in self.columns:
                row_values.append(str(values.get(col, '')))
            row_str = ",".join(row_values)
            rows.append(row_str)
        header_str = ",".join([''] + list(self.columns))
        rows_str = "\n".join(rows)
        return f"{header_str}\n{rows_str}"





        