import re
string = 'fn_function_name(wow)'
print(re.sub(r'([a-zA-Z_]+).+', r'\1', string))