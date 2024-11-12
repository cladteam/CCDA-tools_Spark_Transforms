

# can you do what amounts to a linux "diff" in python?
from io import StringIO
import difflib

file_a = StringIO(
    """
    This is the first line.
    This is the second line.
    This is a third line.
    """)
                  
file_b = StringIO(
    """
    This is the first line.
    This is the second line.
    This is a changed third line.
    This is a fourth line.
    """)
            
print("context_diff")    
diff = difflib.context_diff(file_a.readlines(), file_b.readlines())
delta = ''.join(diff)
print(delta)

file_a.seek(0)
file_b.seek(0) 

print("ndiff")
diff = difflib.ndiff(file_a.readlines(), file_b.readlines())
delta = ''.join(diff)
print(delta)

file_a.seek(0)
file_b.seek(0) 


print("unified")
diff = difflib.unified_diff(file_a.readlines(), file_b.readlines())
delta = ''.join(diff)
print(delta)

print("same through unified")
diff = difflib.unified_diff(
    """
    one line
    two line
    """,
    """
    one line
    two line
    """
    )
delta = ''.join(diff)
print(f"\"{delta}\"")
print(type(delta))
                  
                  
                  