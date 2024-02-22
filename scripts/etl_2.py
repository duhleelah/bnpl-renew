from .constants import *
from .extract import *
from .transform import *
from .load import *
from .read import *
from .join import *
from .misc_changes import *
from .etl import *

def run_etl_2(prefix:str="") -> None:
    """
    Runs the 2nd etl with no joins
    - Parameters
        - prefix: Used for access from other directories
    - Returns
        - None, but saves the necessary files
    """
    internal_raw_files(prefix)
    internal_transform_files_2(prefix)
    return

if __name__ == "__main__":
    run_etl_2()