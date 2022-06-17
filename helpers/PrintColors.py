class PrintColors:
    """Helper class to simplify colorful printing"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_color(message, color, end='\n'):
    """
        (str, PrintColor, str(optional)) -> None
        Prints the specificed message in the specified color, then resets the print color.
    """
    print(f"{color}{message}{PrintColors.ENDC}", end=end)