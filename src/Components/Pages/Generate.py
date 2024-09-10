from datetime import datetime


# Date code calculation
def calculate_date_code(purchase_date_str):
    start_date = datetime(2024, 8, 24)
    purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d")
    date_difference = (purchase_date - start_date).days
    start_counter = 8240
    date_code = start_counter - date_difference
    return date_code


# Map date code to grid locations
def map_date_code_to_grid(date_code):
    date_code_str = str(date_code).zfill(4)
    return {
        'I2': date_code_str[0],
        'H3': date_code_str[1],
        'E1': date_code_str[2],
        'C1': date_code_str[3]
    }


# Map sales counter to grid locations
def map_sales_counter_to_grid(sales_counter):
    sales_counter_str = str(sales_counter).zfill(9)
    return {
        'I1': sales_counter_str[0],
        'G2': sales_counter_str[1],
        'D1': sales_counter_str[2],
        'B3': sales_counter_str[3],
        'B1': sales_counter_str[4],
        'D3': sales_counter_str[5],
        'H2': sales_counter_str[6],
        'A3': sales_counter_str[7],
        'A1': sales_counter_str[8]
    }


# Define ciphers for buyer's and owner's names
buyer_name_cipher = {
    'A': (65, 38), 'B': (10, 89), 'C': (76, 3), 'D': (80, 41), 'E': (17, 97),
    'F': (81, 64), 'G': (27, 7), 'H': (45, 98), 'I': (86, 50), 'J': (1, 30),
    'K': (97, 85), 'L': (40, 19), 'M': (84, 65), 'N': (23, 54), 'O': (77, 51),
    'P': (55, 24), 'Q': (33, 58), 'R': (82, 63), 'S': (5, 49), 'T': (93, 36),
    'U': (62, 95), 'V': (60, 20), 'W': (57, 67), 'X': (73, 56), 'Y': (9, 69),
    'Z': (35, 44)
}

owner_name_cipher = {
    'A': (81, 18), 'B': (46, 53), 'C': (72, 75), 'D': (12, 37), 'E': (90, 6),
    'F': (70, 87), 'G': (31, 32), 'H': (52, 74), 'I': (11, 16), 'J': (85, 47),
    'K': (63, 91), 'L': (28, 66), 'M': (96, 43), 'N': (2, 8), 'O': (42, 92),
    'P': (78, 59), 'Q': (21, 62), 'R': (99, 25), 'S': (51, 34), 'T': (39, 72),
    'U': (28, 13), 'V': (26, 79), 'W': (83, 4), 'X': (29, 68), 'Y': (60, 48),
    'Z': (14, 94)
}

# Define country codes
country_code_mapping = {
    'USA': 7, 'Canada': 4, 'Europe': 8, 'Asia': 9, 'South America': 1,
    'Mexico': 2, 'Africa': 5, 'China': 3
}


# Map names to grid locations
def map_name_to_grid(name, cipher, positions):
    name = name.upper().split()
    if len(name) < 2:
        raise ValueError("Both first name and last name are required")

    first_name = name[0]
    last_name = name[1]

    first_initial = first_name[0]
    last_initial = last_name[0]

    first_initial_values = cipher.get(first_initial, (0, 0))
    last_initial_values = cipher.get(last_initial, (0, 0))

    # Split and map values
    first_initial_digits = [int(d) for d in str(first_initial_values[0])]
    last_initial_digits = [int(d) for d in str(last_initial_values[1])]

    mapping = {
        positions[0]: first_initial_digits[0],
        positions[1]: first_initial_digits[1],
        positions[2]: last_initial_digits[0],
        positions[3]: last_initial_digits[1]
    }
    return mapping


# Get the 4-digit number based on sales counter
def get_4_digit_number(sales_counter, initial_counter):
    # Calculate the number to display
    offset = sales_counter - initial_counter
    return str(offset).zfill(4)


# Assemble the full serial number grid
def assemble_serial_number(purchase_date, sales_counter, buyer_name, owner_name, country, series):
    date_code = calculate_date_code(purchase_date)
    date_code_mapping = map_date_code_to_grid(date_code)
    sales_counter_mapping = map_sales_counter_to_grid(sales_counter)

    # Map buyer and owner names
    buyer_name_mapping = map_name_to_grid(buyer_name, buyer_name_cipher, ['H1', 'G3', 'G1', 'C2'])
    owner_name_mapping = map_name_to_grid(owner_name, owner_name_cipher, ['C3', 'I3', 'B2', 'A2'])

    # Determine the country code
    country_code = country_code_mapping.get(country, 6)
    country_code_mapping_final = {'D2': country_code}

    # Determine the series letter
    series_letter = {
        'founder': 'F',
        'builder': 'B',
        'grandparent': 'G'
    }.get(series.lower(), 'X')  # Default to 'X' if series is not recognized

    series_mapping = {'E2': series_letter}

    # Get the initial sales counter value
    initial_sales_counter = 177402716  # Initial sales counter value

    # Get the incremented 4-digit number
    incremented_number = get_4_digit_number(sales_counter, initial_sales_counter)

    # Map the 4-digit number to specific grid locations
    incremented_number_mapping = {
        'E3': incremented_number[0],
        'F1': incremented_number[1],
        'F2': incremented_number[2],
        'F3': incremented_number[3]
    }

    # Combine all mappings
    serial_number_grid = {**date_code_mapping, **sales_counter_mapping, **buyer_name_mapping, **owner_name_mapping,
                          **country_code_mapping_final, **incremented_number_mapping, **series_mapping}

    return serial_number_grid


# Format the grid for display
def format_grid(serial_number_grid):
    # Define the grid layout
    rows = [
        ['A1', 'A2', 'A3'],
        ['B1', 'B2', 'B3'],
        ['C1', 'C2', 'C3'],
        ['D1', 'D2', 'D3'],
        ['E1', 'E2', 'E3'],
        ['F1', 'F2', 'F3'],
        ['G1', 'G2', 'G3'],
        ['H1', 'H2', 'H3'],
        ['I1', 'I2', 'I3']
    ]

    # Format each row
    def format_row(row):
        return ' '.join(f'{serial_number_grid.get(cell, " "):<2}' for cell in row)

    # Format grid into three parts
    formatted_output = []
    formatted_output.append(' '.join(format_row(row) for row in rows[:3]))  # Top 3 rows
    formatted_output.append(' '.join(format_row(row) for row in rows[3:6]))  # Middle 3 rows
    formatted_output.append(' '.join(format_row(row) for row in rows[6:]))  # Bottom 3 rows

    return '\n'.join(formatted_output)


# Example usage
#purchase_date = "2024-10-09"
#sales_counter = 177402716  # Updated sales counter
#buyer_name = "Donald Trump"
#owner_name = "Kamala Harris"
#country = "USA"
series = "founder"  # Change as needed

#serial_number = assemble_serial_number(purchase_date, sales_counter, buyer_name, owner_name, country, series)
#formatted_grid = format_grid(serial_number)
#print(formatted_grid)

def generate_serial_key(purchase_date, sales_counter, buyer_name, owner_name, country):
    serial_number_grid = assemble_serial_number(purchase_date, sales_counter, buyer_name, owner_name, country, series)
    return format_grid(serial_number_grid)