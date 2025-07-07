# utils/validators.py

def validate_player_data(player):
    """
    Validate individual player dictionary.

    Required fields:
      - name (non-empty string)
      - age (positive integer)
      - club (non-empty string)

    Raises:
        ValueError: if any validation fails
    """
    required_fields = ['name', 'age', 'club']

    for field in required_fields:
        if field not in player:
            raise ValueError(f"Missing required field: {field}")
    
    if not isinstance(player['name'], str) or not player['name'].strip():
        raise ValueError("Invalid player name")

    if not isinstance(player['club'], str) or not player['club'].strip():
        raise ValueError("Invalid player club")

    if not isinstance(player['age'], int) or player['age'] <= 0:
        raise ValueError("Invalid player age")
