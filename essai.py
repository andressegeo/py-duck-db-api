joins = [
    {
        "from_column": "contact",
        "from_path": [
            "user",
            "company",
            "contact_from"
        ],
        "from_table": "company",
        "to_column": "id",
        "to_path": [
            "user",
            "company",
            "phone"
        ],
        "to_table": "phone"
    },
    {
        "from_column": "type",
        "from_path": [
            "user",
            "contact",
            "type_from"
        ],
        "from_table": "phone",
        "to_column": "id",
        "to_path": [
            "user",
            "contact",
            "type"
        ],
        "to_table": "type"
    },
    {
        "from_column": "type",
        "from_path": [
            "user",
            "contact",
            "type_from"
        ],
        "from_table": "phone",
        "to_column": "id",
        "to_path": [
            "user",
            "contact",
            "type"
        ],
        "to_table": "type"
    },
    {
        "from_column": "type",
        "from_path": [
            "user",
            "company",
            "contact",
            "type_from"
        ],
        "from_table": "phone",
        "to_column": "id",
        "to_path": [
            "user",
            "company",
            "contact",
            "type"
        ],
        "to_table": "type"
    },
    {
        "from_column": "type",
        "from_path": [
            "user",
            "company",
            "contact",
            "type_from"
        ],
        "from_table": "phone",
        "to_column": "id",
        "to_path": [
            "user",
            "company",
            "contact",
            "type"
        ],
        "to_table": "type"
    },
    {
        "from_column": "contact",
        "from_path": [
            "user",
            "contact_from"
        ],
        "from_table": "user",
        "to_column": "id",
        "to_path": [
            "user",
            "phone"
        ],
        "to_table": "phone"
    },
    {
        "from_column": "company",
        "from_path": [
            "user",
            "company_from"
        ],
        "from_table": "user",
        "to_column": "id",
        "to_path": [
            "user",
            "company"
        ],
        "to_table": "company"
    }
]


